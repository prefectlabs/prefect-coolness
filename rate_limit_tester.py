from prefect import flow, task
from prefect.settings import PREFECT_API_KEY, PREFECT_API_URL
import httpx
import asyncio
from datetime import datetime

async def make_api_request(client: httpx.AsyncClient, url: str, headers: dict, stats: dict):
    """Make a single API request and track stats."""
    call_num = stats["total_calls"] + 1
    stats["total_calls"] = call_num

    try:
        response = await client.post(
            url,
            json={"limit": 1},
            headers=headers,
            timeout=10.0
        )
        response.raise_for_status()
        stats["successful_calls"] += 1

        if call_num % 100 == 0:
            print(f"[{call_num}] Still succeeding... (429s so far: {stats['count_429s']})")

        return "success"

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            stats["count_429s"] += 1

            if stats["first_429_at"] is None:
                stats["first_429_at"] = call_num
                print(f"\n🚨 First 429 at call #{call_num} (after {stats['successful_calls']} successes)")
                print(f"Will continue for {stats['max_429s_after_first']} more 429s...\n")

            if stats["count_429s"] % 10 == 0:
                print(f"[{call_num}] 429 count: {stats['count_429s']}")

            return "429"
        else:
            print(f"Unexpected error: {e.response.status_code}")
            raise

@task
async def hammer_api(
    operation: str = "deployments",
    pool_size: int = 50,
    max_429s_after_first: int = 50
):
    """
    Rapidly make API requests until hitting rate limits.

    Args:
        operation: API operation to test ("deployments", "flows", "flow_runs", "work_pools")
        pool_size: Number of concurrent requests to maintain
        max_429s_after_first: How many more 429s to receive before stopping
    """
    api_url = PREFECT_API_URL.value()
    api_key = PREFECT_API_KEY.value()

    if not api_url or not api_key:
        raise ValueError("PREFECT_API_URL and PREFECT_API_KEY must be set")

    # Map operation to endpoint
    endpoints = {
        "deployments": "/deployments/filter",
        "flows": "/flows/filter",
        "flow_runs": "/flow_runs/filter",
        "work_pools": "/work_pools/filter",
    }

    if operation not in endpoints:
        raise ValueError(f"Unknown operation: {operation}. Choose from: {list(endpoints.keys())}")

    url = f"{api_url}{endpoints[operation]}"
    headers = {"Authorization": f"Bearer {api_key}"}

    async with httpx.AsyncClient() as client:
        stats = {
            "total_calls": 0,
            "successful_calls": 0,
            "first_429_at": None,
            "count_429s": 0,
            "max_429s_after_first": max_429s_after_first
        }

        print(f"Starting {operation} API requests at {datetime.now()}")
        print(f"Concurrent pool size: {pool_size}")
        print(f"Target API: {url}")

        # Create initial pool of tasks
        pending = set()
        for _ in range(pool_size):
            task = asyncio.create_task(make_api_request(client, url, headers, stats))
            pending.add(task)

        # Keep replacing completed tasks until we hit our 429 limit
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            # Check if we should stop
            if stats["first_429_at"] and stats["count_429s"] >= max_429s_after_first:
                # Cancel remaining tasks
                for task in pending:
                    task.cancel()
                break

            # Replace completed tasks with new ones
            for _ in done:
                new_task = asyncio.create_task(make_api_request(client, url, headers, stats))
                pending.add(new_task)

        print(f"\n✅ Test complete at {datetime.now()}")
        print(f"Total calls: {stats['total_calls']}")
        print(f"Successful: {stats['successful_calls']}")
        print(f"Rate limited (429s): {stats['count_429s']}")
        print(f"First 429 at call: {stats['first_429_at']}")

        return {
            "total_calls": stats["total_calls"],
            "successful_calls": stats["successful_calls"],
            "rate_limited_calls": stats["count_429s"],
            "first_429_at_call": stats["first_429_at"]
        }

@flow(name="rate-limit-tester")
async def test_rate_limits(operation: str = "deployments"):
    """Test API rate limits for a specific operation."""

    result = await hammer_api(
        operation=operation,
        pool_size=50,
        max_429s_after_first=50
    )

    print("\n📊 Use review_rate_limits to see the impact:")
    print("   prefect mcp review-rate-limits")

    return result

if __name__ == "__main__":
    import sys
    operation = sys.argv[1] if len(sys.argv) > 1 else "deployments"
    asyncio.run(test_rate_limits(operation=operation))
