"""Stress test for the Prefect scheduler's race condition handling.

Rapidly mutates a deployment's schedule interval and active state, then
validates that the API settles on the final configuration.
"""

import asyncio
import random
import sys
from datetime import timedelta
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterDeploymentId,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.sorting import FlowRunSort

DEPLOYMENT_ID = UUID("04e839c0-adca-429a-9dda-cae7e435fb7d")
SCHEDULE_ID = UUID("536bb08b-7478-4f7a-8d2a-db3004a2cbab")

MUTATIONS = 40
SETTLE_ATTEMPTS = 12
SETTLE_INTERVAL = 5


async def update_schedule(client, *, minutes: int, active: bool) -> None:
    await client._client.patch(
        f"/deployments/{DEPLOYMENT_ID}/schedules/{SCHEDULE_ID}",
        json={
            "schedule": {"interval": minutes * 60},
            "active": active,
        },
    )


async def get_scheduled_runs(client, deployment_id: UUID) -> list:
    return await client.read_flow_runs(
        flow_run_filter=FlowRunFilter(
            deployment_id=FlowRunFilterDeploymentId(any_=[str(deployment_id)]),
            state=FlowRunFilterState(
                type=FlowRunFilterStateType(any_=["SCHEDULED"])
            ),
        ),
        sort=FlowRunSort.EXPECTED_START_TIME_ASC,
        limit=50,
    )


def check_intervals(
    scheduled: list, expected_delta: timedelta, tolerance: timedelta
) -> tuple[int, list[str]]:
    """Returns (number of bad intervals, lines of output)."""
    issues = 0
    lines: list[str] = []
    for j in range(1, len(scheduled)):
        prev = scheduled[j - 1].expected_start_time
        curr = scheduled[j].expected_start_time
        delta = curr - prev

        ok = abs(delta - expected_delta) <= tolerance
        marker = "OK" if ok else "BAD"
        if not ok:
            issues += 1
        lines.append(
            f"  Run {j}: {prev} -> {curr}  "
            f"delta={delta}  expected={expected_delta}  [{marker}]"
        )
    return issues, lines


async def main() -> None:
    async with get_client() as client:
        deployment = await client.read_deployment(str(DEPLOYMENT_ID))
        print(f"Deployment: {deployment.name}")
        print(f"Schedule ID: {SCHEDULE_ID}")
        print()

        # Build a sequence of mutations: mostly interval changes, with some
        # disable/enable toggles mixed in
        actions: list[tuple[str, int | None, bool]] = []
        for i in range(MUTATIONS):
            if random.random() < 0.15 and i < MUTATIONS - 3:
                actions.append(("disable", None, False))
                actions.append(("enable", None, True))
            else:
                minutes = random.randint(2, 30)
                actions.append(("interval", minutes, True))

        # Make sure the last action is an interval change so we know what to
        # expect, and the schedule is left active
        final_minutes = random.randint(2, 30)
        actions.append(("interval", final_minutes, True))

        print(f"Firing {len(actions)} mutations...")
        print("-" * 60)

        last_interval = None
        for i, (kind, minutes, active) in enumerate(actions, 1):
            if kind == "disable":
                print(f"  [{i:3d}] DISABLE schedule")
                await update_schedule(client, minutes=last_interval or 5, active=False)
            elif kind == "enable":
                print(f"  [{i:3d}] ENABLE  schedule")
                await update_schedule(
                    client, minutes=last_interval or 5, active=True
                )
            else:
                print(f"  [{i:3d}] SET interval -> {minutes} minutes")
                await update_schedule(client, minutes=minutes, active=active)
                last_interval = minutes

            # Random pause to let the scheduler start reacting mid-stream
            pause = random.uniform(0.05, 1.0)
            print(f"          (pause {pause * 1_000:.0f}ms)")
            await asyncio.sleep(pause)

        final_interval = last_interval
        print("-" * 60)
        print(f"Final interval set: {final_interval} minutes")
        print(f"Schedule left: ACTIVE")
        print()

        # Re-read the schedule to confirm it was persisted
        deployment = await client.read_deployment(str(DEPLOYMENT_ID))
        schedule = next(s for s in deployment.schedules if s.id == SCHEDULE_ID)
        actual_interval = schedule.schedule.interval
        actual_active = schedule.active

        print("=== SCHEDULE STATE ===")
        print(f"Expected interval: {timedelta(minutes=final_interval)}")
        print(f"Actual interval:   {actual_interval}")
        print(f"Schedule active:   {actual_active}")

        interval_ok = actual_interval == timedelta(minutes=final_interval)
        active_ok = actual_active is True

        if not interval_ok or not active_ok:
            print()
            print("MISMATCH on schedule state!")
            if not interval_ok:
                print(f"  interval: expected {final_interval}m, got {actual_interval}")
            if not active_ok:
                print(f"  active: expected True, got {actual_active}")
            sys.exit(1)

        print("Schedule state looks correct.")
        print()

        # Poll for the scheduler to reconcile runs
        expected_delta = timedelta(minutes=final_interval)
        tolerance = timedelta(seconds=30)

        for attempt in range(1, SETTLE_ATTEMPTS + 1):
            print(
                f"Waiting for runs to settle... "
                f"(attempt {attempt}/{SETTLE_ATTEMPTS}, "
                f"sleeping {SETTLE_INTERVAL}s)"
            )
            await asyncio.sleep(SETTLE_INTERVAL)

            scheduled = await get_scheduled_runs(client, DEPLOYMENT_ID)

            if not scheduled:
                print("  No scheduled runs found yet.")
                continue

            issues, lines = check_intervals(scheduled, expected_delta, tolerance)

            if issues == 0:
                print(f"  Found {len(scheduled)} runs, all intervals match!")
                print()
                print("=== RUN DETAILS ===")
                for line in lines:
                    print(line)
                print()
                print("ALL GOOD: scheduler settled correctly.")
                sys.exit(0)

            print(f"  Found {len(scheduled)} runs, {issues} bad interval(s) -- not settled yet.")

        # If we get here, it never settled
        print()
        print(f"=== FAILED after {SETTLE_ATTEMPTS * SETTLE_INTERVAL}s ===")
        print(f"Expected all intervals to be {expected_delta}")
        print()
        scheduled = await get_scheduled_runs(client, DEPLOYMENT_ID)
        _, lines = check_intervals(scheduled, expected_delta, tolerance)
        for line in lines:
            print(line)
        print()
        print("Scheduler did NOT settle on the final interval.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
