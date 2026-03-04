"""Stress test for Prefect's proactive automation trigger race condition handling.

Rapidly mutates a proactive automation's `within` interval and enabled state,
then validates that triggered events settle on the correct final interval.
"""

import asyncio
import random
import sys
from datetime import datetime, timedelta, timezone
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.events.actions import DoNothing
from prefect.events.filters import (
    EventFilter,
    EventNameFilter,
    EventOccurredFilter,
    EventOrder,
    EventResourceFilter,
)
from prefect.events.schemas.automations import AutomationCore, EventTrigger, Posture

MUTATIONS = 40
TOLERANCE_FRACTION = 0.25  # 25% tolerance on interval matching


def build_automation(name: str, within: timedelta) -> AutomationCore:
    return AutomationCore(
        name=name,
        description="Stress test for proactive automation triggers",
        enabled=True,
        trigger=EventTrigger(
            posture=Posture.Proactive,
            expect={"stress-test.never-happens"},
            threshold=1,
            within=within,
        ),
        actions=[DoNothing()],
    )


def check_trigger_intervals(
    events: list, expected_interval: timedelta, tolerance: timedelta
) -> tuple[int, list[str]]:
    """Returns (number of bad intervals, lines of output)."""
    issues = 0
    lines: list[str] = []
    for i in range(1, len(events)):
        prev_time = events[i - 1].occurred
        curr_time = events[i].occurred
        delta = curr_time - prev_time

        ok = abs(delta - expected_interval) <= tolerance
        marker = "OK" if ok else "BAD"
        if not ok:
            issues += 1
        lines.append(
            f"  Event {i}: {prev_time} -> {curr_time}  "
            f"delta={delta}  expected={expected_interval}  [{marker}]"
        )
    return issues, lines


async def main() -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    automation_name = f"stress-proactive-{timestamp}"

    async with get_client() as client:
        # --- Setup ---
        initial_within = timedelta(seconds=30)
        automation = build_automation(automation_name, initial_within)

        print(f"Creating proactive automation: {automation_name}")
        automation_id: UUID = await client.create_automation(automation)
        print(f"Automation ID: {automation_id}")
        print()

        try:
            await run_test(client, automation_id, automation_name)
        finally:
            print()
            print(f"Cleaning up: deleting automation {automation_id}")
            await client.delete_automation(automation_id)
            print("Deleted.")


async def run_test(client, automation_id: UUID, automation_name: str) -> None:
    # --- Build mutation sequence ---
    actions: list[tuple[str, int | None]] = []
    for i in range(MUTATIONS):
        if random.random() < 0.15 and i < MUTATIONS - 3:
            actions.append(("disable", None))
            actions.append(("enable", None))
        else:
            seconds = random.randint(10, 60)
            actions.append(("within", seconds))

    # Always end with a within change so we know the final expected interval
    final_seconds = random.randint(10, 60)
    actions.append(("within", final_seconds))

    print(f"Firing {len(actions)} mutations...")
    print("-" * 60)

    last_within = 30  # initial value from setup
    for i, (kind, seconds) in enumerate(actions, 1):
        if kind == "disable":
            print(f"  [{i:3d}] DISABLE automation")
            await client.pause_automation(automation_id)
        elif kind == "enable":
            print(f"  [{i:3d}] ENABLE  automation")
            await client.resume_automation(automation_id)
        else:
            print(f"  [{i:3d}] SET within -> {seconds}s")
            updated = build_automation(automation_name, timedelta(seconds=seconds))
            await client.update_automation(automation_id, updated)
            last_within = seconds

        pause = random.uniform(0.25, 1.0)
        print(f"          (pause {pause * 1_000:.0f}ms)")
        await asyncio.sleep(pause)

    mutations_finished = datetime.now(timezone.utc)
    final_within = last_within

    print("-" * 60)
    print(f"Final within set: {final_within}s")
    print(f"Automation left: ENABLED")
    print(f"Mutations finished at: {mutations_finished.isoformat()}")
    print()

    # --- Verify persisted state ---
    persisted = await client.read_automation(automation_id)
    if persisted is None:
        print("ERROR: automation not found after mutations!")
        sys.exit(1)

    actual_within = persisted.trigger.within
    actual_enabled = persisted.enabled

    print("=== AUTOMATION STATE ===")
    print(f"Expected within:  {timedelta(seconds=final_within)}")
    print(f"Actual within:    {actual_within}")
    print(f"Enabled:          {actual_enabled}")

    within_ok = actual_within == timedelta(seconds=final_within)
    enabled_ok = actual_enabled is True

    if not within_ok or not enabled_ok:
        print()
        print("MISMATCH on automation state!")
        if not within_ok:
            print(f"  within: expected {final_within}s, got {actual_within}")
        if not enabled_ok:
            print(f"  enabled: expected True, got {actual_enabled}")
        sys.exit(1)

    print("Automation state looks correct.")
    print()

    # --- Settle & validate triggered events ---
    expected_interval = timedelta(seconds=final_within)
    tolerance = expected_interval * TOLERANCE_FRACTION

    # We need at least 3 trigger cycles to compare intervals between them,
    # plus some buffer for the system to settle after mutations
    settle_time = expected_interval * 5
    settle_attempts = max(6, int(settle_time.total_seconds() / 10))
    settle_sleep = 10

    print(
        f"Waiting for triggered events (up to {settle_attempts * settle_sleep}s, "
        f"expecting triggers every ~{final_within}s)..."
    )
    print()

    resource_id = f"prefect-cloud.automation.{automation_id}"

    for attempt in range(1, settle_attempts + 1):
        print(
            f"  Polling for triggers... "
            f"(attempt {attempt}/{settle_attempts}, sleeping {settle_sleep}s)"
        )
        await asyncio.sleep(settle_sleep)

        event_filter = EventFilter(
            occurred=EventOccurredFilter(
                since=mutations_finished,
                until=datetime.now(timezone.utc),
            ),
            event=EventNameFilter(
                name=["prefect-cloud.automation.triggered"],
            ),
            resource=EventResourceFilter(
                id=[resource_id],
            ),
            order=EventOrder.ASC,
        )

        page = await client.read_events(filter=event_filter)
        events = page.events

        print(f"    Found {len(events)} triggered event(s) since mutations finished")

        if len(events) < 3:
            print(f"    Need at least 3 events to compare intervals, waiting...")
            continue

        issues, lines = check_trigger_intervals(events, expected_interval, tolerance)

        if issues == 0:
            print()
            print(f"=== TRIGGER DETAILS ({len(events)} events) ===")
            for line in lines:
                print(line)
            print()
            print(
                f"ALL GOOD: proactive triggers settled on {final_within}s interval "
                f"(tolerance ±{tolerance})."
            )
            sys.exit(0)

        print(f"    {issues} bad interval(s) -- not settled yet.")

    # Never settled
    print()
    print(f"=== FAILED after {settle_attempts * settle_sleep}s ===")
    print(f"Expected trigger interval: {expected_interval}")
    print()

    page = await client.read_events(filter=event_filter)
    events = page.events
    if events:
        _, lines = check_trigger_intervals(events, expected_interval, tolerance)
        for line in lines:
            print(line)
    else:
        print("No triggered events found at all!")

    print()
    print("Proactive triggers did NOT settle on the final interval.")
    sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
