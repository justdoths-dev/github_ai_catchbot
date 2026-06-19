from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

from src.services.maintenance import bounded_stale_outbox_hygiene_runner as source
from src.services.maintenance.bounded_stale_outbox_hygiene_runner import (
    BoundedStaleOutboxHygieneRepositoryHandle,
    BoundedStaleOutboxHygieneRuntimeConfig,
    StaleOutboxRow,
    uuid_suffix,
)
from tools import bounded_stale_outbox_hygiene_runner as runner


def _uuid(suffix: str) -> UUID:
    return UUID(hex=f"00000000000040008000{suffix.rjust(12, '0')}")


EVENT_ID = _uuid("5629b78b")
JUDGE_RUN_ID = _uuid("ae1aa579")
JUDGE_OUTPUT_ID = _uuid("35f1e656")
RAW_DB_URL = "redacted_database_locator_sentinel"


class FakeRepository:
    def __init__(self) -> None:
        self.row = StaleOutboxRow(
            event_id=EVENT_ID,
            event_type="judge.output.ready.v1",
            aggregate_type="judge_run",
            aggregate_id=JUDGE_RUN_ID,
            payload_json={"judge_output_id": str(JUDGE_OUTPUT_ID), "raw": "private payload"},
            status="pending",
            created_at=datetime.now(timezone.utc),
        )

    async def fetch_pending_events(self, *, limit):
        return [self.row][:limit]

    async def fetch_events_by_suffix(self, *, event_suffix, limit):
        return [self.row][:limit] if EVENT_ID.hex.endswith(event_suffix) else []

    async def fetch_events_by_suffix_for_update(self, *, event_suffix, limit):
        return [self.row][:limit] if EVENT_ID.hex.endswith(event_suffix) else []

    async def judge_output_exists(self, judge_output_id):
        return judge_output_id == JUDGE_OUTPUT_ID

    async def count_analyses_for_judge_output(self, judge_output_id):
        return 1 if judge_output_id == JUDGE_OUTPUT_ID else 0

    async def count_policy_apply_events_for_judge_output(self, judge_output_id):
        return 0

    async def count_notification_plans_for_candidate_group(self, candidate_group_id):
        return 0

    async def delivery_result_has_plan_and_record(self, *, notification_plan_id, notification_delivery_record_id):
        return False

    async def delivery_result_current_event_published(self, *, notification_plan_id, notification_delivery_record_id):
        return False

    async def has_stale_resolution_proof(self, *, event_id, classification):
        return False

    async def is_canonically_relay_eligible(self, *, event_id):
        return event_id == EVENT_ID

    async def insert_stale_resolution_proof(self, *, event_id, classification):
        raise AssertionError("inventory must not insert proof")


def _loader() -> BoundedStaleOutboxHygieneRuntimeConfig:
    return BoundedStaleOutboxHygieneRuntimeConfig(database_url=RAW_DB_URL)


def test_runner_uses_source_level_module_and_config_type() -> None:
    assert runner.BoundedStaleOutboxHygieneConfig is source.BoundedStaleOutboxHygieneConfig
    assert runner.BoundedStaleOutboxHygieneRuntimeConfig is source.BoundedStaleOutboxHygieneRuntimeConfig


def test_inventory_cli_delegates_to_source_and_prints_redacted_json(capsys) -> None:
    repository = FakeRepository()

    async def builder(runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            assert commit is False
            state.database_rolled_back = True

        return BoundedStaleOutboxHygieneRepositoryHandle(repository=repository, close=close)

    exit_code = runner.main(
        [
            "--mode",
            "inventory",
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--scan-limit",
            "10",
        ],
        runtime_config_loader=_loader,
        repository_builder=builder,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_stale_outbox_hygiene_v1"
    assert parsed["counts_by_classification"]["judge_output_ready_already_handed_off"] == 1
    assert parsed["suffixes_by_classification"]["judge_output_ready_already_handed_off"] == [uuid_suffix(EVENT_ID)]
    for raw in (str(EVENT_ID), str(JUDGE_RUN_ID), str(JUDGE_OUTPUT_ID), RAW_DB_URL, "private payload"):
        assert raw not in captured.out


def test_cli_rejects_full_uuid_suffix_without_loading_runtime_config(capsys) -> None:
    exit_code = runner.main(
        [
            "--mode",
            "plan",
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--target-event-suffix",
            str(EVENT_ID),
        ],
        runtime_config_loader=lambda: (_ for _ in ()).throw(AssertionError("runtime config must not load")),
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 1
    assert parsed["error_code"] == "target_event_suffix_invalid"
    assert str(EVENT_ID) not in output
