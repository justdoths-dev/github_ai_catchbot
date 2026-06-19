from __future__ import annotations

import ast
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest

from src.services.maintenance.bounded_stale_outbox_hygiene_runner import (
    BoundedStaleOutboxHygieneConfig,
    BoundedStaleOutboxHygieneRepositoryHandle,
    BoundedStaleOutboxHygieneRuntimeConfig,
    StaleOutboxRow,
    run_bounded_stale_outbox_hygiene,
    run_bounded_stale_outbox_hygiene_sync,
    uuid_suffix,
)
from src.services.outbox_relay.repositories import OutboxRelayRepository


ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = ROOT / "src/services/maintenance/bounded_stale_outbox_hygiene_runner.py"
TOOL_PATH = ROOT / "tools/bounded_stale_outbox_hygiene_runner.py"


def _uuid(suffix: str) -> UUID:
    return UUID(hex=f"00000000000040008000{suffix.rjust(12, '0')}")


JUDGE_READY_EVENT_ID = _uuid("5629b78b")
JUDGE_RUN_ID = _uuid("ae1aa579")
JUDGE_OUTPUT_ID = _uuid("35f1e656")
POLICY_EVENT_ID = _uuid("265b89")
CANDIDATE_GROUP_ID = _uuid("78dbbb98")
DELIVERY_EVENT_ID = _uuid("d317e001")
NOTIFICATION_PLAN_ID = _uuid("f1a47001")
DELIVERY_RECORD_ID = _uuid("d1e17001")
UNKNOWN_EVENT_ID = _uuid("bad0bad0")
RAW_PAYLOAD_SENTINEL = "private raw payload must not print"
RAW_DB_URL = "redacted_database_locator_sentinel"


class FakeRepository:
    def __init__(self, rows: list[StaleOutboxRow]) -> None:
        self.rows = rows
        self.judge_outputs: set[UUID] = set()
        self.analysis_counts: dict[UUID, int] = {}
        self.policy_event_counts: dict[UUID, int] = {}
        self.notification_plan_counts: dict[UUID, int] = {}
        self.delivery_pairs: set[tuple[UUID, UUID]] = set()
        self.published_delivery_pairs: set[tuple[UUID, UUID]] = set()
        self.proofs: set[tuple[UUID, str]] = set()
        self.insert_calls: list[tuple[UUID, str]] = []
        self.fetch_suffix_calls: list[str] = []
        self.locked_suffix_calls: list[str] = []
        self.force_relay_eligible: dict[UUID, bool] = {}

    async def fetch_pending_events(self, *, limit: int) -> list[StaleOutboxRow]:
        return [row for row in self.rows if row.status == "pending"][:limit]

    async def fetch_events_by_suffix(self, *, event_suffix: str, limit: int) -> list[StaleOutboxRow]:
        self.fetch_suffix_calls.append(event_suffix)
        return [row for row in self.rows if row.event_id.hex.endswith(event_suffix)][:limit]

    async def fetch_events_by_suffix_for_update(self, *, event_suffix: str, limit: int) -> list[StaleOutboxRow]:
        self.locked_suffix_calls.append(event_suffix)
        return [row for row in self.rows if row.event_id.hex.endswith(event_suffix)][:limit]

    async def judge_output_exists(self, judge_output_id: UUID) -> bool:
        return judge_output_id in self.judge_outputs

    async def count_analyses_for_judge_output(self, judge_output_id: UUID) -> int:
        return self.analysis_counts.get(judge_output_id, 0)

    async def count_policy_apply_events_for_judge_output(self, judge_output_id: UUID) -> int:
        return self.policy_event_counts.get(judge_output_id, 0)

    async def count_notification_plans_for_candidate_group(self, candidate_group_id: UUID) -> int:
        return self.notification_plan_counts.get(candidate_group_id, 0)

    async def delivery_result_has_plan_and_record(
        self,
        *,
        notification_plan_id: UUID,
        notification_delivery_record_id: UUID,
    ) -> bool:
        return (notification_plan_id, notification_delivery_record_id) in self.delivery_pairs

    async def delivery_result_current_event_published(
        self,
        *,
        notification_plan_id: UUID,
        notification_delivery_record_id: UUID,
    ) -> bool:
        return (notification_plan_id, notification_delivery_record_id) in self.published_delivery_pairs

    async def has_stale_resolution_proof(self, *, event_id: UUID, classification: str) -> bool:
        return (event_id, classification) in self.proofs

    async def is_canonically_relay_eligible(self, *, event_id: UUID) -> bool:
        if event_id in self.force_relay_eligible:
            return self.force_relay_eligible[event_id]
        row = next((candidate for candidate in self.rows if candidate.event_id == event_id), None)
        if row is None or row.status != "pending":
            return False
        if row.event_type == "judge.output.ready.v1":
            return (event_id, "judge_output_ready_already_handed_off") not in self.proofs
        if row.event_type == "analysis.policy.apply.v1":
            return (event_id, "policy_apply_already_analyzed") not in self.proofs
        return True

    async def insert_stale_resolution_proof(self, *, event_id: UUID, classification: str) -> bool:
        self.insert_calls.append((event_id, classification))
        key = (event_id, classification)
        if key in self.proofs:
            return False
        self.proofs.add(key)
        return True


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository
        self.close_commits: list[bool] = []
        self.calls = 0

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        self.calls += 1
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            if commit:
                state.database_committed = True
            else:
                state.database_rolled_back = True

        return BoundedStaleOutboxHygieneRepositoryHandle(repository=self.repository, close=close)


class LockingFakeRepository(FakeRepository):
    def __init__(self, rows: list[StaleOutboxRow]) -> None:
        super().__init__(rows)
        self.row_lock = asyncio.Lock()

    async def fetch_events_by_suffix_for_update(self, *, event_suffix: str, limit: int) -> list[StaleOutboxRow]:
        await self.row_lock.acquire()
        return await super().fetch_events_by_suffix_for_update(event_suffix=event_suffix, limit=limit)


class LockingFakeRepositoryBuilder(FakeRepositoryBuilder):
    repository: LockingFakeRepository

    async def __call__(self, runtime_config, state, logger):
        handle = await super().__call__(runtime_config, state, logger)

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            if commit:
                state.database_committed = True
            else:
                state.database_rolled_back = True
            if self.repository.row_lock.locked():
                self.repository.row_lock.release()

        return BoundedStaleOutboxHygieneRepositoryHandle(repository=handle.repository, close=close)


class RelayResult:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class RelaySessionFromMaintenanceRepository:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository

    async def execute(self, statement, params=None):
        del statement
        limit = int((params or {}).get("limit", 100))
        rows = []
        for row in self.repository.rows:
            if not await self.repository.is_canonically_relay_eligible(event_id=row.event_id):
                continue
            rows.append(
                {
                    "event_id": row.event_id,
                    "event_type": row.event_type,
                    "aggregate_type": row.aggregate_type,
                    "aggregate_id": row.aggregate_id,
                    "dedupe_key": f"dedupe:{row.event_id.hex[-8:]}",
                    "payload_json": row.payload_json,
                    "status": row.status,
                    "fail_count": 0,
                    "created_at": row.created_at,
                }
            )
        rows.sort(key=lambda item: (item["created_at"], item["event_id"]))
        return RelayResult(rows[:limit])


def _runtime_config() -> BoundedStaleOutboxHygieneRuntimeConfig:
    return BoundedStaleOutboxHygieneRuntimeConfig(database_url=RAW_DB_URL)


def _approved_config(**overrides) -> BoundedStaleOutboxHygieneConfig:
    values = {
        "mode": "inventory",
        "operator_approved": True,
        "allow_runtime_config": True,
        "allow_database_read": True,
        "scan_limit": 50,
    }
    values.update(overrides)
    return BoundedStaleOutboxHygieneConfig(**values)


def _run(repository: FakeRepository, config: BoundedStaleOutboxHygieneConfig):
    builder = FakeRepositoryBuilder(repository)
    result = run_bounded_stale_outbox_hygiene_sync(
        config,
        runtime_config_loader=_runtime_config,
        repository_builder=builder,
    )
    return result, builder


def _row(
    *,
    event_id: UUID,
    event_type: str,
    aggregate_type: str,
    aggregate_id: UUID,
    payload_json: dict[str, object],
    status: str = "pending",
) -> StaleOutboxRow:
    payload = dict(payload_json)
    payload["private_payload"] = RAW_PAYLOAD_SENTINEL
    return StaleOutboxRow(
        event_id=event_id,
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        payload_json=payload,
        status=status,
        created_at=datetime.now(timezone.utc),
    )


def _judge_ready_row(*, status: str = "pending") -> StaleOutboxRow:
    return _row(
        event_id=JUDGE_READY_EVENT_ID,
        event_type="judge.output.ready.v1",
        aggregate_type="judge_run",
        aggregate_id=JUDGE_RUN_ID,
        payload_json={"judge_run_id": str(JUDGE_RUN_ID), "judge_output_id": str(JUDGE_OUTPUT_ID)},
        status=status,
    )


def _policy_row(*, status: str = "pending") -> StaleOutboxRow:
    return _row(
        event_id=POLICY_EVENT_ID,
        event_type="analysis.policy.apply.v1",
        aggregate_type="candidate_group",
        aggregate_id=CANDIDATE_GROUP_ID,
        payload_json={"judge_output_id": str(JUDGE_OUTPUT_ID), "candidate_group_id": str(CANDIDATE_GROUP_ID)},
        status=status,
    )


def _delivery_row() -> StaleOutboxRow:
    return _row(
        event_id=DELIVERY_EVENT_ID,
        event_type="notification.delivery.result.v1",
        aggregate_type="notification_plan",
        aggregate_id=NOTIFICATION_PLAN_ID,
        payload_json={
            "notification_plan_id": str(NOTIFICATION_PLAN_ID),
            "notification_delivery_record_id": str(DELIVERY_RECORD_ID),
        },
    )


def _unknown_row() -> StaleOutboxRow:
    return _row(
        event_id=UNKNOWN_EVENT_ID,
        event_type="unknown.event.v1",
        aggregate_type="unknown",
        aggregate_id=UNKNOWN_EVENT_ID,
        payload_json={"unknown_id": str(UNKNOWN_EVENT_ID)},
    )


def test_inventory_classifies_stale_judge_output_ready_already_handed_off() -> None:
    repository = FakeRepository([_judge_ready_row()])
    repository.judge_outputs.add(JUDGE_OUTPUT_ID)
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1

    result, builder = _run(repository, _approved_config())
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert builder.close_commits == [False]
    assert report["counts_by_classification"]["judge_output_ready_already_handed_off"] == 1
    candidate = report["candidates"][0]
    assert candidate["event_suffix"] == uuid_suffix(JUDGE_READY_EVENT_ID)
    assert candidate["classification"] == "judge_output_ready_already_handed_off"
    assert candidate["next_action"] == "eligible_for_stale_resolution"
    assert candidate["analysis_count"] == 1
    assert candidate["policy_apply_event_count"] == 0
    assert candidate["judge_output_exists"] is True
    assert candidate["matching_resolution_proof_exists"] is False
    assert candidate["canonical_relay_eligible"] is True
    assert candidate["hot_path_candidate"] is True


def test_inventory_classifies_stale_policy_apply_already_analyzed() -> None:
    repository = FakeRepository([_policy_row()])
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1
    repository.notification_plan_counts[CANDIDATE_GROUP_ID] = 5

    result, _builder = _run(repository, _approved_config())
    candidate = result.to_sanitized_dict()["candidates"][0]

    assert candidate["classification"] == "policy_apply_already_analyzed"
    assert candidate["next_action"] == "eligible_for_stale_resolution"
    assert candidate["analysis_count"] == 1
    assert candidate["notification_plan_count"] == 5


def test_delivery_result_hygiene_is_classified_but_not_auto_resolved_by_default() -> None:
    repository = FakeRepository([_delivery_row()])
    repository.delivery_pairs.add((NOTIFICATION_PLAN_ID, DELIVERY_RECORD_ID))
    repository.published_delivery_pairs.add((NOTIFICATION_PLAN_ID, DELIVERY_RECORD_ID))

    result, _builder = _run(repository, _approved_config())
    report = result.to_sanitized_dict()

    candidate = report["candidates"][0]
    assert candidate["classification"] == "delivery_result_hygiene"
    assert candidate["next_action"] == "hygiene_only"
    assert candidate["deliberately_left_unchanged"] is True
    assert candidate["canonical_relay_eligible"] is True
    assert candidate["hot_path_candidate"] is False
    assert report["deliberately_left_unchanged_suffixes"] == [uuid_suffix(DELIVERY_EVENT_ID)]
    assert repository.insert_calls == []


def test_unsafe_or_unknown_is_never_resolved() -> None:
    repository = FakeRepository([_unknown_row()])

    result, builder = _run(
        repository,
        _approved_config(
            mode="execute",
            allow_database_write=True,
            target_event_suffixes=(uuid_suffix(UNKNOWN_EVENT_ID),),
            classifications=("unsafe_or_unknown",),
        ),
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "target_event_classification_not_executable"
    assert report["event_outbox_status_updated_count"] == 0
    assert report["stale_resolution_proofs_inserted_count"] == 0
    assert repository.insert_calls == []
    assert builder.close_commits == [False]


@pytest.mark.parametrize(
    ("config", "error_code"),
    [
        (BoundedStaleOutboxHygieneConfig(), "operator_approval_missing"),
        (_approved_config(mode="execute", allow_database_write=False), "database_write_not_allowed"),
        (_approved_config(mode="execute", allow_database_write=True), "target_event_suffix_required"),
        (
            _approved_config(
                mode="execute",
                allow_database_write=True,
                target_event_suffixes=(uuid_suffix(JUDGE_READY_EVENT_ID),),
            ),
            "classification_required",
        ),
    ],
)
def test_execute_mode_requires_approval_write_exact_suffixes_and_classification(config, error_code) -> None:
    repository = FakeRepository([_judge_ready_row()])
    loader_called = False

    def loader():
        nonlocal loader_called
        loader_called = True
        return _runtime_config()

    result = run_bounded_stale_outbox_hygiene_sync(
        config,
        runtime_config_loader=loader,
        repository_builder=FakeRepositoryBuilder(repository),
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == error_code
    assert report["database_read_attempted"] is False
    assert report["database_write_attempted"] is False
    assert loader_called is False
    assert repository.insert_calls == []


def test_execute_resolves_only_exact_safe_selected_rows_with_idempotent_proof() -> None:
    repository = FakeRepository([_judge_ready_row(), _policy_row()])
    repository.judge_outputs.add(JUDGE_OUTPUT_ID)
    repository.policy_event_counts[JUDGE_OUTPUT_ID] = 1
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1

    result, builder = _run(
        repository,
        _approved_config(
            mode="execute",
            allow_database_write=True,
            target_event_suffixes=(uuid_suffix(JUDGE_READY_EVENT_ID), uuid_suffix(POLICY_EVENT_ID)),
            classifications=("judge_output_ready_already_handed_off", "policy_apply_already_analyzed"),
        ),
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["action_attempted"] is True
    assert report["database_write_attempted"] is True
    assert report["database_committed"] is True
    assert report["post_commit_readback_passed"] is True
    assert report["event_outbox_status_updated_count"] == 0
    assert report["stale_resolution_proofs_inserted_count"] == 2
    assert sorted(report["resolved_or_proven_suffixes"]) == sorted(
        [uuid_suffix(JUDGE_READY_EVENT_ID), uuid_suffix(POLICY_EVENT_ID)]
    )
    assert repository.insert_calls == [
        (JUDGE_READY_EVENT_ID, "judge_output_ready_already_handed_off"),
        (POLICY_EVENT_ID, "policy_apply_already_analyzed"),
    ]
    assert builder.close_commits == [True, False]
    assert repository.locked_suffix_calls == [uuid_suffix(JUDGE_READY_EVENT_ID), uuid_suffix(POLICY_EVENT_ID)]

    repeat, _repeat_builder = _run(
        repository,
        _approved_config(
            mode="execute",
            allow_database_write=True,
            target_event_suffixes=(uuid_suffix(JUDGE_READY_EVENT_ID),),
            classifications=("judge_output_ready_already_handed_off",),
        ),
    )
    repeat_report = repeat.to_sanitized_dict()
    assert repeat.ok is True
    assert repeat_report["stale_resolution_proofs_inserted_count"] == 0
    assert repeat_report["stale_resolution_proofs_already_present_count"] == 1


@pytest.mark.asyncio
async def test_execute_inserted_maintenance_proof_removes_target_from_actual_relay_fetch() -> None:
    repository = FakeRepository([_judge_ready_row()])
    repository.judge_outputs.add(JUDGE_OUTPUT_ID)
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1
    relay_repository = OutboxRelayRepository(RelaySessionFromMaintenanceRepository(repository))

    before = await relay_repository.fetch_pending_batch(limit=10)
    result = await run_bounded_stale_outbox_hygiene(
        _approved_config(
            mode="execute",
            allow_database_write=True,
            target_event_suffixes=(uuid_suffix(JUDGE_READY_EVENT_ID),),
            classifications=("judge_output_ready_already_handed_off",),
        ),
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
    )
    after = await relay_repository.fetch_pending_batch(limit=10)

    assert [row.event_id for row in before] == [JUDGE_READY_EVENT_ID]
    assert result.ok is True
    assert repository.proofs == {(JUDGE_READY_EVENT_ID, "judge_output_ready_already_handed_off")}
    assert [row.event_id for row in after] == []


def test_execute_fail_closed_if_suffix_missing_nonunique_status_or_classification_changed() -> None:
    nonunique_suffix = "11111111"
    duplicate_a = _row(
        event_id=_uuid("11111111"),
        event_type="judge.output.ready.v1",
        aggregate_type="judge_run",
        aggregate_id=JUDGE_RUN_ID,
        payload_json={"judge_output_id": str(JUDGE_OUTPUT_ID)},
    )
    duplicate_b = _row(
        event_id=_uuid("111111111"),
        event_type="judge.output.ready.v1",
        aggregate_type="judge_run",
        aggregate_id=JUDGE_RUN_ID,
        payload_json={"judge_output_id": str(JUDGE_OUTPUT_ID)},
    )

    cases = [
        (FakeRepository([_judge_ready_row()]), "deadbeef", "target_event_suffix_missing"),
        (FakeRepository([duplicate_a, duplicate_b]), nonunique_suffix, "target_event_suffix_not_unique"),
        (
            FakeRepository([_judge_ready_row(status="published")]),
            uuid_suffix(JUDGE_READY_EVENT_ID),
            "target_event_status_changed",
        ),
        (
            FakeRepository([_policy_row()]),
            uuid_suffix(POLICY_EVENT_ID),
            "target_event_classification_changed",
        ),
    ]

    for repository, suffix, error_code in cases:
        repository.judge_outputs.add(JUDGE_OUTPUT_ID)
        repository.analysis_counts[JUDGE_OUTPUT_ID] = 1
        result, _builder = _run(
            repository,
            _approved_config(
                mode="execute",
                allow_database_write=True,
                target_event_suffixes=(suffix,),
                classifications=("judge_output_ready_already_handed_off",),
            ),
        )
        assert result.ok is False
        assert result.to_sanitized_dict()["error_code"] == error_code
        assert repository.insert_calls == []
        if error_code in {"target_event_status_changed", "target_event_classification_changed"}:
            assert repository.locked_suffix_calls == [suffix]


@pytest.mark.parametrize("mode", ["plan", "proof"])
def test_exact_plan_and_proof_fail_closed_if_suffix_missing_or_nonunique(mode: str) -> None:
    duplicate_a = _row(
        event_id=_uuid("11111111"),
        event_type="judge.output.ready.v1",
        aggregate_type="judge_run",
        aggregate_id=JUDGE_RUN_ID,
        payload_json={"judge_output_id": str(JUDGE_OUTPUT_ID)},
    )
    duplicate_b = _row(
        event_id=_uuid("111111111"),
        event_type="judge.output.ready.v1",
        aggregate_type="judge_run",
        aggregate_id=JUDGE_RUN_ID,
        payload_json={"judge_output_id": str(JUDGE_OUTPUT_ID)},
    )
    cases = [
        (FakeRepository([_judge_ready_row()]), "deadbeef", "target_event_suffix_missing"),
        (FakeRepository([duplicate_a, duplicate_b]), "11111111", "target_event_suffix_not_unique"),
    ]

    for repository, suffix, error_code in cases:
        repository.judge_outputs.add(JUDGE_OUTPUT_ID)
        repository.analysis_counts[JUDGE_OUTPUT_ID] = 1
        result, _builder = _run(
            repository,
            _approved_config(
                mode=mode,
                target_event_suffixes=(suffix,),
                classifications=("judge_output_ready_already_handed_off",),
            ),
        )
        assert result.ok is False
        assert result.to_sanitized_dict()["error_code"] == error_code


@pytest.mark.parametrize(
    ("row", "classifications", "error_code"),
    [
        (_judge_ready_row(status="published"), ("judge_output_ready_already_handed_off",), "target_event_status_changed"),
        (_policy_row(), ("judge_output_ready_already_handed_off",), "target_event_classification_changed"),
    ],
)
def test_proof_mode_fails_on_status_or_classification_drift(
    row: StaleOutboxRow,
    classifications: tuple[str, ...],
    error_code: str,
) -> None:
    repository = FakeRepository([row])
    repository.judge_outputs.add(JUDGE_OUTPUT_ID)
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1
    repository.proofs.add((row.event_id, "judge_output_ready_already_handed_off"))

    result, _builder = _run(
        repository,
        _approved_config(
            mode="proof",
            target_event_suffixes=(uuid_suffix(row.event_id),),
            classifications=classifications,
        ),
    )

    assert result.ok is False
    assert result.to_sanitized_dict()["error_code"] == error_code


def test_execute_blocks_if_post_commit_relay_readback_still_eligible() -> None:
    repository = FakeRepository([_judge_ready_row()])
    repository.judge_outputs.add(JUDGE_OUTPUT_ID)
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1
    repository.force_relay_eligible[JUDGE_READY_EVENT_ID] = True

    result, builder = _run(
        repository,
        _approved_config(
            mode="execute",
            allow_database_write=True,
            target_event_suffixes=(uuid_suffix(JUDGE_READY_EVENT_ID),),
            classifications=("judge_output_ready_already_handed_off",),
        ),
    )
    report = result.to_sanitized_dict()

    assert result.ok is False
    assert report["error_code"] == "canonical_relay_exclusion_missing"
    assert report["stale_resolution_proofs_inserted_count"] == 1
    assert report["post_commit_readback_passed"] is False
    assert builder.close_commits == [True, False]


@pytest.mark.asyncio
async def test_concurrent_execute_attempts_insert_one_proof_and_second_reports_already_proven() -> None:
    repository = LockingFakeRepository([_judge_ready_row()])
    repository.judge_outputs.add(JUDGE_OUTPUT_ID)
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1
    builder = LockingFakeRepositoryBuilder(repository)
    config = _approved_config(
        mode="execute",
        allow_database_write=True,
        target_event_suffixes=(uuid_suffix(JUDGE_READY_EVENT_ID),),
        classifications=("judge_output_ready_already_handed_off",),
    )

    first, second = await asyncio.gather(
        run_bounded_stale_outbox_hygiene(config, runtime_config_loader=_runtime_config, repository_builder=builder),
        run_bounded_stale_outbox_hygiene(config, runtime_config_loader=_runtime_config, repository_builder=builder),
    )
    reports = [first.to_sanitized_dict(), second.to_sanitized_dict()]

    assert {report["stale_resolution_proofs_inserted_count"] for report in reports} == {0, 1}
    assert {report["stale_resolution_proofs_already_present_count"] for report in reports} == {0, 1}
    assert repository.proofs == {(JUDGE_READY_EVENT_ID, "judge_output_ready_already_handed_off")}
    assert repository.insert_calls == [(JUDGE_READY_EVENT_ID, "judge_output_ready_already_handed_off")]


def test_inventory_reports_truncation_without_claiming_complete_global_zero() -> None:
    rows = [
        _judge_ready_row(),
        _row(
            event_id=_uuid("c001"),
            event_type="unknown.event.v1",
            aggregate_type="unknown",
            aggregate_id=UNKNOWN_EVENT_ID,
            payload_json={},
        ),
    ]
    repository = FakeRepository(rows)
    repository.judge_outputs.add(JUDGE_OUTPUT_ID)
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1

    result, _builder = _run(repository, _approved_config(scan_limit=1))
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["scan_truncated"] is True
    assert report["scanned_candidate_count"] == 1
    assert report["remaining_hot_path_candidate_count"] == 1
    assert report["remaining_hot_path_candidate_count_is_complete"] is False


def test_matching_proof_changes_exact_inventory_hot_path_count_to_zero() -> None:
    repository = FakeRepository([_judge_ready_row()])
    repository.judge_outputs.add(JUDGE_OUTPUT_ID)
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1
    repository.proofs.add((JUDGE_READY_EVENT_ID, "judge_output_ready_already_handed_off"))

    result, _builder = _run(repository, _approved_config())
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["remaining_hot_path_candidate_count"] == 0
    assert report["candidates"][0]["canonical_relay_eligible"] is False


def test_output_is_suffix_only_and_redacted() -> None:
    repository = FakeRepository([_judge_ready_row()])
    repository.judge_outputs.add(JUDGE_OUTPUT_ID)
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1

    result, _builder = _run(repository, _approved_config())
    output = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert uuid_suffix(JUDGE_READY_EVENT_ID) in output
    for raw in (str(JUDGE_READY_EVENT_ID), str(JUDGE_RUN_ID), str(JUDGE_OUTPUT_ID), RAW_PAYLOAD_SENTINEL, RAW_DB_URL):
        assert raw not in output
    assert '"payload_json":' not in output
    assert '"dedupe_key":' not in output


def test_inventory_counts_hot_path_from_canonical_relay_eligibility() -> None:
    repository = FakeRepository([_judge_ready_row(), _delivery_row(), _unknown_row()])
    repository.judge_outputs.add(JUDGE_OUTPUT_ID)
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1
    repository.delivery_pairs.add((NOTIFICATION_PLAN_ID, DELIVERY_RECORD_ID))

    result, _builder = _run(repository, _approved_config())
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["remaining_hot_path_candidate_count"] == 2
    assert report["remaining_hot_path_candidate_count_is_complete"] is True
    by_suffix = {candidate["event_suffix"]: candidate for candidate in report["candidates"]}
    assert by_suffix[uuid_suffix(JUDGE_READY_EVENT_ID)]["hot_path_candidate"] is True
    assert by_suffix[uuid_suffix(DELIVERY_EVENT_ID)]["hot_path_candidate"] is False


def test_proof_mode_requires_exact_target_and_canonical_exclusion() -> None:
    repository = FakeRepository([_judge_ready_row(), _delivery_row(), _unknown_row()])
    repository.judge_outputs.add(JUDGE_OUTPUT_ID)
    repository.analysis_counts[JUDGE_OUTPUT_ID] = 1
    repository.delivery_pairs.add((NOTIFICATION_PLAN_ID, DELIVERY_RECORD_ID))
    repository.proofs.add((JUDGE_READY_EVENT_ID, "judge_output_ready_already_handed_off"))

    result, _builder = _run(
        repository,
        _approved_config(
            mode="proof",
            target_event_suffixes=(uuid_suffix(JUDGE_READY_EVENT_ID),),
            classifications=("judge_output_ready_already_handed_off",),
        ),
    )
    report = result.to_sanitized_dict()

    assert result.ok is True
    assert report["resolved_or_proven_suffixes"] == [uuid_suffix(JUDGE_READY_EVENT_ID)]
    assert report["remaining_hot_path_candidate_count"] == 0
    assert report["candidates"][0]["canonical_relay_eligible"] is False


def test_static_authority_guard_for_stale_outbox_hygiene_runner() -> None:
    forbidden_import_roots = {
        "openai",
        "httpx",
        "requests",
        "aiohttp",
        "subprocess",
        "docker",
        "redis",
    }
    forbidden_service_imports = {
        "src.services.notifier_telegram",
        "src.services.policy_engine",
        "src.services.analysis_validator",
        "src.services.judge_openai",
        "src.services.gh_enricher",
        "src.services.x_enricher",
        "src.services.web_enricher",
    }
    forbidden_calls = {
        "send",
        "send_message",
        "edit",
        "edit_message_text",
        "xadd",
        "xack",
        "xclaim",
        "xautoclaim",
        "xgroup_create",
        "xreadgroup",
        "run_forever",
    }
    for path in (SOURCE_PATH, TOOL_PATH):
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text)
        imported_roots = set()
        imported_modules = set()
        called_names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported_roots.add(alias.name.split(".", 1)[0])
                    imported_modules.add(alias.name)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
                imported_modules.add(node.module)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    called_names.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    called_names.add(node.func.attr)

        assert imported_roots.isdisjoint(forbidden_import_roots), path
        assert not any(module in forbidden_service_imports for module in imported_modules), path
        assert called_names.isdisjoint(forbidden_calls), path

    source_text = SOURCE_PATH.read_text(encoding="utf-8")
    assert "UPDATE event_outbox" not in source_text
    assert "INSERT INTO job_attempts" in source_text
    assert "FOR UPDATE" in source_text
    assert "canonical_relay_eligible_sql" in source_text
