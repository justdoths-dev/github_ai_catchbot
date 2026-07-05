from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from services.maintenance.source_message_pipeline_inventory import (
    CONFIRM_TOKEN,
    F2_APPROVED_PUBLIC_USERNAMES,
    SELECTION_CONFIRM_TOKEN,
    InventoryCounts,
    NormalizationReadback,
    RuntimeConfigBundle,
    SourceCreatedChannelCandidate,
    SourceCreatedTarget,
    SourceMessagePipelineInventoryComponents,
    SourceMessagePipelineInventoryRequest,
    _fingerprint,
    _target_from_row,
    run_cli,
    run_source_message_pipeline_inventory,
)
from services.router_normalizer.config import RouterNormalizerConfig
from services.router_normalizer.models import NormalizationResult, RedisNormalizeMessage, SourceMessageSnapshot


RAW_UUID = uuid4()
RAW_GITHUB_URL = "https://" + "github.com/" + "example/private-tool"
RAW_TEXT = f"sentinel raw source text {RAW_UUID} {RAW_GITHUB_URL}"
RAW_DB_LOCATOR = "database_locator_sentinel_redacted"
RAW_ERROR_SENTINEL = "error_sentinel_value"
APPROVED_ONE = (F2_APPROVED_PUBLIC_USERNAMES[0],)
APPROVED_THREE = F2_APPROVED_PUBLIC_USERNAMES


class FakeRepository:
    def __init__(
        self,
        *,
        counts: InventoryCounts | None = None,
        targets: list[SourceCreatedTarget] | None = None,
        channel_candidates: list[SourceCreatedChannelCandidate] | None = None,
        readback: NormalizationReadback | None = None,
    ) -> None:
        self.counts = counts or InventoryCounts()
        self.targets = targets or []
        self.channel_candidates = channel_candidates
        self.readback = readback or NormalizationReadback()
        self.inventory_calls = 0
        self.preview_calls = 0
        self.readback_calls = 0

    async def load_inventory_counts(self, *, lookback_hours: int, normalizer_version: str) -> InventoryCounts:
        assert lookback_hours >= 1
        assert normalizer_version == "test-normalizer"
        self.inventory_calls += 1
        return self.counts

    async def load_source_created_preview_targets(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        normalizer_version: str,
        approved_public_usernames: tuple[str, ...],
    ) -> list[SourceCreatedChannelCandidate]:
        assert lookback_hours >= 1
        assert sample_limit >= 1
        assert normalizer_version == "test-normalizer"
        assert approved_public_usernames
        self.preview_calls += 1
        if self.channel_candidates is not None:
            return list(self.channel_candidates)
        candidates: list[SourceCreatedChannelCandidate] = []
        for index, approved_public_username in enumerate(approved_public_usernames):
            target = self.targets[index] if index < len(self.targets) else None
            if target is None:
                candidates.append(
                    SourceCreatedChannelCandidate(
                        approved_public_username=approved_public_username,
                        bucket="missing",
                        reason_code="source_created_event_missing",
                    )
                )
            else:
                candidates.append(
                    SourceCreatedChannelCandidate(
                        approved_public_username=approved_public_username,
                        bucket="selected",
                        reason_code="source_created_target_loaded",
                        target=target,
                    )
                )
        return candidates

    async def load_normalization_readback(
        self,
        *,
        source_message_id: UUID,
        source_version_no: int,
        normalizer_version: str,
    ) -> NormalizationReadback:
        assert source_message_id
        assert source_version_no >= 1
        assert normalizer_version == "test-normalizer"
        self.readback_calls += 1
        return self.readback


class FakeNormalizerService:
    def __init__(self, result: NormalizationResult | None = None) -> None:
        self.result = result or NormalizationResult(
            normalization_run_id=uuid4(),
            signal_detected=True,
            candidate_eligible=True,
            trigger_strength="medium",
            artifact_count=1,
            candidate_group_count=1,
            suppression_reason_codes=[],
        )
        self.calls: list[RedisNormalizeMessage] = []

    async def process_stream_message(self, message: RedisNormalizeMessage) -> NormalizationResult:
        self.calls.append(message)
        return self.result


class CommitRecorder:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> None:
        self.calls += 1


def _router_config() -> RouterNormalizerConfig:
    return RouterNormalizerConfig(
        app_env="test",
        database_url=RAW_DB_LOCATOR,
        redis_url="redis_locator_not_attempted",
        queue_name="q.source.normalize",
        consumer_group="router-normalizer",
        consumer_name="test",
        block_ms=100,
        batch_size=1,
        normalizer_version="test-normalizer",
        short_url_allowlist=("t.co", "bit.ly"),
        short_url_hop_limit=1,
        short_url_timeout_seconds=0.1,
        log_level="INFO",
    )


def _components(
    repository: FakeRepository,
    service: FakeNormalizerService | None = None,
    commit: CommitRecorder | None = None,
) -> tuple[SourceMessagePipelineInventoryComponents, FakeNormalizerService, CommitRecorder]:
    fake_service = service or FakeNormalizerService()
    commit_recorder = commit or CommitRecorder()
    return (
        SourceMessagePipelineInventoryComponents(
            inventory_repository=repository,
            normalizer_service=fake_service,
            commit_active_transaction=commit_recorder,
        ),
        fake_service,
        commit_recorder,
    )


def _target(
    *,
    text: str = "new agent CLI for coding workflows",
    url_surface_json: list[dict] | None = None,
    has_current_normalization: bool = False,
    current_normalization_candidate_eligible: bool | None = None,
    has_candidate_group: bool = False,
    deleted: bool = False,
) -> SourceCreatedTarget:
    source_message_id = uuid4()
    return SourceCreatedTarget(
        event_id=uuid4(),
        source_message_id=source_message_id,
        source_version_no=1,
        snapshot=SourceMessageSnapshot(
            source_message_id=source_message_id,
            source_version_no=1,
            text_body=text,
            caption_text=None,
            text_surface=text,
            entities_json=[],
            url_surface_json=url_surface_json or [],
            raw_message_json={
                "chat_id": 123456789,
                "message_id": 987654321,
                "error_text": RAW_ERROR_SENTINEL,
            },
            deleted_at=datetime.now(timezone.utc) if deleted else None,
        ),
        has_current_normalization=has_current_normalization,
        current_normalization_candidate_eligible=current_normalization_candidate_eligible,
        has_candidate_group=has_candidate_group,
    )


def _version_aware_row(
    *,
    current_version_no: int,
    source_version_no: int,
    current_text: str,
    version_text: str | None,
) -> dict[str, object]:
    source_message_id = uuid4()
    return {
        "event_id": uuid4(),
        "source_message_id": source_message_id,
        "source_version_no": source_version_no,
        "current_version_no": current_version_no,
        "current_text_body": current_text,
        "current_caption_text": None,
        "current_text_surface": current_text,
        "current_entities_json": [],
        "current_url_surface_json": [],
        "current_raw_message_json": {"surface": "current"},
        "deleted_at": None,
        "version_no": source_version_no if version_text is not None else None,
        "version_text_surface": version_text,
        "version_entities_json": [],
        "version_raw_message_json": {"surface": "requested"},
        "current_normalization_candidate_eligible": None,
        "has_current_normalization": False,
        "has_candidate_group": False,
    }


def _counts(**overrides: int) -> InventoryCounts:
    values = {
        "source_message_count_bucketed": 2,
        "source_created_event_count": 2,
        "source_created_pending_count": 1,
        "source_created_published_count": 1,
        "source_created_without_normalization_count": 2,
        "normalization_run_count": 0,
        "normalization_signal_detected_count": 0,
        "normalization_candidate_eligible_count": 0,
        "normalization_suppressed_count": 0,
        "candidate_group_count": 0,
        "artifact_enrichment_request_count": 0,
        "ready_bundle_count": 0,
        "analysis_requested_count": 0,
        "judge_call_requested_count": 0,
        "notification_plan_intent_count": 0,
    }
    values.update(overrides)
    return InventoryCounts(**values)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("argv", "reason_code"),
    [
        (["--mode", "bad", "--env-file", "/tmp/runtime.env"], "invalid_mode"),
        (["--mode", "plan"], "env_file_required"),
        (["--mode", "execute-normalize", "--env-file", "/tmp/runtime.env"], "exact_source_normalization_confirm_missing"),
        (
            [
                "--mode",
                "plan",
                "--env-file",
                "/tmp/runtime.env",
                "--select-latest-unnormalized-source-created",
            ],
            "selection_confirm_missing",
        ),
        (
            [
                "--mode",
                "execute-normalize",
                "--env-file",
                "/tmp/runtime.env",
                "--select-latest-unnormalized-source-created",
                "--selection-confirm",
                SELECTION_CONFIRM_TOKEN,
                "--approved-public-username",
                APPROVED_ONE[0],
                "--confirm",
                CONFIRM_TOKEN,
            ],
            "expected_target_event_fingerprint_missing",
        ),
        (
            [
                "--mode",
                "plan",
                "--env-file",
                "/tmp/runtime.env",
                "--select-latest-unnormalized-source-created",
                "--selection-confirm",
                SELECTION_CONFIRM_TOKEN,
            ],
            "approved_public_username_required",
        ),
        (
            [
                "--mode",
                "plan",
                "--env-file",
                "/tmp/runtime.env",
                "--select-latest-unnormalized-source-created",
                "--selection-confirm",
                SELECTION_CONFIRM_TOKEN,
                "--approved-public-username",
                "trendingrepo",
            ],
            "approved_public_username_not_explicit",
        ),
        (
            [
                "--mode",
                "plan",
                "--env-file",
                "/tmp/runtime.env",
                "--select-latest-unnormalized-source-created",
                "--selection-confirm",
                SELECTION_CONFIRM_TOKEN,
                "--approved-public-username",
                "@nimdaltg",
            ],
            "approved_public_username_not_f2_approved",
        ),
        (["--mode", "plan", "--env-file", "/tmp/runtime.env", "--lookback-hours", "0"], "lookback_hours_out_of_range"),
        (["--mode", "plan", "--env-file", "/tmp/runtime.env", "--sample-limit", "501"], "sample_limit_out_of_range"),
    ],
)
async def test_cli_validation_blocks_before_env_or_db(argv: list[str], reason_code: str) -> None:
    emitted: list[str] = []

    def runtime_loader(_env_file: str) -> RuntimeConfigBundle:
        raise AssertionError("runtime loader must not be called")

    exit_code = await run_cli(argv, emit_json=emitted.append, runtime_config_loader=runtime_loader)

    assert exit_code == 2
    assert json.loads(emitted[0])["reason_code"] == reason_code


@pytest.mark.asyncio
async def test_plan_inventory_read_only_blocks_with_counts_when_no_target() -> None:
    repository = FakeRepository(counts=_counts(), targets=[])
    components, service, commit = _components(repository)

    report = await run_source_message_pipeline_inventory(
        SourceMessagePipelineInventoryRequest(
            mode="plan",
            lookback_hours=72,
            sample_limit=100,
            select_latest_unnormalized_source_created=True,
            approved_public_usernames=APPROVED_ONE,
        ),
        router_config=_router_config(),
        components=components,
    )

    assert report.status == "blocked"
    assert report.reason_code == "approved_channel_selection_missing"
    assert report.source_created_event_count == 2
    assert report.source_created_without_normalization_count == 2
    assert report.normalization_signal_detected_count == 0
    assert report.normalization_attempted is False
    assert report.redis_attempted is False
    assert report.telegram_attempted is False
    assert report.openai_attempted is False
    assert report.external_network_attempted is False
    assert service.calls == []
    assert commit.calls == 0
    assert repository.readback_calls == 0


@pytest.mark.asyncio
async def test_plan_selects_latest_unnormalized_eligible_source_created_target() -> None:
    selected = _target(text="new agent CLI for coding workflows")
    older = _target(text="AI")
    repository = FakeRepository(counts=_counts(), targets=[selected, older])
    components, service, commit = _components(repository)

    report = await run_source_message_pipeline_inventory(
        SourceMessagePipelineInventoryRequest(
            mode="plan",
            lookback_hours=72,
            sample_limit=100,
            select_latest_unnormalized_source_created=True,
            approved_public_usernames=APPROVED_ONE,
        ),
        router_config=_router_config(),
        components=components,
    )

    payload = json.dumps(asdict(report), sort_keys=True)
    assert report.status == "pass"
    assert report.reason_code == "normalization_target_plan_ready"
    assert report.selected_target_event_fingerprint == _fingerprint(selected.event_id)
    assert report.selected_source_message_fingerprint == _fingerprint(selected.source_message_id)
    assert report.selected_source_version_no == 1
    assert report.selected_current_rule_candidate_eligible is True
    assert report.selected_current_rule_reason_codes == ["developer_tool_signal"]
    assert report.approved_channel_count == 1
    assert report.selected_count == 1
    assert report.per_channel[0]["bucket"] == "selected"
    assert selected.source_message_id.hex not in payload
    assert "new agent CLI for coding workflows" not in payload
    assert service.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_plan_selects_one_latest_target_per_explicit_f2_approved_channel() -> None:
    targets = [
        _target(text="new agent CLI for coding workflows"),
        _target(text="new agent CLI for coding workflows"),
        _target(text="new agent CLI for coding workflows"),
        _target(text="older broad target must not be selected"),
    ]
    repository = FakeRepository(counts=_counts(source_created_event_count=4), targets=targets)
    components, service, commit = _components(repository)

    report = await run_source_message_pipeline_inventory(
        SourceMessagePipelineInventoryRequest(
            mode="plan",
            lookback_hours=72,
            sample_limit=1,
            select_latest_unnormalized_source_created=True,
            approved_public_usernames=APPROVED_THREE,
        ),
        router_config=_router_config(),
        components=components,
    )

    payload = json.dumps(asdict(report), sort_keys=True)
    assert report.status == "pass"
    assert report.reason_code == "normalization_target_plan_ready"
    assert report.approved_channel_count == 3
    assert report.selected_count == 3
    assert [item["bucket"] for item in report.per_channel] == ["selected", "selected", "selected"]
    assert [item["target_event_fingerprint"] for item in report.per_channel] == [
        _fingerprint(target.event_id) for target in targets[:3]
    ]
    assert _fingerprint(targets[3].event_id) not in payload
    for username in APPROVED_THREE:
        assert username not in payload
    assert service.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_plan_mismatch_current_eligible_requested_weak_is_not_selected() -> None:
    target = _target_from_row(
        _version_aware_row(
            current_version_no=2,
            source_version_no=1,
            current_text="new agent CLI for coding workflows",
            version_text="AI",
        )
    )
    assert target is not None
    assert target.snapshot.source_version_no == 1
    assert target.snapshot.text_body is None
    assert target.snapshot.caption_text is None
    assert target.snapshot.text_surface == "AI"
    assert target.snapshot.url_surface_json is None
    assert target.snapshot.raw_message_json == {"surface": "requested"}
    repository = FakeRepository(counts=_counts(), targets=[target])
    components, service, commit = _components(repository)

    report = await run_source_message_pipeline_inventory(
        SourceMessagePipelineInventoryRequest(
            mode="plan",
            lookback_hours=72,
            sample_limit=100,
            select_latest_unnormalized_source_created=True,
            approved_public_usernames=APPROVED_ONE,
        ),
        router_config=_router_config(),
        components=components,
    )

    assert report.status == "blocked"
    assert report.reason_code == "approved_channel_selection_blocked"
    assert report.current_rule_candidate_eligible_count == 0
    assert report.current_rule_weak_suppressed_count == 1
    assert report.selected_target_event_fingerprint is None
    assert service.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_plan_mismatch_current_weak_requested_eligible_is_selected() -> None:
    target = _target_from_row(
        _version_aware_row(
            current_version_no=2,
            source_version_no=1,
            current_text="AI",
            version_text="new agent CLI for coding workflows",
        )
    )
    assert target is not None
    assert target.snapshot.text_body is None
    assert target.snapshot.caption_text is None
    assert target.snapshot.text_surface == "new agent CLI for coding workflows"
    assert target.snapshot.url_surface_json is None
    repository = FakeRepository(counts=_counts(), targets=[target])
    components, service, commit = _components(repository)

    report = await run_source_message_pipeline_inventory(
        SourceMessagePipelineInventoryRequest(
            mode="plan",
            lookback_hours=72,
            sample_limit=100,
            select_latest_unnormalized_source_created=True,
            approved_public_usernames=APPROVED_ONE,
        ),
        router_config=_router_config(),
        components=components,
    )

    assert report.status == "pass"
    assert report.reason_code == "normalization_target_plan_ready"
    assert report.selected_target_event_fingerprint == _fingerprint(target.event_id)
    assert report.selected_source_message_fingerprint == _fingerprint(target.source_message_id)
    assert report.selected_source_version_no == 1
    assert report.selected_current_rule_candidate_eligible is True
    assert report.selected_current_rule_reason_codes == ["developer_tool_signal"]
    assert service.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_missing_requested_version_row_is_not_selected_or_executed() -> None:
    missing_version_row = _version_aware_row(
        current_version_no=2,
        source_version_no=1,
        current_text="new agent CLI for coding workflows",
        version_text=None,
    )
    assert _target_from_row(missing_version_row) is None
    repository = FakeRepository(counts=_counts(source_created_without_normalization_count=1), targets=[])
    components, service, commit = _components(repository)

    report = await run_source_message_pipeline_inventory(
        SourceMessagePipelineInventoryRequest(
            mode="execute-normalize",
            lookback_hours=72,
            sample_limit=100,
            select_latest_unnormalized_source_created=True,
            approved_public_usernames=APPROVED_ONE,
            expected_target_event_fingerprint=_fingerprint(missing_version_row["event_id"]),
        ),
        router_config=_router_config(),
        components=components,
    )

    assert report.status == "blocked"
    assert report.reason_code == "approved_channel_selection_missing"
    assert report.selected_target_event_fingerprint is None
    assert report.normalization_attempted is False
    assert service.calls == []
    assert commit.calls == 0
    assert repository.readback_calls == 0


@pytest.mark.asyncio
async def test_execute_normalize_succeeds_with_one_selected_event_and_readback() -> None:
    selected = _target(text="new agent CLI for coding workflows")
    repository = FakeRepository(
        counts=_counts(),
        targets=[selected],
        readback=NormalizationReadback(
            normalization_run_count=1,
            artifact_registry_count=1,
            artifact_observation_count=1,
            candidate_group_proposal_count=1,
            candidate_group_member_count=1,
            candidate_group_primary_member_count=1,
            artifact_enrichment_request_count=0,
        ),
    )
    components, service, commit = _components(repository)

    report = await run_source_message_pipeline_inventory(
        SourceMessagePipelineInventoryRequest(
            mode="execute-normalize",
            lookback_hours=72,
            sample_limit=100,
            select_latest_unnormalized_source_created=True,
            approved_public_usernames=APPROVED_ONE,
            expected_target_event_fingerprint=_fingerprint(selected.event_id),
        ),
        router_config=_router_config(),
        components=components,
    )

    assert report.status == "pass"
    assert report.reason_code == "source_normalization_materialized"
    assert report.normalization_attempted is True
    assert report.normalization_created_or_updated is True
    assert report.candidate_group_created_or_present is True
    assert report.candidate_group_primary_member_count == 1
    assert report.executed_target_count == 1
    assert report.normalization_readbacks == [
        {
            "target_event_fingerprint": _fingerprint(selected.event_id),
            "source_message_fingerprint": _fingerprint(selected.source_message_id),
            "source_version_no": 1,
            "normalization_runs": 1,
            "artifact_registry": 1,
            "artifact_observations": 1,
            "candidate_group_proposals": 1,
            "candidate_group_members": 1,
        }
    ]
    assert len(service.calls) == 1
    assert service.calls[0].trigger_event_id == str(selected.event_id)
    assert service.calls[0].root_object_id == str(selected.source_message_id)
    assert commit.calls == 1
    assert repository.readback_calls == 1
    assert report.redis_attempted is False
    assert report.telegram_attempted is False
    assert report.openai_attempted is False
    assert report.external_network_attempted is False


@pytest.mark.asyncio
async def test_execute_normalize_fingerprint_mismatch_blocks_before_writes() -> None:
    selected = _target(text="new agent CLI for coding workflows")
    repository = FakeRepository(counts=_counts(), targets=[selected])
    components, service, commit = _components(repository)

    report = await run_source_message_pipeline_inventory(
        SourceMessagePipelineInventoryRequest(
            mode="execute-normalize",
            lookback_hours=72,
            sample_limit=100,
            select_latest_unnormalized_source_created=True,
            approved_public_usernames=APPROVED_ONE,
            expected_target_event_fingerprint="0000000000000000",
        ),
        router_config=_router_config(),
        components=components,
    )

    assert report.status == "blocked"
    assert report.reason_code == "target_event_fingerprint_mismatch"
    assert report.normalization_attempted is False
    assert service.calls == []
    assert commit.calls == 0
    assert repository.readback_calls == 0


@pytest.mark.asyncio
async def test_already_normalized_source_is_not_selected_or_reprocessed() -> None:
    normalized = _target(
        text="new agent CLI for coding workflows",
        has_current_normalization=True,
        current_normalization_candidate_eligible=True,
        has_candidate_group=True,
    )
    repository = FakeRepository(
        counts=_counts(source_created_without_normalization_count=0),
        targets=[normalized, _target(text="older broad target must not be selected")],
    )
    components, service, commit = _components(repository)

    report = await run_source_message_pipeline_inventory(
        SourceMessagePipelineInventoryRequest(
            mode="execute-normalize",
            lookback_hours=72,
            sample_limit=100,
            select_latest_unnormalized_source_created=True,
            approved_public_usernames=APPROVED_ONE,
            expected_target_event_fingerprint=_fingerprint(normalized.event_id),
        ),
        router_config=_router_config(),
        components=components,
    )

    assert report.status == "pass"
    assert report.reason_code == "source_normalization_already_materialized"
    assert report.per_channel[0]["bucket"] == "already_normalized"
    assert report.selected_count == 0
    assert service.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_current_rule_recall_candidate_with_existing_old_normalization_is_counted_only() -> None:
    existing_suppressed = _target(
        text="new agent CLI for coding workflows",
        has_current_normalization=True,
        current_normalization_candidate_eligible=False,
        has_candidate_group=False,
    )
    repository = FakeRepository(counts=_counts(), targets=[existing_suppressed])
    components, service, commit = _components(repository)

    report = await run_source_message_pipeline_inventory(
        SourceMessagePipelineInventoryRequest(
            mode="plan",
            lookback_hours=72,
            sample_limit=100,
            select_latest_unnormalized_source_created=True,
            approved_public_usernames=APPROVED_ONE,
        ),
        router_config=_router_config(),
        components=components,
    )

    assert report.status == "pass"
    assert report.reason_code == "source_normalization_already_materialized"
    assert report.current_rule_candidate_eligible_count == 1
    assert report.current_rule_recall_candidate_with_existing_normalization_count == 1
    assert report.selected_target_event_fingerprint is None
    assert service.calls == []
    assert commit.calls == 0


@pytest.mark.asyncio
async def test_redaction_omits_raw_text_ids_urls_db_url_and_error_sentinels() -> None:
    selected = _target(
        text=RAW_TEXT,
        url_surface_json=[
            {
                "observed_url": RAW_GITHUB_URL,
                "source_kind": "entity",
            }
        ],
    )
    repository = FakeRepository(counts=_counts(), targets=[selected])
    components, service, commit = _components(repository)

    report = await run_source_message_pipeline_inventory(
        SourceMessagePipelineInventoryRequest(
            mode="plan",
            lookback_hours=72,
            sample_limit=100,
            select_latest_unnormalized_source_created=True,
            approved_public_usernames=APPROVED_ONE,
        ),
        router_config=_router_config(),
        components=components,
    )

    payload = json.dumps(asdict(report), sort_keys=True)
    assert report.status == "pass"
    assert report.selected_current_rule_reason_codes == ["strong_artifact_link"]
    forbidden = [
        RAW_TEXT,
        str(RAW_UUID),
        str(selected.event_id),
        str(selected.source_message_id),
        "123456789",
        "987654321",
        RAW_DB_LOCATOR,
        RAW_GITHUB_URL,
        RAW_ERROR_SENTINEL,
        APPROVED_ONE[0],
    ]
    for value in forbidden:
        assert value not in payload
    assert service.calls == []
    assert commit.calls == 0
