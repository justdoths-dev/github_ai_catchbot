from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import uuid4

from src.services.analysis_router.repositories import BundleRefreshOutboxRecord
from src.services.maintenance.bounded_candidate_bundle_refresh_event_publish_runner import (
    CandidateBundleRefreshEventRepositoryHandle,
    CandidateCurrentBundleState,
    CONFIRM_TOKEN,
    RefreshEventReference,
)
from src.services.outbox_relay.bounded_candidate_bundle_refresh_outbox_publish_runner import (
    BoundedCandidateBundleRefreshOutboxPublishResult,
    BoundedCandidateBundleRefreshOutboxPublishState,
    BoundedCandidateBundleRefreshPublishRuntimeConfig,
)
from tools import bounded_candidate_bundle_refresh_event_publish_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_candidate_bundle_refresh_event_publish_runner.py"
DB_LOCATOR = "db_locator_omitted_sentinel"
REDIS_LOCATOR = "redis_locator_omitted_sentinel"
RAW_REFRESH_REASON = "secret_reason"
RAW_DEDUPE_KEY = "bundle-refresh:cli-private-dedupe-key"
RAW_PAYLOAD = "payload_json_private"


class FakeRepository:
    def __init__(
        self,
        *,
        candidate_group_id,
        bundle_id,
        existing_event: RefreshEventReference | None = None,
        inserted_event_id=None,
    ) -> None:
        self.candidate_group_id = candidate_group_id
        self.bundle_id = bundle_id
        self.existing_event = existing_event
        self.inserted_event_id = inserted_event_id or uuid4()
        self.insert_calls = []
        self.suffix_calls = []

    async def find_candidate_groups_by_suffix(self, suffix: str, *, limit: int):
        self.suffix_calls.append({"suffix": suffix, "limit": limit})
        return [self.candidate_group_id]

    async def load_candidate_current_bundle(self, candidate_group_id):
        return CandidateCurrentBundleState(
            candidate_group_id=candidate_group_id,
            current_bundle_id=self.bundle_id,
            current_bundle_present=True,
            current_bundle_ready_for_analysis=True,
        )

    async def load_matching_refresh_event(self, *, candidate_group_id, bundle_id, refresh_reason):
        del candidate_group_id, bundle_id, refresh_reason
        return self.existing_event

    async def insert_bundle_refresh_outbox(self, *, candidate_group_id, bundle_id, refresh_reason):
        self.insert_calls.append(
            {
                "candidate_group_id": candidate_group_id,
                "bundle_id": bundle_id,
                "refresh_reason": refresh_reason,
            }
        )
        return BundleRefreshOutboxRecord(event_id=self.inserted_event_id, created=True, status="pending")


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository
        self.close_commits = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)

        return CandidateBundleRefreshEventRepositoryHandle(repository=self.repository, close=close)


class FakePublisherRunner:
    def __init__(self) -> None:
        self.calls = []

    async def __call__(self, config, **kwargs):
        self.calls.append(config)
        state = BoundedCandidateBundleRefreshOutboxPublishState()
        state.redis_publish_attempted = True
        state.event_outbox_status_write_attempted = True
        state.job_attempt_insert_attempted = True
        return BoundedCandidateBundleRefreshOutboxPublishResult(
            status="published",
            ok=True,
            error_code=None,
            error_class=None,
            config=config,
            state=state,
            target_event_id_suffix=None if config.event_id is None else str(config.event_id)[-8:],
            events_seen=1,
            events_published_count=1,
            job_attempts_inserted_count=1,
            queue_name="q.candidate.bundle",
            stage_name="bundle",
            event_outbox_marked_published=True,
        )


def _runtime_config() -> BoundedCandidateBundleRefreshPublishRuntimeConfig:
    return BoundedCandidateBundleRefreshPublishRuntimeConfig(database_url=DB_LOCATOR, redis_url=REDIS_LOCATOR)


def test_parser_exposes_only_approved_bounded_flags() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    parser_flags: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument":
            for arg in node.args:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value.startswith("--"):
                    parser_flags.add(arg.value)

    assert parser_flags == {
        "--mode",
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-database-write",
        "--allow-redis-publish",
        "--candidate-group-id",
        "--candidate-group-suffix",
        "--bundle-id",
        "--refresh-reason",
        "--confirm",
    }


def test_unsupported_authority_flags_return_sanitized_json_and_empty_stderr(capsys) -> None:
    for flag in (
        "--allow-openai",
        "--allow-telegram",
        "--allow-github",
        "--allow-x",
        "--allow-web",
        "--consume",
        "--ack",
        "--run-forever",
        "--database-url",
        "--redis-url",
    ):
        exit_code = runner.main([flag])
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)

        assert exit_code == 1
        assert captured.err == ""
        assert parsed["status"] == "blocked"
        assert parsed["reason_code"] == "unsupported_cli_argument"
        assert parsed["database_read_attempted"] is False
        assert parsed["database_write_attempted"] is False
        assert parsed["redis_publish_attempted"] is False


def test_invalid_uuid_suffix_or_reason_blocks_before_runtime_config(capsys) -> None:
    invalid_candidate_exit = runner.main(
        [
            "--mode",
            "plan",
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--candidate-group-id",
            "not-a-uuid",
            "--refresh-reason",
            "valid_reason",
        ]
    )
    invalid_candidate = json.loads(capsys.readouterr().out)

    invalid_suffix_exit = runner.main(
        [
            "--mode",
            "plan",
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--candidate-group-suffix",
            "nothex",
            "--refresh-reason",
            "valid_reason",
        ]
    )
    invalid_suffix = json.loads(capsys.readouterr().out)

    invalid_reason_exit = runner.main(
        [
            "--mode",
            "plan",
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--candidate-group-suffix",
            "abcd1234",
            "--refresh-reason",
            "InvalidReason",
        ]
    )
    invalid_reason = json.loads(capsys.readouterr().out)

    assert invalid_candidate_exit == 1
    assert invalid_candidate["reason_code"] == "invalid_candidate_group_id"
    assert invalid_candidate["database_read_attempted"] is False
    assert invalid_suffix_exit == 1
    assert invalid_suffix["reason_code"] == "invalid_candidate_group_suffix"
    assert invalid_suffix["database_read_attempted"] is False
    assert invalid_reason_exit == 1
    assert invalid_reason["reason_code"] == "invalid_refresh_reason"
    assert invalid_reason["database_read_attempted"] is False


def test_plan_fake_run_emits_json_only(capsys) -> None:
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    repository = FakeRepository(candidate_group_id=candidate_group_id, bundle_id=bundle_id)
    publisher = FakePublisherRunner()

    exit_code = runner.main(
        [
            "--mode",
            "plan",
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--candidate-group-id",
            str(candidate_group_id),
            "--refresh-reason",
            RAW_REFRESH_REASON,
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        publisher_runner=publisher,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert captured.out.startswith("{")
    assert parsed["status"] == "planned"
    assert parsed["publisher_attempted"] is False
    assert parsed["candidate_group_suffix"] == str(candidate_group_id)[-8:]
    assert publisher.calls == []


def test_execute_fake_run_delegates_to_source_runner_and_emits_json_only(capsys) -> None:
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    event_id = uuid4()
    repository = FakeRepository(
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        existing_event=RefreshEventReference(event_id=event_id, status="pending"),
    )
    publisher = FakePublisherRunner()

    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-database-write",
            "--allow-redis-publish",
            "--candidate-group-suffix",
            str(candidate_group_id)[-8:],
            "--refresh-reason",
            RAW_REFRESH_REASON,
            "--confirm",
            CONFIRM_TOKEN,
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        publisher_runner=publisher,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert parsed["status"] == "published"
    assert parsed["publisher_attempted"] is True
    assert parsed["refresh_event_suffix"] == str(event_id)[-8:]
    assert publisher.calls[0].event_id == event_id
    assert captured.out.startswith("{")


def test_no_raw_secrets_ids_reason_or_payload_in_stdout(capsys) -> None:
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    event_id = uuid4()
    repository = FakeRepository(
        candidate_group_id=candidate_group_id,
        bundle_id=bundle_id,
        inserted_event_id=event_id,
    )

    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--allow-database-write",
            "--allow-redis-publish",
            "--candidate-group-id",
            str(candidate_group_id),
            "--refresh-reason",
            RAW_REFRESH_REASON,
            "--confirm",
            CONFIRM_TOKEN,
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
        publisher_runner=FakePublisherRunner(),
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    for raw in (
        str(candidate_group_id),
        str(bundle_id),
        str(event_id),
        RAW_REFRESH_REASON,
        RAW_DEDUPE_KEY,
        RAW_PAYLOAD,
        DB_LOCATOR,
        REDIS_LOCATOR,
    ):
        assert raw not in captured.out
    assert str(candidate_group_id)[-8:] in captured.out
    assert str(bundle_id)[-8:] in captured.out
    assert str(event_id)[-8:] in captured.out
