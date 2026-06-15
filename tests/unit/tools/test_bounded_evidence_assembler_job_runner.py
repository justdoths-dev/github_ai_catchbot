from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import uuid4

from src.services.evidence_assembler.bounded_bundle_assembler_runner import (
    BoundedBundleAssemblerCounters,
    BoundedBundleAssemblerDatabaseHandle,
    BoundedBundleAssemblerRedisHandle,
    BoundedBundleAssemblerRuntimeConfig,
    RedisBundleMessage,
    TargetedRedisBundleMessage,
    TriggerEventContract,
)
from src.services.evidence_assembler.config import EvidenceAssemblerConfig
from src.services.evidence_assembler.models import AssemblyResult
from tools import bounded_evidence_assembler_job_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_evidence_assembler_job_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://:sentinel_redis_password@127.0.0.1:6379/0"
RAW_IDEMPOTENCY_KEY = "private-cli-bundle-idempotency-key"
RAW_CONTENT_ANCHOR = "private-cli-content-anchor"
RAW_EXCEPTION_DETAIL = "sentinel cli private bundle exception detail"
STREAM_ID = "1710000000476-0"


def _runtime_config() -> BoundedBundleAssemblerRuntimeConfig:
    return BoundedBundleAssemblerRuntimeConfig(
        assembler_config=EvidenceAssemblerConfig(
            app_env="test",
            database_url=DB_URL,
            redis_url=REDIS_URL,
            queue_name="q.candidate.bundle",
            consumer_group="evidence-assembler",
            consumer_name="bounded-cli-test",
            batch_size=1,
            block_ms=100,
            bundle_profile_version="bundle_profile_v1",
            enable_text_idea=True,
            enable_reroot=True,
            log_level="INFO",
        )
    )


def _fields(event_id, artifact_id) -> dict[str, str]:
    return {
        "job_id": str(event_id),
        "stage_name": "bundle",
        "root_object_type": "artifact",
        "root_object_id": str(artifact_id),
        "idempotency_key": RAW_IDEMPOTENCY_KEY,
        "trigger_event_id": str(event_id),
    }


def _selected(event_id, artifact_id) -> TargetedRedisBundleMessage:
    fields = _fields(event_id, artifact_id)
    return TargetedRedisBundleMessage(
        redis_message_id=STREAM_ID,
        fields=fields,
        message=RedisBundleMessage.from_stream_fields(fields),
    )


def _event(event_id, artifact_id) -> TriggerEventContract:
    snapshot_id = uuid4()
    return TriggerEventContract(
        event_id=event_id,
        event_type="artifact.snapshot.updated.v1",
        status="published",
        aggregate_type="artifact",
        aggregate_id=artifact_id,
        payload_json={
            "artifact_id": str(artifact_id),
            "snapshot_id": str(snapshot_id),
            "snapshot_type": "web_article",
            "status": "low_evidence",
            "content_anchor": RAW_CONTENT_ANCHOR,
        },
        snapshot_id=snapshot_id,
        snapshot_type="web_article",
        snapshot_status="low_evidence",
        content_anchor_present=True,
        impacted_candidate_group_count=1,
    )


class FakeConsumer:
    def __init__(self, selected: TargetedRedisBundleMessage, *, ack_error: BaseException | None = None) -> None:
        self.selected = selected
        self.ack_error = ack_error
        self.acked: list[str] = []

    async def find_target(self, config, state):
        del config
        state.redis_consume_attempted = True
        state.redis_group_created = True
        return self.selected, 1, 1

    async def ack(self, message_id: str, state) -> int:
        state.redis_ack_attempted = True
        if self.ack_error is not None:
            raise self.ack_error
        self.acked.append(message_id)
        return 1


class FakeRedisBuilder:
    def __init__(self, consumer: FakeConsumer) -> None:
        self.consumer = consumer

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger

        async def close() -> None:
            state.redis_cleanup_attempted = True

        return BoundedBundleAssemblerRedisHandle(consumer=self.consumer, close=close)


class FakeDatabase:
    def __init__(self, event: TriggerEventContract, counters: BoundedBundleAssemblerCounters) -> None:
        self.event = event
        self.counters = counters

    async def resolve_trigger_event_suffix(self, trigger_event_suffix: str, state):
        del trigger_event_suffix
        state.trigger_suffix_lookup_attempted = True
        return self.event.event_id

    async def validate_trigger_event(self, selected, config, state):
        del selected, config
        state.event_outbox_read_attempted = True
        return self.event

    async def assemble(self, trigger_event_id, state):
        assert trigger_event_id == self.event.event_id
        state.database_write_attempted = True
        self.counters.bundles_written_count = 1
        self.counters.bundle_members_written_count = 1
        self.counters.current_bundle_updates_count = 1
        self.counters.analysis_requested_outbox_count = 1
        return [
            AssemblyResult(
                candidate_group_id=uuid4(),
                bundle_id=uuid4(),
                reused_existing_bundle=False,
                ready_for_analysis=True,
                emitted_analysis_requested=True,
            )
        ]


class FakeDatabaseBuilder:
    def __init__(self, database: FakeDatabase, *, close_error: BaseException | None = None) -> None:
        self.database = database
        self.close_error = close_error
        self.close_commits: list[bool] = []

    async def __call__(self, runtime_config, state, logger, fanout_limit):
        del runtime_config, logger, fanout_limit
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)
            if self.close_error is not None:
                raise self.close_error

        return BoundedBundleAssemblerDatabaseHandle(
            database=self.database,
            counters=self.database.counters,
            close=close,
        )


def test_runner_uses_source_level_module_and_config_types() -> None:
    from src.services.evidence_assembler import bounded_bundle_assembler_runner

    assert runner.BoundedBundleAssemblerConfig is bounded_bundle_assembler_runner.BoundedBundleAssemblerConfig
    assert runner.BoundedBundleAssemblerRuntimeConfig is (
        bounded_bundle_assembler_runner.BoundedBundleAssemblerRuntimeConfig
    )


def test_main_with_no_flags_returns_required_fail_closed_json_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_evidence_assembler_job_runner_v1"
    assert parsed["runner_name"] == "bounded_evidence_assembler_job_runner"
    assert parsed["mode"] == "bundle_assembly_one_shot"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["redis_consume_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["redis_ack_attempted"] is False
    assert parsed["side_effects"]["run_forever_called"] is False


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
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-redis-consume",
        "--allow-database-write",
        "--allow-redis-ack",
        "--trigger-event-id",
        "--artifact-id",
        "--redis-message-id",
        "--trigger-event-suffix",
        "--max-messages",
        "--scan-limit",
        "--candidate-fanout-limit",
    }


def test_unsupported_authority_flags_return_sanitized_json_and_empty_stderr(capsys) -> None:
    for flag in (
        "--allow-openai",
        "--allow-telegram",
        "--allow-github",
        "--allow-x",
        "--allow-web",
        "--allow-policy",
        "--allow-notifier",
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
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["redis_consume_attempted"] is False
        assert parsed["database_write_attempted"] is False


def test_invalid_uuid_or_suffix_returns_sanitized_json_without_runtime_config(capsys) -> None:
    invalid_trigger_exit = runner.main(["--operator-approved", "--trigger-event-id", "not-a-uuid"])
    invalid_trigger = json.loads(capsys.readouterr().out)

    invalid_artifact_exit = runner.main(["--operator-approved", "--artifact-id", "not-a-uuid"])
    invalid_artifact = json.loads(capsys.readouterr().out)

    invalid_suffix_exit = runner.main(["--operator-approved", "--trigger-event-suffix", "not-a-suffix"])
    invalid_suffix = json.loads(capsys.readouterr().out)

    assert invalid_trigger_exit == 1
    assert invalid_trigger["error_code"] == "invalid_trigger_event_id"
    assert invalid_trigger["redis_consume_attempted"] is False
    assert invalid_artifact_exit == 1
    assert invalid_artifact["error_code"] == "invalid_artifact_id"
    assert invalid_suffix_exit == 1
    assert invalid_suffix["error_code"] == "invalid_trigger_event_suffix"


def test_valid_cli_fake_run_prints_json_only_and_delegates_to_source(capsys) -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    consumer = FakeConsumer(selected)
    counters = BoundedBundleAssemblerCounters()
    database = FakeDatabase(_event(event_id, artifact_id), counters)
    database_builder = FakeDatabaseBuilder(database)

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-consume",
            "--allow-database-write",
            "--allow-redis-ack",
            "--trigger-event-id",
            str(event_id),
        ],
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=database_builder,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert parsed["ok"] is True
    assert parsed["status"] == "assembled"
    assert parsed["queue_name"] == "q.candidate.bundle"
    assert parsed["stage_name"] == "bundle"
    assert parsed["messages_seen"] == 1
    assert parsed["messages_matched"] == 1
    assert parsed["messages_processed_count"] == 1
    assert parsed["bundles_written_count"] == 1
    assert parsed["analysis_requested_outbox_count"] == 1
    assert parsed["target_trigger_event_id_suffix"] == str(event_id)[-8:]
    assert parsed["target_artifact_id_suffix"] == str(artifact_id)[-8:]
    assert database_builder.close_commits == [True]
    assert consumer.acked == [STREAM_ID]
    for raw in (
        str(event_id),
        str(artifact_id),
        str(database.event.snapshot_id),
        RAW_IDEMPOTENCY_KEY,
        RAW_CONTENT_ANCHOR,
        STREAM_ID,
        DB_URL,
        REDIS_URL,
    ):
        assert raw not in captured.out


def test_cli_db_close_failure_prints_json_only_and_does_not_ack(capsys) -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    consumer = FakeConsumer(selected)
    database = FakeDatabase(_event(event_id, artifact_id), BoundedBundleAssemblerCounters())

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-consume",
            "--allow-database-write",
            "--allow-redis-ack",
            "--artifact-id",
            str(artifact_id),
        ],
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database, close_error=RuntimeError(RAW_EXCEPTION_DETAIL)),
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["error_code"] == "database_write_failed"
    assert parsed["error_class"] == "RuntimeError"
    assert parsed["redis_ack_attempted"] is False
    assert consumer.acked == []
    assert RAW_EXCEPTION_DETAIL not in captured.out


def test_cli_ack_failure_prints_json_only_and_omits_raw_exception(capsys) -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    consumer = FakeConsumer(selected, ack_error=RuntimeError(RAW_EXCEPTION_DETAIL))
    database = FakeDatabase(_event(event_id, artifact_id), BoundedBundleAssemblerCounters())

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-consume",
            "--allow-database-write",
            "--allow-redis-ack",
            "--redis-message-id",
            STREAM_ID,
        ],
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["error_code"] == "redis_ack_failed"
    assert parsed["error_class"] == "RuntimeError"
    assert parsed["redis_ack_attempted"] is True
    assert parsed["redis_acked_count"] == 0
    assert RAW_EXCEPTION_DETAIL not in captured.out
    assert STREAM_ID not in captured.out


def test_tool_ast_guard_has_no_forbidden_process_network_or_broad_worker_calls() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots: set[str] = set()
    imported_modules: set[str] = set()
    forbidden_call_names = {"system", "popen", "call", "check_call", "check_output", "run_forever"}
    forbidden_call_attrs = forbidden_call_names | {"sleep", "xreadgroup", "xread", "consume", "ack"}

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                assert node.func.attr not in forbidden_call_attrs
            elif isinstance(node.func, ast.Name):
                assert node.func.id not in forbidden_call_names

    assert {"sqlalchemy", "redis", "subprocess", "requests", "httpx", "aiohttp", "telegram", "openai"}.isdisjoint(
        imported_roots
    )
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".analysis_router" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "run_forever(" not in source
