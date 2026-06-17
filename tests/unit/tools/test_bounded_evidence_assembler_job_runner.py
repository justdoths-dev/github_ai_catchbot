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
from src.services.evidence_assembler.models import AssemblyResult, EvidenceBundlePreview
from tools import bounded_evidence_assembler_job_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_evidence_assembler_job_runner.py"
SOURCE_PATH = ROOT / "src/services/evidence_assembler/bounded_bundle_assembler_runner.py"
DB_URL = "postgresql+psycopg://sentinel_user:sentinel_password@127.0.0.1/db"
REDIS_URL = "redis://:sentinel_redis_password@127.0.0.1:6379/0"
RAW_IDEMPOTENCY_KEY = "private-cli-bundle-idempotency-key"
RAW_CONTENT_ANCHOR = "private-cli-content-anchor"
RAW_EXCEPTION_DETAIL = "sentinel cli private bundle exception detail"
STREAM_ID = "1710000000476-0"
STREAM_ID_SUFFIX = "476-0"


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


def _event(event_id, artifact_id, candidate_group_id=None) -> TriggerEventContract:
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
        selected_candidate_group_id=candidate_group_id or uuid4(),
    )


class FakeConsumer:
    def __init__(self, selected: TargetedRedisBundleMessage, *, ack_error: BaseException | None = None) -> None:
        self.selected = selected
        self.ack_error = ack_error
        self.acked: list[str] = []

    async def find_target(self, config, state):
        state.redis_read_attempted = True
        if config.run_mode == "execute":
            state.redis_consume_attempted = True
        if not self.selected.redis_message_id.endswith(config.redis_message_id_suffix):
            return None, 1, 0
        if not self.selected.message.trigger_event_id.endswith(config.trigger_event_suffix):
            return None, 1, 0
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
            pass

        return BoundedBundleAssemblerRedisHandle(consumer=self.consumer, close=close)


class FakeDatabase:
    def __init__(
        self,
        event: TriggerEventContract,
        counters: BoundedBundleAssemblerCounters,
        *,
        reused_existing: bool = False,
        analysis_existing: bool = False,
    ) -> None:
        self.event = event
        self.counters = counters
        self.reused_existing = reused_existing
        self.analysis_existing = analysis_existing

    async def resolve_trigger_event_suffix(self, trigger_event_suffix: str, state):
        assert str(self.event.event_id).endswith(trigger_event_suffix)
        state.trigger_suffix_lookup_attempted = True
        return self.event.event_id

    async def validate_trigger_event(self, selected, config, state):
        del selected, config
        state.event_outbox_read_attempted = True
        state.database_read_attempted = True
        return self.event

    async def preview(self, trigger_event_id, selected_candidate_group_id, state):
        assert trigger_event_id == self.event.event_id
        assert selected_candidate_group_id == self.event.selected_candidate_group_id
        state.database_read_attempted = True
        return [
            EvidenceBundlePreview(
                candidate_group_id=self.event.selected_candidate_group_id,
                current_bundle_present_before=self.reused_existing,
                bundle_input_existing=self.reused_existing,
                ready_for_analysis=True,
                analysis_requested_existing=self.analysis_existing,
                analysis_requested_would_emit=not self.analysis_existing,
            )
        ]

    async def assemble(self, trigger_event_id, selected_candidate_group_id, state):
        assert trigger_event_id == self.event.event_id
        assert selected_candidate_group_id == self.event.selected_candidate_group_id
        state.database_write_attempted = True
        if not self.reused_existing:
            self.counters.bundles_written_count = 1
            self.counters.bundle_members_written_count = 1
            self.counters.current_bundle_updates_count = 1
            self.counters.analysis_requested_outbox_count = 1
        else:
            self.counters.analysis_requested_existing_count = 1 if self.analysis_existing else 0
        return [
            AssemblyResult(
                candidate_group_id=uuid4(),
                bundle_id=uuid4(),
                reused_existing_bundle=self.reused_existing,
                ready_for_analysis=True,
                emitted_analysis_requested=not self.analysis_existing,
                analysis_requested_event_id=uuid4(),
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


def _base_args(event_id) -> list[str]:
    return [
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-redis-read",
        "--queue-name",
        "q.candidate.bundle",
        "--redis-message-id-suffix",
        STREAM_ID_SUFFIX,
        "--trigger-event-suffix",
        str(event_id)[-8:],
    ]


def _preview_args(event_id) -> list[str]:
    return ["--mode", "preview", *_base_args(event_id)]


def _execute_args(event_id) -> list[str]:
    return [
        "--mode",
        "execute",
        *_base_args(event_id),
        "--allow-database-write-for-evidence-bundle-only",
        "--allow-redis-consume",
        "--allow-redis-ack",
    ]


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
    assert parsed["schema_version"] == "bounded_candidate_bundle_to_analysis_route_one_shot_v1"
    assert parsed["runner_name"] == "bounded_evidence_assembler_job_runner"
    assert parsed["mode"] == "candidate_bundle_to_analysis_route_one_shot"
    assert parsed["run_mode"] == "preview"
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["redis_read_attempted"] is False
    assert parsed["redis_consume_attempted"] is False
    assert parsed["redis_group_create_allowed"] is False
    assert parsed["redis_group_destroy_allowed"] is False
    assert parsed["redis_group_create_attempted"] is False
    assert parsed["redis_group_destroy_attempted"] is False
    assert parsed["redis_group_destroy_succeeded"] is False
    assert parsed["redis_group_cleanup_suppressed"] is False
    assert parsed["database_read_attempted"] is False
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
        "--mode",
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-database-write-for-evidence-bundle-only",
        "--allow-redis-read",
        "--allow-redis-consume",
        "--allow-redis-group-create",
        "--allow-redis-group-destroy",
        "--allow-redis-ack",
        "--queue-name",
        "--redis-message-id-suffix",
        "--trigger-event-suffix",
        "--candidate-group-suffix",
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
        "--allow-analysis-route-redis-publish",
        "--allow-analysis-router",
        "--run-forever",
        "--database-url",
        "--redis-url",
        "--runtime-env",
        "--event-id",
        "--trigger-event-id",
        "--artifact-id",
        "--candidate-group-id",
        "--snapshot-id",
        "--job-id",
        "--root-object-id",
        "--redis-message-id",
    ):
        exit_code = runner.main([flag])
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)

        assert exit_code == 1
        assert captured.err == ""
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["redis_read_attempted"] is False
        assert parsed["redis_consume_attempted"] is False
        assert parsed["redis_group_create_attempted"] is False
        assert parsed["redis_group_destroy_attempted"] is False
        assert parsed["database_write_attempted"] is False


def test_invalid_uuid_or_suffix_returns_sanitized_json_without_runtime_config(capsys) -> None:
    invalid_suffix_exit = runner.main(["--operator-approved", "--trigger-event-suffix", "not-a-suffix"])
    invalid_suffix = json.loads(capsys.readouterr().out)

    assert invalid_suffix_exit == 1
    assert invalid_suffix["error_code"] == "invalid_trigger_event_suffix"
    assert invalid_suffix["redis_read_attempted"] is False

    invalid_candidate_exit = runner.main(["--operator-approved", "--candidate-group-suffix", "not-a-suffix"])
    invalid_candidate = json.loads(capsys.readouterr().out)
    assert invalid_candidate_exit == 1
    assert invalid_candidate["error_code"] == "invalid_candidate_group_suffix"

    invalid_redis_exit = runner.main(["--operator-approved", "--redis-message-id-suffix", "not-a-suffix"])
    invalid_redis = json.loads(capsys.readouterr().out)
    assert invalid_redis_exit == 1
    assert invalid_redis["error_code"] == "invalid_redis_message_id_suffix"


def test_preview_reads_exact_target_and_produces_no_writes_or_ack(capsys) -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    candidate_group_id = uuid4()
    selected = _selected(event_id, artifact_id)
    consumer = FakeConsumer(selected)
    counters = BoundedBundleAssemblerCounters()
    database = FakeDatabase(_event(event_id, artifact_id, candidate_group_id), counters)
    database_builder = FakeDatabaseBuilder(database)

    exit_code = runner.main(
        _preview_args(event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=database_builder,
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert parsed["ok"] is True
    assert parsed["status"] == "previewed"
    assert parsed["run_mode"] == "preview"
    assert parsed["queue_name"] == "q.candidate.bundle"
    assert parsed["stage_name"] == "bundle"
    assert parsed["messages_seen"] == 1
    assert parsed["messages_matched"] == 1
    assert parsed["messages_processed_count"] == 0
    assert parsed["bundles_written_count"] == 0
    assert parsed["analysis_requested_outbox_count"] == 0
    assert parsed["bundle_input_new_count"] == 1
    assert parsed["ready_for_analysis_would_be_true"] is True
    assert parsed["analysis_requested_would_be_emitted"] is True
    assert parsed["analysis_route_publish_possible_if_gate_supported"] is True
    assert parsed["target_trigger_event_id_suffix"] == str(event_id)[-8:]
    assert parsed["target_artifact_id_suffix"] == str(artifact_id)[-8:]
    assert parsed["target_candidate_group_suffix"] == str(candidate_group_id)[-8:]
    assert parsed["redis_read_attempted"] is True
    assert parsed["redis_consume_attempted"] is False
    assert parsed["redis_group_create_allowed"] is False
    assert parsed["redis_group_destroy_allowed"] is False
    assert parsed["redis_group_create_attempted"] is False
    assert parsed["redis_group_destroy_attempted"] is False
    assert parsed["redis_group_destroy_succeeded"] is False
    assert parsed["redis_group_cleanup_suppressed"] is False
    assert parsed["database_read_attempted"] is True
    assert parsed["database_write_attempted"] is False
    assert parsed["redis_ack_attempted"] is False
    assert database_builder.close_commits == [False]
    assert consumer.acked == []
    for raw in (
        str(event_id),
        str(artifact_id),
        str(database.event.snapshot_id),
        str(candidate_group_id),
        RAW_IDEMPOTENCY_KEY,
        RAW_CONTENT_ANCHOR,
        STREAM_ID,
        DB_URL,
        REDIS_URL,
    ):
        assert raw not in captured.out


def test_execute_writes_bundle_and_acks_after_safe_completion(capsys) -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    consumer = FakeConsumer(selected)
    counters = BoundedBundleAssemblerCounters()
    database = FakeDatabase(_event(event_id, artifact_id), counters)
    database_builder = FakeDatabaseBuilder(database)

    exit_code = runner.main(
        _execute_args(event_id),
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
    assert parsed["run_mode"] == "execute"
    assert parsed["messages_processed_count"] == 1
    assert parsed["bundles_written_count"] == 1
    assert parsed["analysis_requested_outbox_count"] == 1
    assert parsed["redis_read_attempted"] is True
    assert parsed["redis_consume_attempted"] is True
    assert parsed["redis_group_create_allowed"] is False
    assert parsed["redis_group_destroy_allowed"] is False
    assert parsed["redis_group_create_attempted"] is False
    assert parsed["redis_group_destroy_attempted"] is False
    assert parsed["redis_group_destroy_succeeded"] is False
    assert parsed["redis_group_cleanup_suppressed"] is False
    assert parsed["database_write_attempted"] is True
    assert parsed["redis_ack_attempted"] is True
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


def test_legacy_group_lifecycle_flags_are_optional_and_inert(capsys) -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    consumer = FakeConsumer(selected)
    database = FakeDatabase(_event(event_id, artifact_id), BoundedBundleAssemblerCounters())
    args = [
        *_execute_args(event_id),
        "--allow-redis-group-create",
        "--allow-redis-group-destroy",
    ]

    exit_code = runner.main(
        args,
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert parsed["status"] == "assembled"
    assert parsed["redis_group_create_allowed"] is True
    assert parsed["redis_group_destroy_allowed"] is True
    assert parsed["redis_group_create_attempted"] is False
    assert parsed["redis_group_destroy_attempted"] is False
    assert parsed["database_write_attempted"] is True
    assert parsed["redis_ack_attempted"] is True


def test_execute_reuses_existing_bundle_without_duplicate_analysis_requested(capsys) -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    consumer = FakeConsumer(selected)
    database = FakeDatabase(
        _event(event_id, artifact_id),
        BoundedBundleAssemblerCounters(),
        reused_existing=True,
        analysis_existing=True,
    )

    exit_code = runner.main(
        _execute_args(event_id),
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert parsed["status"] == "assembled"
    assert parsed["bundles_written_count"] == 0
    assert parsed["existing_bundle_reused_count"] == 1
    assert parsed["analysis_requested_outbox_count"] == 0
    assert parsed["analysis_requested_existing_count"] == 1
    assert parsed["redis_ack_status"] == "acked"


def test_exact_target_mismatch_does_not_open_database_or_ack(capsys) -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    consumer = FakeConsumer(selected)
    database = FakeDatabase(_event(event_id, artifact_id), BoundedBundleAssemblerCounters())

    exit_code = runner.main(
        [
            "--mode",
            "preview",
            *_base_args(uuid4()),
        ],
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(consumer),
        database_builder=FakeDatabaseBuilder(database),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["error_code"] == "target_message_not_found"
    assert parsed["messages_seen"] == 1
    assert parsed["messages_matched"] == 0
    assert parsed["database_read_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["redis_ack_attempted"] is False


def test_cli_db_close_failure_prints_json_only_and_does_not_ack(capsys) -> None:
    event_id = uuid4()
    artifact_id = uuid4()
    selected = _selected(event_id, artifact_id)
    consumer = FakeConsumer(selected)
    database = FakeDatabase(_event(event_id, artifact_id), BoundedBundleAssemblerCounters())

    exit_code = runner.main(
        _execute_args(event_id),
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
        _execute_args(event_id),
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
    imported_roots = {module.split(".", 1)[0] for module in imported_modules}
    assert {"openai", "telegram", "requests", "httpx", "aiohttp", "subprocess"}.isdisjoint(imported_roots)
    assert "run_forever(" not in source


def test_source_guard_keeps_live_authority_and_runtime_surfaces_out() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_modules: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)

    forbidden_fragments = {
        "send_message(",
        "edit_message_text(",
        "runtime.env",
    }
    lowered = source.lower()
    assert all(fragment not in lowered for fragment in forbidden_fragments)
    assert "xadd" not in lowered
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".analysis_router" in module for module in imported_modules)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "xreadgroup(" in source
    assert "xack(" in source
    assert "class ExistingGroupRedisTargetConsumer" in source
    assert "xinfo_groups" in source
    assert ".xgroup_create(" not in source
    assert ".xgroup_destroy(" not in source
    assert "allow_redis_group_create" in source
    assert "allow_redis_group_destroy" in source
    runtime_loader_index = source.index("runtime_config = runtime_config_loader()")
    redis_builder_index = source.index("redis_handle = await")
    assert runtime_loader_index < redis_builder_index
