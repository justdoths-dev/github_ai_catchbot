from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.services.analysis_router.bounded_analysis_router_runner import (
    AnalysisRequestOutboxEvent,
    BoundedAnalysisRouteRedisConsumer,
    BoundedAnalysisRouterDatabaseHandle,
    BoundedAnalysisRouterRedisHandle,
    BoundedAnalysisRouterRuntimeConfig,
)
from src.services.analysis_router.config import AnalysisRouterConfig
from src.services.analysis_router.models import BundleRouteRecord, BundleShapeStats, CandidateRouteState
from tools import bounded_analysis_router_job_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_analysis_router_job_runner.py"
DB_URL = "db_locator_omitted_sentinel"
REDIS_URL = "redis_locator_omitted_sentinel"
STREAM_ID = "1710000000000-0"
RAW_PAYLOAD = "sentinel cli raw business payload"
RAW_PROMPT = "sentinel cli prompt material"


class FakeRedisClient:
    def __init__(self, entries) -> None:
        self.entries = entries
        self.cursor = 0
        self.acked = []

    async def xlen(self, name: str) -> int:
        assert name == "q.analysis.route"
        return len(self.entries)

    async def xgroup_create(self, name: str, groupname: str, id: str = "$", mkstream: bool = False) -> None:
        assert name == "q.analysis.route"
        assert groupname == "analysis-router"
        assert id == "0"
        assert mkstream is False

    async def xreadgroup(self, groupname, consumername, streams, count=None, block=None):
        assert groupname == "analysis-router"
        assert consumername == "bounded-cli-test"
        assert streams == {"q.analysis.route": ">"}
        assert block is None
        if self.cursor >= len(self.entries):
            return []
        end = min(len(self.entries), self.cursor + (count or len(self.entries)))
        batch = self.entries[self.cursor : end]
        self.cursor = end
        return [("q.analysis.route", batch)]

    async def xack(self, name: str, groupname: str, *ids: str) -> int:
        assert name == "q.analysis.route"
        assert groupname == "analysis-router"
        self.acked.extend(ids)
        return len(ids)


class FakeRedisBuilder:
    def __init__(self, client: FakeRedisClient) -> None:
        self.client = client

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        consumer = BoundedAnalysisRouteRedisConsumer(
            self.client,
            queue_name="q.analysis.route",
            consumer_group="analysis-router",
            consumer_name="bounded-cli-test",
        )

        async def close() -> None:
            return None

        return BoundedAnalysisRouterRedisHandle(consumer=consumer, close=close)


class FakeRepository:
    def __init__(self, *, event, candidate_state, bundle, shape) -> None:
        self.event = event
        self.candidate_state = candidate_state
        self.bundle = bundle
        self.shape = shape
        self.created_judge_run_id = uuid4()
        self.outbox_calls = []

    async def fetch_analysis_request_event(self, trigger_event_id):
        return self.event if str(self.event.event_id) == str(trigger_event_id) else None

    async def load_candidate_route_state(self, candidate_group_id):
        del candidate_group_id
        return self.candidate_state

    async def load_bundle(self, bundle_id):
        del bundle_id
        return self.bundle

    async def load_bundle_shape_stats(self, bundle_id):
        del bundle_id
        return self.shape

    async def get_or_create_judge_run(self, **kwargs):
        del kwargs
        return self.created_judge_run_id, True

    async def insert_judge_call_requested_outbox(self, **kwargs) -> None:
        self.outbox_calls.append(kwargs)


class FakeDatabaseBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository
        self.close_commits = []

    async def __call__(self, runtime_config, state, logger):
        del runtime_config, logger
        state.database_session_opened = True

        async def close(commit: bool) -> None:
            self.close_commits.append(commit)

        return BoundedAnalysisRouterDatabaseHandle(repository=self.repository, close=close)


def _runtime_config() -> BoundedAnalysisRouterRuntimeConfig:
    return BoundedAnalysisRouterRuntimeConfig(
        router_config=AnalysisRouterConfig(
            app_env="test",
            database_url=DB_URL,
            redis_url=REDIS_URL,
            queue_name="q.analysis.route",
            consumer_group="analysis-router",
            consumer_name="analysis-router-test",
            batch_size=10,
            block_ms=100,
            enable_model_escalation=False,
            default_model="gpt-5.4-mini",
            escalation_model="gpt-5.4",
            default_reasoning_effort="low",
            escalation_reasoning_effort="medium",
            github_prompt_version="judge_github_primary_v1",
            x_prompt_version="judge_x_primary_v1",
            text_idea_prompt_version="judge_text_idea_primary_v1",
            judge_schema_version="judge_output_v1",
            policy_version="verdict_policy_v1",
            log_level="INFO",
        )
    )


def _thin_fields(event_id, candidate_group_id):
    return {
        "job_id": str(event_id),
        "stage_name": "analysis_route",
        "root_object_type": "candidate_group",
        "root_object_id": str(candidate_group_id),
        "idempotency_key": "private-idempotency-key",
        "pipeline_run_id": "",
        "not_before": "",
        "trigger_event_id": str(event_id),
    }


def _fake_parts():
    event_id = uuid4()
    candidate_group_id = uuid4()
    bundle_id = uuid4()
    event = AnalysisRequestOutboxEvent(
        event_id=event_id,
        event_type="analysis.requested.v1",
        aggregate_type="candidate_group",
        aggregate_id=candidate_group_id,
        payload_json={
            "candidate_group_id": str(candidate_group_id),
            "bundle_id": str(bundle_id),
            "judge_profile": "github_primary",
            "escalation_allowed": False,
            "private_payload": RAW_PAYLOAD,
            "prompt_material": RAW_PROMPT,
        },
        status="published",
        dedupe_key="private-dedupe-key",
        created_at=datetime.now(timezone.utc),
    )
    repository = FakeRepository(
        event=event,
        candidate_state=CandidateRouteState(
            candidate_group_id=str(candidate_group_id),
            current_bundle_id=str(bundle_id),
        ),
        bundle=BundleRouteRecord(
            bundle_id=str(bundle_id),
            candidate_group_id=str(candidate_group_id),
            bundle_profile_version="bundle_profile_v1",
            reroot_count=0,
            ready_for_analysis=True,
            token_budget_profile="small",
        ),
        shape=BundleShapeStats(member_count=1, supporting_count=0),
    )
    redis = FakeRedisClient([(STREAM_ID, _thin_fields(event_id, candidate_group_id))])
    return event, repository, redis


def test_main_with_no_flags_returns_json_only_fail_closed_and_empty_stderr(capsys) -> None:
    exit_code = runner.main([])
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 1
    assert captured.err == ""
    assert parsed["schema_version"] == "bounded_analysis_router_runner_v1"
    assert parsed["runner_name"] == "bounded_analysis_router_job_runner"
    assert parsed["mode"] == "analysis_route_one_shot_consume"
    assert parsed["ok"] is False
    assert parsed["status"] == "blocked"
    assert parsed["error_code"] == "operator_approval_missing"
    assert parsed["queue_name"] == "q.analysis.route"
    assert parsed["stage_name"] == "analysis_route"
    assert parsed["redis_ack_attempted"] is False
    assert parsed["database_write_attempted"] is False
    assert parsed["side_effects"]["redis_consume_called"] is False
    assert parsed["side_effects"]["db_write"] is False
    assert parsed["side_effects"]["openai_called"] is False
    assert parsed["side_effects"]["telegram_send_called"] is False
    assert parsed["side_effects"]["github_api_called"] is False
    assert parsed["side_effects"]["x_api_called"] is False
    assert parsed["side_effects"]["web_fetch_called"] is False
    assert parsed["side_effects"]["run_forever_called"] is False


def test_parser_exposes_only_bounded_approved_flags() -> None:
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
        "--trigger-event-suffix",
        "--redis-message-id",
        "--max-messages",
        "--scan-limit",
    }


def test_valid_cli_fake_run_prints_json_only_and_redacts_sensitive_values(capsys) -> None:
    event, repository, redis = _fake_parts()

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-redis-consume",
            "--allow-database-write",
            "--allow-redis-ack",
            "--trigger-event-suffix",
            str(event.event_id)[-8:],
        ],
        runtime_config_loader=_runtime_config,
        redis_builder=FakeRedisBuilder(redis),
        database_builder=FakeDatabaseBuilder(repository),
    )
    captured = capsys.readouterr()
    parsed = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert parsed["ok"] is True
    assert parsed["status"] == "routed"
    assert parsed["target_trigger_event_id_suffix"] == str(event.event_id)[-8:]
    assert parsed["target_candidate_group_suffix"] == str(event.aggregate_id)[-8:]
    assert parsed["redis_ack_status"] == "acked"
    assert parsed["judge_runs_written_count"] == 1
    assert parsed["judge_call_requested_outbox_count"] == 1
    assert redis.acked == [STREAM_ID]
    assert len(repository.outbox_calls) == 1
    assert repository.outbox_calls[0]["candidate_group_id"] == str(event.aggregate_id)
    assert repository.outbox_calls[0]["bundle_id"] == str(event.payload_json["bundle_id"])
    assert captured.out.strip().startswith("{")
    for raw in (
        str(event.event_id),
        str(event.aggregate_id),
        str(event.payload_json["bundle_id"]),
        RAW_PAYLOAD,
        RAW_PROMPT,
        DB_URL,
        REDIS_URL,
        STREAM_ID,
    ):
        assert raw not in captured.out


def test_unsupported_authority_flags_return_sanitized_json_and_empty_stderr(capsys) -> None:
    for flag in (
        "--allow-openai",
        "--allow-telegram",
        "--allow-github",
        "--allow-x",
        "--allow-web",
        "--allow-policy",
        "--allow-evidence-assembler",
        "--run-forever",
        "--consume-q-analysis-judge",
        "--database-url",
        "--redis-url",
        "--candidate-group-id",
        "--bundle-id",
    ):
        exit_code = runner.main([flag])
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)

        assert exit_code == 1
        assert captured.err == ""
        assert parsed["status"] == "blocked"
        assert parsed["error_code"] == "unsupported_cli_argument"
        assert parsed["side_effects"]["redis_consume_called"] is False
        assert parsed["database_write_attempted"] is False


def test_invalid_uuid_or_suffix_returns_sanitized_json_without_runtime_config(capsys) -> None:
    invalid_id_exit = runner.main(["--operator-approved", "--trigger-event-id", "not-a-uuid"])
    invalid_id = json.loads(capsys.readouterr().out)

    invalid_suffix_exit = runner.main(["--operator-approved", "--trigger-event-suffix", "not-a-suffix"])
    invalid_suffix = json.loads(capsys.readouterr().out)

    assert invalid_id_exit == 1
    assert invalid_id["error_code"] == "invalid_trigger_event_id"
    assert invalid_id["side_effects"]["redis_consume_called"] is False
    assert invalid_id["database_write_attempted"] is False
    assert invalid_suffix_exit == 1
    assert invalid_suffix["error_code"] == "invalid_trigger_event_suffix"


def test_tool_ast_guard_has_no_forbidden_process_network_or_worker_calls() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = set()
    imported_modules = set()
    forbidden_call_names = {"system", "popen", "call", "check_call", "check_output", "run_forever"}
    forbidden_call_attrs = forbidden_call_names | {"xreadgroup", "xread", "sleep", "consume"}

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

    assert {"subprocess", "requests", "httpx", "aiohttp", "telegram", "openai"}.isdisjoint(imported_roots)
    assert not any(".judge_openai" in module for module in imported_modules)
    assert not any(".notifier_telegram" in module for module in imported_modules)
    assert not any(".policy_engine" in module for module in imported_modules)
    assert not any(".evidence_assembler" in module for module in imported_modules)
    assert not any(".router_normalizer" in module for module in imported_modules)
    assert not any(".gh_enricher" in module for module in imported_modules)
    assert not any(".x_enricher" in module for module in imported_modules)
    assert not any(".web_enricher" in module for module in imported_modules)
    assert "run_forever(" not in source
