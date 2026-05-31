from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "ops" / "dedicated_vps_judge_openai_single_live_call_smoke.py"

FAKE_DATABASE_CREDENTIAL = "unit" + "-db" + "-credential" + "-live-judge-smoke"
FAKE_REDIS_CREDENTIAL = "unit" + "-redis" + "-credential" + "-live-judge-smoke"
FAKE_DATABASE_URL = (
    "postgresql+psycopg"
    + ":"
    + "/"
    + "/"
    + "github_ai_catchbot_app"
    + ":"
    + FAKE_DATABASE_CREDENTIAL
    + "@"
    + "127.0.0.1"
    + ":5432/"
    + "github_ai_catchbot"
)
FAKE_REDIS_URL = (
    "redis"
    + ":"
    + "/"
    + "/"
    + ":"
    + FAKE_REDIS_CREDENTIAL
    + "@"
    + "127.0.0.1"
    + ":6379/0"
)
FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_STREAM_ID = "1710000000000" + "-0"
FAKE_PROMPT_CONTEXT = "private prompt context should not be reported"
FAKE_RUNTIME_URL = "https://private.example.invalid/repo"
FAKE_DIRECT_OPENAI_KEY = "direct" + "-openai" + "-secret" + "-must-not-leak"
FAKE_OPENAI_FILE_SECRET = "file" + "-openai" + "-secret" + "-must-not-leak"


class FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows

    def first(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None


class FakeResult:
    def __init__(self, *, scalar: Any = None, rows: list[dict[str, Any]] | None = None) -> None:
        self._scalar = scalar
        self._rows = rows or []

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def scalar(self) -> Any:
        return self._scalar

    def scalar_one(self) -> Any:
        return self._scalar

    def mappings(self) -> FakeMappings:
        return FakeMappings(self._rows)

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class FakeSession:
    def __init__(
        self,
        *,
        event_rows: dict[str, dict[str, Any]],
        judge_runs: dict[str, dict[str, Any]],
        bundles: dict[str, dict[str, Any]],
        existing_output_count: int = 0,
        existing_ready_count: int = 0,
        analysis_count: int = 0,
        policy_count: int = 0,
        notification_count: int = 0,
    ) -> None:
        self.event_rows = event_rows
        self.judge_runs = judge_runs
        self.bundles = bundles
        self.existing_output_count = existing_output_count
        self.existing_ready_count = existing_ready_count
        self.analysis_count = analysis_count
        self.policy_count = policy_count
        self.notification_count = notification_count
        self.judge_outputs: list[dict[str, Any]] = []
        self.ready_outbox: list[dict[str, Any]] = []
        self.judge_run_updates: list[dict[str, Any]] = []
        self.statements: list[str] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def in_transaction(self) -> bool:
        return False

    @asynccontextmanager
    async def _begin(self):
        yield self

    def begin(self):
        return self._begin()

    async def execute(self, statement: Any, params: dict[str, Any] | None = None) -> FakeResult:
        params = params or {}
        normalized = _normalize(str(statement))
        self.statements.append(normalized)

        if normalized == _normalize(_module().SET_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult()
        if normalized == _normalize(_module().SHOW_TRANSACTION_READ_ONLY_QUERY):
            return FakeResult(scalar="on")
        if normalized == _normalize(_module().SELECT_ONE_QUERY):
            return FakeResult(scalar=1)
        if "SELECT event_id, event_type, payload_json FROM event_outbox" in normalized:
            row = self.event_rows.get(str(params["event_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT judge_run_id, bundle_id, judge_profile"):
            row = self.judge_runs.get(str(params["judge_run_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized.startswith("SELECT bundle_id, candidate_group_id"):
            row = self.bundles.get(str(params["bundle_id"]))
            return FakeResult(rows=[row] if row else [])
        if normalized == _normalize(_module().COUNT_JUDGE_OUTPUTS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.existing_output_count + len(self.judge_outputs))
        if normalized == _normalize(_module().COUNT_JUDGE_OUTPUT_READY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.existing_ready_count + len(self.ready_outbox))
        if normalized == _normalize(_module().COUNT_ANALYSES_FOR_RUN_QUERY):
            return FakeResult(scalar=self.analysis_count)
        if normalized == _normalize(_module().COUNT_POLICY_OUTBOX_FOR_RUN_QUERY):
            return FakeResult(scalar=self.policy_count)
        if normalized == _normalize(_module().COUNT_NOTIFICATION_ROWS_FOR_RUN_QUERY):
            return FakeResult(scalar=self.notification_count)
        if normalized == _normalize(_module().SELECT_JUDGE_RUN_FINISH_STATE_QUERY):
            row = self.judge_runs.get(str(params["judge_run_id"]))
            if row is None:
                return FakeResult(rows=[])
            return FakeResult(
                rows=[
                    {
                        "status": row["status"],
                        "refusal_detected": row.get("refusal_detected", False),
                    }
                ]
            )
        if normalized.startswith("UPDATE judge_runs SET status = 'running'"):
            row = self.judge_runs[str(params["judge_run_id"])]
            row["status"] = "running"
            self.judge_run_updates.append({"status": "running"})
            return FakeResult()
        if normalized.startswith("UPDATE judge_runs SET schema_retry_count"):
            return FakeResult()
        if normalized.startswith("UPDATE judge_runs SET status = :status"):
            row = self.judge_runs[str(params["judge_run_id"])]
            row["status"] = params["status"]
            row["refusal_detected"] = params["refusal_detected"]
            self.judge_run_updates.append(
                {
                    "status": params["status"],
                    "refusal_detected": params["refusal_detected"],
                    "finish_reason": params["finish_reason"],
                }
            )
            return FakeResult()
        if normalized.startswith("INSERT INTO judge_outputs"):
            judge_output_id = uuid4()
            payload_json = json.loads(params["payload_json"])
            self.judge_outputs.append(
                {
                    "judge_output_id": judge_output_id,
                    "judge_run_id": UUID(str(params["judge_run_id"])),
                    "candidate_group_id": UUID(str(params["candidate_group_id"])),
                    "payload_json": payload_json,
                    "model_proposed_verdict": params["model_proposed_verdict"],
                    "model_confidence_band": params["model_confidence_band"],
                }
            )
            return FakeResult(scalar=judge_output_id)
        if normalized.startswith("INSERT INTO event_outbox") and "judge.output.ready.v1" in normalized:
            self.ready_outbox.append(
                {
                    "event_type": "judge.output.ready.v1",
                    "judge_run_id": UUID(str(params["judge_run_id"])),
                    "payload_json": json.loads(params["payload_json"]),
                }
            )
            return FakeResult()

        raise AssertionError(f"unexpected SQL: {statement}")

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(
        self,
        *,
        exists_count: int = 1,
        entries: list[tuple[str, dict[str, str]]] | None = None,
    ) -> None:
        self.exists_count = exists_count
        self.entries = entries if entries is not None else []
        self.ping_calls = 0
        self.exists_calls: list[str] = []
        self.xrevrange_calls: list[tuple[str, int | None]] = []
        self.ack_calls: list[Any] = []
        self.closed = False

    async def ping(self) -> bool:
        self.ping_calls += 1
        return True

    async def exists(self, name: str) -> int:
        self.exists_calls.append(name)
        return self.exists_count

    async def xrevrange(self, name: str, count: int | None = None) -> list[tuple[str, dict[str, str]]]:
        self.xrevrange_calls.append((name, count))
        return self.entries[: count or len(self.entries)]

    async def xack(self, *args: Any) -> None:
        self.ack_calls.append(args)

    async def close(self) -> None:
        self.closed = True


class FakeLiveOpenAIClient:
    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    async def create_structured_response(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return self.response or {"status": "completed", "output_text": "{}"}


def _module():
    return importlib.import_module(
        "scripts.ops.dedicated_vps_judge_openai_single_live_call_smoke"
    )


def _normalize(statement: str) -> str:
    return " ".join(statement.strip().split())


def _runtime_env(key_file: Path, *, direct_key: bool = False) -> dict[str, str]:
    env = {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "REDIS_URL": FAKE_REDIS_URL,
        "APP_ENV": "prod",
        "OPENAI_API_KEY_FILE": str(key_file),
    }
    if direct_key:
        env["OPENAI_API_KEY"] = FAKE_DIRECT_OPENAI_KEY
    return env


def _valid_payload(candidate_group_id: UUID) -> dict[str, Any]:
    return {
        "judge_schema_version": "judge_output_v1",
        "candidate_group_id": str(candidate_group_id),
        "headline": "Useful repo",
        "summary_one_line_ko": "summary",
        "skeptical_take_ko": "skeptical take",
        "why_it_might_matter_ko": "why it matters",
        "comparables": [],
        "scores": {
            "novelty": 60,
            "practical_usefulness": 70,
            "evidence_strength": 65,
            "hype_penalty": 20,
            "confidence": 55,
            "code_quality": 50,
            "maintenance_signal": 45,
            "specificity": 60,
            "reproducibility_signal": 40,
        },
        "reason_codes": ["has_repo"],
        "red_flags_ko": [],
        "evidence_limitations_ko": ["limited"],
        "recommended_action_ko": "inspect",
        "freshness_note_ko": "fresh",
        "model_proposed_verdict": "later",
        "model_confidence_band": "medium",
    }


def _structured_response(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "completed",
        "output_text": json.dumps(payload),
        "usage": {
            "input_tokens": 100,
            "input_tokens_details": {"cached_tokens": 80},
            "output_tokens": 25,
            "output_tokens_details": {"reasoning_tokens": 7},
        },
    }


def _fixtures(*, judge_run_status: str = "pending", include_bundle: bool = True) -> tuple[
    UUID,
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    tuple[str, dict[str, str]],
]:
    trigger_event_id = uuid4()
    judge_run_id = uuid4()
    bundle_id = uuid4()
    candidate_group_id = uuid4()
    artifact_id = uuid4()
    payload = {
        "judge_run_id": str(judge_run_id),
        "bundle_id": str(bundle_id),
        "model": "gpt-5.4-mini",
        "reasoning_effort": "low",
        "prompt_version": "judge_prompt_v1",
        "prompt_cache_key": "judge:github_primary:judge_prompt_v1:judge_output_v1:policy_v1",
        "prompt_context": FAKE_PROMPT_CONTEXT,
    }
    event_rows = {
        str(trigger_event_id): {
            "event_id": trigger_event_id,
            "event_type": "judge.call.requested.v1",
            "payload_json": payload,
        }
    }
    judge_runs = {
        str(judge_run_id): {
            "judge_run_id": judge_run_id,
            "bundle_id": bundle_id,
            "judge_profile": "github_primary",
            "model": payload["model"],
            "reasoning_effort": payload["reasoning_effort"],
            "prompt_version": payload["prompt_version"],
            "schema_version": "judge_output_v1",
            "policy_version": "policy_v1",
            "prompt_cache_key": payload["prompt_cache_key"],
            "status": judge_run_status,
            "schema_retry_count": 0,
            "refusal_detected": False,
        }
    }
    bundles = {}
    if include_bundle:
        bundles[str(bundle_id)] = {
            "bundle_id": bundle_id,
            "candidate_group_id": candidate_group_id,
            "current_primary_artifact_id": artifact_id,
            "primary_summary": {
                "title": "repo",
                "summary": "evidence only",
                "private_url": FAKE_RUNTIME_URL,
            },
            "supporting_summaries_json": [{"kind": "repo"}],
            "discovered_links_summary_json": [{"url": FAKE_RUNTIME_URL}],
            "evidence_limitations": ["unit fixture"],
            "token_budget_profile": "small",
            "reroot_count": 0,
            "created_at": datetime.now(timezone.utc),
        }
    redis_entry = (
        FAKE_STREAM_ID,
        {
            "job_id": str(trigger_event_id),
            "stage_name": "judge",
            "root_object_type": "judge_run",
            "root_object_id": str(judge_run_id),
            "idempotency_key": "private-dedupe-key",
            "pipeline_run_id": "",
            "not_before": "",
            "trigger_event_id": str(trigger_event_id),
        },
    )
    return trigger_event_id, event_rows, judge_runs, bundles, redis_entry


def _make_key_file(tmp_path: Path, *, content: str = FAKE_OPENAI_FILE_SECRET) -> Path:
    key_file = tmp_path / "openai-key"
    key_file.write_text(content, encoding="utf-8")
    return key_file


def _forbidden_values(
    *,
    key_file: Path,
    key_secret: str,
    event_rows: dict[str, dict[str, Any]],
    judge_runs: dict[str, dict[str, Any]],
    bundles: dict[str, dict[str, Any]],
    redis_entry: tuple[str, dict[str, str]],
) -> set[str]:
    forbidden = {
        FAKE_DATABASE_URL,
        FAKE_REDIS_URL,
        FAKE_RUNTIME_PATH,
        FAKE_DATABASE_CREDENTIAL,
        FAKE_REDIS_CREDENTIAL,
        FAKE_PROMPT_CONTEXT,
        str(key_file),
        key_secret,
        FAKE_STREAM_ID,
        redis_entry[1]["idempotency_key"],
        FAKE_RUNTIME_URL,
    }
    for row in event_rows.values():
        forbidden.add(str(row["event_id"]))
        forbidden.add(str(row["payload_json"]["judge_run_id"]))
        forbidden.add(str(row["payload_json"]["bundle_id"]))
        forbidden.add(str(row["payload_json"]["prompt_context"]))
    for row in judge_runs.values():
        forbidden.add(str(row["judge_run_id"]))
        forbidden.add(str(row["bundle_id"]))
    for row in bundles.values():
        forbidden.update(str(value) for value in row.values() if isinstance(value, UUID))
        forbidden.add(str(row["primary_summary"]["private_url"]))
    return forbidden


def _run_report(
    tmp_path: Path,
    *,
    approve_live_openai: bool = False,
    approve_db_write: bool = False,
    session: FakeSession | None = None,
    redis: FakeRedis | None = None,
    live_client: FakeLiveOpenAIClient | None = None,
    judge_run_status: str = "pending",
    include_bundle: bool = True,
    existing_output_count: int = 0,
    existing_ready_count: int = 0,
    key_file_content: str = FAKE_OPENAI_FILE_SECRET,
    missing_key_file: bool = False,
    direct_key: bool = False,
) -> tuple[Any, FakeSession, FakeRedis, FakeLiveOpenAIClient, set[str]]:
    _trigger_event_id, event_rows, judge_runs, bundles, redis_entry = _fixtures(
        judge_run_status=judge_run_status,
        include_bundle=include_bundle,
    )
    key_file = tmp_path / "missing-openai-key" if missing_key_file else _make_key_file(
        tmp_path,
        content=key_file_content,
    )
    if session is None:
        session = FakeSession(
            event_rows=event_rows,
            judge_runs=judge_runs,
            bundles=bundles,
            existing_output_count=existing_output_count,
            existing_ready_count=existing_ready_count,
        )
    if redis is None:
        redis = FakeRedis(entries=[redis_entry])
    candidate_group_id = next(iter(bundles.values()))["candidate_group_id"] if bundles else uuid4()
    if live_client is None:
        live_client = FakeLiveOpenAIClient(_structured_response(_valid_payload(candidate_group_id)))
    forbidden = _forbidden_values(
        key_file=key_file,
        key_secret=key_file_content.strip(),
        event_rows=event_rows,
        judge_runs=judge_runs,
        bundles=bundles,
        redis_entry=redis_entry,
    )
    if direct_key:
        forbidden.add(FAKE_DIRECT_OPENAI_KEY)
    result = _module().generate_report(
        runtime_env_path=FAKE_RUNTIME_PATH,
        approve_live_openai=approve_live_openai,
        approve_db_write=approve_db_write,
        runtime_env_reader=lambda _path: _runtime_env(key_file, direct_key=direct_key),
        database_session_factory=lambda _url: session,
        redis_client_factory=lambda _url: redis,
        openai_client_factory=lambda _config: live_client,
        forbidden_raw_values=(FAKE_RUNTIME_PATH, str(key_file), key_file_content.strip()),
    )
    return result, session, redis, live_client, forbidden


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_file_path_execution_from_repo_root_bootstraps_src_imports_without_pythonpath(
    tmp_path: Path,
) -> None:
    missing_runtime_env = tmp_path / "missing-runtime.env"
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)

    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--runtime-env-path", str(missing_runtime_env)],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )

    combined_output = completed.stdout + completed.stderr
    assert "ModuleNotFoundError" not in combined_output
    assert "No module named 'src'" not in combined_output

    report = json.loads(completed.stdout)
    assert completed.returncode == 1
    assert report["contract_status"] == _module().STATUS_NOT_READY
    assert report["runtime_env_read"] is False
    assert report["checks_failed"] == ["runtime_env.read"]


def test_default_mode_is_read_only_preflight_and_performs_no_openai_call_or_db_write(
    tmp_path: Path,
) -> None:
    result, session, redis, live_client, _forbidden = _run_report(tmp_path)

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_PREFLIGHT_PASSED
    assert result.report["runtime_env_read"] is True
    assert result.report["database_connected"] is True
    assert result.report["redis_connected"] is True
    assert result.report["candidate_judge_message_found_bucket"] == "one"
    assert result.report["openai_key_file_configured"] is True
    assert result.report["openai_key_file_read_bucket"] == "zero"
    assert result.report["live_openai_call_attempted"] is False
    assert result.report["fake_openai_used"] is False
    assert live_client.calls == []
    assert session.judge_outputs == []
    assert session.ready_outbox == []
    assert session.committed is False
    assert session.rolled_back is True
    assert redis.ack_calls == []


def test_missing_approval_pair_blocks_approved_execution(tmp_path: Path) -> None:
    result, session, _redis, live_client, _forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_APPROVAL_MISSING
    assert "approval.required_pair" in result.report["checks_failed"]
    assert result.report["openai_key_file_read_bucket"] == "zero"
    assert live_client.calls == []
    assert session.judge_outputs == []
    assert session.committed is False


def test_direct_openai_api_key_in_runtime_env_blocks(tmp_path: Path) -> None:
    result, session, _redis, live_client, forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
        direct_key=True,
    )

    rendered = _module().render_json(result.report)
    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DIRECT_OPENAI_API_KEY_PRESENT
    assert result.report["direct_openai_api_key_present"] is True
    assert live_client.calls == []
    assert session.judge_outputs == []
    assert FAKE_DIRECT_OPENAI_KEY in forbidden
    assert FAKE_DIRECT_OPENAI_KEY not in rendered


def test_missing_key_file_blocks_before_openai_call(tmp_path: Path) -> None:
    result, session, _redis, live_client, _forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
        missing_key_file=True,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_OPENAI_SECRET_NOT_READY
    assert result.report["openai_key_file_read_bucket"] == "zero"
    assert live_client.calls == []
    assert session.judge_outputs == []


def test_empty_key_file_blocks_before_openai_call(tmp_path: Path) -> None:
    result, session, _redis, live_client, _forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
        key_file_content=" \n",
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_OPENAI_SECRET_NOT_READY
    assert result.report["openai_key_file_read_bucket"] == "zero"
    assert live_client.calls == []
    assert session.judge_outputs == []


def test_zero_redis_candidate_blocks(tmp_path: Path) -> None:
    result, _session, redis, live_client, _forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
        redis=FakeRedis(entries=[]),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NO_CANDIDATE
    assert result.report["candidate_judge_message_found_bucket"] == "zero"
    assert live_client.calls == []
    assert redis.xrevrange_calls == [("q.analysis.judge", 2)]


def test_multiple_redis_candidates_block(tmp_path: Path) -> None:
    _trigger_event_id, event_rows, judge_runs, bundles, redis_entry = _fixtures()
    second = (
        "1710000000001-0",
        {**redis_entry[1], "trigger_event_id": str(uuid4()), "job_id": str(uuid4())},
    )
    session = FakeSession(event_rows=event_rows, judge_runs=judge_runs, bundles=bundles)
    result, session, _redis, live_client, _forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
        session=session,
        redis=FakeRedis(entries=[redis_entry, second]),
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_AMBIGUOUS_CANDIDATE
    assert result.report["candidate_judge_message_found_bucket"] == "multiple"
    assert live_client.calls == []
    assert session.judge_outputs == []


def test_existing_judge_output_blocks_before_openai_call(tmp_path: Path) -> None:
    result, session, _redis, live_client, _forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
        existing_output_count=1,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DUPLICATE_OUTPUT
    assert result.report["existing_judge_output_for_run_bucket"] == "one"
    assert live_client.calls == []
    assert session.judge_outputs == []
    assert session.committed is False


def test_existing_ready_outbox_blocks_before_openai_call(tmp_path: Path) -> None:
    result, session, _redis, live_client, _forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
        existing_ready_count=1,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_DUPLICATE_READY_OUTBOX
    assert result.report["existing_judge_output_ready_outbox_for_run_bucket"] == "one"
    assert live_client.calls == []
    assert session.ready_outbox == []


def test_non_pending_judge_run_blocks(tmp_path: Path) -> None:
    result, session, _redis, live_client, _forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
        judge_run_status="succeeded",
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_NON_PENDING_RUN
    assert result.report["judge_run_pending_bucket"] == "zero"
    assert live_client.calls == []
    assert session.judge_outputs == []


def test_missing_unusable_bundle_blocks(tmp_path: Path) -> None:
    result, session, _redis, live_client, _forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
        include_bundle=False,
    )

    assert result.exit_code == 1
    assert result.report["contract_status"] == _module().STATUS_MISSING_BUNDLE
    assert result.report["bundle_ready_for_judge_bucket"] == "zero"
    assert live_client.calls == []
    assert session.judge_outputs == []


def test_approved_path_with_injected_live_client_calls_once_and_writes_expected_rows(
    tmp_path: Path,
) -> None:
    result, session, redis, live_client, _forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
    )

    assert result.exit_code == 0
    assert result.report["contract_status"] == _module().STATUS_LIVE_CALL_PASSED
    assert result.report["openai_key_file_read_bucket"] == "one"
    assert result.report["live_openai_call_attempted"] is True
    assert result.report["live_openai_call_attempted_bucket"] == "one"
    assert result.report["fake_openai_used"] is False
    assert result.report["judge_outputs_written_bucket"] == "one"
    assert result.report["judge_run_updated_bucket"] == "one"
    assert result.report["judge_output_ready_outbox_written_bucket"] == "one"
    assert len(live_client.calls) == 1
    assert len(session.judge_outputs) == 1
    assert len(session.ready_outbox) == 1
    assert session.judge_run_updates[-1]["status"] == "succeeded"
    assert session.committed is True
    assert redis.ack_calls == []


def test_report_contains_no_raw_ids_urls_runtime_env_secret_or_prompt_context(
    tmp_path: Path,
) -> None:
    result, _session, _redis, _live_client, forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
    )
    rendered = _module().render_json(result.report)

    assert result.report["raw_values_emitted"] is False
    for value in forbidden:
        assert value not in rendered


def test_script_source_has_no_forbidden_adjacent_service_imports_or_telegram_side_effects() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    forbidden = (
        "src.services.analysis_validator",
        "src.services.policy_engine",
        "src.services.notifier_telegram",
        "src.services.collector_telegram",
        "src.services.gh_enricher",
        "src.services.x_enricher",
        "src.services.web_enricher",
        "from openai",
        "import openai",
        "FakeRefusalOpenAIClient",
        ".xack(",
        "sendMessage",
        "Telegram",
    )
    for token in forbidden:
        assert token not in text


def test_redis_ack_is_not_attempted(tmp_path: Path) -> None:
    result, _session, redis, _live_client, _forbidden = _run_report(
        tmp_path,
        approve_live_openai=True,
        approve_db_write=True,
    )

    assert result.exit_code == 0
    assert result.report["redis_ack_attempted"] is False
    assert result.report["redis_ack_skipped_by_contract"] is True
    assert redis.ack_calls == []
