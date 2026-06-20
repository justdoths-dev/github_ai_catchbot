from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, replace
from pathlib import Path

import pytest

from services.maintenance import main as maintenance_main
from services.maintenance import worker_runtime_crash_probe
from services.maintenance.worker_runtime_crash_probe import (
    DUE_RETRY_PROMOTION_WORKER_LABEL,
    FATAL_REPORT_SCHEMA_VERSION,
    MAINTENANCE_QUEUE_WORKER_LABEL,
    REPLAY_QUEUE_WORKER_LABEL,
    WORKER_TASK_LABELS,
    WorkerRuntimeCrashProbeRequest,
    WorkerRuntimeTaskResult,
    read_worker_runtime_fatal_report,
    run_worker_runtime_crash_probe,
    write_worker_runtime_fatal_report,
    worker_task_failed_reason_code,
    worker_task_returned_reason_code,
)
from tests.component.services.maintenance._fakes import config


RAW_DATABASE_URL = "sentinel-database-secret-value"
RAW_REDIS_URL = "sentinel-redis-secret-value"
RUNTIME_ENV_PATH_SENTINEL = "sentinel-runtime-path"


class FakeEngine:
    def __init__(self, *, dispose_raises: bool = False) -> None:
        self.dispose_raises = dispose_raises
        self.dispose_called = False

    async def dispose(self) -> None:
        self.dispose_called = True
        if self.dispose_raises:
            raise RuntimeError("sentinel-dispose-secret")


class FakeSessionFactory:
    def begin(self):
        raise AssertionError("fake runtime workers must not open DB sessions in unit tests")


class FakeRedis:
    def __init__(self, *, groups: dict[str, list[str]] | None = None, close_raises: bool = False) -> None:
        self.groups = groups if groups is not None else {
            "q.maintenance": ["maintenance"],
            "q.replay": ["maintenance-replay"],
        }
        self.close_raises = close_raises
        self.xinfo_calls: list[str] = []
        self.xgroup_create_attempted = False
        self.xreadgroup_attempted = False
        self.xack_attempted = False
        self.closed = False

    async def xinfo_groups(self, name: str):
        self.xinfo_calls.append(name)
        return [{"name": group} for group in self.groups.get(name, [])]

    async def xgroup_create(self, *args, **kwargs):
        del args, kwargs
        self.xgroup_create_attempted = True
        raise AssertionError("runtime crash probe must not create Redis groups")

    async def xreadgroup(self, *args, **kwargs):
        del args, kwargs
        self.xreadgroup_attempted = True
        return []

    async def xack(self, *args, **kwargs):
        del args, kwargs
        self.xack_attempted = True

    async def aclose(self) -> None:
        self.closed = True
        if self.close_raises:
            raise RuntimeError("sentinel-redis-close-secret")


class FakeRuntimeWorker:
    def __init__(self, label: str, behavior, *, consumer=None) -> None:
        self.label = label
        self.behavior = behavior
        self.consumer = consumer
        self.cancelled = False

    async def run_forever(self):
        return await self.behavior(self)


class FakeComponents:
    def __init__(self) -> None:
        self.engine = FakeEngine()
        self.redis_client = FakeRedis()
        self.maintenance_worker = object()
        self.replay_worker = object()
        self.due_retry_worker = object()


def _config():
    return replace(config(), database_url=RAW_DATABASE_URL, redis_url=RAW_REDIS_URL)


def _request(*, max_runtime_sec: int = 1, confirm_run: bool = True, mode: str = "execute"):
    return WorkerRuntimeCrashProbeRequest(
        mode=mode,
        max_runtime_sec=max_runtime_sec,
        confirm_run=confirm_run,
    )


def _worker_factory(label: str, behavior, created: list[FakeRuntimeWorker] | None = None):
    def factory(_config, **kwargs):
        worker = FakeRuntimeWorker(label, behavior, consumer=kwargs.get("consumer"))
        if created is not None:
            created.append(worker)
        return worker

    return factory


async def _wait_forever(worker: FakeRuntimeWorker):
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        worker.cancelled = True
        raise


async def _return_unexpectedly(worker: FakeRuntimeWorker):
    del worker
    return None


async def _raise_during_cancel(worker: FakeRuntimeWorker):
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        worker.cancelled = True
        raise RuntimeError("sentinel-cancellation-secret") from None


def _raise_with_secret(secret: str):
    async def behavior(worker: FakeRuntimeWorker):
        del worker
        raise RuntimeError(secret)

    return behavior


def _assert_no_secret_leaks(output: str, *extra_values: object) -> None:
    forbidden = [
        RAW_DATABASE_URL,
        RAW_REDIS_URL,
        "sentinel-db-user",
        "sentinel-db-pass",
        "sentinel-db-host",
        "sentinel-db-name",
        "sentinel-redis-token",
        "sentinel-redis-host",
        RUNTIME_ENV_PATH_SENTINEL,
        "Traceback",
        *[str(value) for value in extra_values],
    ]
    for value in forbidden:
        if value:
            assert value not in output


def _clear_runtime_env(monkeypatch) -> None:
    for key in maintenance_main.ONE_SHOT_RUNTIME_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _patch_worker_fatal_report_path(monkeypatch, report_path: Path) -> None:
    original_write = worker_runtime_crash_probe.write_worker_runtime_fatal_report
    original_read = worker_runtime_crash_probe.read_worker_runtime_fatal_report

    def write_to_tmp(**kwargs):
        return original_write(**kwargs, report_path=report_path)

    def read_from_tmp(**kwargs):
        del kwargs
        return original_read(report_path=report_path)

    monkeypatch.setattr(maintenance_main, "write_worker_runtime_fatal_report", write_to_tmp)
    monkeypatch.setattr(maintenance_main, "read_worker_runtime_fatal_report", read_from_tmp)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_parser_accepts_worker_runtime_crash_probe_execute_shape() -> None:
    args = maintenance_main.build_parser().parse_args(
        [
            "worker-runtime-crash-probe",
            "--mode",
            "execute",
            "--confirm",
            "run",
            "--max-runtime-sec",
            "10",
            "--env-file",
            f"/tmp/{RUNTIME_ENV_PATH_SENTINEL}.env",
        ]
    )

    assert args.command == "worker-runtime-crash-probe"
    assert args.mode == "execute"
    assert args.confirm == "run"
    assert args.max_runtime_sec == 10
    assert maintenance_main._worker_runtime_crash_probe_request_error(args) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("confirm_args", [[], ["--confirm", "nope"]])
async def test_confirm_run_required_before_env_or_dependency_calls(monkeypatch, tmp_path, capsys, confirm_args) -> None:
    _clear_runtime_env(monkeypatch)
    env_file = tmp_path / f"{RUNTIME_ENV_PATH_SENTINEL}.env"

    def fail_overlay(*args, **kwargs):
        del args, kwargs
        raise AssertionError("confirmation failure must block before env-file read")

    async def fail_probe_operation(config_arg, args_arg):
        del config_arg, args_arg
        raise AssertionError("confirmation failure must block before runtime probe operation")

    monkeypatch.setattr(maintenance_main, "_resolve_one_shot_runtime_env_file_overlay", fail_overlay)
    monkeypatch.setattr(maintenance_main, "_run_worker_runtime_crash_probe_operation", fail_probe_operation)

    exit_code = await maintenance_main._run(
        [
            "worker-runtime-crash-probe",
            "--mode",
            "execute",
            "--max-runtime-sec",
            "10",
            "--env-file",
            str(env_file),
            *confirm_args,
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 2
    assert payload["schema_version"] == "maintenance_worker_runtime_crash_probe_report_v1"
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "probe_request_not_confirmed"
    assert payload["config_loaded"] is False
    assert payload["worker_dependencies_constructed"] is False
    assert payload["broad_worker_run_started"] is False
    _assert_no_secret_leaks(output, env_file)


@pytest.mark.asyncio
@pytest.mark.parametrize("max_runtime_sec", [0, 31])
async def test_max_runtime_bounds_enforced_before_env_or_dependency_calls(
    monkeypatch,
    tmp_path,
    capsys,
    max_runtime_sec: int,
) -> None:
    _clear_runtime_env(monkeypatch)
    env_file = tmp_path / f"{RUNTIME_ENV_PATH_SENTINEL}.env"

    def fail_overlay(*args, **kwargs):
        del args, kwargs
        raise AssertionError("runtime bound failure must block before env-file read")

    async def fail_probe_operation(config_arg, args_arg):
        del config_arg, args_arg
        raise AssertionError("runtime bound failure must block before runtime probe operation")

    monkeypatch.setattr(maintenance_main, "_resolve_one_shot_runtime_env_file_overlay", fail_overlay)
    monkeypatch.setattr(maintenance_main, "_run_worker_runtime_crash_probe_operation", fail_probe_operation)

    exit_code = await maintenance_main._run(
        [
            "worker-runtime-crash-probe",
            "--mode",
            "execute",
            "--confirm",
            "run",
            "--max-runtime-sec",
            str(max_runtime_sec),
            "--env-file",
            str(env_file),
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["reason_code"] == "max_runtime_not_allowed"
    assert payload["config_loaded"] is False
    _assert_no_secret_leaks(output, env_file)


@pytest.mark.asyncio
async def test_runtime_config_error_returns_probe_schema_without_raw_value_or_exception_leak(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    _clear_runtime_env(monkeypatch)
    env_file = tmp_path / f"{RUNTIME_ENV_PATH_SENTINEL}.env"
    env_file.write_text(
        "\n".join(
            [
                f"DATABASE_URL={RAW_DATABASE_URL}",
                f"REDIS_URL={RAW_REDIS_URL}",
                "MAINTENANCE_BATCH_SIZE=0",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = await maintenance_main._run(
        [
            "worker-runtime-crash-probe",
            "--mode",
            "execute",
            "--confirm",
            "run",
            "--max-runtime-sec",
            "10",
            "--env-file",
            str(env_file),
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 1
    assert payload["schema_version"] == "maintenance_worker_runtime_crash_probe_report_v1"
    assert payload["status"] == "failed"
    assert payload["reason_code"] == "runtime_config_error"
    assert payload["config_loaded"] is False
    _assert_no_secret_leaks(output, env_file, "MAINTENANCE_BATCH_SIZE must be between 1 and 500")


@pytest.mark.asyncio
async def test_setup_failure_classified_without_raw_exception_leakage() -> None:
    def fail_engine(database_url: str):
        del database_url
        raise RuntimeError("sentinel-engine-construction-secret")

    report = await run_worker_runtime_crash_probe(
        _request(),
        config=_config(),
        engine_factory=fail_engine,
        session_factory_builder=lambda engine: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: FakeRedis(),
    )
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == "worker_runtime_setup_failed"
    assert report.config_loaded is True
    assert report.db_engine_constructed is False
    assert report.redis_client_constructed is False
    assert report.worker_dependencies_constructed is False
    _assert_no_secret_leaks(output, "sentinel-engine-construction-secret")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("crashing_label", "expected_reason"),
    [
        (MAINTENANCE_QUEUE_WORKER_LABEL, "maintenance_queue_worker_failed"),
        (REPLAY_QUEUE_WORKER_LABEL, "replay_queue_worker_failed"),
        (DUE_RETRY_PROMOTION_WORKER_LABEL, "due_retry_promotion_worker_failed"),
    ],
)
async def test_task_exception_is_classified_by_worker_label(crashing_label: str, expected_reason: str) -> None:
    engine = FakeEngine()
    redis = FakeRedis()
    created: list[FakeRuntimeWorker] = []

    def behavior_for(label: str):
        if label == crashing_label:
            return _raise_with_secret(f"sentinel-{label}-secret")
        return _wait_forever

    report = await run_worker_runtime_crash_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: engine,
        session_factory_builder=lambda engine_arg: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: redis,
        maintenance_worker_factory=_worker_factory(
            MAINTENANCE_QUEUE_WORKER_LABEL,
            behavior_for(MAINTENANCE_QUEUE_WORKER_LABEL),
            created,
        ),
        replay_worker_factory=_worker_factory(
            REPLAY_QUEUE_WORKER_LABEL,
            behavior_for(REPLAY_QUEUE_WORKER_LABEL),
            created,
        ),
        due_retry_worker_factory=_worker_factory(
            DUE_RETRY_PROMOTION_WORKER_LABEL,
            behavior_for(DUE_RETRY_PROMOTION_WORKER_LABEL),
            created,
        ),
    )
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == expected_reason
    assert report.crashed_task == crashing_label
    assert report.unexpected_return_task is None
    assert report.tasks_started == list(WORKER_TASK_LABELS)
    assert report.broad_worker_run_started is True
    assert report.cancelled_remaining_tasks is True
    assert engine.dispose_called is True
    assert redis.closed is True
    for worker in created:
        if worker.label != crashing_label:
            assert worker.cancelled is True
    _assert_no_secret_leaks(output, f"sentinel-{crashing_label}-secret")


@pytest.mark.asyncio
async def test_unexpected_task_return_is_classified() -> None:
    report = await run_worker_runtime_crash_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: FakeEngine(),
        session_factory_builder=lambda engine_arg: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: FakeRedis(),
        maintenance_worker_factory=_worker_factory(MAINTENANCE_QUEUE_WORKER_LABEL, _return_unexpectedly),
        replay_worker_factory=_worker_factory(REPLAY_QUEUE_WORKER_LABEL, _wait_forever),
        due_retry_worker_factory=_worker_factory(DUE_RETRY_PROMOTION_WORKER_LABEL, _wait_forever),
    )

    assert report.status == "failed"
    assert report.reason_code == "maintenance_queue_worker_returned"
    assert report.crashed_task is None
    assert report.unexpected_return_task == MAINTENANCE_QUEUE_WORKER_LABEL
    assert report.cancelled_remaining_tasks is True


@pytest.mark.asyncio
async def test_timeout_without_crash_is_pass_and_resources_are_closed() -> None:
    engine = FakeEngine()
    redis = FakeRedis()
    created: list[FakeRuntimeWorker] = []

    report = await run_worker_runtime_crash_probe(
        _request(max_runtime_sec=1),
        config=_config(),
        engine_factory=lambda database_url: engine,
        session_factory_builder=lambda engine_arg: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: redis,
        maintenance_worker_factory=_worker_factory(MAINTENANCE_QUEUE_WORKER_LABEL, _wait_forever, created),
        replay_worker_factory=_worker_factory(REPLAY_QUEUE_WORKER_LABEL, _wait_forever, created),
        due_retry_worker_factory=_worker_factory(DUE_RETRY_PROMOTION_WORKER_LABEL, _wait_forever, created),
    )

    assert report.status == "pass"
    assert report.reason_code == "timeout_without_crash"
    assert report.timeout_reached is True
    assert report.cancelled_remaining_tasks is True
    assert report.cleanup_completed is True
    assert engine.dispose_called is True
    assert redis.closed is True
    assert {worker.label for worker in created if worker.cancelled} == set(WORKER_TASK_LABELS)


@pytest.mark.asyncio
async def test_probe_uses_no_create_group_consumers() -> None:
    redis = FakeRedis()

    async def ensure_group_then_wait(worker: FakeRuntimeWorker):
        await worker.consumer.ensure_group()
        await _wait_forever(worker)

    report = await run_worker_runtime_crash_probe(
        _request(max_runtime_sec=1),
        config=_config(),
        engine_factory=lambda database_url: FakeEngine(),
        session_factory_builder=lambda engine_arg: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: redis,
        maintenance_worker_factory=_worker_factory(MAINTENANCE_QUEUE_WORKER_LABEL, ensure_group_then_wait),
        replay_worker_factory=_worker_factory(REPLAY_QUEUE_WORKER_LABEL, ensure_group_then_wait),
        due_retry_worker_factory=_worker_factory(DUE_RETRY_PROMOTION_WORKER_LABEL, _wait_forever),
    )

    assert report.status == "pass"
    assert redis.xgroup_create_attempted is False
    assert redis.xinfo_calls == ["q.maintenance", "q.replay"]
    assert report.redis_group_create_attempted is False
    assert report.redis_consume_possible is True
    assert report.redis_ack_possible is True
    assert report.db_write_possible is True
    assert report.systemd_attempted is False
    assert report.docker_attempted is False
    assert report.external_api_attempted is False


@pytest.mark.asyncio
async def test_cleanup_failure_is_classified_without_raw_exception_leakage() -> None:
    report = await run_worker_runtime_crash_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: FakeEngine(dispose_raises=True),
        session_factory_builder=lambda engine_arg: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: FakeRedis(close_raises=True),
        maintenance_worker_factory=_worker_factory(MAINTENANCE_QUEUE_WORKER_LABEL, _return_unexpectedly),
        replay_worker_factory=_worker_factory(REPLAY_QUEUE_WORKER_LABEL, _wait_forever),
        due_retry_worker_factory=_worker_factory(DUE_RETRY_PROMOTION_WORKER_LABEL, _wait_forever),
    )
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == "cleanup_failed"
    assert report.cleanup_completed is False
    _assert_no_secret_leaks(output, "sentinel-dispose-secret", "sentinel-redis-close-secret")


@pytest.mark.asyncio
async def test_cancellation_failure_is_classified_without_raw_exception_leakage() -> None:
    report = await run_worker_runtime_crash_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: FakeEngine(),
        session_factory_builder=lambda engine_arg: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: FakeRedis(),
        maintenance_worker_factory=_worker_factory(MAINTENANCE_QUEUE_WORKER_LABEL, _return_unexpectedly),
        replay_worker_factory=_worker_factory(REPLAY_QUEUE_WORKER_LABEL, _raise_during_cancel),
        due_retry_worker_factory=_worker_factory(DUE_RETRY_PROMOTION_WORKER_LABEL, _wait_forever),
    )
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == "cancellation_failed"
    assert report.unexpected_return_task == MAINTENANCE_QUEUE_WORKER_LABEL
    assert report.cancelled_remaining_tasks is True
    assert report.cleanup_completed is True
    _assert_no_secret_leaks(output, "sentinel-cancellation-secret")


def test_redaction_flags_and_source_do_not_include_disallowed_runtime_surfaces() -> None:
    source = Path(worker_runtime_crash_probe.__file__).read_text(encoding="utf-8")

    assert "journalctl" not in source
    assert "systemctl" not in source
    assert "docker run" not in source.lower()
    assert "docker compose" not in source.lower()
    report = worker_runtime_crash_probe.build_worker_runtime_crash_probe_blocked_report(
        mode="execute",
        max_runtime_sec=10,
        reason_code="probe_request_not_confirmed",
    )
    assert report.redactions_applied["runtime_env_path_omitted"] is True
    assert report.redactions_applied["runtime_env_values_omitted"] is True
    assert report.redactions_applied["database_url_omitted"] is True
    assert report.redactions_applied["redis_url_omitted"] is True
    assert report.redactions_applied["raw_exception_body_omitted"] is True
    assert report.redactions_applied["traceback_omitted"] is True
    assert report.redactions_applied["payload_json_omitted"] is True


def test_shared_helper_maps_task_labels_to_bounded_reason_codes() -> None:
    assert worker_task_failed_reason_code(MAINTENANCE_QUEUE_WORKER_LABEL) == "maintenance_queue_worker_failed"
    assert worker_task_failed_reason_code(REPLAY_QUEUE_WORKER_LABEL) == "replay_queue_worker_failed"
    assert worker_task_failed_reason_code(DUE_RETRY_PROMOTION_WORKER_LABEL) == "due_retry_promotion_worker_failed"
    assert worker_task_returned_reason_code(MAINTENANCE_QUEUE_WORKER_LABEL) == "maintenance_queue_worker_returned"
    assert worker_task_returned_reason_code(REPLAY_QUEUE_WORKER_LABEL) == "replay_queue_worker_returned"
    assert worker_task_returned_reason_code(DUE_RETRY_PROMOTION_WORKER_LABEL) == "due_retry_promotion_worker_returned"


@pytest.mark.asyncio
async def test_run_worker_returns_zero_only_on_cancellation(monkeypatch, tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    components = FakeComponents()

    def fake_build_worker_runtime_components(*args, **kwargs):
        del args, kwargs
        return components

    async def fake_run_labeled_worker_tasks(task_specs):
        del task_specs
        raise asyncio.CancelledError()

    monkeypatch.setattr(maintenance_main, "build_worker_runtime_components", fake_build_worker_runtime_components)
    monkeypatch.setattr(maintenance_main, "worker_runtime_task_specs", lambda components_arg: [])
    monkeypatch.setattr(maintenance_main, "run_labeled_worker_tasks", fake_run_labeled_worker_tasks)
    monkeypatch.setattr(
        maintenance_main,
        "write_worker_runtime_fatal_report",
        lambda **kwargs: report_path.write_text(json.dumps(kwargs), encoding="utf-8"),
    )

    assert await maintenance_main._run_worker(config()) == 0
    assert components.engine.dispose_called is True
    assert components.redis_client.closed is True
    assert not report_path.exists()


@pytest.mark.asyncio
async def test_worker_command_config_failure_writes_redacted_pre_runtime_fatal_report(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _clear_runtime_env(monkeypatch)
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    _patch_worker_fatal_report_path(monkeypatch, report_path)
    raw_exception = (
        "Traceback DATABASE_URL REDIS_URL "
        f"{RAW_DATABASE_URL} {RAW_REDIS_URL} /tmp/{RUNTIME_ENV_PATH_SENTINEL}.env"
    )

    def fail_config_load(cls):
        del cls
        raise maintenance_main.MaintenanceConfigurationError(raw_exception)

    async def fail_worker(config_arg):
        del config_arg
        raise AssertionError("worker command config failure must block before _run_worker")

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(fail_config_load))
    monkeypatch.setattr(maintenance_main, "_run_worker", fail_worker)

    exit_code = await maintenance_main._run(["worker"])
    output = capsys.readouterr().out
    payload = _read_json(report_path)
    serialized_report = json.dumps(payload, sort_keys=True)

    assert exit_code == 1
    assert output == ""
    assert payload["schema_version"] == FATAL_REPORT_SCHEMA_VERSION
    assert payload["reason_code"] == "worker_runtime_config_error"
    assert payload["phase"] == "config_load"
    assert payload["tasks_started"] == []
    assert payload["broad_worker_run_started"] is False
    assert payload["cleanup_completed"] is True
    assert payload["redactions_applied"]["runtime_env_path_omitted"] is True
    assert payload["redactions_applied"]["runtime_env_values_omitted"] is True
    assert payload["redactions_applied"]["database_url_omitted"] is True
    assert payload["redactions_applied"]["redis_url_omitted"] is True
    assert payload["redactions_applied"]["raw_exception_body_omitted"] is True
    assert payload["redactions_applied"]["traceback_omitted"] is True
    assert payload["report_path_omitted"] is True
    _assert_no_secret_leaks(
        serialized_report,
        raw_exception,
        "DATABASE_URL",
        "REDIS_URL",
        report_path,
    )


@pytest.mark.asyncio
async def test_worker_runtime_fatal_report_cli_reads_pre_runtime_config_report(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    _patch_worker_fatal_report_path(monkeypatch, report_path)
    write_worker_runtime_fatal_report(
        reason_code="worker_runtime_config_error",
        phase="config_load",
        cleanup_completed=True,
        tasks_started=[],
        broad_worker_run_started=False,
        report_path=report_path,
    )

    def fail_config_load(*args, **kwargs):
        del args, kwargs
        raise AssertionError("fatal-report readback must not load runtime config")

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(fail_config_load))

    exit_code = await maintenance_main._run(["worker-runtime-fatal-report", "--mode", "read"])
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 0
    assert payload["status"] == "pass"
    assert payload["latest_report_reason_code"] == "worker_runtime_config_error"
    assert payload["latest_report_phase"] == "config_load"
    assert payload["latest_report_cleanup_completed"] is True
    assert payload["latest_report_tasks_started"] == []
    assert payload["latest_report_broad_worker_run_started"] is False
    _assert_no_secret_leaks(output, report_path)


@pytest.mark.asyncio
async def test_worker_command_unexpected_pre_runtime_error_writes_redacted_report(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    _patch_worker_fatal_report_path(monkeypatch, report_path)
    raw_exception = (
        "Traceback DATABASE_URL REDIS_URL "
        f"{RAW_DATABASE_URL} {RAW_REDIS_URL} /tmp/{RUNTIME_ENV_PATH_SENTINEL}.env"
    )

    def fail_config_setup(*args, **kwargs):
        del args, kwargs
        raise RuntimeError(raw_exception)

    async def fail_worker(config_arg):
        del config_arg
        raise AssertionError("unexpected pre-runtime failure must block before _run_worker")

    monkeypatch.setattr(maintenance_main, "_load_maintenance_one_shot_runtime_config", fail_config_setup)
    monkeypatch.setattr(maintenance_main, "_run_worker", fail_worker)

    exit_code = await maintenance_main._run(["worker"])
    output = capsys.readouterr().out
    payload = _read_json(report_path)
    serialized_report = json.dumps(payload, sort_keys=True)

    assert exit_code == 1
    assert output == ""
    assert payload["reason_code"] == "worker_command_pre_runtime_error"
    assert payload["phase"] == "pre_worker"
    assert payload["tasks_started"] == []
    assert payload["broad_worker_run_started"] is False
    assert payload["cleanup_completed"] is None
    _assert_no_secret_leaks(
        serialized_report,
        raw_exception,
        "DATABASE_URL",
        "REDIS_URL",
        report_path,
    )


@pytest.mark.asyncio
async def test_run_worker_returns_nonzero_on_worker_crash(monkeypatch, tmp_path: Path) -> None:
    _patch_worker_fatal_report_path(
        monkeypatch,
        tmp_path / "state/maintenance/worker-runtime-fatal-report.json",
    )
    components = FakeComponents()

    def fake_build_worker_runtime_components(*args, **kwargs):
        del args, kwargs
        return components

    async def fake_run_labeled_worker_tasks(task_specs):
        del task_specs
        return WorkerRuntimeTaskResult(
            status="failed",
            reason_code="maintenance_queue_worker_failed",
            elapsed_ms=1,
            tasks_started=list(WORKER_TASK_LABELS),
            crashed_task=MAINTENANCE_QUEUE_WORKER_LABEL,
            unexpected_return_task=None,
            timeout_reached=False,
            cancelled_remaining_tasks=True,
        )

    monkeypatch.setattr(maintenance_main, "build_worker_runtime_components", fake_build_worker_runtime_components)
    monkeypatch.setattr(maintenance_main, "worker_runtime_task_specs", lambda components_arg: [])
    monkeypatch.setattr(maintenance_main, "run_labeled_worker_tasks", fake_run_labeled_worker_tasks)

    assert await maintenance_main._run_worker(config()) == 1
    assert components.engine.dispose_called is True
    assert components.redis_client.closed is True


@pytest.mark.asyncio
async def test_run_worker_setup_failure_writes_redacted_fatal_report(monkeypatch, tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    _patch_worker_fatal_report_path(monkeypatch, report_path)

    def fake_build_worker_runtime_components(*args, **kwargs):
        del args, kwargs
        raise worker_runtime_crash_probe.WorkerRuntimeSetupError(engine=FakeEngine(), redis_client=FakeRedis())

    monkeypatch.setattr(maintenance_main, "build_worker_runtime_components", fake_build_worker_runtime_components)

    assert await maintenance_main._run_worker(_config()) == 1
    payload = _read_json(report_path)
    output = json.dumps(payload, sort_keys=True)

    assert payload["schema_version"] == FATAL_REPORT_SCHEMA_VERSION
    assert payload["reason_code"] == "worker_runtime_setup_failed"
    assert payload["tasks_started"] == []
    assert payload["broad_worker_run_started"] is False
    assert payload["cleanup_completed"] is True
    assert payload["report_path_omitted"] is True
    _assert_no_secret_leaks(output, report_path)


@pytest.mark.asyncio
async def test_run_worker_task_crash_writes_redacted_fatal_report_with_crashed_task(
    monkeypatch,
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    _patch_worker_fatal_report_path(monkeypatch, report_path)
    components = FakeComponents()

    def fake_build_worker_runtime_components(*args, **kwargs):
        del args, kwargs
        return components

    async def fake_run_labeled_worker_tasks(task_specs):
        del task_specs
        return WorkerRuntimeTaskResult(
            status="failed",
            reason_code="maintenance_queue_worker_failed",
            elapsed_ms=1,
            tasks_started=list(WORKER_TASK_LABELS),
            crashed_task=MAINTENANCE_QUEUE_WORKER_LABEL,
            unexpected_return_task=None,
            timeout_reached=False,
            cancelled_remaining_tasks=True,
        )

    monkeypatch.setattr(maintenance_main, "build_worker_runtime_components", fake_build_worker_runtime_components)
    monkeypatch.setattr(maintenance_main, "worker_runtime_task_specs", lambda components_arg: [])
    monkeypatch.setattr(maintenance_main, "run_labeled_worker_tasks", fake_run_labeled_worker_tasks)

    assert await maintenance_main._run_worker(_config()) == 1
    payload = _read_json(report_path)
    output = json.dumps(payload, sort_keys=True)

    assert payload["reason_code"] == "maintenance_queue_worker_failed"
    assert payload["crashed_task"] == MAINTENANCE_QUEUE_WORKER_LABEL
    assert payload["unexpected_return_task"] is None
    assert payload["tasks_started"] == list(WORKER_TASK_LABELS)
    assert payload["broad_worker_run_started"] is True
    assert payload["cleanup_completed"] is True
    _assert_no_secret_leaks(output)


@pytest.mark.asyncio
async def test_run_worker_unexpected_task_return_writes_redacted_fatal_report(
    monkeypatch,
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    _patch_worker_fatal_report_path(monkeypatch, report_path)
    components = FakeComponents()

    def fake_build_worker_runtime_components(*args, **kwargs):
        del args, kwargs
        return components

    async def fake_run_labeled_worker_tasks(task_specs):
        del task_specs
        return WorkerRuntimeTaskResult(
            status="failed",
            reason_code="replay_queue_worker_returned",
            elapsed_ms=1,
            tasks_started=list(WORKER_TASK_LABELS),
            crashed_task=None,
            unexpected_return_task=REPLAY_QUEUE_WORKER_LABEL,
            timeout_reached=False,
            cancelled_remaining_tasks=True,
        )

    monkeypatch.setattr(maintenance_main, "build_worker_runtime_components", fake_build_worker_runtime_components)
    monkeypatch.setattr(maintenance_main, "worker_runtime_task_specs", lambda components_arg: [])
    monkeypatch.setattr(maintenance_main, "run_labeled_worker_tasks", fake_run_labeled_worker_tasks)

    assert await maintenance_main._run_worker(_config()) == 1
    payload = _read_json(report_path)

    assert payload["reason_code"] == "replay_queue_worker_returned"
    assert payload["crashed_task"] is None
    assert payload["unexpected_return_task"] == REPLAY_QUEUE_WORKER_LABEL
    assert payload["cleanup_completed"] is True


@pytest.mark.asyncio
async def test_run_worker_cleanup_failure_overwrites_fatal_report_with_cleanup_false(
    monkeypatch,
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    _patch_worker_fatal_report_path(monkeypatch, report_path)
    components = FakeComponents()
    components.engine = FakeEngine(dispose_raises=True)
    components.redis_client = FakeRedis(close_raises=True)

    def fake_build_worker_runtime_components(*args, **kwargs):
        del args, kwargs
        return components

    async def fake_run_labeled_worker_tasks(task_specs):
        del task_specs
        return WorkerRuntimeTaskResult(
            status="failed",
            reason_code="due_retry_promotion_worker_failed",
            elapsed_ms=1,
            tasks_started=list(WORKER_TASK_LABELS),
            crashed_task=DUE_RETRY_PROMOTION_WORKER_LABEL,
            unexpected_return_task=None,
            timeout_reached=False,
            cancelled_remaining_tasks=True,
        )

    monkeypatch.setattr(maintenance_main, "build_worker_runtime_components", fake_build_worker_runtime_components)
    monkeypatch.setattr(maintenance_main, "worker_runtime_task_specs", lambda components_arg: [])
    monkeypatch.setattr(maintenance_main, "run_labeled_worker_tasks", fake_run_labeled_worker_tasks)

    assert await maintenance_main._run_worker(_config()) == 1
    payload = _read_json(report_path)
    output = json.dumps(payload, sort_keys=True)

    assert payload["reason_code"] == "cleanup_failed"
    assert payload["crashed_task"] == DUE_RETRY_PROMOTION_WORKER_LABEL
    assert payload["cleanup_completed"] is False
    _assert_no_secret_leaks(output, "sentinel-dispose-secret", "sentinel-redis-close-secret")


def test_worker_runtime_fatal_report_readback_blocks_when_missing(tmp_path: Path) -> None:
    report = read_worker_runtime_fatal_report(report_path=tmp_path / "state/maintenance/missing.json")
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "blocked"
    assert report.reason_code == "fatal_report_missing"
    assert report.report_present is False
    assert report.raw_report_path_omitted is True
    _assert_no_secret_leaks(output, tmp_path)


def test_worker_runtime_fatal_report_readback_returns_pass_with_redacted_fields(tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    write_worker_runtime_fatal_report(
        reason_code="maintenance_queue_worker_failed",
        crashed_task=MAINTENANCE_QUEUE_WORKER_LABEL,
        cleanup_completed=True,
        tasks_started=list(WORKER_TASK_LABELS),
        broad_worker_run_started=True,
        report_path=report_path,
    )

    report = read_worker_runtime_fatal_report(report_path=report_path)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "pass"
    assert report.reason_code is None
    assert report.report_present is True
    assert report.report_schema_version == FATAL_REPORT_SCHEMA_VERSION
    assert report.latest_report_reason_code == "maintenance_queue_worker_failed"
    assert report.latest_report_crashed_task == MAINTENANCE_QUEUE_WORKER_LABEL
    assert report.latest_report_unexpected_return_task is None
    assert report.latest_report_cleanup_completed is True
    assert report.latest_report_tasks_started == list(WORKER_TASK_LABELS)
    assert report.raw_report_path_omitted is True
    assert report.raw_exception_body_omitted is True
    assert report.traceback_omitted is True
    assert report.database_url_omitted is True
    assert report.redis_url_omitted is True
    assert report.runtime_env_values_omitted is True
    _assert_no_secret_leaks(output, report_path)


def test_worker_runtime_fatal_report_readback_sanitizes_corrupt_secret_values(tmp_path: Path) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "schema_version": FATAL_REPORT_SCHEMA_VERSION,
                "reason_code": "sentinel-secret-reason",
                "phase": "sentinel-secret-phase",
                "crashed_task": "Traceback sentinel-secret",
                "unexpected_return_task": RAW_DATABASE_URL,
                "cleanup_completed": True,
                "tasks_started": [RAW_REDIS_URL, MAINTENANCE_QUEUE_WORKER_LABEL],
                "broad_worker_run_started": True,
                "created_at_utc": "2026-06-20T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    report = read_worker_runtime_fatal_report(report_path=report_path)
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "blocked"
    assert report.reason_code == "fatal_report_unknown_reason_code"
    assert report.latest_report_reason_code == "probe_runtime_error"
    assert report.latest_report_phase == "runtime"
    assert report.latest_report_crashed_task is None
    assert report.latest_report_unexpected_return_task is None
    assert report.latest_report_tasks_started == [MAINTENANCE_QUEUE_WORKER_LABEL]
    _assert_no_secret_leaks(output, "sentinel-secret-reason", "sentinel-secret-phase", "Traceback sentinel-secret")


@pytest.mark.asyncio
async def test_worker_runtime_fatal_report_cli_readback_does_not_load_config_or_env(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    report_path = tmp_path / "state/maintenance/worker-runtime-fatal-report.json"
    _patch_worker_fatal_report_path(monkeypatch, report_path)
    write_worker_runtime_fatal_report(
        reason_code="maintenance_queue_worker_returned",
        unexpected_return_task=MAINTENANCE_QUEUE_WORKER_LABEL,
        cleanup_completed=True,
        tasks_started=[MAINTENANCE_QUEUE_WORKER_LABEL],
        broad_worker_run_started=True,
        report_path=report_path,
    )

    def fail_config_load(*args, **kwargs):
        del args, kwargs
        raise AssertionError("fatal-report readback must not load runtime config")

    monkeypatch.setattr(maintenance_main.MaintenanceConfig, "from_env", classmethod(fail_config_load))

    exit_code = await maintenance_main._run(["worker-runtime-fatal-report", "--mode", "read"])
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert exit_code == 0
    assert payload["status"] == "pass"
    assert payload["latest_report_unexpected_return_task"] == MAINTENANCE_QUEUE_WORKER_LABEL
    _assert_no_secret_leaks(output, report_path)
