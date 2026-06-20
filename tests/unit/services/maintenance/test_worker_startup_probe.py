from __future__ import annotations

import json
from dataclasses import asdict, replace
from pathlib import Path

import pytest

from services.maintenance import worker_startup_probe
from services.maintenance.worker import DueRetryPromotionWorker, MaintenanceQueueWorker, ReplayQueueWorker
from services.maintenance.worker_startup_probe import (
    WorkerStartupProbeRequest,
    run_worker_startup_probe,
    worker_startup_probe_request_error,
)
from tests.component.services.maintenance._fakes import config


RAW_DATABASE_URL = "postgresql+psycopg://sentinel-db-user:sentinel-db-pass@sentinel-db-host/sentinel-db-name"
RAW_REDIS_URL = "redis://:sentinel-redis-token@sentinel-redis-host:6379/0"


class FakeConnection:
    def __init__(self, engine: "FakeEngine") -> None:
        self._engine = engine

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, statement) -> None:
        statement_text = str(statement)
        self._engine.executed_sql.append(statement_text)
        if statement_text.strip().upper() != "SELECT 1":
            self._engine.db_write_attempted = True
            raise AssertionError("probe attempted non-read-only SQL")


class FakeEngine:
    def __init__(self, *, connect_raises: bool = False) -> None:
        self.connect_raises = connect_raises
        self.executed_sql: list[str] = []
        self.db_write_attempted = False
        self.dispose_called = False

    def connect(self):
        if self.connect_raises:
            raise RuntimeError("sentinel-db-connectivity-secret")
        return FakeConnection(self)

    async def dispose(self) -> None:
        self.dispose_called = True


class FakeSessionFactory:
    def begin(self):
        raise AssertionError("startup probe must not open DB write sessions")


class FakeRedis:
    def __init__(
        self,
        *,
        groups: dict[str, list[str]] | None = None,
        ping_raises: bool = False,
    ) -> None:
        self.groups = groups if groups is not None else {
            "q.maintenance": ["maintenance"],
            "q.replay": ["maintenance-replay"],
        }
        self.ping_raises = ping_raises
        self.ping_called = False
        self.xinfo_calls: list[str] = []
        self.xreadgroup_attempted = False
        self.xack_attempted = False
        self.xgroup_create_attempted = False
        self.xadd_attempted = False
        self.xdel_attempted = False
        self.closed = False

    async def ping(self) -> bool:
        self.ping_called = True
        if self.ping_raises:
            raise RuntimeError("sentinel-redis-connectivity-secret")
        return True

    async def xinfo_groups(self, name: str):
        self.xinfo_calls.append(name)
        return [{"name": group} for group in self.groups.get(name, [])]

    async def xreadgroup(self, *args, **kwargs):
        self.xreadgroup_attempted = True
        raise AssertionError("startup probe must not consume Redis messages")

    async def xack(self, *args, **kwargs):
        self.xack_attempted = True
        raise AssertionError("startup probe must not ACK Redis messages")

    async def xgroup_create(self, *args, **kwargs):
        self.xgroup_create_attempted = True
        raise AssertionError("startup probe must not create Redis groups")

    async def xadd(self, *args, **kwargs):
        self.xadd_attempted = True
        raise AssertionError("startup probe must not write Redis")

    async def xdel(self, *args, **kwargs):
        self.xdel_attempted = True
        raise AssertionError("startup probe must not delete Redis entries")

    async def aclose(self) -> None:
        self.closed = True


def _request(*, confirm_run: bool = True, mode: str = "execute") -> WorkerStartupProbeRequest:
    return WorkerStartupProbeRequest(mode=mode, confirm_run=confirm_run)


def _config():
    return replace(config(), database_url=RAW_DATABASE_URL, redis_url=RAW_REDIS_URL)


@pytest.mark.asyncio
async def test_worker_startup_probe_passes_with_fake_read_only_dependencies(monkeypatch) -> None:
    engine = FakeEngine()
    redis = FakeRedis()

    async def fail_run_forever(self):
        raise AssertionError("startup probe must not call run_forever")

    async def fail_run_once(self):
        raise AssertionError("startup probe must not call run_once")

    monkeypatch.setattr(MaintenanceQueueWorker, "run_forever", fail_run_forever)
    monkeypatch.setattr(ReplayQueueWorker, "run_forever", fail_run_forever)
    monkeypatch.setattr(DueRetryPromotionWorker, "run_forever", fail_run_forever)
    monkeypatch.setattr(MaintenanceQueueWorker, "run_once", fail_run_once)
    monkeypatch.setattr(ReplayQueueWorker, "run_once", fail_run_once)
    monkeypatch.setattr(DueRetryPromotionWorker, "run_once", fail_run_once)

    report = await run_worker_startup_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: engine,
        session_factory_builder=lambda constructed_engine: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: redis,
    )

    assert report.schema_version == "maintenance_worker_startup_probe_report_v1"
    assert report.status == "pass"
    assert report.reason_code is None
    assert report.config_loaded is True
    assert report.db_url_present_redacted is True
    assert report.redis_url_present_redacted is True
    assert report.db_engine_constructed is True
    assert report.db_connectivity_checked is True
    assert report.db_connectivity_ok is True
    assert report.redis_client_constructed is True
    assert report.redis_connectivity_checked is True
    assert report.redis_connectivity_ok is True
    assert report.maintenance_queue_name_present is True
    assert report.maintenance_consumer_group_present is True
    assert report.maintenance_consumer_name_present is True
    assert report.replay_queue_name_present is True
    assert report.replay_consumer_group_present is True
    assert report.replay_consumer_name_present is True
    assert report.maintenance_group_checked is True
    assert report.maintenance_group_exists is True
    assert report.replay_group_checked is True
    assert report.replay_group_exists is True
    assert report.worker_dependencies_constructed is True
    assert report.broad_worker_run_started is False
    assert report.redis_consume_attempted is False
    assert report.redis_ack_attempted is False
    assert report.redis_group_create_attempted is False
    assert report.redis_write_attempted is False
    assert report.db_write_attempted is False
    assert report.systemd_attempted is False
    assert report.docker_attempted is False
    assert report.external_api_attempted is False
    assert report.redactions_applied == {
        "runtime_env_path_omitted": True,
        "runtime_env_values_omitted": True,
        "secret_values_omitted": True,
        "database_url_omitted": True,
        "redis_url_omitted": True,
        "raw_exception_body_omitted": True,
        "redis_message_id_omitted": True,
        "payload_json_omitted": True,
    }
    assert engine.executed_sql == ["SELECT 1"]
    assert engine.db_write_attempted is False
    assert engine.dispose_called is True
    assert redis.ping_called is True
    assert redis.xinfo_calls == ["q.maintenance", "q.replay"]
    assert redis.xreadgroup_attempted is False
    assert redis.xack_attempted is False
    assert redis.xgroup_create_attempted is False
    assert redis.xadd_attempted is False
    assert redis.xdel_attempted is False
    assert redis.closed is True


def test_worker_startup_probe_request_requires_execute_mode_and_confirm_run() -> None:
    assert worker_startup_probe_request_error(_request()) is None
    assert worker_startup_probe_request_error(_request(confirm_run=False)) == "probe_request_not_confirmed"
    assert worker_startup_probe_request_error(_request(mode="plan")) == "mode_not_allowed"


@pytest.mark.asyncio
async def test_db_engine_construction_failure_is_classified_without_raw_exception_leakage() -> None:
    def fail_engine(database_url: str):
        raise RuntimeError("sentinel-db-engine-secret")

    report = await run_worker_startup_probe(
        _request(),
        config=_config(),
        engine_factory=fail_engine,
        session_factory_builder=lambda constructed_engine: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: FakeRedis(),
    )
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == "db_engine_construction_failed"
    assert report.db_engine_constructed is False
    assert "sentinel-db-engine-secret" not in output
    assert RAW_DATABASE_URL not in output


@pytest.mark.asyncio
async def test_db_connectivity_failure_is_classified() -> None:
    report = await run_worker_startup_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: FakeEngine(connect_raises=True),
        session_factory_builder=lambda constructed_engine: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: FakeRedis(),
    )

    assert report.status == "failed"
    assert report.reason_code == "db_connectivity_failed"
    assert report.db_engine_constructed is True
    assert report.db_connectivity_checked is True
    assert report.db_connectivity_ok is False
    assert report.redis_client_constructed is False


@pytest.mark.asyncio
async def test_redis_client_construction_failure_is_classified_without_raw_exception_leakage() -> None:
    def fail_redis(redis_url: str):
        raise RuntimeError("sentinel-redis-client-secret")

    report = await run_worker_startup_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: FakeEngine(),
        session_factory_builder=lambda constructed_engine: FakeSessionFactory(),
        redis_client_factory=fail_redis,
    )
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == "redis_client_construction_failed"
    assert report.redis_client_constructed is False
    assert "sentinel-redis-client-secret" not in output
    assert RAW_REDIS_URL not in output


@pytest.mark.asyncio
async def test_redis_connectivity_failure_is_classified() -> None:
    report = await run_worker_startup_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: FakeEngine(),
        session_factory_builder=lambda constructed_engine: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: FakeRedis(ping_raises=True),
    )

    assert report.status == "failed"
    assert report.reason_code == "redis_connectivity_failed"
    assert report.redis_client_constructed is True
    assert report.redis_connectivity_checked is True
    assert report.redis_connectivity_ok is False
    assert report.maintenance_group_checked is False


@pytest.mark.asyncio
async def test_missing_maintenance_group_blocks_without_replay_check() -> None:
    redis = FakeRedis(groups={"q.maintenance": [], "q.replay": ["maintenance-replay"]})

    report = await run_worker_startup_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: FakeEngine(),
        session_factory_builder=lambda constructed_engine: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: redis,
    )

    assert report.status == "blocked"
    assert report.reason_code == "maintenance_group_missing"
    assert report.maintenance_group_checked is True
    assert report.maintenance_group_exists is False
    assert report.replay_group_checked is False
    assert redis.xgroup_create_attempted is False
    assert redis.xreadgroup_attempted is False
    assert redis.xack_attempted is False


@pytest.mark.asyncio
async def test_missing_replay_group_blocks_after_maintenance_group_check() -> None:
    redis = FakeRedis(groups={"q.maintenance": ["maintenance"], "q.replay": []})

    report = await run_worker_startup_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: FakeEngine(),
        session_factory_builder=lambda constructed_engine: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: redis,
    )

    assert report.status == "blocked"
    assert report.reason_code == "replay_group_missing"
    assert report.maintenance_group_checked is True
    assert report.maintenance_group_exists is True
    assert report.replay_group_checked is True
    assert report.replay_group_exists is False
    assert redis.xgroup_create_attempted is False
    assert redis.xreadgroup_attempted is False
    assert redis.xack_attempted is False


@pytest.mark.asyncio
async def test_worker_dependency_construction_failure_is_classified() -> None:
    def fail_worker(*args, **kwargs):
        raise RuntimeError("sentinel-worker-dependency-secret")

    report = await run_worker_startup_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: FakeEngine(),
        session_factory_builder=lambda constructed_engine: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: FakeRedis(),
        maintenance_worker_factory=fail_worker,
    )
    output = json.dumps(asdict(report), sort_keys=True)

    assert report.status == "failed"
    assert report.reason_code == "worker_dependency_construction_failed"
    assert report.worker_dependencies_constructed is False
    assert "sentinel-worker-dependency-secret" not in output


@pytest.mark.asyncio
async def test_probe_report_keeps_systemd_docker_external_api_and_journal_boundaries_false() -> None:
    report = await run_worker_startup_probe(
        _request(),
        config=_config(),
        engine_factory=lambda database_url: FakeEngine(),
        session_factory_builder=lambda constructed_engine: FakeSessionFactory(),
        redis_client_factory=lambda redis_url: FakeRedis(),
    )

    assert report.systemd_attempted is False
    assert report.docker_attempted is False
    assert report.external_api_attempted is False
    assert report.broad_worker_run_started is False


def test_probe_source_does_not_call_journalctl_or_systemctl() -> None:
    source = Path(worker_startup_probe.__file__).read_text(encoding="utf-8")

    assert "journalctl" not in source
    assert "systemctl" not in source
