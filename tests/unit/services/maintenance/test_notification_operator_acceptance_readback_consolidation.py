from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.maintenance.restricted_delivery_result_maintenance_drain_proof_runner import (
    REASON_PASSED as DELIVERY_DRAIN_REASON_PASSED,
    _redis_group_lag_pending,
    run_restricted_delivery_result_maintenance_drain_proof,
)
from services.maintenance.restricted_live_notification_queue_chain_proof_runner import (
    REASON_PASSED as QUEUE_CHAIN_REASON_PASSED,
    run_restricted_live_notification_queue_chain_proof,
)
from services.notifier_telegram.main import (
    SEND_DISABLED_PROOF_REASON_CODE,
    _run_restricted_live_queued_worker_once,
    _run_send_disabled_worker_once_proof,
)
from src.services.maintenance.exact_target_source_to_analysis_materializer import (
    build_mvp_closure_packet,
    run_cli,
)
from tests.component.services.notifier_telegram._fakes import RaisingTelegramClient
from tests.unit.services.maintenance.test_exact_target_source_to_analysis_materializer import (
    FakeStageFactoryContext,
    Ledger,
    ROOT,
    packet_json,
    run as run_source_materializer,
    runtime_bundle,
)
from tests.unit.services.maintenance.test_restricted_delivery_result_maintenance_drain_proof_runner import (
    FakeMaintenanceRunner,
    FakePublisherRunner as DeliveryDrainPublisherRunner,
    FakeReadbackLoader,
    RAW_SECRET,
    TARGET_EVENT_ID,
    _config as _delivery_drain_config,
    _maintenance_result,
    _publisher_result as _delivery_drain_publisher_result,
    _readback,
)
from tests.unit.services.maintenance.test_restricted_live_notification_queue_chain_proof_runner import (
    FakePublisherRepositoryBuilder,
    FakeRedisPublisher,
    FakeRedisPublisherBuilder,
    _config as _queue_chain_config,
    _fake_session_factory_builder as _queue_chain_session_factory_builder,
    _queue_chain_repository,
    _runtime_config as _queue_chain_runtime_config,
)
from tests.unit.services.notifier_telegram.test_restricted_live_queued_worker_once_cli import (
    FakeRedis as QueuedWorkerFakeRedis,
    _live_config as _queued_worker_live_config,
)
from tests.unit.services.notifier_telegram.test_send_disabled_worker_once_proof_cli import (
    FakeRedis as SendDisabledFakeRedis,
    _fake_session_factory_builder as _send_disabled_session_factory_builder,
    _proof_config as _send_disabled_config,
    _proof_repository as _send_disabled_repository,
    _run_fake_worker_once as _run_send_disabled_fake_worker_once,
)


@pytest.mark.asyncio
async def test_notification_operator_acceptance_consolidates_closed_readbacks_without_live_authority() -> None:
    consolidated = await _notification_acceptance_consolidation()
    send_disabled = consolidated["surfaces"]["restricted_send_disabled"]
    queued_worker = consolidated["surfaces"]["restricted_queued_worker"]
    queue_chain = consolidated["surfaces"]["restricted_queue_chain"]
    delivery_drain = consolidated["surfaces"]["delivery_result_drain"]
    zero_readback = consolidated["surfaces"]["zero_preserving_readback"]

    assert send_disabled == {
        "status": "pass",
        "reason_code": "send_disabled_worker_once_proof_passed",
        "transport_reason_code": SEND_DISABLED_PROOF_REASON_CODE,
        "telegram_client_calls": 0,
        "render_count": 1,
        "delivery_record_count": 1,
        "delivery_outbox_count": 1,
        "worker_acked": True,
        "checks_failed_count": 0,
    }
    assert queued_worker == {
        "status": "noop",
        "reason_code": "no_queued_message",
        "redis_pending": 0,
        "redis_lag": 0,
        "worker_invoked": False,
        "database_session_opened": False,
    }
    assert queue_chain == {
        "status": "pass",
        "reason_code": QUEUE_CHAIN_REASON_PASSED,
        "target_status_after_publish": "published",
        "redis_xadd_count": 1,
        "redis_consume_attempted": False,
        "telegram_transport_attempted": False,
        "raw_payload_printed": False,
        "raw_ids_printed": False,
    }
    assert delivery_drain == {
        "status": "pass",
        "reason_code": DELIVERY_DRAIN_REASON_PASSED,
        "publisher_redis_xadd_count": 1,
        "worker_acked": True,
        "readback_redis_pending": 0,
        "readback_redis_lag": 0,
        "maintenance_receipt_present": True,
        "q_notification_send_consumed": False,
        "telegram_transport_attempted": False,
        "raw_payload_printed": False,
        "raw_ids_printed": False,
    }
    assert zero_readback == {
        "string_key_lag": 0,
        "string_key_pending": 0,
        "bytes_key_lag": 0,
        "bytes_key_pending": 0,
    }

    rendered = json.dumps(consolidated, ensure_ascii=False, sort_keys=True)
    for forbidden in (
        "postgresql+psycopg" + "://",
        "redis" + "://",
        "payload_json",
        "telegram_response_json",
        "TELEGRAM_BOT_TOKEN",
        "DATABASE_URL",
        "REDIS_URL",
        "Traceback",
        "pass" + "word",
        "source text",
        RAW_SECRET,
        str(TARGET_EVENT_ID),
    ):
        assert forbidden not in rendered


@pytest.mark.asyncio
async def test_mvp_closure_packet_consumes_m1_and_source_channel_proofs_without_final_claims() -> None:
    m1_readback = await _notification_acceptance_consolidation()
    source_report = await run_source_materializer(Ledger(), mode="execute")

    packet = build_mvp_closure_packet(
        source_report,
        m1_notification_ux_acceptance_closed=m1_readback["status"] == "pass",
        m1_notification_ux_readback_schema_version=str(m1_readback["schema_version"]),
        restricted_source_channel_proof=source_report.restricted_source_channel_proof,
    )

    assert source_report.restricted_source_channel_proof["status"] == "pass"
    assert packet["schema_version"] == "github_ai_catchbot_mvp_closure_packet_v1"
    assert packet["status"] == "pass"
    assert packet["reason_code"] == "mvp_code_proof_ux_packet_ready"
    assert packet["m1_notification_ux_acceptance_closed"] is True
    assert packet["m2_restricted_source_channel_proof_closed"] is True
    assert packet["mvp_closure_packet_ready"] is True
    assert packet["closed_capabilities"] == [
        "M1 notification UX acceptance packet closed",
        "M2 restricted source/channel proof closed",
        "MVP code/proof/UX packet ready",
    ]
    assert packet["open_gates"] == [
        "AUTHORITY_OPEN",
        "ROLLOUT_OPEN",
        "PRODUCTION_ROLLOUT_OPEN",
        "FUNCTION_COMPLETE_OPEN",
        "full live collector rollout open",
        "provider live authority open",
        "always-on worker/systemd rollout open",
        "final function-complete/production-complete claims open",
    ]
    assert packet["authority"] == {
        "live_telegram_read_opened": False,
        "live_telegram_send_opened": False,
        "openai_called": False,
        "github_called": False,
        "x_called": False,
        "web_called": False,
        "redis_consume_or_ack": False,
        "docker_or_systemd_called": False,
        "alembic_or_ddl_ran": False,
        "production_db_mutation": False,
    }
    assert packet["completion_claims"] == {
        "mvp_code_proof_ux_packet_ready": True,
        "final_function_complete": False,
        "production_complete": False,
        "bot_complete": False,
        "one_hundred_percent_complete": False,
    }
    rendered = json.dumps(packet, ensure_ascii=False, sort_keys=True)
    for forbidden in (
        "postgresql+psycopg" + "://",
        "redis" + "://",
        "TELEGRAM_BOT_TOKEN",
        "DATABASE_URL",
        "REDIS_URL",
        "Traceback",
        RAW_SECRET,
        str(TARGET_EVENT_ID),
    ):
        assert forbidden not in rendered


@pytest.mark.asyncio
async def test_materializer_cli_consumes_m1_readback_and_emits_operator_pass_packet(
    tmp_path: Path,
) -> None:
    m1_readback = await _notification_acceptance_consolidation()
    packet_path = tmp_path / "source-packet.json"
    readback_path = tmp_path / "m1-notification-ux-readback.json"
    packet_path.write_text(packet_json(), encoding="utf-8")
    readback_path.write_text(
        json.dumps(m1_readback, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    emitted: list[str] = []
    ledger = Ledger()
    runtime_loads: list[str] = []

    def runtime_loader(env_file: str):
        runtime_loads.append(env_file)
        return runtime_bundle()

    exit_code = await run_cli(
        [
            "--mode",
            "execute",
            "--source-packet-json",
            str(packet_path),
            "--env-file",
            "/tmp/not-read-by-test.env",
            "--confirm",
            "materialize-source-analysis",
            "--m1-notification-ux-readback-json",
            str(readback_path),
        ],
        emit_json=emitted.append,
        runtime_config_loader=runtime_loader,
        stage_factory_builder=lambda runtime_config: FakeStageFactoryContext(ledger),
        repo_root=ROOT,
    )

    payload = json.loads(emitted[0])
    closure = payload["mvp_closure_packet"]
    claims = closure["completion_claims"]
    assert exit_code == 0
    assert runtime_loads == ["/tmp/not-read-by-test.env"]
    assert payload["status"] == "pass"
    assert payload["reason_code"] == "analysis_request_materialized"
    assert payload["restricted_source_channel_proof"]["status"] == "pass"
    assert payload["restricted_source_channel_proof"]["reason_code"] == (
        "restricted_source_channel_proof_closed"
    )
    assert closure["status"] == "pass"
    assert closure["reason_code"] == "mvp_code_proof_ux_packet_ready"
    assert closure["m1_notification_ux_acceptance_closed"] is True
    assert closure["m1_notification_ux_readback_schema_version"] == (
        "notification_operator_acceptance_readback_consolidation_v1"
    )
    assert closure["m2_restricted_source_channel_proof_closed"] is True
    assert closure["mvp_closure_packet_ready"] is True
    assert "AUTHORITY_OPEN" in closure["open_gates"]
    assert "ROLLOUT_OPEN" in closure["open_gates"]
    assert "FUNCTION_COMPLETE_OPEN" in closure["open_gates"]
    assert "PRODUCTION_ROLLOUT_OPEN" in closure["open_gates"]
    assert claims["mvp_code_proof_ux_packet_ready"] is True
    assert claims["final_function_complete"] is False
    assert claims["production_complete"] is False
    assert claims["bot_complete"] is False
    assert claims["one_hundred_percent_complete"] is False

    rendered = emitted[0]
    for forbidden in (
        str(packet_path),
        str(readback_path),
        "https://t.me/SynthChannel/12345",
        "SynthChannel/12345",
        "회사에서 llm 사용 권한",
        "llm",
        "postgresql+psycopg" + "://",
        "postgresql" + "://",
        "redis" + "://",
        "TELEGRAM_BOT_TOKEN",
        "DATABASE_URL",
        "REDIS_URL",
        "OPENAI_API_KEY",
        "Traceback",
        "payload_json",
        "telegram_response_json",
        "runtime.env",
        RAW_SECRET,
        str(TARGET_EVENT_ID),
        "TARGET_EVENT_ID",
    ):
        assert forbidden not in rendered


async def _notification_acceptance_consolidation() -> dict[str, object]:
    send_disabled = await _send_disabled_acceptance()
    queued_worker = await _queued_worker_acceptance()
    queue_chain = await _queue_chain_acceptance()
    delivery_drain = await _delivery_drain_acceptance()
    zero_readback = _zero_readback_acceptance()

    return {
        "schema_version": "notification_operator_acceptance_readback_consolidation_v1",
        "status": "pass",
        "surfaces": {
            "restricted_send_disabled": send_disabled,
            "restricted_queued_worker": queued_worker,
            "restricted_queue_chain": queue_chain,
            "delivery_result_drain": delivery_drain,
            "zero_preserving_readback": zero_readback,
        },
        "authority": {
            "live_telegram_transport_attempted": False,
            "live_openai_called": False,
            "live_github_called": False,
            "live_x_called": False,
            "live_web_called": False,
            "docker_or_systemd_called": False,
            "alembic_or_ddl_ran": False,
            "runtime_values_printed": False,
            "raw_payload_printed": False,
            "raw_ids_printed": False,
        },
    }


async def _send_disabled_acceptance() -> dict[str, object]:
    repository, source_plan_id, _ = _send_disabled_repository()
    redis = SendDisabledFakeRedis()
    telegram_client = RaisingTelegramClient()
    emitted: list[str] = []

    async def worker_once_runner(config, emit_json):
        return await _run_send_disabled_fake_worker_once(
            config,
            emit_json,
            repository=repository,
            redis=redis,
            client=telegram_client,
        )

    code = await _run_send_disabled_worker_once_proof(
        _send_disabled_config(),
        source_plan_id,
        "proof-key-01",
        emit_json=emitted.append,
        session_factory_builder=_send_disabled_session_factory_builder,
        redis_client_builder=lambda redis_url: redis,
        repository_builder=lambda session: repository,
        worker_once_runner=worker_once_runner,
    )
    payload = json.loads(emitted[0])

    assert code == 0
    return {
        "status": payload["status"],
        "reason_code": payload["reason_code"],
        "transport_reason_code": repository.delivery_records[0]["transport_error_code"],
        "telegram_client_calls": telegram_client.calls,
        "render_count": len(repository.renders),
        "delivery_record_count": len(repository.delivery_records),
        "delivery_outbox_count": len(repository.delivery_outbox),
        "worker_acked": payload["worker_once"]["acked"],
        "checks_failed_count": len(payload["checks_failed"]),
    }


async def _queued_worker_acceptance() -> dict[str, object]:
    emitted: list[str] = []
    redis = QueuedWorkerFakeRedis(pending=0, lag=0)

    async def fail_if_worker_invoked(config, emit_json):
        del config, emit_json
        raise AssertionError("lag-zero queued-worker proof must not invoke worker or transport")

    code = await _run_restricted_live_queued_worker_once(
        _queued_worker_live_config(),
        emit_json=emitted.append,
        redis_client_builder=lambda redis_url: redis,
        worker_once_runner=fail_if_worker_invoked,
    )
    payload = json.loads(emitted[0])

    assert code == 0
    return {
        "status": payload["status"],
        "reason_code": payload["reason_code"],
        "redis_pending": payload["redis_precheck"]["pending"],
        "redis_lag": payload["redis_precheck"]["lag"],
        "worker_invoked": "worker_once" in payload,
        "database_session_opened": payload["authority"]["database_session_opened"],
    }


async def _queue_chain_acceptance() -> dict[str, object]:
    repository, source_plan_id, _ = _queue_chain_repository()
    redis = FakeRedisPublisher()

    report = await run_restricted_live_notification_queue_chain_proof(
        _queue_chain_config(source_plan_id),
        runtime_config_loader=_queue_chain_runtime_config,
        session_factory_builder=_queue_chain_session_factory_builder,
        proof_repository_builder=lambda session: repository,
        publisher_repository_builder=FakePublisherRepositoryBuilder(repository),
        redis_publisher_builder=FakeRedisPublisherBuilder(redis),
    )

    return {
        "status": report["status"],
        "reason_code": report["reason_code"],
        "target_status_after_publish": report["target"]["outbox_status_after_publish"],
        "redis_xadd_count": report["publisher"]["redis_xadd_count"],
        "redis_consume_attempted": report["authority"]["redis_consume_attempted"],
        "telegram_transport_attempted": report["authority"]["telegram_transport_attempted"],
        "raw_payload_printed": report["authority"]["raw_payload_printed"],
        "raw_ids_printed": report["authority"]["raw_ids_printed"],
    }


async def _delivery_drain_acceptance() -> dict[str, object]:
    report = await run_restricted_delivery_result_maintenance_drain_proof(
        _delivery_drain_config(),
        publisher_runner=DeliveryDrainPublisherRunner(_delivery_drain_publisher_result()),
        maintenance_runner=FakeMaintenanceRunner(
            preview_result=_maintenance_result(mode="preview", lag=1, pending=0),
            execute_result=_maintenance_result(mode="execute", lag=1, pending=0, acked=True, handler_called=True),
        ),
        readback_loader=FakeReadbackLoader(_readback()),
    )

    return {
        "status": report["status"],
        "reason_code": report["reason_code"],
        "publisher_redis_xadd_count": report["publisher"]["redis_xadd_count"],
        "worker_acked": report["worker_once"]["acked"],
        "readback_redis_pending": report["readback"]["redis_pending"],
        "readback_redis_lag": report["readback"]["redis_lag"],
        "maintenance_receipt_present": report["readback"]["maintenance_receipt_present"],
        "q_notification_send_consumed": report["authority"]["q_notification_send_consumed"],
        "telegram_transport_attempted": report["authority"]["telegram_transport_attempted"],
        "raw_payload_printed": report["authority"]["raw_payload_printed"],
        "raw_ids_printed": report["authority"]["raw_ids_printed"],
    }


def _zero_readback_acceptance() -> dict[str, int | None]:
    string_key_lag, string_key_pending = _redis_group_lag_pending(
        [{"name": "maintenance", "lag": 0, "pending": 0}],
        "maintenance",
    )
    bytes_key_lag, bytes_key_pending = _redis_group_lag_pending(
        [{b"name": b"maintenance", b"lag": 0, b"pending": 0}],
        "maintenance",
    )
    return {
        "string_key_lag": string_key_lag,
        "string_key_pending": string_key_pending,
        "bytes_key_lag": bytes_key_lag,
        "bytes_key_pending": bytes_key_pending,
    }
