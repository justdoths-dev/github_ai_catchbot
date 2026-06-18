from __future__ import annotations

import json

from tests.unit.services.notifier_telegram.test_bounded_notification_send_dry_run_runner import (
    FakeRuntime,
    FakeRuntimeBuilder,
    _context,
    _intent,
    _runtime_config_loader,
)
from tools import bounded_notification_send_dry_run_runner as runner


def _argv(intent) -> list[str]:
    return [
        "--mode",
        "execute",
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-redis-read",
        "--allow-database-read",
        "--allow-redis-consume",
        "--allow-database-write",
        "--allow-redis-ack",
        "--allow-render-write",
        "--allow-delivery-record-write",
        "--allow-delivery-result-outbox-write",
        "--allow-maintenance-outbox-publish",
        "--allow-maintenance-redis-publish",
        "--trigger-event-suffix",
        str(intent.trigger_event_id)[-8:],
        "--notification-plan-id-suffix",
        str(intent.notification_plan_id)[-8:],
        "--analysis-id-suffix",
        str(intent.analysis_id)[-8:],
        "--redis-message-suffix",
        "0000000000000-0"[-8:],
    ]


def test_cli_reports_durable_readback_before_ack_without_raw_ids_or_text(capsys) -> None:
    intent = _intent()
    runtime = FakeRuntime(context=_context(intent))

    exit_code = runner.main(
        _argv(intent),
        runtime_config_loader=_runtime_config_loader,
        runtime_builder=FakeRuntimeBuilder(runtime),
    )
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    encoded = json.dumps(report, sort_keys=True)

    assert exit_code == 0
    assert captured.err == ""
    assert report["durable_readback"]["ack_safe"] is True
    assert report["durable_readback"]["notification_plan_exactly_once"] is True
    assert report["durable_readback"]["notification_render_exactly_once"] is True
    assert report["durable_readback"]["notification_delivery_record_exactly_once"] is True
    assert report["durable_readback"]["notification_delivery_result_event_exactly_once"] is True
    assert report["durable_readback"]["q_maintenance_message_thin"] is True
    assert report["redis_ack_after_durable_readback"] is True
    assert runtime.call_order == [
        "inspect",
        "load_context",
        "consume",
        "execute",
        "publish_maintenance",
        "mark_published",
        "readback",
        "ack",
        "close",
    ]
    assert str(intent.trigger_event_id) not in encoded
    assert str(intent.notification_plan_id) not in encoded
    assert str(intent.analysis_id) not in encoded
    assert str(intent.target_chat_id) not in encoded
    assert "1718000000001-0" not in encoded
    assert "Rendered operator text" not in encoded
