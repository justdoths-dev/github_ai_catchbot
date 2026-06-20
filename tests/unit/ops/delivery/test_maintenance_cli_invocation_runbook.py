from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]


def _read_runbook(name: str) -> str:
    return (ROOT / "ops" / "delivery" / "runbooks" / name).read_text(encoding="utf-8")


def test_maintenance_cli_invocation_documents_one_shot_compose_and_confirm() -> None:
    text = _read_runbook("maintenance_cli_invocation.md")

    assert "python -m src.services.maintenance.worker_bootstrap" in text
    assert "python -m src.services.maintenance.main worker" not in text
    assert "docker compose run --rm maintenance" in text
    assert "docker compose up" in text
    assert "Never run batch-recovery via `docker compose up`" in text
    assert "--confirm write" in text
    assert "The gate does not apply flags" in text
    assert "replay_requests" in text
    assert "event_outbox" in text


def test_delivery_gate_handoff_documents_restricted_full_and_fail_rules() -> None:
    text = _read_runbook("delivery_gate_handoff.md")

    assert "Restricted Rollout Handoff" in text
    assert "Full Rollout Handoff" in text
    assert "Fail Rules" in text
    assert "15-30 minutes" in text
    assert "operator_review_passed=true" in text
    assert "ENABLE_NOTIFICATION_SEND=false" in text
    assert "MAINTENANCE_ENABLE_NOTIFICATION_RETRY_PROMOTION=false" in text
    assert "does not apply flags automatically" in text
    assert "--confirm write" in text
