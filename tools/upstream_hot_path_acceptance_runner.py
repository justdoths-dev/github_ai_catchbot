from __future__ import annotations

import argparse
import asyncio
import json
import sys
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Sequence
from unittest.mock import patch


SCHEMA_VERSION = "upstream_hot_path_acceptance_v1"
OUTCOMES = ("send_now", "suppress")
REQUIRED_RESULT_KEYS = (
    "schema_version",
    "status",
    "delivery_decision",
    "event_sequence",
    "live_telegram_called",
    "openai_called",
    "workers_started",
    "redis_mutation",
    "notifier_boundary_reached",
)
FORBIDDEN_TRUE_FLAGS = (
    "live_telegram_called",
    "openai_called",
    "workers_started",
    "redis_mutation",
)


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the fake/offline upstream hot-path acceptance harness and "
            "print stable JSON without production env, DB, Redis, workers, "
            "Telegram, OpenAI, Docker, systemd, or Alembic authority."
        )
    )
    parser.add_argument("--outcome", choices=OUTCOMES, required=True)
    return parser


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True) + "\n"


def run(outcome: str) -> RunnerResult:
    try:
        report = asyncio.run(_run_harness(outcome))
    except Exception as exc:  # noqa: BLE001 - keep operator output JSON-shaped.
        report = {
            "schema_version": SCHEMA_VERSION,
            "status": "fail",
            "delivery_decision": outcome,
            "event_sequence": [],
            "live_telegram_called": False,
            "openai_called": False,
            "workers_started": False,
            "redis_mutation": False,
            "notifier_boundary_reached": False,
            "checks_failed": ["harness_execution"],
            "failure_type": exc.__class__.__name__,
        }
        return RunnerResult(exit_code=1, report=report)

    checks_failed = _invariant_failures(report)
    report["checks_failed"] = checks_failed
    if checks_failed:
        report["status"] = "fail"
    return RunnerResult(exit_code=0 if report.get("status") == "pass" else 1, report=report)


async def _run_harness(outcome: str) -> dict[str, Any]:
    _bootstrap_repo_imports()
    from tests.support.upstream_hot_path_acceptance import run_upstream_hot_path_acceptance

    with _runtime_tripwires():
        acceptance = await run_upstream_hot_path_acceptance(outcome=outcome)  # type: ignore[arg-type]
    return dict(acceptance.result)


def _bootstrap_repo_imports() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_root = repo_root / "src"
    for path in (repo_root, src_root):
        value = str(path)
        if value not in sys.path:
            sys.path.insert(0, value)


@contextmanager
def _runtime_tripwires() -> Iterator[None]:
    _bootstrap_repo_imports()
    from services.judge_openai import openai_client
    from services.maintenance import worker as maintenance_worker
    from services.notifier_telegram import telegram_client
    from services.outbox_relay import redis_streams as outbox_redis_streams
    from services.outbox_relay import service as outbox_relay_service

    def fail_forbidden_runtime(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("upstream acceptance runner must not open runtime infrastructure")

    with ExitStack() as stack:
        stack.enter_context(patch.object(openai_client, "OpenAIJudgeClient", fail_forbidden_runtime))
        stack.enter_context(patch.object(telegram_client.TelegramBotClient, "send_message", fail_forbidden_runtime))
        stack.enter_context(patch.object(telegram_client.TelegramBotClient, "edit_message_text", fail_forbidden_runtime))
        stack.enter_context(patch.object(outbox_redis_streams, "RedisStreamsPublisher", fail_forbidden_runtime))
        stack.enter_context(patch.object(outbox_relay_service, "OutboxRelayService", fail_forbidden_runtime))
        stack.enter_context(patch.object(maintenance_worker, "MaintenanceQueueWorker", fail_forbidden_runtime))
        stack.enter_context(patch.object(maintenance_worker, "ReplayQueueWorker", fail_forbidden_runtime))
        stack.enter_context(patch.object(maintenance_worker, "DueRetryPromotionWorker", fail_forbidden_runtime))
        yield


def _invariant_failures(report: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    for key in REQUIRED_RESULT_KEYS:
        if key not in report:
            failures.append(f"missing:{key}")
    if report.get("schema_version") != SCHEMA_VERSION:
        failures.append("schema_version")
    if report.get("status") != "pass":
        failures.append("status")
    for key in FORBIDDEN_TRUE_FLAGS:
        if report.get(key) is not False:
            failures.append(key)
    if not isinstance(report.get("event_sequence"), list):
        failures.append("event_sequence")

    delivery_decision = report.get("delivery_decision")
    if delivery_decision == "send_now":
        if report.get("notifier_boundary_reached") is not True:
            failures.append("send_now:notifier_boundary_reached")
    elif delivery_decision == "suppress":
        if report.get("notification_plan_created") is not False:
            failures.append("suppress:notification_plan_created")
        if report.get("notifier_boundary_reached") is not False:
            failures.append("suppress:notifier_boundary_reached")
    else:
        failures.append("delivery_decision")
    return failures


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args.outcome)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
