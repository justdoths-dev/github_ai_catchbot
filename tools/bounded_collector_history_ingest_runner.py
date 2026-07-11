from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.collector_telegram.bounded_history_ingest_runner import (
    BoundedTelegramCollectorGitHubSearchResult,
    BoundedTelegramCollectorHistoryIngestConfig,
    BoundedTelegramCollectorHistoryIngestResult,
    BoundedTelegramCollectorHistoryIngestRuntimeBuilder,
    CollectorTelegramConfig,
    EXECUTE_CONFIRM_TOKEN,
    FULL_REGISTRY_EXECUTE_CONFIRM_TOKEN,
    SEARCH_CONFIRM_TOKEN,
    TARGET_LOCATOR_SCHEMA_VERSION,
    THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN,
    argument_error_report,
    render_sanitized_json,
    run_bounded_telegram_collector_history_ingest_sync,
    search_argument_error_report,
)


class CliArgumentError(ValueError):
    pass


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Run bounded Telegram collector history ingest for exact or capped full-registry targets.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("plan", "execute", "search"), default="plan")
    parser.add_argument("--rollout-scope", default="exact-targets")
    parser.add_argument("--source-kind", default="public_username")
    parser.add_argument("--source-value", action="append", dest="source_values")
    parser.add_argument("--target-message-id", type=int, default=None)
    parser.add_argument("--target-locator-path", default=None)
    parser.add_argument("--target-locator-output-path", default=None)
    parser.add_argument("--allow-target-locator-write", action="store_true")
    parser.add_argument("--registry-id-suffix", default=None)
    parser.add_argument("--max-targets", type=int, default=None)
    parser.add_argument("--history-limit", type=int, default=10)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--confirm-token", default=None)
    parser.add_argument("--allow-runtime-config", action="store_true")
    parser.add_argument("--allow-database-read", action="store_true")
    parser.add_argument("--allow-telegram-read", action="store_true")
    parser.add_argument("--allow-database-write", action="store_true")
    parser.add_argument("--allow-source-message-write", action="store_true")
    parser.add_argument("--allow-source-version-write", action="store_true")
    parser.add_argument("--allow-source-outbox-write", action="store_true")
    parser.add_argument("--allow-source-outbox-publish", action="store_true")
    parser.add_argument("--allow-redis-publish", action="store_true")
    return parser


def run(
    args: argparse.Namespace,
    *,
    runtime_config_loader=None,
    runtime_builder: BoundedTelegramCollectorHistoryIngestRuntimeBuilder | None = None,
) -> RunnerResult:
    runner_kwargs: dict[str, Any] = {
        "runtime_builder": runtime_builder,
    }
    if runtime_config_loader is not None:
        runner_kwargs["runtime_config_loader"] = runtime_config_loader
    source_values = tuple(args.source_values or ())
    result = run_bounded_telegram_collector_history_ingest_sync(
        BoundedTelegramCollectorHistoryIngestConfig(
            mode=str(args.mode),
            rollout_scope=str(args.rollout_scope),
            source_kind=str(args.source_kind),
            source_value=source_values[0] if len(source_values) == 1 else None,
            source_values=source_values,
            target_message_id=args.target_message_id,
            target_locator_path=args.target_locator_path,
            target_locator_output_path=args.target_locator_output_path,
            allow_target_locator_write=bool(args.allow_target_locator_write),
            registry_id_suffix=args.registry_id_suffix,
            max_targets=args.max_targets,
            history_limit=int(args.history_limit),
            max_messages=args.max_messages,
            operator_approved=bool(args.operator_approved),
            confirm_token=args.confirm_token,
            allow_runtime_config=bool(args.allow_runtime_config),
            allow_database_read=bool(args.allow_database_read),
            allow_telegram_read=bool(args.allow_telegram_read),
            allow_database_write=bool(args.allow_database_write),
            allow_source_message_write=bool(args.allow_source_message_write),
            allow_source_version_write=bool(args.allow_source_version_write),
            allow_source_outbox_write=bool(args.allow_source_outbox_write),
            allow_source_outbox_publish=bool(args.allow_source_outbox_publish),
            allow_redis_publish=bool(args.allow_redis_publish),
        ),
        **runner_kwargs,
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_config_loader=None,
    runtime_builder: BoundedTelegramCollectorHistoryIngestRuntimeBuilder | None = None,
) -> int:
    effective_argv = list(sys.argv[1:] if argv is None else argv)
    try:
        args = build_parser().parse_args(effective_argv)
    except CliArgumentError as exc:
        error_report = (
            search_argument_error_report(str(exc))
            if _argv_requests_search(effective_argv)
            else argument_error_report(str(exc))
        )
        sys.stdout.write(render_sanitized_json(error_report))
        return 1
    result = run(
        args,
        runtime_config_loader=runtime_config_loader,
        runtime_builder=runtime_builder,
    )
    sys.stdout.write(render_sanitized_json(result.report))
    return result.exit_code


def _argv_requests_search(argv: Sequence[str]) -> bool:
    for index, token in enumerate(argv):
        if token == "--mode" and index + 1 < len(argv):
            return argv[index + 1].strip().lower() == "search"
        if token.startswith("--mode="):
            return token.partition("=")[2].strip().lower() == "search"
    return False


__all__ = [
    "BoundedTelegramCollectorGitHubSearchResult",
    "BoundedTelegramCollectorHistoryIngestConfig",
    "BoundedTelegramCollectorHistoryIngestResult",
    "BoundedTelegramCollectorHistoryIngestRuntimeBuilder",
    "CliArgumentError",
    "CollectorTelegramConfig",
    "EXECUTE_CONFIRM_TOKEN",
    "FULL_REGISTRY_EXECUTE_CONFIRM_TOKEN",
    "RunnerResult",
    "SEARCH_CONFIRM_TOKEN",
    "TARGET_LOCATOR_SCHEMA_VERSION",
    "THREE_CHANNEL_EXECUTE_CONFIRM_TOKEN",
    "build_parser",
    "main",
    "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
