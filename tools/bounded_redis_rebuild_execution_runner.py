from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any, Awaitable, Callable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.maintenance.redis_rebuild_execution import (  # noqa: E402
    RedisRebuildExecutionRequest,
    blocked_report,
    render_sanitized_json,
    run_redis_rebuild_execution_from_runtime_env_file,
)


class CliArgumentError(ValueError):
    pass


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


RuntimeRunner = Callable[[str, RedisRebuildExecutionRequest], Awaitable[dict[str, Any]]]


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Execute a bounded Redis rebuild for one exact queue after explicit approval.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("plan", "execute"), default="plan")
    parser.add_argument("--runtime-env-file")
    parser.add_argument("--queue")
    parser.add_argument("--max-rebuild-jobs", type=int)
    parser.add_argument("--expected-head")
    parser.add_argument("--i-understand-this-mutates-redis", action="store_true")
    parser.add_argument("--approve-redis-rebuild-execution", action="store_true")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_runner: RuntimeRunner = run_redis_rebuild_execution_from_runtime_env_file,
) -> int:
    try:
        args = build_parser().parse_args(argv)
        request = RedisRebuildExecutionRequest(
            mode=str(args.mode),
            queue_selector=str(args.queue).strip() if args.queue else None,
            max_rebuild_jobs=args.max_rebuild_jobs,
            expected_head=str(args.expected_head).strip() if args.expected_head else None,
            understand_mutates_redis=bool(args.i_understand_this_mutates_redis),
            approve_redis_rebuild_execution=bool(args.approve_redis_rebuild_execution),
        )
        path_error = _runtime_env_file_error(args.runtime_env_file)
        if path_error is not None:
            report = blocked_report(path_error, mode=request.mode)
        else:
            report = asyncio.run(runtime_runner(str(args.runtime_env_file), request))
        sys.stdout.write(render_sanitized_json(report))
        return 0 if report.get("status") == "pass" else 1
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(_blocked(str(exc))))
        return 1
    except Exception:
        sys.stdout.write(render_sanitized_json(_blocked("runner_error", status="failed")))
        return 1


def _runtime_env_file_error(runtime_env_file: str | None) -> str | None:
    if not runtime_env_file:
        return "runtime_env_file_required"
    try:
        path = Path(runtime_env_file)
    except (TypeError, ValueError):
        return "runtime_env_file_invalid"
    if not path.is_absolute():
        return "runtime_env_file_not_absolute"
    return None


def _blocked(reason_code: str, *, status: str = "blocked") -> dict[str, Any]:
    return blocked_report(reason_code, mode="plan", status=status)


if __name__ == "__main__":
    raise SystemExit(main())
