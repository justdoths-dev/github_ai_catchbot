from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.policy_engine.noise_duplicate_suppression import (
    build_noise_duplicate_suppression_proof,
    render_sanitized_json,
)


class CliArgumentError(ValueError):
    pass


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Run the bounded duplicate/noise suppression proof without live authority.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("plan", "execute"), default="plan")
    parser.add_argument("--operator-approved", action="store_true")
    return parser


def _blocked(reason_code: str, *, mode: str, operator_approved: bool) -> dict[str, object]:
    return {
        "schema_version": "noise_duplicate_suppression_proof_v1",
        "runner_name": "bounded_noise_duplicate_suppression_runner",
        "mode": mode,
        "ok": False,
        "status": "blocked",
        "reason_code": reason_code,
        "authority": {
            "operator_approved": operator_approved,
            "database_read_allowed": False,
            "database_write_allowed": False,
            "redis_allowed": False,
            "telegram_allowed": False,
            "openai_allowed": False,
            "external_network_allowed": False,
        },
        "raw_values_printed": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(_blocked(str(exc), mode="argument_error", operator_approved=False)))
        return 1
    if args.mode == "execute" and not args.operator_approved:
        sys.stdout.write(
            render_sanitized_json(
                _blocked("operator_approval_missing", mode=str(args.mode), operator_approved=False)
            )
        )
        return 1
    report = build_noise_duplicate_suppression_proof()
    report["runner_name"] = "bounded_noise_duplicate_suppression_runner"
    report["mode"] = str(args.mode)
    report["authority"]["operator_approved"] = bool(args.operator_approved)
    sys.stdout.write(render_sanitized_json(report))
    return 0 if report["ok"] is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
