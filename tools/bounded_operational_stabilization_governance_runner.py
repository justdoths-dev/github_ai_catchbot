from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.maintenance.operational_stabilization_governance_proof import (  # noqa: E402
    AUTHORITY_FLAG_NAMES,
    RUNNER_NAME,
    SCHEMA_VERSION,
    build_operational_stabilization_governance_proof,
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
        description="Emit a sanitized operational stabilization governance code proof.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("plan", "proof"), default="proof")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(_blocked(str(exc), mode="argument_error")))
        return 1

    try:
        report = build_operational_stabilization_governance_proof(mode=str(args.mode))
        sys.stdout.write(render_sanitized_json(report))
        return 0 if report.get("ok") is True else 1
    except Exception:
        sys.stdout.write(render_sanitized_json(_blocked("runner_error", mode="proof")))
        return 1


def _blocked(reason_code: str, *, mode: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": mode,
        "ok": False,
        "status": "blocked",
        "reason_code": reason_code,
        "side_effect_authority": {name: False for name in AUTHORITY_FLAG_NAMES},
        "redactions_applied": {
            "raw_ids_omitted": True,
            "raw_stream_ids_omitted": True,
            "raw_dedupe_keys_omitted": True,
            "raw_urls_omitted": True,
            "raw_source_text_omitted": True,
            "telegram_chat_ids_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "api_keys_omitted": True,
            "bearer_tokens_omitted": True,
            "runtime_config_values_omitted": True,
            "raw_headers_omitted": True,
            "raw_openai_token_usage_omitted": True,
            "exception_bodies_omitted": True,
            "stderr_omitted": True,
            "raw_filesystem_locators_omitted": True,
        },
        "raw_values_printed": False,
    }


if __name__ == "__main__":
    raise SystemExit(main())
