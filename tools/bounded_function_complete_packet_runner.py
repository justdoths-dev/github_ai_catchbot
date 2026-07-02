from __future__ import annotations

import argparse
import json
import sys
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.policy_engine.function_complete_packet import (
    build_function_complete_packet,
    render_sanitized_json,
)
from src.services.policy_engine.noise_duplicate_suppression import build_noise_duplicate_suppression_proof


MAX_EVIDENCE_BYTES = 128 * 1024


class CliArgumentError(ValueError):
    pass


class BoundedFunctionCompletePacketError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Consume F9 duplicate/noise proof output and emit a sanitized function-complete packet.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("plan", "execute"), default="plan")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--f9-proof-json")
    parser.add_argument("--allow-f9-proof-file-read", action="store_true")
    parser.add_argument("--origin-evidence-json")
    parser.add_argument("--allow-origin-evidence-file-read", action="store_true")
    parser.add_argument("--vps-evidence-json")
    parser.add_argument("--allow-vps-evidence-file-read", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(_blocked(str(exc), mode="argument_error", operator_approved=False)))
        return 1

    try:
        if args.mode == "execute" and not args.operator_approved:
            raise BoundedFunctionCompletePacketError("operator_approval_missing")
        if args.f9_proof_json and not args.allow_f9_proof_file_read:
            raise BoundedFunctionCompletePacketError("f9_proof_file_read_not_allowed")
        if args.origin_evidence_json and not args.allow_origin_evidence_file_read:
            raise BoundedFunctionCompletePacketError("origin_evidence_file_read_not_allowed")
        if args.vps_evidence_json and not args.allow_vps_evidence_file_read:
            raise BoundedFunctionCompletePacketError("vps_evidence_file_read_not_allowed")

        f9_proof = (
            _read_json_object(args.f9_proof_json, file_kind="f9_proof")
            if args.f9_proof_json
            else build_noise_duplicate_suppression_proof()
        )
        origin_evidence = (
            _read_json_object(args.origin_evidence_json, file_kind="origin_evidence")
            if args.origin_evidence_json
            else None
        )
        vps_evidence = (
            _read_json_object(args.vps_evidence_json, file_kind="vps_evidence") if args.vps_evidence_json else None
        )
        packet = build_function_complete_packet(
            f9_proof=f9_proof,
            origin_evidence=origin_evidence,
            vps_evidence=vps_evidence,
        )
        packet["mode"] = str(args.mode)
        packet["authority"] = {
            "operator_approved": bool(args.operator_approved),
            "f9_proof_file_read_allowed": bool(args.allow_f9_proof_file_read),
            "origin_evidence_file_read_allowed": bool(args.allow_origin_evidence_file_read),
            "vps_evidence_file_read_allowed": bool(args.allow_vps_evidence_file_read),
            "database_read_allowed": False,
            "database_write_allowed": False,
            "redis_allowed": False,
            "telegram_allowed": False,
            "openai_allowed": False,
            "external_network_allowed": False,
        }
        sys.stdout.write(render_sanitized_json(packet))
        return 0 if packet["ok"] is True else 1
    except BoundedFunctionCompletePacketError as exc:
        sys.stdout.write(render_sanitized_json(_blocked(exc.reason_code, mode=str(args.mode), operator_approved=bool(args.operator_approved))))
        return 1
    except Exception as exc:
        del exc
        sys.stdout.write(render_sanitized_json(_blocked("runner_error", mode=str(args.mode), operator_approved=bool(args.operator_approved))))
        return 1


def _read_json_object(path_text: str, *, file_kind: str) -> dict[str, Any]:
    path = Path(path_text)
    lowered_parts = {part.lower() for part in path.parts}
    if {".env", "runtime.env"} & lowered_parts:
        raise BoundedFunctionCompletePacketError(f"{file_kind}_file_not_allowed")
    if path.suffix != ".json":
        raise BoundedFunctionCompletePacketError(f"{file_kind}_file_extension_not_allowed")
    if not path.exists() or not path.is_file():
        raise BoundedFunctionCompletePacketError(f"{file_kind}_file_missing")
    if path.stat().st_size > MAX_EVIDENCE_BYTES:
        raise BoundedFunctionCompletePacketError(f"{file_kind}_file_too_large")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, JSONDecodeError) as exc:
        raise BoundedFunctionCompletePacketError(f"{file_kind}_file_unreadable") from exc
    if not isinstance(payload, dict):
        raise BoundedFunctionCompletePacketError(f"{file_kind}_file_invalid_json_object")
    return payload


def _blocked(reason_code: str, *, mode: str, operator_approved: bool) -> dict[str, Any]:
    return {
        "schema_version": "function_complete_packet_v1",
        "runner_name": "bounded_function_complete_packet_runner",
        "mode": mode,
        "ok": False,
        "status": "blocked",
        "packet_status": "FUNCTION_COMPLETE_PACKET_BLOCKED",
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
        "redactions_applied": {
            "full_ids_omitted": True,
            "raw_urls_omitted": True,
            "raw_source_text_omitted": True,
            "dedupe_keys_omitted": True,
            "material_hashes_omitted": True,
            "db_redis_urls_omitted": True,
            "env_values_omitted": True,
            "exception_bodies_omitted": True,
            "tracebacks_omitted": True,
        },
        "raw_values_printed": False,
    }


if __name__ == "__main__":
    raise SystemExit(main())
