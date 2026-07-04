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

from src.services.maintenance.notification_operator_acceptance import (
    blocked_notification_operator_acceptance_readback,
    build_notification_operator_acceptance_readback,
    render_sanitized_json,
)


MAX_INPUT_BYTES = 128 * 1024


class CliArgumentError(ValueError):
    pass


class AcceptancePacketInputError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Consolidate existing notification UX and restricted readbacks into an M1 acceptance packet.",
        add_help=False,
    )
    parser.add_argument("--allow-input-file-read", action="store_true")
    parser.add_argument("--notification-ux-render-preview-json", required=True)
    parser.add_argument("--restricted-send-disabled-json", required=True)
    parser.add_argument("--restricted-queued-worker-json", required=True)
    parser.add_argument("--restricted-queue-chain-json", required=True)
    parser.add_argument("--delivery-result-drain-json", required=True)
    parser.add_argument("--zero-preserving-readback-json", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(
            render_sanitized_json(
                blocked_notification_operator_acceptance_readback(
                    str(exc),
                    input_file_read_attempted=False,
                )
            )
        )
        return 1

    if not args.allow_input_file_read:
        sys.stdout.write(
            render_sanitized_json(
                blocked_notification_operator_acceptance_readback(
                    "input_file_read_not_allowed",
                    input_file_read_attempted=False,
                )
            )
        )
        return 1

    try:
        packet = build_notification_operator_acceptance_readback(
            notification_ux_render_preview=_read_json_object(args.notification_ux_render_preview_json),
            restricted_send_disabled=_read_json_object(args.restricted_send_disabled_json),
            restricted_queued_worker=_read_json_object(args.restricted_queued_worker_json),
            restricted_queue_chain=_read_json_object(args.restricted_queue_chain_json),
            delivery_result_drain=_read_json_object(args.delivery_result_drain_json),
            zero_preserving_readback=_read_json_object(args.zero_preserving_readback_json),
            input_file_read_attempted=True,
        )
    except AcceptancePacketInputError as exc:
        packet = blocked_notification_operator_acceptance_readback(
            exc.reason_code,
            input_file_read_attempted=True,
        )
    except Exception:
        packet = blocked_notification_operator_acceptance_readback(
            "acceptance_packet_runner_error",
            input_file_read_attempted=True,
        )

    sys.stdout.write(render_sanitized_json(packet))
    return 0 if packet["status"] == "pass" else 1


def _read_json_object(path_text: str) -> dict[str, Any]:
    path = Path(path_text)
    if path.suffix != ".json":
        raise AcceptancePacketInputError("input_file_extension_not_allowed")
    try:
        if not path.is_file():
            raise AcceptancePacketInputError("input_file_missing")
        if path.stat().st_size > MAX_INPUT_BYTES:
            raise AcceptancePacketInputError("input_file_too_large")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except AcceptancePacketInputError:
        raise
    except (OSError, UnicodeDecodeError, JSONDecodeError) as exc:
        raise AcceptancePacketInputError("input_file_unreadable") from exc
    if not isinstance(payload, dict):
        raise AcceptancePacketInputError("input_file_not_object")
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
