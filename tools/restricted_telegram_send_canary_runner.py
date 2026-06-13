from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.notifier_telegram.restricted_send_canary import (
    DEFAULT_MAX_MESSAGE_CHARS,
    DEFAULT_MAX_REQUESTS,
    DEFAULT_TELEGRAM_API_BASE_URL,
    DEFAULT_TIMEOUT_MS,
    RestrictedTelegramSendCanaryConfig,
    RestrictedTelegramSendLiveTransport,
    run_restricted_telegram_send_canary,
)


DEFAULT_TELEGRAM_BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"


@dataclass(frozen=True, slots=True)
class RunnerResult:
    exit_code: int
    report: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a restricted operator-approved Telegram send canary."
    )
    parser.add_argument("--chat-id", help="Explicit approved Telegram target chat ID")
    parser.add_argument("--operator-approved", action="store_true")
    parser.add_argument("--allow-send", action="store_true")
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--telegram-bot-token-env", default=DEFAULT_TELEGRAM_BOT_TOKEN_ENV)
    parser.add_argument("--telegram-api-base-url", default=DEFAULT_TELEGRAM_API_BASE_URL)
    parser.add_argument("--message", help="Optional bounded synthetic canary message override")
    parser.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS)
    parser.add_argument("--timeout-ms", type=int, default=DEFAULT_TIMEOUT_MS)
    parser.add_argument("--max-message-chars", type=int, default=DEFAULT_MAX_MESSAGE_CHARS)
    return parser


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=False) + "\n"


def run(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    transport: Any | None = None,
) -> RunnerResult:
    return asyncio.run(run_async(args, env=env, transport=transport))


async def run_async(
    args: argparse.Namespace,
    *,
    env: Mapping[str, str] | None = None,
    transport: Any | None = None,
) -> RunnerResult:
    effective_env = env if env is not None else os.environ
    token_env_name = str(args.telegram_bot_token_env or "").strip()
    bot_token = str(effective_env.get(token_env_name, "")).strip() if token_env_name else ""
    config = RestrictedTelegramSendCanaryConfig(
        operator_approved=bool(args.operator_approved),
        allow_send=bool(args.allow_send),
        allow_network=bool(args.allow_network),
        bot_token=bot_token,
        chat_id=args.chat_id,
        telegram_api_base_url=str(args.telegram_api_base_url or "").strip(),
        message=args.message,
        max_requests=int(args.max_requests),
        timeout_ms=int(args.timeout_ms),
        max_message_chars=int(args.max_message_chars),
    )
    result = await run_restricted_telegram_send_canary(
        config,
        transport=transport or RestrictedTelegramSendLiveTransport(),
    )
    report = result.to_sanitized_dict()
    return RunnerResult(exit_code=0 if result.ok else 1, report=report)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    sys.stdout.write(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
