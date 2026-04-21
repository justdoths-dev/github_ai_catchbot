from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Any, TextIO

from .config import CollectorTelegramConfig
from .exceptions import ConfigurationError
from .health import CollectorHealthService
from .runtime import CollectorRuntime
from .service import CollectorTelegramService


_BASE_RECORD_KEYS = set(logging.makeLogRecord({}).__dict__.keys())


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key in _BASE_RECORD_KEYS or key in {"message", "asctime"}:
                continue
            if key.startswith("_"):
                continue
            payload[key] = self._normalize(value)

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)

    def _normalize(self, value: Any) -> Any:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc).isoformat()
        if isinstance(value, dict):
            return {str(k): self._normalize(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._normalize(v) for v in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)


def configure_logging(log_level: str, *, stream: TextIO | None = None) -> logging.Logger:
    level = getattr(logging, log_level.upper(), logging.INFO)
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter())

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    for logger_name in ("collector_telegram", "services.collector_telegram"):
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.handlers.clear()
        logger.propagate = True

    return logging.getLogger("collector_telegram")


def build_logger(log_level: str) -> logging.Logger:
    return configure_logging(log_level)


async def _run() -> int:
    try:
        config = CollectorTelegramConfig.from_env()
    except ConfigurationError as exc:
        print(
            json.dumps(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "level": "ERROR",
                    "service": "collector-telegram",
                    "event": "collector_config_invalid",
                    "stage": "collector_bootstrap",
                    "error": str(exc),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2

    logger = build_logger(config.log_level)
    health = CollectorHealthService(logger=logger.getChild("health"))
    runtime = CollectorRuntime(
        config,
        health=health,
        logger=logger.getChild("runtime"),
    )
    service = CollectorTelegramService(config, runtime, logger=logger)

    loop = asyncio.get_running_loop()

    def _schedule_stop() -> None:
        loop.create_task(service.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _schedule_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _schedule_stop())

    try:
        await service.run()
    except asyncio.CancelledError:
        logger.info(
            "collector_main_cancelled",
            extra={
                "service": "collector-telegram",
                "event": "collector_main_cancelled",
                "stage": "collector_bootstrap",
            },
        )
        return 0
    except Exception:
        logger.exception(
            "collector_main_failed",
            extra={
                "service": "collector-telegram",
                "event": "collector_main_failed",
                "stage": "collector_bootstrap",
            },
        )
        return 1

    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()