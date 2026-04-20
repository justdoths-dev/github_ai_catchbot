"""CLI entrypoint for the collector bootstrap skeleton."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import json
import logging
import signal
from typing import TextIO

from .config import CollectorTelegramConfig
from .exceptions import CollectorTelegramConfigError
from .service import CollectorTelegramService

logger = logging.getLogger(__name__)
_RESERVED_LOG_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__)


class StructuredJsonFormatter(logging.Formatter):
    """Serialize log records to a stable JSON shape."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "service": getattr(record, "service", "collector-telegram"),
            "logger": record.name,
            "message": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key in _RESERVED_LOG_RECORD_FIELDS or key.startswith("_"):
                continue
            payload[key] = value

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, sort_keys=True)


def _resolve_log_level(log_level: str) -> int:
    level_name = log_level.upper()
    resolved_level = logging.getLevelName(level_name)
    if isinstance(resolved_level, int):
        return resolved_level
    return logging.INFO


def configure_logging(log_level: str, stream: TextIO | None = None) -> None:
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(_resolve_log_level(log_level))

    handler = logging.StreamHandler(stream)
    handler.setFormatter(StructuredJsonFormatter())
    root_logger.addHandler(handler)


def _install_signal_handlers(service: CollectorTelegramService) -> None:
    loop = asyncio.get_running_loop()

    def _request_stop(signame: str) -> None:
        logger.info(
            "collector_signal_received",
            extra={
                "service": "collector-telegram",
                "event": "collector_signal_received",
                "signal": signame,
            },
        )
        service.request_stop(f"signal:{signame.lower()}")

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                signum,
                _request_stop,
                signum.name,
            )
        except NotImplementedError:
            signal.signal(
                signum,
                lambda _sig, _frame, signame=signum.name: _request_stop(signame),
            )


async def _run_service(config: CollectorTelegramConfig) -> int:
    service = CollectorTelegramService(config)
    _install_signal_handlers(service)
    await service.start()
    await service.wait_closed()
    return 0


def run() -> int:
    try:
        config = CollectorTelegramConfig.from_env()
    except CollectorTelegramConfigError as exc:
        configure_logging("INFO")
        logger.error(
            "collector_configuration_error",
            extra={
                "service": "collector-telegram",
                "event": "collector_configuration_error",
                "error": str(exc),
            },
        )
        return 2

    configure_logging(config.log_level)
    try:
        return asyncio.run(_run_service(config))
    except KeyboardInterrupt:
        logger.info(
            "collector_keyboard_interrupt",
            extra={"service": "collector-telegram", "event": "collector_keyboard_interrupt"},
        )
        return 130


if __name__ == "__main__":
    raise SystemExit(run())
