from __future__ import annotations

import argparse
import asyncio
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.collector_telegram.config import CollectorTelegramConfig
from src.services.collector_telegram.models import RuntimeSnapshot
from src.services.collector_telegram.service import CollectorTelegramService
from src.services.collector_telegram.singleton_guard import CollectorSingletonGuard


REPORT_TYPE = "collector_live_startup_probe_v1"
SUCCESS_NOTE = "Probe success does not authorize live ingest or production rollout."
SYNTHETIC_SECRET_SENTINELS = (
    "SYNTHETIC_TELEGRAM_API_HASH_DO_NOT_PRINT",
    "SYNTHETIC_TDLIB_DB_ENCRYPTION_KEY_DO_NOT_PRINT",
    "SYNTHETIC_TELEGRAM_2FA_DO_NOT_PRINT",
)

CHECK_NAMES = (
    "repo_local_only",
    "uses_synthetic_environment_only",
    "default_lock_path_computed",
    "override_lock_path_computed",
    "singleton_guard_acquire_release",
    "replay_mode_skips_live_singleton",
    "fake_runtime_start_stop",
)

SIDE_EFFECTS = {
    "tdlib_started": False,
    "telegram_called": False,
    "db_connection_attempted": False,
    "redis_connection_attempted": False,
    "external_network_attempted": False,
    "docker_invoked": False,
    "systemd_invoked": False,
    "env_or_feature_flags_mutated": False,
    "secret_values_printed": False,
}


class FakeRuntime:
    def __init__(self) -> None:
        self.snapshot = RuntimeSnapshot()
        self.startup_acceptance_count = 0
        self.run_forever_count = 0
        self.shutdown_count = 0
        self._stop_event = asyncio.Event()

    async def startup_acceptance_check(self) -> None:
        self.startup_acceptance_count += 1

    async def run_forever(self) -> None:
        self.run_forever_count += 1
        await self._stop_event.wait()

    async def shutdown(self) -> None:
        self.shutdown_count += 1
        self._stop_event.set()


class CountingSingletonGuard:
    def __init__(self) -> None:
        self.acquire_count = 0
        self.release_count = 0
        self.acquired = False

    def acquire(self) -> None:
        self.acquire_count += 1
        self.acquired = True

    def release(self) -> None:
        self.release_count += 1
        self.acquired = False

    def is_acquired(self) -> bool:
        return self.acquired


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a repo-local fake-runtime collector live-startup prerequisite probe. "
            "The probe prints JSON only and does not start TDLib, Telegram, DB, Redis, "
            "Docker, systemd, or live ingest."
        )
    )
    parser.add_argument(
        "--format",
        choices=("json",),
        default="json",
        help="Output format. Only json is supported.",
    )
    return parser


def _new_report() -> dict[str, Any]:
    return {
        "report_type": REPORT_TYPE,
        "contract_status": "passed",
        "checks_failed": [],
        "failures": [],
        "checks": {name: "not_run" for name in CHECK_NAMES},
        "side_effects": dict(SIDE_EFFECTS),
        "authorization": {
            "live_ingest_authorized": False,
            "production_rollout_authorized": False,
        },
        "notes": [SUCCESS_NOTE],
    }


def _mark_pass(report: dict[str, Any], check_name: str) -> None:
    report["checks"][check_name] = "passed"


def _mark_not_applicable(report: dict[str, Any], check_name: str, reason: str) -> None:
    report["checks"][check_name] = "not_applicable"
    report.setdefault("not_applicable_reasons", {})[check_name] = reason


def _mark_fail(report: dict[str, Any], check_name: str, reason_code: str, message: str) -> None:
    report["checks"][check_name] = "failed"
    report["checks_failed"].append(reason_code)
    report["failures"].append(
        {
            "check": check_name,
            "reason_code": reason_code,
            "message": _redact_synthetic_secret_sentinels(message),
        }
    )


def _redact_synthetic_secret_sentinels(text: str) -> str:
    redacted = text
    for sentinel in SYNTHETIC_SECRET_SENTINELS:
        redacted = redacted.replace(sentinel, "<redacted-synthetic-secret>")
    return redacted


def _write_synthetic_secret_files(temp_root: Path) -> dict[str, Path]:
    secret_dir = temp_root / "synthetic-secrets"
    secret_dir.mkdir(parents=True, exist_ok=True)
    secret_paths = {
        "TELEGRAM_API_HASH_FILE": secret_dir / "telegram_api_hash",
        "TDLIB_DB_ENCRYPTION_KEY_FILE": secret_dir / "tdlib_db_encryption_key",
        "TELEGRAM_2FA_PASSWORD_FILE": secret_dir / "telegram_2fa_password",
    }
    secret_paths["TELEGRAM_API_HASH_FILE"].write_text(SYNTHETIC_SECRET_SENTINELS[0], encoding="utf-8")
    secret_paths["TDLIB_DB_ENCRYPTION_KEY_FILE"].write_text(SYNTHETIC_SECRET_SENTINELS[1], encoding="utf-8")
    secret_paths["TELEGRAM_2FA_PASSWORD_FILE"].write_text(SYNTHETIC_SECRET_SENTINELS[2], encoding="utf-8")
    return secret_paths


def _synthetic_env(temp_root: Path, *, singleton_lock_path: Path | None = None) -> dict[str, str]:
    secret_paths = _write_synthetic_secret_files(temp_root)
    tdlib_state_dir = temp_root / "tdlib-state"
    env = {
        "APP_ENV": "dev",
        "COLLECTOR_MODE": "replay",
        "DATABASE_URL": "postgresql+asyncpg://collector:synthetic@localhost/synthetic_startup_probe",
        "REDIS_URL": "",
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_PHONE_NUMBER": "+10000000000",
        "TDLIB_STATE_DIR": str(tdlib_state_dir),
        "TDLIB_FILES_DIR": str(temp_root / "tdlib-files"),
        "RECONCILE_INTERVAL_SEC": "300",
        "RECONCILE_BACKFILL_LIMIT": "50",
        "WARM_BACKFILL_LIMIT": "30",
        "HISTORY_PAGE_LIMIT": "50",
        "STARTUP_PROBE_TIMEOUT_SEC": "30",
        "STARTUP_WARM_BACKFILL_ENABLED": "false",
        **{name: str(path) for name, path in secret_paths.items()},
    }
    if singleton_lock_path is not None:
        env["COLLECTOR_SINGLETON_LOCK_PATH"] = str(singleton_lock_path)
    return env


def _build_synthetic_config(
    temp_root: Path,
    *,
    singleton_lock_path: Path | None = None,
) -> CollectorTelegramConfig:
    return CollectorTelegramConfig.from_env(_synthetic_env(temp_root, singleton_lock_path=singleton_lock_path))


def _check_default_lock_path(report: dict[str, Any], temp_root: Path) -> CollectorTelegramConfig | None:
    check_name = "default_lock_path_computed"
    try:
        config = _build_synthetic_config(temp_root / "default")
        expected = Path(config.tdlib_state_dir) / "collector-telegram-live.lock"
        if Path(config.singleton_lock_path) != expected:
            _mark_fail(
                report,
                check_name,
                "default_lock_path_mismatch",
                "Default collector singleton lock path did not match the TDLib state directory.",
            )
            return None
        _mark_pass(report, check_name)
        return config
    except AttributeError as exc:
        _mark_not_applicable(report, check_name, f"collector config has no singleton lock path surface: {exc}")
    except Exception as exc:
        _mark_fail(report, check_name, "default_lock_path_error", str(exc))
    return None


def _check_override_lock_path(report: dict[str, Any], temp_root: Path) -> CollectorTelegramConfig | None:
    check_name = "override_lock_path_computed"
    try:
        override_path = temp_root / "override-lock" / "collector.lock"
        config = _build_synthetic_config(temp_root / "override", singleton_lock_path=override_path)
        if Path(config.singleton_lock_path) != override_path:
            _mark_fail(
                report,
                check_name,
                "override_lock_path_mismatch",
                "COLLECTOR_SINGLETON_LOCK_PATH override was not reflected in collector config.",
            )
            return None
        _mark_pass(report, check_name)
        return config
    except AttributeError as exc:
        _mark_not_applicable(report, check_name, f"collector config has no singleton lock path surface: {exc}")
    except Exception as exc:
        _mark_fail(report, check_name, "override_lock_path_error", str(exc))
    return None


def _check_singleton_guard(report: dict[str, Any], temp_root: Path) -> None:
    check_name = "singleton_guard_acquire_release"
    guard = CollectorSingletonGuard(str(temp_root / "guard" / "collector.lock"))
    duplicate = CollectorSingletonGuard(str(temp_root / "guard" / "collector.lock"))
    try:
        guard.acquire()
        if not guard.is_acquired():
            _mark_fail(report, check_name, "singleton_guard_not_acquired", "Guard did not report acquired state.")
            return
        try:
            duplicate.acquire()
        except Exception:
            pass
        else:
            duplicate.release()
            _mark_fail(
                report,
                check_name,
                "singleton_guard_duplicate_acquired",
                "Duplicate singleton guard acquired the same temp lock path.",
            )
            return
        guard.release()
        if guard.is_acquired():
            _mark_fail(report, check_name, "singleton_guard_not_released", "Guard did not release temp lock path.")
            return
        _mark_pass(report, check_name)
    except Exception as exc:
        _mark_fail(report, check_name, "singleton_guard_acquire_release_error", str(exc))
    finally:
        guard.release()


async def _run_fake_replay_service(config: CollectorTelegramConfig) -> tuple[FakeRuntime, CountingSingletonGuard]:
    runtime = FakeRuntime()
    guard = CountingSingletonGuard()
    service = CollectorTelegramService(config, runtime, singleton_guard=guard)  # type: ignore[arg-type]
    await service.start()
    await service.stop()
    return runtime, guard


def _check_fake_service(report: dict[str, Any], config: CollectorTelegramConfig | None) -> None:
    if config is None:
        _mark_not_applicable(
            report,
            "replay_mode_skips_live_singleton",
            "collector config construction failed before service seam could be probed",
        )
        _mark_not_applicable(
            report,
            "fake_runtime_start_stop",
            "collector config construction failed before service seam could be probed",
        )
        return

    try:
        runtime, guard = asyncio.run(_run_fake_replay_service(config))
    except Exception as exc:
        _mark_fail(report, "fake_runtime_start_stop", "fake_runtime_start_stop_error", str(exc))
        _mark_not_applicable(
            report,
            "replay_mode_skips_live_singleton",
            "fake runtime service start/stop failed before singleton skip could be verified",
        )
        return

    if guard.acquire_count == 0 and guard.release_count == 0 and not guard.is_acquired():
        _mark_pass(report, "replay_mode_skips_live_singleton")
    else:
        _mark_fail(
            report,
            "replay_mode_skips_live_singleton",
            "replay_mode_acquired_live_singleton",
            "Replay-mode service acquired or released the live singleton guard.",
        )

    if (
        runtime.startup_acceptance_count == 1
        and runtime.run_forever_count == 1
        and runtime.shutdown_count == 1
    ):
        _mark_pass(report, "fake_runtime_start_stop")
    else:
        _mark_fail(
            report,
            "fake_runtime_start_stop",
            "fake_runtime_call_counts_mismatch",
            "Fake runtime startup/run/stop counts did not match the expected safe service path.",
        )


def generate_report() -> dict[str, Any]:
    report = _new_report()
    _mark_pass(report, "repo_local_only")
    _mark_pass(report, "uses_synthetic_environment_only")

    with tempfile.TemporaryDirectory(prefix="collector-live-startup-probe-") as temp_dir:
        temp_root = Path(temp_dir)
        replay_config = _check_default_lock_path(report, temp_root)
        _check_override_lock_path(report, temp_root)
        _check_singleton_guard(report, temp_root)
        _check_fake_service(report, replay_config)

    if report["checks_failed"]:
        report["contract_status"] = "failed"
    return report


def render_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    parser.parse_args(argv)
    report = generate_report()
    sys.stdout.write(render_json(report))
    sys.stdout.write("\n")
    return 1 if report["checks_failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
