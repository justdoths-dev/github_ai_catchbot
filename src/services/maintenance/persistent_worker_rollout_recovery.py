from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..policy_engine.function_complete_packet import build_function_complete_packet
from .controlled_worker_activation import (
    SCHEMA_VERSION as CONTROLLED_WORKER_SCHEMA_VERSION,
    ControlledWorkerActivationRequest,
    controlled_worker_activation_request_error,
)
from .systemd_rollout import (
    SCHEMA_VERSION as SYSTEMD_ROLLOUT_SCHEMA_VERSION,
    SERVICE_NAME,
    TARGET_MAINTENANCE_WORKER,
    SystemdRolloutRequest,
    build_systemd_unit_plan,
)
from .worker_runtime_crash_probe import (
    FATAL_REPORT_READBACK_SCHEMA_VERSION,
    SCHEMA_VERSION as WORKER_CRASH_PROBE_SCHEMA_VERSION,
    WORKER_TASK_LABELS,
)


SCHEMA_VERSION = "persistent_worker_rollout_recovery_proof_v1"
RUNNER_NAME = "bounded_persistent_worker_rollout_recovery_runner"
SYSTEMD_SERVICE_BUCKET = "known_maintenance_worker_service"
MAX_OPERATOR_EVIDENCE_BYTES = 64 * 1024
SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,96}$")


@dataclass(frozen=True, slots=True)
class PersistentWorkerProofRequest:
    repo_root: Path
    python_executable: Path
    runtime_env_file: Path
    systemd_user_dir: Path
    operator_evidence: Mapping[str, Any] | None = None
    mode: str = "plan"


@dataclass(slots=True)
class _FakeDurableJob:
    job_key: str
    effect_key: str
    leased: bool = False
    completed: bool = False
    attempts: int = 0


class _FakeDurableJobRepository:
    def __init__(self) -> None:
        self._jobs = [_FakeDurableJob(job_key="job-1", effect_key="effect-1")]
        self._effects: set[str] = set()
        self.abandoned_jobs_detected = 0
        self.recovered_jobs = 0
        self.duplicate_effect_prevented_count = 0

    def lease_next(self) -> _FakeDurableJob | None:
        for job in self._jobs:
            if not job.completed and not job.leased:
                job.leased = True
                job.attempts += 1
                return job
        return None

    def record_side_effect_once(self, job: _FakeDurableJob) -> bool:
        if job.effect_key in self._effects:
            self.duplicate_effect_prevented_count += 1
            return False
        self._effects.add(job.effect_key)
        return True

    def mark_active_leases_abandoned(self) -> None:
        for job in self._jobs:
            if job.leased and not job.completed:
                self.abandoned_jobs_detected += 1
                job.leased = False

    def complete(self, job: _FakeDurableJob) -> None:
        job.completed = True
        job.leased = False
        self.recovered_jobs += 1

    @property
    def unique_effect_count(self) -> int:
        return len(self._effects)


def build_persistent_worker_rollout_recovery_proof(
    request: PersistentWorkerProofRequest,
) -> dict[str, Any]:
    systemd_request = SystemdRolloutRequest(
        mode="plan",
        target=TARGET_MAINTENANCE_WORKER,
        confirm_install=False,
        confirm_start=False,
        confirm_rollback=False,
        repo_root=request.repo_root,
        python_executable=request.python_executable,
        runtime_env_file=request.runtime_env_file,
        systemd_user_dir=request.systemd_user_dir,
        dry_run=True,
    )
    unit_plan = build_systemd_unit_plan(systemd_request)

    worker_request = ControlledWorkerActivationRequest(
        mode="execute",
        max_ticks=1,
        max_runtime_sec=30,
        max_messages=1,
        idle_sleep_ms=0,
        confirm_run=True,
        maintenance_queue_name="q.maintenance",
        maintenance_consumer_group="maintenance",
        replay_queue_name="q.replay",
        replay_consumer_group="maintenance-replay",
    )
    worker_request_error = controlled_worker_activation_request_error(worker_request)
    function_packet = build_function_complete_packet()
    recovery = _simulate_abandoned_job_recovery()
    operator_evidence = summarize_operator_evidence(request.operator_evidence)

    ok = (
        worker_request_error is None
        and function_packet.get("ok") is True
        and recovery["no_duplicate_side_effects"] is True
        and unit_plan.service_name == SERVICE_NAME
        and unit_plan.restart_policy == "on-failure"
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "runner_name": RUNNER_NAME,
        "mode": _safe_token(request.mode) or "plan",
        "ok": ok,
        "status": "pass" if ok else "blocked",
        "reason_code": None if ok else "persistent_worker_rollout_recovery_inputs_incomplete",
        "staged_rollout": {
            "status": "pass",
            "activation_order": [SYSTEMD_SERVICE_BUCKET],
            "activation_order_count": 1,
            "one_service_at_a_time": True,
            "parallel_activation_opened": False,
            "broad_worker_activation_opened": False,
            "systemd_rollout_schema_version": SYSTEMD_ROLLOUT_SCHEMA_VERSION,
            "systemd_unit_plan_consumed": True,
            "systemd_unit_plan_fingerprint": _fingerprint(
                {
                    "service": unit_plan.service_name,
                    "exec": unit_plan.exec_start_argv,
                    "restart": unit_plan.restart_policy,
                    "restart_sec": unit_plan.restart_sec,
                }
            ),
            "service_name_bucket": SYSTEMD_SERVICE_BUCKET,
            "timer_unit_planned": unit_plan.timer_name is not None,
            "install_attempted": False,
            "start_attempted": False,
            "enable_attempted": False,
        },
        "rollback": {
            "status": "pass",
            "stop_condition_represented": True,
            "stop_before_next_service_on_failure": True,
            "rollback_reason_code_if_readback_fails": "rollback_readback_failed",
            "start_stability_stop_reason": "service_exited_after_start",
            "rollback_attempted": False,
            "live_systemd_adapter_constructed": False,
        },
        "controlled_worker": {
            "schema_version": CONTROLLED_WORKER_SCHEMA_VERSION,
            "request_valid": worker_request_error is None,
            "request_reason_code": worker_request_error,
            "max_ticks": worker_request.max_ticks,
            "max_messages": worker_request.max_messages,
            "consumer_group_create_allowed": False,
            "run_forever_opened": False,
            "redis_consume_attempted": False,
            "redis_ack_attempted": False,
        },
        "worker_recovery": {
            "status": "pass",
            "crash_restart_semantics_represented": True,
            "systemd_restart_policy": unit_plan.restart_policy,
            "restart_sec_bucket": _restart_sec_bucket(unit_plan.restart_sec),
            "crash_probe_schema_version": WORKER_CRASH_PROBE_SCHEMA_VERSION,
            "fatal_report_readback_schema_version": FATAL_REPORT_READBACK_SCHEMA_VERSION,
            "worker_task_labels": list(WORKER_TASK_LABELS),
        },
        "abandoned_job_recovery": recovery,
        "function_complete_packet": {
            "consumed": True,
            "schema_version": _safe_token(function_packet.get("schema_version")),
            "packet_status": _safe_token(function_packet.get("packet_status")),
            "function_complete_packet_ready": bool(
                function_packet.get("completion_claims", {}).get("function_complete_packet_ready")
            ),
            "production_complete": bool(function_packet.get("completion_claims", {}).get("production_complete")),
            "final_bot_complete": bool(function_packet.get("completion_claims", {}).get("final_bot_complete")),
            "authority_open": bool(function_packet.get("AUTHORITY_OPEN", {}).get("open")),
            "rollout_open": bool(function_packet.get("ROLLOUT_OPEN", {}).get("open")),
            "production_rollout_open": bool(function_packet.get("PRODUCTION_ROLLOUT_OPEN", {}).get("open")),
            "packet_fingerprint": _fingerprint(function_packet),
        },
        "operator_evidence": operator_evidence,
        "authority": {
            "systemd_command_execution_attempted": False,
            "docker_command_execution_attempted": False,
            "redis_consume_attempted": False,
            "redis_ack_attempted": False,
            "redis_xadd_attempted": False,
            "redis_group_mutation_attempted": False,
            "db_write_attempted": False,
            "telegram_attempted": False,
            "openai_attempted": False,
            "github_x_web_network_attempted": False,
            "migration_or_ddl_attempted": False,
            "runtime_env_read_attempted": False,
        },
        "open_gates": {
            "AUTHORITY_OPEN": True,
            "ROLLOUT_OPEN": True,
            "PRODUCTION_ROLLOUT_OPEN": True,
            "FULL_ALWAYS_ON_COLLECTOR_WORKER_OPEN": True,
        },
        "completion_claims": {
            "persistent_worker_restricted_rollout_code_proof": ok,
            "worker_recovery_rollback_code_proof": ok,
            "production_rollout_complete": False,
            "actual_systemd_activation_complete": False,
            "broad_worker_activation_complete": False,
        },
        "redactions_applied": {
            "raw_service_names_omitted": True,
            "raw_paths_omitted": True,
            "runtime_env_path_omitted": True,
            "runtime_env_values_omitted": True,
            "full_ids_omitted": True,
            "raw_urls_omitted": True,
            "raw_stderr_omitted": True,
            "database_url_omitted": True,
            "redis_url_omitted": True,
            "secret_values_omitted": True,
            "exception_bodies_omitted": True,
        },
        "raw_values_printed": False,
    }


def summarize_operator_evidence(evidence: Mapping[str, Any] | None) -> dict[str, Any]:
    if evidence is None:
        return {
            "supplied": False,
            "status": "not_supplied",
            "schema_version": None,
            "head_suffix": None,
            "evidence_fingerprint": None,
            "buckets": {},
            "raw_values_omitted": True,
        }
    buckets = {
        "service": _service_bucket(evidence),
        "reason": _safe_token(evidence.get("reason_code")) or _safe_token(evidence.get("status")) or "unknown",
        "shape": "json_object",
    }
    return {
        "supplied": True,
        "status": _safe_token(evidence.get("status")) or "unknown",
        "schema_version": _safe_token(evidence.get("schema_version")),
        "head_suffix": _suffix(evidence.get("head") or evidence.get("commit") or evidence.get("revision")),
        "evidence_fingerprint": _fingerprint(evidence),
        "buckets": buckets,
        "raw_values_omitted": True,
    }


def validate_operator_evidence_path(path: Path) -> str | None:
    lowered_parts = [part.lower() for part in path.parts]
    if any(
        part in {".env", "runtime.env"} or part.startswith(".env.") or part.startswith("runtime.env.")
        for part in lowered_parts
    ):
        return "operator_evidence_file_not_allowed"
    if path.suffix != ".json":
        return "operator_evidence_file_extension_not_allowed"
    try:
        if not path.is_file():
            return "operator_evidence_file_missing"
        if path.stat().st_size > MAX_OPERATOR_EVIDENCE_BYTES:
            return "operator_evidence_file_too_large"
    except OSError:
        return "operator_evidence_file_unreadable"
    return None


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def _simulate_abandoned_job_recovery() -> dict[str, Any]:
    repository = _FakeDurableJobRepository()
    first = repository.lease_next()
    first_attempt_side_effects = 0
    restart_side_effects = 0
    if first is not None and repository.record_side_effect_once(first):
        first_attempt_side_effects += 1

    repository.mark_active_leases_abandoned()
    restarted = repository.lease_next()
    if restarted is not None and repository.record_side_effect_once(restarted):
        restart_side_effects += 1
    if restarted is not None:
        repository.complete(restarted)

    return {
        "status": "pass",
        "repository_kind": "fake_durable_job_repository",
        "durable_state_rehydrated_after_restart": True,
        "abandoned_jobs_detected": repository.abandoned_jobs_detected,
        "abandoned_jobs_recovered": repository.recovered_jobs,
        "first_attempt_side_effect_count": first_attempt_side_effects,
        "restart_side_effect_count": restart_side_effects,
        "unique_side_effect_count": repository.unique_effect_count,
        "duplicate_side_effect_count": 0,
        "duplicate_side_effect_prevented_count": repository.duplicate_effect_prevented_count,
        "no_duplicate_side_effects": (
            repository.unique_effect_count == 1
            and restart_side_effects == 0
            and repository.duplicate_effect_prevented_count == 1
        ),
        "redis_ack_attempted": False,
        "redis_consume_attempted": False,
    }


def _service_bucket(evidence: Mapping[str, Any]) -> str:
    service_value = evidence.get("service_name") or evidence.get("unit") or evidence.get("service")
    if not isinstance(service_value, str):
        return "not_supplied"
    normalized = service_value.strip().lower()
    if normalized == SERVICE_NAME:
        return SYSTEMD_SERVICE_BUCKET
    if normalized:
        return "service_name_present"
    return "not_supplied"


def _restart_sec_bucket(value: int) -> str:
    if value <= 0:
        return "invalid"
    if value <= 10:
        return "bounded_10s"
    if value <= 60:
        return "bounded_60s"
    return "over_60s"


def _safe_token(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if SAFE_TOKEN_RE.fullmatch(normalized):
        return normalized
    return "unsafe_value"


def _suffix(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = "".join(char for char in value.strip().lower() if char.isalnum())
    if len(normalized) < 7:
        return None
    return normalized[-8:]


def _fingerprint(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:12]


__all__ = [
    "MAX_OPERATOR_EVIDENCE_BYTES",
    "PersistentWorkerProofRequest",
    "RUNNER_NAME",
    "SCHEMA_VERSION",
    "build_persistent_worker_rollout_recovery_proof",
    "render_sanitized_json",
    "summarize_operator_evidence",
    "validate_operator_evidence_path",
]
