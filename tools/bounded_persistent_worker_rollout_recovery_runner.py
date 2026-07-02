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

from src.services.maintenance.persistent_worker_rollout_recovery import (  # noqa: E402
    PersistentWorkerProofRequest,
    build_persistent_worker_rollout_recovery_proof,
    render_sanitized_json,
    validate_operator_evidence_path,
)


class CliArgumentError(ValueError):
    pass


class BoundedPersistentWorkerProofError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class JsonOnlyArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise CliArgumentError("unsupported_cli_argument")


def build_parser() -> argparse.ArgumentParser:
    parser = JsonOnlyArgumentParser(
        description="Emit a sanitized persistent-worker rollout and recovery proof without live runtime authority.",
        add_help=False,
    )
    parser.add_argument("--mode", choices=("plan", "proof"), default="plan")
    parser.add_argument("--operator-evidence-json")
    parser.add_argument("--allow-operator-evidence-read", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
    except CliArgumentError as exc:
        sys.stdout.write(render_sanitized_json(_blocked(str(exc), mode="argument_error")))
        return 1

    try:
        evidence = _load_operator_evidence(args.operator_evidence_json, bool(args.allow_operator_evidence_read))
        request = PersistentWorkerProofRequest(
            mode=str(args.mode),
            repo_root=REPO_ROOT.resolve(),
            python_executable=Path(sys.executable).resolve(),
            runtime_env_file=Path("/tmp/github_ai_catchbot_worker_runtime.env"),
            systemd_user_dir=Path("/tmp/github_ai_catchbot_user_systemd"),
            operator_evidence=evidence,
        )
        report = build_persistent_worker_rollout_recovery_proof(request)
        report["authority"]["operator_evidence_file_read_allowed"] = bool(args.allow_operator_evidence_read)
        report["authority"]["operator_evidence_file_read_attempted"] = bool(args.operator_evidence_json)
        sys.stdout.write(render_sanitized_json(report))
        return 0 if report["ok"] is True else 1
    except BoundedPersistentWorkerProofError as exc:
        sys.stdout.write(render_sanitized_json(_blocked(exc.reason_code, mode=str(args.mode))))
        return 1
    except Exception:
        sys.stdout.write(render_sanitized_json(_blocked("runner_error", mode=str(args.mode))))
        return 1


def _load_operator_evidence(path_text: str | None, allowed: bool) -> dict[str, Any] | None:
    if not path_text:
        return None
    if not allowed:
        raise BoundedPersistentWorkerProofError("operator_evidence_file_read_not_allowed")
    path = Path(path_text)
    path_error = validate_operator_evidence_path(path)
    if path_error is not None:
        raise BoundedPersistentWorkerProofError(path_error)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, JSONDecodeError) as exc:
        raise BoundedPersistentWorkerProofError("operator_evidence_file_unreadable") from exc
    if not isinstance(payload, dict):
        raise BoundedPersistentWorkerProofError("operator_evidence_file_invalid_json_object")
    return payload


def _blocked(reason_code: str, *, mode: str) -> dict[str, Any]:
    return {
        "schema_version": "persistent_worker_rollout_recovery_proof_v1",
        "runner_name": "bounded_persistent_worker_rollout_recovery_runner",
        "mode": mode,
        "ok": False,
        "status": "blocked",
        "reason_code": reason_code,
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
        "redactions_applied": {
            "raw_service_names_omitted": True,
            "raw_paths_omitted": True,
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


if __name__ == "__main__":
    raise SystemExit(main())
