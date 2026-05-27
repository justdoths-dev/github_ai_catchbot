from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse
from uuid import UUID


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_router_normalizer_candidate_path_readiness_probe"
REPORT_TYPE = "router_normalizer_candidate_path_readiness_probe_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_MAX_SOURCE_ROWS = 50
MAX_SOURCE_ROWS_HARD_LIMIT = 200

STATUS_CANDIDATE_FOUND = "router_normalizer_candidate_path_readiness_probe_candidate_found"
STATUS_NO_CANDIDATE_FOUND = "router_normalizer_candidate_path_readiness_probe_no_candidate_found"
STATUS_BLOCKED_NOT_READY = "blocked_router_normalizer_candidate_path_readiness_probe_not_ready"
STATUS_BLOCKED_SIDE_EFFECT = "blocked_forbidden_side_effect_detected"

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"

REQUIRED_TABLES = (
    "source_messages",
    "source_message_versions",
    "event_outbox",
    "normalization_runs",
    "normalization_suppression_traces",
    "artifact_registry",
    "artifact_observations",
    "candidate_group_proposals",
    "candidate_group_members",
)

# Deterministic scan order:
# 1. non-deleted current source rows first, because deleted rows are suppression-only;
# 2. newest posted_at, then last_seen_at, then first_seen_at;
# 3. source_message_id as the stable final tie-breaker.
# The LEFT JOIN includes the current source row and its current source version row
# when the requested version record exists.
SELECT_SOURCE_ROWS_QUERY = """
SELECT
    sm.source_message_id,
    sm.current_version_no,
    sm.text_body,
    sm.caption_text,
    sm.text_surface,
    sm.entities_json,
    sm.url_surface_json,
    sm.raw_message_json,
    sm.deleted_at,
    smv.version_no AS version_no,
    smv.text_surface AS version_text_surface,
    smv.entities_json AS version_entities_json,
    smv.raw_message_json AS version_raw_message_json
FROM source_messages sm
LEFT JOIN LATERAL (
    SELECT
        version_no,
        text_surface,
        entities_json,
        raw_message_json
    FROM source_message_versions
    WHERE source_message_id = sm.source_message_id
      AND version_no = sm.current_version_no
    ORDER BY observed_at DESC, source_message_version_id DESC
    LIMIT 1
) smv ON true
ORDER BY
    (sm.deleted_at IS NOT NULL) ASC,
    sm.posted_at DESC NULLS LAST,
    sm.last_seen_at DESC NULLS LAST,
    sm.first_seen_at DESC NULLS LAST,
    sm.source_message_id DESC
LIMIT :limit
"""

COUNT_EXISTING_NORMALIZATION_RUNS_QUERY = """
SELECT COUNT(*)
FROM normalization_runs
WHERE source_message_id = ANY(CAST(:source_message_ids AS uuid[]))
"""

COUNT_EXISTING_CANDIDATE_GROUPS_QUERY = """
SELECT COUNT(*)
FROM candidate_group_proposals
WHERE source_message_id = ANY(CAST(:source_message_ids AS uuid[]))
"""

COUNT_EXISTING_ENRICH_OUTBOX_QUERY = """
SELECT COUNT(*)
FROM event_outbox
WHERE event_type = 'artifact.enrich.requested.v1'
  AND payload_json->>'source_message_id' = ANY(CAST(:source_message_ids_text AS text[]))
"""

SIDE_EFFECT_REPORT_FIELDS = (
    "source_tables_mutation_performed",
    "normalizer_tables_mutation_performed",
    "event_outbox_mutation_performed",
    "redis_mutation_performed",
    "downstream_service_started",
    "external_network_attempted",
    "docker_or_systemd_changed",
    "alembic_run",
    "raw_values_emitted",
)


class AsyncSessionLike(Protocol):
    async def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> Any: ...

    async def rollback(self) -> None: ...

    async def close(self) -> None: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
ShortUrlResolverFactory = Callable[[], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class SourceRowForPlanning:
    current_snapshot: Any
    version_snapshot: Any | None


@dataclass(frozen=True, slots=True)
class PlanningResult:
    signal_detected: bool
    candidate_eligible: bool
    suppression_only: bool
    artifact_count: int
    candidate_group_count: int
    has_github_route: bool
    has_x_route: bool
    has_web_route: bool
    text_idea_only: bool
    raw_artifacts: tuple[Any, ...]


def _repo_root_for_imports() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = _repo_root_for_imports()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.router_normalizer.canonicalizer import (  # noqa: E402
    build_text_idea_artifact,
    canonicalize_resolved_urls,
)
from src.services.router_normalizer.models import ResolvedUrl, SourceMessageSnapshot  # noqa: E402
from src.services.router_normalizer.service import _with_inferred_repo_anchors  # noqa: E402
from src.services.router_normalizer.text_surfaces import build_text_surfaces  # noqa: E402
from src.services.router_normalizer.trigger_rules import evaluate_triggers  # noqa: E402
from src.services.router_normalizer.url_extraction import extract_urls  # noqa: E402


class _DefaultDatabaseSession:
    def __init__(self, engine: Any, session: Any) -> None:
        self._engine = engine
        self._session = session

    async def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._session.execute(statement, params or {})

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()
        await self._engine.dispose()


class _NoNetworkShortUrlResolver:
    async def resolve(self, url: Any) -> ResolvedUrl:
        return ResolvedUrl(
            observed_url=url.observed_url,
            normalized_url=_strip_url_fragment(url.observed_url),
            resolved_url=None,
            source_kind=url.source_kind,
            context_path=url.context_path,
            resolution_status="network_disabled",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only dedicated-VPS router-normalizer candidate path readiness "
            "probe. It scans a bounded recent source_messages window, runs "
            "deterministic normalizer planning with short URL network disabled, "
            "and emits only sanitized bucketed readiness fields."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument(
        "--max-source-rows",
        type=_bounded_positive_int_named(
            "max-source-rows",
            upper_bound=MAX_SOURCE_ROWS_HARD_LIMIT,
        ),
        default=DEFAULT_MAX_SOURCE_ROWS,
    )
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def _bounded_positive_int_named(field_name: str, *, upper_bound: int) -> Callable[[str], int]:
    def parse(raw: str) -> int:
        try:
            value = int(raw)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be a positive integer"
            ) from exc
        if value <= 0 or value > upper_bound:
            raise argparse.ArgumentTypeError(
                f"{field_name} must be between 1 and {upper_bound}"
            )
        return value

    return parse


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_BLOCKED_NOT_READY,
        "checks_failed": [],
        "runtime_env_read": False,
        "database_connected": False,
        "read_only_transaction": False,
        "source_rows_scanned_bucket": "zero",
        "source_versions_scanned_bucket": "zero",
        "candidate_eligible_rows_bucket": "zero",
        "signal_detected_rows_bucket": "zero",
        "suppression_only_rows_bucket": "zero",
        "planned_artifacts_bucket": "zero",
        "planned_candidate_groups_bucket": "zero",
        "planned_github_route_bucket": "zero",
        "planned_x_route_bucket": "zero",
        "planned_web_route_bucket": "zero",
        "planned_text_idea_bucket": "zero",
        "existing_recent_normalization_runs_bucket": "zero",
        "existing_recent_candidate_groups_bucket": "zero",
        "existing_recent_enrich_outbox_bucket": "zero",
        "source_tables_mutation_performed": False,
        "normalizer_tables_mutation_performed": False,
        "event_outbox_mutation_performed": False,
        "redis_mutation_performed": False,
        "downstream_service_started": False,
        "external_network_attempted": False,
        "docker_or_systemd_changed": False,
        "alembic_run": False,
        "raw_values_emitted": False,
    }


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def parse_runtime_env_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        values[key] = _strip_optional_quotes(raw_value)
    return values


def parse_runtime_env_file(path: str | Path) -> dict[str, str]:
    return parse_runtime_env_text(Path(path).read_text(encoding="utf-8", errors="replace"))


def _strip_optional_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _read_runtime_env(
    path: str | Path,
    runtime_env_reader: RuntimeEnvReader | None,
) -> Mapping[str, str]:
    if runtime_env_reader is not None:
        return runtime_env_reader(path)
    return parse_runtime_env_file(path)


def _database_url_is_supported(database_url: str) -> bool:
    scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", database_url)
    if not scheme_match:
        return False
    scheme = scheme_match.group(1).lower()
    return scheme == "postgresql" or scheme.startswith("postgresql+")


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"on", "true", "1", "yes"}


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def _sql(statement: str) -> Any:
    from sqlalchemy import text  # type: ignore[import-not-found]

    return text(statement)


async def _open_default_database_session(database_url: str) -> AsyncSessionLike:
    from sqlalchemy.ext.asyncio import (  # type: ignore[import-not-found]
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return _DefaultDatabaseSession(engine, session_factory())


async def _open_database_session(
    database_url: str,
    database_session_factory: DatabaseSessionFactory | None,
) -> AsyncSessionLike:
    if database_session_factory is not None:
        return await _maybe_await(database_session_factory(database_url))
    return await _open_default_database_session(database_url)


async def _close_database_session(session: AsyncSessionLike | None) -> None:
    if session is not None:
        await _maybe_await(session.close())


async def _execute(
    session: AsyncSessionLike,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    return await session.execute(_sql(statement), params or {})


def _first_mapping(result: Any) -> Mapping[str, Any] | None:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "first"):
            return mappings.first()
        if hasattr(mappings, "all"):
            rows = list(mappings.all())
            return rows[0] if rows else None
    if hasattr(result, "fetchall"):
        rows = list(result.fetchall())
        return rows[0] if rows else None
    if isinstance(result, list):
        return result[0] if result else None
    return None


def _all_mappings(result: Any) -> list[Mapping[str, Any]]:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "all"):
            return list(mappings.all())
        if hasattr(mappings, "first"):
            row = mappings.first()
            return [] if row is None else [row]
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if isinstance(result, list):
        return list(result)
    first = _first_mapping(result)
    return [] if first is None else [first]


async def _scalar(result: Any) -> Any:
    if hasattr(result, "scalar_one_or_none"):
        return result.scalar_one_or_none()
    if hasattr(result, "scalar"):
        return result.scalar()
    row = _first_mapping(result)
    if not row:
        return None
    if hasattr(row, "_mapping"):
        return next(iter(row._mapping.values()))
    if isinstance(row, Mapping):
        return next(iter(row.values()))
    if isinstance(row, (tuple, list)):
        return row[0] if row else None
    return row


async def _check_required_tables(session: AsyncSessionLike) -> bool:
    for table in REQUIRED_TABLES:
        available = bool(
            await _scalar(
                await _execute(
                    session,
                    TABLE_AVAILABLE_QUERY,
                    {"qualified_table_name": f"public.{table}"},
                )
            )
        )
        if not available:
            return False
    return True


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _coerce_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _coerce_optional_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return None


async def _load_source_rows(
    *,
    session: AsyncSessionLike,
    max_source_rows: int,
) -> list[SourceRowForPlanning]:
    result = await _execute(session, SELECT_SOURCE_ROWS_QUERY, {"limit": max_source_rows})
    rows: list[SourceRowForPlanning] = []
    for row in _all_mappings(result):
        current_snapshot = SourceMessageSnapshot(
            source_message_id=_coerce_uuid(row["source_message_id"]),
            source_version_no=int(row["current_version_no"]),
            text_body=row["text_body"],
            caption_text=row["caption_text"],
            text_surface=row["text_surface"],
            entities_json=_json_loads(row["entities_json"]),
            url_surface_json=_json_loads(row["url_surface_json"]),
            raw_message_json=_json_loads(row["raw_message_json"]) or {},
            deleted_at=_coerce_optional_datetime(row["deleted_at"]),
        )
        version_snapshot = None
        if row.get("version_no") is not None:
            version_snapshot = SourceMessageSnapshot(
                source_message_id=_coerce_uuid(row["source_message_id"]),
                source_version_no=int(row["version_no"]),
                text_body=None,
                caption_text=None,
                text_surface=row["version_text_surface"],
                entities_json=_json_loads(row["version_entities_json"]),
                url_surface_json=None,
                raw_message_json=_json_loads(row["version_raw_message_json"]) or {},
                deleted_at=None,
            )
        rows.append(SourceRowForPlanning(current_snapshot=current_snapshot, version_snapshot=version_snapshot))
    return rows


async def _count_existing_rows_for_sources(
    *,
    session: AsyncSessionLike,
    source_message_ids: Sequence[UUID],
) -> tuple[int, int, int]:
    if not source_message_ids:
        return (0, 0, 0)
    source_ids = [str(source_id) for source_id in source_message_ids]
    normalization_runs = int(
        await _scalar(
            await _execute(
                session,
                COUNT_EXISTING_NORMALIZATION_RUNS_QUERY,
                {"source_message_ids": source_ids},
            )
        )
        or 0
    )
    candidate_groups = int(
        await _scalar(
            await _execute(
                session,
                COUNT_EXISTING_CANDIDATE_GROUPS_QUERY,
                {"source_message_ids": source_ids},
            )
        )
        or 0
    )
    enrich_outbox = int(
        await _scalar(
            await _execute(
                session,
                COUNT_EXISTING_ENRICH_OUTBOX_QUERY,
                {"source_message_ids_text": source_ids},
            )
        )
        or 0
    )
    return (normalization_runs, candidate_groups, enrich_outbox)


def _snapshot_for_planning(row: SourceRowForPlanning) -> Any:
    if row.current_snapshot.deleted_at is not None:
        return row.current_snapshot
    if row.version_snapshot is not None:
        return row.version_snapshot
    return row.current_snapshot


async def _build_planning_result(
    snapshot: Any,
    resolver_factory: ShortUrlResolverFactory | None,
) -> PlanningResult:
    if snapshot.deleted_at is not None:
        return PlanningResult(
            signal_detected=False,
            candidate_eligible=False,
            suppression_only=True,
            artifact_count=0,
            candidate_group_count=0,
            has_github_route=False,
            has_x_route=False,
            has_web_route=False,
            text_idea_only=False,
            raw_artifacts=(),
        )

    surfaces = build_text_surfaces(snapshot)
    extracted_urls = extract_urls(snapshot, surfaces)
    resolver = resolver_factory() if resolver_factory is not None else _NoNetworkShortUrlResolver()
    resolved_urls = [await _maybe_await(resolver.resolve(url)) for url in extracted_urls]
    artifacts = _with_inferred_repo_anchors(canonicalize_resolved_urls(resolved_urls))
    evaluation = evaluate_triggers(surfaces, artifacts)
    if evaluation.candidate_eligible and not artifacts:
        artifacts = [build_text_idea_artifact(surfaces)]

    provider_routes = {artifact.provider_route for artifact in artifacts}
    artifact_types = {artifact.artifact_type for artifact in artifacts}
    return PlanningResult(
        signal_detected=evaluation.signal_detected,
        candidate_eligible=evaluation.candidate_eligible,
        suppression_only=not evaluation.candidate_eligible and evaluation.signal_detected,
        artifact_count=len(artifacts) if evaluation.candidate_eligible else 0,
        candidate_group_count=_planned_candidate_group_count(artifacts)
        if evaluation.candidate_eligible
        else 0,
        has_github_route=evaluation.candidate_eligible and "github" in provider_routes,
        has_x_route=evaluation.candidate_eligible and "x" in provider_routes,
        has_web_route=evaluation.candidate_eligible and "web" in provider_routes,
        text_idea_only=evaluation.candidate_eligible and artifact_types == {"text_idea"},
        raw_artifacts=tuple(artifacts),
    )


def _planned_candidate_group_count(artifacts: Sequence[Any]) -> int:
    primary_ids: set[str] = set()
    for artifact in artifacts:
        if (
            artifact.artifact_type in {"github_subpath", "github_repo_page"}
            and artifact.inferred_repo is not None
        ):
            primary_ids.add(artifact.inferred_repo.canonical_id)
        else:
            primary_ids.add(artifact.canonical_id)
    return len(primary_ids)


def _bucket_count(count: int) -> str:
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    return "multiple"


def _apply_source_scan_to_report(
    *,
    report: dict[str, Any],
    rows: Sequence[SourceRowForPlanning],
    plans: Sequence[PlanningResult],
    existing_counts: tuple[int, int, int],
) -> None:
    report["source_rows_scanned_bucket"] = _bucket_count(len(rows))
    report["source_versions_scanned_bucket"] = _bucket_count(
        sum(1 for row in rows if row.version_snapshot is not None)
    )
    report["candidate_eligible_rows_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.candidate_eligible)
    )
    report["signal_detected_rows_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.signal_detected)
    )
    report["suppression_only_rows_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.suppression_only)
    )
    report["planned_artifacts_bucket"] = _bucket_count(
        sum(plan.artifact_count for plan in plans)
    )
    report["planned_candidate_groups_bucket"] = _bucket_count(
        sum(plan.candidate_group_count for plan in plans)
    )
    report["planned_github_route_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.has_github_route)
    )
    report["planned_x_route_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.has_x_route)
    )
    report["planned_web_route_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.has_web_route)
    )
    report["planned_text_idea_bucket"] = _bucket_count(
        sum(1 for plan in plans if plan.text_idea_only)
    )
    report["existing_recent_normalization_runs_bucket"] = _bucket_count(existing_counts[0])
    report["existing_recent_candidate_groups_bucket"] = _bucket_count(existing_counts[1])
    report["existing_recent_enrich_outbox_bucket"] = _bucket_count(existing_counts[2])


def _extract_runtime_config(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> str | None:
    database_url = str(values.get("DATABASE_URL", "")).strip()
    if database_url:
        raw_values.add(database_url)
    if not database_url:
        _set_status(report, STATUS_BLOCKED_NOT_READY, "database.url_missing")
        return None
    if not _database_url_is_supported(database_url):
        _set_status(report, STATUS_BLOCKED_NOT_READY, "database.url_unsupported")
        return None
    return database_url


def _apply_side_effect_flags(
    report: dict[str, Any],
    side_effect_flags: Mapping[str, bool] | None,
) -> None:
    if not side_effect_flags:
        return
    for field in SIDE_EFFECT_REPORT_FIELDS:
        if bool(side_effect_flags.get(field, False)):
            report[field] = True


def _forbidden_side_effect_detected(report: Mapping[str, Any]) -> bool:
    return any(bool(report[field]) for field in SIDE_EFFECT_REPORT_FIELDS)


def _collect_raw_values_from_source_row(row: SourceRowForPlanning, raw_values: set[str]) -> None:
    _collect_raw_values_from_source_snapshot(row.current_snapshot, raw_values)
    if row.version_snapshot is not None:
        _collect_raw_values_from_source_snapshot(row.version_snapshot, raw_values)


def _collect_raw_values_from_source_snapshot(snapshot: Any, raw_values: set[str]) -> None:
    raw_values.add(str(snapshot.source_message_id))
    for value in (snapshot.text_body, snapshot.caption_text, snapshot.text_surface):
        if isinstance(value, str):
            raw_values.add(value)
            for url in re.findall(r"https?://[^\s<>'\")\]]+", value, flags=re.IGNORECASE):
                raw_values.add(url.rstrip(".,;:!?"))
    raw_values.add(json.dumps(snapshot.raw_message_json, sort_keys=True, default=str))
    raw_values.add(json.dumps(snapshot.entities_json, sort_keys=True, default=str))
    raw_values.add(json.dumps(snapshot.url_surface_json, sort_keys=True, default=str))


def _collect_raw_values_from_plan(plan: PlanningResult, raw_values: set[str]) -> None:
    for artifact in plan.raw_artifacts:
        for value in (
            artifact.canonical_id,
            artifact.canonical_url,
            artifact.observed_url,
            artifact.normalized_url,
            artifact.resolved_url,
        ):
            if isinstance(value, str):
                raw_values.add(value)


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(value in rendered for value in raw_values if len(value) >= 6)


def _strip_url_fragment(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    return parsed._replace(fragment="").geturl()


def _finalize_result(
    *,
    report: dict[str, Any],
    raw_values: set[str],
    exit_code: int,
) -> ScriptResult:
    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_BLOCKED_SIDE_EFFECT, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)
    return ScriptResult(exit_code=exit_code, report=report)


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_source_rows: int = DEFAULT_MAX_SOURCE_ROWS,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    short_url_resolver_factory: ShortUrlResolverFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    report = _base_report()
    _apply_side_effect_flags(report, side_effect_flags)
    raw_values: set[str] = {value for value in forbidden_raw_values if len(value) >= 6}
    raw_values.add(str(runtime_env_path))
    if _forbidden_side_effect_detected(report):
        _set_status(report, STATUS_BLOCKED_SIDE_EFFECT, "side_effect.forbidden")
        return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

    bounded_max_source_rows = max(1, min(int(max_source_rows), MAX_SOURCE_ROWS_HARD_LIMIT))
    session: AsyncSessionLike | None = None
    try:
        try:
            values = _read_runtime_env(runtime_env_path, runtime_env_reader)
            report["runtime_env_read"] = True
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "runtime_env.read")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        database_url = _extract_runtime_config(report=report, values=values, raw_values=raw_values)
        if database_url is None:
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        try:
            session = await _open_database_session(database_url, database_session_factory)
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.connection")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        try:
            await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
            read_only_value = await _scalar(await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY))
            report["read_only_transaction"] = _transaction_read_only_enabled(read_only_value)
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.read_only_transaction")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
        if not report["read_only_transaction"]:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.read_only_transaction")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        try:
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not await _check_required_tables(session):
                _set_status(report, STATUS_BLOCKED_NOT_READY, "database.required_tables")
                return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

            rows = await _load_source_rows(
                session=session,
                max_source_rows=bounded_max_source_rows,
            )
            for row in rows:
                _collect_raw_values_from_source_row(row, raw_values)
            existing_counts = await _count_existing_rows_for_sources(
                session=session,
                source_message_ids=[row.current_snapshot.source_message_id for row in rows],
            )
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "database.connection_or_schema")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        plans: list[PlanningResult] = []
        try:
            for row in rows:
                plan = await _build_planning_result(
                    _snapshot_for_planning(row),
                    short_url_resolver_factory,
                )
                _collect_raw_values_from_plan(plan, raw_values)
                plans.append(plan)
        except Exception:
            _set_status(report, STATUS_BLOCKED_NOT_READY, "planning.failed")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        _apply_source_scan_to_report(
            report=report,
            rows=rows,
            plans=plans,
            existing_counts=existing_counts,
        )

        if _forbidden_side_effect_detected(report):
            _set_status(report, STATUS_BLOCKED_SIDE_EFFECT, "side_effect.forbidden")
            return _finalize_result(report=report, raw_values=raw_values, exit_code=1)

        if any(plan.candidate_eligible for plan in plans):
            _set_status(report, STATUS_CANDIDATE_FOUND)
        else:
            _set_status(report, STATUS_NO_CANDIDATE_FOUND)
        return _finalize_result(report=report, raw_values=raw_values, exit_code=0)
    except Exception:
        if session is not None:
            await _maybe_await(session.rollback())
        _set_status(report, STATUS_BLOCKED_NOT_READY, "unexpected")
        return _finalize_result(report=report, raw_values=raw_values, exit_code=1)
    finally:
        if session is not None:
            await _maybe_await(session.rollback())
        await _close_database_session(session)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_source_rows: int = DEFAULT_MAX_SOURCE_ROWS,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    short_url_resolver_factory: ShortUrlResolverFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            max_source_rows=max_source_rows,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            short_url_resolver_factory=short_url_resolver_factory,
            side_effect_flags=side_effect_flags,
            forbidden_raw_values=forbidden_raw_values,
        )
    )


def render_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = generate_report(
        runtime_env_path=args.runtime_env_path,
        max_source_rows=args.max_source_rows,
    )
    print(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
