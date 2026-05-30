from __future__ import annotations

import argparse
import asyncio
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4


SCHEMA_VERSION = "1.0"
SCRIPT_NAME = "dedicated_vps_x_enricher_operator_approved_bounded_observation_snapshot_smoke"
REPORT_TYPE = "x_enricher_operator_approved_bounded_observation_snapshot_smoke_v1"
DEFAULT_RUNTIME_ENV_PATH = "/etc/github-ai-catchbot/runtime.env"
DEFAULT_MAX_STREAM_ENTRIES = 20
MAX_STREAM_ENTRIES_HARD_LIMIT = 100
DEFAULT_X_REQUEST_TIMEOUT_SEC = 10.0
EXPECTED_STREAM_NAME = "q.artifact.enrich.x"
EXPECTED_STAGE_NAME = "enrich_x"
EXPECTED_EVENT_TYPE = "artifact.enrich.requested.v1"
EXPECTED_PROVIDER_ROUTE = "x"
EXPECTED_ARTIFACT_TYPE = "x_post"
EXPECTED_SNAPSHOT_TYPE = "x_post"
DEFAULT_REFRESH_MODE = "standard"
DEFAULT_DEPTH_BUDGET = 1
ALLOWED_ROOT_OBJECT_TYPES = {"candidate_group", "artifact"}
REQUIRED_THIN_FIELDS = {
    "job_id",
    "stage_name",
    "root_object_type",
    "root_object_id",
    "idempotency_key",
    "pipeline_run_id",
    "not_before",
    "trigger_event_id",
}
FORBIDDEN_REDIS_FIELD_TOKENS = (
    "payload",
    "payload_json",
    "source",
    "raw",
    "text",
    "caption",
    "url",
    "message",
    "database_url",
    "redis_url",
    "secret",
    "token",
    "password",
    "api_key",
    "apikey",
)
PUBLIC_REPORT_LABEL_VALUES = {
    EXPECTED_EVENT_TYPE,
    EXPECTED_PROVIDER_ROUTE,
    EXPECTED_ARTIFACT_TYPE,
    EXPECTED_SNAPSHOT_TYPE,
    EXPECTED_STAGE_NAME,
    DEFAULT_REFRESH_MODE,
    "published",
    "candidate_group",
    "artifact",
    "ready",
    "partial_ready",
    "low_evidence",
    "access_denied",
    "request_invalid",
    "rate_limited",
    "failed_transient",
    "failed_permanent",
    "not_attempted",
    "not_applicable",
    "none",
    "zero",
    "one",
    "multiple",
    "present",
    "missing",
}
SIDE_EFFECT_REPORT_FIELDS = (
    "source_tables_mutation_performed",
    "telegram_raw_updates_mutation_performed",
    "registry_mutation_performed",
    "candidate_mutation_performed",
    "evidence_assembler_started",
    "judge_policy_notifier_started",
    "docker_or_systemd_changed",
    "alembic_run",
    "raw_values_emitted",
)

STATUS_READY = "x_enricher_operator_approved_bounded_observation_snapshot_smoke_ready"
STATUS_SNAPSHOT_WRITTEN = (
    "x_enricher_operator_approved_bounded_observation_snapshot_smoke_snapshot_written"
)
STATUS_MISSING_APPROVAL = (
    "blocked_x_enricher_operator_approved_bounded_observation_snapshot_smoke_missing_approval"
)
STATUS_INVALID_STREAM = (
    "blocked_x_enricher_operator_approved_bounded_observation_snapshot_smoke_invalid_stream"
)
STATUS_REHYDRATE_FAILED = (
    "blocked_x_enricher_operator_approved_bounded_observation_snapshot_smoke_rehydrate_failed"
)
STATUS_X_API_FAILED = (
    "blocked_x_enricher_operator_approved_bounded_observation_snapshot_smoke_x_api_failed"
)
STATUS_DB_WRITE_FAILED = (
    "blocked_x_enricher_operator_approved_bounded_observation_snapshot_smoke_db_write_failed"
)
STATUS_REDIS_ACK_FAILED = (
    "blocked_x_enricher_operator_approved_bounded_observation_snapshot_smoke_redis_ack_failed"
)
STATUS_FORBIDDEN_SIDE_EFFECT = "blocked_forbidden_side_effect_detected"

SET_TRANSACTION_READ_ONLY_QUERY = "SET TRANSACTION READ ONLY"
SHOW_TRANSACTION_READ_ONLY_QUERY = "SHOW transaction_read_only"
SELECT_ONE_QUERY = "SELECT 1"
TABLE_AVAILABLE_QUERY = "SELECT to_regclass(:qualified_table_name) IS NOT NULL"
SELECT_EVENT_OUTBOX_BY_ID_QUERY = """
SELECT
    event_id,
    event_type,
    aggregate_type,
    aggregate_id,
    dedupe_key,
    payload_json,
    status,
    created_at,
    published_at
FROM event_outbox
WHERE event_id = CAST(:event_id AS uuid)
LIMIT 1
"""
SELECT_ARTIFACT_BY_ID_QUERY = """
SELECT
    artifact_id,
    artifact_type,
    canonical_id,
    canonical_url,
    normalized_host,
    artifact_key_json,
    current_snapshot_id,
    current_status
FROM artifact_registry
WHERE artifact_id = CAST(:artifact_id AS uuid)
LIMIT 1
"""
COUNT_CANDIDATE_GROUP_ARTIFACT_MEMBERSHIP_QUERY = """
SELECT COUNT(*)
FROM candidate_group_proposals cgp
JOIN candidate_group_members cgm
  ON cgm.candidate_group_id = cgp.candidate_group_id
WHERE cgp.candidate_group_id = CAST(:candidate_group_id AS uuid)
  AND cgm.artifact_id = CAST(:artifact_id AS uuid)
"""
SELECT_ARTIFACT_CANDIDATE_MEMBERSHIPS_QUERY = """
SELECT cgm.candidate_group_id
FROM candidate_group_members cgm
JOIN candidate_group_proposals cgp
  ON cgp.candidate_group_id = cgm.candidate_group_id
WHERE cgm.artifact_id = CAST(:artifact_id AS uuid)
ORDER BY cgm.created_at DESC NULLS LAST, cgm.candidate_group_id DESC
LIMIT 2
"""
INSERT_ENRICHMENT_RUN_QUERY = """
INSERT INTO artifact_enrichment_runs (
    artifact_id,
    provider,
    refresh_mode,
    depth_budget,
    status,
    content_anchor,
    job_idempotency_key,
    requested_at,
    started_at
)
VALUES (
    CAST(:artifact_id AS uuid),
    'x',
    :refresh_mode,
    :depth_budget,
    'fetching'::snapshot_status_enum,
    NULL,
    :job_idempotency_key,
    now(),
    now()
)
ON CONFLICT (job_idempotency_key)
DO UPDATE SET
    status = 'fetching'::snapshot_status_enum,
    started_at = COALESCE(artifact_enrichment_runs.started_at, now())
RETURNING artifact_enrichment_run_id
"""
INSERT_ARTIFACT_SNAPSHOT_QUERY = """
INSERT INTO artifact_snapshots (
    artifact_id,
    provider,
    snapshot_type,
    status,
    fetched_at,
    content_anchor,
    auth_mode,
    normalized_projection,
    raw_payload_ref,
    evidence_limitations,
    fetch_anomalies
)
VALUES (
    CAST(:artifact_id AS uuid),
    'x',
    'x_post',
    CAST(:status AS snapshot_status_enum),
    now(),
    :content_anchor,
    'bearer_app_only',
    CAST(:normalized_projection AS jsonb),
    NULL,
    CAST(:evidence_limitations AS jsonb),
    CAST(:fetch_anomalies AS jsonb)
)
ON CONFLICT ON CONSTRAINT uq_artifact_snapshots_artifact_provider_anchor_type
DO UPDATE SET status = EXCLUDED.status
RETURNING snapshot_id
"""
UPSERT_ARTIFACT_SNAPSHOT_X_POST_QUERY = """
INSERT INTO artifact_snapshot_x_post (
    snapshot_id,
    post_id,
    content_anchor_post_version,
    author_summary_json,
    text_full,
    text_excerpt,
    conversation_id,
    referenced_post_ids_json,
    discovered_links_json,
    media_summary_json,
    metrics_summary_json
)
VALUES (
    CAST(:snapshot_id AS uuid),
    :post_id,
    :content_anchor_post_version,
    CAST(:author_summary_json AS jsonb),
    :text_full,
    :text_excerpt,
    :conversation_id,
    CAST(:referenced_post_ids_json AS jsonb),
    CAST(:discovered_links_json AS jsonb),
    CAST(:media_summary_json AS jsonb),
    CAST(:metrics_summary_json AS jsonb)
)
ON CONFLICT (snapshot_id) DO UPDATE SET
    content_anchor_post_version = EXCLUDED.content_anchor_post_version,
    author_summary_json = EXCLUDED.author_summary_json,
    text_full = EXCLUDED.text_full,
    text_excerpt = EXCLUDED.text_excerpt,
    conversation_id = EXCLUDED.conversation_id,
    referenced_post_ids_json = EXCLUDED.referenced_post_ids_json,
    discovered_links_json = EXCLUDED.discovered_links_json,
    media_summary_json = EXCLUDED.media_summary_json,
    metrics_summary_json = EXCLUDED.metrics_summary_json
"""
INSERT_DISCOVERED_URL_OBSERVATION_QUERY = """
INSERT INTO discovered_url_observations (
    parent_candidate_group_id,
    parent_artifact_id,
    parent_snapshot_id,
    observed_url,
    context_path,
    discovery_reason,
    depth_remaining,
    created_at
)
VALUES (
    CAST(:parent_candidate_group_id AS uuid),
    CAST(:parent_artifact_id AS uuid),
    CAST(:parent_snapshot_id AS uuid),
    :observed_url,
    :context_path,
    :discovery_reason,
    :depth_remaining,
    now()
)
"""
UPDATE_ARTIFACT_REGISTRY_CURRENT_SNAPSHOT_QUERY = """
UPDATE artifact_registry
SET current_snapshot_id = CAST(:snapshot_id AS uuid),
    current_status = CAST(:status AS snapshot_status_enum),
    updated_at = now()
WHERE artifact_id = CAST(:artifact_id AS uuid)
"""
INSERT_SNAPSHOT_UPDATED_OUTBOX_QUERY = """
INSERT INTO event_outbox (
    event_type,
    aggregate_type,
    aggregate_id,
    dedupe_key,
    payload_json,
    status,
    created_at
)
VALUES (
    'artifact.snapshot.updated.v1',
    'artifact',
    CAST(:artifact_id AS uuid),
    :dedupe_key,
    CAST(:payload_json AS jsonb),
    'pending'::outbox_status_enum,
    now()
)
ON CONFLICT (dedupe_key) DO NOTHING
"""
FINISH_ENRICHMENT_RUN_QUERY = """
UPDATE artifact_enrichment_runs
SET status = CAST(:status AS snapshot_status_enum),
    content_anchor = :content_anchor,
    finished_at = now()
WHERE artifact_enrichment_run_id = CAST(:artifact_enrichment_run_id AS uuid)
"""
REQUIRED_TABLES = (
    "event_outbox",
    "artifact_registry",
    "candidate_group_proposals",
    "candidate_group_members",
    "artifact_enrichment_runs",
    "artifact_snapshots",
    "artifact_snapshot_x_post",
    "discovered_url_observations",
)
X_TWEET_FIELDS = (
    "id",
    "text",
    "author_id",
    "created_at",
    "conversation_id",
    "edit_history_tweet_ids",
    "referenced_tweets",
    "entities",
    "public_metrics",
    "attachments",
)
X_EXPANSIONS = (
    "author_id",
    "attachments.media_keys",
    "referenced_tweets.id",
    "referenced_tweets.id.author_id",
    "referenced_tweets.id.attachments.media_keys",
    "edit_history_tweet_ids",
)
X_USER_FIELDS = ("id", "username", "name", "verified", "created_at", "public_metrics")
X_MEDIA_FIELDS = (
    "media_key",
    "type",
    "preview_image_url",
    "url",
    "alt_text",
    "duration_ms",
    "width",
    "height",
    "public_metrics",
)


class AsyncSessionLike(Protocol):
    async def execute(
        self,
        statement: Any,
        params: dict[str, Any] | None = None,
    ) -> Any: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def close(self) -> None: ...


class RedisClientLike(Protocol):
    async def ping(self) -> Any: ...

    async def xlen(self, name: str) -> Any: ...

    async def xrange(
        self,
        name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ) -> Any: ...

    async def xrevrange(
        self,
        name: str,
        max: str = "+",
        min: str = "-",
        count: int | None = None,
    ) -> Any: ...

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> Any: ...

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: int | None = None,
        block: int | None = None,
    ) -> Any: ...

    async def xack(self, name: str, groupname: str, *ids: str) -> Any: ...


RuntimeEnvReader = Callable[[str | Path], Mapping[str, str]]
DatabaseSessionFactory = Callable[[str], Any]
RedisClientFactory = Callable[[str], Any]
XApiClientFactory = Callable[["RuntimeConfig"], Any]


@dataclass(frozen=True, slots=True)
class ScriptResult:
    exit_code: int
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    database_url: str
    redis_url: str
    x_bearer_token: str
    x_api_base_url: str
    x_request_timeout_sec: float


@dataclass(frozen=True, slots=True)
class ObservationApprovals:
    approved_x_api_observation_smoke: bool
    approved_external_network: bool
    approved_x_api_read: bool
    approved_db_write: bool
    approved_artifact_enrichment_run_write: bool
    approved_artifact_snapshot_write: bool
    approved_discovered_url_observation_write: bool
    approved_event_outbox_write: bool
    approved_targeted_redis_ack: bool

    @property
    def all_granted(self) -> bool:
        return all(
            (
                self.approved_x_api_observation_smoke,
                self.approved_external_network,
                self.approved_x_api_read,
                self.approved_db_write,
                self.approved_artifact_enrichment_run_write,
                self.approved_artifact_snapshot_write,
                self.approved_discovered_url_observation_write,
                self.approved_event_outbox_write,
                self.approved_targeted_redis_ack,
            )
        )

    @property
    def any_granted(self) -> bool:
        return any(
            (
                self.approved_x_api_observation_smoke,
                self.approved_external_network,
                self.approved_x_api_read,
                self.approved_db_write,
                self.approved_artifact_enrichment_run_write,
                self.approved_artifact_snapshot_write,
                self.approved_discovered_url_observation_write,
                self.approved_event_outbox_write,
                self.approved_targeted_redis_ack,
            )
        )

    def missing_checks(self) -> list[str]:
        checks: list[str] = []
        if not self.approved_x_api_observation_smoke:
            checks.append("approval.x_api_observation_smoke")
        if not self.approved_external_network:
            checks.append("approval.external_network")
        if not self.approved_x_api_read:
            checks.append("approval.x_api_read")
        if not self.approved_db_write:
            checks.append("approval.db_write")
        if not self.approved_artifact_enrichment_run_write:
            checks.append("approval.artifact_enrichment_run_write")
        if not self.approved_artifact_snapshot_write:
            checks.append("approval.artifact_snapshot_write")
        if not self.approved_discovered_url_observation_write:
            checks.append("approval.discovered_url_observation_write")
        if not self.approved_event_outbox_write:
            checks.append("approval.event_outbox_write")
        if not self.approved_targeted_redis_ack:
            checks.append("approval.targeted_redis_ack")
        return checks


@dataclass(frozen=True, slots=True)
class StreamEntry:
    stream_id: str
    fields: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ThinPayloadContract:
    entry: StreamEntry
    shape_valid: bool
    stage_valid: bool
    root_valid: bool
    trigger_event_id: UUID | None
    root_object_type: str | None
    root_object_id: UUID | None
    checks_failed: list[str]

    @property
    def valid(self) -> bool:
        return self.shape_valid and self.stage_valid and self.root_valid


@dataclass(frozen=True, slots=True)
class EventRecord:
    event_id: UUID
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    dedupe_key: str
    payload_json: dict[str, Any]
    status: str


@dataclass(frozen=True, slots=True)
class EventContract:
    artifact_id: UUID
    candidate_group_id: UUID
    refresh_mode: str
    depth_budget: int


@dataclass(frozen=True, slots=True)
class ArtifactRecord:
    artifact_id: UUID
    artifact_type: str
    canonical_id: str
    canonical_url: Any
    normalized_host: Any
    artifact_key_json: Any
    current_snapshot_id: Any
    current_status: Any


@dataclass(frozen=True, slots=True)
class XApiRequestPlan:
    post_id: str
    endpoint_path: str
    tweet_fields: tuple[str, ...]
    expansions: tuple[str, ...]
    user_fields: tuple[str, ...]
    media_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class XApiResponse:
    status_code: int
    payload: dict[str, Any] | None
    malformed_json: bool = False
    network_error: bool = False


@dataclass(frozen=True, slots=True)
class SnapshotDraft:
    post_id: str
    status: str
    content_anchor: str
    author_summary_json: dict[str, Any] | None
    text_full: str | None
    text_excerpt: str | None
    conversation_id: str | None
    referenced_post_ids_json: list[str]
    discovered_links_json: list[dict[str, Any]]
    discovered_observations: list[dict[str, Any]]
    media_summary_json: list[dict[str, Any]]
    metrics_summary_json: dict[str, Any] | None
    normalized_projection: dict[str, Any]
    evidence_limitations: list[str]
    fetch_anomalies: list[str]


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

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()
        await self._engine.dispose()


class _DefaultXApiClient:
    def __init__(self, config: RuntimeConfig) -> None:
        self._base_url = config.x_api_base_url.rstrip("/")
        self._bearer_token = config.x_bearer_token
        self._timeout = config.x_request_timeout_sec

    async def get_post(self, plan: XApiRequestPlan) -> XApiResponse:
        return await asyncio.to_thread(self._get_post_sync, plan)

    async def close(self) -> None:
        return None

    def _get_post_sync(self, plan: XApiRequestPlan) -> XApiResponse:
        query = urllib.parse.urlencode(
            {
                "ids": plan.post_id,
                "tweet.fields": ",".join(plan.tweet_fields),
                "expansions": ",".join(plan.expansions),
                "user.fields": ",".join(plan.user_fields),
                "media.fields": ",".join(plan.media_fields),
            }
        )
        request = urllib.request.Request(
            f"{self._base_url}{plan.endpoint_path}?{query}",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._bearer_token}",
                "User-Agent": "github-ai-catchbot-x-observation-smoke",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                body = response.read().decode("utf-8", errors="replace")
                try:
                    payload = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    return XApiResponse(status_code=response.status, payload=None, malformed_json=True)
                return XApiResponse(
                    status_code=response.status,
                    payload=payload if isinstance(payload, dict) else {"data": payload},
                )
        except urllib.error.HTTPError as exc:
            return XApiResponse(status_code=exc.code, payload=None)
        except (urllib.error.URLError, TimeoutError):
            return XApiResponse(status_code=0, payload=None, network_error=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Operator-approved bounded X API observation snapshot smoke. Default mode "
            "rehydrates one q.artifact.enrich.x thin item, plans the X lookup and DB "
            "write, and emits sanitized JSON without network/write/ack."
        )
    )
    parser.add_argument("--runtime-env-path", default=DEFAULT_RUNTIME_ENV_PATH)
    parser.add_argument(
        "--max-stream-entries",
        type=_bounded_positive_int_named(
            "max-stream-entries",
            upper_bound=MAX_STREAM_ENTRIES_HARD_LIMIT,
        ),
        default=DEFAULT_MAX_STREAM_ENTRIES,
    )
    parser.add_argument("--format", choices=("json",), default="json")
    parser.add_argument("--approved-x-api-observation-smoke", action="store_true")
    parser.add_argument("--approved-external-network", action="store_true")
    parser.add_argument("--approved-x-api-read", action="store_true")
    parser.add_argument("--approved-db-write", action="store_true")
    parser.add_argument("--approved-artifact-enrichment-run-write", action="store_true")
    parser.add_argument("--approved-artifact-snapshot-write", action="store_true")
    parser.add_argument("--approved-discovered-url-observation-write", action="store_true")
    parser.add_argument("--approved-event-outbox-write", action="store_true")
    parser.add_argument("--approved-targeted-redis-ack", action="store_true")
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


def approvals_from_args(args: argparse.Namespace) -> ObservationApprovals:
    return ObservationApprovals(
        approved_x_api_observation_smoke=bool(args.approved_x_api_observation_smoke),
        approved_external_network=bool(args.approved_external_network),
        approved_x_api_read=bool(args.approved_x_api_read),
        approved_db_write=bool(args.approved_db_write),
        approved_artifact_enrichment_run_write=bool(args.approved_artifact_enrichment_run_write),
        approved_artifact_snapshot_write=bool(args.approved_artifact_snapshot_write),
        approved_discovered_url_observation_write=bool(args.approved_discovered_url_observation_write),
        approved_event_outbox_write=bool(args.approved_event_outbox_write),
        approved_targeted_redis_ack=bool(args.approved_targeted_redis_ack),
    )


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "report_type": REPORT_TYPE,
        "contract_status": STATUS_REHYDRATE_FAILED,
        "runtime_env_read": False,
        "database_connected": False,
        "redis_connected": False,
        "read_only_transaction": False,
        "x_stream_exists": False,
        "target_stream_entry_found_bucket": "zero",
        "thin_payload_valid_bucket": "zero",
        "event_outbox_rehydrate_succeeded_bucket": "zero",
        "artifact_registry_rehydrate_succeeded_bucket": "zero",
        "candidate_membership_valid_bucket": "zero",
        "x_bearer_token_present_bucket": "missing",
        "x_api_request_planned_bucket": "zero",
        "x_api_call_attempted": False,
        "x_api_call_succeeded_bucket": "zero",
        "x_api_status_bucket": "not_attempted",
        "x_api_result_class": "not_attempted",
        "targeted_stream_delivery_attempted": False,
        "targeted_stream_delivery_succeeded_bucket": "zero",
        "delivered_target_match_bucket": "zero",
        "database_write_attempted": False,
        "artifact_enrichment_run_written_bucket": "zero",
        "artifact_snapshot_written_bucket": "zero",
        "artifact_snapshot_x_post_written_bucket": "zero",
        "discovered_url_observations_written_bucket": "zero",
        "artifact_snapshot_updated_outbox_written_bucket": "zero",
        "artifact_registry_current_snapshot_updated_bucket": "zero",
        "redis_ack_attempted": False,
        "redis_ack_succeeded_bucket": "zero",
        "redis_ack_failure_class": "none",
        "source_tables_mutation_performed": False,
        "telegram_raw_updates_mutation_performed": False,
        "registry_mutation_performed": False,
        "candidate_mutation_performed": False,
        "evidence_assembler_started": False,
        "judge_policy_notifier_started": False,
        "external_network_attempted": False,
        "docker_or_systemd_changed": False,
        "alembic_run": False,
        "raw_values_emitted": False,
        "checks_failed": [],
    }


def _set_status(report: dict[str, Any], status: str, check: str | None = None) -> None:
    report["contract_status"] = status
    if check is not None and check not in report["checks_failed"]:
        report["checks_failed"].append(check)


def _bucket_count(count: int) -> str:
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    return "multiple"


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


def _redis_url_is_supported(redis_url: str) -> bool:
    scheme_match = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):", redis_url)
    if not scheme_match:
        return False
    return scheme_match.group(1).lower() in {"redis", "rediss", "unix"}


def _extract_runtime_config(
    *,
    report: dict[str, Any],
    values: Mapping[str, str],
    raw_values: set[str],
) -> RuntimeConfig | None:
    database_url = str(values.get("DATABASE_URL", "")).strip()
    redis_url = str(values.get("REDIS_URL", "")).strip()
    token = str(values.get("X_BEARER_TOKEN", "")).strip()
    x_api_base_url = str(
        values.get("X_API_BASE_URL", values.get("X_BASE_URL", "https://api.x.com"))
    ).strip()
    timeout_raw = str(values.get("X_ENRICHER_REQUEST_TIMEOUT_SEC", values.get("X_REQUEST_TIMEOUT_SEC", ""))).strip()
    for raw in (database_url, redis_url, token, x_api_base_url):
        if raw:
            raw_values.add(raw)
    if not database_url or not _database_url_is_supported(database_url):
        _set_status(report, STATUS_REHYDRATE_FAILED, "runtime.database_url")
        return None
    if not redis_url or not _redis_url_is_supported(redis_url):
        _set_status(report, STATUS_INVALID_STREAM, "runtime.redis_url")
        return None
    if not x_api_base_url.startswith("https://"):
        _set_status(report, STATUS_REHYDRATE_FAILED, "runtime.x_api_base_url")
        return None
    try:
        timeout_sec = float(timeout_raw) if timeout_raw else DEFAULT_X_REQUEST_TIMEOUT_SEC
    except ValueError:
        _set_status(report, STATUS_REHYDRATE_FAILED, "runtime.x_request_timeout")
        return None
    if timeout_sec <= 0:
        _set_status(report, STATUS_REHYDRATE_FAILED, "runtime.x_request_timeout")
        return None
    report["x_bearer_token_present_bucket"] = "present" if token else "missing"
    return RuntimeConfig(
        database_url=database_url,
        redis_url=redis_url,
        x_bearer_token=token,
        x_api_base_url=x_api_base_url,
        x_request_timeout_sec=timeout_sec,
    )


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


async def _open_default_redis_client(redis_url: str) -> RedisClientLike:
    from redis.asyncio import Redis  # type: ignore[import-not-found]

    return Redis.from_url(redis_url, decode_responses=True)


async def _open_redis_client(
    redis_url: str,
    redis_client_factory: RedisClientFactory | None,
) -> RedisClientLike:
    if redis_client_factory is not None:
        return await _maybe_await(redis_client_factory(redis_url))
    return await _open_default_redis_client(redis_url)


async def _open_x_api_client(
    runtime_config: RuntimeConfig,
    x_api_client_factory: XApiClientFactory | None,
) -> Any:
    if x_api_client_factory is not None:
        return await _maybe_await(x_api_client_factory(runtime_config))
    return _DefaultXApiClient(runtime_config)


async def _close_database_session(session: AsyncSessionLike | None) -> None:
    if session is not None:
        await _maybe_await(session.close())


async def _close_redis_client(redis_client: RedisClientLike | None) -> None:
    if redis_client is None:
        return
    close = getattr(redis_client, "aclose", None) or getattr(redis_client, "close", None)
    if close is not None:
        await _maybe_await(close())


async def _close_x_api_client(client: Any) -> None:
    if client is None:
        return
    close = getattr(client, "aclose", None) or getattr(client, "close", None)
    if close is not None:
        await _maybe_await(close())


async def _execute(
    session: AsyncSessionLike,
    statement: str,
    params: dict[str, Any] | None = None,
) -> Any:
    return await session.execute(_sql(statement), params or {})


async def _scalar(result: Any) -> Any:
    if hasattr(result, "scalar_one_or_none"):
        return result.scalar_one_or_none()
    if hasattr(result, "scalar"):
        return result.scalar()
    rows = _rows(result)
    if not rows:
        return None
    first = rows[0]
    if isinstance(first, (tuple, list)):
        return first[0] if first else None
    if hasattr(first, "_mapping"):
        return next(iter(first._mapping.values()))
    if isinstance(first, Mapping):
        return next(iter(first.values()))
    return first


def _rows(result: Any) -> list[Any]:
    if hasattr(result, "fetchall"):
        return list(result.fetchall())
    if hasattr(result, "mappings"):
        return list(result.mappings().all())
    if isinstance(result, list):
        return result
    return list(result)


def _first_mapping(result: Any) -> Mapping[str, Any] | None:
    if hasattr(result, "mappings"):
        mappings = result.mappings()
        if hasattr(mappings, "first"):
            row = mappings.first()
            return row
        rows = mappings.all()
        return rows[0] if rows else None
    rows = _rows(result)
    if not rows:
        return None
    first = rows[0]
    if hasattr(first, "_mapping"):
        return first._mapping
    if isinstance(first, Mapping):
        return first
    return None


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


def _transaction_read_only_enabled(raw_value: Any) -> bool:
    return str(raw_value).strip().lower() in {"on", "true", "1", "yes"}


def _safe_count(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _coerce_uuid(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _json_loads(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _jsonb_dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, default=str)


async def _inspect_target_stream(
    *,
    redis_client: RedisClientLike,
    max_stream_entries: int,
    report: dict[str, Any],
    raw_values: set[str],
) -> ThinPayloadContract | None:
    await _maybe_await(redis_client.ping())
    report["redis_connected"] = True
    stream_length = _safe_count(await _maybe_await(redis_client.xlen(EXPECTED_STREAM_NAME)))
    report["x_stream_exists"] = stream_length > 0
    if stream_length <= 0:
        _set_status(report, STATUS_INVALID_STREAM, "redis.x_stream_entry_missing")
        return None

    xrevrange = getattr(redis_client, "xrevrange", None)
    if xrevrange is not None:
        raw_entries = await _maybe_await(
            xrevrange(EXPECTED_STREAM_NAME, max="+", min="-", count=max_stream_entries)
        )
    else:
        raw_entries = await _maybe_await(
            redis_client.xrange(EXPECTED_STREAM_NAME, min="-", max="+", count=max_stream_entries)
        )
    entries = _decode_stream_entries(raw_entries)
    if not entries:
        _set_status(report, STATUS_INVALID_STREAM, "redis.x_stream_entry_missing")
        return None

    valid_contracts: list[ThinPayloadContract] = []
    valid_count = 0
    checks_failed: list[str] = []
    for entry in entries:
        raw_values.add(entry.stream_id)
        _collect_raw_values_from_stream_fields(entry.fields, raw_values)
        contract = _validate_thin_payload_entry(entry)
        checks_failed.extend(contract.checks_failed)
        if contract.valid:
            valid_count += 1
            valid_contracts.append(contract)

    report["thin_payload_valid_bucket"] = _bucket_count(valid_count)
    if len(valid_contracts) == 1:
        report["target_stream_entry_found_bucket"] = "one"
        return valid_contracts[0]
    if len(valid_contracts) > 1:
        _set_status(report, STATUS_INVALID_STREAM, "redis.target_stream_entry_duplicate")
        return None

    _set_status(report, STATUS_INVALID_STREAM)
    for check in checks_failed or ["redis.thin_payload"]:
        _set_status(report, report["contract_status"], check)
    return None


def _decode_stream_entries(raw_entries: Any) -> list[StreamEntry]:
    entries: list[StreamEntry] = []
    for message_id, fields in raw_entries or []:
        decoded_id = message_id.decode("utf-8") if isinstance(message_id, bytes) else str(message_id)
        entries.append(StreamEntry(stream_id=decoded_id, fields=_decode_fields(fields)))
    return entries


def _decode_fields(fields: Mapping[Any, Any]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in fields.items():
        decoded_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        decoded_value = value.decode("utf-8") if isinstance(value, bytes) else value
        decoded[decoded_key] = decoded_value
    return decoded


def _validate_thin_payload_entry(entry: StreamEntry) -> ThinPayloadContract:
    checks_failed: list[str] = []
    keys = set(entry.fields)
    shape_valid = keys == REQUIRED_THIN_FIELDS
    if not shape_valid:
        checks_failed.append("redis.thin_payload_shape")
    if any(_is_forbidden_redis_field(key) for key in keys):
        shape_valid = False
        checks_failed.append("redis.thin_payload_forbidden_field")

    for key in (
        "job_id",
        "stage_name",
        "root_object_type",
        "root_object_id",
        "idempotency_key",
        "trigger_event_id",
    ):
        if not str(entry.fields.get(key, "")).strip():
            shape_valid = False
            checks_failed.append(f"redis.{key}_missing")

    trigger_event_id: UUID | None = None
    root_object_id: UUID | None = None
    try:
        trigger_event_id = _coerce_uuid(entry.fields.get("trigger_event_id"))
    except (TypeError, ValueError):
        shape_valid = False
        checks_failed.append("redis.trigger_event_id_invalid")
    try:
        root_object_id = _coerce_uuid(entry.fields.get("root_object_id"))
    except (TypeError, ValueError):
        shape_valid = False
        checks_failed.append("redis.root_object_id_invalid")

    stage_valid = str(entry.fields.get("stage_name", "")) == EXPECTED_STAGE_NAME
    if not stage_valid:
        checks_failed.append("redis.stage_name_mismatch")
    root_object_type = str(entry.fields.get("root_object_type", ""))
    root_valid = root_object_type in ALLOWED_ROOT_OBJECT_TYPES and root_object_id is not None
    if root_object_type not in ALLOWED_ROOT_OBJECT_TYPES:
        checks_failed.append("redis.root_object_type_mismatch")

    return ThinPayloadContract(
        entry=entry,
        shape_valid=shape_valid,
        stage_valid=stage_valid,
        root_valid=root_valid,
        trigger_event_id=trigger_event_id,
        root_object_type=root_object_type if root_object_type else None,
        root_object_id=root_object_id,
        checks_failed=checks_failed,
    )


def _is_forbidden_redis_field(field_name: str) -> bool:
    lowered = field_name.lower()
    return any(token in lowered for token in FORBIDDEN_REDIS_FIELD_TOKENS)


def _collect_raw_values_from_stream_fields(fields: Mapping[str, Any], raw_values: set[str]) -> None:
    for key in (
        "job_id",
        "root_object_id",
        "idempotency_key",
        "pipeline_run_id",
        "not_before",
        "trigger_event_id",
    ):
        value = fields.get(key)
        if value is not None:
            raw_values.add(str(value))


async def _load_event_record(
    *,
    session: AsyncSessionLike,
    trigger_event_id: UUID,
    report: dict[str, Any],
    raw_values: set[str],
) -> EventRecord | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_EVENT_OUTBOX_BY_ID_QUERY,
            {"event_id": str(trigger_event_id)},
        )
    )
    if row is None:
        _set_status(report, STATUS_REHYDRATE_FAILED, "event_outbox.row_missing")
        return None
    payload = _json_loads(row["payload_json"]) or {}
    if not isinstance(payload, dict):
        payload = {}
    event = EventRecord(
        event_id=_coerce_uuid(row["event_id"]),
        event_type=str(row["event_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=_coerce_uuid(row["aggregate_id"]),
        dedupe_key=str(row["dedupe_key"]),
        payload_json=payload,
        status=str(row["status"]),
    )
    report["event_outbox_rehydrate_succeeded_bucket"] = "one"
    _collect_raw_values_from_event(event, raw_values)
    return event


async def _validate_event_contract(
    *,
    session: AsyncSessionLike,
    event: EventRecord,
    thin: ThinPayloadContract,
    report: dict[str, Any],
) -> EventContract | None:
    payload = event.payload_json
    if event.event_type != EXPECTED_EVENT_TYPE:
        _set_status(report, STATUS_REHYDRATE_FAILED, "event_outbox.event_type")
        return None
    if event.status != "published":
        _set_status(report, STATUS_REHYDRATE_FAILED, "event_outbox.status")
        return None
    if event.aggregate_type not in ALLOWED_ROOT_OBJECT_TYPES:
        _set_status(report, STATUS_REHYDRATE_FAILED, "event_outbox.aggregate_type")
        return None
    if thin.root_object_type != event.aggregate_type or thin.root_object_id != event.aggregate_id:
        _set_status(report, STATUS_REHYDRATE_FAILED, "event_outbox.thin_root_mismatch")
        return None
    if _payload_str(payload, "provider_route") != EXPECTED_PROVIDER_ROUTE:
        _set_status(report, STATUS_REHYDRATE_FAILED, "payload.provider_route")
        return None
    if _payload_str(payload, "artifact_type") != EXPECTED_ARTIFACT_TYPE:
        _set_status(report, STATUS_REHYDRATE_FAILED, "payload.artifact_type")
        return None

    artifact_id = _payload_uuid(payload, "artifact_id")
    if artifact_id is None:
        _set_status(report, STATUS_REHYDRATE_FAILED, "payload.artifact_id")
        return None
    if event.aggregate_type == "artifact" and event.aggregate_id != artifact_id:
        _set_status(report, STATUS_REHYDRATE_FAILED, "event_outbox.artifact_aggregate")
        return None

    refresh_mode = _payload_str(payload, "refresh_mode") or DEFAULT_REFRESH_MODE
    depth_budget = _payload_int(payload.get("depth_budget"), default=DEFAULT_DEPTH_BUDGET)
    if depth_budget != DEFAULT_DEPTH_BUDGET:
        _set_status(report, STATUS_REHYDRATE_FAILED, "payload.depth_budget")
        return None
    if not refresh_mode:
        _set_status(report, STATUS_REHYDRATE_FAILED, "payload.refresh_mode")
        return None

    candidate_group_id = await _resolve_candidate_group_id(
        session=session,
        event=event,
        artifact_id=artifact_id,
        payload_candidate_group_id=_payload_uuid(payload, "candidate_group_id"),
        report=report,
    )
    if candidate_group_id is None:
        return None

    return EventContract(
        artifact_id=artifact_id,
        candidate_group_id=candidate_group_id,
        refresh_mode=refresh_mode,
        depth_budget=depth_budget,
    )


async def _resolve_candidate_group_id(
    *,
    session: AsyncSessionLike,
    event: EventRecord,
    artifact_id: UUID,
    payload_candidate_group_id: UUID | None,
    report: dict[str, Any],
) -> UUID | None:
    if event.aggregate_type == "candidate_group":
        candidate_group_id = event.aggregate_id
        if payload_candidate_group_id is not None and payload_candidate_group_id != candidate_group_id:
            _set_status(report, STATUS_REHYDRATE_FAILED, "candidate.payload_candidate_group_mismatch")
            return None
        if await _candidate_group_contains_artifact(
            session=session,
            candidate_group_id=candidate_group_id,
            artifact_id=artifact_id,
        ):
            report["candidate_membership_valid_bucket"] = "one"
            return candidate_group_id
        _set_status(report, STATUS_REHYDRATE_FAILED, "candidate.membership")
        return None

    if payload_candidate_group_id is not None:
        if await _candidate_group_contains_artifact(
            session=session,
            candidate_group_id=payload_candidate_group_id,
            artifact_id=artifact_id,
        ):
            report["candidate_membership_valid_bucket"] = "one"
            return payload_candidate_group_id
        _set_status(report, STATUS_REHYDRATE_FAILED, "candidate.payload_candidate_group_mismatch")
        return None

    rows = _rows(
        await _execute(
            session,
            SELECT_ARTIFACT_CANDIDATE_MEMBERSHIPS_QUERY,
            {"artifact_id": str(artifact_id)},
        )
    )
    if len(rows) != 1:
        _set_status(report, STATUS_REHYDRATE_FAILED, "candidate.membership_cardinality")
        return None
    first = rows[0]
    if hasattr(first, "_mapping"):
        raw_id = first._mapping["candidate_group_id"]
    elif isinstance(first, Mapping):
        raw_id = first["candidate_group_id"]
    else:
        raw_id = first[0]
    candidate_group_id = _coerce_uuid(raw_id)
    report["candidate_membership_valid_bucket"] = "one"
    return candidate_group_id


async def _candidate_group_contains_artifact(
    *,
    session: AsyncSessionLike,
    candidate_group_id: UUID,
    artifact_id: UUID,
) -> bool:
    value = await _scalar(
        await _execute(
            session,
            COUNT_CANDIDATE_GROUP_ARTIFACT_MEMBERSHIP_QUERY,
            {
                "candidate_group_id": str(candidate_group_id),
                "artifact_id": str(artifact_id),
            },
        )
    )
    return _safe_count(value) > 0


async def _load_artifact_record(
    *,
    session: AsyncSessionLike,
    artifact_id: UUID,
    report: dict[str, Any],
    raw_values: set[str],
) -> ArtifactRecord | None:
    row = _first_mapping(
        await _execute(
            session,
            SELECT_ARTIFACT_BY_ID_QUERY,
            {"artifact_id": str(artifact_id)},
        )
    )
    if row is None:
        _set_status(report, STATUS_REHYDRATE_FAILED, "artifact_registry.row_missing")
        return None
    artifact = ArtifactRecord(
        artifact_id=_coerce_uuid(row["artifact_id"]),
        artifact_type=str(row["artifact_type"]),
        canonical_id=str(row["canonical_id"]),
        canonical_url=row["canonical_url"],
        normalized_host=row["normalized_host"],
        artifact_key_json=_json_loads(row["artifact_key_json"]),
        current_snapshot_id=row["current_snapshot_id"],
        current_status=row["current_status"],
    )
    report["artifact_registry_rehydrate_succeeded_bucket"] = "one"
    _collect_raw_values_from_artifact(artifact, raw_values)
    if artifact.artifact_type != EXPECTED_ARTIFACT_TYPE:
        _set_status(report, STATUS_REHYDRATE_FAILED, "artifact_registry.artifact_type")
        return None
    if extract_x_post_id_from_canonical_id(artifact.canonical_id) is None:
        _set_status(report, STATUS_REHYDRATE_FAILED, "artifact_registry.canonical_id")
        return None
    return artifact


def _payload_str(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _payload_uuid(payload: Mapping[str, Any], key: str) -> UUID | None:
    raw = _payload_str(payload, key)
    if raw is None:
        return None
    try:
        return _coerce_uuid(raw)
    except ValueError:
        return None


def _payload_int(value: Any, *, default: int) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def extract_x_post_id_from_canonical_id(canonical_id: str) -> str | None:
    if canonical_id.startswith("x:post:"):
        post_id = canonical_id.split("x:post:", 1)[1]
        return post_id or None
    if canonical_id.startswith("x_post:"):
        post_id = canonical_id.split("x_post:", 1)[1]
        return post_id or None
    return None


def _build_x_api_request_plan(*, artifact: ArtifactRecord) -> XApiRequestPlan | None:
    post_id = extract_x_post_id_from_canonical_id(artifact.canonical_id)
    if post_id is None:
        return None
    return XApiRequestPlan(
        post_id=post_id,
        endpoint_path="/2/tweets",
        tweet_fields=X_TWEET_FIELDS,
        expansions=X_EXPANSIONS,
        user_fields=X_USER_FIELDS,
        media_fields=X_MEDIA_FIELDS,
    )


def _classify_x_api_response(response: XApiResponse) -> tuple[str, str, bool]:
    if response.network_error:
        return "network_error", "failed_transient", False
    if response.malformed_json:
        return "malformed_json", "failed_transient", False
    status = response.status_code
    if 200 <= status < 300:
        return "2xx", "response_available", True
    if status in {401, 403}:
        return "401_403", "access_denied", False
    if status == 400:
        return "400", "request_invalid", False
    if status == 404:
        return "404", "failed_permanent", False
    if status == 429:
        return "429", "rate_limited", False
    if 400 <= status <= 499:
        return "4xx_other", "failed_permanent", False
    if 500 <= status <= 599:
        return "5xx", "failed_transient", False
    return "non_2xx", "failed_transient", False


def _build_snapshot_draft(
    *,
    response: XApiResponse,
    requested_post_id: str,
    candidate_group_id: UUID,
    artifact_id: UUID,
    depth_budget: int,
    raw_values: set[str],
) -> SnapshotDraft | None:
    payload = response.payload or {}
    data = payload.get("data") or []
    if isinstance(data, dict):
        data = [data]
    includes = payload.get("includes") or {}
    errors = payload.get("errors") or []
    root_post = next(
        (
            item
            for item in data
            if isinstance(item, dict) and str(item.get("id")) == requested_post_id
        ),
        None,
    )
    if root_post is None:
        return None
    raw_values.add(json.dumps(root_post, sort_keys=True, default=str))
    raw_values.add(requested_post_id)

    users = includes.get("users") or []
    media = includes.get("media") or []
    users_by_id = {
        str(user.get("id")): user
        for user in users
        if isinstance(user, dict) and user.get("id") is not None
    }
    media_by_key = {
        str(item.get("media_key")): item
        for item in media
        if isinstance(item, dict) and item.get("media_key") is not None
    }
    posts_by_id = {
        str(post.get("id")): post
        for post in data
        if isinstance(post, dict) and post.get("id") is not None
    }

    edit_ids = _usable_edit_history_ids(root_post)
    if not edit_ids:
        return None
    latest_edit_id = edit_ids[-1]
    fetch_anomalies: list[str] = []
    raw_values.add(latest_edit_id)
    content_anchor = f"xpost:{requested_post_id}:{latest_edit_id}"
    raw_values.add(content_anchor)

    text_full = _post_text(root_post)
    if text_full:
        raw_values.add(text_full)
    author_id = _as_str(root_post.get("author_id"))
    author = users_by_id.get(author_id) if author_id else None
    author_summary = _author_summary(author)
    referenced_items = root_post.get("referenced_tweets") or []
    referenced_ids: list[str] = []
    referenced_posts: list[dict[str, Any]] = []
    missing_references = False
    for item in referenced_items:
        if not isinstance(item, dict):
            continue
        ref_id = _as_str(item.get("id"))
        if not ref_id:
            continue
        raw_values.add(ref_id)
        referenced_ids.append(ref_id)
        ref_post = posts_by_id.get(ref_id)
        if ref_post is None:
            missing_references = True
        ref_text = _post_text(ref_post)
        if ref_text:
            raw_values.add(ref_text)
        referenced_posts.append(
            {
                "post_id": ref_id,
                "relation_type": _as_str(item.get("type")),
                "author_id": _as_str(ref_post.get("author_id")) if ref_post else None,
                "text_excerpt": _excerpt(ref_text, 280) if ref_post else None,
                "raw_post": ref_post,
            }
        )

    media_keys = _root_media_keys(root_post)
    media_summary = [_media_summary(media_by_key[key]) for key in media_keys if key in media_by_key]
    missing_media = bool(media_keys) and len(media_summary) < len(media_keys)

    normalized_projection = {
        "root_post": root_post,
        "referenced_posts": referenced_posts,
        "includes": {
            "users": users,
            "media": media,
        },
        "errors": errors,
        "depth_budget_applied": 1,
    }
    discovered_links_json, discovered_observations = _discover_urls(
        projection=normalized_projection,
        candidate_group_id=candidate_group_id,
        artifact_id=artifact_id,
        depth_remaining=max(0, depth_budget - 1),
        raw_values=raw_values,
    )

    evidence_limitations: list[str] = []
    status = "ready"
    if errors:
        fetch_anomalies.append("partial_errors_present")
        status = "partial_ready"
    if author_id and author is None:
        evidence_limitations.append("x_author_summary_missing")
        status = "partial_ready" if status == "ready" else status
    if missing_references:
        evidence_limitations.append("x_referenced_posts_missing")
        status = "partial_ready" if status == "ready" else status
    if missing_media:
        evidence_limitations.append("x_media_summary_missing")
        status = "partial_ready" if status == "ready" else status
    if not text_full:
        evidence_limitations.append("x_text_missing")
        status = "low_evidence"

    return SnapshotDraft(
        post_id=requested_post_id,
        status=status,
        content_anchor=content_anchor,
        author_summary_json=author_summary,
        text_full=text_full,
        text_excerpt=_excerpt(text_full, 500),
        conversation_id=_as_str(root_post.get("conversation_id")),
        referenced_post_ids_json=referenced_ids,
        discovered_links_json=discovered_links_json,
        discovered_observations=discovered_observations,
        media_summary_json=media_summary,
        metrics_summary_json=root_post.get("public_metrics") if isinstance(root_post.get("public_metrics"), dict) else None,
        normalized_projection=normalized_projection,
        evidence_limitations=evidence_limitations,
        fetch_anomalies=fetch_anomalies,
    )


def _post_text(post: dict[str, Any] | None) -> str | None:
    if not isinstance(post, dict):
        return None
    note_tweet = post.get("note_tweet")
    if isinstance(note_tweet, dict):
        note_text = note_tweet.get("text")
        if isinstance(note_text, str) and note_text.strip():
            return note_text.strip()
    text = post.get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    return None


def _usable_edit_history_ids(root_post: Mapping[str, Any]) -> list[str]:
    edit_history = root_post.get("edit_history_tweet_ids")
    if not isinstance(edit_history, list):
        return []
    edit_ids: list[str] = []
    for raw_id in edit_history:
        edit_id = _as_str(raw_id)
        if edit_id:
            edit_ids.append(edit_id)
    return edit_ids


def _root_post_edit_history_missing(response: XApiResponse, requested_post_id: str) -> bool:
    payload = response.payload or {}
    data = payload.get("data") or []
    if isinstance(data, dict):
        data = [data]
    for item in data:
        if not isinstance(item, dict) or str(item.get("id")) != requested_post_id:
            continue
        return not _usable_edit_history_ids(item)
    return False


def _root_media_keys(post: dict[str, Any]) -> list[str]:
    attachments = post.get("attachments") or {}
    if not isinstance(attachments, dict):
        return []
    keys = attachments.get("media_keys") or []
    return [str(key) for key in keys if key] if isinstance(keys, list) else []


def _author_summary(author: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(author, dict):
        return None
    return {
        "user_id": _as_str(author.get("id")),
        "username": _as_str(author.get("username")),
        "name": _as_str(author.get("name")),
        "verified": author.get("verified"),
        "created_at": _as_str(author.get("created_at")),
        "public_metrics": author.get("public_metrics") if isinstance(author.get("public_metrics"), dict) else None,
    }


def _media_summary(media: dict[str, Any]) -> dict[str, Any]:
    return {
        "media_key": _as_str(media.get("media_key")),
        "media_type": _as_str(media.get("type")),
        "preview_image_url": _as_str(media.get("preview_image_url")),
        "url": _as_str(media.get("url")),
        "alt_text": _as_str(media.get("alt_text")),
        "duration_ms": media.get("duration_ms"),
        "width": media.get("width"),
        "height": media.get("height"),
        "public_metrics": media.get("public_metrics") if isinstance(media.get("public_metrics"), dict) else None,
    }


def _excerpt(text: str | None, length: int) -> str | None:
    return text[:length] if text is not None else None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    value_str = str(value).strip()
    return value_str or None


def _discover_urls(
    *,
    projection: dict[str, Any],
    candidate_group_id: UUID,
    artifact_id: UUID,
    depth_remaining: int,
    raw_values: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    observations: list[dict[str, str]] = []
    root_post = projection.get("root_post")
    for idx, url in enumerate(_extract_urls_from_post(root_post)):
        observations.append(
            {
                "observed_url": url,
                "context_path": f"root_post.entities.urls[{idx}]",
                "source_kind": "x_entities",
            }
        )
    for ref_idx, ref in enumerate(projection.get("referenced_posts") or []):
        if not isinstance(ref, dict):
            continue
        raw_post = ref.get("raw_post")
        for url_idx, url in enumerate(_extract_urls_from_post(raw_post)):
            observations.append(
                {
                    "observed_url": url,
                    "context_path": f"referenced_posts[{ref_idx}].entities.urls[{url_idx}]",
                    "source_kind": "x_referenced_entities",
                }
            )

    links_json: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in observations:
        observed_url = item["observed_url"]
        context_path = item["context_path"]
        raw_values.add(observed_url)
        key = (observed_url, context_path)
        if key in seen:
            continue
        seen.add(key)
        links_json.append(
            {
                "source_kind": item["source_kind"],
                "context_path": context_path,
                "observed_url_present": True,
            }
        )
        rows.append(
            {
                "parent_candidate_group_id": str(candidate_group_id),
                "parent_artifact_id": str(artifact_id),
                "observed_url": observed_url,
                "context_path": context_path,
                "depth_remaining": depth_remaining,
            }
        )
    return links_json, rows


def _extract_urls_from_post(post: Any) -> list[str]:
    if not isinstance(post, dict):
        return []
    entities = post.get("entities") or {}
    if not isinstance(entities, dict):
        return []
    urls = entities.get("urls") or []
    result: list[str] = []
    for entry in urls:
        if not isinstance(entry, dict):
            continue
        expanded = entry.get("expanded_url")
        raw = entry.get("url")
        candidate = expanded if isinstance(expanded, str) and expanded.strip() else raw
        if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
            result.append(candidate.strip())
    return result


async def _deliver_target_stream_entry(
    *,
    redis_client: RedisClientLike,
    thin: ThinPayloadContract,
    report: dict[str, Any],
) -> tuple[str, str] | None:
    report["targeted_stream_delivery_attempted"] = True
    group_name = f"x-observation-smoke-{uuid4().hex[:16]}"
    consumer_name = f"{group_name}-1"
    group_start_id = _previous_stream_id(thin.entry.stream_id)
    await _maybe_await(
        redis_client.xgroup_create(
            EXPECTED_STREAM_NAME,
            group_name,
            id=group_start_id,
            mkstream=False,
        )
    )
    raw = await _maybe_await(
        redis_client.xreadgroup(
            group_name,
            consumer_name,
            {EXPECTED_STREAM_NAME: ">"},
            count=1,
            block=1000,
        )
    )
    delivered = _decode_xreadgroup_entries(raw)
    if len(delivered) != 1:
        _set_status(report, STATUS_INVALID_STREAM, "redis.target_delivery_count")
        return None
    delivered_entry = delivered[0]
    report["targeted_stream_delivery_succeeded_bucket"] = "one"
    delivered_contract = _validate_thin_payload_entry(delivered_entry)
    if delivered_entry.stream_id != thin.entry.stream_id or delivered_contract != thin:
        _set_status(report, STATUS_INVALID_STREAM, "redis.delivered_target_mismatch")
        return None
    report["delivered_target_match_bucket"] = "one"
    return group_name, consumer_name


def _previous_stream_id(stream_id: str) -> str:
    match = re.fullmatch(r"(\d+)-(\d+)", stream_id)
    if match is None:
        return "0-0"
    millis = int(match.group(1))
    seq = int(match.group(2))
    if seq > 0:
        return f"{millis}-{seq - 1}"
    if millis > 0:
        return f"{millis - 1}-18446744073709551615"
    return "0-0"


def _decode_xreadgroup_entries(raw: Any) -> list[StreamEntry]:
    entries: list[StreamEntry] = []
    for _stream_name, stream_entries in raw or []:
        entries.extend(_decode_stream_entries(stream_entries))
    return entries


async def _write_snapshot_transaction(
    *,
    session: AsyncSessionLike,
    event: EventRecord,
    contract: EventContract,
    artifact: ArtifactRecord,
    draft: SnapshotDraft,
    report: dict[str, Any],
) -> UUID:
    report["database_write_attempted"] = True
    run_key = f"enrich:x:{contract.artifact_id}:{event.event_id}:{contract.refresh_mode}:{contract.depth_budget}"
    run_id = await _scalar(
        await _execute(
            session,
            INSERT_ENRICHMENT_RUN_QUERY,
            {
                "artifact_id": str(contract.artifact_id),
                "refresh_mode": contract.refresh_mode,
                "depth_budget": contract.depth_budget,
                "job_idempotency_key": run_key,
            },
        )
    )
    report["artifact_enrichment_run_written_bucket"] = "one"
    snapshot_id = await _scalar(
        await _execute(
            session,
            INSERT_ARTIFACT_SNAPSHOT_QUERY,
            {
                "artifact_id": str(contract.artifact_id),
                "status": draft.status,
                "content_anchor": draft.content_anchor,
                "normalized_projection": _jsonb_dumps(draft.normalized_projection),
                "evidence_limitations": _jsonb_dumps(draft.evidence_limitations),
                "fetch_anomalies": _jsonb_dumps(draft.fetch_anomalies),
            },
        )
    )
    snapshot_uuid = _coerce_uuid(snapshot_id)
    report["artifact_snapshot_written_bucket"] = "one"
    await _execute(
        session,
        UPSERT_ARTIFACT_SNAPSHOT_X_POST_QUERY,
        {
            "snapshot_id": str(snapshot_uuid),
            "post_id": draft.post_id,
            "content_anchor_post_version": draft.content_anchor,
            "author_summary_json": _jsonb_dumps(draft.author_summary_json),
            "text_full": draft.text_full,
            "text_excerpt": draft.text_excerpt,
            "conversation_id": draft.conversation_id,
            "referenced_post_ids_json": _jsonb_dumps(draft.referenced_post_ids_json),
            "discovered_links_json": _jsonb_dumps(draft.discovered_links_json),
            "media_summary_json": _jsonb_dumps(draft.media_summary_json),
            "metrics_summary_json": _jsonb_dumps(draft.metrics_summary_json),
        },
    )
    report["artifact_snapshot_x_post_written_bucket"] = "one"
    for observation in draft.discovered_observations:
        await _execute(
            session,
            INSERT_DISCOVERED_URL_OBSERVATION_QUERY,
            {
                **observation,
                "parent_snapshot_id": str(snapshot_uuid),
                "discovery_reason": "x_post_embedded_link",
            },
        )
    report["discovered_url_observations_written_bucket"] = _bucket_count(
        len(draft.discovered_observations)
    )
    await _execute(
        session,
        UPDATE_ARTIFACT_REGISTRY_CURRENT_SNAPSHOT_QUERY,
        {
            "artifact_id": str(artifact.artifact_id),
            "snapshot_id": str(snapshot_uuid),
            "status": draft.status,
        },
    )
    report["artifact_registry_current_snapshot_updated_bucket"] = "one"
    await _execute(
        session,
        INSERT_SNAPSHOT_UPDATED_OUTBOX_QUERY,
        {
            "artifact_id": str(artifact.artifact_id),
            "dedupe_key": f"artifact:snapshot_updated:{artifact.artifact_id}:{snapshot_uuid}",
            "payload_json": _jsonb_dumps(
                {
                    "artifact_id": str(artifact.artifact_id),
                    "snapshot_id": str(snapshot_uuid),
                    "provider": "x",
                    "provider_route": "x",
                    "snapshot_type": EXPECTED_SNAPSHOT_TYPE,
                    "status": draft.status,
                    "content_anchor": draft.content_anchor,
                }
            ),
        },
    )
    report["artifact_snapshot_updated_outbox_written_bucket"] = "one"
    await _execute(
        session,
        FINISH_ENRICHMENT_RUN_QUERY,
        {
            "artifact_enrichment_run_id": str(run_id),
            "status": draft.status,
            "content_anchor": draft.content_anchor,
        },
    )
    await _maybe_await(session.commit())
    return snapshot_uuid


async def _ack_target(
    *,
    redis_client: RedisClientLike,
    group_name: str,
    stream_id: str,
    report: dict[str, Any],
) -> bool:
    report["redis_ack_attempted"] = True
    try:
        await _maybe_await(redis_client.xack(EXPECTED_STREAM_NAME, group_name, stream_id))
    except Exception:
        report["redis_ack_failure_class"] = "redis_ack_failed"
        _set_status(report, STATUS_REDIS_ACK_FAILED, "redis.ack")
        return False
    report["redis_ack_succeeded_bucket"] = "one"
    return True


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


def _collect_raw_values_from_event(event: EventRecord, raw_values: set[str]) -> None:
    raw_values.update(
        {
            str(event.event_id),
            str(event.aggregate_id),
            event.dedupe_key,
            json.dumps(event.payload_json, sort_keys=True, default=str),
        }
    )
    _collect_raw_json_values(event.payload_json, raw_values)


def _collect_raw_values_from_artifact(artifact: ArtifactRecord, raw_values: set[str]) -> None:
    raw_values.update(
        {
            str(artifact.artifact_id),
            artifact.canonical_id,
            str(artifact.canonical_url or ""),
            str(artifact.normalized_host or ""),
            json.dumps(artifact.artifact_key_json, sort_keys=True, default=str),
            str(artifact.current_snapshot_id or ""),
            str(artifact.current_status or ""),
        }
    )
    post_id = extract_x_post_id_from_canonical_id(artifact.canonical_id)
    if post_id is not None:
        raw_values.add(post_id)


def _collect_raw_json_values(value: Any, raw_values: set[str]) -> None:
    if isinstance(value, Mapping):
        for nested in value.values():
            _collect_raw_json_values(nested, raw_values)
        return
    if isinstance(value, list):
        for nested in value:
            _collect_raw_json_values(nested, raw_values)
        return
    if value is not None:
        raw_values.add(str(value))


def _report_contains_raw_values(report: Mapping[str, Any], raw_values: set[str]) -> bool:
    rendered = render_json(report)
    return any(
        value in rendered
        for value in raw_values
        if len(value) >= 6 and value not in PUBLIC_REPORT_LABEL_VALUES
    )


def _finish_result(
    *,
    exit_code: int,
    report: dict[str, Any],
    raw_values: set[str],
) -> ScriptResult:
    if _report_contains_raw_values(report, raw_values):
        report["raw_values_emitted"] = True
        _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "output.raw_values")
        return ScriptResult(exit_code=1, report=report)
    return ScriptResult(exit_code=exit_code, report=report)


async def generate_report_async(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_stream_entries: int = DEFAULT_MAX_STREAM_ENTRIES,
    approvals: ObservationApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    x_api_client_factory: XApiClientFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    approvals = approvals or ObservationApprovals(
        approved_x_api_observation_smoke=False,
        approved_external_network=False,
        approved_x_api_read=False,
        approved_db_write=False,
        approved_artifact_enrichment_run_write=False,
        approved_artifact_snapshot_write=False,
        approved_discovered_url_observation_write=False,
        approved_event_outbox_write=False,
        approved_targeted_redis_ack=False,
    )
    report = _base_report()
    raw_values: set[str] = {value for value in forbidden_raw_values if len(value) >= 6}
    _apply_side_effect_flags(report, side_effect_flags)
    if _forbidden_side_effect_detected(report):
        _set_status(report, STATUS_FORBIDDEN_SIDE_EFFECT, "side_effect.forbidden")
        return _finish_result(exit_code=1, report=report, raw_values=raw_values)

    if approvals.any_granted and not approvals.all_granted:
        _set_status(report, STATUS_MISSING_APPROVAL)
        for check in approvals.missing_checks():
            _set_status(report, STATUS_MISSING_APPROVAL, check)
        return _finish_result(exit_code=1, report=report, raw_values=raw_values)

    if max_stream_entries <= 0 or max_stream_entries > MAX_STREAM_ENTRIES_HARD_LIMIT:
        _set_status(report, STATUS_INVALID_STREAM, "max_stream_entries.out_of_bounds")
        return _finish_result(exit_code=1, report=report, raw_values=raw_values)

    session: AsyncSessionLike | None = None
    redis_client: RedisClientLike | None = None
    x_api_client: Any = None

    try:
        try:
            values = _read_runtime_env(runtime_env_path, runtime_env_reader)
            report["runtime_env_read"] = True
            raw_values.add(str(runtime_env_path))
        except Exception:
            _set_status(report, STATUS_REHYDRATE_FAILED, "runtime_env.read")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        runtime_config = _extract_runtime_config(
            report=report,
            values=values,
            raw_values=raw_values,
        )
        if runtime_config is None:
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        try:
            session = await _open_database_session(
                runtime_config.database_url,
                database_session_factory,
            )
            await _execute(session, SET_TRANSACTION_READ_ONLY_QUERY)
            read_only_value = await _scalar(
                await _execute(session, SHOW_TRANSACTION_READ_ONLY_QUERY)
            )
            report["read_only_transaction"] = _transaction_read_only_enabled(read_only_value)
            if not report["read_only_transaction"]:
                _set_status(report, STATUS_REHYDRATE_FAILED, "database.read_only_transaction")
                return _finish_result(exit_code=1, report=report, raw_values=raw_values)
            await _execute(session, SELECT_ONE_QUERY)
            report["database_connected"] = True
            if not await _check_required_tables(session):
                _set_status(report, STATUS_REHYDRATE_FAILED, "database.required_tables")
                return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        except Exception:
            _set_status(report, STATUS_REHYDRATE_FAILED, "database.connection_or_schema")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        try:
            redis_client = await _open_redis_client(runtime_config.redis_url, redis_client_factory)
            thin_contract = await _inspect_target_stream(
                redis_client=redis_client,
                max_stream_entries=max_stream_entries,
                report=report,
                raw_values=raw_values,
            )
        except Exception:
            _set_status(report, STATUS_INVALID_STREAM, "redis.connection_or_stream_inspection")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        if thin_contract is None:
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        if thin_contract.trigger_event_id is None:
            _set_status(report, STATUS_INVALID_STREAM, "redis.trigger_event_id_invalid")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        try:
            event = await _load_event_record(
                session=session,
                trigger_event_id=thin_contract.trigger_event_id,
                report=report,
                raw_values=raw_values,
            )
        except Exception:
            _set_status(report, STATUS_REHYDRATE_FAILED, "event_outbox.rehydrate")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        if event is None:
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        try:
            contract = await _validate_event_contract(
                session=session,
                event=event,
                thin=thin_contract,
                report=report,
            )
        except Exception:
            _set_status(report, STATUS_REHYDRATE_FAILED, "event_outbox.contract")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        if contract is None:
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        try:
            artifact = await _load_artifact_record(
                session=session,
                artifact_id=contract.artifact_id,
                report=report,
                raw_values=raw_values,
            )
        except Exception:
            _set_status(report, STATUS_REHYDRATE_FAILED, "artifact_registry.rehydrate")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        if artifact is None:
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        if not runtime_config.x_bearer_token:
            _set_status(report, STATUS_REHYDRATE_FAILED, "runtime.x_bearer_token_missing")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        plan = _build_x_api_request_plan(artifact=artifact)
        if plan is None:
            _set_status(report, STATUS_REHYDRATE_FAILED, "x_api.request_plan")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        report["x_api_request_planned_bucket"] = "one"
        raw_values.add(plan.post_id)

        if not approvals.all_granted:
            _set_status(report, STATUS_READY)
            return _finish_result(exit_code=0, report=report, raw_values=raw_values)

        await _maybe_await(session.rollback())

        delivery = await _deliver_target_stream_entry(
            redis_client=redis_client,
            thin=thin_contract,
            report=report,
        )
        if delivery is None:
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        group_name, _consumer_name = delivery

        try:
            x_api_client = await _open_x_api_client(runtime_config, x_api_client_factory)
            report["external_network_attempted"] = True
            report["x_api_call_attempted"] = True
            response = await _maybe_await(x_api_client.get_post(plan))
        except Exception:
            _set_status(report, STATUS_X_API_FAILED, "x_api.call")
            report["x_api_status_bucket"] = "network_error"
            report["x_api_result_class"] = "failed_transient"
            await _maybe_await(session.rollback())
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        status_bucket, result_class, transport_success = _classify_x_api_response(response)
        report["x_api_status_bucket"] = status_bucket
        report["x_api_result_class"] = result_class
        if not transport_success:
            _set_status(report, STATUS_X_API_FAILED, f"x_api.{status_bucket}")
            await _maybe_await(session.rollback())
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        draft = _build_snapshot_draft(
            response=response,
            requested_post_id=plan.post_id,
            candidate_group_id=contract.candidate_group_id,
            artifact_id=contract.artifact_id,
            depth_budget=contract.depth_budget,
            raw_values=raw_values,
        )
        if draft is None:
            if _root_post_edit_history_missing(response, plan.post_id):
                report["x_api_result_class"] = "edit_history_missing"
                _set_status(report, STATUS_X_API_FAILED, "x_api.edit_history_missing")
            else:
                report["x_api_result_class"] = "root_post_missing"
                _set_status(report, STATUS_X_API_FAILED, "x_api.root_post_missing")
            await _maybe_await(session.rollback())
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)
        report["x_api_call_succeeded_bucket"] = "one"
        report["x_api_result_class"] = draft.status

        try:
            await _write_snapshot_transaction(
                session=session,
                event=event,
                contract=contract,
                artifact=artifact,
                draft=draft,
                report=report,
            )
        except Exception:
            await _maybe_await(session.rollback())
            _set_status(report, STATUS_DB_WRITE_FAILED, "database.write")
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        if not await _ack_target(
            redis_client=redis_client,
            group_name=group_name,
            stream_id=thin_contract.entry.stream_id,
            report=report,
        ):
            return _finish_result(exit_code=1, report=report, raw_values=raw_values)

        _set_status(report, STATUS_SNAPSHOT_WRITTEN)
        return _finish_result(exit_code=0, report=report, raw_values=raw_values)
    except Exception:
        if session is not None:
            await _maybe_await(session.rollback())
        _set_status(report, STATUS_REHYDRATE_FAILED, "unexpected")
        return _finish_result(exit_code=1, report=report, raw_values=raw_values)
    finally:
        await _close_x_api_client(x_api_client)
        await _close_redis_client(redis_client)
        await _close_database_session(session)


def generate_report(
    *,
    runtime_env_path: str | Path = DEFAULT_RUNTIME_ENV_PATH,
    max_stream_entries: int = DEFAULT_MAX_STREAM_ENTRIES,
    approvals: ObservationApprovals | None = None,
    runtime_env_reader: RuntimeEnvReader | None = None,
    database_session_factory: DatabaseSessionFactory | None = None,
    redis_client_factory: RedisClientFactory | None = None,
    x_api_client_factory: XApiClientFactory | None = None,
    side_effect_flags: Mapping[str, bool] | None = None,
    forbidden_raw_values: Sequence[str] = (),
) -> ScriptResult:
    return asyncio.run(
        generate_report_async(
            runtime_env_path=runtime_env_path,
            max_stream_entries=max_stream_entries,
            approvals=approvals,
            runtime_env_reader=runtime_env_reader,
            database_session_factory=database_session_factory,
            redis_client_factory=redis_client_factory,
            x_api_client_factory=x_api_client_factory,
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
        max_stream_entries=args.max_stream_entries,
        approvals=approvals_from_args(args),
    )
    print(render_json(result.report))
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
