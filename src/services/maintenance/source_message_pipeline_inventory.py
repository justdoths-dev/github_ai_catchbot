from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Protocol
from uuid import UUID

import sqlalchemy as sa

from ..router_normalizer.bounded_source_normalize_runner import OfflineShortUrlResolver
from ..router_normalizer.canonicalizer import build_text_idea_artifact, canonicalize_resolved_urls
from ..router_normalizer.config import RouterNormalizerConfig
from ..router_normalizer.models import (
    CanonicalArtifact,
    NormalizationResult,
    RedisNormalizeMessage,
    SourceMessageSnapshot,
    TriggerEvaluation,
)
from ..router_normalizer.repositories import RouterNormalizerRepository
from ..router_normalizer.service import RouterNormalizerService, _with_inferred_repo_anchors
from ..router_normalizer.text_surfaces import build_text_surfaces
from ..router_normalizer.trigger_rules import evaluate_triggers
from ..router_normalizer.url_extraction import extract_urls


SCHEMA_VERSION = "source_message_pipeline_inventory_report_v1"
CONFIRM_TOKEN = "exact-source-normalization"
SELECTION_CONFIRM_TOKEN = "latest-unnormalized-source-created"
PLACEHOLDER_REDIS_URL = "redis_locator_not_attempted"

RUNTIME_VALUE_KEYS = {
    "APP_ENV",
    "DATABASE_URL",
    "ROUTER_NORMALIZER_VERSION",
    "ROUTER_NORMALIZER_SHORT_URL_ALLOWLIST",
    "ROUTER_NORMALIZER_SHORT_URL_HOP_LIMIT",
    "ROUTER_NORMALIZER_SHORT_URL_TIMEOUT_SECONDS",
}
RUNTIME_FILE_KEYS = {"DATABASE_URL_FILE"}
RUNTIME_ENV_KEYS = RUNTIME_VALUE_KEYS | RUNTIME_FILE_KEYS


class SourceMessagePipelineInventoryConfigError(ValueError):
    pass


class SilentArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # pragma: no cover - argparse calls this
        del message
        raise SourceMessagePipelineInventoryConfigError("invalid_cli_arguments")


@dataclass(slots=True, frozen=True)
class InventoryCounts:
    source_message_count_bucketed: int = 0
    source_created_event_count: int = 0
    source_created_pending_count: int = 0
    source_created_published_count: int = 0
    source_created_without_normalization_count: int = 0
    normalization_run_count: int = 0
    normalization_signal_detected_count: int = 0
    normalization_candidate_eligible_count: int = 0
    normalization_suppressed_count: int = 0
    suppression_reason_counts: tuple[tuple[str, int], ...] = ()
    candidate_group_count: int = 0
    artifact_enrichment_request_count: int = 0
    ready_bundle_count: int = 0
    analysis_requested_count: int = 0
    judge_call_requested_count: int = 0
    notification_plan_intent_count: int = 0


@dataclass(slots=True, frozen=True)
class CurrentRuleSummary:
    current_rule_candidate_eligible_count: int = 0
    current_rule_text_idea_candidate_count: int = 0
    current_rule_url_candidate_count: int = 0
    current_rule_weak_suppressed_count: int = 0
    current_rule_recall_candidate_with_existing_normalization_count: int = 0


@dataclass(slots=True, frozen=True)
class SourceCreatedTarget:
    event_id: UUID
    source_message_id: UUID
    source_version_no: int
    snapshot: SourceMessageSnapshot
    has_current_normalization: bool = False
    current_normalization_candidate_eligible: bool | None = None
    has_candidate_group: bool = False


@dataclass(slots=True, frozen=True)
class CurrentRulePreview:
    signal_detected: bool
    candidate_eligible: bool
    trigger_strength: str | None
    reason_codes: tuple[str, ...]
    artifact_count: int
    text_idea_candidate: bool
    url_candidate: bool


@dataclass(slots=True, frozen=True)
class SelectedTarget:
    target: SourceCreatedTarget
    preview: CurrentRulePreview

    @property
    def target_event_fingerprint(self) -> str:
        return _fingerprint(self.target.event_id)

    @property
    def source_message_fingerprint(self) -> str:
        return _fingerprint(self.target.source_message_id)


@dataclass(slots=True, frozen=True)
class NormalizationReadback:
    normalization_run_count: int = 0
    candidate_group_count: int = 0
    candidate_group_primary_member_count: int = 0
    artifact_enrichment_request_count: int = 0
    suppression_reason_counts: tuple[tuple[str, int], ...] = ()


@dataclass(slots=True, frozen=True)
class SourceMessagePipelineInventoryReport:
    schema_version: str
    mode: str
    status: str
    reason_code: str
    lookback_hours: int
    sample_limit: int
    source_message_count_bucketed: int
    source_created_event_count: int
    source_created_pending_count: int
    source_created_published_count: int
    source_created_without_normalization_count: int
    normalization_run_count: int
    normalization_signal_detected_count: int
    normalization_candidate_eligible_count: int
    normalization_suppressed_count: int
    suppression_reason_counts: list[dict[str, int | str]]
    candidate_group_count: int
    artifact_enrichment_request_count: int
    ready_bundle_count: int
    analysis_requested_count: int
    judge_call_requested_count: int
    notification_plan_intent_count: int
    current_rule_candidate_eligible_count: int
    current_rule_text_idea_candidate_count: int
    current_rule_url_candidate_count: int
    current_rule_weak_suppressed_count: int
    current_rule_recall_candidate_with_existing_normalization_count: int
    selected_target_event_fingerprint: str | None
    selected_source_message_fingerprint: str | None
    selected_source_version_no: int | None
    selected_target_reason_code: str | None
    selected_current_rule_signal_detected: bool | None
    selected_current_rule_candidate_eligible: bool | None
    selected_current_rule_trigger_strength: str | None
    selected_current_rule_reason_codes: list[str]
    normalization_attempted: bool
    normalization_created_or_updated: bool
    candidate_group_created_or_present: bool
    enrichment_request_created_or_present: bool
    candidate_group_primary_member_count: int
    redis_attempted: bool
    telegram_attempted: bool
    openai_attempted: bool
    external_network_attempted: bool
    redactions_applied: bool
    cleanup_completed: bool


@dataclass(slots=True, frozen=True)
class SourceMessagePipelineInventoryRequest:
    mode: str
    lookback_hours: int
    sample_limit: int
    select_latest_unnormalized_source_created: bool = False
    expected_target_event_fingerprint: str | None = None


@dataclass(slots=True, frozen=True)
class RuntimeConfigBundle:
    database_url: str
    values: Mapping[str, str]
    router_config: RouterNormalizerConfig


class InventoryRepositoryProtocol(Protocol):
    async def load_inventory_counts(
        self,
        *,
        lookback_hours: int,
        normalizer_version: str,
    ) -> InventoryCounts: ...

    async def load_source_created_preview_targets(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        normalizer_version: str,
    ) -> Sequence[SourceCreatedTarget]: ...

    async def load_normalization_readback(
        self,
        *,
        source_message_id: UUID,
        source_version_no: int,
        normalizer_version: str,
    ) -> NormalizationReadback: ...


class NormalizerServiceProtocol(Protocol):
    async def process_stream_message(self, message: RedisNormalizeMessage) -> NormalizationResult: ...


@dataclass(slots=True, frozen=True)
class SourceMessagePipelineInventoryComponents:
    inventory_repository: InventoryRepositoryProtocol
    normalizer_service: NormalizerServiceProtocol
    commit_active_transaction: Callable[[], Awaitable[None]]


class SqlSourceMessagePipelineInventoryRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def load_inventory_counts(
        self,
        *,
        lookback_hours: int,
        normalizer_version: str,
    ) -> InventoryCounts:
        row = await self._one(
            """
            WITH
            source_window AS (
                SELECT source_message_id
                FROM source_messages
                WHERE first_seen_at >= now() - make_interval(hours => :lookback_hours)
                   OR posted_at >= now() - make_interval(hours => :lookback_hours)
            ),
            source_created AS (
                SELECT
                    eo.event_id,
                    eo.status,
                    eo.aggregate_id AS source_message_id,
                    CASE
                        WHEN eo.payload_json->>'current_version_no' ~ '^[0-9]+$'
                            THEN (eo.payload_json->>'current_version_no')::int
                        WHEN eo.payload_json->>'source_version_no' ~ '^[0-9]+$'
                            THEN (eo.payload_json->>'source_version_no')::int
                        ELSE NULL
                    END AS source_version_no
                FROM event_outbox eo
                WHERE eo.event_type = 'source_message.created.v1'
                  AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
            )
            SELECT
                (SELECT count(*) FROM source_window) AS source_message_count,
                (SELECT count(*) FROM source_created) AS source_created_event_count,
                (SELECT count(*) FROM source_created WHERE status = 'pending') AS source_created_pending_count,
                (SELECT count(*) FROM source_created WHERE status = 'published') AS source_created_published_count,
                (
                    SELECT count(*)
                    FROM source_created sc
                    WHERE sc.source_version_no IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM normalization_runs nr
                          WHERE nr.source_message_id = sc.source_message_id
                            AND nr.source_version_no = sc.source_version_no
                            AND nr.normalizer_version = :normalizer_version
                      )
                ) AS source_created_without_normalization_count,
                (
                    SELECT count(*)
                    FROM normalization_runs nr
                    WHERE nr.completed_at >= now() - make_interval(hours => :lookback_hours)
                      AND nr.normalizer_version = :normalizer_version
                ) AS normalization_run_count,
                (
                    SELECT count(*)
                    FROM normalization_runs nr
                    WHERE nr.completed_at >= now() - make_interval(hours => :lookback_hours)
                      AND nr.normalizer_version = :normalizer_version
                      AND nr.signal_detected IS TRUE
                ) AS normalization_signal_detected_count,
                (
                    SELECT count(*)
                    FROM normalization_runs nr
                    WHERE nr.completed_at >= now() - make_interval(hours => :lookback_hours)
                      AND nr.normalizer_version = :normalizer_version
                      AND nr.candidate_eligible IS TRUE
                ) AS normalization_candidate_eligible_count,
                (
                    SELECT count(*)
                    FROM normalization_runs nr
                    WHERE nr.completed_at >= now() - make_interval(hours => :lookback_hours)
                      AND nr.normalizer_version = :normalizer_version
                      AND nr.candidate_eligible IS FALSE
                ) AS normalization_suppressed_count,
                (
                    SELECT count(*)
                    FROM candidate_group_proposals cgp
                    WHERE cgp.created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS candidate_group_count,
                (
                    SELECT count(*)
                    FROM event_outbox eo
                    WHERE eo.event_type = 'artifact.enrich.requested.v1'
                      AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS artifact_enrichment_request_count,
                (
                    SELECT count(*)
                    FROM candidate_evidence_bundles ceb
                    WHERE ceb.ready_for_analysis IS TRUE
                      AND ceb.created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS ready_bundle_count,
                (
                    SELECT count(*)
                    FROM event_outbox eo
                    WHERE eo.event_type = 'analysis.requested.v1'
                      AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS analysis_requested_count,
                (
                    SELECT count(*)
                    FROM event_outbox eo
                    WHERE eo.event_type = 'judge.call.requested.v1'
                      AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS judge_call_requested_count,
                (
                    SELECT count(*)
                    FROM event_outbox eo
                    WHERE eo.event_type = 'notification.plan.created.v1'
                      AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
                ) AS notification_plan_intent_count
            """,
            {
                "lookback_hours": lookback_hours,
                "normalizer_version": normalizer_version,
            },
        )
        reasons = await self._rows(
            """
            SELECT nst.reason_code, count(*) AS reason_count
            FROM normalization_suppression_traces nst
            JOIN normalization_runs nr ON nr.normalization_run_id = nst.normalization_run_id
            WHERE nr.completed_at >= now() - make_interval(hours => :lookback_hours)
              AND nr.normalizer_version = :normalizer_version
            GROUP BY nst.reason_code
            ORDER BY reason_count DESC, nst.reason_code ASC
            LIMIT 8
            """,
            {
                "lookback_hours": lookback_hours,
                "normalizer_version": normalizer_version,
            },
        )
        return InventoryCounts(
            source_message_count_bucketed=_int(row["source_message_count"]),
            source_created_event_count=_int(row["source_created_event_count"]),
            source_created_pending_count=_int(row["source_created_pending_count"]),
            source_created_published_count=_int(row["source_created_published_count"]),
            source_created_without_normalization_count=_int(row["source_created_without_normalization_count"]),
            normalization_run_count=_int(row["normalization_run_count"]),
            normalization_signal_detected_count=_int(row["normalization_signal_detected_count"]),
            normalization_candidate_eligible_count=_int(row["normalization_candidate_eligible_count"]),
            normalization_suppressed_count=_int(row["normalization_suppressed_count"]),
            suppression_reason_counts=tuple((str(item["reason_code"]), _int(item["reason_count"])) for item in reasons),
            candidate_group_count=_int(row["candidate_group_count"]),
            artifact_enrichment_request_count=_int(row["artifact_enrichment_request_count"]),
            ready_bundle_count=_int(row["ready_bundle_count"]),
            analysis_requested_count=_int(row["analysis_requested_count"]),
            judge_call_requested_count=_int(row["judge_call_requested_count"]),
            notification_plan_intent_count=_int(row["notification_plan_intent_count"]),
        )

    async def load_source_created_preview_targets(
        self,
        *,
        lookback_hours: int,
        sample_limit: int,
        normalizer_version: str,
    ) -> Sequence[SourceCreatedTarget]:
        rows = await self._rows(
            """
            WITH source_created AS (
                SELECT
                    eo.event_id,
                    eo.created_at,
                    eo.aggregate_id AS source_message_id,
                    CASE
                        WHEN eo.payload_json->>'current_version_no' ~ '^[0-9]+$'
                            THEN (eo.payload_json->>'current_version_no')::int
                        WHEN eo.payload_json->>'source_version_no' ~ '^[0-9]+$'
                            THEN (eo.payload_json->>'source_version_no')::int
                        ELSE NULL
                    END AS source_version_no
                FROM event_outbox eo
                WHERE eo.event_type = 'source_message.created.v1'
                  AND eo.created_at >= now() - make_interval(hours => :lookback_hours)
            )
            SELECT
                sc.event_id,
                sc.source_message_id,
                sc.source_version_no,
                sm.current_version_no,
                sm.text_body AS current_text_body,
                sm.caption_text AS current_caption_text,
                sm.text_surface AS current_text_surface,
                sm.entities_json AS current_entities_json,
                sm.url_surface_json AS current_url_surface_json,
                sm.raw_message_json AS current_raw_message_json,
                sm.deleted_at,
                smv.version_no AS version_no,
                smv.text_surface AS version_text_surface,
                smv.entities_json AS version_entities_json,
                smv.raw_message_json AS version_raw_message_json,
                nr.candidate_eligible AS current_normalization_candidate_eligible,
                (nr.normalization_run_id IS NOT NULL) AS has_current_normalization,
                EXISTS (
                    SELECT 1
                    FROM candidate_group_proposals cgp
                    WHERE cgp.source_message_id = sc.source_message_id
                      AND cgp.source_version_no = sc.source_version_no
                ) AS has_candidate_group
            FROM source_created sc
            JOIN source_messages sm ON sm.source_message_id = sc.source_message_id
            LEFT JOIN source_message_versions smv
              ON smv.source_message_id = sc.source_message_id
             AND smv.version_no = sc.source_version_no
             AND sm.current_version_no <> sc.source_version_no
            LEFT JOIN normalization_runs nr
              ON nr.source_message_id = sc.source_message_id
             AND nr.source_version_no = sc.source_version_no
             AND nr.normalizer_version = :normalizer_version
            WHERE sc.source_version_no IS NOT NULL
              AND (
                    sm.current_version_no = sc.source_version_no
                    OR smv.source_message_id IS NOT NULL
              )
              AND (
                    nr.normalization_run_id IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                        FROM candidate_group_proposals cgp
                        WHERE cgp.source_message_id = sc.source_message_id
                          AND cgp.source_version_no = sc.source_version_no
                    )
              )
            ORDER BY sc.created_at DESC, sc.event_id DESC
            LIMIT :sample_limit
            """,
            {
                "lookback_hours": lookback_hours,
                "sample_limit": sample_limit,
                "normalizer_version": normalizer_version,
            },
        )
        return tuple(target for row in rows if (target := _target_from_row(row)) is not None)

    async def load_normalization_readback(
        self,
        *,
        source_message_id: UUID,
        source_version_no: int,
        normalizer_version: str,
    ) -> NormalizationReadback:
        row = await self._one(
            """
            SELECT
                (
                    SELECT count(*)
                    FROM normalization_runs nr
                    WHERE nr.source_message_id = CAST(:source_message_id AS uuid)
                      AND nr.source_version_no = :source_version_no
                      AND nr.normalizer_version = :normalizer_version
                ) AS normalization_run_count,
                (
                    SELECT count(*)
                    FROM candidate_group_proposals cgp
                    WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
                      AND cgp.source_version_no = :source_version_no
                ) AS candidate_group_count,
                (
                    SELECT count(*)
                    FROM candidate_group_members cgm
                    JOIN candidate_group_proposals cgp
                      ON cgp.candidate_group_id = cgm.candidate_group_id
                    WHERE cgp.source_message_id = CAST(:source_message_id AS uuid)
                      AND cgp.source_version_no = :source_version_no
                      AND cgm.member_role = 'primary'
                ) AS candidate_group_primary_member_count,
                (
                    SELECT count(*)
                    FROM event_outbox eo
                    WHERE eo.event_type = 'artifact.enrich.requested.v1'
                      AND eo.payload_json->>'source_message_id' = :source_message_id
                      AND eo.payload_json->>'source_version_no' = :source_version_no_text
                ) AS artifact_enrichment_request_count
            """,
            {
                "source_message_id": str(source_message_id),
                "source_version_no": source_version_no,
                "source_version_no_text": str(source_version_no),
                "normalizer_version": normalizer_version,
            },
        )
        reasons = await self._rows(
            """
            SELECT nst.reason_code, count(*) AS reason_count
            FROM normalization_suppression_traces nst
            JOIN normalization_runs nr ON nr.normalization_run_id = nst.normalization_run_id
            WHERE nr.source_message_id = CAST(:source_message_id AS uuid)
              AND nr.source_version_no = :source_version_no
              AND nr.normalizer_version = :normalizer_version
            GROUP BY nst.reason_code
            ORDER BY reason_count DESC, nst.reason_code ASC
            LIMIT 8
            """,
            {
                "source_message_id": str(source_message_id),
                "source_version_no": source_version_no,
                "normalizer_version": normalizer_version,
            },
        )
        return NormalizationReadback(
            normalization_run_count=_int(row["normalization_run_count"]),
            candidate_group_count=_int(row["candidate_group_count"]),
            candidate_group_primary_member_count=_int(row["candidate_group_primary_member_count"]),
            artifact_enrichment_request_count=_int(row["artifact_enrichment_request_count"]),
            suppression_reason_counts=tuple((str(item["reason_code"]), _int(item["reason_count"])) for item in reasons),
        )

    async def _rows(self, sql: str, params: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        result = await self._session.execute(sa.text(sql), dict(params))
        return list(result.mappings().all())

    async def _one(self, sql: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        result = await self._session.execute(sa.text(sql), dict(params))
        return result.mappings().one()


async def run_source_message_pipeline_inventory(
    request: SourceMessagePipelineInventoryRequest,
    *,
    router_config: RouterNormalizerConfig,
    components: SourceMessagePipelineInventoryComponents,
) -> SourceMessagePipelineInventoryReport:
    report = _report(
        mode=request.mode,
        status="failed",
        reason_code="unhandled_error",
        lookback_hours=request.lookback_hours,
        sample_limit=request.sample_limit,
    )
    try:
        counts = await components.inventory_repository.load_inventory_counts(
            lookback_hours=request.lookback_hours,
            normalizer_version=router_config.normalizer_version,
        )
        previews = await _load_current_rule_previews(
            components.inventory_repository,
            lookback_hours=request.lookback_hours,
            sample_limit=request.sample_limit,
            router_config=router_config,
        )
        report = _apply_counts(report, counts)
        report = _apply_current_rule_summary(report, _current_rule_summary(previews))
        selected = _select_latest_target(previews) if request.select_latest_unnormalized_source_created else None
        report = _apply_selected_target(report, selected)

        if request.mode == "plan":
            if selected is not None:
                return replace(
                    report,
                    status="pass",
                    reason_code="normalization_target_plan_ready",
                )
            if not request.select_latest_unnormalized_source_created:
                return replace(report, status="pass", reason_code="inventory_plan_complete")
            return replace(report, status="blocked", reason_code=_no_target_reason(counts))

        if selected is None:
            return replace(report, status="blocked", reason_code=_no_target_reason(counts))
        if request.expected_target_event_fingerprint != selected.target_event_fingerprint:
            return replace(report, status="blocked", reason_code="target_event_fingerprint_mismatch")

        report = replace(report, normalization_attempted=True)
        result = await components.normalizer_service.process_stream_message(
            _normalize_message_for_target(selected.target)
        )
        try:
            await components.commit_active_transaction()
        except Exception:
            return replace(report, status="failed", reason_code="normalization_commit_failed")

        readback = await components.inventory_repository.load_normalization_readback(
            source_message_id=selected.target.source_message_id,
            source_version_no=selected.target.source_version_no,
            normalizer_version=router_config.normalizer_version,
        )
        report = _apply_readback(report, readback)
        if not result.candidate_eligible or result.suppression_reason_codes:
            return replace(report, status="failed", reason_code="normalization_suppressed_unexpectedly")
        if readback.normalization_run_count < 1:
            return replace(report, status="failed", reason_code="normalization_run_missing_after_normalization")
        if readback.candidate_group_count < 1:
            return replace(report, status="failed", reason_code="candidate_group_missing_after_normalization")
        return replace(report, status="pass", reason_code="source_normalization_materialized")
    except SourceMessagePipelineInventoryConfigError as exc:
        return replace(report, status="blocked", reason_code=_safe_reason_code(exc))
    except Exception:
        return replace(report, status="failed", reason_code="unhandled_error")


async def _load_current_rule_previews(
    repository: InventoryRepositoryProtocol,
    *,
    lookback_hours: int,
    sample_limit: int,
    router_config: RouterNormalizerConfig,
) -> tuple[tuple[SourceCreatedTarget, CurrentRulePreview], ...]:
    rows = await repository.load_source_created_preview_targets(
        lookback_hours=lookback_hours,
        sample_limit=sample_limit,
        normalizer_version=router_config.normalizer_version,
    )
    previews: list[tuple[SourceCreatedTarget, CurrentRulePreview]] = []
    for row in rows:
        preview = await _build_current_rule_preview(
            row.snapshot,
            short_url_allowlist=router_config.short_url_allowlist,
        )
        previews.append((row, preview))
    return tuple(previews)


async def _build_current_rule_preview(
    snapshot: SourceMessageSnapshot,
    *,
    short_url_allowlist: tuple[str, ...],
) -> CurrentRulePreview:
    surfaces = build_text_surfaces(snapshot)
    extracted_urls = extract_urls(snapshot, surfaces)
    resolver = OfflineShortUrlResolver(short_url_allowlist)
    resolved_urls = [await resolver.resolve(url) for url in extracted_urls]
    artifacts = _with_inferred_repo_anchors(canonicalize_resolved_urls(resolved_urls))
    evaluation = evaluate_triggers(surfaces, artifacts)
    if evaluation.candidate_eligible and not artifacts:
        artifacts = [build_text_idea_artifact(surfaces)]
    return _preview_from_evaluation(evaluation, artifacts)


def _preview_from_evaluation(
    evaluation: TriggerEvaluation,
    artifacts: Sequence[CanonicalArtifact],
) -> CurrentRulePreview:
    text_idea = evaluation.candidate_eligible and any(artifact.artifact_type == "text_idea" for artifact in artifacts)
    url_candidate = evaluation.candidate_eligible and any(artifact.artifact_type != "text_idea" for artifact in artifacts)
    return CurrentRulePreview(
        signal_detected=evaluation.signal_detected,
        candidate_eligible=evaluation.candidate_eligible,
        trigger_strength=evaluation.trigger_strength,
        reason_codes=tuple(_safe_reason_list(evaluation.reason_codes)),
        artifact_count=len(artifacts),
        text_idea_candidate=text_idea,
        url_candidate=url_candidate,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = SilentArgumentParser(prog="source-message-pipeline-inventory")
    parser.add_argument("--mode")
    parser.add_argument("--env-file")
    parser.add_argument("--lookback-hours", type=int, default=72)
    parser.add_argument("--sample-limit", type=int, default=100)
    parser.add_argument("--select-latest-unnormalized-source-created", action="store_true")
    parser.add_argument("--selection-confirm", default=None)
    parser.add_argument("--expected-target-event-fingerprint", default=None)
    parser.add_argument("--confirm", default=None)
    return parser


async def run_cli(
    argv: Sequence[str] | None = None,
    *,
    emit_json: Callable[[str], None] = print,
    runtime_config_loader: Callable[[str], RuntimeConfigBundle] | None = None,
    components_builder: Callable[
        [RuntimeConfigBundle],
        AsyncIterator[SourceMessagePipelineInventoryComponents],
    ]
    | None = None,
) -> int:
    try:
        args = build_parser().parse_args(list(argv) if argv is not None else None)
    except SourceMessagePipelineInventoryConfigError as exc:
        emit_json(_compact_json(asdict(_argument_report(str(exc)))))
        return 2

    validation_error = _cli_request_error(args)
    mode = str(args.mode) if args.mode in {"plan", "execute-normalize"} else "unknown"
    if validation_error is not None:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=mode,
                        status="blocked",
                        reason_code=validation_error,
                        lookback_hours=_bounded_cli_int(args.lookback_hours, default=72),
                        sample_limit=_bounded_cli_int(args.sample_limit, default=100),
                    )
                )
            )
        )
        return 2

    try:
        runtime = (runtime_config_loader or load_runtime_config)(str(args.env_file))
    except SourceMessagePipelineInventoryConfigError as exc:
        emit_json(
            _compact_json(
                asdict(
                    _report(
                        mode=mode,
                        status="blocked",
                        reason_code=_safe_reason_code(exc),
                        lookback_hours=int(args.lookback_hours),
                        sample_limit=int(args.sample_limit),
                    )
                )
            )
        )
        return 2

    request = SourceMessagePipelineInventoryRequest(
        mode=str(args.mode),
        lookback_hours=int(args.lookback_hours),
        sample_limit=int(args.sample_limit),
        select_latest_unnormalized_source_created=bool(args.select_latest_unnormalized_source_created),
        expected_target_event_fingerprint=_optional_str(args.expected_target_event_fingerprint),
    )

    builder = components_builder or sql_inventory_components
    async with builder(runtime) as components:
        report = await run_source_message_pipeline_inventory(
            request,
            router_config=runtime.router_config,
            components=components,
        )
    emit_json(_compact_json(asdict(report)))
    return 0 if report.status == "pass" else 2


def load_runtime_config(env_file: str) -> RuntimeConfigBundle:
    values = _read_runtime_env_file(env_file)
    resolved_values = dict(values)
    database_url = _resolve_file_indirection(
        resolved_values,
        value_key="DATABASE_URL",
        file_key="DATABASE_URL_FILE",
        missing_reason_code="database_url_missing",
        file_missing_reason_code="database_url_file_missing",
        file_empty_reason_code="database_url_file_empty",
    )
    try:
        router_config = RouterNormalizerConfig(
            app_env=_read(resolved_values, "APP_ENV", "dev").lower(),
            database_url=database_url,
            redis_url=PLACEHOLDER_REDIS_URL,
            queue_name="q.source.normalize",
            consumer_group="router-normalizer",
            consumer_name="source-message-pipeline-inventory",
            block_ms=5000,
            batch_size=1,
            normalizer_version=_read(
                resolved_values,
                "ROUTER_NORMALIZER_VERSION",
                "router-normalizer-v1",
            ),
            short_url_allowlist=_short_url_allowlist(resolved_values),
            short_url_hop_limit=int(_read(resolved_values, "ROUTER_NORMALIZER_SHORT_URL_HOP_LIMIT", "3")),
            short_url_timeout_seconds=float(
                _read(resolved_values, "ROUTER_NORMALIZER_SHORT_URL_TIMEOUT_SECONDS", "2.0")
            ),
            log_level="INFO",
        )
        router_config.validate()
    except (TypeError, ValueError):
        raise SourceMessagePipelineInventoryConfigError("router_normalizer_config_invalid") from None
    return RuntimeConfigBundle(
        database_url=database_url,
        values=resolved_values,
        router_config=router_config,
    )


@asynccontextmanager
async def sql_inventory_components(
    runtime: RuntimeConfigBundle,
) -> AsyncIterator[SourceMessagePipelineInventoryComponents]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with session_factory() as session:
            yield SourceMessagePipelineInventoryComponents(
                inventory_repository=SqlSourceMessagePipelineInventoryRepository(session),
                normalizer_service=RouterNormalizerService(
                    runtime.router_config,
                    repository=RouterNormalizerRepository(session),
                    short_url_resolver=OfflineShortUrlResolver(runtime.router_config.short_url_allowlist),
                ),
                commit_active_transaction=session.commit,
            )
    finally:
        await engine.dispose()


def _cli_request_error(args: argparse.Namespace) -> str | None:
    if args.mode not in {"plan", "execute-normalize"}:
        return "invalid_mode"
    if not args.env_file:
        return "env_file_required"
    if args.lookback_hours < 1 or args.lookback_hours > 720:
        return "lookback_hours_out_of_range"
    if args.sample_limit < 1 or args.sample_limit > 500:
        return "sample_limit_out_of_range"
    if args.mode == "plan" and args.confirm is not None:
        return "confirm_not_allowed_for_plan"
    if args.select_latest_unnormalized_source_created and args.selection_confirm != SELECTION_CONFIRM_TOKEN:
        return "selection_confirm_missing"
    if not args.select_latest_unnormalized_source_created and args.selection_confirm is not None:
        return "selection_confirm_without_selector"
    if args.mode == "plan" and args.expected_target_event_fingerprint is not None:
        return "expected_target_event_fingerprint_not_allowed_for_plan"
    if args.mode == "execute-normalize":
        if args.confirm != CONFIRM_TOKEN:
            return "exact_source_normalization_confirm_missing"
        if not args.select_latest_unnormalized_source_created:
            return "selector_required_for_execute"
        if not _optional_str(args.expected_target_event_fingerprint):
            return "expected_target_event_fingerprint_missing"
    return None


def _current_rule_summary(
    previews: Sequence[tuple[SourceCreatedTarget, CurrentRulePreview]],
) -> CurrentRuleSummary:
    return CurrentRuleSummary(
        current_rule_candidate_eligible_count=sum(1 for _target, preview in previews if preview.candidate_eligible),
        current_rule_text_idea_candidate_count=sum(
            1 for _target, preview in previews if preview.text_idea_candidate
        ),
        current_rule_url_candidate_count=sum(1 for _target, preview in previews if preview.url_candidate),
        current_rule_weak_suppressed_count=sum(
            1 for _target, preview in previews if preview.signal_detected and not preview.candidate_eligible
        ),
        current_rule_recall_candidate_with_existing_normalization_count=sum(
            1
            for target, preview in previews
            if preview.candidate_eligible
            and target.has_current_normalization
            and not bool(target.current_normalization_candidate_eligible)
        ),
    )


def _select_latest_target(
    previews: Sequence[tuple[SourceCreatedTarget, CurrentRulePreview]],
) -> SelectedTarget | None:
    for target, preview in previews:
        if target.snapshot.deleted_at is not None:
            continue
        if target.has_current_normalization:
            continue
        if target.has_candidate_group:
            continue
        if not preview.candidate_eligible:
            continue
        return SelectedTarget(target=target, preview=preview)
    return None


def _no_target_reason(counts: InventoryCounts) -> str:
    if counts.source_message_count_bucketed == 0:
        return "no_source_messages_in_lookback"
    if counts.source_created_event_count == 0:
        return "no_source_created_events_in_lookback"
    if counts.source_created_without_normalization_count == 0:
        return "source_pipeline_already_normalized_or_empty"
    return "no_unnormalized_candidate_eligible_source_created_target"


def _normalize_message_for_target(target: SourceCreatedTarget) -> RedisNormalizeMessage:
    return RedisNormalizeMessage(
        job_id=str(target.event_id),
        stage_name="normalize",
        root_object_type="source_message",
        root_object_id=str(target.source_message_id),
        idempotency_key=f"source-message-pipeline-inventory:{target.event_id}",
        trigger_event_id=str(target.event_id),
    )


def _report(
    *,
    mode: str,
    status: str,
    reason_code: str,
    lookback_hours: int,
    sample_limit: int,
) -> SourceMessagePipelineInventoryReport:
    return SourceMessagePipelineInventoryReport(
        schema_version=SCHEMA_VERSION,
        mode=mode,
        status=status,
        reason_code=reason_code,
        lookback_hours=lookback_hours,
        sample_limit=sample_limit,
        source_message_count_bucketed=0,
        source_created_event_count=0,
        source_created_pending_count=0,
        source_created_published_count=0,
        source_created_without_normalization_count=0,
        normalization_run_count=0,
        normalization_signal_detected_count=0,
        normalization_candidate_eligible_count=0,
        normalization_suppressed_count=0,
        suppression_reason_counts=[],
        candidate_group_count=0,
        artifact_enrichment_request_count=0,
        ready_bundle_count=0,
        analysis_requested_count=0,
        judge_call_requested_count=0,
        notification_plan_intent_count=0,
        current_rule_candidate_eligible_count=0,
        current_rule_text_idea_candidate_count=0,
        current_rule_url_candidate_count=0,
        current_rule_weak_suppressed_count=0,
        current_rule_recall_candidate_with_existing_normalization_count=0,
        selected_target_event_fingerprint=None,
        selected_source_message_fingerprint=None,
        selected_source_version_no=None,
        selected_target_reason_code=None,
        selected_current_rule_signal_detected=None,
        selected_current_rule_candidate_eligible=None,
        selected_current_rule_trigger_strength=None,
        selected_current_rule_reason_codes=[],
        normalization_attempted=False,
        normalization_created_or_updated=False,
        candidate_group_created_or_present=False,
        enrichment_request_created_or_present=False,
        candidate_group_primary_member_count=0,
        redis_attempted=False,
        telegram_attempted=False,
        openai_attempted=False,
        external_network_attempted=False,
        redactions_applied=True,
        cleanup_completed=True,
    )


def _argument_report(reason_code: str) -> SourceMessagePipelineInventoryReport:
    return _report(
        mode="unknown",
        status="blocked",
        reason_code=reason_code,
        lookback_hours=72,
        sample_limit=100,
    )


def _apply_counts(
    report: SourceMessagePipelineInventoryReport,
    counts: InventoryCounts,
) -> SourceMessagePipelineInventoryReport:
    return replace(
        report,
        source_message_count_bucketed=counts.source_message_count_bucketed,
        source_created_event_count=counts.source_created_event_count,
        source_created_pending_count=counts.source_created_pending_count,
        source_created_published_count=counts.source_created_published_count,
        source_created_without_normalization_count=counts.source_created_without_normalization_count,
        normalization_run_count=counts.normalization_run_count,
        normalization_signal_detected_count=counts.normalization_signal_detected_count,
        normalization_candidate_eligible_count=counts.normalization_candidate_eligible_count,
        normalization_suppressed_count=counts.normalization_suppressed_count,
        suppression_reason_counts=_reason_counts(counts.suppression_reason_counts),
        candidate_group_count=counts.candidate_group_count,
        artifact_enrichment_request_count=counts.artifact_enrichment_request_count,
        ready_bundle_count=counts.ready_bundle_count,
        analysis_requested_count=counts.analysis_requested_count,
        judge_call_requested_count=counts.judge_call_requested_count,
        notification_plan_intent_count=counts.notification_plan_intent_count,
    )


def _apply_current_rule_summary(
    report: SourceMessagePipelineInventoryReport,
    summary: CurrentRuleSummary,
) -> SourceMessagePipelineInventoryReport:
    return replace(
        report,
        current_rule_candidate_eligible_count=summary.current_rule_candidate_eligible_count,
        current_rule_text_idea_candidate_count=summary.current_rule_text_idea_candidate_count,
        current_rule_url_candidate_count=summary.current_rule_url_candidate_count,
        current_rule_weak_suppressed_count=summary.current_rule_weak_suppressed_count,
        current_rule_recall_candidate_with_existing_normalization_count=(
            summary.current_rule_recall_candidate_with_existing_normalization_count
        ),
    )


def _apply_selected_target(
    report: SourceMessagePipelineInventoryReport,
    selected: SelectedTarget | None,
) -> SourceMessagePipelineInventoryReport:
    if selected is None:
        return report
    return replace(
        report,
        selected_target_event_fingerprint=selected.target_event_fingerprint,
        selected_source_message_fingerprint=selected.source_message_fingerprint,
        selected_source_version_no=selected.target.source_version_no,
        selected_target_reason_code="latest_unnormalized_source_created_candidate_eligible",
        selected_current_rule_signal_detected=selected.preview.signal_detected,
        selected_current_rule_candidate_eligible=selected.preview.candidate_eligible,
        selected_current_rule_trigger_strength=selected.preview.trigger_strength,
        selected_current_rule_reason_codes=list(selected.preview.reason_codes),
    )


def _apply_readback(
    report: SourceMessagePipelineInventoryReport,
    readback: NormalizationReadback,
) -> SourceMessagePipelineInventoryReport:
    return replace(
        report,
        normalization_created_or_updated=readback.normalization_run_count > 0,
        candidate_group_created_or_present=readback.candidate_group_count > 0,
        enrichment_request_created_or_present=readback.artifact_enrichment_request_count > 0,
        candidate_group_primary_member_count=readback.candidate_group_primary_member_count,
        suppression_reason_counts=_reason_counts(readback.suppression_reason_counts) or report.suppression_reason_counts,
    )


def _target_from_row(row: Mapping[str, Any]) -> SourceCreatedTarget | None:
    source_message_id = UUID(str(row["source_message_id"]))
    source_version_no = int(row["source_version_no"])
    current_version_no = int(row["current_version_no"])
    if current_version_no == source_version_no:
        text_body = _optional_str(row.get("current_text_body"))
        caption_text = _optional_str(row.get("current_caption_text"))
        text_surface = _optional_str(row.get("current_text_surface"))
        entities_json = _json_loads(row.get("current_entities_json"))
        url_surface_json = _json_loads(row.get("current_url_surface_json"))
        raw_message_json = _json_loads(row.get("current_raw_message_json")) or {}
    else:
        if row.get("version_no") is None:
            return None
        text_body = None
        caption_text = None
        text_surface = _optional_str(row.get("version_text_surface"))
        entities_json = _json_loads(row.get("version_entities_json"))
        url_surface_json = None
        raw_message_json = _json_loads(row.get("version_raw_message_json")) or {}
    return SourceCreatedTarget(
        event_id=UUID(str(row["event_id"])),
        source_message_id=source_message_id,
        source_version_no=source_version_no,
        snapshot=SourceMessageSnapshot(
            source_message_id=source_message_id,
            source_version_no=source_version_no,
            text_body=text_body,
            caption_text=caption_text,
            text_surface=text_surface,
            entities_json=entities_json,
            url_surface_json=url_surface_json,
            raw_message_json=raw_message_json,
            deleted_at=row.get("deleted_at"),
        ),
        has_current_normalization=bool(row.get("has_current_normalization")),
        current_normalization_candidate_eligible=_optional_bool(row.get("current_normalization_candidate_eligible")),
        has_candidate_group=bool(row.get("has_candidate_group")),
    )


def _read_runtime_env_file(env_file: str) -> dict[str, str]:
    path = Path(env_file)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        raise SourceMessagePipelineInventoryConfigError("env_file_missing") from None
    except OSError:
        raise SourceMessagePipelineInventoryConfigError("env_file_unreadable") from None

    values: dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in RUNTIME_ENV_KEYS:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    if not values:
        raise SourceMessagePipelineInventoryConfigError("env_file_no_runtime_config")
    return values


def _resolve_file_indirection(
    values: dict[str, str],
    *,
    value_key: str,
    file_key: str,
    missing_reason_code: str,
    file_missing_reason_code: str,
    file_empty_reason_code: str,
) -> str:
    direct = values.get(value_key, "").strip()
    if direct:
        return direct
    file_path = values.get(file_key, "").strip()
    if not file_path:
        raise SourceMessagePipelineInventoryConfigError(missing_reason_code)
    path = Path(file_path)
    if not path.is_file():
        raise SourceMessagePipelineInventoryConfigError(file_missing_reason_code)
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError:
        raise SourceMessagePipelineInventoryConfigError(file_missing_reason_code) from None
    if not value:
        raise SourceMessagePipelineInventoryConfigError(file_empty_reason_code)
    return value


def _short_url_allowlist(values: Mapping[str, str]) -> tuple[str, ...]:
    raw = _read(
        values,
        "ROUTER_NORMALIZER_SHORT_URL_ALLOWLIST",
        "bit.ly,t.co,tinyurl.com,ow.ly,lnkd.in,buff.ly,goo.gl",
    )
    return tuple(host.strip().lower() for host in raw.split(",") if host.strip())


def _read(values: Mapping[str, str], key: str, default: str) -> str:
    value = values.get(key, default)
    return value.strip() if isinstance(value, str) else default


def _json_loads(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


def _reason_counts(items: Sequence[tuple[str, int]]) -> list[dict[str, int | str]]:
    return [{"reason_code": reason, "count": int(count)} for reason, count in items[:8] if _safe_token(reason)]


def _safe_reason_list(values: Sequence[str]) -> list[str]:
    return [value for value in values if _safe_token(value)]


def _safe_token(value: str) -> bool:
    return re.fullmatch(r"[a-z0-9_]{1,80}", value) is not None


def _safe_reason_code(exc: Exception) -> str:
    value = str(exc)
    if value and _safe_token(value):
        return value
    return "configuration_error"


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]


def _optional_str(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _bounded_cli_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _compact_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=_json_default)


def _json_default(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"unsupported json type: {type(value)!r}")


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run_cli(argv))


if __name__ == "__main__":
    raise SystemExit(main())
