from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from json import JSONDecodeError
from typing import Any, Iterable, Mapping, Protocol
from uuid import UUID


ALLOWED_FEEDBACK_LABELS = frozenset(
    {
        "useful",
        "useful_now",
        "useful_later",
        "false_positive",
        "false_negative",
        "hype",
        "duplicate",
        "stale",
        "wrong_priority",
        "wrong_primary",
        "insufficient_evidence",
        "bad_summary",
        "bad_channel_fit",
    }
)
VERDICTS = ("inspect_now", "later", "skip")
DELIVERY_DECISIONS = ("send_now", "send_digest", "suppress")
URGENCY_PROFILES = ("high", "normal_silent", "digest", "suppressed")
MAX_FEEDBACK_ROWS = 100
MAX_FEEDBACK_WINDOW_DAYS = 365
DEFAULT_FEEDBACK_WINDOW_DAYS = 90
MIN_CHANNEL_POLICY_SAMPLE = 5
SAFE_REASON_RE = re.compile(r"^[a-z][a-z0-9_:-]{0,63}$")
SAFE_SUFFIX_RE = re.compile(r"^[0-9a-f-]{4,36}$")
SAFE_OPERATOR_ACTION_KEY_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
SAFE_CHANNEL_FINGERPRINT_RE = re.compile(r"^fp_[0-9a-f]{12}$")
ARTIFACT_TYPES = frozenset(
    {
        "github_repo",
        "github_subpath",
        "github_gist",
        "github_repo_page",
        "x_post",
        "web_article",
        "text_idea",
        "unknown_link",
        "short_url_unresolved",
        "unknown",
    }
)
USEFUL_FEEDBACK_LABELS = frozenset({"useful", "useful_now", "useful_later"})
NOISE_FEEDBACK_LABELS = frozenset(
    {
        "false_positive",
        "duplicate",
        "stale",
        "wrong_priority",
        "hype",
        "bad_channel_fit",
    }
)


@dataclass(frozen=True, slots=True)
class FeedbackRecord:
    label: str
    candidate_group_id_suffix: str | None = None
    analysis_id_suffix: str | None = None
    operator_score: int | None = None
    reason_code: str | None = None
    channel_tier: str | None = None
    verdict: str | None = None
    delivery_decision: str | None = None
    urgency_profile: str | None = None

    def with_policy_outcome(
        self,
        *,
        verdict: str | None,
        delivery_decision: str | None,
        urgency_profile: str | None,
    ) -> FeedbackRecord:
        return FeedbackRecord(
            candidate_group_id_suffix=self.candidate_group_id_suffix,
            analysis_id_suffix=self.analysis_id_suffix,
            label=self.label,
            operator_score=self.operator_score,
            reason_code=self.reason_code,
            channel_tier=self.channel_tier,
            verdict=verdict if verdict in VERDICTS else None,
            delivery_decision=delivery_decision if delivery_decision in DELIVERY_DECISIONS else None,
            urgency_profile=urgency_profile if urgency_profile in URGENCY_PROFILES else None,
        )


@dataclass(frozen=True, slots=True)
class FeedbackParseResult:
    records: tuple[FeedbackRecord, ...]
    rows_seen: int
    row_cap: int
    invalid_feedback_count: int
    invalid_reason_distribution: dict[str, int]

    @property
    def valid_feedback_count(self) -> int:
        return len(self.records)

    @property
    def feedback_count(self) -> int:
        return self.valid_feedback_count + self.invalid_feedback_count


@dataclass(frozen=True, slots=True)
class FeedbackEvalResult:
    feedback_count: int
    valid_feedback_count: int
    invalid_feedback_count: int
    label_distribution: dict[str, int]
    usefulness_score_average_bucket: str
    false_positive_count_bucket: str
    false_negative_count_bucket: str
    delivery_distribution: dict[str, dict[str, int]]
    policy_outcome_alignment: dict[str, int]
    invalid_reason_distribution: dict[str, int]
    reason_code_distribution: dict[str, int]

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "feedback_count": self.feedback_count,
            "valid_feedback_count": self.valid_feedback_count,
            "invalid_feedback_count": self.invalid_feedback_count,
            "label_distribution": dict(self.label_distribution),
            "usefulness_score_average_bucket": self.usefulness_score_average_bucket,
            "false_positive_count_bucket": self.false_positive_count_bucket,
            "false_negative_count_bucket": self.false_negative_count_bucket,
            "delivery_distribution": {
                "verdict": dict(self.delivery_distribution["verdict"]),
                "delivery_decision": dict(self.delivery_distribution["delivery_decision"]),
                "urgency_profile": dict(self.delivery_distribution["urgency_profile"]),
            },
            "policy_outcome_alignment": dict(self.policy_outcome_alignment),
            "invalid_reason_distribution": dict(self.invalid_reason_distribution),
            "reason_code_distribution": dict(self.reason_code_distribution),
        }


@dataclass(frozen=True, slots=True)
class FeedbackTargetContext:
    analysis_id: UUID
    candidate_group_id: UUID
    notification_plan_id: UUID | None
    notification_delivery_record_id: UUID | None
    channel_registry_id: UUID | None
    channel_fingerprint: str | None
    verdict: str
    delivery_decision: str
    primary_artifact_type: str


@dataclass(frozen=True, slots=True)
class StoredNotificationFeedback:
    feedback_id: UUID
    operator_action_key: str
    feedback_category: str
    analysis_id: UUID
    candidate_group_id: UUID
    notification_plan_id: UUID | None
    notification_delivery_record_id: UUID | None
    channel_registry_id: UUID | None
    verdict: str
    delivery_decision: str
    primary_artifact_type: str


@dataclass(frozen=True, slots=True)
class NotificationFeedbackRequest:
    operator_action_key: str
    feedback_category: str
    analysis_id: UUID
    notification_plan_id: UUID | None = None
    notification_delivery_record_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class ChannelFeedbackObservation:
    feedback_category: str
    verdict: str
    delivery_decision: str
    primary_artifact_type: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ChannelFeedbackSample:
    channel_fingerprint: str | None
    observations: tuple[ChannelFeedbackObservation, ...]
    sample_limit: int = MAX_FEEDBACK_ROWS
    window_days: int = DEFAULT_FEEDBACK_WINDOW_DAYS


@dataclass(frozen=True, slots=True)
class ChannelFeedbackAggregate:
    channel_fingerprint: str | None
    sample_count: int
    minimum_sample: int
    sample_limit: int
    window_days: int
    sufficient_sample: bool
    channel_tier: str
    policy_active: bool
    category_distribution: dict[str, int]
    verdict_distribution: dict[str, int]
    delivery_distribution: dict[str, int]
    artifact_type_distribution: dict[str, int]
    useful_rate_basis_points: int
    noise_rate_basis_points: int

    @classmethod
    def neutral(
        cls,
        *,
        channel_fingerprint: str | None = None,
        sample_limit: int = MAX_FEEDBACK_ROWS,
        window_days: int = DEFAULT_FEEDBACK_WINDOW_DAYS,
        minimum_sample: int = MIN_CHANNEL_POLICY_SAMPLE,
    ) -> ChannelFeedbackAggregate:
        return cls(
            channel_fingerprint=_safe_channel_fingerprint(channel_fingerprint),
            sample_count=0,
            minimum_sample=minimum_sample,
            sample_limit=sample_limit,
            window_days=window_days,
            sufficient_sample=False,
            channel_tier="B",
            policy_active=False,
            category_distribution={label: 0 for label in sorted(ALLOWED_FEEDBACK_LABELS)},
            verdict_distribution={key: 0 for key in VERDICTS},
            delivery_distribution={key: 0 for key in DELIVERY_DECISIONS},
            artifact_type_distribution={},
            useful_rate_basis_points=0,
            noise_rate_basis_points=0,
        )

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "channel_fingerprint": self.channel_fingerprint,
            "sample_count": self.sample_count,
            "minimum_sample": self.minimum_sample,
            "sample_limit": self.sample_limit,
            "window_days": self.window_days,
            "sufficient_sample": self.sufficient_sample,
            "channel_tier": self.channel_tier,
            "policy_active": self.policy_active,
            "category_distribution": dict(self.category_distribution),
            "verdict_distribution": dict(self.verdict_distribution),
            "delivery_distribution": dict(self.delivery_distribution),
            "artifact_type_distribution": dict(self.artifact_type_distribution),
            "useful_rate_basis_points": self.useful_rate_basis_points,
            "noise_rate_basis_points": self.noise_rate_basis_points,
            "statistical_confidence_claimed": False,
        }


@dataclass(frozen=True, slots=True)
class NotificationFeedbackCaptureResult:
    created: bool
    feedback_category: str
    operator_action_fingerprint: str
    analysis_bound: bool
    notification_plan_bound: bool
    notification_delivery_record_bound: bool
    candidate_group_bound: bool
    channel_bound: bool
    aggregate: ChannelFeedbackAggregate

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "notification_feedback_capture_v1",
            "status": "created" if self.created else "noop",
            "reason_code": "feedback_created" if self.created else "feedback_idempotent_noop",
            "feedback_category": self.feedback_category,
            "operator_action_fingerprint": self.operator_action_fingerprint,
            "identity_binding": {
                "analysis_bound": self.analysis_bound,
                "notification_plan_bound": self.notification_plan_bound,
                "notification_delivery_record_bound": self.notification_delivery_record_bound,
                "candidate_group_bound": self.candidate_group_bound,
                "channel_bound": self.channel_bound,
            },
            "aggregate": self.aggregate.to_sanitized_dict(),
            "side_effects": {
                "historical_analysis_mutated": False,
                "judge_output_mutated": False,
                "evidence_bundle_mutated": False,
                "delivery_record_mutated": False,
                "send_triggered": False,
                "retry_triggered": False,
                "replay_triggered": False,
                "provider_called": False,
            },
        }


@dataclass(frozen=True, slots=True)
class NotificationFeedbackReadbackResult:
    analysis_bound: bool
    notification_plan_bound: bool
    notification_delivery_record_bound: bool
    candidate_group_bound: bool
    channel_bound: bool
    aggregate: ChannelFeedbackAggregate

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "notification_feedback_readback_v1",
            "status": "pass",
            "reason_code": "feedback_channel_readback_complete",
            "identity_binding": {
                "analysis_bound": self.analysis_bound,
                "notification_plan_bound": self.notification_plan_bound,
                "notification_delivery_record_bound": self.notification_delivery_record_bound,
                "candidate_group_bound": self.candidate_group_bound,
                "channel_bound": self.channel_bound,
            },
            "aggregate": self.aggregate.to_sanitized_dict(),
        }


class NotificationFeedbackError(ValueError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class NotificationFeedbackRepositoryProtocol(Protocol):
    def transaction(self): ...

    async def load_feedback_target(
        self,
        *,
        analysis_id: UUID,
        notification_plan_id: UUID | None,
        notification_delivery_record_id: UUID | None,
    ) -> FeedbackTargetContext | None: ...

    async def load_feedback_by_action_key(
        self,
        operator_action_key: str,
    ) -> StoredNotificationFeedback | None: ...

    async def insert_notification_feedback(
        self,
        *,
        request: NotificationFeedbackRequest,
        target: FeedbackTargetContext,
    ) -> StoredNotificationFeedback | None: ...

    async def load_channel_feedback_sample(
        self,
        *,
        channel_registry_id: UUID,
        sample_limit: int,
        window_days: int,
    ) -> ChannelFeedbackSample: ...


class FeedbackEvalEngine:
    def parse_jsonl(self, text: str, *, row_cap: int = MAX_FEEDBACK_ROWS) -> FeedbackParseResult:
        if not 1 <= row_cap <= MAX_FEEDBACK_ROWS:
            raise ValueError("row_cap_out_of_range")

        records: list[FeedbackRecord] = []
        invalid_reasons: Counter[str] = Counter()
        lines = text.splitlines()
        rows_seen = len(lines)

        for row_number, line in enumerate(lines[:row_cap], start=1):
            record, error_code = _parse_feedback_line(line, row_number=row_number)
            if error_code is not None:
                invalid_reasons[error_code] += 1
                continue
            if record is not None:
                records.append(record)

        if len(lines) > row_cap:
            invalid_reasons["row_cap_exceeded"] += len(lines) - row_cap

        return FeedbackParseResult(
            records=tuple(records),
            rows_seen=rows_seen,
            row_cap=row_cap,
            invalid_feedback_count=sum(invalid_reasons.values()),
            invalid_reason_distribution=dict(sorted(invalid_reasons.items())),
        )

    def evaluate(
        self,
        records: Iterable[FeedbackRecord],
        *,
        invalid_feedback_count: int = 0,
        invalid_reason_distribution: Mapping[str, int] | None = None,
        total_feedback_count: int | None = None,
    ) -> FeedbackEvalResult:
        normalized_records = tuple(records)
        labels = Counter(record.label for record in normalized_records)
        reason_codes = Counter(
            record.reason_code
            for record in normalized_records
            if record.reason_code is not None and _safe_reason_code(record.reason_code) == record.reason_code
        )
        scores = [record.operator_score for record in normalized_records if record.operator_score is not None]
        delivery_distribution = {
            "verdict": {key: 0 for key in VERDICTS},
            "delivery_decision": {key: 0 for key in DELIVERY_DECISIONS},
            "urgency_profile": {key: 0 for key in URGENCY_PROFILES},
        }
        alignment = {
            "aligned_positive": 0,
            "possible_over_send": 0,
            "possible_under_send": 0,
            "duplicate_risk": 0,
            "evidence_risk": 0,
        }

        for record in normalized_records:
            if record.verdict in VERDICTS:
                delivery_distribution["verdict"][record.verdict] += 1
            if record.delivery_decision in DELIVERY_DECISIONS:
                delivery_distribution["delivery_decision"][record.delivery_decision] += 1
            if record.urgency_profile in URGENCY_PROFILES:
                delivery_distribution["urgency_profile"][record.urgency_profile] += 1

            if record.label == "useful_now" and record.verdict == "inspect_now":
                alignment["aligned_positive"] += 1
            if record.label == "false_positive" and record.verdict == "inspect_now":
                alignment["possible_over_send"] += 1
            if record.label == "false_negative" and record.verdict in {"skip", "later"}:
                alignment["possible_under_send"] += 1
            if record.label == "duplicate" and record.delivery_decision == "send_now":
                alignment["duplicate_risk"] += 1
            if record.label == "insufficient_evidence" and record.verdict == "inspect_now":
                alignment["evidence_risk"] += 1

        valid_count = len(normalized_records)
        feedback_count = total_feedback_count if total_feedback_count is not None else valid_count + invalid_feedback_count
        return FeedbackEvalResult(
            feedback_count=feedback_count,
            valid_feedback_count=valid_count,
            invalid_feedback_count=invalid_feedback_count,
            label_distribution=_ordered_label_distribution(labels),
            usefulness_score_average_bucket=_score_average_bucket(scores),
            false_positive_count_bucket=_count_bucket(labels["false_positive"]),
            false_negative_count_bucket=_count_bucket(labels["false_negative"]),
            delivery_distribution=delivery_distribution,
            policy_outcome_alignment=alignment,
            invalid_reason_distribution=dict(sorted((invalid_reason_distribution or {}).items())),
            reason_code_distribution=dict(sorted(reason_codes.items())),
        )

    def aggregate_channel_sample(
        self,
        sample: ChannelFeedbackSample,
        *,
        minimum_sample: int = MIN_CHANNEL_POLICY_SAMPLE,
    ) -> ChannelFeedbackAggregate:
        if not 1 <= sample.sample_limit <= MAX_FEEDBACK_ROWS:
            raise ValueError("sample_limit_out_of_range")
        if not 1 <= sample.window_days <= MAX_FEEDBACK_WINDOW_DAYS:
            raise ValueError("window_days_out_of_range")
        if not 1 <= minimum_sample <= sample.sample_limit:
            raise ValueError("minimum_sample_out_of_range")
        if len(sample.observations) > sample.sample_limit:
            raise ValueError("sample_limit_exceeded")

        category_counts = Counter(observation.feedback_category for observation in sample.observations)
        verdict_counts = Counter(observation.verdict for observation in sample.observations)
        delivery_counts = Counter(observation.delivery_decision for observation in sample.observations)
        artifact_counts = Counter(
            _safe_artifact_type(observation.primary_artifact_type)
            for observation in sample.observations
        )
        sample_count = len(sample.observations)
        useful_count = sum(category_counts[label] for label in USEFUL_FEEDBACK_LABELS)
        noise_count = sum(category_counts[label] for label in NOISE_FEEDBACK_LABELS)
        useful_rate = _rate_basis_points(useful_count, sample_count)
        noise_rate = _rate_basis_points(noise_count, sample_count)
        sufficient_sample = sample_count >= minimum_sample

        if sufficient_sample and noise_count >= 3 and noise_rate >= 5_000:
            channel_tier = "C"
        elif sufficient_sample and noise_count == 0 and useful_rate >= 7_500:
            channel_tier = "A"
        else:
            channel_tier = "B"

        return ChannelFeedbackAggregate(
            channel_fingerprint=_safe_channel_fingerprint(sample.channel_fingerprint),
            sample_count=sample_count,
            minimum_sample=minimum_sample,
            sample_limit=sample.sample_limit,
            window_days=sample.window_days,
            sufficient_sample=sufficient_sample,
            channel_tier=channel_tier,
            policy_active=sufficient_sample and channel_tier == "C",
            category_distribution={
                label: int(category_counts[label]) for label in sorted(ALLOWED_FEEDBACK_LABELS)
            },
            verdict_distribution={key: int(verdict_counts[key]) for key in VERDICTS},
            delivery_distribution={key: int(delivery_counts[key]) for key in DELIVERY_DECISIONS},
            artifact_type_distribution=dict(sorted(artifact_counts.items())),
            useful_rate_basis_points=useful_rate,
            noise_rate_basis_points=noise_rate,
        )


class NotificationFeedbackService:
    def __init__(
        self,
        repository: NotificationFeedbackRepositoryProtocol,
        *,
        eval_engine: FeedbackEvalEngine | None = None,
    ) -> None:
        self._repository = repository
        self._eval_engine = eval_engine or FeedbackEvalEngine()

    async def capture(self, request: NotificationFeedbackRequest) -> NotificationFeedbackCaptureResult:
        _validate_feedback_request(request)
        async with self._repository.transaction():
            existing = await self._repository.load_feedback_by_action_key(request.operator_action_key)
            if existing is not None:
                if not _feedback_request_matches(existing, request):
                    raise NotificationFeedbackError("feedback_idempotency_conflict")
                target = _feedback_target_from_stored(existing)
                created = False
            else:
                target = await self._repository.load_feedback_target(
                    analysis_id=request.analysis_id,
                    notification_plan_id=request.notification_plan_id,
                    notification_delivery_record_id=request.notification_delivery_record_id,
                )
                if target is None:
                    raise NotificationFeedbackError("feedback_target_missing_or_mismatch")
                inserted = await self._repository.insert_notification_feedback(request=request, target=target)
                if inserted is None:
                    inserted = await self._repository.load_feedback_by_action_key(request.operator_action_key)
                    if inserted is None or not _feedback_request_matches(inserted, request):
                        raise NotificationFeedbackError("feedback_idempotency_conflict")
                    target = _feedback_target_from_stored(inserted)
                    created = False
                else:
                    created = True

            aggregate = await self._aggregate_for_target(target)

        return NotificationFeedbackCaptureResult(
            created=created,
            feedback_category=request.feedback_category,
            operator_action_fingerprint=_fingerprint(request.operator_action_key),
            analysis_bound=True,
            notification_plan_bound=target.notification_plan_id is not None,
            notification_delivery_record_bound=target.notification_delivery_record_id is not None,
            candidate_group_bound=True,
            channel_bound=target.channel_registry_id is not None,
            aggregate=aggregate,
        )

    async def readback(
        self,
        *,
        analysis_id: UUID,
        notification_plan_id: UUID | None = None,
        notification_delivery_record_id: UUID | None = None,
    ) -> NotificationFeedbackReadbackResult:
        async with self._repository.transaction():
            target = await self._repository.load_feedback_target(
                analysis_id=analysis_id,
                notification_plan_id=notification_plan_id,
                notification_delivery_record_id=notification_delivery_record_id,
            )
            if target is None:
                raise NotificationFeedbackError("feedback_target_missing_or_mismatch")
            aggregate = await self._aggregate_for_target(target)
        return NotificationFeedbackReadbackResult(
            analysis_bound=True,
            notification_plan_bound=target.notification_plan_id is not None,
            notification_delivery_record_bound=target.notification_delivery_record_id is not None,
            candidate_group_bound=True,
            channel_bound=target.channel_registry_id is not None,
            aggregate=aggregate,
        )

    async def _aggregate_for_target(self, target: FeedbackTargetContext) -> ChannelFeedbackAggregate:
        if target.channel_registry_id is None:
            return ChannelFeedbackAggregate.neutral(
                channel_fingerprint=target.channel_fingerprint,
            )
        sample = await self._repository.load_channel_feedback_sample(
            channel_registry_id=target.channel_registry_id,
            sample_limit=MAX_FEEDBACK_ROWS,
            window_days=DEFAULT_FEEDBACK_WINDOW_DAYS,
        )
        return self._eval_engine.aggregate_channel_sample(sample)


def _parse_feedback_line(line: str, *, row_number: int) -> tuple[FeedbackRecord | None, str | None]:
    del row_number
    if not line.strip():
        return None, "empty_row"
    try:
        payload = json.loads(line)
    except JSONDecodeError:
        return None, "invalid_json"
    if not isinstance(payload, dict):
        return None, "invalid_json_object"

    label = payload.get("label")
    if not isinstance(label, str) or label not in ALLOWED_FEEDBACK_LABELS:
        return None, "invalid_label"

    candidate_suffix = _safe_suffix(payload.get("candidate_group_id_suffix"))
    analysis_suffix = _safe_suffix(payload.get("analysis_id_suffix"))
    if candidate_suffix is None and analysis_suffix is None:
        return None, "missing_target_suffix"

    score, score_error = _operator_score(payload.get("operator_score"))
    if score_error is not None:
        return None, score_error

    return (
        FeedbackRecord(
            candidate_group_id_suffix=candidate_suffix,
            analysis_id_suffix=analysis_suffix,
            label=label,
            operator_score=score,
            reason_code=_safe_reason_code(payload.get("reason_code")),
            channel_tier=_safe_channel_tier(payload.get("channel_tier")),
        ),
        None,
    )


def _operator_score(value: Any) -> tuple[int | None, str | None]:
    if value is None:
        return None, None
    if isinstance(value, bool) or not isinstance(value, int):
        return None, "invalid_operator_score"
    if not 1 <= value <= 5:
        return None, "invalid_operator_score"
    return value, None


def _safe_suffix(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized if SAFE_SUFFIX_RE.fullmatch(normalized) else None


def _safe_reason_code(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return "unsafe_reason_code"
    normalized = value.strip().lower()
    return normalized if SAFE_REASON_RE.fullmatch(normalized) else "unsafe_reason_code"


def _safe_channel_tier(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    return normalized if normalized in {"A", "B", "C"} else None


def _ordered_label_distribution(labels: Counter[str]) -> dict[str, int]:
    return {label: int(labels[label]) for label in sorted(ALLOWED_FEEDBACK_LABELS)}


def _score_average_bucket(scores: list[int | None]) -> str:
    numeric = [score for score in scores if score is not None]
    if not numeric:
        return "none"
    average = sum(numeric) / len(numeric)
    if average < 2.5:
        return "score_1_to_2"
    if average < 3.5:
        return "score_3"
    return "score_4_to_5"


def _count_bucket(count: int) -> str:
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    if count <= 5:
        return "two_to_five"
    if count <= 20:
        return "six_to_twenty"
    return "over_twenty"


def _validate_feedback_request(request: NotificationFeedbackRequest) -> None:
    if request.feedback_category not in ALLOWED_FEEDBACK_LABELS:
        raise NotificationFeedbackError("invalid_feedback_category")
    if not SAFE_OPERATOR_ACTION_KEY_RE.fullmatch(request.operator_action_key):
        raise NotificationFeedbackError("invalid_operator_action_key")


def _feedback_request_matches(
    existing: StoredNotificationFeedback,
    request: NotificationFeedbackRequest,
) -> bool:
    plan_matches = (
        request.notification_plan_id is None
        or existing.notification_plan_id == request.notification_plan_id
    )
    delivery_matches = (
        request.notification_delivery_record_id is None
        or existing.notification_delivery_record_id == request.notification_delivery_record_id
    )
    return (
        existing.operator_action_key == request.operator_action_key
        and existing.feedback_category == request.feedback_category
        and existing.analysis_id == request.analysis_id
        and plan_matches
        and delivery_matches
    )


def _feedback_target_from_stored(existing: StoredNotificationFeedback) -> FeedbackTargetContext:
    return FeedbackTargetContext(
        analysis_id=existing.analysis_id,
        candidate_group_id=existing.candidate_group_id,
        notification_plan_id=existing.notification_plan_id,
        notification_delivery_record_id=existing.notification_delivery_record_id,
        channel_registry_id=existing.channel_registry_id,
        channel_fingerprint=channel_fp(existing.channel_registry_id) if existing.channel_registry_id else None,
        verdict=existing.verdict,
        delivery_decision=existing.delivery_decision,
        primary_artifact_type=existing.primary_artifact_type,
    )


def _rate_basis_points(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        return 0
    return numerator * 10_000 // denominator


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def channel_fp(value: UUID | str) -> str:
    digest = hashlib.sha256(f"github-ai-catchbot:{value}".encode("utf-8")).hexdigest()
    return f"fp_{digest[:12]}"


def _safe_channel_fingerprint(value: str | None) -> str | None:
    if value is None:
        return None
    return value if SAFE_CHANNEL_FINGERPRINT_RE.fullmatch(value) else None


def _safe_artifact_type(value: str) -> str:
    return value if value in ARTIFACT_TYPES else "unknown"
