from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Any, Iterable, Mapping


ALLOWED_FEEDBACK_LABELS = frozenset(
    {
        "useful_now",
        "useful_later",
        "false_positive",
        "false_negative",
        "hype",
        "duplicate",
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
SAFE_REASON_RE = re.compile(r"^[a-z][a-z0-9_:-]{0,63}$")
SAFE_SUFFIX_RE = re.compile(r"^[0-9a-f-]{4,36}$")


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
