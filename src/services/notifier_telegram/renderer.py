from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from .entity_builder import TelegramEntityBuilder
from .keyboard_builder import InlineKeyboardBuilder
from .models import AnalysisRenderContext, CandidateRenderContext, JudgeOutputRenderContext, NotificationRenderDraft


_UUID_TEXT_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_DB_REDIS_URL_TEXT_RE = re.compile(
    r"\b(?:postgres(?:ql)?(?:\+[A-Za-z0-9_]+)?|redis(?:\+[A-Za-z0-9_]+)?)://[^\s<>)\"']+",
    flags=re.IGNORECASE,
)
_HTTP_URL_TEXT_RE = re.compile(r"https?://[^\s<>)\"']+", flags=re.IGNORECASE)
_SENSITIVE_TEXT_RE = re.compile(
    r"\b(?:DATABASE_URL|REDIS_URL|OPENAI_API_KEY|TELEGRAM_BOT_TOKEN|"
    r"runtime\.env|secret|token|password|credential|api[_-]?key)\b",
    flags=re.IGNORECASE,
)
_EXCEPTION_TEXT_RE = re.compile(
    r"\b(?:Traceback|OperationalError|RuntimeError|ValueError|Exception)\b[^\n]*",
    flags=re.IGNORECASE,
)
_RAW_PAYLOAD_TEXT_RE = re.compile(
    r"(?:\"?payload_json\"?\s*:|\"?source_text_surface\"?\s*:|"
    r"\"?telegram_response_json\"?\s*:|raw source text|TDLib|traceback|stderr)",
    flags=re.IGNORECASE,
)

_GITHUB_ARTIFACT_TYPES = {"github_repo", "github_subpath", "github_gist", "github_repo_page"}
_SOURCE_TYPE_LABELS = {
    "x_post": "X",
    "text_idea": "Idea",
    "web_article": "Web",
}


@dataclass(slots=True, frozen=True)
class RenderInput:
    analysis: AnalysisRenderContext
    judge_output: JudgeOutputRenderContext | None
    candidate: CandidateRenderContext | None
    urgency_profile: str


class NotificationRenderer:
    def __init__(
        self,
        *,
        entity_builder: TelegramEntityBuilder | None = None,
        keyboard_builder: InlineKeyboardBuilder | None = None,
        max_message_chars: int = 3800,
    ) -> None:
        self._entity_builder = entity_builder or TelegramEntityBuilder()
        self._keyboard_builder = keyboard_builder or InlineKeyboardBuilder()
        self._max_message_chars = max_message_chars

    @property
    def max_message_chars(self) -> int:
        return self._max_message_chars

    def render(self, *, notification_plan_id: UUID, payload: RenderInput) -> NotificationRenderDraft:
        analysis = payload.analysis
        judge_payload = payload.judge_output.payload_json if payload.judge_output is not None else {}
        candidate = payload.candidate
        source_type = source_type_label(candidate)
        severity = severity_band(
            verdict=analysis.verdict,
            urgency_profile=payload.urgency_profile,
            delivery_decision=analysis.delivery_decision,
        )
        confidence = confidence_display(analysis, payload.judge_output)
        headline = _string_or_none(judge_payload.get("headline")) or _string_or_none(
            judge_payload.get("title")
        ) or _safe_candidate_label(candidate) or "Untitled candidate"
        summary = _string_or_none(judge_payload.get("summary_one_line_ko")) or "누락: 요약 없음."
        skeptical_take = _string_or_none(judge_payload.get("skeptical_take_ko")) or "누락: 냉정 평가 없음."
        why_matter = _string_or_none(judge_payload.get("why_it_might_matter_ko")) or _string_or_none(
            judge_payload.get("why_it_might_matter")
        ) or "누락: 실용 포인트 없음."
        limitations = _coerce_text(analysis.evidence_limitations_ko) or "누락: 증거 한계 없음."
        action = _coerce_text(analysis.recommended_action_ko) or "누락: 추천 행동 없음."
        reason_codes = ", ".join(analysis.reason_codes_json) if analysis.reason_codes_json else "누락: 사유 코드 없음."
        risk = _risk_text(judge_payload, analysis, skeptical_take=skeptical_take)
        comparables = _comparables_text(judge_payload) if source_type == "GitHub" else None
        verdict_line = f"판정: {analysis.verdict}"
        if confidence is not None:
            verdict_line = f"{verdict_line} | confidence {confidence}"

        lines = [
            f"[{severity}] [{source_type}]",
            verdict_line,
            f"제목: {headline}",
            f"한줄 요약: {summary}",
            f"냉정 평가: {skeptical_take}",
            f"왜 볼만한가: {why_matter}",
        ]
        if comparables is not None:
            lines.append(f"기존 도구 대비: {comparables}")
        lines.extend(
            [
                f"리스크: {risk}",
                f"증거 한계: {limitations}",
                f"추천 행동: {action}",
                f"사유 코드: {reason_codes}",
            ]
        )
        if analysis.freshness_note_ko:
            lines.append(f"최신성: {analysis.freshness_note_ko}")

        message_text = self._truncate_preserving_core_sections(_redact_message_text("\n".join(lines)))
        entities = self._entity_builder.build(message_text)
        reply_markup = self._keyboard_builder.build(
            source_message_link=candidate.source_message_link if candidate else None,
            primary_url=candidate.primary_canonical_url if candidate else None,
            primary_artifact_type=candidate.primary_artifact_type if candidate else None,
        )
        render_hash = stable_render_hash(
            {
                "message_text": message_text,
                "entities_json": entities,
                "reply_markup_json": reply_markup,
                "link_preview_options_json": {"is_disabled": True},
            }
        )
        return NotificationRenderDraft(
            notification_plan_id=notification_plan_id,
            message_text=message_text,
            entities_json=entities,
            link_preview_options_json={"is_disabled": True},
            reply_markup_json=reply_markup,
            disable_notification=payload.urgency_profile != "high",
            protect_content=False,
            parse_strategy="entities",
            render_hash=render_hash,
        )

    def _truncate_preserving_core_sections(self, text: str) -> str:
        if len(text) <= self._max_message_chars:
            return text
        lines = text.splitlines()
        for prefix in ("최신성:", "사유 코드:", "기존 도구 대비:"):
            if len("\n".join(lines)) <= self._max_message_chars:
                break
            lines = [line for line in lines if not line.startswith(prefix)]
        return self._truncate("\n".join(lines))

    def _truncate(self, text: str) -> str:
        if len(text) <= self._max_message_chars:
            return text
        suffix = "\n[truncated to Telegram length budget]"
        return text[: max(0, self._max_message_chars - len(suffix))].rstrip() + suffix


def stable_render_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def source_type_label(candidate: CandidateRenderContext | None) -> str:
    artifact_type = _string_or_none(candidate.primary_artifact_type if candidate else None)
    if artifact_type in _GITHUB_ARTIFACT_TYPES:
        return "GitHub"
    if artifact_type in _SOURCE_TYPE_LABELS:
        return _SOURCE_TYPE_LABELS[artifact_type]
    return "Link"


def severity_band(*, verdict: str, urgency_profile: str, delivery_decision: str) -> str:
    normalized_verdict = str(verdict or "").strip()
    normalized_urgency = str(urgency_profile or "").strip()
    normalized_delivery = str(delivery_decision or "").strip()
    if normalized_verdict == "inspect_now" and normalized_urgency == "high":
        return "HIGH"
    if normalized_verdict == "skip" or normalized_urgency == "suppressed" or normalized_delivery == "suppress":
        return "LOW"
    if normalized_verdict == "later" or normalized_urgency == "normal_silent":
        return "MID"
    return "MID"


def confidence_display(
    analysis: AnalysisRenderContext,
    judge_output: JudgeOutputRenderContext | None,
) -> str | None:
    analysis_confidence = _numeric_confidence(analysis.scores_json.get("confidence"))
    if analysis_confidence is not None:
        return analysis_confidence
    judge_payload = judge_output.payload_json if judge_output is not None else {}
    scores = judge_payload.get("scores")
    if isinstance(scores, Mapping):
        judge_confidence = _numeric_confidence(scores.get("confidence"))
        if judge_confidence is not None:
            return judge_confidence
    band = _string_or_none(judge_payload.get("model_confidence_band")) or _string_or_none(
        judge_output.model_confidence_band if judge_output is not None else None
    )
    return band


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _coerce_text(value: Any) -> str | None:
    if isinstance(value, list):
        return "; ".join(str(item).strip() for item in value if str(item).strip()) or None
    return _string_or_none(value)


def _numeric_confidence(value: Any) -> str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else f"{value:.1f}".rstrip("0").rstrip(".")
    return None


def _risk_text(
    judge_payload: Mapping[str, Any],
    analysis: AnalysisRenderContext,
    *,
    skeptical_take: str,
) -> str:
    red_flags = _coerce_text(judge_payload.get("red_flags_ko"))
    if red_flags:
        return red_flags
    if analysis.reason_codes_json:
        return ", ".join(analysis.reason_codes_json)
    limitations = _coerce_text(analysis.evidence_limitations_ko)
    if limitations:
        return limitations
    return skeptical_take


def _comparables_text(judge_payload: Mapping[str, Any]) -> str | None:
    comparables = judge_payload.get("comparables")
    if not isinstance(comparables, list):
        return None
    values = [_comparable_item_text(item) for item in comparables]
    compact = [value for value in values if value]
    if not compact:
        return None
    return "; ".join(compact[:3])


def _comparable_item_text(item: Any) -> str | None:
    if isinstance(item, str):
        return _string_or_none(item)
    if isinstance(item, Mapping):
        for key in ("name", "title", "tool", "label", "summary_ko", "comparison"):
            value = _string_or_none(item.get(key))
            if value:
                return value
    return None


def _safe_candidate_label(candidate: CandidateRenderContext | None) -> str | None:
    value = _string_or_none(candidate.primary_canonical_id if candidate else None)
    if value is None:
        return None
    try:
        UUID(value)
    except ValueError:
        return value
    return None


def _redact_message_text(text: str) -> str:
    redacted = _EXCEPTION_TEXT_RE.sub("[redacted-error]", text)
    redacted = _DB_REDIS_URL_TEXT_RE.sub("[redacted-runtime-url]", redacted)
    redacted = _HTTP_URL_TEXT_RE.sub("[redacted-url]", redacted)
    redacted = _UUID_TEXT_RE.sub("[redacted-id]", redacted)
    redacted = _RAW_PAYLOAD_TEXT_RE.sub("[redacted-payload]", redacted)
    return _SENSITIVE_TEXT_RE.sub("[redacted-sensitive]", redacted)
