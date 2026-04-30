from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from .entity_builder import TelegramEntityBuilder
from .keyboard_builder import InlineKeyboardBuilder
from .models import AnalysisRenderContext, CandidateRenderContext, JudgeOutputRenderContext, NotificationRenderDraft


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

    def render(self, *, notification_plan_id: UUID, payload: RenderInput) -> NotificationRenderDraft:
        analysis = payload.analysis
        judge_payload = payload.judge_output.payload_json if payload.judge_output is not None else {}
        candidate = payload.candidate
        headline = _string_or_none(judge_payload.get("headline")) or _string_or_none(
            judge_payload.get("title")
        ) or _string_or_none(candidate.primary_canonical_id if candidate else None) or "Untitled candidate"
        summary = _string_or_none(judge_payload.get("summary_one_line_ko")) or "Omitted: summary unavailable."
        skeptical_take = _string_or_none(judge_payload.get("skeptical_take_ko")) or "Omitted: skeptical take unavailable."
        why_matter = _string_or_none(judge_payload.get("why_it_might_matter_ko")) or _string_or_none(
            judge_payload.get("why_it_might_matter")
        ) or "Omitted: why-it-matters unavailable."
        limitations = _coerce_text(analysis.evidence_limitations_ko) or "Omitted: evidence limitations unavailable."
        action = _coerce_text(analysis.recommended_action_ko) or "Omitted: recommended action unavailable."
        reason_codes = ", ".join(analysis.reason_codes_json) if analysis.reason_codes_json else "Omitted: reason codes unavailable."

        lines = [
            f"Verdict: {analysis.verdict}",
            f"Delivery: {analysis.delivery_decision} / {payload.urgency_profile}",
            f"Headline: {headline}",
            f"Summary: {summary}",
            f"Skeptical take: {skeptical_take}",
            f"Why it might matter: {why_matter}",
            f"Reason codes: {reason_codes}",
            f"Evidence limitations: {limitations}",
            f"Recommended action: {action}",
        ]
        if analysis.freshness_note_ko:
            lines.append(f"Freshness: {analysis.freshness_note_ko}")
        if candidate and candidate.primary_canonical_url:
            lines.append(f"Primary: {candidate.primary_canonical_url}")
        if candidate and candidate.source_message_link:
            lines.append(f"Source: {candidate.source_message_link}")

        message_text = self._truncate("\n".join(lines))
        entities = self._entity_builder.build(message_text)
        reply_markup = self._keyboard_builder.build(
            source_message_link=candidate.source_message_link if candidate else None,
            primary_url=candidate.primary_canonical_url if candidate else None,
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

    def _truncate(self, text: str) -> str:
        if len(text) <= self._max_message_chars:
            return text
        suffix = "\n[truncated to Telegram length budget]"
        return text[: max(0, self._max_message_chars - len(suffix))].rstrip() + suffix


def stable_render_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _coerce_text(value: Any) -> str | None:
    if isinstance(value, list):
        return "; ".join(str(item).strip() for item in value if str(item).strip()) or None
    return _string_or_none(value)
