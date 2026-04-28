from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any
from uuid import UUID

from .models import TextIdeaSnapshotDraft


_DEV_KEYWORDS = (
    "agent",
    "ai",
    "api",
    "automation",
    "code",
    "coding",
    "dev",
    "github",
    "library",
    "prompt",
    "repo",
    "sdk",
    "tool",
    "workflow",
)


class TextIdeaBuilder:
    def build(
        self,
        *,
        artifact_id: UUID,
        source_message_id: UUID,
        source_version_no: int,
        text_surface: str | None,
    ) -> TextIdeaSnapshotDraft | None:
        display_surface = self._normalize_display(text_surface)
        if display_surface is None:
            return None

        hash_surface = self._hash_surface(display_surface)
        lowered = display_surface.lower()
        signals: dict[str, Any] = {
            "keyword_hits": [keyword for keyword in _DEV_KEYWORDS if keyword in lowered],
            "has_code_fence": "```" in display_surface,
            "has_url": "http://" in lowered or "https://" in lowered,
            "length_chars": len(display_surface),
        }
        limitations: list[str] = []
        if not signals["keyword_hits"]:
            limitations.append("weak_dev_context")
        if signals["length_chars"] < 40:
            limitations.append("short_text_surface")

        return TextIdeaSnapshotDraft(
            artifact_id=artifact_id,
            source_message_id=source_message_id,
            source_version_no=source_version_no,
            hash_surface=hash_surface,
            display_surface=display_surface,
            dev_context_signals_json=signals,
            status="low_evidence" if limitations else "ready",
            evidence_limitations=limitations,
        )

    @staticmethod
    def content_anchor(draft: TextIdeaSnapshotDraft) -> str:
        return TextIdeaBuilder.input_hash(draft)

    @staticmethod
    def input_hash(draft: TextIdeaSnapshotDraft) -> str:
        payload = {
            "artifact_id": str(draft.artifact_id),
            "source_message_id": str(draft.source_message_id),
            "source_version_no": draft.source_version_no,
            "hash_surface": draft.hash_surface,
            "display_surface": draft.display_surface,
            "dev_context_signals_json": draft.dev_context_signals_json,
            "status": draft.status,
            "evidence_limitations": draft.evidence_limitations,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_display(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = unicodedata.normalize("NFKC", value).replace("\r\n", "\n").replace("\r", "\n").strip()
        return normalized or None

    @staticmethod
    def _hash_surface(value: str) -> str:
        normalized = unicodedata.normalize("NFKC", value)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return "text_idea:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()
