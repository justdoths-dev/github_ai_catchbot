from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class PreflightResult:
    developer_prompt: str
    user_context: str
    notes: list[str]
    flags: dict[str, object]


class ModelContextPreflight:
    def apply(self, *, developer_prompt: str, user_context: str) -> PreflightResult:
        raise NotImplementedError


class NoopModelContextPreflight(ModelContextPreflight):
    def apply(self, *, developer_prompt: str, user_context: str) -> PreflightResult:
        return PreflightResult(
            developer_prompt=developer_prompt,
            user_context=user_context,
            notes=[],
            flags={},
        )


class HeuristicSanitizingPreflight(ModelContextPreflight):
    """Sanitize-only hook; it never blocks, quarantines, or writes durable state."""

    _INSTRUCTION_RE = re.compile(
        r"(?im)^\s*(ignore (all )?(previous|above) instructions|"
        r"system prompt|developer message|reveal your prompt|"
        r"print your hidden rules|disregard (all )?(previous|above) instructions).*$"
    )

    def apply(self, *, developer_prompt: str, user_context: str) -> PreflightResult:
        sanitized = self._INSTRUCTION_RE.sub("[sanitized_instruction_like_line]", user_context)
        notes: list[str] = []
        flags: dict[str, object] = {}
        if sanitized != user_context:
            notes.append("sanitized_instruction_like_line")
            flags["prompt_guard_sanitized"] = True
        return PreflightResult(
            developer_prompt=developer_prompt,
            user_context=sanitized,
            notes=notes,
            flags=flags,
        )
