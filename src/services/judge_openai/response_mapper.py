from __future__ import annotations

import json
import time
from typing import Any

from .models import OpenAIJudgeResult, OpenAIJudgeUsage


class OpenAIResponseMapper:
    def parse(self, response: Any, *, started_monotonic: float) -> OpenAIJudgeResult:
        payload_json = self._extract_structured_payload(response)
        refusal_text = None if payload_json is not None else self._extract_refusal_text(response)
        return OpenAIJudgeResult(
            payload_json=payload_json,
            refusal_text=refusal_text,
            finish_reason=self._extract_finish_reason(response),
            usage=self._extract_usage(response, started_monotonic=started_monotonic),
            raw_response_id=self._string_or_none(self._get(response, "id")),
        )

    def build_refusal_envelope(
        self,
        *,
        candidate_group_id: str,
        schema_version: str,
        refusal_text: str | None,
    ) -> dict[str, Any]:
        return {
            "judge_schema_version": schema_version,
            "candidate_group_id": candidate_group_id,
            "output_kind": "refusal",
            "refusal_text": refusal_text or "",
        }

    def _extract_structured_payload(self, response: Any) -> dict[str, Any] | None:
        output_text = self._get(response, "output_text")
        if isinstance(output_text, str) and output_text.strip():
            return self._json_object_or_none(output_text)

        for block in self._content_blocks(response):
            if self._get(block, "type") != "output_text":
                continue
            text = self._get(block, "text")
            if isinstance(text, str) and text.strip():
                parsed = self._json_object_or_none(text)
                if parsed is not None:
                    return parsed
        return None

    def _extract_refusal_text(self, response: Any) -> str | None:
        texts: list[str] = []
        for block in self._content_blocks(response):
            if self._get(block, "type") != "refusal":
                continue
            text = self._get(block, "refusal") or self._get(block, "text")
            if isinstance(text, str) and text.strip():
                texts.append(text.strip())
        return "\n".join(texts) if texts else None

    def _content_blocks(self, response: Any) -> list[Any]:
        output = self._get(response, "output")
        if not isinstance(output, list):
            return []

        blocks: list[Any] = []
        for item in output:
            if self._get(item, "type") != "message":
                continue
            content = self._get(item, "content")
            if isinstance(content, list):
                blocks.extend(content)
        return blocks

    def _extract_usage(self, response: Any, *, started_monotonic: float) -> OpenAIJudgeUsage:
        usage = self._get(response, "usage")
        input_details = self._get(usage, "input_tokens_details") if usage is not None else None
        output_details = self._get(usage, "output_tokens_details") if usage is not None else None
        latency_ms = int((time.monotonic() - started_monotonic) * 1000)
        return OpenAIJudgeUsage(
            input_tokens=self._int_or_none(self._get(usage, "input_tokens")),
            cached_input_tokens=self._int_or_none(self._get(input_details, "cached_tokens")),
            output_tokens=self._int_or_none(self._get(usage, "output_tokens")),
            reasoning_tokens=self._int_or_none(self._get(output_details, "reasoning_tokens")),
            latency_ms=latency_ms,
        )

    def _extract_finish_reason(self, response: Any) -> str | None:
        incomplete_details = self._get(response, "incomplete_details")
        incomplete_reason = self._string_or_none(self._get(incomplete_details, "reason"))
        if incomplete_reason:
            return incomplete_reason
        return self._string_or_none(self._get(response, "status"))

    @staticmethod
    def _json_object_or_none(text: str) -> dict[str, Any] | None:
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    @staticmethod
    def _get(value: Any, key: str) -> Any:
        if value is None:
            return None
        if isinstance(value, dict):
            return value.get(key)
        return getattr(value, key, None)

    @staticmethod
    def _string_or_none(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value)
        return text if text else None

    @staticmethod
    def _int_or_none(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
