from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .context_builder import JudgeContextBuilder
from .models import BundleJudgeContext, JudgeRunRecord
from .preflight import NoopModelContextPreflight
from .prompt_library import PromptLibrary


LOCKED_HOT_PATH_MODEL = "gpt-5.4-mini"
LOCKED_ESCALATION_MODEL = "gpt-5.4"
SUPPORTED_LOCKED_MODELS = frozenset({LOCKED_HOT_PATH_MODEL, LOCKED_ESCALATION_MODEL})
SUPPORTED_REASONING_EFFORTS = frozenset({"none", "low", "medium", "high", "xhigh"})
SUPPORTED_SCHEMA_TYPES = frozenset(
    {"string", "number", "boolean", "integer", "object", "array", "null"}
)
UNSUPPORTED_SCHEMA_KEYWORDS = frozenset(
    {
        "allOf",
        "not",
        "dependentRequired",
        "dependentSchemas",
        "if",
        "then",
        "else",
        "patternProperties",
    }
)
ALLOWED_TOP_LEVEL_KEYS = frozenset(
    {
        "model",
        "reasoning",
        "input",
        "text",
        "tools",
        "max_output_tokens",
        "prompt_cache_key",
    }
)
OPTIONAL_TOP_LEVEL_KEYS = frozenset({"max_output_tokens", "prompt_cache_key"})
_FORMAT_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


@dataclass(slots=True, frozen=True)
class RequestShapeDiagnostic:
    issue_codes: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return not self.issue_codes


class JudgeOpenAIRequestEnvelopeError(ValueError):
    def __init__(self, issue_codes: tuple[str, ...]) -> None:
        super().__init__("judge_openai_request_envelope_invalid")
        self.issue_codes = issue_codes


@dataclass(slots=True, frozen=True)
class JudgeOpenAIRequestEnvelope:
    model: str
    reasoning_effort: str
    prompt_version: str
    schema_version: str
    policy_version: str
    judge_profile: str
    prompt_cache_key: str | None
    developer_prompt_text: str
    user_context: str
    structured_output_schema_id: str
    structured_output_schema: dict[str, Any]
    max_output_tokens: int | None
    request_timeout_sec: float | None
    preflight_notes: tuple[str, ...] = ()
    preflight_flags: Mapping[str, Any] | None = None

    @property
    def context_character_count(self) -> int:
        return len(self.user_context)

    def to_responses_request(self, *, include_prompt_cache_key: bool = True) -> dict[str, Any]:
        return build_responses_request(
            model=self.model,
            reasoning_effort=self.reasoning_effort,
            developer_prompt=self.developer_prompt_text,
            user_context=self.user_context,
            json_schema=self.structured_output_schema,
            max_output_tokens=self.max_output_tokens,
            prompt_cache_key=self.prompt_cache_key,
            schema_name=self.structured_output_schema_id,
            include_prompt_cache_key=include_prompt_cache_key,
        )


class JudgeOpenAIRequestEnvelopeBuilder:
    def __init__(
        self,
        *,
        prompt_library: PromptLibrary | None = None,
        context_builder: JudgeContextBuilder | None = None,
        structured_output_schema: Mapping[str, Any] | None = None,
        structured_output_schema_id: str = "judge_output_v1",
        max_output_tokens: int | None = None,
        request_timeout_sec: float | None = None,
    ) -> None:
        self._prompt_library = prompt_library or PromptLibrary()
        self._context_builder = context_builder or JudgeContextBuilder(
            preflight=NoopModelContextPreflight()
        )
        self._structured_output_schema = dict(structured_output_schema or build_judge_output_schema())
        self._structured_output_schema_id = structured_output_schema_id
        self._max_output_tokens = max_output_tokens
        self._request_timeout_sec = request_timeout_sec

    def build(
        self,
        *,
        judge_run: JudgeRunRecord,
        bundle: BundleJudgeContext,
    ) -> JudgeOpenAIRequestEnvelope:
        developer_prompt = self._prompt_library.render(
            judge_profile=judge_run.judge_profile,
            prompt_version=judge_run.prompt_version,
        )
        prepared = self._context_builder.build(
            developer_prompt=developer_prompt,
            bundle=bundle,
        )
        envelope = JudgeOpenAIRequestEnvelope(
            model=judge_run.model,
            reasoning_effort=judge_run.reasoning_effort,
            prompt_version=judge_run.prompt_version,
            schema_version=judge_run.schema_version,
            policy_version=judge_run.policy_version,
            judge_profile=judge_run.judge_profile,
            prompt_cache_key=judge_run.prompt_cache_key,
            developer_prompt_text=prepared.developer_prompt,
            user_context=prepared.user_context,
            structured_output_schema_id=self._structured_output_schema_id,
            structured_output_schema=dict(self._structured_output_schema),
            max_output_tokens=self._max_output_tokens,
            request_timeout_sec=self._request_timeout_sec,
            preflight_notes=tuple(prepared.preflight_notes),
            preflight_flags=dict(prepared.preflight_flags),
        )
        diagnostic = validate_responses_request_shape(envelope.to_responses_request())
        if not diagnostic.valid:
            raise JudgeOpenAIRequestEnvelopeError(diagnostic.issue_codes)
        return envelope


def build_responses_request(
    *,
    model: str,
    reasoning_effort: str,
    developer_prompt: str,
    user_context: str,
    json_schema: Mapping[str, Any],
    max_output_tokens: int | None,
    prompt_cache_key: str | None,
    schema_name: str = "judge_output_v1",
    include_prompt_cache_key: bool = False,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "model": model,
        "reasoning": {"effort": reasoning_effort},
        "input": [
            {
                "role": "developer",
                "content": [{"type": "input_text", "text": developer_prompt}],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": user_context}],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "strict": True,
                "schema": dict(json_schema),
            }
        },
        "tools": [],
    }
    if max_output_tokens is not None:
        request["max_output_tokens"] = max_output_tokens
    if include_prompt_cache_key and prompt_cache_key:
        request["prompt_cache_key"] = prompt_cache_key
    return request


def build_judge_output_schema() -> dict[str, Any]:
    score_0_to_100 = {"type": "integer", "minimum": 0, "maximum": 100}
    nullable_score_0_to_100 = {"type": ["integer", "null"], "minimum": 0, "maximum": 100}
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "judge_schema_version",
            "candidate_group_id",
            "headline",
            "summary_one_line_ko",
            "skeptical_take_ko",
            "why_it_might_matter_ko",
            "comparables",
            "scores",
            "reason_codes",
            "red_flags_ko",
            "evidence_limitations_ko",
            "recommended_action_ko",
            "freshness_note_ko",
            "model_proposed_verdict",
            "model_confidence_band",
        ],
        "properties": {
            "judge_schema_version": {"type": "string"},
            "candidate_group_id": {"type": "string"},
            "headline": {"type": "string"},
            "summary_one_line_ko": {"type": "string"},
            "skeptical_take_ko": {"type": "string"},
            "why_it_might_matter_ko": {"type": "string"},
            "comparables": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Comparable tools only when supported by the provided CandidateEvidenceBundle; "
                    "do not use latent/general knowledge; GitHub-primary comparables strengthen evidence "
                    "but are not mandatory when primary bundle evidence is strong; use [] when no reliable "
                    "comparables are available and explain the limitation with comparison_gap or "
                    "insufficient_comparables instead of inventing one."
                ),
            },
            "scores": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "novelty",
                    "practical_usefulness",
                    "evidence_strength",
                    "hype_penalty",
                    "confidence",
                    "code_quality",
                    "maintenance_signal",
                    "specificity",
                    "reproducibility_signal",
                ],
                "properties": {
                    "novelty": score_0_to_100,
                    "practical_usefulness": score_0_to_100,
                    "evidence_strength": score_0_to_100,
                    "hype_penalty": score_0_to_100,
                    "confidence": score_0_to_100,
                    "code_quality": nullable_score_0_to_100,
                    "maintenance_signal": nullable_score_0_to_100,
                    "specificity": nullable_score_0_to_100,
                    "reproducibility_signal": nullable_score_0_to_100,
                },
            },
            "reason_codes": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Include comparison_gap or insufficient_comparables when comparables is [] "
                    "because no reliable comparable tools are available; treat that gap as an evidence "
                    "limitation and confidence penalty, not an automatic veto."
                ),
            },
            "red_flags_ko": {"type": "array", "items": {"type": "string"}},
            "evidence_limitations_ko": {"type": "array", "items": {"type": "string"}},
            "recommended_action_ko": {"type": "string"},
            "freshness_note_ko": {"type": "string"},
            "model_proposed_verdict": {
                "type": ["string", "null"],
                "enum": ["inspect_now", "later", "skip", None],
                "description": (
                    "Model's provisional action hint only; policy remains final. GitHub-primary "
                    "later or inspect_now may be proposed without comparables only when primary bundle "
                    "evidence is strong and specific; missing comparables should reduce evidence_strength "
                    "and/or confidence unless that primary evidence compensates, and must use comparison_gap "
                    "or insufficient_comparables instead of unsupported comparables."
                ),
            },
            "model_confidence_band": {
                "type": ["string", "null"],
                "enum": ["low", "medium", "high", None],
            },
        },
    }


def validate_responses_request_shape(request: Mapping[str, Any]) -> RequestShapeDiagnostic:
    issues: list[str] = []
    _validate_top_level(request, issues)
    _validate_input(request.get("input"), issues)
    schema = _validate_text_format(request.get("text"), issues)
    if schema is not None:
        _validate_structured_output_schema(schema, issues)
    _validate_generation_controls(request, issues)
    return RequestShapeDiagnostic(issue_codes=tuple(dict.fromkeys(issues)))


def summarize_responses_request_shape(request: Mapping[str, Any]) -> dict[str, Any]:
    diagnostic = validate_responses_request_shape(request)
    text_format = _as_mapping(_as_mapping(request.get("text")).get("format"))
    reasoning = _as_mapping(request.get("reasoning"))
    input_items = request.get("input")
    tools = request.get("tools")
    optional_null_fields = _optional_null_fields(request)
    text_format_type = _text_format_type_bucket(text_format.get("type"))
    max_output_tokens_present = "one" if "max_output_tokens" in request else "zero"
    prompt_cache_key_present = "one" if "prompt_cache_key" in request else "zero"
    tools_count = _bucket_count(len(tools) if isinstance(tools, list) else 0)
    strict_bucket = "one" if text_format.get("strict") is True else "zero"
    return {
        "request_shape_valid_bucket": "one" if diagnostic.valid else "zero",
        "request_shape_issue_count_bucket": _bucket_count(len(diagnostic.issue_codes)),
        "request_shape_issue_buckets": list(diagnostic.issue_codes),
        "top_level_request_key_presence_buckets": {
            key: "one" if key in request else "zero"
            for key in sorted(ALLOWED_TOP_LEVEL_KEYS)
        },
        "optional_null_field_count_bucket": _bucket_count(len(optional_null_fields)),
        "optional_null_field_name_buckets": optional_null_fields,
        "model_bucket": _model_bucket(request.get("model")),
        "reasoning_effort_bucket": _reasoning_effort_bucket(reasoning.get("effort")),
        "input_message_count_bucket": _bucket_count(len(input_items) if isinstance(input_items, list) else 0),
        "text_format_type_bucket": text_format_type,
        "text_format_json_schema_bucket": "one"
        if text_format_type == "json_schema"
        else "zero",
        "json_schema_strict_bucket": strict_bucket,
        "strict_schema_bucket": strict_bucket,
        "tools_count_bucket": tools_count,
        "tools_bucket": tools_count,
        "max_output_tokens_presence_bucket": max_output_tokens_present,
        "max_output_tokens_present_bucket": max_output_tokens_present,
        "max_output_tokens_null_bucket": "one"
        if request.get("max_output_tokens") is None and "max_output_tokens" in request
        else "zero",
        "prompt_cache_key_presence_bucket": prompt_cache_key_present,
        "prompt_cache_key_present_bucket": prompt_cache_key_present,
        "openai_call_attempted": False,
        "openai_key_file_read_bucket": "zero",
        "database_write_attempted": False,
        "redis_write_attempted": False,
        "raw_values_emitted": False,
    }


def _validate_top_level(request: Mapping[str, Any], issues: list[str]) -> None:
    unsupported = set(request) - ALLOWED_TOP_LEVEL_KEYS
    if unsupported:
        issues.append("top_level.unsupported_parameter")
    model = request.get("model")
    if not isinstance(model, str) or not model.strip():
        issues.append("model.missing_or_empty")
    elif model not in SUPPORTED_LOCKED_MODELS:
        issues.append("model.outside_locked_set")
    reasoning = request.get("reasoning")
    if not isinstance(reasoning, Mapping):
        issues.append("reasoning.missing_or_invalid")
        return
    if set(reasoning) != {"effort"}:
        issues.append("reasoning.unsupported_parameter")
    effort = reasoning.get("effort")
    if effort not in SUPPORTED_REASONING_EFFORTS:
        issues.append("reasoning.unsupported_effort")


def _validate_input(input_items: Any, issues: list[str]) -> None:
    if not isinstance(input_items, list) or len(input_items) != 2:
        issues.append("input.message_count")
        return
    _validate_message(input_items[0], expected_role="developer", issues=issues)
    _validate_message(input_items[1], expected_role="user", issues=issues)


def _validate_message(message: Any, *, expected_role: str, issues: list[str]) -> None:
    if not isinstance(message, Mapping):
        issues.append(f"input.{expected_role}.message_invalid")
        return
    if message.get("role") != expected_role:
        issues.append(f"input.{expected_role}.role")
    content = message.get("content")
    if not isinstance(content, list) or len(content) != 1:
        issues.append(f"input.{expected_role}.content_count")
        return
    block = content[0]
    if not isinstance(block, Mapping):
        issues.append(f"input.{expected_role}.content_invalid")
        return
    if block.get("type") != "input_text":
        issues.append(f"input.{expected_role}.content_type")
    text = block.get("text")
    if not isinstance(text, str) or not text.strip():
        issues.append(f"input.{expected_role}.text_missing")


def _validate_text_format(text: Any, issues: list[str]) -> Mapping[str, Any] | None:
    text_mapping = _as_mapping(text)
    format_mapping = _as_mapping(text_mapping.get("format"))
    if not format_mapping:
        issues.append("text.format_missing")
        return None
    if format_mapping.get("type") != "json_schema":
        issues.append("text.format_type")
    name = format_mapping.get("name")
    if not isinstance(name, str) or not _FORMAT_NAME_RE.fullmatch(name):
        issues.append("text.format_name")
    if format_mapping.get("strict") is not True:
        issues.append("text.format_strict")
    schema = format_mapping.get("schema")
    if not isinstance(schema, Mapping):
        issues.append("text.format_schema")
        return None
    return schema


def _validate_generation_controls(request: Mapping[str, Any], issues: list[str]) -> None:
    tools = request.get("tools")
    if tools != []:
        issues.append("tools.non_empty_or_missing")
    if "max_output_tokens" in request:
        value = request.get("max_output_tokens")
        if value is None:
            issues.append("max_output_tokens.null")
        elif not isinstance(value, int) or value <= 0:
            issues.append("max_output_tokens.invalid")
    if "prompt_cache_key" in request:
        value = request.get("prompt_cache_key")
        if value is None:
            issues.append("prompt_cache_key.null")
        elif not isinstance(value, str) or not value.strip():
            issues.append("prompt_cache_key.invalid")


def _validate_structured_output_schema(schema: Mapping[str, Any], issues: list[str]) -> None:
    if schema.get("type") != "object":
        issues.append("schema.root_not_object")
    if "anyOf" in schema:
        issues.append("schema.root_anyof")
    _walk_schema(schema, issues=issues)


def _walk_schema(schema: Mapping[str, Any], *, issues: list[str]) -> None:
    for keyword in UNSUPPORTED_SCHEMA_KEYWORDS:
        if keyword in schema:
            issues.append("schema.unsupported_keyword")
    _validate_schema_type(schema.get("type"), issues)

    if schema.get("type") == "object":
        _validate_object_schema(schema, issues)
    if schema.get("type") == "array":
        items = schema.get("items")
        if not isinstance(items, Mapping):
            issues.append("schema.array_items_missing")
        else:
            _walk_schema(items, issues=issues)

    any_of = schema.get("anyOf")
    if any_of is not None:
        if not isinstance(any_of, Sequence) or isinstance(any_of, (str, bytes)) or not any_of:
            issues.append("schema.anyof_invalid")
        else:
            for branch in any_of:
                if not isinstance(branch, Mapping):
                    issues.append("schema.anyof_branch_invalid")
                else:
                    _walk_schema(branch, issues=issues)


def _validate_object_schema(schema: Mapping[str, Any], issues: list[str]) -> None:
    if schema.get("additionalProperties") is not False:
        issues.append("schema.object_additional_properties")
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        issues.append("schema.object_properties")
        return
    required = schema.get("required")
    if not isinstance(required, list) or set(required) != set(properties):
        issues.append("schema.object_required")
    for value in properties.values():
        if isinstance(value, Mapping):
            _walk_schema(value, issues=issues)
        else:
            issues.append("schema.property_invalid")


def _validate_schema_type(value: Any, issues: list[str]) -> None:
    if isinstance(value, str):
        if value not in SUPPORTED_SCHEMA_TYPES:
            issues.append("schema.unsupported_type")
        return
    if isinstance(value, list):
        value_set = set(value)
        if not value or not all(isinstance(item, str) for item in value):
            issues.append("schema.type_union_invalid")
            return
        if value_set - SUPPORTED_SCHEMA_TYPES:
            issues.append("schema.unsupported_type")
        if "null" in value_set and len(value_set) != 2:
            issues.append("schema.null_union_invalid")
        return
    if value is not None:
        issues.append("schema.type_invalid")


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _bucket_count(count: int) -> str:
    if count <= 0:
        return "zero"
    if count == 1:
        return "one"
    return "multiple"


def _optional_null_fields(request: Mapping[str, Any]) -> list[str]:
    return [
        key
        for key in sorted(OPTIONAL_TOP_LEVEL_KEYS)
        if key in request and request.get(key) is None
    ]


def _text_format_type_bucket(value: Any) -> str:
    if value == "json_schema":
        return "json_schema"
    if isinstance(value, str) and value:
        return "other"
    return "zero"


def _model_bucket(value: Any) -> str:
    if value == LOCKED_HOT_PATH_MODEL:
        return "locked_hot_path"
    if value == LOCKED_ESCALATION_MODEL:
        return "locked_escalation"
    if isinstance(value, str) and value:
        return "other"
    return "zero"


def _reasoning_effort_bucket(value: Any) -> str:
    if value in SUPPORTED_REASONING_EFFORTS:
        return str(value)
    if isinstance(value, str) and value:
        return "other"
    return "zero"
