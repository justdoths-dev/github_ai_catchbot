from __future__ import annotations

import inspect
import json
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

from .models import OpenAIJudgeUsage
from .openai_client import OpenAIJudgeClient, OpenAIRequestShapeError
from .request_shape import validate_responses_request_shape
from .response_mapper import OpenAIResponseMapper


CANARY_NAME = "restricted_openai_judge_canary"
MODE = "restricted_live_judge"
DEFAULT_MODEL = "gpt-5.4-mini"
ALLOWED_MODELS = frozenset({DEFAULT_MODEL, "gpt-5.4"})
DEFAULT_REASONING_EFFORT = "low"
ALLOWED_REASONING_EFFORTS = frozenset({DEFAULT_REASONING_EFFORT, "medium"})
DEFAULT_FIXTURE_PROFILE = "github_primary_minimal"
ALLOWED_FIXTURE_PROFILES = frozenset({DEFAULT_FIXTURE_PROFILE})
DEFAULT_MAX_REQUESTS = 1
DEFAULT_TIMEOUT_MS = 30_000
DEFAULT_MAX_OUTPUT_TOKENS = 900
DEFAULT_MAX_INPUT_CHARS = 12_000
MAX_OUTPUT_TOKENS_HARD_CAP = 2_000
MAX_INPUT_CHARS_HARD_CAP = 50_000
PROMPT_VERSION = "restricted_openai_judge_canary_prompt_v1"
SCHEMA_VERSION = "judge_output_v1"
POLICY_VERSION = "verdict_policy_v1"
CANDIDATE_GROUP_ID = "00000000-0000-0000-0000-000000000001"
ALLOWED_VERDICTS = frozenset({"inspect_now", "later", "skip"})
ALLOWED_CONFIDENCE_BANDS = frozenset({"low", "medium", "high"})
COMMON_SCORE_KEYS = (
    "novelty",
    "practical_usefulness",
    "evidence_strength",
    "hype_penalty",
    "confidence",
)
NULLABLE_SCORE_KEYS = (
    "code_quality",
    "maintenance_signal",
    "specificity",
    "reproducibility_signal",
)
SCORE_KEYS = (*COMMON_SCORE_KEYS, *NULLABLE_SCORE_KEYS)
REQUIRED_OUTPUT_FIELDS = (
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
)
BLOCKED_ERROR_CODES = frozenset(
    {
        "operator_approval_missing",
        "network_not_allowed",
        "credential_missing",
        "model_not_allowed",
        "reasoning_effort_not_allowed",
        "fixture_profile_invalid",
        "request_cap_invalid",
        "request_cap_exceeded",
        "output_token_cap_invalid",
        "input_char_cap_invalid",
    }
)
ERROR_CODES = BLOCKED_ERROR_CODES | frozenset(
    {
        "openai_rate_limited",
        "openai_auth_failed",
        "openai_quota_or_billing",
        "openai_bad_request",
        "openai_transient_error",
        "openai_response_invalid",
        "openai_schema_invalid",
        "openai_refusal",
    }
)
SIDE_EFFECT_FLAGS = {
    "database_write": False,
    "redis_write": False,
    "outbox_emit": False,
    "artifact_snapshot_write": False,
    "judge_run_write": False,
    "judge_output_write": False,
    "analysis_write": False,
    "notification_write": False,
    "telegram_call": False,
    "github_call": False,
    "x_call": False,
    "web_call": False,
    "worker_started": False,
    "repo_state_mutation": False,
}
OPENAI_QUOTA_OR_BILLING_MARKERS = frozenset(
    {
        "insufficient_quota",
        "quota_exceeded",
        "billing_hard_limit",
        "billing_hard_limit_reached",
        "billing_not_active",
        "payment_required",
        "billing",
    }
)
OPENAI_RATE_LIMIT_MARKERS = frozenset({"ratelimiterror", "rate_limit_error", "rate_limit_exceeded"})
OPENAI_AUTH_MARKERS = frozenset(
    {
        "authenticationerror",
        "permissiondeniederror",
        "authentication_error",
        "permission_denied",
        "invalid_api_key",
        "incorrect_api_key",
        "missing_api_key",
    }
)
OPENAI_TIMEOUT_OR_CONNECTION_MARKERS = frozenset(
    {
        "apitimeouterror",
        "apiconnectionerror",
        "timeout",
        "timed_out",
        "connection_error",
        "connectionerror",
    }
)


class RestrictedOpenAIJudgeClientProtocol(Protocol):
    async def create_structured_response(
        self,
        *,
        model: str,
        reasoning_effort: str,
        developer_prompt: str,
        user_context: str,
        json_schema: dict[str, Any],
        max_output_tokens: int | None,
        prompt_cache_key: str | None,
    ) -> Any: ...


class RedactedOpenAIJudgeCanaryError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        safe_code = error_code if error_code in ERROR_CODES else "openai_transient_error"
        self.error_code = safe_code
        super().__init__(safe_code)


@dataclass(slots=True)
class RestrictedOpenAIJudgeCanaryRequestBudget:
    max_requests: int = DEFAULT_MAX_REQUESTS
    request_count: int = 0
    network_attempted: bool = False

    def acquire(self) -> None:
        if self.request_count >= self.max_requests:
            raise RedactedOpenAIJudgeCanaryError("request_cap_exceeded")
        self.request_count += 1
        self.network_attempted = True


@dataclass(slots=True, frozen=True)
class RestrictedOpenAIJudgeCanaryConfig:
    operator_approved: bool
    allow_network: bool
    api_key: str | None
    model: str = DEFAULT_MODEL
    reasoning_effort: str = DEFAULT_REASONING_EFFORT
    fixture_profile: str = DEFAULT_FIXTURE_PROFILE
    max_requests: int = DEFAULT_MAX_REQUESTS
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS
    max_input_chars: int = DEFAULT_MAX_INPUT_CHARS
    mode: str = MODE


@dataclass(slots=True, frozen=True)
class RestrictedOpenAIJudgeCanaryResult:
    canary_name: str
    mode: str
    model: str
    reasoning_effort: str
    prompt_version: str
    schema_version: str
    policy_version: str
    fixture_profile: str
    network_attempted: bool
    request_count: int
    max_requests: int
    status: str
    ok: bool
    error_code: str | None
    finish_reason: str | None
    refusal_detected: bool
    schema_valid: bool
    required_field_count: int
    observed_model_proposed_verdict: str | None
    observed_confidence_band: str | None
    input_tokens: int | None
    cached_input_tokens: int | None
    output_tokens: int | None
    reasoning_tokens: int | None
    redactions_applied: tuple[str, ...]
    side_effects: Mapping[str, bool] = field(default_factory=dict)

    def to_sanitized_dict(self) -> dict[str, Any]:
        return {
            "canary_name": self.canary_name,
            "mode": self.mode,
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "prompt_version": self.prompt_version,
            "schema_version": self.schema_version,
            "policy_version": self.policy_version,
            "fixture_profile": self.fixture_profile,
            "network_attempted": self.network_attempted,
            "request_count": self.request_count,
            "max_requests": self.max_requests,
            "status": self.status,
            "ok": self.ok,
            "error_code": self.error_code,
            "finish_reason": self.finish_reason,
            "refusal_detected": self.refusal_detected,
            "schema_valid": self.schema_valid,
            "required_field_count": self.required_field_count,
            "observed_model_proposed_verdict": self.observed_model_proposed_verdict,
            "observed_confidence_band": self.observed_confidence_band,
            "input_tokens": self.input_tokens,
            "cached_input_tokens": self.cached_input_tokens,
            "output_tokens": self.output_tokens,
            "reasoning_tokens": self.reasoning_tokens,
            "redactions_applied": list(self.redactions_applied),
            "side_effects": dict(self.side_effects),
        }


class RestrictedOpenAIJudgeLiveClient:
    def __init__(self, *, api_key: str, timeout_ms: int = DEFAULT_TIMEOUT_MS, client: Any | None = None) -> None:
        self._api_key = api_key
        self._timeout_ms = _positive_int(timeout_ms, DEFAULT_TIMEOUT_MS)
        self._client = client

    async def create_structured_response(
        self,
        *,
        model: str,
        reasoning_effort: str,
        developer_prompt: str,
        user_context: str,
        json_schema: dict[str, Any],
        max_output_tokens: int | None,
        prompt_cache_key: str | None,
    ) -> Any:
        request = OpenAIJudgeClient.build_request(
            model=model,
            reasoning_effort=reasoning_effort,
            developer_prompt=developer_prompt,
            user_context=user_context,
            json_schema=json_schema,
            max_output_tokens=max_output_tokens,
            prompt_cache_key=prompt_cache_key,
        )
        diagnostic = validate_responses_request_shape(request)
        if not diagnostic.valid:
            raise OpenAIRequestShapeError(diagnostic.issue_codes)

        client = self._client or self._build_client()
        response = client.responses.create(**request)
        if inspect.isawaitable(response):
            return await response
        return response

    def _build_client(self) -> Any:
        try:
            from openai import AsyncOpenAI  # type: ignore[import-not-found]
        except Exception as exc:  # noqa: BLE001 - keep operator error bucketed.
            raise RedactedOpenAIJudgeCanaryError("openai_transient_error") from exc
        return AsyncOpenAI(api_key=self._api_key, timeout=self._timeout_ms / 1000.0)


class RestrictedOpenAIJudgeCanary:
    def __init__(
        self,
        config: RestrictedOpenAIJudgeCanaryConfig,
        *,
        client: RestrictedOpenAIJudgeClientProtocol | None = None,
        request_budget: RestrictedOpenAIJudgeCanaryRequestBudget | None = None,
        response_mapper: OpenAIResponseMapper | None = None,
    ) -> None:
        self._config = config
        self._client = client
        self._request_budget = request_budget or RestrictedOpenAIJudgeCanaryRequestBudget(
            max_requests=_int_or_zero(config.max_requests)
        )
        self._response_mapper = response_mapper or OpenAIResponseMapper()

    async def run(self) -> RestrictedOpenAIJudgeCanaryResult:
        error_code: str | None = None
        finish_reason: str | None = None
        refusal_detected = False
        schema_valid = False
        required_field_count = 0
        observed_verdict: str | None = None
        observed_confidence_band: str | None = None
        usage = OpenAIJudgeUsage()

        try:
            target = self._validate_preconditions()
            prompts = build_restricted_judge_canary_prompts(target["fixture_profile"])
            _validate_input_char_cap(
                prompts["developer_prompt"],
                prompts["user_context"],
                max_input_chars=_positive_int(self._config.max_input_chars, 0),
            )
            active_client = self._client or RestrictedOpenAIJudgeLiveClient(
                api_key=target["api_key"],
                timeout_ms=_positive_int(self._config.timeout_ms, DEFAULT_TIMEOUT_MS),
            )

            self._request_budget.acquire()
            started = time.monotonic()
            response = await active_client.create_structured_response(
                model=target["model"],
                reasoning_effort=target["reasoning_effort"],
                developer_prompt=prompts["developer_prompt"],
                user_context=prompts["user_context"],
                json_schema=restricted_judge_output_schema(),
                max_output_tokens=target["max_output_tokens"],
                prompt_cache_key=None,
            )
            mapped = self._response_mapper.parse(response, started_monotonic=started)
            usage = mapped.usage
            finish_reason = _safe_finish_reason(mapped.finish_reason)
            refusal_detected = mapped.refusal_detected

            if refusal_detected:
                raise RedactedOpenAIJudgeCanaryError("openai_refusal")
            if mapped.payload_json is None:
                raise RedactedOpenAIJudgeCanaryError("openai_response_invalid")

            required_field_count = _required_field_count(mapped.payload_json)
            schema_valid = validate_restricted_judge_output(mapped.payload_json)
            if not schema_valid:
                raise RedactedOpenAIJudgeCanaryError("openai_schema_invalid")

            observed_verdict = _safe_enum(mapped.payload_json.get("model_proposed_verdict"), ALLOWED_VERDICTS)
            observed_confidence_band = _safe_enum(
                mapped.payload_json.get("model_confidence_band"),
                ALLOWED_CONFIDENCE_BANDS,
            )
        except RedactedOpenAIJudgeCanaryError as exc:
            error_code = exc.error_code
        except OpenAIRequestShapeError:
            error_code = "openai_bad_request"
        except Exception as exc:  # noqa: BLE001 - never expose raw exception text to operators.
            error_code = classify_openai_exception(exc)

        return self._result(
            error_code=error_code,
            finish_reason=finish_reason,
            refusal_detected=refusal_detected,
            schema_valid=schema_valid,
            required_field_count=required_field_count,
            observed_model_proposed_verdict=observed_verdict,
            observed_confidence_band=observed_confidence_band,
            usage=usage,
        )

    def _validate_preconditions(self) -> dict[str, Any]:
        if not self._config.operator_approved:
            raise RedactedOpenAIJudgeCanaryError("operator_approval_missing")
        if not self._config.allow_network:
            raise RedactedOpenAIJudgeCanaryError("network_not_allowed")

        api_key = str(self._config.api_key or "").strip()
        if not api_key:
            raise RedactedOpenAIJudgeCanaryError("credential_missing")

        model = str(self._config.model or "").strip()
        if model not in ALLOWED_MODELS:
            raise RedactedOpenAIJudgeCanaryError("model_not_allowed")

        reasoning_effort = str(self._config.reasoning_effort or "").strip()
        if reasoning_effort not in ALLOWED_REASONING_EFFORTS:
            raise RedactedOpenAIJudgeCanaryError("reasoning_effort_not_allowed")

        fixture_profile = str(self._config.fixture_profile or "").strip()
        if fixture_profile not in ALLOWED_FIXTURE_PROFILES:
            raise RedactedOpenAIJudgeCanaryError("fixture_profile_invalid")

        if _int_or_zero(self._config.max_requests) != DEFAULT_MAX_REQUESTS:
            raise RedactedOpenAIJudgeCanaryError("request_cap_invalid")

        max_output_tokens = _int_or_zero(self._config.max_output_tokens)
        if max_output_tokens <= 0 or max_output_tokens > MAX_OUTPUT_TOKENS_HARD_CAP:
            raise RedactedOpenAIJudgeCanaryError("output_token_cap_invalid")

        max_input_chars = _int_or_zero(self._config.max_input_chars)
        if max_input_chars <= 0 or max_input_chars > MAX_INPUT_CHARS_HARD_CAP:
            raise RedactedOpenAIJudgeCanaryError("input_char_cap_invalid")

        return {
            "api_key": api_key,
            "model": model,
            "reasoning_effort": reasoning_effort,
            "fixture_profile": fixture_profile,
            "max_output_tokens": max_output_tokens,
        }

    def _result(
        self,
        *,
        error_code: str | None,
        finish_reason: str | None,
        refusal_detected: bool,
        schema_valid: bool,
        required_field_count: int,
        observed_model_proposed_verdict: str | None,
        observed_confidence_band: str | None,
        usage: OpenAIJudgeUsage,
    ) -> RestrictedOpenAIJudgeCanaryResult:
        return RestrictedOpenAIJudgeCanaryResult(
            canary_name=CANARY_NAME,
            mode=MODE,
            model=_safe_model_for_output(self._config.model),
            reasoning_effort=_safe_reasoning_effort_for_output(self._config.reasoning_effort),
            prompt_version=PROMPT_VERSION,
            schema_version=SCHEMA_VERSION,
            policy_version=POLICY_VERSION,
            fixture_profile=_safe_fixture_profile_for_output(self._config.fixture_profile),
            network_attempted=self._request_budget.network_attempted,
            request_count=self._request_budget.request_count,
            max_requests=self._request_budget.max_requests,
            status=_result_status(error_code, self._request_budget.network_attempted),
            ok=error_code is None,
            error_code=error_code,
            finish_reason=finish_reason,
            refusal_detected=refusal_detected,
            schema_valid=schema_valid,
            required_field_count=required_field_count,
            observed_model_proposed_verdict=observed_model_proposed_verdict,
            observed_confidence_band=observed_confidence_band,
            input_tokens=_safe_optional_int(usage.input_tokens),
            cached_input_tokens=_safe_optional_int(usage.cached_input_tokens),
            output_tokens=_safe_optional_int(usage.output_tokens),
            reasoning_tokens=_safe_optional_int(usage.reasoning_tokens),
            redactions_applied=_redactions(error_code),
            side_effects=SIDE_EFFECT_FLAGS,
        )


async def run_restricted_openai_judge_canary(
    config: RestrictedOpenAIJudgeCanaryConfig,
    *,
    client: RestrictedOpenAIJudgeClientProtocol | None = None,
    request_budget: RestrictedOpenAIJudgeCanaryRequestBudget | None = None,
) -> RestrictedOpenAIJudgeCanaryResult:
    return await RestrictedOpenAIJudgeCanary(
        config,
        client=client,
        request_budget=request_budget,
    ).run()


def synthetic_candidate_evidence_bundle(fixture_profile: str = DEFAULT_FIXTURE_PROFILE) -> dict[str, Any]:
    if fixture_profile != DEFAULT_FIXTURE_PROFILE:
        raise RedactedOpenAIJudgeCanaryError("fixture_profile_invalid")
    return {
        "schema_version": "candidate_evidence_bundle_v1",
        "candidate_group_id": CANDIDATE_GROUP_ID,
        "primary_artifact": {
            "artifact_type": "github_repo",
            "canonical_id": "github:repo:example/synthetic-dev-tool",
            "canonical_url": "https://github.com/example/synthetic-dev-tool",
        },
        "primary_summary": {
            "snapshot_type": "github_repo",
            "status": "partial_ready",
            "headline": "Synthetic developer workflow tool",
            "readme_excerpt": "A small synthetic repository used only for canary validation.",
            "detected_languages": ["Python"],
            "test_paths": ["tests/test_smoke.py"],
            "ci_paths": [".github/workflows/ci.yml"],
        },
        "supporting_summaries": [],
        "evidence_limitations": [
            "synthetic canary fixture",
            "not a production candidate",
        ],
        "token_budget_profile": "small",
    }


def build_restricted_judge_canary_prompts(fixture_profile: str = DEFAULT_FIXTURE_PROFILE) -> dict[str, str]:
    bundle_json = json.dumps(
        synthetic_candidate_evidence_bundle(fixture_profile),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    developer_prompt = "\n".join(
        [
            "You are the restricted stage-6 OpenAI judge canary for github_ai_catchbot.",
            "Return only strict judge_output_v1 JSON matching the supplied schema.",
            "The supplied CandidateEvidenceBundle is synthetic and validates output contract only.",
            "Good points are secondary; start from why the candidate may be weak.",
            "Evidence limitations must be explicit.",
            "Do not use any information outside the supplied synthetic bundle.",
            "Do not browse, search, fetch, retrieve, call tools, or use external evidence.",
            "Do not invent comparables. If uncertain, return an empty array and include a limitation.",
            "Do not decide final delivery. model_proposed_verdict is only a proposal.",
            "Prefer conservative scoring for synthetic or partial evidence.",
            "Output must satisfy the strict schema.",
        ]
    )
    user_context = "\n".join(
        [
            f"prompt_version={PROMPT_VERSION}",
            f"schema_version={SCHEMA_VERSION}",
            f"policy_version={POLICY_VERSION}",
            f"fixture_profile={fixture_profile}",
            "Synthetic CandidateEvidenceBundle JSON:",
            bundle_json,
        ]
    )
    return {"developer_prompt": developer_prompt, "user_context": user_context}


def restricted_judge_output_schema() -> dict[str, Any]:
    score_0_to_100 = {"type": "integer", "minimum": 0, "maximum": 100}
    nullable_score_0_to_100 = {"type": ["integer", "null"], "minimum": 0, "maximum": 100}
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(REQUIRED_OUTPUT_FIELDS),
        "properties": {
            "judge_schema_version": {"type": "string"},
            "candidate_group_id": {"type": "string"},
            "headline": {"type": "string"},
            "summary_one_line_ko": {"type": "string"},
            "skeptical_take_ko": {"type": "string"},
            "why_it_might_matter_ko": {"type": "string"},
            "comparables": {"type": "array", "items": {"type": "string"}},
            "scores": {
                "type": "object",
                "additionalProperties": False,
                "required": list(SCORE_KEYS),
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
            "reason_codes": {"type": "array", "items": {"type": "string"}},
            "red_flags_ko": {"type": "array", "items": {"type": "string"}},
            "evidence_limitations_ko": {"type": "array", "items": {"type": "string"}},
            "recommended_action_ko": {"type": "string"},
            "freshness_note_ko": {"type": "string"},
            "model_proposed_verdict": {"type": "string", "enum": sorted(ALLOWED_VERDICTS)},
            "model_confidence_band": {"type": "string", "enum": sorted(ALLOWED_CONFIDENCE_BANDS)},
        },
    }


def validate_restricted_judge_output(payload: Mapping[str, Any]) -> bool:
    if not isinstance(payload, Mapping):
        return False
    if set(payload) != set(REQUIRED_OUTPUT_FIELDS):
        return False
    if payload.get("judge_schema_version") != SCHEMA_VERSION:
        return False
    if payload.get("candidate_group_id") != CANDIDATE_GROUP_ID:
        return False
    for key in (
        "headline",
        "summary_one_line_ko",
        "skeptical_take_ko",
        "why_it_might_matter_ko",
        "recommended_action_ko",
        "freshness_note_ko",
    ):
        if not _is_nonempty_string(payload.get(key)):
            return False
    for key in (
        "comparables",
        "reason_codes",
        "red_flags_ko",
        "evidence_limitations_ko",
    ):
        if not _is_string_list(payload.get(key)):
            return False
    if _safe_enum(payload.get("model_proposed_verdict"), ALLOWED_VERDICTS) is None:
        return False
    if _safe_enum(payload.get("model_confidence_band"), ALLOWED_CONFIDENCE_BANDS) is None:
        return False
    return _scores_valid(payload.get("scores"))


def classify_openai_exception(exc: BaseException) -> str:
    status_code = _status_code(exc)
    marker_values = _exception_marker_values(exc)

    if _has_marker(marker_values, OPENAI_QUOTA_OR_BILLING_MARKERS) or status_code == 402:
        return "openai_quota_or_billing"
    if _has_marker(marker_values, OPENAI_RATE_LIMIT_MARKERS) or status_code == 429:
        return "openai_rate_limited"
    if _has_marker(marker_values, OPENAI_AUTH_MARKERS) or status_code in {401, 403}:
        return "openai_auth_failed"
    if status_code in {400, 404, 409, 422}:
        return "openai_bad_request"
    if status_code is not None and status_code >= 500:
        return "openai_transient_error"
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return "openai_transient_error"
    if _has_marker(marker_values, OPENAI_TIMEOUT_OR_CONNECTION_MARKERS):
        return "openai_transient_error"
    return "openai_transient_error"


def _validate_input_char_cap(developer_prompt: str, user_context: str, *, max_input_chars: int) -> None:
    if max_input_chars <= 0 or len(developer_prompt) + len(user_context) > max_input_chars:
        raise RedactedOpenAIJudgeCanaryError("input_char_cap_invalid")


def _required_field_count(payload: Mapping[str, Any]) -> int:
    if not isinstance(payload, Mapping):
        return 0
    return sum(1 for key in REQUIRED_OUTPUT_FIELDS if key in payload)


def _scores_valid(value: Any) -> bool:
    if not isinstance(value, Mapping) or set(value) != set(SCORE_KEYS):
        return False
    for key in COMMON_SCORE_KEYS:
        if not _score_0_to_100(value.get(key), nullable=False):
            return False
    for key in NULLABLE_SCORE_KEYS:
        if not _score_0_to_100(value.get(key), nullable=True):
            return False
    return True


def _score_0_to_100(value: Any, *, nullable: bool) -> bool:
    if nullable and value is None:
        return True
    if type(value) is not int:
        return False
    return 0 <= value <= 100


def _is_nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_string_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _result_status(error_code: str | None, network_attempted: bool) -> str:
    if error_code is None:
        return "pass"
    if error_code in BLOCKED_ERROR_CODES and not network_attempted:
        return "blocked"
    return "fail"


def _redactions(error_code: str | None) -> tuple[str, ...]:
    redactions = [
        "api_key_omitted",
        "authorization_header_omitted",
        "raw_request_body_omitted",
        "raw_response_body_omitted",
        "raw_model_output_omitted",
        "generated_korean_fields_omitted",
        "prompt_omitted",
        "response_headers_omitted",
        "organization_project_ids_omitted",
    ]
    if error_code is not None:
        redactions.append("exception_detail_omitted")
    return tuple(redactions)


def _safe_model_for_output(value: Any) -> str:
    raw = str(value or "").strip()
    return raw if raw in ALLOWED_MODELS else DEFAULT_MODEL


def _safe_reasoning_effort_for_output(value: Any) -> str:
    raw = str(value or "").strip()
    return raw if raw in ALLOWED_REASONING_EFFORTS else DEFAULT_REASONING_EFFORT


def _safe_fixture_profile_for_output(value: Any) -> str:
    raw = str(value or "").strip()
    return raw if raw in ALLOWED_FIXTURE_PROFILES else DEFAULT_FIXTURE_PROFILE


def _safe_finish_reason(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = "".join(ch for ch in value[:80] if ch.isalnum() or ch in {"_", "-", "."})
    return normalized or "other"


def _safe_enum(value: Any, allowed: frozenset[str]) -> str | None:
    return value if isinstance(value, str) and value in allowed else None


def _safe_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _int_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _positive_int(value: Any, default: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return number if number > 0 else default


def _status_code(exc: BaseException) -> int | None:
    value = getattr(exc, "status_code", None)
    try:
        status_code = int(value)
    except (TypeError, ValueError):
        return None
    return status_code if 100 <= status_code <= 599 else None


def _exception_marker_values(exc: BaseException) -> tuple[str, ...]:
    values: list[str] = [type(exc).__name__.lower()]
    for attr in ("code", "type", "param"):
        value = getattr(exc, attr, None)
        if value is not None:
            values.append(str(value).lower())
    body = getattr(exc, "body", None)
    if isinstance(body, Mapping):
        for key in ("code", "type", "message"):
            value = body.get(key)
            if value is not None:
                values.append(str(value).lower())
        error = body.get("error")
        if isinstance(error, Mapping):
            for key in ("code", "type", "message"):
                value = error.get(key)
                if value is not None:
                    values.append(str(value).lower())
    return tuple(values)


def _has_marker(values: tuple[str, ...], markers: frozenset[str]) -> bool:
    return any(marker in value for value in values for marker in markers)
