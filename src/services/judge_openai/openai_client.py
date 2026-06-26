from __future__ import annotations

from typing import Any

from .request_shape import build_responses_request, validate_responses_request_shape


RETRYABLE_OPENAI_SAFE_CODES = frozenset(
    {
        "openai_retryable_rate_limited",
        "openai_retryable_timeout",
        "openai_retryable_connection",
        "openai_retryable_server_error",
        "openai_retryable_unknown",
    }
)
PERMANENT_OPENAI_SAFE_CODES = frozenset(
    {
        "openai_permanent_auth",
        "openai_permanent_permission",
        "openai_permanent_bad_request",
        "openai_permanent_not_found",
        "openai_permanent_client_error",
        "openai_permanent_unknown",
    }
)
OPENAI_SAFE_CODES = RETRYABLE_OPENAI_SAFE_CODES | PERMANENT_OPENAI_SAFE_CODES


class OpenAIClientConfigurationError(RuntimeError):
    pass


class OpenAITransientError(RuntimeError):
    default_safe_code = "openai_retryable_unknown"

    def __init__(self, message: str = "openai_retryable_error", *, safe_code: str | None = None) -> None:
        resolved_safe_code = safe_code if safe_code in RETRYABLE_OPENAI_SAFE_CODES else self.default_safe_code
        super().__init__(message)
        self.safe_code = resolved_safe_code


class OpenAIPermanentError(RuntimeError):
    default_safe_code = "openai_permanent_unknown"

    def __init__(self, message: str = "openai_permanent_error", *, safe_code: str | None = None) -> None:
        resolved_safe_code = safe_code if safe_code in PERMANENT_OPENAI_SAFE_CODES else self.default_safe_code
        super().__init__(message)
        self.safe_code = resolved_safe_code


class OpenAIRequestShapeError(OpenAIPermanentError):
    def __init__(self, issue_codes: tuple[str, ...]) -> None:
        super().__init__("invalid_request_shape", safe_code="openai_permanent_bad_request")
        self.issue_codes = issue_codes


class OpenAIJudgeClient:
    def __init__(
        self,
        *,
        api_key: str,
        project: str | None,
        timeout_sec: float,
        client: Any | None = None,
    ) -> None:
        self._client = client or self._build_client(
            api_key=api_key,
            project=project,
            timeout_sec=timeout_sec,
        )

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
        request = self.build_request(
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
        try:
            return await self._client.responses.create(**request)
        except Exception as exc:
            self._raise_mapped_exception(exc)

    @staticmethod
    def build_request(
        *,
        model: str,
        reasoning_effort: str,
        developer_prompt: str,
        user_context: str,
        json_schema: dict[str, Any],
        max_output_tokens: int | None,
        prompt_cache_key: str | None,
    ) -> dict[str, Any]:
        # OpenAI transport compatibility guard after a live 400 on
        # prompt_cache_key: keep the internal audit/cache-intent argument but
        # omit it from Responses API request kwargs.
        return build_responses_request(
            model=model,
            reasoning_effort=reasoning_effort,
            developer_prompt=developer_prompt,
            user_context=user_context,
            json_schema=json_schema,
            max_output_tokens=max_output_tokens,
            prompt_cache_key=prompt_cache_key,
            include_prompt_cache_key=False,
        )

    @staticmethod
    def _build_client(*, api_key: str, project: str | None, timeout_sec: float) -> Any:
        try:
            from openai import AsyncOpenAI  # type: ignore[import-not-found]
        except ModuleNotFoundError as exc:
            raise OpenAIClientConfigurationError(
                "openai package is required to run judge-openai"
            ) from exc
        return AsyncOpenAI(api_key=api_key, project=project, timeout=timeout_sec)

    @staticmethod
    def _raise_mapped_exception(exc: Exception) -> None:
        mapped = _classify_openai_exception(exc)
        if mapped[0] == "retryable":
            raise OpenAITransientError(mapped[1], safe_code=mapped[1]) from exc
        raise OpenAIPermanentError(mapped[1], safe_code=mapped[1]) from exc


def _classify_openai_exception(exc: Exception) -> tuple[str, str]:
        status_code = getattr(exc, "status_code", None)
        name = type(exc).__name__
        if name == "RateLimitError" or status_code == 429:
            return ("retryable", "openai_retryable_rate_limited")
        if name == "APITimeoutError":
            return ("retryable", "openai_retryable_timeout")
        if name == "APIConnectionError":
            return ("retryable", "openai_retryable_connection")
        if isinstance(status_code, int) and status_code >= 500:
            return ("retryable", "openai_retryable_server_error")
        if name == "AuthenticationError" or status_code == 401:
            return ("permanent", "openai_permanent_auth")
        if name == "PermissionDeniedError" or status_code == 403:
            return ("permanent", "openai_permanent_permission")
        if name == "BadRequestError" or status_code == 400:
            return ("permanent", "openai_permanent_bad_request")
        if name == "NotFoundError" or status_code == 404:
            return ("permanent", "openai_permanent_not_found")
        if isinstance(status_code, int) and status_code >= 400:
            return ("permanent", "openai_permanent_client_error")
        return ("retryable", "openai_retryable_unknown")
