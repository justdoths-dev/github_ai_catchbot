from __future__ import annotations

from typing import Any

from .request_shape import build_responses_request, validate_responses_request_shape


class OpenAIClientConfigurationError(RuntimeError):
    pass


class OpenAITransientError(RuntimeError):
    pass


class OpenAIPermanentError(RuntimeError):
    pass


class OpenAIRequestShapeError(OpenAIPermanentError):
    def __init__(self, issue_codes: tuple[str, ...]) -> None:
        super().__init__("invalid_request_shape")
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
        status_code = getattr(exc, "status_code", None)
        name = type(exc).__name__
        if name in {"RateLimitError", "APITimeoutError", "APIConnectionError"}:
            raise OpenAITransientError(name) from exc
        if isinstance(status_code, int) and status_code >= 500:
            raise OpenAITransientError(f"{name}:{status_code}") from exc
        if isinstance(status_code, int) and status_code >= 400:
            raise OpenAIPermanentError(f"{name}:{status_code}") from exc
        raise OpenAITransientError(name) from exc
