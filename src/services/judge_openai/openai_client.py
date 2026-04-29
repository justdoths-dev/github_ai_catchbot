from __future__ import annotations

from typing import Any


class OpenAIClientConfigurationError(RuntimeError):
    pass


class OpenAITransientError(RuntimeError):
    pass


class OpenAIPermanentError(RuntimeError):
    pass


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
                    "name": "judge_output_v1",
                    "strict": True,
                    "schema": json_schema,
                }
            },
            "tools": [],
        }
        if max_output_tokens is not None:
            request["max_output_tokens"] = max_output_tokens
        # Older replayed judge.call.requested events may not have this optional cache hint.
        if prompt_cache_key:
            request["prompt_cache_key"] = prompt_cache_key
        return request

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
