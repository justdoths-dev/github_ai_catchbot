from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import stat
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol

try:
    import sqlalchemy as sa
except ModuleNotFoundError:  # pragma: no cover - compile-only fallback
    sa = None


SCHEMA_VERSION = "channel_candidate_inventory_v1"
PASS_REASON_CODE = "channel_candidate_inventory_ready"
DEFAULT_LIMIT = 20
MAX_LIMIT = 50
LOOKBACK_DAYS = 7
RECOMMENDED_COUNT = 3
MAX_MESSAGES_NEXT_STEP = 1

_PUBLIC_USERNAME_RE = re.compile(r"^@?[A-Za-z0-9_]{5,32}$")
_BAD_ACCESS_STATES = frozenset({"access_lost", "forbidden", "not_found", "left"})


@dataclass(frozen=True, slots=True)
class ChannelCandidateInventoryConfig:
    operator_approved: bool = False
    allow_runtime_env_read: bool = False
    allow_database_read: bool = False
    runtime_env_file: str | None = None
    limit: int = DEFAULT_LIMIT


@dataclass(frozen=True, slots=True)
class ChannelCandidateInventoryRuntimeConfig:
    database_url: str


@dataclass(slots=True)
class ChannelCandidateInventoryState:
    runtime_env_read_attempted: bool = False
    runtime_config_loaded: bool = False
    database_session_opened: bool = False
    database_read_attempted: bool = False


@dataclass(frozen=True, slots=True)
class SignalBuckets:
    github_link_seen: bool = False
    x_link_seen: bool = False
    vibe_coding_seen: bool = False
    ai_dev_context_seen: bool = False
    generic_ai_noise_only: bool = False

    def to_sanitized_dict(self) -> dict[str, bool]:
        return {
            "github_link_seen": self.github_link_seen,
            "x_link_seen": self.x_link_seen,
            "vibe_coding_seen": self.vibe_coding_seen,
            "ai_dev_context_seen": self.ai_dev_context_seen,
            "generic_ai_noise_only": self.generic_ai_noise_only,
        }


@dataclass(frozen=True, slots=True)
class ChannelCandidate:
    public_username: str
    title_snapshot: str | None
    desired_state: str
    access_state: str
    last_seen_message_date: datetime | str | None
    last_history_sync_at: datetime | str | None
    recent_messages_7d: int
    recent_signal_messages_7d: int
    signal_buckets_7d: SignalBuckets
    recommended_bucket: str
    sort_score: float = field(repr=False)

    @property
    def selectable(self) -> bool:
        return (
            self.desired_state == "active"
            and self.access_state == "joined_active"
            and self.recent_messages_7d > 0
            and not self.recommended_bucket.startswith("avoid_")
        )

    def to_sanitized_dict(self, *, rank: int) -> dict[str, Any]:
        density = 0.0
        if self.recent_messages_7d > 0:
            density = round(self.recent_signal_messages_7d / self.recent_messages_7d, 3)
        return {
            "rank": rank,
            "public_username": self.public_username,
            "title_snapshot": self.title_snapshot,
            "desired_state": self.desired_state,
            "access_state": self.access_state,
            "last_seen_message_date": _iso_or_none(self.last_seen_message_date),
            "last_history_sync_at": _iso_or_none(self.last_history_sync_at),
            "recent_messages_7d": self.recent_messages_7d,
            "recent_signal_messages_7d": self.recent_signal_messages_7d,
            "signal_density_7d": density,
            "signal_buckets_7d": self.signal_buckets_7d.to_sanitized_dict(),
            "recommended_bucket": self.recommended_bucket,
        }


@dataclass(frozen=True, slots=True)
class ChannelCandidateInventoryResult:
    status: str
    reason_code: str
    config: ChannelCandidateInventoryConfig
    state: ChannelCandidateInventoryState = field(default_factory=ChannelCandidateInventoryState)
    candidates: tuple[ChannelCandidate, ...] = ()
    error_class: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == "pass"

    def to_sanitized_dict(self) -> dict[str, Any]:
        selectable_count = sum(1 for candidate in self.candidates if candidate.selectable)
        return {
            "schema_version": SCHEMA_VERSION,
            "status": self.status,
            "reason_code": self.reason_code,
            "authority": {
                "database_read_allowed": self.config.allow_database_read,
                "database_write_allowed": False,
                "redis_allowed": False,
                "telegram_live_read_allowed": False,
                "telegram_send_allowed": False,
                "provider_calls_allowed": False,
                "openai_allowed": False,
                "docker_systemd_allowed": False,
                "alembic_allowed": False,
            },
            "redactions_applied": {
                "database_url_omitted": True,
                "runtime_env_values_omitted": True,
                "raw_chat_ids_omitted": True,
                "raw_message_ids_omitted": True,
                "raw_message_text_omitted": True,
                "raw_urls_omitted": True,
                "exception_bodies_omitted": True,
                "stderr_omitted": True,
            },
            "selection_guidance": {
                "recommended_count": RECOMMENDED_COUNT,
                "max_messages_next_step": MAX_MESSAGES_NEXT_STEP,
                "avoid": ["removed", "access_lost", "no_recent_activity"],
            },
            "candidate_count": len(self.candidates),
            "selectable_candidate_count": selectable_count,
            "candidates": [
                candidate.to_sanitized_dict(rank=index)
                for index, candidate in enumerate(self.candidates, start=1)
            ],
            "raw_values_printed": False,
        }


class ChannelCandidateInventoryError(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class ChannelCandidateInventoryRepository(Protocol):
    async def load_channel_candidate_rows(
        self,
        *,
        limit: int,
        lookback_days: int,
    ) -> Sequence[Mapping[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class ChannelCandidateInventoryRepositoryHandle:
    repository: ChannelCandidateInventoryRepository
    close: Callable[[], Any]


class ChannelCandidateInventoryRepositoryBuilder(Protocol):
    async def __call__(
        self,
        runtime_config: ChannelCandidateInventoryRuntimeConfig,
        state: ChannelCandidateInventoryState,
        logger: logging.Logger,
    ) -> ChannelCandidateInventoryRepositoryHandle: ...


class SqlAlchemyChannelCandidateInventoryRepository:
    def __init__(self, session: Any) -> None:
        self._session = session

    async def load_channel_candidate_rows(
        self,
        *,
        limit: int,
        lookback_days: int,
    ) -> Sequence[Mapping[str, Any]]:
        if sa is None:  # pragma: no cover - import guard
            raise ChannelCandidateInventoryError("sqlalchemy_missing")
        scan_limit = max(limit * 4, RECOMMENDED_COUNT * 4)
        result = await self._session.execute(
            sa.text(
                """
                WITH registry AS (
                    SELECT
                        registry_id,
                        source_value,
                        username_snapshot,
                        title_snapshot,
                        desired_state,
                        access_state,
                        priority_weight,
                        last_seen_message_date,
                        last_history_sync_at,
                        chat_id
                    FROM telegram_channel_registry
                    WHERE source_kind = 'public_username'
                    ORDER BY
                        CASE
                            WHEN desired_state = 'active' AND access_state = 'joined' THEN 0
                            WHEN desired_state = 'active' THEN 1
                            ELSE 2
                        END,
                        priority_weight DESC,
                        registry_id ASC
                    LIMIT :scan_limit
                ),
                message_window AS (
                    SELECT
                        r.registry_id,
                        lower(coalesce(sm.text_surface, '')) AS text_lc,
                        lower(coalesce(CAST(sm.url_surface_json AS text), '')) AS urls_lc
                    FROM registry r
                    JOIN source_messages sm
                      ON sm.platform = 'telegram'
                     AND sm.chat_id = r.chat_id
                    WHERE r.chat_id IS NOT NULL
                      AND sm.deleted_at IS NULL
                      AND (
                        sm.posted_at >= now() - make_interval(days => :lookback_days)
                        OR sm.first_seen_at >= now() - make_interval(days => :lookback_days)
                      )
                ),
                signals AS (
                    SELECT
                        registry_id,
                        count(*) AS recent_messages_7d,
                        count(*) FILTER (
                            WHERE
                                text_lc LIKE '%github.com/%'
                                OR urls_lc LIKE '%github.com/%'
                                OR text_lc LIKE '%x.com/%'
                                OR text_lc LIKE '%twitter.com/%'
                                OR urls_lc LIKE '%x.com/%'
                                OR urls_lc LIKE '%twitter.com/%'
                                OR text_lc LIKE '%vibe coding%'
                                OR text_lc LIKE '%vibecoding%'
                                OR text_lc LIKE '%cursor%'
                                OR text_lc LIKE '%windsurf%'
                                OR text_lc LIKE '%claude code%'
                                OR text_lc LIKE '%codex%'
                                OR text_lc LIKE '%coding agent%'
                                OR text_lc LIKE '%developer tool%'
                                OR text_lc LIKE '%dev tool%'
                                OR text_lc LIKE '%open source%'
                                OR text_lc LIKE '%repository%'
                        ) AS recent_signal_messages_7d,
                        bool_or(text_lc LIKE '%github.com/%' OR urls_lc LIKE '%github.com/%')
                            AS github_link_seen,
                        bool_or(
                            text_lc LIKE '%x.com/%'
                            OR text_lc LIKE '%twitter.com/%'
                            OR urls_lc LIKE '%x.com/%'
                            OR urls_lc LIKE '%twitter.com/%'
                        ) AS x_link_seen,
                        bool_or(
                            text_lc LIKE '%vibe coding%'
                            OR text_lc LIKE '%vibecoding%'
                            OR text_lc LIKE '%cursor%'
                            OR text_lc LIKE '%windsurf%'
                            OR text_lc LIKE '%claude code%'
                            OR text_lc LIKE '%codex%'
                        ) AS vibe_coding_seen,
                        bool_or(
                            text_lc LIKE '%coding agent%'
                            OR text_lc LIKE '%developer tool%'
                            OR text_lc LIKE '%dev tool%'
                            OR text_lc LIKE '%open source%'
                            OR text_lc LIKE '%repository%'
                            OR text_lc LIKE '%github.com/%'
                            OR urls_lc LIKE '%github.com/%'
                        ) AS ai_dev_context_seen,
                        bool_or(text_lc ~ '(^|[^a-z])ai([^a-z]|$)' OR text_lc LIKE '%llm%' OR text_lc LIKE '%gpt%')
                            AS generic_ai_seen
                    FROM message_window
                    GROUP BY registry_id
                )
                SELECT
                    r.source_value,
                    r.username_snapshot,
                    r.title_snapshot,
                    r.desired_state,
                    r.access_state,
                    r.priority_weight,
                    r.last_seen_message_date,
                    r.last_history_sync_at,
                    coalesce(s.recent_messages_7d, 0) AS recent_messages_7d,
                    coalesce(s.recent_signal_messages_7d, 0) AS recent_signal_messages_7d,
                    coalesce(s.github_link_seen, false) AS github_link_seen,
                    coalesce(s.x_link_seen, false) AS x_link_seen,
                    coalesce(s.vibe_coding_seen, false) AS vibe_coding_seen,
                    coalesce(s.ai_dev_context_seen, false) AS ai_dev_context_seen,
                    coalesce(s.generic_ai_seen, false)
                      AND NOT (
                        coalesce(s.github_link_seen, false)
                        OR coalesce(s.x_link_seen, false)
                        OR coalesce(s.vibe_coding_seen, false)
                        OR coalesce(s.ai_dev_context_seen, false)
                      ) AS generic_ai_noise_only
                FROM registry r
                LEFT JOIN signals s ON s.registry_id = r.registry_id
                """
            ),
            {"scan_limit": scan_limit, "lookback_days": lookback_days},
        )
        return [dict(row) for row in result.mappings().all()]


def load_channel_candidate_inventory_runtime_config(
    runtime_env_file: str | None,
    state: ChannelCandidateInventoryState,
) -> ChannelCandidateInventoryRuntimeConfig:
    state.runtime_env_read_attempted = True
    database_url = _read_database_url_from_runtime_env_file(runtime_env_file)
    return ChannelCandidateInventoryRuntimeConfig(database_url=database_url)


async def build_default_channel_candidate_inventory_repository(
    runtime_config: ChannelCandidateInventoryRuntimeConfig,
    state: ChannelCandidateInventoryState,
    logger: logging.Logger,
) -> ChannelCandidateInventoryRepositoryHandle:
    del logger
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    engine = create_async_engine(runtime_config.database_url, future=True)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    session = session_factory()
    state.database_session_opened = True
    repository = SqlAlchemyChannelCandidateInventoryRepository(session)

    async def close() -> None:
        await session.close()
        await engine.dispose()

    return ChannelCandidateInventoryRepositoryHandle(repository=repository, close=close)


async def run_channel_candidate_inventory(
    config: ChannelCandidateInventoryConfig,
    *,
    runtime_config_loader: Callable[
        [str | None, ChannelCandidateInventoryState],
        ChannelCandidateInventoryRuntimeConfig,
    ] = load_channel_candidate_inventory_runtime_config,
    repository_builder: ChannelCandidateInventoryRepositoryBuilder | None = None,
    logger: logging.Logger | None = None,
) -> ChannelCandidateInventoryResult:
    state = ChannelCandidateInventoryState()
    config_error = _config_error(config)
    if config_error is not None:
        return _result("blocked", config_error, config=config, state=state)

    effective_logger = logger or logging.getLogger(__name__)
    repository_handle: ChannelCandidateInventoryRepositoryHandle | None = None
    try:
        runtime_config = runtime_config_loader(config.runtime_env_file, state)
        state.runtime_config_loaded = True
        repository_handle = await (repository_builder or build_default_channel_candidate_inventory_repository)(
            runtime_config,
            state,
            effective_logger,
        )
        state.database_read_attempted = True
        rows = await repository_handle.repository.load_channel_candidate_rows(
            limit=config.limit,
            lookback_days=LOOKBACK_DAYS,
        )
        candidates = _rank_candidates(rows, limit=config.limit)
        selectable_count = sum(1 for candidate in candidates if candidate.selectable)
        if selectable_count < RECOMMENDED_COUNT:
            return _result(
                "blocked",
                "insufficient_selectable_channel_candidates",
                config=config,
                state=state,
                candidates=candidates,
            )
        return _result("pass", PASS_REASON_CODE, config=config, state=state, candidates=candidates)
    except ChannelCandidateInventoryError as exc:
        return _result("blocked", exc.reason_code, config=config, state=state)
    except Exception as exc:
        return _result(
            "blocked",
            "channel_candidate_inventory_failed",
            config=config,
            state=state,
            error_class=_safe_exception_class(exc),
        )
    finally:
        if repository_handle is not None:
            try:
                result = repository_handle.close()
                if hasattr(result, "__await__"):
                    await result
            except Exception:
                pass


def run_channel_candidate_inventory_sync(
    config: ChannelCandidateInventoryConfig,
    *,
    runtime_config_loader: Callable[
        [str | None, ChannelCandidateInventoryState],
        ChannelCandidateInventoryRuntimeConfig,
    ] = load_channel_candidate_inventory_runtime_config,
    repository_builder: ChannelCandidateInventoryRepositoryBuilder | None = None,
    logger: logging.Logger | None = None,
) -> ChannelCandidateInventoryResult:
    return asyncio.run(
        run_channel_candidate_inventory(
            config,
            runtime_config_loader=runtime_config_loader,
            repository_builder=repository_builder,
            logger=logger,
        )
    )


def argument_error_report(reason_code: str) -> dict[str, Any]:
    return _result(
        "blocked",
        reason_code,
        config=ChannelCandidateInventoryConfig(),
        state=ChannelCandidateInventoryState(),
    ).to_sanitized_dict()


def render_sanitized_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n"


def _config_error(config: ChannelCandidateInventoryConfig) -> str | None:
    if not 1 <= config.limit <= MAX_LIMIT:
        return "invalid_limit"
    if not config.operator_approved:
        return "operator_approval_missing"
    if not config.allow_runtime_env_read:
        return "runtime_env_read_not_allowed"
    if not config.runtime_env_file:
        return "runtime_env_file_required"
    if not config.allow_database_read:
        return "database_read_not_allowed"
    return None


def _result(
    status: str,
    reason_code: str,
    *,
    config: ChannelCandidateInventoryConfig,
    state: ChannelCandidateInventoryState,
    candidates: Sequence[ChannelCandidate] = (),
    error_class: str | None = None,
) -> ChannelCandidateInventoryResult:
    return ChannelCandidateInventoryResult(
        status=status,
        reason_code=reason_code,
        config=config,
        state=state,
        candidates=tuple(candidates),
        error_class=error_class,
    )


def _rank_candidates(rows: Sequence[Mapping[str, Any]], *, limit: int) -> tuple[ChannelCandidate, ...]:
    candidates = []
    for row in rows:
        public_username = _public_username(row)
        if public_username is None:
            continue
        buckets = SignalBuckets(
            github_link_seen=_safe_bool(row.get("github_link_seen")),
            x_link_seen=_safe_bool(row.get("x_link_seen")),
            vibe_coding_seen=_safe_bool(row.get("vibe_coding_seen")),
            ai_dev_context_seen=_safe_bool(row.get("ai_dev_context_seen")),
            generic_ai_noise_only=_safe_bool(row.get("generic_ai_noise_only")),
        )
        desired_state = _safe_str(row.get("desired_state")) or "unknown"
        source_access_state = _safe_str(row.get("access_state")) or "unknown"
        recent_messages = _safe_int(row.get("recent_messages_7d"))
        recent_signals = _safe_int(row.get("recent_signal_messages_7d"))
        access_state = _access_state_summary(desired_state, source_access_state)
        bucket = _recommended_bucket(
            desired_state=desired_state,
            access_state=source_access_state,
            recent_messages=recent_messages,
            recent_signals=recent_signals,
            signal_buckets=buckets,
            last_history_sync_at=row.get("last_history_sync_at"),
        )
        candidates.append(
            ChannelCandidate(
                public_username=public_username,
                title_snapshot=_safe_title(row.get("title_snapshot")),
                desired_state=desired_state,
                access_state=access_state,
                last_seen_message_date=row.get("last_seen_message_date"),
                last_history_sync_at=row.get("last_history_sync_at"),
                recent_messages_7d=recent_messages,
                recent_signal_messages_7d=recent_signals,
                signal_buckets_7d=buckets,
                recommended_bucket=bucket,
                sort_score=_score_candidate(
                    desired_state=desired_state,
                    access_state=source_access_state,
                    recent_messages=recent_messages,
                    recent_signals=recent_signals,
                    signal_buckets=buckets,
                    last_history_sync_at=row.get("last_history_sync_at"),
                    priority_weight=_safe_int(row.get("priority_weight")),
                ),
            )
        )
    return tuple(sorted(candidates, key=lambda candidate: (-candidate.sort_score, candidate.public_username))[:limit])


def _public_username(row: Mapping[str, Any]) -> str | None:
    for key in ("username_snapshot", "source_value"):
        value = _safe_str(row.get(key))
        if value is None:
            continue
        value = value.strip()
        if not value:
            continue
        normalized = value if value.startswith("@") else f"@{value}"
        if _PUBLIC_USERNAME_RE.fullmatch(normalized):
            return normalized.lower()
    return None


def _access_state_summary(desired_state: str, access_state: str) -> str:
    if desired_state == "removed":
        return "removed"
    if desired_state == "active" and access_state == "joined":
        return "joined_active"
    return access_state


def _recommended_bucket(
    *,
    desired_state: str,
    access_state: str,
    recent_messages: int,
    recent_signals: int,
    signal_buckets: SignalBuckets,
    last_history_sync_at: Any,
) -> str:
    if desired_state == "removed" or access_state in _BAD_ACCESS_STATES:
        return "avoid_inaccessible"
    if recent_messages <= 0:
        return "avoid_no_recent_activity"
    if recent_signals >= 3 and (
        signal_buckets.github_link_seen
        or signal_buckets.x_link_seen
        or signal_buckets.vibe_coding_seen
        or signal_buckets.ai_dev_context_seen
    ):
        return "good_f2_candidate"
    if signal_buckets.github_link_seen or signal_buckets.x_link_seen:
        return "likely_github_ai_signal_channel"
    if last_history_sync_at is not None and recent_messages >= 5:
        return "stable_proven_channel"
    if recent_signals > 0:
        return "accessible_noisy_channel"
    return "needs_operator_review"


def _score_candidate(
    *,
    desired_state: str,
    access_state: str,
    recent_messages: int,
    recent_signals: int,
    signal_buckets: SignalBuckets,
    last_history_sync_at: Any,
    priority_weight: int,
) -> float:
    score = float(priority_weight)
    if desired_state == "active":
        score += 1000
    if access_state == "joined":
        score += 1000
    elif access_state in _BAD_ACCESS_STATES:
        score -= 1000
    score += min(recent_messages, 100) * 4
    score += min(recent_signals, 50) * 20
    if signal_buckets.github_link_seen:
        score += 90
    if signal_buckets.x_link_seen:
        score += 40
    if signal_buckets.vibe_coding_seen:
        score += 70
    if signal_buckets.ai_dev_context_seen:
        score += 50
    if signal_buckets.generic_ai_noise_only:
        score -= 40
    if last_history_sync_at is not None:
        score += 25
    if recent_messages == 0:
        score -= 200
    return score


def _read_database_url_from_runtime_env_file(runtime_env_file: str | None) -> str:
    if not runtime_env_file:
        raise ChannelCandidateInventoryError("runtime_env_file_required")
    path = Path(runtime_env_file)
    try:
        mode_error = _runtime_env_file_error(path)
    except OSError as exc:
        raise ChannelCandidateInventoryError("runtime_env_file_unreadable") from exc
    if mode_error is not None:
        raise ChannelCandidateInventoryError(mode_error)
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ChannelCandidateInventoryError("runtime_env_file_unreadable") from exc
    values: dict[str, str] = {}
    duplicate_database_url = False
    for raw_line in lines:
        parsed = _parse_env_line(raw_line)
        if parsed is None:
            continue
        key, value = parsed
        if key == "DATABASE_URL_FILE":
            raise ChannelCandidateInventoryError("database_url_file_indirection_not_allowed")
        if key != "DATABASE_URL":
            continue
        if key in values:
            duplicate_database_url = True
            continue
        values[key] = value
    if duplicate_database_url:
        raise ChannelCandidateInventoryError("duplicate_database_url")
    database_url = values.get("DATABASE_URL", "").strip()
    if not database_url:
        raise ChannelCandidateInventoryError("database_url_missing")
    return database_url


def _runtime_env_file_error(path: Path) -> str | None:
    if not path.is_file():
        return "runtime_env_file_missing"
    mode = stat.S_IMODE(path.stat().st_mode)
    if os.name != "nt" and mode & (stat.S_IRWXG | stat.S_IRWXO):
        return "runtime_env_file_permissions_too_open"
    return None


def _parse_env_line(raw_line: str) -> tuple[str, str] | None:
    line = raw_line.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith("export "):
        line = line[len("export ") :].lstrip()
    if "=" not in line:
        return None
    key, value = line.split("=", 1)
    key = key.strip()
    if not key:
        return None
    value = value.strip()
    if value.startswith(("'", '"')) and len(value) >= 2 and value[-1] == value[0]:
        value = value[1:-1]
    return key, value


def _safe_bool(value: Any) -> bool:
    return bool(value)


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_title(value: Any) -> str | None:
    text = _safe_str(value)
    if text is None:
        return None
    compact = " ".join(text.split())
    return compact[:120]


def _iso_or_none(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        effective = value
        if effective.tzinfo is None:
            effective = effective.replace(tzinfo=timezone.utc)
        return effective.astimezone(timezone.utc).isoformat()
    text = str(value).strip()
    return text or None


def _safe_exception_class(exc: BaseException) -> str:
    return exc.__class__.__name__


__all__ = [
    "ChannelCandidate",
    "ChannelCandidateInventoryConfig",
    "ChannelCandidateInventoryRepository",
    "ChannelCandidateInventoryRepositoryBuilder",
    "ChannelCandidateInventoryRepositoryHandle",
    "ChannelCandidateInventoryResult",
    "ChannelCandidateInventoryRuntimeConfig",
    "ChannelCandidateInventoryState",
    "SqlAlchemyChannelCandidateInventoryRepository",
    "argument_error_report",
    "load_channel_candidate_inventory_runtime_config",
    "render_sanitized_json",
    "run_channel_candidate_inventory",
    "run_channel_candidate_inventory_sync",
]
