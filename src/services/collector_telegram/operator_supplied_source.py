from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol
from urllib.parse import urlparse

from .idempotency import IdempotencyPolicy
from .models import OutboxEventDraft, SourceMessageProjection
from .outbox import CollectorOutboxBuilder


PACKET_SCHEMA_VERSION = "operator_supplied_telegram_source_v1"
VERSION_REASON = "operator_supplied_canary"
MAX_PACKET_BYTES = 64 * 1024
MAX_MESSAGE_TEXT_CHARS = 12_000
_PUBLIC_USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,31}$")
_URL_RE = re.compile(r"https?://[^\s<>'\")\]]+", re.IGNORECASE)


class OperatorSuppliedSourceError(ValueError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(slots=True, frozen=True)
class ParsedTelegramSourceRef:
    normalized_source_ref: str
    public_username: str
    message_id: int


@dataclass(slots=True, frozen=True)
class OperatorSuppliedTelegramSourcePacket:
    schema_version: str
    source_ref: str
    posted_at: datetime
    message_text: str
    parsed_ref: ParsedTelegramSourceRef
    packet_fingerprint: str
    source_ref_fingerprint: str

    @property
    def normalized_public_username(self) -> str:
        return self.parsed_ref.public_username.lower()


@dataclass(slots=True, frozen=True)
class TelegramRegistryTarget:
    registry_id: str
    chat_id: int


@dataclass(slots=True, frozen=True)
class ExistingSourceState:
    source_message_id: str
    current_version_no: int | None
    latest_content_hash: str | None


@dataclass(slots=True, frozen=True)
class OperatorSourceIngestResult:
    source_message_id: str | None
    source_version_no: int | None
    source_event_id: str | None
    source_message_created: bool
    source_version_created: bool
    source_outbox_created: bool
    duplicate: bool = False
    reason_code: str | None = None


class OperatorSuppliedSourceRepository(Protocol):
    async def find_public_username_registry_targets(
        self,
        normalized_source_value: str,
    ) -> list[Mapping[str, Any]]: ...

    async def get_source_message(
        self,
        *,
        platform: str,
        chat_id: int,
        message_id: int,
    ) -> Mapping[str, Any] | None: ...

    async def get_latest_version(
        self,
        source_message_id: str,
    ) -> Mapping[str, Any] | None: ...

    async def upsert_source_message(
        self,
        projection: SourceMessageProjection,
        *,
        platform: str = "telegram",
    ) -> Mapping[str, Any]: ...

    async def append_source_message_version(
        self,
        *,
        source_message_id: str,
        projection: SourceMessageProjection,
        version_reason: str,
        observed_at: datetime | None = None,
        telegram_edit_date: datetime | None = None,
    ) -> Mapping[str, Any]: ...

    async def insert_outbox_event(self, event: OutboxEventDraft) -> bool: ...

    async def get_outbox_event_by_dedupe_key(
        self,
        dedupe_key: str,
    ) -> Mapping[str, Any] | None: ...


class OperatorSuppliedSourceAdapter:
    def __init__(
        self,
        *,
        outbox_builder: CollectorOutboxBuilder | None = None,
    ) -> None:
        self._outbox_builder = outbox_builder or CollectorOutboxBuilder(IdempotencyPolicy())

    async def resolve_registry_target(
        self,
        repository: OperatorSuppliedSourceRepository,
        packet: OperatorSuppliedTelegramSourcePacket,
    ) -> TelegramRegistryTarget:
        rows = await repository.find_public_username_registry_targets(
            packet.normalized_public_username
        )
        if not rows:
            raise OperatorSuppliedSourceError("telegram_channel_registry_target_missing")
        usable = [row for row in rows if row.get("chat_id") is not None]
        if len(rows) > 1 or len(usable) > 1:
            raise OperatorSuppliedSourceError("telegram_channel_registry_target_ambiguous")
        if not usable:
            raise OperatorSuppliedSourceError("telegram_channel_registry_target_missing")
        row = usable[0]
        return TelegramRegistryTarget(
            registry_id=str(row["registry_id"]),
            chat_id=int(row["chat_id"]),
        )

    async def inspect_existing_source(
        self,
        repository: OperatorSuppliedSourceRepository,
        *,
        packet: OperatorSuppliedTelegramSourcePacket,
        registry_target: TelegramRegistryTarget,
    ) -> ExistingSourceState | None:
        projection = build_source_projection(packet=packet, registry_target=registry_target)
        row = await repository.get_source_message(
            platform="telegram",
            chat_id=registry_target.chat_id,
            message_id=packet.parsed_ref.message_id,
        )
        if row is None:
            return None
        source_message_id = _require_nonempty_str(row.get("source_message_id"))
        latest = await repository.get_latest_version(source_message_id)
        latest_content_hash = None if latest is None else _optional_str(latest.get("content_hash"))
        current_version_no = _optional_int(row.get("current_version_no"))
        if latest_content_hash == projection.content_hash:
            return ExistingSourceState(
                source_message_id=source_message_id,
                current_version_no=current_version_no,
                latest_content_hash=latest_content_hash,
            )
        raise OperatorSuppliedSourceError("source_identity_content_conflict")

    async def ingest_source(
        self,
        repository: OperatorSuppliedSourceRepository,
        *,
        packet: OperatorSuppliedTelegramSourcePacket,
        registry_target: TelegramRegistryTarget,
    ) -> OperatorSourceIngestResult:
        existing = await self.inspect_existing_source(
            repository,
            packet=packet,
            registry_target=registry_target,
        )
        if existing is not None:
            return OperatorSourceIngestResult(
                source_message_id=existing.source_message_id,
                source_version_no=existing.current_version_no,
                source_event_id=None,
                source_message_created=False,
                source_version_created=False,
                source_outbox_created=False,
                duplicate=True,
                reason_code="source_packet_already_materialized",
            )

        projection = build_source_projection(packet=packet, registry_target=registry_target)
        current_row = await repository.upsert_source_message(projection, platform="telegram")
        source_message_id = _require_nonempty_str(current_row.get("source_message_id"))
        version_row = await repository.append_source_message_version(
            source_message_id=source_message_id,
            projection=projection,
            version_reason=VERSION_REASON,
            observed_at=datetime.now(timezone.utc),
            telegram_edit_date=None,
        )
        version_no = _require_positive_int(version_row.get("version_no"))
        event = self._outbox_builder.build_created(
            source_message_id=source_message_id,
            current_version_no=version_no,
            logical_post_key=projection.logical_post_key,
            occurred_at=projection.posted_at,
        )
        outbox_created = await repository.insert_outbox_event(event)
        outbox_row = await repository.get_outbox_event_by_dedupe_key(event.dedupe_key)
        if not outbox_created or outbox_row is None:
            raise OperatorSuppliedSourceError("source_outbox_event_not_created")
        return OperatorSourceIngestResult(
            source_message_id=source_message_id,
            source_version_no=version_no,
            source_event_id=_require_nonempty_str(outbox_row.get("event_id")),
            source_message_created=True,
            source_version_created=True,
            source_outbox_created=True,
        )


def load_operator_source_packet(
    packet_path: str | Path,
    *,
    repo_root: Path,
) -> OperatorSuppliedTelegramSourcePacket:
    path = _validate_packet_path(packet_path, repo_root=repo_root)
    try:
        size = path.stat().st_size
    except OSError:
        raise OperatorSuppliedSourceError("source_packet_unreadable") from None
    if size > MAX_PACKET_BYTES:
        raise OperatorSuppliedSourceError("source_packet_too_large")
    try:
        raw = path.read_bytes()
    except OSError:
        raise OperatorSuppliedSourceError("source_packet_unreadable") from None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise OperatorSuppliedSourceError("source_packet_not_utf8") from None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        raise OperatorSuppliedSourceError("source_packet_invalid_json") from None
    if not isinstance(payload, dict):
        raise OperatorSuppliedSourceError("source_packet_must_be_object")
    return parse_operator_source_packet(payload)


def parse_operator_source_packet(
    payload: Mapping[str, Any],
) -> OperatorSuppliedTelegramSourcePacket:
    schema_version = _string_field(payload, "schema_version")
    if schema_version != PACKET_SCHEMA_VERSION:
        raise OperatorSuppliedSourceError("source_packet_schema_version_invalid")
    source_ref = _string_field(payload, "source_ref")
    posted_at = _parse_posted_at(_string_field(payload, "posted_at"))
    message_text = _string_field(payload, "message_text")
    if not message_text.strip():
        raise OperatorSuppliedSourceError("source_packet_message_text_empty")
    if len(message_text) > MAX_MESSAGE_TEXT_CHARS:
        raise OperatorSuppliedSourceError("source_packet_message_text_too_large")
    parsed_ref = parse_telegram_source_ref(source_ref)
    packet_fingerprint = fingerprint_value(
        {
            "schema_version": schema_version,
            "source_ref": parsed_ref.normalized_source_ref,
            "posted_at": posted_at.isoformat(),
            "message_text": message_text,
        }
    )
    return OperatorSuppliedTelegramSourcePacket(
        schema_version=schema_version,
        source_ref=source_ref,
        posted_at=posted_at,
        message_text=message_text,
        parsed_ref=parsed_ref,
        packet_fingerprint=packet_fingerprint,
        source_ref_fingerprint=fingerprint_value(parsed_ref.normalized_source_ref),
    )


def parse_telegram_source_ref(source_ref: str) -> ParsedTelegramSourceRef:
    parsed = urlparse(source_ref)
    if parsed.scheme not in {"https", "http"} or parsed.netloc.lower() != "t.me":
        raise OperatorSuppliedSourceError("source_ref_invalid")
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) != 2:
        raise OperatorSuppliedSourceError("source_ref_invalid")
    username, raw_message_id = parts
    if not _PUBLIC_USERNAME_RE.match(username):
        raise OperatorSuppliedSourceError("source_ref_invalid")
    try:
        message_id = int(raw_message_id)
    except ValueError:
        raise OperatorSuppliedSourceError("source_ref_invalid") from None
    if message_id <= 0:
        raise OperatorSuppliedSourceError("source_ref_invalid")
    normalized = f"https://t.me/{username}/{message_id}"
    return ParsedTelegramSourceRef(
        normalized_source_ref=normalized,
        public_username=username,
        message_id=message_id,
    )


def build_source_projection(
    *,
    packet: OperatorSuppliedTelegramSourcePacket,
    registry_target: TelegramRegistryTarget,
) -> SourceMessageProjection:
    text_surface = _normalize_text_surface(packet.message_text)
    url_surface_json = _url_surface_from_text(text_surface)
    logical_post_key = f"tg:{registry_target.chat_id}:{packet.parsed_ref.message_id}"
    raw_message_json = {
        "schema_version": PACKET_SCHEMA_VERSION,
        "operator_supplied": True,
        "live_telegram_read": False,
        "source_ref_fingerprint": packet.source_ref_fingerprint,
        "posted_at": packet.posted_at.isoformat(),
        "content": {"text": packet.message_text},
    }
    projection = SourceMessageProjection(
        chat_id=registry_target.chat_id,
        message_id=packet.parsed_ref.message_id,
        logical_post_key=logical_post_key,
        is_channel_post=True,
        posted_at=packet.posted_at,
        edited_at=None,
        message_link=None,
        author_signature=None,
        forward_info_json={
            "operator_supplied": True,
            "live_telegram_read": False,
            "source_ref_fingerprint": packet.source_ref_fingerprint,
        },
        content_type="text",
        text_body=packet.message_text,
        caption_text=None,
        text_surface=text_surface,
        entities_json=None,
        url_surface_json=url_surface_json or None,
        raw_message_json=raw_message_json,
        content_hash="",
    )
    return SourceMessageProjection(
        chat_id=projection.chat_id,
        message_id=projection.message_id,
        logical_post_key=projection.logical_post_key,
        is_channel_post=projection.is_channel_post,
        posted_at=projection.posted_at,
        edited_at=projection.edited_at,
        message_link=projection.message_link,
        author_signature=projection.author_signature,
        forward_info_json=projection.forward_info_json,
        content_type=projection.content_type,
        text_body=projection.text_body,
        caption_text=projection.caption_text,
        text_surface=projection.text_surface,
        entities_json=projection.entities_json,
        url_surface_json=projection.url_surface_json,
        raw_message_json=projection.raw_message_json,
        content_hash=_content_hash(projection),
    )


def fingerprint_value(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def _validate_packet_path(packet_path: str | Path, *, repo_root: Path) -> Path:
    raw_path = Path(packet_path)
    if not raw_path.is_absolute():
        raise OperatorSuppliedSourceError("source_packet_path_not_absolute")
    if raw_path.is_symlink():
        raise OperatorSuppliedSourceError("source_packet_path_symlink")
    resolved = raw_path.resolve(strict=False)
    resolved_repo = repo_root.resolve(strict=False)
    try:
        common = os.path.commonpath([str(resolved), str(resolved_repo)])
    except ValueError:
        common = ""
    if common == str(resolved_repo):
        raise OperatorSuppliedSourceError("source_packet_path_inside_repo")
    if not raw_path.exists():
        raise OperatorSuppliedSourceError("source_packet_missing")
    if not raw_path.is_file():
        raise OperatorSuppliedSourceError("source_packet_not_regular_file")
    return raw_path


def _parse_posted_at(raw: str) -> datetime:
    value = raw.strip()
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise OperatorSuppliedSourceError("source_packet_posted_at_invalid") from None
    if parsed.tzinfo is None:
        raise OperatorSuppliedSourceError("source_packet_posted_at_invalid")
    return parsed.astimezone(timezone.utc)


def _string_field(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise OperatorSuppliedSourceError(f"source_packet_{key}_invalid")
    return value


def _normalize_text_surface(value: str) -> str:
    return unicodedata.normalize("NFKC", value).replace("\r\n", "\n").replace("\r", "\n").strip()


def _url_surface_from_text(text_surface: str) -> list[dict[str, str]]:
    seen: set[str] = set()
    rows: list[dict[str, str]] = []
    for match in _URL_RE.findall(text_surface):
        observed_url = match.rstrip(".,;:!?")
        if observed_url in seen:
            continue
        seen.add(observed_url)
        rows.append({"observed_url": observed_url, "source_kind": "regex", "context": "message_text"})
    return rows


def _content_hash(projection: SourceMessageProjection) -> str:
    payload = {
        "content_type": projection.content_type,
        "text_body": _normalized_hash_text(projection.text_body),
        "caption_text": None,
        "text_surface": _normalized_hash_text(projection.text_surface),
        "entities_json": projection.entities_json or [],
        "url_surface_json": projection.url_surface_json or [],
        "author_signature": projection.author_signature,
        "forward_info_json": projection.forward_info_json,
        "logical_post_key": projection.logical_post_key,
        "is_channel_post": projection.is_channel_post,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _normalized_hash_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = unicodedata.normalize("NFKC", value)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


def _require_nonempty_str(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if value is not None:
        text = str(value)
        if text:
            return text
    raise OperatorSuppliedSourceError("repository_row_invalid")


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _require_positive_int(value: Any) -> int:
    parsed = _optional_int(value)
    if parsed is None or parsed <= 0:
        raise OperatorSuppliedSourceError("repository_row_invalid")
    return parsed
