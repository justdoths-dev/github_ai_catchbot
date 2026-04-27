from __future__ import annotations

from typing import Any

from .models import XPostSnapshotDraft


def compute_content_anchor(post_id: str, edit_history_tweet_ids: list[Any] | None) -> str:
    latest_edit_id = post_id
    if edit_history_tweet_ids:
        latest_edit_id = str(edit_history_tweet_ids[-1])
    return f"xpost:{post_id}:{latest_edit_id}"


class XResponseMapper:
    def map_post_lookup_response(
        self,
        *,
        requested_post_id: str,
        payload: dict[str, Any],
    ) -> XPostSnapshotDraft:
        data = payload.get("data") or []
        if isinstance(data, dict):
            data = [data]
        includes = payload.get("includes") or {}
        errors = payload.get("errors") or []

        root_post = next(
            (item for item in data if isinstance(item, dict) and str(item.get("id")) == requested_post_id),
            None,
        )
        if root_post is None:
            return _empty_root_draft(
                post_id=requested_post_id,
                status="failed_permanent",
                fetch_anomalies=["root_post_missing"],
                errors=errors,
            )

        includes_users = includes.get("users") or []
        includes_media = includes.get("media") or []
        users_by_id = {
            str(user.get("id")): user
            for user in includes_users
            if isinstance(user, dict) and user.get("id") is not None
        }
        media_by_key = {
            str(media.get("media_key")): media
            for media in includes_media
            if isinstance(media, dict) and media.get("media_key") is not None
        }
        posts_by_id = {
            str(post.get("id")): post
            for post in data
            if isinstance(post, dict) and post.get("id") is not None
        }

        author_id = _as_str(root_post.get("author_id"))
        author = users_by_id.get(author_id) if author_id else None
        author_summary = _author_summary(author)

        edit_history = root_post.get("edit_history_tweet_ids")
        edit_history_ids = edit_history if isinstance(edit_history, list) else None
        content_anchor = compute_content_anchor(requested_post_id, edit_history_ids)
        text_full = _post_text(root_post)
        referenced_items = root_post.get("referenced_tweets") or []
        referenced_post_ids: list[str] = []
        referenced_posts: list[dict[str, Any]] = []
        missing_references = False
        for item in referenced_items:
            if not isinstance(item, dict):
                continue
            ref_id = _as_str(item.get("id"))
            if not ref_id:
                continue
            referenced_post_ids.append(ref_id)
            ref_post = posts_by_id.get(ref_id)
            if ref_post is None:
                missing_references = True
            referenced_posts.append(
                {
                    "post_id": ref_id,
                    "relation_type": _as_str(item.get("type")),
                    "author_id": _as_str(ref_post.get("author_id")) if ref_post else None,
                    "text_excerpt": _excerpt(_post_text(ref_post), 280) if ref_post else None,
                    "raw_post": ref_post,
                }
            )

        media_keys = _root_media_keys(root_post)
        media_summary = [_media_summary(media_by_key[key]) for key in media_keys if key in media_by_key]
        missing_media = bool(media_keys) and len(media_summary) < len(media_keys)

        fetch_anomalies: list[str] = []
        evidence_limitations: list[str] = []
        status = "ready"
        if errors:
            fetch_anomalies.append("partial_errors_present")
            status = "partial_ready"
        if author_id and author is None:
            evidence_limitations.append("x_author_summary_missing")
            status = "partial_ready" if status == "ready" else status
        if missing_references:
            evidence_limitations.append("x_referenced_posts_missing")
            status = "partial_ready" if status == "ready" else status
        if missing_media:
            evidence_limitations.append("x_media_summary_missing")
            status = "partial_ready" if status == "ready" else status
        if not text_full:
            evidence_limitations.append("x_text_missing")
            status = "low_evidence"

        normalized_projection = {
            "root_post": root_post,
            "referenced_posts": referenced_posts,
            "includes": {
                "users": includes_users,
                "media": includes_media,
            },
            "errors": errors,
            "depth_budget_applied": 1,
        }
        return XPostSnapshotDraft(
            snapshot_type="x_post",
            status=status,  # type: ignore[arg-type]
            content_anchor=content_anchor,
            auth_mode="bearer_app_only",
            normalized_projection=normalized_projection,
            raw_payload_ref=None,
            evidence_limitations=evidence_limitations,
            fetch_anomalies=fetch_anomalies,
            post_id=requested_post_id,
            content_anchor_post_version=content_anchor,
            author_summary_json=author_summary,
            text_full=text_full,
            text_excerpt=_excerpt(text_full, 500),
            conversation_id=_as_str(root_post.get("conversation_id")),
            referenced_post_ids_json=referenced_post_ids,
            discovered_links_json=[],
            media_summary_json=media_summary,
            metrics_summary_json=root_post.get("public_metrics") if isinstance(root_post.get("public_metrics"), dict) else None,
        )


def _empty_root_draft(
    *,
    post_id: str,
    status: str,
    fetch_anomalies: list[str],
    errors: Any,
) -> XPostSnapshotDraft:
    return XPostSnapshotDraft(
        snapshot_type="x_post",
        status=status,  # type: ignore[arg-type]
        content_anchor=compute_content_anchor(post_id, None),
        auth_mode="bearer_app_only",
        normalized_projection={"errors": errors or []},
        raw_payload_ref=None,
        evidence_limitations=["x_root_post_unavailable"],
        fetch_anomalies=fetch_anomalies,
        post_id=post_id,
        content_anchor_post_version=compute_content_anchor(post_id, None),
        author_summary_json=None,
        text_full=None,
        text_excerpt=None,
        conversation_id=None,
        referenced_post_ids_json=[],
        discovered_links_json=[],
        media_summary_json=[],
        metrics_summary_json=None,
    )


def _post_text(post: dict[str, Any] | None) -> str | None:
    if not isinstance(post, dict):
        return None
    note_tweet = post.get("note_tweet")
    if isinstance(note_tweet, dict):
        note_text = note_tweet.get("text")
        if isinstance(note_text, str) and note_text.strip():
            return note_text.strip()
    text = post.get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    return None


def _root_media_keys(post: dict[str, Any]) -> list[str]:
    attachments = post.get("attachments") or {}
    if not isinstance(attachments, dict):
        return []
    keys = attachments.get("media_keys") or []
    return [str(key) for key in keys if key] if isinstance(keys, list) else []


def _author_summary(author: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(author, dict):
        return None
    return {
        "user_id": _as_str(author.get("id")),
        "username": _as_str(author.get("username")),
        "name": _as_str(author.get("name")),
        "verified": author.get("verified"),
        "created_at": _as_str(author.get("created_at")),
        "public_metrics": author.get("public_metrics") if isinstance(author.get("public_metrics"), dict) else None,
    }


def _media_summary(media: dict[str, Any]) -> dict[str, Any]:
    return {
        "media_key": _as_str(media.get("media_key")),
        "media_type": _as_str(media.get("type")),
        "preview_image_url": _as_str(media.get("preview_image_url")),
        "url": _as_str(media.get("url")),
        "alt_text": _as_str(media.get("alt_text")),
        "duration_ms": media.get("duration_ms"),
        "width": media.get("width"),
        "height": media.get("height"),
        "public_metrics": media.get("public_metrics") if isinstance(media.get("public_metrics"), dict) else None,
    }


def _excerpt(text: str | None, length: int) -> str | None:
    if text is None:
        return None
    return text[:length]


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    value_str = str(value).strip()
    return value_str or None
