from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qs, urlparse

from .models import CanonicalArtifact, ResolvedUrl, TextSurfaces


_GITHUB_PAGE_PREFIXES = {
    "issues",
    "pull",
    "pulls",
    "discussions",
    "releases",
    "actions",
    "wiki",
    "commit",
    "commits",
    "tags",
    "branches",
    "packages",
    "security",
    "network",
    "pulse",
    "graphs",
}
_GITHUB_SUBPATH_PREFIXES = {"blob", "tree"}
_X_STATUS_RE = re.compile(r"^/([^/]+)/status(?:es)?/([0-9]+)(?:/|$)", re.IGNORECASE)


def canonicalize_resolved_urls(urls: list[ResolvedUrl]) -> list[CanonicalArtifact]:
    artifacts: list[CanonicalArtifact] = []
    seen: set[str] = set()
    for url in urls:
        target = url.resolved_url or url.normalized_url
        artifact = canonicalize_url(target, observed=url)
        if artifact.canonical_id in seen:
            continue
        seen.add(artifact.canonical_id)
        artifacts.append(artifact)
    return artifacts


def build_text_idea_artifact(surfaces: TextSurfaces) -> CanonicalArtifact:
    digest = hashlib.sha256(surfaces.hash_surface.encode("utf-8")).hexdigest()
    return CanonicalArtifact(
        artifact_type="text_idea",
        canonical_id=f"text_idea:{digest}",
        canonical_url=None,
        normalized_host=None,
        artifact_key_json={"hash": digest},
        observed_url=None,
        normalized_url=None,
        resolved_url=None,
        source_kind="text",
        classification="text_idea",
        provider_route=None,
    )


def canonicalize_url(url: str, *, observed: ResolvedUrl | None = None) -> CanonicalArtifact:
    parsed = urlparse(url)
    host = _normalize_host(parsed.hostname or "")
    path_parts = [part for part in parsed.path.split("/") if part]
    if observed is not None and observed.resolution_status == "short_url_unresolved":
        return _artifact(
            artifact_type="short_url_unresolved",
            canonical_id=f"short_url_unresolved:{_stable_url_hash(observed.normalized_url)}",
            canonical_url=observed.normalized_url,
            normalized_host=host,
            artifact_key_json={"url": observed.normalized_url},
            observed=observed,
            classification="short_url_unresolved",
        )
    if host == "github.com":
        github = _canonicalize_github(url, path_parts, observed=observed)
        if github is not None:
            return github
    if host == "gist.github.com":
        gist = _canonicalize_gist(url, path_parts, observed=observed)
        if gist is not None:
            return gist
    if host in {"x.com", "twitter.com", "mobile.twitter.com"}:
        x_post = _canonicalize_x_post(url, parsed.path, observed=observed)
        if x_post is not None:
            return x_post
    if parsed.scheme in {"http", "https"} and host:
        return _artifact(
            artifact_type="web_article",
            canonical_id=f"web_article:{_stable_url_hash(_canonical_web_url(parsed))}",
            canonical_url=_canonical_web_url(parsed),
            normalized_host=host,
            artifact_key_json={"url": _canonical_web_url(parsed), "host": host},
            observed=observed,
            classification="web_article",
            provider_route="web",
        )
    return _artifact(
        artifact_type="unknown_link",
        canonical_id=f"unknown_link:{_stable_url_hash(url)}",
        canonical_url=url,
        normalized_host=host or None,
        artifact_key_json={"url": url},
        observed=observed,
        classification="unknown_link",
    )


def _canonicalize_github(
    url: str,
    path_parts: list[str],
    *,
    observed: ResolvedUrl | None,
) -> CanonicalArtifact | None:
    if len(path_parts) < 2:
        return None
    owner = _clean_github_segment(path_parts[0])
    repo = _clean_github_segment(path_parts[1]).removesuffix(".git")
    if not owner or not repo:
        return None
    owner_repo = f"{owner}/{repo}".lower()
    repo_url = f"https://github.com/{owner_repo}"
    repo_anchor = _artifact(
        artifact_type="github_repo",
        canonical_id=f"github_repo:{owner_repo}",
        canonical_url=repo_url,
        normalized_host="github.com",
        artifact_key_json={"owner": owner, "repo": repo, "repo_full_name": owner_repo},
        observed=observed,
        classification="github_repo",
        provider_route="github",
    )
    if len(path_parts) == 2:
        return repo_anchor

    prefix = path_parts[2].lower()
    if prefix in _GITHUB_SUBPATH_PREFIXES and len(path_parts) >= 5:
        ref = path_parts[3]
        subpath = "/".join(path_parts[4:])
        canonical_url = f"{repo_url}/{prefix}/{ref}/{subpath}"
        return _artifact(
            artifact_type="github_subpath",
            canonical_id=f"github_subpath:{owner_repo}:{prefix}:{ref}:{subpath}".lower(),
            canonical_url=canonical_url,
            normalized_host="github.com",
            artifact_key_json={
                "owner": owner,
                "repo": repo,
                "repo_full_name": owner_repo,
                "subpath_kind": prefix,
                "ref": ref,
                "path": subpath,
            },
            observed=observed,
            classification="github_subpath",
            provider_route="github",
            inferred_repo=repo_anchor,
        )
    if prefix in _GITHUB_PAGE_PREFIXES:
        page_path = "/".join(path_parts[2:])
        canonical_url = f"{repo_url}/{page_path}"
        return _artifact(
            artifact_type="github_repo_page",
            canonical_id=f"github_repo_page:{owner_repo}:{page_path}".lower(),
            canonical_url=canonical_url,
            normalized_host="github.com",
            artifact_key_json={"owner": owner, "repo": repo, "repo_full_name": owner_repo, "page_path": page_path},
            observed=observed,
            classification="github_repo_page",
            provider_route="github",
            inferred_repo=repo_anchor,
        )
    return repo_anchor


def _canonicalize_gist(
    url: str,
    path_parts: list[str],
    *,
    observed: ResolvedUrl | None,
) -> CanonicalArtifact | None:
    if len(path_parts) < 2:
        return None
    owner = _clean_github_segment(path_parts[0])
    gist_id = _clean_github_segment(path_parts[1])
    if not owner or not gist_id:
        return None
    canonical_url = f"https://gist.github.com/{owner}/{gist_id}"
    return _artifact(
        artifact_type="github_gist",
        canonical_id=f"github_gist:{gist_id.lower()}",
        canonical_url=canonical_url,
        normalized_host="gist.github.com",
        artifact_key_json={"owner": owner, "gist_id": gist_id},
        observed=observed,
        classification="github_gist",
        provider_route="github",
    )


def _canonicalize_x_post(
    url: str,
    path: str,
    *,
    observed: ResolvedUrl | None,
) -> CanonicalArtifact | None:
    match = _X_STATUS_RE.match(path)
    if match is None:
        return None
    author = match.group(1)
    post_id = match.group(2)
    canonical_url = f"https://x.com/{author}/status/{post_id}"
    return _artifact(
        artifact_type="x_post",
        canonical_id=f"x_post:{post_id}",
        canonical_url=canonical_url,
        normalized_host="x.com",
        artifact_key_json={"author": author, "post_id": post_id},
        observed=observed,
        classification="x_post",
        provider_route="x",
    )


def _artifact(
    *,
    artifact_type: str,
    canonical_id: str,
    canonical_url: str | None,
    normalized_host: str | None,
    artifact_key_json: dict,
    observed: ResolvedUrl | None,
    classification: str | None,
    provider_route: str | None = None,
    inferred_repo: CanonicalArtifact | None = None,
) -> CanonicalArtifact:
    return CanonicalArtifact(
        artifact_type=artifact_type,  # type: ignore[arg-type]
        canonical_id=canonical_id,
        canonical_url=canonical_url,
        normalized_host=normalized_host,
        artifact_key_json=artifact_key_json,
        observed_url=None if observed is None else observed.observed_url,
        normalized_url=None if observed is None else observed.normalized_url,
        resolved_url=None if observed is None else observed.resolved_url,
        source_kind="derived" if observed is None else observed.source_kind,
        context_path=None if observed is None else observed.context_path,
        classification=classification,
        provider_route=provider_route,
        inferred_repo=inferred_repo,
    )


def _canonical_web_url(parsed) -> str:
    query = parse_qs(parsed.query, keep_blank_values=True)
    clean_query_parts: list[str] = []
    for key in sorted(query):
        if key.lower().startswith("utm_"):
            continue
        for value in query[key]:
            clean_query_parts.append(f"{key}={value}")
    query_string = "&".join(clean_query_parts)
    return parsed._replace(netloc=_normalize_host(parsed.hostname or ""), fragment="", query=query_string).geturl()


def _normalize_host(host: str) -> str:
    host = host.lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _clean_github_segment(value: str) -> str:
    return value.strip()


def _stable_url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()

