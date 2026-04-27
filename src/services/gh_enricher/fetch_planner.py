from __future__ import annotations

from urllib.parse import urlparse

from .models import ArtifactRecord, GitHubArtifactLocator


class GitHubFetchPlanner:
    def build_locator(self, artifact: ArtifactRecord) -> GitHubArtifactLocator:
        key = artifact.artifact_key_json or {}
        artifact_type = artifact.artifact_type

        if artifact_type == "github_gist":
            gist_id = self._maybe_str(key.get("gist_id"))
            if gist_id:
                return GitHubArtifactLocator(artifact_type="github_gist", gist_id=gist_id)
            parsed = urlparse(artifact.canonical_url or "")
            parts = [part for part in parsed.path.split("/") if part]
            if parts:
                return GitHubArtifactLocator(artifact_type="github_gist", gist_id=parts[-1])
            raise ValueError("github_gist artifact missing gist_id")

        owner = self._maybe_str(key.get("owner"))
        repo = self._maybe_str(key.get("repo"))
        if owner and repo:
            return GitHubArtifactLocator(
                artifact_type=artifact_type,
                owner=owner,
                repo=repo.removesuffix(".git"),
                ref=self._maybe_str(key.get("ref")),
                path=self._maybe_str(key.get("path")),
                page_path=self._maybe_str(key.get("page_path")),
            )

        parsed = urlparse(artifact.canonical_url or "")
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 2:
            return GitHubArtifactLocator(
                artifact_type=artifact_type,
                owner=parts[0],
                repo=parts[1].removesuffix(".git"),
            )

        raise ValueError(f"unable to derive github locator for artifact_id={artifact.artifact_id}")

    @staticmethod
    def _maybe_str(value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None
