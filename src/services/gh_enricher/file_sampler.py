from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from typing import Any

from .models import GitHubFileSample


@dataclass(slots=True, frozen=True)
class CandidatePath:
    path: str
    role: str


class GitHubFileSampler:
    _ROLE_ORDER = [
        ("README", ("README", "README.md", "README.rst", "README.txt")),
        (
            "manifest",
            ("package.json", "pyproject.toml", "requirements.txt", "requirements-dev.txt", "Cargo.toml", "go.mod", "pom.xml"),
        ),
        ("lockfile", ("package-lock.json", "pnpm-lock.yaml", "yarn.lock", "poetry.lock", "Cargo.lock")),
        ("ci", (".github/workflows/",)),
        ("tests", ("tests/", "test/", "__tests__/")),
        ("examples", ("examples/", "example/", "demo/")),
        ("docs", ("docs/", "doc/")),
        ("entrypoint", ("main.py", "src/main.py", "app.py", "cli.py")),
        ("config", (".env.example", "config/", "settings/")),
    ]

    def select_paths(self, tree_entries: list[dict[str, Any]], *, max_files: int) -> list[CandidatePath]:
        blob_paths = [str(entry.get("path", "")) for entry in tree_entries if entry.get("type") == "blob"]
        seen: set[str] = set()
        selected: list[CandidatePath] = []

        for role, prefixes in self._ROLE_ORDER:
            for path in blob_paths:
                if path in seen:
                    continue
                if any(path == prefix or path.startswith(prefix) for prefix in prefixes):
                    selected.append(CandidatePath(path=path, role=role))
                    seen.add(path)
                    if len(selected) >= max_files:
                        return selected
        return selected[:max_files]

    def build_sample(
        self,
        *,
        path: str,
        role: str,
        raw_text: str,
        size_bytes: int | None,
        excerpt_chars: int,
    ) -> GitHubFileSample:
        excerpt = raw_text[:excerpt_chars] if raw_text else None
        content_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest() if raw_text else None
        return GitHubFileSample(
            path=path,
            role=role,
            size_bytes=size_bytes,
            content_hash=content_hash,
            excerpt=excerpt,
            raw_blob_ref=None,
        )

    @staticmethod
    def decode_contents_response(payload: dict[str, Any]) -> str:
        if payload.get("encoding") == "base64" and isinstance(payload.get("content"), str):
            return base64.b64decode(payload["content"]).decode("utf-8", errors="replace")
        if isinstance(payload.get("content"), str):
            return str(payload["content"])
        return ""
