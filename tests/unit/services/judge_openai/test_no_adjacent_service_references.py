from __future__ import annotations

from pathlib import Path


def test_judge_openai_source_contains_no_adjacent_service_references() -> None:
    source_root = Path(__file__).resolve().parents[4] / "src" / "services" / "judge_openai"
    source_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(source_root.glob("*.py"))
        if path.name != "__init__.py"
    )

    forbidden_references = [
        "analysis_validator",
        "policy_engine",
        "notifier_telegram",
        "collector_telegram",
        "gh_enricher",
        "x_enricher",
        "web_enricher",
    ]
    for forbidden in forbidden_references:
        assert forbidden not in source_text
