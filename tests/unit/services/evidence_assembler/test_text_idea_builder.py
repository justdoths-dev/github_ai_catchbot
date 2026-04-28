from __future__ import annotations

from uuid import uuid4

from services.evidence_assembler.text_idea_builder import TextIdeaBuilder


def test_text_idea_builder_uses_deterministic_content_anchor() -> None:
    builder = TextIdeaBuilder()
    kwargs = {
        "artifact_id": uuid4(),
        "source_message_id": uuid4(),
        "source_version_no": 1,
        "text_surface": " Build a GitHub automation tool with Python. ",
    }

    first = builder.build(**kwargs)
    second = builder.build(**kwargs)

    assert first is not None
    assert second is not None
    assert first.hash_surface == second.hash_surface
    assert builder.content_anchor(first) == builder.input_hash(first)
    assert builder.content_anchor(second) == builder.input_hash(second)
    assert first.status == "ready"
    assert "github" in first.dev_context_signals_json["keyword_hits"]


def test_text_idea_builder_marks_weak_short_text_low_evidence() -> None:
    draft = TextIdeaBuilder().build(
        artifact_id=uuid4(),
        source_message_id=uuid4(),
        source_version_no=1,
        text_surface="hello",
    )

    assert draft is not None
    assert draft.status == "low_evidence"
    assert "short_text_surface" in draft.evidence_limitations
