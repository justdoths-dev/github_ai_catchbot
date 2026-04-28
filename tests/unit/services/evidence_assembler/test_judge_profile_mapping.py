from __future__ import annotations

from services.evidence_assembler.config import EvidenceAssemblerConfig
from services.evidence_assembler.service import EvidenceAssemblerService


class FakeRepository:
    pass


def _service() -> EvidenceAssemblerService:
    return EvidenceAssemblerService(
        EvidenceAssemblerConfig(
            app_env="test",
            database_url="postgresql://test",
            redis_url="redis://test",
            queue_name="q.candidate.bundle",
            consumer_group="evidence-assembler",
            consumer_name="test",
            batch_size=1,
            block_ms=1,
            bundle_profile_version="bundle_profile_v1",
            enable_text_idea=True,
            enable_reroot=True,
            log_level="INFO",
        ),
        repository=FakeRepository(),  # type: ignore[arg-type]
    )


def test_judge_profile_mapping_uses_locked_profile_names() -> None:
    service = _service()

    assert service._judge_profile_for_primary("github_repo") == "github_primary"
    assert service._judge_profile_for_primary("github_subpath") == "github_primary"
    assert service._judge_profile_for_primary("github_repo_page") == "github_primary"
    assert service._judge_profile_for_primary("github_gist") == "github_primary"
    assert service._judge_profile_for_primary("x_post") == "x_primary"
    assert service._judge_profile_for_primary("web_article") == "text_idea_primary"
    assert service._judge_profile_for_primary("text_idea") == "text_idea_primary"
    assert service._judge_profile_for_primary("unknown_link") is None
