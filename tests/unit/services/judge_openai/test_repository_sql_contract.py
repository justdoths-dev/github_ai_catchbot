from __future__ import annotations

import re
from pathlib import Path


def test_repository_sql_references_only_judge_openai_boundary_tables() -> None:
    source_root = Path(__file__).resolve().parents[4] / "src" / "services" / "judge_openai"
    source = (source_root / "repositories.py").read_text(encoding="utf-8")
    sql_text = "\n".join(re.findall(r"sa\.text\(\s*\"\"\"(.*?)\"\"\"", source, flags=re.S))
    referenced_tables = {
        table.lower()
        for table in re.findall(r"\b(?:FROM|UPDATE|INTO|JOIN)\s+([a-z_][a-z0-9_]*)", sql_text, flags=re.I)
    }

    assert referenced_tables <= {
        "event_outbox",
        "judge_runs",
        "candidate_evidence_bundles",
        "judge_outputs",
    }
    assert {
        "source_messages",
        "telegram_raw_updates",
        "artifact_snapshots",
        "candidate_group_proposals",
        "analyses",
        "state_transitions",
        "notification_plans",
        "notification_renders",
        "notification_delivery_records",
    }.isdisjoint(referenced_tables)


def test_repository_event_types_are_limited_to_requested_and_ready_contracts() -> None:
    source_root = Path(__file__).resolve().parents[4] / "src" / "services" / "judge_openai"
    source = (source_root / "repositories.py").read_text(encoding="utf-8")
    event_type_literals = set(re.findall(r"judge\.[a-z.]+\.v1", source))

    assert event_type_literals == {"judge.call.requested.v1", "judge.output.ready.v1"}
