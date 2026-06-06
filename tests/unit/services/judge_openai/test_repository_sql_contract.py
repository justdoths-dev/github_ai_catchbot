from __future__ import annotations

import json
import re
from pathlib import Path
from uuid import uuid4

import pytest

from services.judge_openai.repositories import JudgeOpenAIRepository


class _SingleRowResult:
    def __init__(self, row: dict) -> None:
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class _SingleRowSession:
    def __init__(self, row: dict) -> None:
        self._row = row

    def in_transaction(self) -> bool:
        return False

    def begin(self):
        raise AssertionError("load-only repository test must not open a transaction")

    async def execute(self, statement, params=None):
        return _SingleRowResult(self._row)


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


@pytest.mark.asyncio
async def test_repository_rehydrates_legacy_judge_call_without_prompt_cache_key() -> None:
    trigger_event_id = uuid4()
    judge_run_id = uuid4()
    bundle_id = uuid4()
    row = {
        "event_id": trigger_event_id,
        "event_type": "judge.call.requested.v1",
        "payload_json": json.dumps(
            {
                "judge_run_id": str(judge_run_id),
                "bundle_id": str(bundle_id),
                "model": "gpt-5.4-mini",
                "reasoning_effort": "low",
                "prompt_version": "judge_github_primary_v1",
            }
        ),
    }

    job = await JudgeOpenAIRepository(_SingleRowSession(row)).load_job_by_trigger_event_id(trigger_event_id)

    assert job is not None
    assert job.judge_run_id == judge_run_id
    assert job.bundle_id == bundle_id
    assert job.prompt_cache_key is None
