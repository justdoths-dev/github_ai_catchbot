from __future__ import annotations

import ast
import json
from pathlib import Path
from uuid import UUID

from tools import bounded_feedback_channel_policy_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_feedback_channel_policy_runner.py"
POLICY_OWNER_PATHS = (
    ROOT / "src/services/policy_engine/feedback_eval.py",
    ROOT / "src/services/policy_engine/channel_override_policy.py",
)
ANALYSIS_ID = "11111111-1111-4111-8111-11111111abcd"
CANDIDATE_GROUP_ID = "22222222-2222-4222-8222-22222222dcba"
DB_LOCATOR = "sentinel-private-db-locator"
REDIS_LOCATOR = "sentinel-private-redis-locator"
RAW_UNSAFE_VALUE = "private url redacted sentinel"
RAW_NOTE = "operator raw private note"
RAW_CHAT_ID = "-100123456789"
RAW_DEDUPE = "notify:dedupe:secret"
RAW_MATERIAL_HASH = "material-secret-hash"


class FakeRepository:
    def __init__(self, rows: list[runner.PolicyReadbackRow]) -> None:
        self.rows = rows
        self.calls = []

    async def load_policy_readbacks(self, *, analysis_id_suffix, candidate_group_id_suffix, max_rows):
        self.calls.append(
            {
                "analysis_id_suffix": analysis_id_suffix,
                "candidate_group_id_suffix": candidate_group_id_suffix,
                "max_rows": max_rows,
            }
        )
        return self.rows[: max_rows + 1]


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository

    async def __call__(self, runtime_config, state):
        del runtime_config
        state.database_session_opened = True

        async def close() -> None:
            return None

        return runner.FeedbackChannelPolicyRepositoryHandle(repository=self.repository, close=close)


def _runtime_config() -> runner.BoundedFeedbackChannelPolicyRuntimeConfig:
    return runner.BoundedFeedbackChannelPolicyRuntimeConfig(database_url=DB_LOCATOR)


def _row(
    *,
    analysis_id: str = ANALYSIS_ID,
    candidate_group_id: str = CANDIDATE_GROUP_ID,
    primary_artifact_type: str = "text_idea",
    verdict: str = "later",
    delivery_decision: str = "send_now",
    urgency_profile: str = "normal_silent",
    reason_codes: tuple[str, ...] = ("weak_ai_context", "ai_noise"),
) -> runner.PolicyReadbackRow:
    return runner.PolicyReadbackRow(
        analysis_id=analysis_id,
        candidate_group_id=candidate_group_id,
        verdict=verdict,
        delivery_decision=delivery_decision,
        urgency_profile=urgency_profile,
        reason_codes=reason_codes,
        primary_artifact_type=primary_artifact_type,
        notification_plan_count=1,
        render_count=1,
        delivery_record_count=1,
        suppressed_count=1,
    )


def test_plan_mode_does_not_read_feedback_file_unless_allowed(tmp_path, capsys) -> None:
    feedback_path = tmp_path / "feedback.jsonl"
    feedback_path.write_text(json.dumps({"analysis_id_suffix": "abcd", "label": "useful_now"}) + "\n")

    exit_code = runner.main(["--feedback-jsonl", str(feedback_path)])
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "feedback_file_read_not_allowed"
    assert parsed["side_effects"]["feedback_file_read_attempted"] is False


def test_execute_mode_requires_operator_approval_for_local_feedback_and_channel_policy_reads(tmp_path, capsys) -> None:
    feedback_path = tmp_path / "feedback.jsonl"
    feedback_path.write_text(json.dumps({"analysis_id_suffix": "abcd", "label": "useful_now"}) + "\n")
    channel_path = tmp_path / "policy.json"
    channel_path.write_text(json.dumps({"default_channel_tier": "C"}))

    exit_code = runner.main(
        [
            "--mode",
            "execute",
            "--feedback-jsonl",
            str(feedback_path),
            "--allow-feedback-file-read",
            "--channel-policy-json",
            str(channel_path),
            "--allow-channel-policy-file-read",
        ]
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["reason_code"] == "operator_approval_missing"
    assert parsed["side_effects"]["feedback_file_read_attempted"] is False
    assert parsed["side_effects"]["channel_policy_file_read_attempted"] is False


def test_exact_suffix_ambiguity_blocks_before_pass(capsys) -> None:
    repository = FakeRepository(
        [
            _row(analysis_id="11111111-1111-4111-8111-11111111abcd"),
            _row(analysis_id="33333333-3333-4333-8333-33333333abcd", candidate_group_id=CANDIDATE_GROUP_ID),
        ]
    )

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--analysis-id-suffix",
            "abcd",
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["status"] == "blocked"
    assert parsed["reason_code"] == "ambiguous_analysis_id_suffix"
    assert parsed["target_analysis_fingerprint"] is None


def test_db_readback_uses_exact_selector_and_row_cap(capsys) -> None:
    repository = FakeRepository([_row()])

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--analysis-id-suffix",
            "abcd",
            "--max-rows",
            "7",
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert repository.calls == [
        {
            "analysis_id_suffix": "abcd",
            "candidate_group_id_suffix": None,
            "max_rows": 7,
        }
    ]
    assert parsed["notification_plan_count_bucket"] == "one"
    assert parsed["delivery_record_count_bucket"] == "one"


def test_missing_channel_context_reports_unavailable(capsys) -> None:
    repository = FakeRepository([_row()])

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--analysis-id-suffix",
            "abcd",
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert parsed["channel_context_status"] == "unavailable_in_current_schema"
    assert parsed["channel_tier_observed_or_unknown"] == "unknown"


def test_output_redacts_raw_ids_urls_notes_chat_ids_dedupe_hash_env_and_exception(tmp_path, capsys, monkeypatch) -> None:
    feedback_path = tmp_path / "feedback.jsonl"
    feedback_path.write_text(
        json.dumps(
            {
                "analysis_id_suffix": "abcd",
                "label": "hype",
                "operator_score": 1,
                "reason_code": RAW_UNSAFE_VALUE,
                "notes": RAW_NOTE,
            }
        )
        + "\n"
    )
    monkeypatch.setenv("REDIS_URL", REDIS_LOCATOR)
    repository = FakeRepository(
        [
            _row(
                reason_codes=(
                    RAW_UNSAFE_VALUE,
                    RAW_DEDUPE,
                    RAW_MATERIAL_HASH,
                    RAW_CHAT_ID,
                    "ai_noise",
                )
            )
        ]
    )

    exit_code = runner.main(
        [
            "--operator-approved",
            "--allow-runtime-config",
            "--allow-database-read",
            "--analysis-id-suffix",
            "abcd",
            "--feedback-jsonl",
            str(feedback_path),
            "--allow-feedback-file-read",
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["raw_values_printed"] is False
    for raw in (
        ANALYSIS_ID,
        CANDIDATE_GROUP_ID,
        RAW_UNSAFE_VALUE,
        RAW_NOTE,
        RAW_CHAT_ID,
        RAW_DEDUPE,
        RAW_MATERIAL_HASH,
        DB_LOCATOR,
        REDIS_LOCATOR,
    ):
        assert raw not in output
    assert "unsafe_reason_code" in output


def test_static_ast_has_no_forbidden_live_imports_or_calls() -> None:
    imported_roots: set[str] = set()
    imported_modules: set[str] = set()
    call_names: set[str] = set()
    for path in (TOOL_PATH, *POLICY_OWNER_PATHS):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
                imported_modules.add(node.module)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    call_names.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    call_names.add(node.func.attr)

    assert {"redis", "telegram", "openai", "requests", "httpx", "aiohttp", "subprocess"}.isdisjoint(imported_roots)
    assert not any("gh_enricher" in module or "x_enricher" in module or "web_enricher" in module for module in imported_modules)
    assert {"systemctl", "docker", "alembic", "run_forever"}.isdisjoint(call_names)
    assert "print(" not in TOOL_PATH.read_text(encoding="utf-8")


def test_static_ast_does_not_import_notifier_judge_collector_or_enricher_ownership() -> None:
    for path in (TOOL_PATH, *POLICY_OWNER_PATHS):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_modules: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_modules.add(node.module)
        assert not any(
            forbidden in module
            for module in imported_modules
            for forbidden in (
                "notifier_telegram",
                "judge_openai",
                "collector_telegram",
                "gh_enricher",
                "x_enricher",
                "web_enricher",
                "evidence_assembler",
            )
        )


def test_pure_policy_modules_are_consumed_by_bounded_runner() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")

    assert "src.services.policy_engine.feedback_eval" in source
    assert "src.services.policy_engine.channel_override_policy" in source


def test_uuid_sentinels_are_valid() -> None:
    assert UUID(ANALYSIS_ID)
    assert UUID(CANDIDATE_GROUP_ID)
