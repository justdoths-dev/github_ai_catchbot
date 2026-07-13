from __future__ import annotations

import ast
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

from tools import bounded_feedback_channel_policy_runner as runner


ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = ROOT / "tools/bounded_feedback_channel_policy_runner.py"
POLICY_OWNER_PATHS = (
    ROOT / "src/services/policy_engine/feedback_eval.py",
    ROOT / "src/services/policy_engine/channel_override_policy.py",
)
ANALYSIS_ID = "11111111-1111-4111-8111-11111111abcd"
CANDIDATE_GROUP_ID = "22222222-2222-4222-8222-22222222dcba"
NOTIFICATION_PLAN_ID = "33333333-3333-4333-8333-33333333beef"
DELIVERY_RECORD_ID = "44444444-4444-4444-8444-44444444cafe"
CHANNEL_REGISTRY_ID = "55555555-5555-4555-8555-55555555fade"
DB_LOCATOR = "sentinel-private-db-locator"
REDIS_LOCATOR = "sentinel-private-redis-locator"
RAW_UNSAFE_VALUE = "private url redacted sentinel"
RAW_NOTE = "operator raw private note"
RAW_CHAT_ID = "-100123456789"
RAW_DEDUPE = "notify:dedupe:secret"
RAW_MATERIAL_HASH = "material-secret-hash"


class Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeRepository:
    def __init__(
        self,
        rows: list[runner.PolicyReadbackRow],
        *,
        feedback_target: runner.FeedbackTargetContext | None = None,
    ) -> None:
        self.rows = rows
        self.calls = []
        self.feedback_target = feedback_target
        self.feedback_by_action: dict[str, runner.StoredNotificationFeedback] = {}
        self.feedback_observations: list[runner.ChannelFeedbackObservation] = []
        self.historical_mutation_calls = 0

    def transaction(self):
        return Tx()

    async def load_policy_readbacks(self, *, analysis_id_suffix, candidate_group_id_suffix, max_rows):
        self.calls.append(
            {
                "analysis_id_suffix": analysis_id_suffix,
                "candidate_group_id_suffix": candidate_group_id_suffix,
                "max_rows": max_rows,
            }
        )
        return self.rows[: max_rows + 1]

    async def load_feedback_target(
        self,
        *,
        analysis_id,
        notification_plan_id,
        notification_delivery_record_id,
    ):
        target = self.feedback_target
        if target is None or target.analysis_id != analysis_id:
            return None
        if notification_plan_id is not None and target.notification_plan_id != notification_plan_id:
            return None
        if (
            notification_delivery_record_id is not None
            and target.notification_delivery_record_id != notification_delivery_record_id
        ):
            return None
        return target

    async def load_feedback_by_action_key(self, operator_action_key):
        return self.feedback_by_action.get(operator_action_key)

    async def insert_notification_feedback(self, *, request, target):
        self.state.database_write_attempted = True
        if request.operator_action_key in self.feedback_by_action:
            return None
        stored = runner.StoredNotificationFeedback(
            feedback_id=uuid4(),
            operator_action_key=request.operator_action_key,
            feedback_category=request.feedback_category,
            analysis_id=target.analysis_id,
            candidate_group_id=target.candidate_group_id,
            notification_plan_id=target.notification_plan_id,
            notification_delivery_record_id=target.notification_delivery_record_id,
            channel_registry_id=target.channel_registry_id,
            verdict=target.verdict,
            delivery_decision=target.delivery_decision,
            primary_artifact_type=target.primary_artifact_type,
        )
        self.feedback_by_action[request.operator_action_key] = stored
        self.feedback_observations.append(
            runner.ChannelFeedbackObservation(
                feedback_category=request.feedback_category,
                verdict=target.verdict,
                delivery_decision=target.delivery_decision,
                primary_artifact_type=target.primary_artifact_type,
                created_at=datetime(2026, 7, 13, tzinfo=timezone.utc),
            )
        )
        return stored

    async def load_channel_feedback_sample(self, *, channel_registry_id, sample_limit, window_days):
        return runner.ChannelFeedbackSample(
            channel_fingerprint=runner.channel_fp(channel_registry_id),
            observations=tuple(self.feedback_observations[:sample_limit]),
            sample_limit=sample_limit,
            window_days=window_days,
        )


class FakeRepositoryBuilder:
    def __init__(self, repository: FakeRepository) -> None:
        self.repository = repository

    async def __call__(self, runtime_config, state):
        del runtime_config
        state.database_session_opened = True
        self.repository.state = state

        async def close() -> None:
            return None

        return runner.FeedbackChannelPolicyRepositoryHandle(repository=self.repository, close=close)


class SqlRows:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return self.rows


class SqlTransaction:
    def __init__(self, session) -> None:
        self.session = session

    async def __aenter__(self):
        self.session.transaction_open = True
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.session.commit_count += 1
        else:
            self.session.rollback_count += 1
        self.session.transaction_open = False
        return False


class SqlFeedbackSession:
    def __init__(self) -> None:
        self.transaction_open = False
        self.commit_count = 0
        self.rollback_count = 0
        self.stored_row = None
        self.insert_count = 0
        self.include_plan = True
        self.include_delivery = True
        self.plan_match_count = 1

    def in_transaction(self):
        return self.transaction_open

    def begin(self):
        return SqlTransaction(self)

    async def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        if "FROM analyses a" in sql:
            return SqlRows(
                [
                    {
                        "analysis_id": UUID(ANALYSIS_ID),
                        "candidate_group_id": UUID(CANDIDATE_GROUP_ID),
                        "verdict": "later",
                        "delivery_decision": "send_now",
                        "primary_artifact_type": "github_repo",
                        "notification_plan_id": UUID(NOTIFICATION_PLAN_ID) if self.include_plan else None,
                        "plan_analysis_id": UUID(ANALYSIS_ID) if self.include_plan else None,
                        "plan_candidate_group_id": UUID(CANDIDATE_GROUP_ID) if self.include_plan else None,
                        "plan_match_count": self.plan_match_count if self.include_plan else None,
                        "notification_delivery_record_id": (
                            UUID(DELIVERY_RECORD_ID) if self.include_plan and self.include_delivery else None
                        ),
                        "delivery_plan_id": (
                            UUID(NOTIFICATION_PLAN_ID) if self.include_plan and self.include_delivery else None
                        ),
                        "channel_registry_id": UUID(CHANNEL_REGISTRY_ID),
                    }
                ]
            )
        if "FROM notification_feedback" in sql and "operator_action_key =" in sql:
            return SqlRows([self.stored_row] if self.stored_row else [])
        if "INSERT INTO notification_feedback" in sql:
            self.insert_count += 1
            self.stored_row = {
                "feedback_id": uuid4(),
                "operator_action_key": params["operator_action_key"],
                "feedback_category": params["feedback_category"],
                "analysis_id": UUID(params["analysis_id"]),
                "candidate_group_id": UUID(params["candidate_group_id"]),
                "notification_plan_id": UUID(params["notification_plan_id"]),
                "notification_delivery_record_id": UUID(params["delivery_record_id"]),
                "channel_registry_id": UUID(params["channel_registry_id"]),
                "verdict": params["verdict"],
                "delivery_decision": params["delivery_decision"],
                "primary_artifact_type": params["primary_artifact_type"],
            }
            return SqlRows([self.stored_row])
        if "WHERE channel_registry_id" in sql:
            if self.stored_row is None:
                return SqlRows([])
            return SqlRows(
                [
                    {
                        "feedback_category": self.stored_row["feedback_category"],
                        "verdict": self.stored_row["verdict"],
                        "delivery_decision": self.stored_row["delivery_decision"],
                        "primary_artifact_type": self.stored_row["primary_artifact_type"],
                        "created_at": datetime(2026, 7, 13, tzinfo=timezone.utc),
                    }
                ]
            )
        raise AssertionError(sql)


class SqlRepositoryBuilder:
    def __init__(self, session: SqlFeedbackSession) -> None:
        self.session = session

    async def __call__(self, runtime_config, state):
        del runtime_config
        state.database_session_opened = True
        repository = runner.SqlAlchemyFeedbackChannelPolicyRepository(self.session, state)

        async def close() -> None:
            return None

        return runner.FeedbackChannelPolicyRepositoryHandle(repository=repository, close=close)


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


def _feedback_target() -> runner.FeedbackTargetContext:
    return runner.FeedbackTargetContext(
        analysis_id=UUID(ANALYSIS_ID),
        candidate_group_id=UUID(CANDIDATE_GROUP_ID),
        notification_plan_id=UUID(NOTIFICATION_PLAN_ID),
        notification_delivery_record_id=UUID(DELIVERY_RECORD_ID),
        channel_registry_id=UUID(CHANNEL_REGISTRY_ID),
        channel_fingerprint=runner.channel_fp(UUID(CHANNEL_REGISTRY_ID)),
        verdict="later",
        delivery_decision="send_now",
        primary_artifact_type="github_repo",
    )


def _feedback_capture_args(*, category: str = "useful", action_key: str = "operator-action-001") -> list[str]:
    return [
        "--mode",
        "execute",
        "--operator-approved",
        "--allow-runtime-config",
        "--allow-database-read",
        "--allow-database-write",
        "--confirm-feedback-write",
        "--analysis-id",
        ANALYSIS_ID,
        "--notification-plan-id",
        NOTIFICATION_PLAN_ID,
        "--notification-delivery-record-id",
        DELIVERY_RECORD_ID,
        "--feedback-category",
        category,
        "--operator-action-key",
        action_key,
    ]


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


def test_durable_feedback_capture_is_append_only_idempotent_and_sanitized(capsys) -> None:
    repository = FakeRepository([], feedback_target=_feedback_target())
    builder = FakeRepositoryBuilder(repository)

    first_exit = runner.main(
        _feedback_capture_args(),
        runtime_config_loader=_runtime_config,
        repository_builder=builder,
    )
    first_output = capsys.readouterr().out
    first = json.loads(first_output)

    second_exit = runner.main(
        _feedback_capture_args(),
        runtime_config_loader=_runtime_config,
        repository_builder=builder,
    )
    second_output = capsys.readouterr().out
    second = json.loads(second_output)

    assert first_exit == second_exit == 0
    assert first["durable_feedback_capture"]["status"] == "created"
    assert second["durable_feedback_capture"]["status"] == "noop"
    assert len(repository.feedback_by_action) == 1
    assert repository.historical_mutation_calls == 0
    assert first["durable_feedback_capture"]["aggregate"]["sample_count"] == 1
    assert first["durable_feedback_capture"]["aggregate"]["policy_active"] is False
    assert first["side_effects"]["database_write_attempted"] is True
    for raw in (
        ANALYSIS_ID,
        CANDIDATE_GROUP_ID,
        NOTIFICATION_PLAN_ID,
        DELIVERY_RECORD_ID,
        CHANNEL_REGISTRY_ID,
        "operator-action-001",
        DB_LOCATOR,
    ):
        assert raw not in first_output
        assert raw not in second_output


def test_default_sql_repository_commits_created_feedback_and_reuses_readback(capsys) -> None:
    session = SqlFeedbackSession()
    builder = SqlRepositoryBuilder(session)

    first_exit = runner.main(
        _feedback_capture_args(),
        runtime_config_loader=_runtime_config,
        repository_builder=builder,
    )
    first = json.loads(capsys.readouterr().out)
    second_exit = runner.main(
        _feedback_capture_args(),
        runtime_config_loader=_runtime_config,
        repository_builder=builder,
    )
    second = json.loads(capsys.readouterr().out)

    assert first_exit == second_exit == 0
    assert first["durable_feedback_capture"]["status"] == "created"
    assert second["durable_feedback_capture"]["status"] == "noop"
    assert session.insert_count == 1
    assert session.commit_count == 2
    assert session.rollback_count == 0
    assert session.stored_row is not None


def test_sql_readback_rehydrates_unique_plan_and_latest_delivery_from_analysis(capsys) -> None:
    session = SqlFeedbackSession()

    exit_code = runner.main(
        ["--allow-runtime-config", "--allow-database-read", "--analysis-id", ANALYSIS_ID],
        runtime_config_loader=_runtime_config,
        repository_builder=SqlRepositoryBuilder(session),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    binding = parsed["durable_feedback_readback"]["identity_binding"]
    assert binding["notification_plan_bound"] is True
    assert binding["notification_delivery_record_bound"] is True
    assert session.insert_count == 0


def test_sql_analysis_only_capture_rehydrates_identity_and_retries_as_noop(capsys) -> None:
    session = SqlFeedbackSession()
    args = _feedback_capture_args()
    for flag in ("--notification-plan-id", "--notification-delivery-record-id"):
        index = args.index(flag)
        del args[index : index + 2]

    first_exit = runner.main(
        args,
        runtime_config_loader=_runtime_config,
        repository_builder=SqlRepositoryBuilder(session),
    )
    first = json.loads(capsys.readouterr().out)
    second_exit = runner.main(
        args,
        runtime_config_loader=_runtime_config,
        repository_builder=SqlRepositoryBuilder(session),
    )
    second = json.loads(capsys.readouterr().out)

    assert first_exit == second_exit == 0
    assert first["durable_feedback_capture"]["status"] == "created"
    assert first["durable_feedback_capture"]["identity_binding"]["notification_plan_bound"] is True
    assert first["durable_feedback_capture"]["identity_binding"]["notification_delivery_record_bound"] is True
    assert second["durable_feedback_capture"]["status"] == "noop"
    assert session.insert_count == 1


def test_sql_readback_allows_legitimately_absent_plan_and_blocks_ambiguous_plan(capsys) -> None:
    absent_session = SqlFeedbackSession()
    absent_session.include_plan = False
    absent_session.include_delivery = False
    absent_exit = runner.main(
        ["--allow-runtime-config", "--allow-database-read", "--analysis-id", ANALYSIS_ID],
        runtime_config_loader=_runtime_config,
        repository_builder=SqlRepositoryBuilder(absent_session),
    )
    absent = json.loads(capsys.readouterr().out)

    ambiguous_session = SqlFeedbackSession()
    ambiguous_session.plan_match_count = 2
    ambiguous_exit = runner.main(
        ["--allow-runtime-config", "--allow-database-read", "--analysis-id", ANALYSIS_ID],
        runtime_config_loader=_runtime_config,
        repository_builder=SqlRepositoryBuilder(ambiguous_session),
    )
    ambiguous = json.loads(capsys.readouterr().out)

    assert absent_exit == 0
    assert absent["durable_feedback_readback"]["identity_binding"]["notification_plan_bound"] is False
    assert absent["durable_feedback_readback"]["identity_binding"]["notification_delivery_record_bound"] is False
    assert ambiguous_exit == 1
    assert ambiguous["reason_code"] == "feedback_target_missing_or_mismatch"


def test_idempotent_retry_uses_stored_channel_snapshot_after_registry_drift(capsys) -> None:
    repository = FakeRepository([], feedback_target=_feedback_target())
    builder = FakeRepositoryBuilder(repository)
    assert runner.main(
        _feedback_capture_args(),
        runtime_config_loader=_runtime_config,
        repository_builder=builder,
    ) == 0
    capsys.readouterr()
    repository.feedback_target = replace(
        _feedback_target(),
        channel_registry_id=None,
        channel_fingerprint=None,
    )

    exit_code = runner.main(
        _feedback_capture_args(),
        runtime_config_loader=_runtime_config,
        repository_builder=builder,
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert parsed["durable_feedback_capture"]["status"] == "noop"
    assert parsed["durable_feedback_readback"]["identity_binding"]["channel_bound"] is True
    assert len(repository.feedback_by_action) == 1


def test_conflicting_idempotency_key_and_mismatched_target_fail_closed(capsys) -> None:
    repository = FakeRepository([], feedback_target=_feedback_target())
    builder = FakeRepositoryBuilder(repository)
    assert runner.main(
        _feedback_capture_args(),
        runtime_config_loader=_runtime_config,
        repository_builder=builder,
    ) == 0
    capsys.readouterr()

    conflict_exit = runner.main(
        _feedback_capture_args(category="false_positive"),
        runtime_config_loader=_runtime_config,
        repository_builder=builder,
    )
    conflict = json.loads(capsys.readouterr().out)

    mismatched_args = _feedback_capture_args(action_key="operator-action-002")
    plan_index = mismatched_args.index(NOTIFICATION_PLAN_ID)
    mismatched_args[plan_index] = str(uuid4())
    mismatch_exit = runner.main(
        mismatched_args,
        runtime_config_loader=_runtime_config,
        repository_builder=builder,
    )
    mismatch = json.loads(capsys.readouterr().out)

    assert conflict_exit == mismatch_exit == 1
    assert conflict["reason_code"] == "feedback_idempotency_conflict"
    assert mismatch["reason_code"] == "feedback_target_missing_or_mismatch"
    assert len(repository.feedback_by_action) == 1


def test_feedback_write_gate_blocks_before_runtime_or_repository_access(capsys) -> None:
    repository = FakeRepository([], feedback_target=_feedback_target())
    args = _feedback_capture_args()
    args.remove("--confirm-feedback-write")

    exit_code = runner.main(
        args,
        runtime_config_loader=lambda: (_ for _ in ()).throw(AssertionError("runtime config must stay closed")),
        repository_builder=FakeRepositoryBuilder(repository),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["reason_code"] == "feedback_write_confirmation_missing"
    assert parsed["side_effects"]["runtime_config_loaded"] is False
    assert repository.feedback_by_action == {}


def test_durable_capture_rejects_legacy_suffix_selector_before_opening_session(capsys) -> None:
    repository = FakeRepository([], feedback_target=_feedback_target())

    exit_code = runner.main(
        [*_feedback_capture_args(), "--analysis-id-suffix", "abcd"],
        runtime_config_loader=lambda: (_ for _ in ()).throw(AssertionError("runtime config must stay closed")),
        repository_builder=FakeRepositoryBuilder(repository),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert parsed["reason_code"] == "mixed_feedback_selector_modes"
    assert parsed["side_effects"]["database_session_opened"] is False


def test_false_negative_capture_does_not_require_plan_or_delivery_identity(capsys) -> None:
    target = replace(
        _feedback_target(),
        notification_plan_id=None,
        notification_delivery_record_id=None,
    )
    repository = FakeRepository([], feedback_target=target)
    args = _feedback_capture_args(category="false_negative")
    for flag in ("--notification-plan-id", "--notification-delivery-record-id"):
        index = args.index(flag)
        del args[index : index + 2]

    exit_code = runner.main(
        args,
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
    )
    parsed = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    binding = parsed["durable_feedback_capture"]["identity_binding"]
    assert binding["analysis_bound"] is True
    assert binding["candidate_group_bound"] is True
    assert binding["notification_plan_bound"] is False
    assert binding["notification_delivery_record_bound"] is False


def test_durable_channel_aggregate_has_read_only_exact_target_readback(capsys) -> None:
    repository = FakeRepository([], feedback_target=_feedback_target())
    repository.feedback_observations.extend(
        [
            runner.ChannelFeedbackObservation(
                feedback_category=category,
                verdict="later",
                delivery_decision="send_now",
                primary_artifact_type="github_repo",
                created_at=datetime(2026, 7, 13, tzinfo=timezone.utc),
            )
            for category in ("false_positive", "duplicate", "stale", "wrong_priority", "useful")
        ]
    )

    exit_code = runner.main(
        [
            "--allow-runtime-config",
            "--allow-database-read",
            "--analysis-id",
            ANALYSIS_ID,
        ],
        runtime_config_loader=_runtime_config,
        repository_builder=FakeRepositoryBuilder(repository),
    )
    output = capsys.readouterr().out
    parsed = json.loads(output)

    assert exit_code == 0
    assert parsed["durable_feedback_capture"] is None
    assert parsed["durable_feedback_readback"]["aggregate"]["channel_tier"] == "C"
    assert parsed["durable_feedback_readback"]["aggregate"]["policy_active"] is True
    assert parsed["side_effects"]["database_write_attempted"] is False
    assert ANALYSIS_ID not in output
    assert CHANNEL_REGISTRY_ID not in output


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


def test_feedback_write_surface_is_append_only_and_cannot_trigger_pipeline_side_effects() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")
    upper = source.upper()

    assert "INSERT INTO NOTIFICATION_FEEDBACK" in upper
    for forbidden in (
        "UPDATE ANALYSES",
        "UPDATE JUDGE_OUTPUTS",
        "UPDATE CANDIDATE_EVIDENCE_BUNDLES",
        "UPDATE NOTIFICATION_DELIVERY_RECORDS",
        "INSERT INTO EVENT_OUTBOX",
        "INSERT INTO REPLAY_REQUESTS",
    ):
        assert forbidden not in upper


def test_pure_policy_modules_are_consumed_by_bounded_runner() -> None:
    source = TOOL_PATH.read_text(encoding="utf-8")

    assert "src.services.policy_engine.feedback_eval" in source
    assert "src.services.policy_engine.channel_override_policy" in source


def test_uuid_sentinels_are_valid() -> None:
    assert UUID(ANALYSIS_ID)
    assert UUID(CANDIDATE_GROUP_ID)
