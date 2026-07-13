from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from src.services.policy_engine.feedback_eval import (
    ChannelFeedbackObservation,
    ChannelFeedbackSample,
    FeedbackEvalEngine,
    FeedbackRecord,
    channel_fp,
)
from src.services.policy_engine.repositories import PolicyEngineRepository


SAFE_CHANNEL_FP = channel_fp("safe-channel-test")


def test_feedback_label_parsing_and_invalid_row_handling() -> None:
    text = "\n".join(
        [
            json.dumps(
                {
                    "candidate_group_id_suffix": "deadbeef",
                    "analysis_id_suffix": "cafed00d",
                    "label": "useful_now",
                    "operator_score": 4,
                    "reason_code": "strong_tool",
                    "channel_tier": "A",
                    "notes": "raw note must be ignored",
                }
            ),
            json.dumps({"candidate_group_id_suffix": "deadbeef", "label": "not_allowed"}),
            json.dumps({"analysis_id_suffix": "cafed00d", "label": "false_positive", "operator_score": 6}),
            json.dumps({"label": "false_negative"}),
        ]
    )

    result = FeedbackEvalEngine().parse_jsonl(text)

    assert result.valid_feedback_count == 1
    assert result.invalid_feedback_count == 3
    assert result.invalid_reason_distribution == {
        "invalid_label": 1,
        "invalid_operator_score": 1,
        "missing_target_suffix": 1,
    }
    assert result.records[0].reason_code == "strong_tool"


def test_usefulness_score_bucketing() -> None:
    result = FeedbackEvalEngine().evaluate(
        [
            FeedbackRecord(label="useful_now", candidate_group_id_suffix="aaaa", operator_score=4),
            FeedbackRecord(label="useful_later", candidate_group_id_suffix="bbbb", operator_score=5),
        ]
    )

    assert result.usefulness_score_average_bucket == "score_4_to_5"


def test_false_positive_false_negative_distribution() -> None:
    result = FeedbackEvalEngine().evaluate(
        [
            FeedbackRecord(label="false_positive", candidate_group_id_suffix="aaaa"),
            FeedbackRecord(label="false_negative", candidate_group_id_suffix="bbbb"),
            FeedbackRecord(label="false_negative", candidate_group_id_suffix="cccc"),
        ]
    )

    assert result.label_distribution["false_positive"] == 1
    assert result.label_distribution["false_negative"] == 2
    assert result.false_positive_count_bucket == "one"
    assert result.false_negative_count_bucket == "two_to_five"


def test_delivery_outcome_alignment() -> None:
    result = FeedbackEvalEngine().evaluate(
        [
            FeedbackRecord(label="useful_now", candidate_group_id_suffix="aaaa", verdict="inspect_now"),
            FeedbackRecord(label="false_positive", candidate_group_id_suffix="bbbb", verdict="inspect_now"),
            FeedbackRecord(label="false_negative", candidate_group_id_suffix="cccc", verdict="skip"),
            FeedbackRecord(label="duplicate", candidate_group_id_suffix="dddd", delivery_decision="send_now"),
            FeedbackRecord(label="insufficient_evidence", candidate_group_id_suffix="eeee", verdict="inspect_now"),
        ]
    )

    assert result.delivery_distribution["verdict"]["inspect_now"] == 3
    assert result.delivery_distribution["verdict"]["skip"] == 1
    assert result.delivery_distribution["delivery_decision"]["send_now"] == 1
    assert result.policy_outcome_alignment == {
        "aligned_positive": 1,
        "possible_over_send": 1,
        "possible_under_send": 1,
        "duplicate_risk": 1,
        "evidence_risk": 1,
    }


def _observation(category: str, *, artifact_type: str = "github_repo") -> ChannelFeedbackObservation:
    return ChannelFeedbackObservation(
        feedback_category=category,
        verdict="later",
        delivery_decision="send_now",
        primary_artifact_type=artifact_type,
        created_at=datetime(2026, 7, 13, tzinfo=timezone.utc),
    )


def test_channel_aggregation_is_bounded_deterministic_and_channel_safe() -> None:
    sample = ChannelFeedbackSample(
        channel_fingerprint=SAFE_CHANNEL_FP,
        observations=(
            _observation("false_positive"),
            _observation("duplicate"),
            _observation("stale", artifact_type="text_idea"),
            _observation("wrong_priority", artifact_type="text_idea"),
            _observation("useful"),
        ),
        sample_limit=5,
        window_days=30,
    )

    result = FeedbackEvalEngine().aggregate_channel_sample(sample, minimum_sample=5)
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.channel_tier == "C"
    assert result.policy_active is True
    assert result.noise_rate_basis_points == 8_000
    assert result.category_distribution["false_positive"] == 1
    assert result.verdict_distribution == {"inspect_now": 0, "later": 5, "skip": 0}
    assert result.delivery_distribution == {"send_now": 5, "send_digest": 0, "suppress": 0}
    assert result.artifact_type_distribution == {"github_repo": 3, "text_idea": 2}
    assert SAFE_CHANNEL_FP in rendered
    for forbidden in ("raw source sentinel", "https://private.invalid", "-100123456789"):
        assert forbidden not in rendered


def test_empty_below_minimum_and_false_negative_only_samples_are_neutral() -> None:
    engine = FeedbackEvalEngine()

    empty = engine.aggregate_channel_sample(
        ChannelFeedbackSample(channel_fingerprint=None, observations=(), sample_limit=5, window_days=30),
        minimum_sample=5,
    )
    below_minimum = engine.aggregate_channel_sample(
        ChannelFeedbackSample(
            channel_fingerprint=SAFE_CHANNEL_FP,
            observations=(_observation("false_positive"),),
            sample_limit=5,
            window_days=30,
        ),
        minimum_sample=5,
    )
    false_negative_only = engine.aggregate_channel_sample(
        ChannelFeedbackSample(
            channel_fingerprint=SAFE_CHANNEL_FP,
            observations=tuple(_observation("false_negative") for _ in range(5)),
            sample_limit=5,
            window_days=30,
        ),
        minimum_sample=5,
    )

    assert (empty.channel_tier, empty.policy_active) == ("B", False)
    assert (below_minimum.channel_tier, below_minimum.policy_active) == ("B", False)
    assert (false_negative_only.channel_tier, false_negative_only.policy_active) == ("B", False)
    assert false_negative_only.noise_rate_basis_points == 0


def test_channel_aggregation_redacts_untrusted_fingerprint_and_artifact_dimension() -> None:
    observation = _observation("useful", artifact_type="https://private.invalid/raw-sentinel")
    result = FeedbackEvalEngine().aggregate_channel_sample(
        ChannelFeedbackSample(
            channel_fingerprint="raw-channel-username-sentinel",
            observations=(observation,),
            sample_limit=5,
            window_days=30,
        ),
        minimum_sample=5,
    )
    rendered = json.dumps(result.to_sanitized_dict(), sort_keys=True)

    assert result.channel_fingerprint is None
    assert result.artifact_type_distribution == {"unknown": 1}
    assert "private.invalid" not in rendered
    assert "raw-channel-username-sentinel" not in rendered


@pytest.mark.parametrize(
    ("sample_limit", "window_days", "reason"),
    [
        (101, 30, "sample_limit_out_of_range"),
        (5, 366, "window_days_out_of_range"),
    ],
)
def test_channel_aggregation_hard_bounds(sample_limit: int, window_days: int, reason: str) -> None:
    sample = ChannelFeedbackSample(
        channel_fingerprint=SAFE_CHANNEL_FP,
        observations=(),
        sample_limit=sample_limit,
        window_days=window_days,
    )

    with pytest.raises(ValueError, match=reason):
        FeedbackEvalEngine().aggregate_channel_sample(sample)


class _MappingResult:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return self.rows


class _ChannelFeedbackSession:
    def __init__(self, channel_registry_id):
        self.channel_registry_id = channel_registry_id
        self.calls = []

    async def execute(self, statement, params=None):
        sql = str(statement)
        self.calls.append((sql, params or {}))
        if "SELECT CASE WHEN channel_ref.registry_count" in sql:
            return _MappingResult([{"channel_registry_id": self.channel_registry_id}])
        if "FROM notification_feedback" in sql:
            return _MappingResult(
                [
                    {
                        "feedback_category": "false_positive",
                        "verdict": "later",
                        "delivery_decision": "send_now",
                        "primary_artifact_type": "github_repo",
                        "created_at": datetime(2026, 7, 13, tzinfo=timezone.utc),
                    }
                ]
            )
        raise AssertionError(sql)


@pytest.mark.asyncio
async def test_policy_repository_feedback_read_is_exact_channel_bounded_and_ordered() -> None:
    channel_registry_id = uuid4()
    session = _ChannelFeedbackSession(channel_registry_id)

    sample = await PolicyEngineRepository(session).load_channel_feedback_sample(
        uuid4(),
        sample_limit=25,
        window_days=30,
    )

    assert sample.channel_fingerprint == channel_fp(channel_registry_id)
    assert len(sample.observations) == 1
    feedback_sql, feedback_params = session.calls[1]
    assert "WHERE channel_registry_id = CAST(:channel_registry_id AS uuid)" in feedback_sql
    assert "ORDER BY created_at DESC, feedback_id DESC" in feedback_sql
    assert "LIMIT :sample_limit" in feedback_sql
    assert feedback_params == {
        "channel_registry_id": str(channel_registry_id),
        "window_days": 30,
        "sample_limit": 25,
    }
