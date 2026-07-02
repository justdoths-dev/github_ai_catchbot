from __future__ import annotations

import json

from src.services.policy_engine.feedback_eval import FeedbackEvalEngine, FeedbackRecord


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
