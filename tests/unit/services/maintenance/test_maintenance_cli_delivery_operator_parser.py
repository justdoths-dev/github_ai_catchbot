from __future__ import annotations

from uuid import uuid4

from services.maintenance import main as maintenance_main


def test_delivery_result_operator_parser_accepts_plan_execute_proof_modes() -> None:
    parser = maintenance_main.build_parser()

    plan_args = parser.parse_args(
        ["delivery-result", "--mode", "plan", "--event-id-suffix", "abcd1234"]
    )
    execute_args = parser.parse_args(
        [
            "delivery-result",
            "--mode",
            "execute",
            "--event-id-suffix",
            "abcd1234",
            "--confirm",
            "write",
        ]
    )
    proof_args = parser.parse_args(
        ["delivery-result", "--mode", "proof", "--event-id-suffix", "abcd1234"]
    )

    assert plan_args.command == "delivery-result"
    assert plan_args.mode == "plan"
    assert execute_args.confirm == "write"
    assert proof_args.mode == "proof"


def test_due_retry_operator_parser_accepts_plan_execute_proof_modes() -> None:
    parser = maintenance_main.build_parser()

    plan_args = parser.parse_args(["due-retry", "--mode", "plan", "--limit", "10"])
    execute_args = parser.parse_args(["due-retry", "--mode", "execute", "--limit", "10", "--confirm", "write"])
    proof_args = parser.parse_args(["due-retry", "--mode", "proof", "--limit", "10"])

    assert plan_args.command == "due-retry"
    assert plan_args.limit == 10
    assert execute_args.confirm == "write"
    assert proof_args.mode == "proof"


def test_delivery_replay_operator_parser_accepts_plan_execute_proof_modes() -> None:
    replay_request_id = str(uuid4())
    parser = maintenance_main.build_parser()

    plan_args = parser.parse_args(
        ["delivery-replay", "--mode", "plan", "--replay-request-id", replay_request_id]
    )
    execute_args = parser.parse_args(
        [
            "delivery-replay",
            "--mode",
            "execute",
            "--replay-request-id",
            replay_request_id,
            "--confirm",
            "write",
        ]
    )
    proof_args = parser.parse_args(
        ["delivery-replay", "--mode", "proof", "--replay-request-id", replay_request_id]
    )

    assert plan_args.command == "delivery-replay"
    assert execute_args.confirm == "write"
    assert proof_args.mode == "proof"


def test_delivery_operator_report_is_suffix_only_and_redacted() -> None:
    event_id = uuid4()

    report = maintenance_main._operator_report(
        "delivery-result",
        "proof",
        "pass",
        None,
        event_id=event_id,
        receipt_exists=True,
        relay_eligible=False,
    )

    assert report["event_id_suffix"] == str(event_id)[-8:]
    assert str(event_id) not in str(report)
    assert report["redactions_applied"]["database_url_omitted"] is True
    assert report["redactions_applied"]["redis_url_omitted"] is True
    assert report["redactions_applied"]["payload_json_omitted"] is True
