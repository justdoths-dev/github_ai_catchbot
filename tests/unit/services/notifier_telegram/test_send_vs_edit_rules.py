from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.notifier_telegram.models import CandidateRenderContext, NotificationPlanDraft
from tests.component.services.notifier_telegram._fakes import config, repo_with_valid_case, service


@pytest.mark.asyncio
async def test_no_recent_delivery_defaults_to_send() -> None:
    repository, intent = repo_with_valid_case()

    action = await service(repository).decide_delivery_action(intent, candidate=repository.candidates[intent.candidate_group_id])

    assert action.mode == "send"


@pytest.mark.asyncio
async def test_same_material_is_noop() -> None:
    repository, intent = repo_with_valid_case()
    _add_recent_delivery(repository, intent, material_change_hash=intent.material_change_hash)

    action = await service(repository, cfg=config(allow_edits=True)).decide_delivery_action(
        intent,
        candidate=repository.candidates[intent.candidate_group_id],
    )

    assert action.mode == "noop"


@pytest.mark.asyncio
async def test_urgency_escalation_forces_send_not_edit() -> None:
    repository, intent = repo_with_valid_case()
    _add_recent_delivery(repository, intent, urgency_profile="normal_silent")

    action = await service(repository, cfg=config(allow_edits=True)).decide_delivery_action(
        intent,
        candidate=repository.candidates[intent.candidate_group_id],
    )

    assert action.mode == "send"
    assert action.reason_code == "notification_urgency_escalation_new_send"


@pytest.mark.asyncio
async def test_primary_canonical_url_change_forces_send() -> None:
    repository, intent = repo_with_valid_case()
    _add_recent_delivery(repository, intent, primary_url="https://github.com/example/old")

    action = await service(repository, cfg=config(allow_edits=True)).decide_delivery_action(
        intent,
        candidate=repository.candidates[intent.candidate_group_id],
    )

    assert action.mode == "send"
    assert action.reason_code == "notification_primary_subject_changed"


@pytest.mark.asyncio
async def test_edits_disabled_forces_send() -> None:
    repository, intent = repo_with_valid_case()
    _add_recent_delivery(repository, intent)

    action = await service(repository, cfg=config(allow_edits=False)).decide_delivery_action(
        intent,
        candidate=repository.candidates[intent.candidate_group_id],
    )

    assert action.mode == "send"
    assert action.reason_code == "notification_edits_disabled"


@pytest.mark.asyncio
async def test_eligible_same_subject_material_changed_edits() -> None:
    repository, intent = repo_with_valid_case()
    _add_recent_delivery(repository, intent)

    action = await service(repository, cfg=config(allow_edits=True)).decide_delivery_action(
        intent,
        candidate=repository.candidates[intent.candidate_group_id],
    )

    assert action.mode == "edit"
    assert action.existing_message_id == 1001


def _add_recent_delivery(
    repository,
    intent,
    *,
    material_change_hash: str = "material-old",
    urgency_profile: str = "high",
    primary_url: str | None = None,
) -> None:
    plan_id = uuid4()
    candidate_group_id = uuid4()
    incoming_candidate = repository.candidates[intent.candidate_group_id]
    repository.candidates[candidate_group_id] = replace(
        incoming_candidate,
        candidate_group_id=candidate_group_id,
        primary_canonical_url=primary_url or incoming_candidate.primary_canonical_url,
    )
    repository.plans[plan_id] = NotificationPlanDraft(
        notification_plan_id=plan_id,
        analysis_id=intent.analysis_id,
        candidate_group_id=candidate_group_id,
        delivery_decision="send_now",
        urgency_profile=urgency_profile,
        target_chat_id=intent.target_chat_id,
        target_thread_id=None,
        render_profile=intent.render_profile,
        dedupe_subject_key=intent.dedupe_subject_key,
        material_change_hash=material_change_hash,
        send_after=None,
        suppress_reason_code=None,
    )
    repository.delivery_records.append(
        {
            "notification_delivery_record_id": uuid4(),
            "notification_plan_id": plan_id,
            "result_status": "sent",
            "telegram_chat_id": intent.target_chat_id,
            "telegram_message_id": 1001,
            "created_at": datetime.now(timezone.utc),
        }
    )
