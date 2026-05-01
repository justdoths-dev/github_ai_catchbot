from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest

from tests.component.services.notifier_telegram._fakes import repo_with_valid_case, service


@pytest.mark.asyncio
async def test_same_notification_plan_id_does_not_create_duplicate_plan() -> None:
    repository, intent = repo_with_valid_case()
    notifier = service(repository)

    first = await notifier._concretize_plan(intent, status="planned")  # noqa: SLF001
    second = await notifier._concretize_plan(intent, status="planned")  # noqa: SLF001

    assert first == intent.notification_plan_id
    assert second == intent.notification_plan_id
    assert len(repository.plans) == 1


@pytest.mark.asyncio
async def test_same_analysis_target_material_does_not_create_duplicate_plan() -> None:
    repository, intent = repo_with_valid_case()
    notifier = service(repository)
    duplicate_intent = replace(intent, notification_plan_id=uuid4())

    first = await notifier._concretize_plan(intent, status="planned")  # noqa: SLF001
    second = await notifier._concretize_plan(duplicate_intent, status="planned")  # noqa: SLF001

    assert first == intent.notification_plan_id
    assert second == intent.notification_plan_id
    assert len(repository.plans) == 1
