from __future__ import annotations

from uuid import uuid4

import pytest

from services.analysis_validator.repositories import AnalysisValidatorRepository


class FakeSession:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.params: list[dict] = []

    def in_transaction(self) -> bool:
        return False

    def begin(self):
        raise AssertionError("transaction not used")

    async def execute(self, statement, params=None):
        self.statements.append(str(statement))
        self.params.append(params or {})


@pytest.mark.asyncio
async def test_insert_state_transition_includes_explicit_generated_id() -> None:
    session = FakeSession()
    repository = AnalysisValidatorRepository(session)
    object_id = uuid4()

    await repository.insert_state_transition(
        object_type="judge_run",
        object_id=object_id,
        from_state="succeeded",
        to_state="analysis_validated",
        reason_code="validator_passed",
    )

    sql = " ".join(session.statements[0].split())
    assert "INSERT INTO state_transitions ( state_transition_id," in sql
    assert "VALUES ( gen_random_uuid()," in sql
    assert session.params[0]["object_id"] == str(object_id)
