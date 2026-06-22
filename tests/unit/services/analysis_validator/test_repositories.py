from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from services.analysis_validator.repositories import AnalysisValidatorRepository


class FakeScalarResult:
    def __init__(self, scalar_value: UUID | None) -> None:
        self.scalar_value = scalar_value
        self.scalar_one_or_none_calls = 0

    def scalar_one_or_none(self) -> UUID | None:
        self.scalar_one_or_none_calls += 1
        return self.scalar_value


class FakeSession:
    def __init__(self, execute_result=None) -> None:
        self.execute_result = execute_result
        self.statements: list[str] = []
        self.params: list[dict] = []
        self.execute_calls = 0

    def in_transaction(self) -> bool:
        return False

    def begin(self):
        raise AssertionError("transaction not used")

    async def execute(self, statement, params=None):
        self.execute_calls += 1
        self.statements.append(str(statement))
        self.params.append(params or {})
        return self.execute_result


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scalar_value", "expected_inserted"),
    [(uuid4(), True), (None, False)],
)
async def test_insert_analysis_policy_apply_outbox_consumes_returning_event_id(
    scalar_value: UUID | None,
    expected_inserted: bool,
) -> None:
    execute_result = FakeScalarResult(scalar_value)
    session = FakeSession(execute_result=execute_result)
    repository = AnalysisValidatorRepository(session)

    inserted = await repository.insert_analysis_policy_apply_outbox(
        judge_run_id=uuid4(),
        judge_output_id=uuid4(),
        candidate_group_id=uuid4(),
        bundle_id=uuid4(),
    )

    sql = " ".join(session.statements[0].split())
    assert inserted is expected_inserted
    assert session.execute_calls == 1
    assert execute_result.scalar_one_or_none_calls == 1
    assert "ON CONFLICT (dedupe_key) DO NOTHING" in sql
    assert "RETURNING event_id" in sql
