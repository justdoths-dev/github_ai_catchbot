from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MIGRATION_DIR = ROOT / "migrations" / "versions"
MAX_ALEMBIC_REVISION_LENGTH = 32


def _literal_assignment(module: ast.Module, name: str) -> object:
    for statement in module.body:
        if not isinstance(statement, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == name for target in statement.targets):
            continue
        return ast.literal_eval(statement.value)
    raise AssertionError(f"missing {name!r} assignment")


def _down_revisions(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, tuple):
        if all(isinstance(item, str) for item in value):
            return value
    raise AssertionError(f"unsupported down_revision value: {value!r}")


def test_alembic_revision_ids_fit_default_version_column_and_form_valid_chain() -> None:
    # Alembic creates alembic_version.version_num as VARCHAR(32) by default.
    migration_files = sorted(MIGRATION_DIR.glob("*.py"))

    assert migration_files

    revisions_by_file: dict[Path, str] = {}
    down_revisions_by_file: dict[Path, tuple[str, ...]] = {}

    for migration_file in migration_files:
        module = ast.parse(migration_file.read_text(encoding="utf-8"))
        revision = _literal_assignment(module, "revision")
        down_revision = _literal_assignment(module, "down_revision")

        assert isinstance(revision, str), migration_file
        assert revision, migration_file
        assert len(revision) <= MAX_ALEMBIC_REVISION_LENGTH, migration_file

        revisions_by_file[migration_file] = revision
        down_revisions_by_file[migration_file] = _down_revisions(down_revision)

    revisions = set(revisions_by_file.values())

    assert len(revisions) == len(revisions_by_file)

    for migration_file, down_revisions in down_revisions_by_file.items():
        for down_revision in down_revisions:
            assert down_revision in revisions, migration_file


def test_product_feedback_migration_preserves_legacy_notification_plan_history() -> None:
    migration_source = (MIGRATION_DIR / "0005_product_feedback.py").read_text(encoding="utf-8")

    assert '"notification_material_claims"' in migration_source
    assert "uq_notification_material_claims_subject_target_material" in migration_source
    assert "uq_notification_plans_subject_target_material" not in migration_source
    assert "op.alter_column(\n        \"notification_plans\"" not in migration_source
    assert "op.drop_table(\"notification_plans\")" not in migration_source
