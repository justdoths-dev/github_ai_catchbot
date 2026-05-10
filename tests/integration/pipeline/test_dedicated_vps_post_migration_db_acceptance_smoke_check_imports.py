from __future__ import annotations

import importlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_dedicated_vps_post_migration_db_acceptance_smoke_check_imports_successfully() -> None:
    module = importlib.import_module("scripts.ops.dedicated_vps_post_migration_db_acceptance_smoke_check")

    assert module.REPORT_TYPE == "dedicated_vps_post_migration_db_acceptance_smoke_check_v1"


def test_cli_entrypoint_exists() -> None:
    module = importlib.import_module("scripts.ops.dedicated_vps_post_migration_db_acceptance_smoke_check")

    assert callable(module.main)


def test_report_generation_passes_against_current_repo_root() -> None:
    module = importlib.import_module("scripts.ops.dedicated_vps_post_migration_db_acceptance_smoke_check")

    result = module.generate_report(ROOT)

    assert result.exit_code == 0
    assert result.report["checks_failed"] == []
    assert result.report["repo_text_only"] is True
