from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]

BOUNDED_RECOVERY_FILES = [
    "src/services/maintenance/delivery_result_worker.py",
    "src/services/maintenance/delivery_retry.py",
    "src/services/maintenance/delivery_replay.py",
    "src/services/maintenance/service.py",
    "src/services/maintenance/repositories.py",
]

FORBIDDEN_IMPORT_ROOTS = {"openai", "httpx", "requests", "aiohttp", "subprocess"}
FORBIDDEN_CALL_NAMES = {
    "TelegramBotClient",
    "send",
    "edit",
    "run_forever",
    "xgroup_create",
    "xreadgroup",
    "xack",
    "xclaim",
    "xautoclaim",
}
FORBIDDEN_TEXT_MARKERS = {"docker", "systemctl", "runtime.env"}


def test_bounded_delivery_recovery_path_has_no_live_or_broad_runtime_authority() -> None:
    for relative_path in BOUNDED_RECOVERY_FILES:
        path = ROOT / relative_path
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text)

        imported_roots = set()
        called_names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    called_names.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    called_names.add(node.func.attr)

        assert imported_roots.isdisjoint(FORBIDDEN_IMPORT_ROOTS), relative_path
        assert called_names.isdisjoint(FORBIDDEN_CALL_NAMES), relative_path
        lowered = text.lower()
        for marker in FORBIDDEN_TEXT_MARKERS:
            assert marker not in lowered, relative_path
