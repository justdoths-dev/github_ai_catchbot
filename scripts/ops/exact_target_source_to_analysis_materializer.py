from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC_ROOT = _REPO_ROOT / "src"
for _path in (_REPO_ROOT, _SRC_ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from src.services.maintenance.exact_target_source_to_analysis_materializer import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
