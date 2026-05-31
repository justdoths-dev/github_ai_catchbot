from __future__ import annotations

import importlib
import json
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
SCRIPT = (
    ROOT
    / "scripts"
    / "ops"
    / "dedicated_vps_openai_key_secret_materialization_readiness_gate.py"
)

FAKE_RUNTIME_PATH = "/etc/github-ai-catchbot/private-runtime.env"
FAKE_OPENAI_KEY = "unit" + "-openai" + "-credential" + "-must-not-render"
FAKE_EXCEPTION_BODY = "unit" + "-private-exception-body"
FAKE_RUNTIME_CONTENT = "unit" + "-runtime-env-content-must-not-render"
FAKE_CONFIGURED_SECRET_PATH = "/etc/github-ai-catchbot/private-configured-openai-key"
FAKE_PASSWORD = "unit" + "-password" + "-must-not-render"


@dataclass(frozen=True, slots=True)
class FakeStat:
    st_mode: int
    st_uid: int = 1001
    st_gid: int = 1001
    st_size: int = 32


class FakeFilesystem:
    def __init__(
        self,
        module: Any,
        *,
        runtime_text: str | None = None,
        runtime_stat: FakeStat | None = None,
        runtime_read_error: Exception | None = None,
        secret_stat: FakeStat | None = None,
        secret_lstat: FakeStat | None = None,
        missing_runtime: bool = False,
        missing_secret: bool = False,
    ) -> None:
        self.module = module
        self.runtime_path = FAKE_RUNTIME_PATH
        self.secret_path = module.EXPECTED_OPENAI_API_KEY_FILE_PATH
        self.runtime_text = runtime_text if runtime_text is not None else _valid_runtime_text(module)
        self.runtime_stat = runtime_stat or _regular_file()
        self.runtime_read_error = runtime_read_error
        self.secret_stat = secret_stat or _regular_file(size=32)
        self.secret_lstat = secret_lstat or self.secret_stat
        self.missing_runtime = missing_runtime
        self.missing_secret = missing_secret
        self.read_paths: list[str] = []
        self.stat_paths: list[str] = []
        self.lstat_paths: list[str] = []

    def read_text(self, path: str | Path) -> str:
        path_text = str(path)
        self.read_paths.append(path_text)
        if path_text != self.runtime_path:
            raise AssertionError("secret content must not be read")
        if self.runtime_read_error is not None:
            raise self.runtime_read_error
        return self.runtime_text

    def stat(self, path: str | Path) -> FakeStat:
        path_text = str(path)
        self.stat_paths.append(path_text)
        if path_text == self.runtime_path:
            if self.missing_runtime:
                raise FileNotFoundError(FAKE_EXCEPTION_BODY)
            return self.runtime_stat
        if path_text == self.secret_path:
            if self.missing_secret:
                raise FileNotFoundError(FAKE_EXCEPTION_BODY)
            return self.secret_stat
        raise FileNotFoundError(FAKE_EXCEPTION_BODY)

    def lstat(self, path: str | Path) -> FakeStat:
        path_text = str(path)
        self.lstat_paths.append(path_text)
        if path_text == self.secret_path:
            if self.missing_secret:
                raise FileNotFoundError(FAKE_EXCEPTION_BODY)
            return self.secret_lstat
        raise FileNotFoundError(FAKE_EXCEPTION_BODY)

    def owner_name(self, uid: int) -> str:
        return "deploy" if uid == 1001 else "root"

    def group_name(self, gid: int) -> str:
        return "deploy" if gid == 1001 else "root"


def _module() -> Any:
    return importlib.import_module(
        "scripts.ops.dedicated_vps_openai_key_secret_materialization_readiness_gate"
    )


def _regular_file(
    *,
    mode: int = 0o600,
    uid: int = 1001,
    gid: int = 1001,
    size: int = 32,
) -> FakeStat:
    return FakeStat(stat.S_IFREG | mode, uid, gid, size)


def _directory_file() -> FakeStat:
    return FakeStat(stat.S_IFDIR | 0o600, 1001, 1001, 32)


def _symlink_file() -> FakeStat:
    return FakeStat(stat.S_IFLNK | 0o777, 1001, 1001, 32)


def _valid_runtime_text(module: Any) -> str:
    return "\n".join(
        (
            "# comment is ignored",
            "IGNORED_EXPORT_STYLE line",
            f'OPENAI_API_KEY_FILE="{module.EXPECTED_OPENAI_API_KEY_FILE_PATH}"',
            "INVALID-KEY=value",
            "",
        )
    )


def _run(
    fs: FakeFilesystem,
    *,
    side_effect_flags: dict[str, bool] | None = None,
    forbidden_raw_values: tuple[str, ...] = (),
) -> Any:
    module = fs.module
    return module.generate_report(
        runtime_env_path=fs.runtime_path,
        read_text_func=fs.read_text,
        stat_func=fs.stat,
        lstat_func=fs.lstat,
        owner_name_resolver=fs.owner_name,
        group_name_resolver=fs.group_name,
        side_effect_flags=side_effect_flags,
        forbidden_raw_values=forbidden_raw_values,
    )


def _rendered(result: Any) -> str:
    return json.dumps(result.report, sort_keys=True)


def test_script_exists() -> None:
    assert SCRIPT.exists()


def test_happy_path_passes_with_bucketed_secret_materialization_report() -> None:
    module = _module()
    fs = FakeFilesystem(module)

    result = _run(fs)

    assert result.exit_code == 0
    report = result.report
    assert report["contract_status"] == module.STATUS_PASSED
    assert report["runtime_env_read"] is True
    assert report["runtime_env_exists"] is True
    assert report["runtime_env_is_file"] is True
    assert report["runtime_env_owner_deploy_bucket"] == "one"
    assert report["runtime_env_group_deploy_bucket"] == "one"
    assert report["runtime_env_mode_600_bucket"] == "one"
    assert report["runtime_has_direct_openai_api_key"] is False
    assert report["runtime_has_openai_api_key_file"] is True
    assert report["openai_api_key_file_matches_expected_bucket"] == "one"
    assert report["secret_file_exists"] is True
    assert report["secret_file_is_file"] is True
    assert report["secret_file_is_symlink"] is False
    assert report["secret_file_non_empty_bucket"] == "one"
    assert report["secret_owner_deploy_bucket"] == "one"
    assert report["secret_group_deploy_bucket"] == "one"
    assert report["secret_mode_600_bucket"] == "one"
    assert report["checks_failed"] == []


def test_runtime_env_missing_fails_closed() -> None:
    module = _module()
    fs = FakeFilesystem(module, missing_runtime=True)

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_RUNTIME_ENV_UNREADABLE
    assert result.report["runtime_env_exists"] is False
    assert "runtime_env.stat" in result.report["checks_failed"]


def test_runtime_env_unreadable_exception_is_sanitized() -> None:
    module = _module()
    fs = FakeFilesystem(
        module,
        runtime_read_error=PermissionError("unreadable " + FAKE_EXCEPTION_BODY),
    )

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_RUNTIME_ENV_UNREADABLE
    assert result.report["runtime_env_exists"] is True
    assert result.report["runtime_env_read"] is False
    rendered = _rendered(result)
    assert FAKE_EXCEPTION_BODY not in rendered


def test_runtime_env_not_regular_file_fails_closed() -> None:
    module = _module()
    fs = FakeFilesystem(module, runtime_stat=_directory_file())

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_RUNTIME_ENV_INVALID
    assert result.report["runtime_env_is_file"] is False
    assert "runtime_env.metadata" in result.report["checks_failed"]


def test_runtime_env_wrong_owner_fails_closed() -> None:
    module = _module()
    fs = FakeFilesystem(module, runtime_stat=_regular_file(uid=2002))

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_RUNTIME_ENV_INVALID
    assert result.report["runtime_env_owner_deploy_bucket"] == "zero"


def test_runtime_env_wrong_group_fails_closed() -> None:
    module = _module()
    fs = FakeFilesystem(module, runtime_stat=_regular_file(gid=2002))

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_RUNTIME_ENV_INVALID
    assert result.report["runtime_env_group_deploy_bucket"] == "zero"


def test_runtime_env_wrong_mode_fails_closed() -> None:
    module = _module()
    fs = FakeFilesystem(module, runtime_stat=_regular_file(mode=0o640))

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_RUNTIME_ENV_INVALID
    assert result.report["runtime_env_mode_600_bucket"] == "zero"


def test_direct_openai_api_key_present_fails_closed() -> None:
    module = _module()
    fs = FakeFilesystem(
        module,
        runtime_text=_valid_runtime_text(module) + f"\nOPENAI_API_KEY={FAKE_OPENAI_KEY}\n",
    )

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_DIRECT_KEY_PRESENT
    assert result.report["runtime_has_direct_openai_api_key"] is True


def test_openai_api_key_file_missing_fails_closed() -> None:
    module = _module()
    fs = FakeFilesystem(module, runtime_text="OTHER_VALUE=1\n")

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_FILE_REF_MISSING
    assert result.report["runtime_has_openai_api_key_file"] is False


def test_openai_api_key_file_wrong_value_fails_closed() -> None:
    module = _module()
    fs = FakeFilesystem(module, runtime_text=f"OPENAI_API_KEY_FILE={FAKE_CONFIGURED_SECRET_PATH}\n")

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_FILE_REF_INVALID
    assert result.report["openai_api_key_file_matches_expected_bucket"] == "zero"


def test_secret_missing_fails_closed() -> None:
    module = _module()
    fs = FakeFilesystem(module, missing_secret=True)

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_SECRET_FILE_INVALID
    assert result.report["secret_file_exists"] is False


def test_secret_not_regular_file_fails_closed() -> None:
    module = _module()
    fs = FakeFilesystem(module, secret_stat=_directory_file(), secret_lstat=_directory_file())

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_SECRET_FILE_INVALID
    assert result.report["secret_file_exists"] is True
    assert result.report["secret_file_is_file"] is False


def test_secret_symlink_fails_closed_before_followed_stat() -> None:
    module = _module()
    fs = FakeFilesystem(module, secret_lstat=_symlink_file())

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_SECRET_FILE_INVALID
    assert result.report["secret_file_is_symlink"] is True
    assert fs.stat_paths == [fs.runtime_path]


def test_secret_empty_fails_closed() -> None:
    module = _module()
    fs = FakeFilesystem(module, secret_stat=_regular_file(size=0))

    result = _run(fs)

    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_SECRET_FILE_INVALID
    assert result.report["secret_file_non_empty_bucket"] == "zero"


def test_secret_wrong_owner_group_or_mode_fails_closed() -> None:
    module = _module()
    cases = (
        ("secret_owner_deploy_bucket", _regular_file(uid=2002)),
        ("secret_group_deploy_bucket", _regular_file(gid=2002)),
        ("secret_mode_600_bucket", _regular_file(mode=0o640)),
    )
    for field, secret_stat in cases:
        fs = FakeFilesystem(module, secret_stat=secret_stat, secret_lstat=secret_stat)

        result = _run(fs)

        assert result.exit_code == 1
        assert result.report["contract_status"] == module.STATUS_SECRET_FILE_INVALID
        assert result.report[field] == "zero"


def test_no_secret_file_content_read() -> None:
    module = _module()
    fs = FakeFilesystem(module)

    result = _run(fs)

    assert result.exit_code == 0
    assert fs.read_paths == [fs.runtime_path]
    assert fs.lstat_paths == [fs.secret_path]


def test_no_openai_judge_validator_policy_notifier_or_network_path() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "from openai" not in text
    assert "import openai" not in text
    assert "AsyncOpenAI" not in text
    assert "responses.create" not in text
    assert "src.services.judge_openai" not in text
    assert "src.services.analysis_validator" not in text
    assert "src.services.policy_engine" not in text
    assert "src.services.notifier_telegram" not in text
    assert "subprocess" not in text
    assert "socket" not in text
    assert "requests" not in text
    assert "urllib" not in text


def test_no_db_or_redis_usage_and_forbidden_side_effect_flags_fail_closed() -> None:
    module = _module()
    fs = FakeFilesystem(module)
    text = SCRIPT.read_text(encoding="utf-8").lower()

    assert "database_url" not in text
    assert "redis_url" not in text
    assert "sqlalchemy" not in text
    assert "psycopg" not in text
    assert "redis." not in text
    assert "xadd" not in text
    assert "xack" not in text
    result = _run(fs, side_effect_flags={"database_connected": True})
    assert result.exit_code == 1
    assert result.report["contract_status"] == module.STATUS_FORBIDDEN_SIDE_EFFECT
    assert fs.read_paths == []
    assert fs.stat_paths == []


def test_redaction_guard_omits_raw_key_values_paths_runtime_content_and_exception_bodies() -> None:
    module = _module()
    runtime_text = "\n".join(
        (
            "OPENAI_API_KEY=" + FAKE_OPENAI_KEY,
            f"OPENAI_API_KEY_FILE={FAKE_CONFIGURED_SECRET_PATH}",
            "PASSWORD=" + FAKE_PASSWORD,
            "# " + FAKE_RUNTIME_CONTENT,
        )
    )
    fs = FakeFilesystem(
        module,
        runtime_text=runtime_text,
        runtime_read_error=RuntimeError("runtime read failed " + FAKE_EXCEPTION_BODY),
    )

    result = _run(
        fs,
        forbidden_raw_values=(
            module.DEFAULT_RUNTIME_ENV_PATH,
            module.EXPECTED_OPENAI_API_KEY_FILE_PATH,
            FAKE_RUNTIME_PATH,
            FAKE_CONFIGURED_SECRET_PATH,
            FAKE_OPENAI_KEY,
            FAKE_PASSWORD,
            FAKE_RUNTIME_CONTENT,
            FAKE_EXCEPTION_BODY,
        ),
    )

    assert result.exit_code == 1
    rendered = _rendered(result)
    for value in (
        module.DEFAULT_RUNTIME_ENV_PATH,
        module.EXPECTED_OPENAI_API_KEY_FILE_PATH,
        FAKE_RUNTIME_PATH,
        FAKE_CONFIGURED_SECRET_PATH,
        FAKE_OPENAI_KEY,
        FAKE_PASSWORD,
        FAKE_RUNTIME_CONTENT,
        FAKE_EXCEPTION_BODY,
        "runtime read failed",
    ):
        assert value not in rendered
    assert result.report["raw_values_emitted"] is False
