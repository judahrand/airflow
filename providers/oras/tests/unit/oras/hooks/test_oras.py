# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import json
import stat
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

import airflow.providers.oras.hooks.oras as hook_module
from airflow.providers.oras.get_provider_info import get_provider_info
from airflow.providers.oras.hooks.oras import OrasHook

DIGEST = "sha256:" + "a" * 64
SHA512_DIGEST = "sha512:" + "b" * 128
TAGGED_DIGEST = f"latest@{DIGEST}"
TAGGED_SHA512_DIGEST = f"release@{SHA512_DIGEST}"


def build_connection(**overrides: Any) -> SimpleNamespace:
    values: dict[str, Any] = {
        "host": "registry.test",
        "port": None,
        "login": None,
        "password": None,
        "extra_dejson": {},
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def use_connection(
    monkeypatch: pytest.MonkeyPatch,
    configured_connection: SimpleNamespace,
) -> None:
    monkeypatch.setattr(
        OrasHook,
        "get_connection",
        lambda self, conn_id: configured_connection,
    )


def test_hook_metadata_and_provider_registration() -> None:
    assert OrasHook.conn_name_attr == "oras_conn_id"
    assert OrasHook.default_conn_name == "oras_default"
    assert OrasHook.conn_type == "oras"
    assert OrasHook.hook_name == "ORAS"

    provider_info = get_provider_info()
    assert provider_info["package-name"] == "apache-airflow-providers-oras"
    connection_type = provider_info["connection-types"][0]
    assert connection_type["hook-class-name"] == "airflow.providers.oras.hooks.oras.OrasHook"
    ui_behaviour = connection_type["ui-field-behaviour"]
    hook_ui_behaviour = OrasHook.get_ui_field_behaviour()
    assert ui_behaviour["hidden-fields"] == hook_ui_behaviour["hidden_fields"]
    assert ui_behaviour["relabeling"] == hook_ui_behaviour["relabeling"]
    assert ui_behaviour["placeholders"] == hook_ui_behaviour["placeholders"]
    assert provider_info["bundles"][0]["python-modules"] == ["airflow.providers.oras.bundles.oras"]


def test_anonymous_hook_does_not_load_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_if_called(*_: Any) -> None:
        pytest.fail("get_connection should not be called")

    monkeypatch.setattr(OrasHook, "get_connection", fail_if_called)

    hook = OrasHook(repository="registry.test/example/dags", oras_conn_id=None)

    assert hook.registry == "registry.test"
    assert hook.username is None
    assert hook.password is None


@mock.patch.object(
    OrasHook,
    "get_connection",
    spec=OrasHook.get_connection,
    side_effect=RuntimeError("Connection lookup failed"),
)
def test_configured_connection_failure_does_not_fall_back_to_anonymous(
    get_connection: mock.Mock,
) -> None:
    with pytest.raises(RuntimeError, match="Connection lookup failed"):
        OrasHook(repository="registry.test/example/dags")

    get_connection.assert_called_once_with("oras_default")


def test_hook_rejects_invalid_repository() -> None:
    with pytest.raises(ValueError, match="must not include a tag"):
        OrasHook(repository="registry.test/example/dags:latest", oras_conn_id=None)


def test_connection_port_is_normalized(monkeypatch: pytest.MonkeyPatch) -> None:
    use_connection(monkeypatch, build_connection(host="HTTPS://Registry.Test/", port=5000))

    hook = OrasHook(repository="registry.test:5000/example/dags")

    assert hook.registry == "registry.test:5000"


def test_connection_requires_registry_host(monkeypatch: pytest.MonkeyPatch) -> None:
    use_connection(monkeypatch, build_connection(host=None))

    with pytest.raises(ValueError, match="host must identify the registry"):
        OrasHook(repository="registry.test/example/dags")


def test_rejects_connection_for_different_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    use_connection(
        monkeypatch,
        build_connection(host="other.test", password="do-not-expose"),
    )

    with pytest.raises(ValueError, match="does not match") as raised:
        OrasHook(repository="registry.test/example/dags")

    assert "do-not-expose" not in str(raised.value)


def test_rejects_login_without_password(monkeypatch: pytest.MonkeyPatch) -> None:
    use_connection(monkeypatch, build_connection(login="airflow"))

    with pytest.raises(ValueError, match="provide a password"):
        OrasHook(repository="registry.test/example/dags")


@pytest.mark.parametrize("extras", [{"plain_http": True}, {"unknown": "value"}])
def test_rejects_connection_extras(
    monkeypatch: pytest.MonkeyPatch,
    extras: dict[str, object],
) -> None:
    use_connection(monkeypatch, build_connection(extra_dejson=extras))

    with pytest.raises(ValueError, match="must not define extras"):
        OrasHook(repository="registry.test/example/dags")


@pytest.mark.parametrize(
    ("kwargs", "exception_type", "message"),
    [
        ({"plain_http": "false"}, TypeError, "plain_http must be a boolean"),
        ({"insecure": 1}, TypeError, "insecure must be a boolean"),
        ({"ca_file": 1}, TypeError, "ca_file must be a path string"),
        ({"ca_file": ""}, ValueError, "ca_file must be a non-empty path"),
        ({"cert_file": "/cert.pem"}, ValueError, "cert_file and key_file"),
        ({"key_file": "/key.pem"}, ValueError, "cert_file and key_file"),
        ({"plain_http": True, "insecure": True}, ValueError, "mutually exclusive"),
    ],
)
def test_rejects_invalid_transport_configuration(
    kwargs: dict[str, Any],
    exception_type: type[Exception],
    message: str,
) -> None:
    with pytest.raises(exception_type, match=message):
        OrasHook(repository="registry.test/example/dags", oras_conn_id=None, **kwargs)


def test_resolve_uses_password_stdin_and_isolated_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "super-secret"
    use_connection(
        monkeypatch,
        build_connection(login="airflow", password=secret),
    )
    captured: dict[str, Any] = {}

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        captured.update(kwargs)
        config_path = Path(command[command.index("--registry-config") + 1])
        captured["config_path"] = config_path
        assert config_path.is_file()
        assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
        assert json.loads(config_path.read_text()) == {"auths": {}}
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({"digest": DIGEST}),
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    hook = OrasHook(repository="registry.test/example/dags")

    assert hook.resolve("latest") == DIGEST

    command = captured["command"]
    assert command[:4] == ["oras", "manifest", "fetch", "--descriptor"]
    assert command[-1] == "registry.test/example/dags:latest"
    assert command[4:7] == ["--username", "airflow", "--password-stdin"]
    assert secret not in command
    assert captured["input"] == f"{secret}\n"
    assert captured["text"] is True
    assert captured["capture_output"] is True
    assert captured["check"] is False
    assert not captured["config_path"].exists()


def test_identity_token_and_transport_flags_are_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    use_connection(
        monkeypatch,
        build_connection(password="identity-token"),
    )
    captured: dict[str, Any] = {}

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({"digest": DIGEST}),
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    hook = OrasHook(
        repository="registry.test/example/dags",
        insecure=True,
        ca_file="/ca.pem",
        cert_file="/cert.pem",
        key_file="/key.pem",
    )

    hook.resolve("stable")

    command = captured["command"]
    assert "--identity-token-stdin" in command
    assert "--insecure" in command
    assert command[command.index("--ca-file") + 1] == "/ca.pem"
    assert command[command.index("--cert-file") + 1] == "/cert.pem"
    assert command[command.index("--key-file") + 1] == "/key.pem"
    assert "identity-token" not in command
    assert captured["input"] == "identity-token\n"


@pytest.mark.parametrize(
    ("reference", "digest"),
    [
        (DIGEST, DIGEST),
        (SHA512_DIGEST, SHA512_DIGEST),
        (TAGGED_DIGEST, DIGEST),
        (TAGGED_SHA512_DIGEST, SHA512_DIGEST),
    ],
)
def test_pinned_reference_resolution_does_not_invoke_oras(
    monkeypatch: pytest.MonkeyPatch,
    reference: str,
    digest: str,
) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("ORAS should not be invoked"),
    )
    hook = OrasHook(repository="registry.test/example/dags", oras_conn_id=None)

    assert hook.resolve(reference) == digest


def test_resolve_accepts_sha512_descriptor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda command, **_: subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({"digest": SHA512_DIGEST}),
            stderr="",
        ),
    )
    hook = OrasHook(repository="registry.test/example/dags", oras_conn_id=None)

    assert hook.resolve("latest") == SHA512_DIGEST


def test_resolve_rejects_invalid_reference_before_invoking_oras(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("ORAS should not be invoked"),
    )
    hook = OrasHook(repository="registry.test/example/dags", oras_conn_id=None)

    with pytest.raises(ValueError, match="reference"):
        hook.resolve("bad:tag")


def test_pull_rejects_invalid_reference_before_invoking_oras(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("ORAS should not be invoked"),
    )
    hook = OrasHook(repository="registry.test/example/dags", oras_conn_id=None)

    with pytest.raises(ValueError, match="reference"):
        hook.pull("bad:tag", tmp_path)


def test_pull_builds_digest_target_and_plain_http_flag(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    use_connection(monkeypatch, build_connection())
    captured: dict[str, Any] = {}

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    hook = OrasHook(repository="registry.test/example/dags", plain_http=True)
    destination = tmp_path / "artifact"

    hook.pull(DIGEST, destination)

    command = captured["command"]
    assert command[:5] == ["oras", "pull", "--no-tty", "--output", str(destination)]
    assert "--plain-http" in command
    assert command[-1] == f"registry.test/example/dags@{DIGEST}"


def test_pull_builds_sha512_digest_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, Any] = {}

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    hook = OrasHook(repository="registry.test/example/dags", oras_conn_id=None)

    hook.pull(SHA512_DIGEST, tmp_path / "artifact")

    assert captured["command"][-1] == f"registry.test/example/dags@{SHA512_DIGEST}"


@pytest.mark.parametrize("reference", [TAGGED_DIGEST, TAGGED_SHA512_DIGEST])
def test_pull_builds_tagged_digest_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reference: str,
) -> None:
    captured: dict[str, Any] = {}

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["command"] = command
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    hook = OrasHook(repository="registry.test/example/dags", oras_conn_id=None)

    hook.pull(reference, tmp_path / "artifact")

    assert captured["command"][-1] == f"registry.test/example/dags:{reference}"


@pytest.mark.parametrize(
    ("stdout", "message"),
    [
        ("not-json", "invalid manifest descriptor"),
        (json.dumps({}), "invalid manifest descriptor"),
        (json.dumps({"digest": "sha256:bad"}), "invalid manifest digest"),
    ],
)
def test_rejects_invalid_resolve_output(
    monkeypatch: pytest.MonkeyPatch,
    stdout: str,
    message: str,
) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda command, **_: subprocess.CompletedProcess(command, 0, stdout=stdout, stderr=""),
    )
    hook = OrasHook(repository="registry.test/example/dags", oras_conn_id=None)

    with pytest.raises(ValueError, match=message):
        hook.resolve("latest")


def test_missing_oras_binary_has_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(*_: Any, **__: Any) -> None:
        raise FileNotFoundError

    monkeypatch.setattr(subprocess, "run", missing)
    hook = OrasHook(repository="registry.test/example/dags", oras_conn_id=None)

    with pytest.raises(FileNotFoundError, match=r"install oras 1\.3\.2"):
        hook.resolve("latest")


@mock.patch.object(subprocess, "run", autospec=True, side_effect=OSError("sensitive launch detail"))
def test_os_error_has_actionable_message_without_exposing_detail(run: mock.Mock) -> None:
    hook = OrasHook(repository="registry.test/example/dags", oras_conn_id=None)

    with pytest.raises(OSError, match="Could not resolve OCI artifact with the ORAS CLI") as raised:
        hook.resolve("latest")

    assert "sensitive launch detail" not in str(raised.value)
    run.assert_called_once()


@mock.patch.object(subprocess, "run", autospec=True)
def test_cli_error_detail_is_bounded_and_operation_is_not_retried(run: mock.Mock) -> None:
    run.return_value = subprocess.CompletedProcess(
        ["oras"],
        1,
        stdout="",
        stderr="x" * 2000,
    )
    hook = OrasHook(repository="registry.test/example/dags", oras_conn_id=None)

    with pytest.raises(RuntimeError) as raised:
        hook.resolve("latest")

    prefix, separator, detail = str(raised.value).partition(": ")
    assert prefix == "ORAS CLI failed to resolve OCI artifact (exit code 1)"
    assert separator == ": "
    assert detail == "x" * 1000
    run.assert_called_once()


def test_cli_error_redacts_credentials_and_registered_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection_secret = "connection-secret"
    registered_secret = "registered-secret"
    use_connection(monkeypatch, build_connection(password=connection_secret))
    captured: dict[str, Path] = {}

    def redact(detail: str) -> str:
        assert connection_secret not in detail
        return detail.replace(registered_secret, "***")

    monkeypatch.setattr(hook_module, "redact", redact)

    def fail(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        config_path = Path(command[command.index("--registry-config") + 1])
        captured["config_path"] = config_path
        assert config_path.is_file()
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr=f"registry rejected {connection_secret} and {registered_secret}",
        )

    monkeypatch.setattr(subprocess, "run", fail)
    hook = OrasHook(repository="registry.test/example/dags")

    with pytest.raises(RuntimeError, match=r"registry rejected \*\*\* and \*\*\*") as raised:
        hook.resolve("latest")

    assert connection_secret not in str(raised.value)
    assert registered_secret not in str(raised.value)
    assert not captured["config_path"].exists()
