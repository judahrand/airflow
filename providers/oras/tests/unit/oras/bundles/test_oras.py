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

import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest import mock

import pytest

import airflow.providers.oras.bundles.oras as bundle_module
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.oras.bundles.oras import OrasDagBundle
from airflow.providers.oras.exceptions import OrasDagBundleError

if TYPE_CHECKING:
    from airflow.providers.oras.hooks.oras import OrasHook

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
SHA512_DIGEST = "sha512:" + "c" * 128


class FakeOrasHook:
    def __init__(
        self,
        current: str,
        artifacts: dict[str, dict[str, bytes]],
        *,
        expected_reference: str = "latest",
    ) -> None:
        self.current = current
        self.artifacts = artifacts
        self.expected_reference = expected_reference
        self.resolve_calls = 0
        self.resolve_error: Exception | None = None
        self.pull_calls: list[str] = []
        self.fail_pull_for: str | None = None

    def resolve(self, reference: str) -> str:
        assert reference == self.expected_reference
        self.resolve_calls += 1
        if self.resolve_error is not None:
            raise self.resolve_error
        return self.current

    def pull(self, reference: str, destination: Path) -> None:
        self.pull_calls.append(reference)
        assert destination.is_dir()
        if self.fail_pull_for == reference:
            (destination / "partial").write_text("partial")
            raise RuntimeError("simulated pull failure")
        for relative_path, content in self.artifacts[reference].items():
            target = destination / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)


def build_artifact(version: str) -> dict[str, bytes]:
    return {"project/dag.py": f"VERSION = {version!r}\n".encode()}


def build_configured_bundle(
    bundle_storage: Path,
    *,
    name: str = "oras-dags",
    version: str | None = None,
    oras_conn_id: str | None = None,
    tracking_ref: str = "latest",
    plain_http: bool = False,
    insecure: bool = False,
    ca_file: str | None = None,
    cert_file: str | None = None,
    key_file: str | None = None,
    view_url_template: str | None = None,
) -> OrasDagBundle:
    del bundle_storage
    return OrasDagBundle(
        name=name,
        repository="registry.test/example/dags",
        tracking_ref=tracking_ref,
        oras_conn_id=oras_conn_id,
        plain_http=plain_http,
        insecure=insecure,
        ca_file=ca_file,
        cert_file=cert_file,
        key_file=key_file,
        view_url_template=view_url_template,
        version=version,
    )


def attach_hook(monkeypatch: pytest.MonkeyPatch, hook: FakeOrasHook) -> None:
    monkeypatch.setattr(bundle_module, "OrasHook", lambda **_: cast("OrasHook", hook))


def get_current_version(bundle: OrasDagBundle) -> str:
    result = bundle.get_current_version()
    if isinstance(result, str):
        return result
    return result.version


def get_tracking_digest_path(bundle: OrasDagBundle) -> Path:
    return bundle.tracking_path / "manifest-digest"


def test_bundle_uses_provider_module_path() -> None:
    assert OrasDagBundle.__module__ == "airflow.providers.oras.bundles.oras"


def test_bundle_error_is_an_airflow_exception() -> None:
    assert issubclass(OrasDagBundleError, AirflowException)


def test_supports_versioning_and_uses_bundle_storage_root(bundle_storage: Path) -> None:
    bundle = build_configured_bundle(bundle_storage)

    assert bundle.supports_versioning is True
    assert bundle.path.is_relative_to(bundle_storage)


def test_repr_contains_bundle_configuration(bundle_storage: Path) -> None:
    bundle = build_configured_bundle(bundle_storage, oras_conn_id="registry")

    representation = repr(bundle)
    assert "OrasDagBundle" in representation
    assert "registry.test/example/dags" in representation
    assert "tracking_ref='latest'" in representation
    assert "oras_conn_id='registry'" in representation


def test_view_url_is_only_available_from_a_user_template(bundle_storage: Path) -> None:
    bundle = build_configured_bundle(bundle_storage)
    template = "https://registry.test/example/dags/manifests/{version}"
    configured = build_configured_bundle(bundle_storage, view_url_template=template)

    assert bundle.view_url_template() is None
    assert bundle.view_url() is None
    assert bundle.view_url(DIGEST_A) is None
    assert configured.view_url_template() == template
    assert configured.view_url(DIGEST_A) == f"https://registry.test/example/dags/manifests/{DIGEST_A}"


def test_airflow_3_0_stores_view_url_template(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bundle_module, "AIRFLOW_V_3_1_PLUS", False)
    template = "https://registry.test/example/dags/manifests/{version}"

    bundle = build_configured_bundle(bundle_storage, view_url_template=template)

    assert bundle.view_url_template() == template


def test_tracking_ref_is_required(bundle_storage: Path) -> None:
    del bundle_storage
    bundle_type = cast("Any", OrasDagBundle)
    with pytest.raises(TypeError, match="tracking_ref"):
        bundle_type(
            name="missing-tracking-ref",
            repository="registry.test/example/dags",
        )


def test_initializes_tracking_bundle_and_returns_digest(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)

    bundle.initialize()

    assert bundle.is_initialized
    assert bundle.path == bundle.tracking_path / "content"
    assert (bundle.path / "project" / "dag.py").read_text() == "VERSION = 'a'\n"
    assert get_tracking_digest_path(bundle).read_text() == DIGEST_A
    assert get_current_version(bundle) == DIGEST_A
    assert hook.resolve_calls == 1
    assert hook.pull_calls == [DIGEST_A]


def test_empty_tracking_bundle_materialization_is_reused(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: {}})
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)

    bundle.initialize()
    bundle.refresh()

    assert bundle.path.is_dir()
    assert list(bundle.path.iterdir()) == []
    assert hook.pull_calls == [DIGEST_A]


def test_tracking_version_requires_initialization(bundle_storage: Path) -> None:
    bundle = build_configured_bundle(bundle_storage)

    with pytest.raises(RuntimeError, match="initialized"):
        bundle.get_current_version()


@pytest.mark.parametrize("digest", [DIGEST_A, SHA512_DIGEST])
def test_fixed_version_is_reported_before_initialization(
    bundle_storage: Path,
    digest: str,
) -> None:
    bundle = build_configured_bundle(bundle_storage, version=digest)

    assert get_current_version(bundle) == digest


def test_airflow_before_3_3_reports_version_as_string(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bundle_module, "AIRFLOW_V_3_3_PLUS", False)
    bundle = build_configured_bundle(bundle_storage, version=DIGEST_A)

    assert bundle.get_current_version() == DIGEST_A


def test_fixed_version_materializes_without_resolving(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_B, {DIGEST_A: build_artifact("a")})
    bundle = build_configured_bundle(bundle_storage, version=DIGEST_A)
    attach_hook(monkeypatch, hook)

    bundle.initialize()

    assert get_current_version(bundle) == DIGEST_A
    assert hook.resolve_calls == 0
    assert hook.pull_calls == [DIGEST_A]
    assert (bundle.path / "project" / "dag.py").read_text() == "VERSION = 'a'\n"
    assert not (bundle.path.parent / "manifest-digest").exists()


def test_tracking_bundle_persists_sha512_digest(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(SHA512_DIGEST, {SHA512_DIGEST: build_artifact("sha512")})
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)

    bundle.initialize()

    assert get_tracking_digest_path(bundle).read_text() == SHA512_DIGEST
    assert get_current_version(bundle) == SHA512_DIGEST


@pytest.mark.parametrize("digest", [DIGEST_A, SHA512_DIGEST])
def test_tracking_bundle_accepts_tagged_digest_reference(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
    digest: str,
) -> None:
    tracking_ref = f"release@{digest}"
    hook = FakeOrasHook(
        digest,
        {digest: build_artifact("pinned")},
        expected_reference=tracking_ref,
    )
    bundle = build_configured_bundle(bundle_storage, tracking_ref=tracking_ref)
    attach_hook(monkeypatch, hook)

    bundle.initialize()

    assert bundle.tracking_ref == tracking_ref
    assert get_current_version(bundle) == digest
    assert get_tracking_digest_path(bundle).read_text() == digest


def test_valid_cached_fixed_version_does_not_construct_hook(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = build_configured_bundle(bundle_storage, version=DIGEST_A)
    (bundle.versions_dir / DIGEST_A / "content").mkdir(parents=True)
    (bundle.path / "dag.py").write_text("DAG = object()\n")
    monkeypatch.setattr(
        bundle_module,
        "OrasHook",
        lambda **_: pytest.fail("hook should not be constructed"),
    )

    bundle.initialize()

    assert bundle.is_initialized
    assert get_current_version(bundle) == DIGEST_A


def test_invalid_cached_fixed_version_is_repaired(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("repaired")})
    bundle = build_configured_bundle(bundle_storage, version=DIGEST_A)
    bundle.path.parent.mkdir(parents=True)
    bundle.path.write_text("not a directory")
    attach_hook(monkeypatch, hook)

    bundle.initialize()

    assert hook.pull_calls == [DIGEST_A]
    assert (bundle.path / "project" / "dag.py").read_text() == "VERSION = 'repaired'\n"


def test_invalid_resolved_digest_is_rejected(bundle_storage: Path) -> None:
    bundle = build_configured_bundle(bundle_storage)

    with pytest.raises(ValueError, match="resolved bundle version"):
        bundle._materialize("latest", storage_dir=bundle.versions_dir)


def test_cached_version_file_is_rejected(bundle_storage: Path) -> None:
    bundle = build_configured_bundle(bundle_storage, version=DIGEST_A)
    bundle.versions_dir.mkdir(parents=True)
    (bundle.versions_dir / DIGEST_A).write_text("not a directory")

    with pytest.raises(OrasDagBundleError, match="not a directory"):
        bundle.initialize()


def test_refresh_moves_tag_and_preserves_historical_version(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(
        DIGEST_A,
        {DIGEST_A: build_artifact("a"), DIGEST_B: build_artifact("b")},
    )
    tracking = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)
    tracking.initialize()

    hook.current = DIGEST_B
    tracking.refresh()

    assert get_current_version(tracking) == DIGEST_B
    assert (tracking.path / "project" / "dag.py").read_text() == "VERSION = 'b'\n"
    assert not tracking._previous_tracking_path.exists()

    historical = build_configured_bundle(bundle_storage, version=DIGEST_A)
    historical.initialize()
    assert get_current_version(historical) == DIGEST_A
    assert (historical.path / "project" / "dag.py").read_text() == "VERSION = 'a'\n"


def test_unchanged_refresh_does_not_pull_again(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)
    bundle.initialize()

    bundle.refresh()

    assert hook.resolve_calls == 2
    assert hook.pull_calls == [DIGEST_A]


def test_new_bundle_instance_reuses_persisted_tracking_version(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    first = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, first_hook)
    first.initialize()

    restarted_hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    restarted = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, restarted_hook)
    restarted.initialize()

    assert restarted_hook.resolve_calls == 1
    assert restarted_hook.pull_calls == []
    assert get_current_version(restarted) == DIGEST_A
    assert (restarted.path / "project" / "dag.py").read_text() == "VERSION = 'a'\n"


def test_initialized_bundle_reads_version_refreshed_by_another_instance(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    updater_hook = FakeOrasHook(
        DIGEST_A,
        {DIGEST_A: build_artifact("a"), DIGEST_B: build_artifact("b")},
    )
    updater = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, updater_hook)
    updater.initialize()

    observer_hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    observer = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, observer_hook)
    observer.initialize()

    updater_hook.current = DIGEST_B
    attach_hook(monkeypatch, updater_hook)
    updater.refresh()

    assert get_current_version(observer) == DIGEST_B
    assert (observer.path / "project" / "dag.py").read_text() == "VERSION = 'b'\n"


def test_each_operation_constructs_a_fresh_hook(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    second = FakeOrasHook(DIGEST_B, {DIGEST_B: build_artifact("b")})
    hooks = iter([first, second])
    factory_calls: list[dict[str, Any]] = []
    bundle = build_configured_bundle(
        bundle_storage,
        oras_conn_id="registry",
        insecure=True,
        ca_file="/ca.pem",
        cert_file="/cert.pem",
        key_file="/key.pem",
    )

    def build_hook(**kwargs: Any) -> OrasHook:
        factory_calls.append(kwargs)
        return cast("OrasHook", next(hooks))

    monkeypatch.setattr(bundle_module, "OrasHook", build_hook)
    bundle.initialize()
    bundle.refresh()

    assert factory_calls == [
        {
            "repository": "registry.test/example/dags",
            "oras_conn_id": "registry",
            "plain_http": False,
            "insecure": True,
            "ca_file": "/ca.pem",
            "cert_file": "/cert.pem",
            "key_file": "/key.pem",
        },
        {
            "repository": "registry.test/example/dags",
            "oras_conn_id": "registry",
            "plain_http": False,
            "insecure": True,
            "ca_file": "/ca.pem",
            "cert_file": "/cert.pem",
            "key_file": "/key.pem",
        },
    ]
    assert get_current_version(bundle) == DIGEST_B


def test_failed_cli_operation_is_not_retried(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    hook.resolve_error = RuntimeError("unauthorized")
    factory_calls = 0
    bundle = build_configured_bundle(bundle_storage)

    def build_hook(**_: Any) -> OrasHook:
        nonlocal factory_calls
        factory_calls += 1
        return cast("OrasHook", hook)

    monkeypatch.setattr(bundle_module, "OrasHook", build_hook)

    with pytest.raises(OrasDagBundleError, match="unauthorized"):
        bundle.initialize()

    assert factory_calls == 1
    assert hook.resolve_calls == 1


@mock.patch.object(
    OrasDagBundle,
    "_get_hook",
    autospec=True,
    side_effect=AirflowException("Connection lookup failed"),
)
def test_connection_failure_is_translated_at_bundle_boundary(
    get_hook: mock.Mock,
    bundle_storage: Path,
) -> None:
    bundle = build_configured_bundle(bundle_storage, oras_conn_id="missing")

    with pytest.raises(OrasDagBundleError, match="Connection lookup failed"):
        bundle.initialize()

    get_hook.assert_called_once_with(bundle)


@pytest.mark.parametrize("operation", ["initialize", "refresh"])
@mock.patch.object(
    OrasDagBundle,
    "lock",
    autospec=True,
    side_effect=OSError("Could not acquire bundle lock"),
)
def test_lock_failure_is_translated_at_bundle_boundary(
    lock: mock.Mock,
    bundle_storage: Path,
    operation: str,
) -> None:
    bundle = build_configured_bundle(bundle_storage)

    with pytest.raises(OrasDagBundleError, match="Could not acquire bundle lock"):
        getattr(bundle, operation)()

    lock.assert_called_once_with(bundle)


def test_initialize_recovers_interrupted_tracking_replace(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)
    bundle.initialize()
    bundle.tracking_path.rename(bundle._previous_tracking_path)

    restarted = build_configured_bundle(bundle_storage)
    restarted.initialize()

    assert not restarted._previous_tracking_path.exists()
    assert get_tracking_digest_path(restarted).read_text() == DIGEST_A
    assert (restarted.path / "project" / "dag.py").read_text() == "VERSION = 'a'\n"
    assert hook.pull_calls == [DIGEST_A]


def test_initialize_removes_stale_previous_tracking_copy(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)
    bundle.initialize()
    bundle._previous_tracking_path.mkdir()

    restarted = build_configured_bundle(bundle_storage)
    restarted.initialize()

    assert not restarted._previous_tracking_path.exists()
    assert hook.pull_calls == [DIGEST_A]


def test_failed_refresh_preserves_previous_bundle(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(
        DIGEST_A,
        {DIGEST_A: build_artifact("a"), DIGEST_B: build_artifact("b")},
    )
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)
    bundle.initialize()
    hook.current = DIGEST_B
    hook.fail_pull_for = DIGEST_B

    with pytest.raises(OrasDagBundleError, match="simulated pull failure"):
        bundle.refresh()

    assert get_current_version(bundle) == DIGEST_A
    assert get_tracking_digest_path(bundle).read_text() == DIGEST_A
    assert (bundle.path / "project" / "dag.py").read_text() == "VERSION = 'a'\n"
    assert not bundle._previous_tracking_path.exists()


def test_failed_atomic_replace_restores_previous_bundle(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(
        DIGEST_A,
        {DIGEST_A: build_artifact("a"), DIGEST_B: build_artifact("b")},
    )
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)
    bundle.initialize()
    hook.current = DIGEST_B
    real_replace = os.replace

    def replace(source: Path, destination: Path) -> None:
        if Path(source).name == DIGEST_B and Path(destination) == bundle.tracking_path:
            raise OSError("simulated atomic replacement failure")
        real_replace(source, destination)

    monkeypatch.setattr(bundle_module.os, "replace", replace)

    with pytest.raises(OrasDagBundleError, match="simulated atomic replacement failure"):
        bundle.refresh()

    assert get_current_version(bundle) == DIGEST_A
    assert get_tracking_digest_path(bundle).read_text() == DIGEST_A
    assert (bundle.path / "project" / "dag.py").read_text() == "VERSION = 'a'\n"
    assert not bundle._previous_tracking_path.exists()


def test_specific_version_cannot_be_refreshed(bundle_storage: Path) -> None:
    bundle = build_configured_bundle(bundle_storage, version=DIGEST_A)

    with pytest.raises(RuntimeError, match="specific"):
        bundle.refresh()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"repository": "registry.test/example/dags:latest"}, "must not include a tag"),
        ({"tracking_ref": "bad:tag"}, "reference"),
        ({"version": "latest"}, "version must be"),
    ],
)
def test_validates_configuration(
    bundle_storage: Path,
    kwargs: dict[str, object],
    message: str,
) -> None:
    del bundle_storage
    arguments: dict[str, Any] = {
        "name": "invalid",
        "repository": "registry.test/example/dags",
        "tracking_ref": "latest",
    }
    arguments.update(kwargs)

    with pytest.raises(ValueError, match=message):
        OrasDagBundle(**arguments)


def test_concurrent_fixed_version_initializers_share_materialization(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    first = build_configured_bundle(bundle_storage, name="concurrent", version=DIGEST_A)
    second = build_configured_bundle(bundle_storage, name="concurrent", version=DIGEST_A)
    attach_hook(monkeypatch, hook)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(bundle.initialize) for bundle in (first, second)]
        for future in futures:
            future.result(timeout=10)

    assert hook.pull_calls == [DIGEST_A]
    assert (first.path / "project" / "dag.py").is_file()


def test_invalid_cached_version_path_is_rejected(bundle_storage: Path) -> None:
    bundle = build_configured_bundle(bundle_storage, version=DIGEST_A)
    bundle.versions_dir.mkdir(parents=True)
    outside = bundle_storage / "outside"
    outside.mkdir()
    (bundle.versions_dir / DIGEST_A).symlink_to(outside, target_is_directory=True)

    with pytest.raises(OrasDagBundleError, match="symbolic link"):
        bundle.initialize()


def test_tracking_path_symlink_is_rejected(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)
    outside = bundle_storage / "outside-tracking"
    outside.mkdir(parents=True)
    bundle.base_dir.mkdir(parents=True)
    bundle.tracking_path.symlink_to(outside, target_is_directory=True)

    with pytest.raises(OrasDagBundleError, match="symbolic link"):
        bundle.initialize()


def test_tracking_path_file_is_rejected(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)
    bundle.base_dir.mkdir(parents=True)
    bundle.tracking_path.write_text("not a directory")

    with pytest.raises(OrasDagBundleError, match="not a directory"):
        bundle.initialize()


@pytest.mark.parametrize("kind", ["symlink", "file"])
def test_invalid_previous_tracking_path_is_rejected(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)
    bundle.base_dir.mkdir(parents=True)
    if kind == "symlink":
        outside = bundle_storage / "outside-previous"
        outside.mkdir()
        bundle._previous_tracking_path.symlink_to(outside, target_is_directory=True)
        message = "symbolic link"
    else:
        bundle._previous_tracking_path.write_text("not a directory")
        message = "not a directory"

    with pytest.raises(OrasDagBundleError, match=message):
        bundle.initialize()


@pytest.mark.parametrize("marker_value", [None, "latest", "sha256:not-a-digest"])
def test_invalid_tracking_digest_marker_is_repaired(
    bundle_storage: Path,
    marker_value: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    first = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, first_hook)
    first.initialize()
    marker = get_tracking_digest_path(first)
    if marker_value is None:
        marker.unlink()
    else:
        marker.write_text(marker_value)

    repair_hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("repaired")})
    repaired = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, repair_hook)
    repaired.initialize()

    assert repair_hook.pull_calls == [DIGEST_A]
    assert marker.read_text() == DIGEST_A
    assert (repaired.path / "project" / "dag.py").read_text() == "VERSION = 'repaired'\n"


def test_unreadable_tracking_digest_marker_is_repaired(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    first = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, first_hook)
    first.initialize()
    marker = get_tracking_digest_path(first)
    real_read_text = Path.read_text

    def read_text(path: Path, *args: Any, **kwargs: Any) -> str:
        if path == marker:
            raise OSError("simulated unreadable marker")
        return real_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read_text)
    repair_hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("repaired")})
    repaired = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, repair_hook)

    repaired.initialize()

    assert repair_hook.pull_calls == [DIGEST_A]
    assert real_read_text(get_tracking_digest_path(repaired)) == DIGEST_A


def test_non_file_tracking_digest_marker_is_rejected(bundle_storage: Path) -> None:
    bundle = build_configured_bundle(bundle_storage)
    get_tracking_digest_path(bundle).mkdir(parents=True)

    with pytest.raises(ValueError, match="not a regular file"):
        bundle._read_tracking_digest()


def test_non_ascii_tracking_digest_marker_is_rejected(bundle_storage: Path) -> None:
    bundle = build_configured_bundle(bundle_storage)
    get_tracking_digest_path(bundle).parent.mkdir(parents=True)
    get_tracking_digest_path(bundle).write_bytes(b"\xff")

    with pytest.raises(ValueError, match="not ASCII"):
        bundle._read_tracking_digest()


def test_invalid_tracking_digest_cannot_be_written(bundle_storage: Path) -> None:
    bundle = build_configured_bundle(bundle_storage)

    with pytest.raises(ValueError, match="not a valid OCI digest"):
        bundle._write_tracking_digest(bundle.base_dir, "latest")


def test_tracking_digest_marker_symlink_is_repaired_without_changing_target(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    first = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, first_hook)
    first.initialize()
    marker = get_tracking_digest_path(first)
    outside = bundle_storage / "outside-digest"
    outside.write_text(DIGEST_B)
    marker.unlink()
    marker.symlink_to(outside)

    repair_hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("repaired")})
    repaired = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, repair_hook)
    repaired.initialize()

    assert repair_hook.pull_calls == [DIGEST_A]
    assert not get_tracking_digest_path(repaired).is_symlink()
    assert get_tracking_digest_path(repaired).read_text() == DIGEST_A
    assert outside.read_text() == DIGEST_B


def test_invalid_tracking_content_is_repaired(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    first = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, first_hook)
    first.initialize()
    shutil.rmtree(first.path)
    first.path.write_text("not a directory")

    repair_hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("repaired")})
    repaired = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, repair_hook)
    repaired.initialize()

    assert repair_hook.pull_calls == [DIGEST_A]
    assert (repaired.path / "project" / "dag.py").read_text() == "VERSION = 'repaired'\n"


@pytest.mark.parametrize("kind", ["missing", "symlink"])
def test_invalid_cached_version_content_is_repaired(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("repaired")})
    bundle = build_configured_bundle(bundle_storage, version=DIGEST_A)
    bundle.path.parent.mkdir(parents=True)
    if kind == "symlink":
        outside = bundle_storage / "outside-version-content"
        outside.mkdir()
        bundle.path.symlink_to(outside, target_is_directory=True)
    attach_hook(monkeypatch, hook)

    bundle.initialize()

    assert hook.pull_calls == [DIGEST_A]
    assert not bundle.path.is_symlink()
    assert (bundle.path / "project" / "dag.py").read_text() == "VERSION = 'repaired'\n"


def test_get_current_version_rejects_corrupted_marker_after_initialization(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)
    bundle.initialize()
    get_tracking_digest_path(bundle).write_text("invalid")

    with pytest.raises(ValueError, match="not a valid OCI digest"):
        bundle.get_current_version()


def test_partial_work_directory_is_cleaned_after_failure(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    hook.fail_pull_for = DIGEST_A
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)

    with pytest.raises(OrasDagBundleError, match="simulated pull failure"):
        bundle.initialize()

    assert not list(bundle.base_dir.glob(".tracking.*"))
    assert not bundle.tracking_path.exists()


def test_previous_tracking_cleanup_failure_does_not_fail_bundle(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = build_configured_bundle(bundle_storage)
    bundle._previous_tracking_path.mkdir(parents=True)

    def fail_cleanup(path: Path) -> None:
        assert path == bundle._previous_tracking_path
        raise OSError("simulated cleanup failure")

    monkeypatch.setattr(shutil, "rmtree", fail_cleanup)

    bundle._remove_previous_tracking_materialization()


def test_tracking_bundle_is_outside_stale_version_cleanup(
    bundle_storage: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = FakeOrasHook(DIGEST_A, {DIGEST_A: build_artifact("a")})
    bundle = build_configured_bundle(bundle_storage)
    attach_hook(monkeypatch, hook)
    bundle.initialize()

    stale_version = bundle.versions_dir / DIGEST_A
    stale_version.mkdir(parents=True)
    shutil.rmtree(stale_version)

    assert (bundle.path / "project" / "dag.py").is_file()
