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

import importlib
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest
from packaging.version import Version

from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.providers.oras.bundles.oras import OrasDagBundle
from airflow.providers.oras.hooks.oras import OrasHook
from airflow.providers.oras.validators import is_digest

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

pytestmark = pytest.mark.integration("oras")

_REPOSITORY_NAME = "airflow/integration-bundle"
_REFERENCE = "integration"
_IMMUTABLE_REFERENCE = "immutable"
_DAG_ID = "oci_bundle_integration"
_BUNDLE_NAME = "oci-integration"
_MINIMUM_ORAS_VERSION = Version("1.3.2")
_REGISTRY_AUTHORITY = "oras-registry:5000"
_ARTIFACT_TYPE = "application/vnd.airflow-oci-bundle.dag-bundle.v1"
_CONNECTION_ID = "oras_integration"
_BUNDLE_DAG_BAG_CLASS = getattr(
    importlib.import_module(
        "airflow.dag_processing.dagbag" if AIRFLOW_V_3_1_PLUS else "airflow.models.dagbag"
    ),
    "BundleDagBag",
)


@dataclass(frozen=True, slots=True)
class PublishedBundle:
    repository: str
    reference: str
    digest: str
    dag_files: dict[str, str]


@pytest.fixture(scope="session", autouse=True)
def require_oras_cli() -> None:
    executable = shutil.which("oras")
    if executable is None:
        pytest.fail(
            "ORAS provider integration tests require oras 1.3.2 or newer on PATH; "
            "see https://oras.land/docs/installation/"
        )
    result = subprocess.run(
        [executable, "version"],
        check=False,
        capture_output=True,
        text=True,
    )
    output = f"{result.stdout}\n{result.stderr}"
    match = re.search(r"Version:\s*v?([0-9]+\.[0-9]+\.[0-9]+)", output)
    if result.returncode != 0 or match is None or Version(match.group(1)) < _MINIMUM_ORAS_VERSION:
        pytest.fail(
            "ORAS provider integration tests require oras 1.3.2 or newer on PATH; "
            f"found version output: {output.strip() or '<empty>'}"
        )


def configure_oras_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "AIRFLOW_CONN_ORAS_INTEGRATION",
        json.dumps(
            {
                "conn_type": "oras",
                "host": "oras-registry",
                "port": 5000,
            }
        ),
    )


def unpack_current_version(bundle: Any) -> tuple[str, dict[str, Any] | None]:
    result = bundle.get_current_version()
    if isinstance(result, str):
        return result, None
    return cast("str", result.version), cast("dict[str, Any] | None", result.data)


def load_bundle_dag_bag(
    *,
    dag_file: Path,
    bundle_path: Path,
    bundle_name: str,
) -> Any:
    kwargs = {
        "dag_folder": dag_file,
        "bundle_path": bundle_path,
        "load_op_links": False,
    }
    if AIRFLOW_V_3_1_PLUS:
        kwargs["bundle_name"] = bundle_name
    return _BUNDLE_DAG_BAG_CLASS(**kwargs)


@pytest.fixture(scope="session")
def bundle_source(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("oci-bundle")
    return build_dag_source(
        root,
        filename="integration.py",
        dag_id=_DAG_ID,
        package_name="example_project",
    )


def build_dag_source(
    root: Path,
    *,
    filename: str,
    dag_id: str,
    package_name: str | None = None,
) -> Path:
    package_name = package_name or f"bundle_{dag_id}"
    source_root = root / package_name
    dags = source_root / "dags"
    shared = source_root / "shared"
    dags.mkdir(parents=True)
    shared.mkdir()
    (source_root / "__init__.py").write_text("")
    (shared / "__init__.py").write_text("")
    helper_module = f"{dag_id}_helper"
    (shared / f"{helper_module}.py").write_text(f'DAG_ID = "{dag_id}"\n')
    (dags / filename).write_text(
        "from airflow.sdk import DAG\n"
        f"from {package_name}.shared.{helper_module} import DAG_ID\n\n"
        "dag = DAG(dag_id=DAG_ID)\n"
    )
    return source_root


def publish_with_oras(
    source: Path,
    *,
    reference: str,
) -> str:
    command = [
        "oras",
        "push",
        "--plain-http",
        "--artifact-type",
        _ARTIFACT_TYPE,
        f"{_REGISTRY_AUTHORITY}/{_REPOSITORY_NAME}:{reference}",
        source.name,
    ]
    return run_oras_publish(command, cwd=source.parent)


def run_oras_publish(command: list[str], *, cwd: Path | None = None) -> str:
    result = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    output = f"{result.stdout}\n{result.stderr}"
    if result.returncode != 0:
        pytest.fail(f"ORAS failed to publish the integration artifact:\n{output}")
    match = re.search(r"Digest:\s+(sha(?:256|512):[0-9a-f]+)", output)
    if match is None or not is_digest(match.group(1)):
        pytest.fail(f"ORAS did not return a supported manifest digest:\n{output}")
    return match.group(1)


@pytest.fixture(scope="session")
def published_bundle(
    bundle_source: Path,
) -> PublishedBundle:
    repository = f"{_REGISTRY_AUTHORITY}/{_REPOSITORY_NAME}"
    digest = publish_with_oras(
        bundle_source,
        reference=_REFERENCE,
    )
    return PublishedBundle(
        repository=repository,
        reference=_REFERENCE,
        digest=digest,
        dag_files={_DAG_ID: "example_project/dags/integration.py"},
    )


@pytest.fixture(scope="session")
def published_index(
    published_bundle: PublishedBundle,
    tmp_path_factory: pytest.TempPathFactory,
) -> PublishedBundle:
    second_dag_id = "oci_bundle_index_second"
    second_source = build_dag_source(
        tmp_path_factory.mktemp("oci-index-bundle"),
        filename="second.py",
        dag_id=second_dag_id,
        package_name="second_project",
    )
    second_digest = publish_with_oras(second_source, reference="index-second")
    repository = published_bundle.repository
    reference = "integration-index"
    digest = run_oras_publish(
        [
            "oras",
            "manifest",
            "index",
            "create",
            "--plain-http",
            f"{repository}:{reference}",
            published_bundle.digest,
            second_digest,
        ]
    )
    dag_files = {**published_bundle.dag_files, second_dag_id: "second_project/dags/second.py"}
    return PublishedBundle(repository=repository, reference=reference, digest=digest, dag_files=dag_files)


def test_oras_hook_resolves_and_pulls_published_bundle_artifact(
    tmp_path: Path,
    published_bundle: PublishedBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check_oras_hook_resolves_and_pulls_bundle(tmp_path, published_bundle, monkeypatch)


def test_oras_hook_resolves_and_pulls_index_with_multiple_artifacts(
    tmp_path: Path,
    published_index: PublishedBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check_oras_hook_resolves_and_pulls_bundle(tmp_path, published_index, monkeypatch)


def check_oras_hook_resolves_and_pulls_bundle(
    tmp_path: Path,
    published_bundle: PublishedBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_oras_connection(monkeypatch)
    hook = OrasHook(
        repository=published_bundle.repository,
        oras_conn_id=_CONNECTION_ID,
        plain_http=True,
    )

    digest = hook.resolve(published_bundle.reference)
    tagged_digest = f"{published_bundle.reference}@{published_bundle.digest}"
    assert hook.resolve(tagged_digest) == published_bundle.digest
    pulled = tmp_path / "bundle"
    hook.pull(tagged_digest, pulled)

    assert digest == published_bundle.digest
    for relative_path in published_bundle.dag_files.values():
        assert (pulled / relative_path).is_file()
        assert ((pulled / relative_path).parents[1] / "shared").is_dir()


def test_airflow_loads_and_parses_the_configured_oci_bundle(
    tmp_path: Path,
    published_bundle: PublishedBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check_airflow_loads_and_parses_bundle(tmp_path, published_bundle, monkeypatch)


def test_airflow_loads_and_parses_index_with_multiple_artifacts(
    tmp_path: Path,
    published_index: PublishedBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    check_airflow_loads_and_parses_bundle(tmp_path, published_index, monkeypatch)


def check_airflow_loads_and_parses_bundle(
    tmp_path: Path,
    published_bundle: PublishedBundle,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure_oras_connection(monkeypatch)
    storage = tmp_path / "airflow-bundle-storage"
    config = [
        {
            "name": _BUNDLE_NAME,
            "classpath": "airflow.providers.oras.bundles.oras.OrasDagBundle",
            "kwargs": {
                "repository": published_bundle.repository,
                "tracking_ref": published_bundle.reference,
                "oras_conn_id": _CONNECTION_ID,
                "plain_http": True,
            },
        }
    ]
    # BundleDagBag deliberately retains its bundle path. Isolate that change to this test.
    monkeypatch.setattr(sys, "path", list(sys.path))
    with conf_vars(
        {
            ("dag_processor", "dag_bundle_config_list"): json.dumps(config),
            ("dag_processor", "dag_bundle_storage_path"): str(storage),
            ("core", "load_examples"): "False",
        }
    ):
        manager = DagBundlesManager()
        assert [bundle.name for bundle in manager.get_all_dag_bundles()] == [_BUNDLE_NAME]

        bundle = manager.get_bundle(_BUNDLE_NAME)
        bundle.initialize()
        version, version_data = unpack_current_version(bundle)

        assert bundle.is_initialized
        assert bundle.base_dir == storage / _BUNDLE_NAME
        assert version == published_bundle.digest
        assert version_data is None

        materialized_mtime = bundle.path.stat().st_mtime_ns
        restarted = OrasDagBundle(
            name=_BUNDLE_NAME,
            repository=published_bundle.repository,
            tracking_ref=f"{published_bundle.reference}@{published_bundle.digest}",
            oras_conn_id=_CONNECTION_ID,
            plain_http=True,
        )
        restarted.initialize()
        restarted_version, restarted_version_data = unpack_current_version(restarted)

        assert restarted_version == published_bundle.digest
        assert restarted_version_data is None
        assert restarted.path.stat().st_mtime_ns == materialized_mtime
        assert (restarted.tracking_path / "manifest-digest").read_text() == published_bundle.digest

        worker_bundle = manager.get_bundle(_BUNDLE_NAME, version=version)
        worker_bundle.initialize()
        worker_version, worker_version_data = unpack_current_version(worker_bundle)

        assert worker_version == published_bundle.digest
        assert worker_version_data is None
        assert worker_bundle.path == storage / _BUNDLE_NAME / "versions" / version / "content"

        for materialization in (bundle, worker_bundle):
            dag_bag = load_bundle_dag_bag(
                dag_file=materialization.path,
                bundle_path=materialization.path,
                bundle_name=materialization.name,
            )

            assert dag_bag.import_errors == {}
            assert set(dag_bag.dag_ids) == set(published_bundle.dag_files)
            for dag_id, relative_path in published_bundle.dag_files.items():
                assert dag_bag.dags[dag_id].relative_fileloc == relative_path

        assert {
            path.relative_to(worker_bundle.path): path.read_bytes()
            for path in worker_bundle.path.rglob("*.py")
        } == {path.relative_to(bundle.path): path.read_bytes() for path in bundle.path.rglob("*.py")}


def test_airflow_loads_an_immutable_version_after_its_tag_moves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_dag_id = "immutable_original"
    replacement_dag_id = "immutable_replacement"
    original = build_dag_source(
        tmp_path / "original",
        filename="immutable.py",
        dag_id=original_dag_id,
    )
    original_digest = publish_with_oras(
        original,
        reference=_IMMUTABLE_REFERENCE,
    )

    replacement = build_dag_source(
        tmp_path / "replacement",
        filename="immutable.py",
        dag_id=replacement_dag_id,
    )
    replacement_digest = publish_with_oras(
        replacement,
        reference=_IMMUTABLE_REFERENCE,
    )
    assert replacement_digest != original_digest

    configure_oras_connection(monkeypatch)
    repository = f"{_REGISTRY_AUTHORITY}/{_REPOSITORY_NAME}"
    storage = tmp_path / "airflow-bundle-storage"
    config = [
        {
            "name": _BUNDLE_NAME,
            "classpath": "airflow.providers.oras.bundles.oras.OrasDagBundle",
            "kwargs": {
                "repository": repository,
                "tracking_ref": _IMMUTABLE_REFERENCE,
                "oras_conn_id": _CONNECTION_ID,
                "plain_http": True,
            },
        }
    ]
    monkeypatch.setattr(sys, "path", list(sys.path))
    with conf_vars(
        {
            ("dag_processor", "dag_bundle_config_list"): json.dumps(config),
            ("dag_processor", "dag_bundle_storage_path"): str(storage),
            ("core", "load_examples"): "False",
        }
    ):
        bundle = DagBundlesManager().get_bundle(_BUNDLE_NAME, version=original_digest)
        bundle.initialize()
        version, version_data = unpack_current_version(bundle)

        assert version == original_digest
        assert version_data is None
        assert bundle.path == storage / _BUNDLE_NAME / "versions" / original_digest / "content"

        dag_file = bundle.path / f"bundle_{original_dag_id}" / "dags" / "immutable.py"
        dag_bag = load_bundle_dag_bag(
            dag_file=dag_file,
            bundle_path=bundle.path,
            bundle_name=bundle.name,
        )
        assert dag_bag.import_errors == {}
        assert dag_bag.dag_ids == [original_dag_id]
        assert replacement_dag_id not in dag_bag.dags

        materialized_mtime = bundle.path.stat().st_mtime_ns
        bundle.initialize()
        assert bundle.path.stat().st_mtime_ns == materialized_mtime
