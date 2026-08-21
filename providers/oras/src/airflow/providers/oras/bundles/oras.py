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

"""Versioned Airflow Dag bundle backed by the ORAS CLI."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import structlog

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_3_PLUS
from airflow.providers.oras.exceptions import OrasDagBundleError
from airflow.providers.oras.hooks.oras import OrasHook
from airflow.providers.oras.validators import is_digest, validate_reference, validate_repository

if AIRFLOW_V_3_3_PLUS:
    from airflow.dag_processing.bundles.base import BundleVersion

log = structlog.get_logger(__name__)

_TRACKING_DIGEST_FILENAME = "manifest-digest"


class OrasDagBundle(BaseDagBundle):
    """
    Expose a versioned OCI artifact as an Airflow Dag bundle.

    :param repository: Tagless OCI repository including its registry hostname.
    :param tracking_ref: OCI tag, digest, or pinned ``tag@digest`` reference followed by the Dag
        processor.
    :param oras_conn_id: Optional ORAS Connection containing credentials. Pass ``None`` for
        anonymous access.
    :param plain_http: Use plain HTTP for registry communication.
    :param insecure: Skip TLS certificate verification.
    :param ca_file: Path to a custom certificate authority bundle.
    :param cert_file: Path to a client certificate. Must be used with ``key_file``.
    :param key_file: Path to a client key. Must be used with ``cert_file``.
    :param view_url_template: Optional registry-specific URL template containing ``{version}``.
    """

    supports_versioning = True

    def __init__(
        self,
        *,
        repository: str,
        tracking_ref: str,
        oras_conn_id: str | None = None,
        plain_http: bool = False,
        insecure: bool = False,
        ca_file: str | None = None,
        cert_file: str | None = None,
        key_file: str | None = None,
        view_url_template: str | None = None,
        **kwargs: Any,
    ) -> None:
        if AIRFLOW_V_3_1_PLUS:
            super().__init__(view_url_template=view_url_template, **kwargs)
        else:
            super().__init__(**kwargs)
            self._view_url_template = view_url_template
        self.repository, self.registry = validate_repository(repository)
        self.tracking_ref = validate_reference(tracking_ref)
        if self.version is not None and not is_digest(self.version):
            raise ValueError("version must be a sha256 or sha512 OCI manifest digest")

        self.oras_conn_id = oras_conn_id
        self.plain_http = plain_http
        self.insecure = insecure
        self.ca_file = ca_file
        self.cert_file = cert_file
        self.key_file = key_file
        self.tracking_path = self.base_dir / "tracking"
        self._previous_tracking_path = self.base_dir / "tracking.previous"
        self._log = log.bind(
            bundle_name=self.name,
            repository=self.repository,
            tracking_ref=self.tracking_ref,
            version=self.version,
            tracking_path=self.tracking_path,
            versions_path=self.versions_dir,
            oras_conn_id=self.oras_conn_id,
        )
        self._log.debug("bundle configured")

    def __repr__(self) -> str:
        return (
            f"<OrasDagBundle("
            f"name={self.name!r}, "
            f"repository={self.repository!r}, "
            f"tracking_ref={self.tracking_ref!r}, "
            f"oras_conn_id={self.oras_conn_id!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def view_url(self, version: str | None = None) -> str | None:
        """Return a registry URL rendered from the user-provided template."""
        if not version:
            return None
        template = self.view_url_template()
        if not template:
            return None
        return template.format(version=version)

    def view_url_template(self) -> str | None:
        """Return the user-provided registry URL template without modification."""
        return self._view_url_template

    @property
    def path(self) -> Path:
        """Return the stable processor path or an immutable worker version path."""
        root = self.versions_dir / self.version if self.version is not None else self.tracking_path
        return root / "content"

    def initialize(self) -> None:
        """Materialize the requested artifact version before Airflow uses it."""
        self._initialize()
        super().initialize()

    def _initialize(self) -> None:
        try:
            with self.lock():
                if self.version is not None:
                    digest = self.version
                    self._materialize_version(digest)
                else:
                    hook = self._get_hook()
                    digest = hook.resolve(self.tracking_ref)
                    self._materialize_tracking(digest, hook=hook)
                self._log.debug("bundle initialized", resolved_version=digest)
        except (AirflowException, OSError, RuntimeError, TypeError, ValueError) as error:
            raise OrasDagBundleError(f"Could not initialize ORAS Dag bundle: {error}") from error

    def get_current_version(self) -> str | BundleVersion:
        """Return the immutable manifest digest represented by this bundle."""
        version = self.version
        if version is None and not self.is_initialized:
            raise RuntimeError("ORAS Dag bundle must be initialized before reading its version")
        if version is None:
            version = self._read_tracking_digest()
        if AIRFLOW_V_3_3_PLUS:
            return BundleVersion(version=version)
        return version

    def refresh(self) -> None:
        """Resolve and atomically expose the latest tracking-reference digest."""
        if self.version is not None:
            raise RuntimeError("Refreshing a specific ORAS Dag bundle version is not supported")
        try:
            with self.lock():
                hook = self._get_hook()
                digest = hook.resolve(self.tracking_ref)
                changed = self._materialize_tracking(digest, hook=hook)
                if not changed:
                    self._log.debug("bundle already current", resolved_version=digest)
                    return
                self._log.debug("bundle refreshed", resolved_version=digest)
        except (AirflowException, OSError, RuntimeError, TypeError, ValueError) as error:
            raise OrasDagBundleError(f"Could not refresh ORAS Dag bundle: {error}") from error

    def _materialize_version(self, digest: str) -> None:
        self._materialize(digest, storage_dir=self.versions_dir)

    def _get_hook(self) -> OrasHook:
        return OrasHook(
            repository=self.repository,
            oras_conn_id=self.oras_conn_id,
            plain_http=self.plain_http,
            insecure=self.insecure,
            ca_file=self.ca_file,
            cert_file=self.cert_file,
            key_file=self.key_file,
        )

    def _materialize(
        self,
        digest: str,
        *,
        storage_dir: Path,
        hook: OrasHook | None = None,
    ) -> None:
        if not is_digest(digest):
            raise ValueError("resolved bundle version is not a valid OCI digest")
        destination = storage_dir / digest
        if destination.is_symlink():
            raise ValueError("bundle version path must not be a symbolic link")
        if destination.exists():
            if not destination.is_dir():
                raise NotADirectoryError("bundle version path is not a directory")
            try:
                self._validate_materialization(destination)
            except (FileNotFoundError, NotADirectoryError, ValueError) as error:
                self._log.warning(
                    "existing immutable materialization cannot be reused; replacing it",
                    exc=error,
                )
                shutil.rmtree(destination)
            else:
                return

        storage_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
        work_path = Path(tempfile.mkdtemp(prefix=f".{digest}.", dir=storage_dir))
        materialized_path = work_path / "materialized"
        content_path = materialized_path / "content"
        try:
            if hook is None:
                hook = self._get_hook()
            materialized_path.mkdir(mode=0o755)
            content_path.mkdir(mode=0o755)
            hook.pull(digest, content_path)
            self._validate_materialization(materialized_path)
            os.replace(materialized_path, destination)
        finally:
            shutil.rmtree(work_path, ignore_errors=True)

    def _materialize_tracking(self, digest: str, *, hook: OrasHook) -> bool:
        """Replace the processor's stable tracking directory with one artifact."""
        self.base_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
        self._recover_interrupted_tracking_replace()
        if self.tracking_path.is_symlink():
            raise ValueError("bundle tracking path must not be a symbolic link")
        if self.tracking_path.exists() and not self.tracking_path.is_dir():
            raise NotADirectoryError("bundle tracking path is not a directory")
        if self._is_tracking_materialization_current(digest):
            return False

        staging_root = Path(tempfile.mkdtemp(prefix=".tracking.", dir=self.base_dir))
        try:
            self._materialize(digest, storage_dir=staging_root, hook=hook)
            staged = staging_root / digest
            self._write_tracking_digest(staged, digest)
            had_tracking = self.tracking_path.exists()
            if had_tracking:
                os.replace(self.tracking_path, self._previous_tracking_path)
            try:
                os.replace(staged, self.tracking_path)
            except OSError:
                if had_tracking and not self.tracking_path.exists():
                    os.replace(self._previous_tracking_path, self.tracking_path)
                raise
            self._remove_previous_tracking_materialization()
        finally:
            shutil.rmtree(staging_root, ignore_errors=True)
        return True

    def _is_tracking_materialization_current(self, digest: str) -> bool:
        if not self.tracking_path.exists():
            return False
        try:
            self._validate_materialization(self.tracking_path)
            current_digest = self._read_tracking_digest()
        except (OSError, ValueError) as error:
            self._log.warning(
                "existing tracking materialization cannot be reused; replacing it",
                exc=error,
            )
            return False
        return current_digest == digest

    def _read_tracking_digest(self) -> str:
        marker = self.tracking_path / _TRACKING_DIGEST_FILENAME
        if marker.is_symlink():
            raise ValueError("bundle tracking digest marker must not be a symbolic link")
        if not marker.exists():
            raise FileNotFoundError("bundle tracking digest marker is missing")
        if not marker.is_file():
            raise ValueError("bundle tracking digest marker is not a regular file")
        try:
            digest = marker.read_text(encoding="ascii")
        except UnicodeError as error:
            raise ValueError("bundle tracking digest marker is not ASCII") from error
        except OSError as error:
            raise OSError("could not read bundle tracking digest marker") from error
        if not is_digest(digest):
            raise ValueError("bundle tracking digest marker is not a valid OCI digest")
        return digest

    @staticmethod
    def _write_tracking_digest(root: Path, digest: str) -> None:
        if not is_digest(digest):
            raise ValueError("bundle tracking digest marker is not a valid OCI digest")
        marker = root / _TRACKING_DIGEST_FILENAME
        descriptor = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(descriptor, "w", encoding="ascii") as marker_file:
            marker_file.write(digest)

    def _recover_interrupted_tracking_replace(self) -> None:
        previous = self._previous_tracking_path
        if previous.is_symlink():
            raise ValueError("previous bundle tracking path must not be a symbolic link")
        if not previous.exists():
            return
        if not previous.is_dir():
            raise NotADirectoryError("previous bundle tracking path is not a directory")
        if self.tracking_path.exists() or self.tracking_path.is_symlink():
            self._remove_previous_tracking_materialization()
        else:
            os.replace(previous, self.tracking_path)

    def _remove_previous_tracking_materialization(self) -> None:
        try:
            shutil.rmtree(self._previous_tracking_path)
        except FileNotFoundError:
            pass
        except OSError as error:
            self._log.warning("could not remove previous bundle", exc=error)

    @staticmethod
    def _validate_materialization(root: Path) -> None:
        content_path = root / "content"
        if content_path.is_symlink():
            raise ValueError("materialized bundle content/ path must not be a symbolic link")
        if not content_path.exists():
            raise FileNotFoundError("materialized bundle does not contain a content/ directory")
        if not content_path.is_dir():
            raise NotADirectoryError("materialized bundle content/ path is not a directory")
