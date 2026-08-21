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

"""Airflow hook for the ORAS command-line client."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from airflow.providers.common.compat.sdk import BaseHook, redact
from airflow.providers.oras.validators import (
    get_reference_digest,
    is_digest,
    normalize_registry_authority,
    validate_reference,
    validate_repository,
)


class OrasHook(BaseHook):
    """
    Run ORAS CLI operations against a configured OCI repository.

    :param repository: Tagless OCI repository including its registry hostname.
    :param oras_conn_id: Optional Airflow Connection containing registry credentials.
        Pass ``None`` for anonymous access.
    :param plain_http: Use plain HTTP for registry communication.
    :param insecure: Skip TLS certificate verification.
    :param ca_file: Path to a custom certificate authority bundle.
    :param cert_file: Path to a client certificate. Must be used with ``key_file``.
    :param key_file: Path to a client key. Must be used with ``cert_file``.
    """

    conn_name_attr = "oras_conn_id"
    default_conn_name = "oras_default"
    conn_type = "oras"
    hook_name = "ORAS"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Describe the ORAS Connection fields shown by Airflow."""
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {
                "host": "Registry Authority",
                "login": "Username",
                "password": "Password or identity token",
            },
            "placeholders": {
                "host": "ghcr.io",
                "port": "443",
                "login": "registry-user",
                "password": "registry password or identity token",
            },
        }

    def __init__(
        self,
        *,
        repository: str,
        oras_conn_id: str | None = default_conn_name,
        plain_http: bool = False,
        insecure: bool = False,
        ca_file: str | None = None,
        cert_file: str | None = None,
        key_file: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.oras_conn_id = oras_conn_id
        self.repository, self.registry = validate_repository(repository)

        self.username: str | None = None
        self.password: str | None = None
        self.plain_http = self._validate_bool(plain_http, "plain_http")
        self.insecure = self._validate_bool(insecure, "insecure")
        if self.plain_http and self.insecure:
            raise ValueError("plain_http and insecure are mutually exclusive")
        self.ca_file = self._validate_path(ca_file, "ca_file")
        self.cert_file = self._validate_path(cert_file, "cert_file")
        self.key_file = self._validate_path(key_file, "key_file")
        if (self.cert_file is None) != (self.key_file is None):
            raise ValueError("cert_file and key_file must be provided together")

        if oras_conn_id is not None:
            self._configure_connection(oras_conn_id)

    def resolve(self, reference: str) -> str:
        """Resolve a tag, digest, or ``tag@digest`` reference to an immutable digest."""
        reference = validate_reference(reference)
        digest = get_reference_digest(reference)
        if digest is not None:
            return digest

        output = self._run_oras(
            ["manifest", "fetch", "--descriptor"],
            self._build_target(reference),
            operation="resolve OCI artifact",
        )
        try:
            descriptor = json.loads(output)
            digest = descriptor["digest"]
        except (KeyError, TypeError, json.JSONDecodeError) as error:
            raise ValueError("ORAS returned an invalid manifest descriptor") from error
        if not isinstance(digest, str) or not is_digest(digest):
            raise ValueError("ORAS returned an invalid manifest digest")
        return digest

    def pull(self, reference: str, output_dir: Path) -> None:
        """Pull an OCI artifact into *output_dir* using the ORAS CLI."""
        reference = validate_reference(reference)
        self._run_oras(
            ["pull", "--no-tty", "--output", os.fspath(output_dir)],
            self._build_target(reference),
            operation="pull OCI artifact",
        )

    def _configure_connection(self, oras_conn_id: str) -> None:
        connection = self.get_connection(oras_conn_id)
        if not connection.host:
            raise ValueError("ORAS Connection host must identify the registry")
        connection_registry = normalize_registry_authority(
            connection.host,
            port=connection.port,
        )
        if connection_registry != self.registry:
            raise ValueError("ORAS Connection host does not match the repository registry")

        self.username = connection.login
        self.password = connection.password
        if self.username and not self.password:
            raise ValueError("ORAS Connection must provide a password when login is configured")

        extras = connection.extra_dejson or {}
        if extras:
            raise ValueError("ORAS Connection must not define extras")

    @staticmethod
    def _validate_bool(value: bool, name: str) -> bool:
        if not isinstance(value, bool):
            raise TypeError(f"{name} must be a boolean")
        return value

    @staticmethod
    def _validate_path(value: str | None, name: str) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a path string")
        if not value:
            raise ValueError(f"{name} must be a non-empty path")
        return value

    def _build_target(self, reference: str) -> str:
        separator = "@" if is_digest(reference) else ":"
        return f"{self.repository}{separator}{reference}"

    def _run_oras(self, args: list[str], target: str, *, operation: str) -> str:
        with tempfile.TemporaryDirectory(prefix="airflow-oras-") as temporary_directory:
            registry_config = Path(temporary_directory, "config.json")
            descriptor = os.open(registry_config, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as config_file:
                json.dump({"auths": {}}, config_file)

            command = [
                "oras",
                *args,
                *self._build_connection_args(),
                "--registry-config",
                str(registry_config),
            ]
            command.append(target)
            self.log.debug("Running ORAS command: %s", command)
            try:
                result = subprocess.run(
                    command,
                    input=f"{self.password}\n" if self.password is not None else None,
                    text=True,
                    capture_output=True,
                    check=False,
                )
            except FileNotFoundError as error:
                raise FileNotFoundError(
                    "ORAS CLI executable was not found; install oras 1.3.2 or newer"
                ) from error
            except OSError as error:
                raise OSError(f"Could not {operation} with the ORAS CLI") from error

            if result.returncode != 0:
                detail = self._sanitize_cli_error(result.stderr)
                message = f"ORAS CLI failed to {operation} (exit code {result.returncode})"
                if detail:
                    message = f"{message}: {detail}"
                raise RuntimeError(message)
            return result.stdout

    def _build_connection_args(self) -> list[str]:
        args: list[str] = []
        if self.username is not None:
            args.extend(["--username", self.username, "--password-stdin"])
        elif self.password is not None:
            args.append("--identity-token-stdin")
        if self.plain_http:
            args.append("--plain-http")
        if self.insecure:
            args.append("--insecure")
        if self.ca_file is not None:
            args.extend(["--ca-file", self.ca_file])
        if self.cert_file is not None and self.key_file is not None:
            args.extend(["--cert-file", self.cert_file, "--key-file", self.key_file])
        return args

    def _sanitize_cli_error(self, stderr: str) -> str:
        detail = " ".join(stderr.split())
        if self.password:
            detail = detail.replace(self.password, "***")
        return str(redact(detail))[:1000]
