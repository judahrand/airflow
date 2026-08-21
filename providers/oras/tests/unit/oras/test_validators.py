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

import pytest

from airflow.providers.oras.validators import (
    get_reference_digest,
    is_digest,
    normalize_registry_authority,
    validate_reference,
    validate_repository,
)

SHA256_DIGEST = "sha256:" + "a" * 64
SHA512_DIGEST = "sha512:" + "b" * 128


@pytest.mark.parametrize("digest", [SHA256_DIGEST, SHA512_DIGEST])
def test_is_digest_accepts_supported_digests(digest: str) -> None:
    assert is_digest(digest)


@pytest.mark.parametrize(
    "digest",
    [
        "",
        "sha256:" + "a" * 63,
        "sha256:" + "A" * 64,
        "sha384:" + "a" * 96,
        "sha512:" + "b" * 127,
        f"{SHA256_DIGEST} ",
    ],
)
def test_is_digest_rejects_unsupported_or_malformed_digests(digest: str) -> None:
    assert not is_digest(digest)


@pytest.mark.parametrize("digest", [SHA256_DIGEST, SHA512_DIGEST])
def test_get_reference_digest_extracts_supported_digest(digest: str) -> None:
    assert get_reference_digest(digest) == digest
    assert get_reference_digest(f"release@{digest}") == digest


@pytest.mark.parametrize(
    "reference",
    [
        "latest",
        "v1.2_3-rc.1",
        SHA256_DIGEST,
        SHA512_DIGEST,
        f"release@{SHA256_DIGEST}",
        f"release@{SHA512_DIGEST}",
    ],
)
def test_validate_reference_returns_valid_reference(reference: str) -> None:
    assert validate_reference(reference) == reference


@pytest.mark.parametrize(
    "reference",
    [
        "",
        "bad:tag",
        "a" * 129,
        "sha256:bad",
        f"bad:tag@{SHA256_DIGEST}",
        f"@{SHA256_DIGEST}",
        "latest@",
        "latest@sha512:bad",
        f"latest@{SHA256_DIGEST}@{SHA256_DIGEST}",
    ],
)
def test_validate_reference_rejects_malformed_reference(reference: str) -> None:
    assert get_reference_digest(reference) is None
    with pytest.raises(ValueError, match="reference"):
        validate_reference(reference)


@pytest.mark.parametrize(
    ("value", "port", "expected"),
    [
        ("REGISTRY.EXAMPLE.COM/", None, "registry.example.com"),
        ("https://Registry.Example.Com/", None, "registry.example.com"),
        ("registry.example.com:5000", None, "registry.example.com:5000"),
        ("registry.example.com", 5000, "registry.example.com:5000"),
        ("127.0.0.1", 5000, "127.0.0.1:5000"),
        ("[2001:0db8::1]", None, "[2001:db8::1]"),
    ],
)
def test_normalize_registry_authority(
    value: str,
    port: int | None,
    expected: str,
) -> None:
    assert normalize_registry_authority(value, port=port) == expected


@pytest.mark.parametrize(
    ("value", "port", "message"),
    [
        ("", None, "hostname"),
        ("ftp://registry.example.com", None, "unsupported URL scheme"),
        ("user@registry.example.com", None, "only a hostname"),
        ("registry.example.com/path", None, "only a hostname"),
        ("registry.example.com?query", None, "only a hostname"),
        ("registry_example.com", None, "invalid hostname"),
        ("registry.example.com:bad", None, "invalid port"),
        ("registry.example.com:5000", 5001, "different from the Connection port"),
        ("registry.example.com", 0, "invalid port"),
        ("registry.example.com", 65536, "invalid port"),
    ],
)
def test_normalize_registry_authority_rejects_invalid_authority(
    value: str,
    port: int | None,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        normalize_registry_authority(value, port=port)


@pytest.mark.parametrize(
    ("repository", "expected_authority"),
    [
        ("registry.example.com/airflow/dags", "registry.example.com"),
        ("localhost:5000/airflow/dags", "localhost:5000"),
        ("[2001:db8::1]:5000/airflow/dags", "[2001:db8::1]:5000"),
        ("registry.example.com/team_name/dag-bundle.v1", "registry.example.com"),
    ],
)
def test_validate_repository_returns_repository_and_authority(
    repository: str,
    expected_authority: str,
) -> None:
    assert validate_repository(repository) == (repository, expected_authority)


@pytest.mark.parametrize(
    ("repository", "message"),
    [
        ("", "tagless OCI repository"),
        (" registry.example.com/airflow/dags", "tagless OCI repository"),
        ("registry.example.com/airflow/dags/", "tagless OCI repository"),
        ("https://registry.example.com/airflow/dags", "tagless OCI repository"),
        (f"registry.example.com/airflow/dags@{SHA256_DIGEST}", "tagless OCI repository"),
        ("registry.example.com", "explicit registry"),
        ("registry.example.com/airflow/dags:latest", "must not include a tag"),
        ("dags/airflow", "localhost or contain a dot or port"),
        ("registry.example.com/Airflow/dags", "invalid repository-name component"),
        ("registry.example.com/airflow//dags", "invalid repository-name component"),
    ],
)
def test_validate_repository_rejects_invalid_repository(
    repository: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        validate_repository(repository)
