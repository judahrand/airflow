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

"""Validation helpers shared by the ORAS provider components."""

from __future__ import annotations

import ipaddress
import re
from urllib.parse import urlsplit

_DIGEST = re.compile(r"(?:sha256:[0-9a-f]{64}|sha512:[0-9a-f]{128})")
_TAG = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._-]{0,127}")
_HOST_LABEL = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")
_REPOSITORY_COMPONENT = re.compile(r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*")


def is_digest(value: str) -> bool:
    """Return whether *value* is a supported sha256 or sha512 OCI digest."""
    return _DIGEST.fullmatch(value) is not None


def get_reference_digest(value: str) -> str | None:
    """Return the digest from a digest or valid ``tag@digest`` reference."""
    if is_digest(value):
        return value
    tag, separator, digest = value.partition("@")
    if separator and _TAG.fullmatch(tag) is not None and is_digest(digest):
        return digest
    return None


def validate_reference(value: str) -> str:
    """Validate and return an OCI tag, digest, or ``tag@digest`` reference."""
    if get_reference_digest(value) is not None or _TAG.fullmatch(value) is not None:
        return value
    raise ValueError("reference must be an OCI tag, tag@digest, or sha256/sha512 digest")


def normalize_registry_authority(value: str, *, port: int | None = None) -> str:
    """Normalize a registry host and optional port for safe comparison."""
    authority = value.strip().rstrip("/")
    parsed = urlsplit(authority) if "://" in authority else urlsplit(f"//{authority}")
    if parsed.scheme and parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("registry host has an unsupported URL scheme")
    if (
        parsed.username
        or parsed.password
        or not parsed.hostname
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("registry host must contain only a hostname and optional port")
    try:
        parsed_port = parsed.port
    except ValueError as error:
        raise ValueError("registry host has an invalid port") from error
    if port is not None and parsed_port is not None and port != parsed_port:
        raise ValueError("registry host contains a port different from the Connection port")
    effective_port = port if port is not None else parsed_port
    if effective_port is not None and not 1 <= effective_port <= 65535:
        raise ValueError("registry host has an invalid port")
    hostname = parsed.hostname.lower()
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        labels = hostname.split(".")
        if len(hostname) > 253 or not all(_HOST_LABEL.fullmatch(label) for label in labels):
            raise ValueError("registry host has an invalid hostname") from None
    else:
        hostname = address.compressed
    if ":" in hostname:
        hostname = f"[{hostname}]"
    return f"{hostname}:{effective_port}" if effective_port is not None else hostname


def validate_repository(repository: str) -> tuple[str, str]:
    """Validate a tagless repository and return it with its registry authority."""
    value = repository.strip().rstrip("/")
    if not value or value != repository or "://" in value or "@" in value:
        raise ValueError("repository must be a tagless OCI repository without a URL scheme")
    registry, separator, name = value.partition("/")
    if not separator or not name:
        raise ValueError("repository must include an explicit registry and repository name")
    if ":" in name:
        raise ValueError("repository must not include a tag")
    authority = normalize_registry_authority(registry)
    parsed_authority = urlsplit(f"//{authority}")
    authority_hostname = parsed_authority.hostname
    if authority_hostname is None or (
        "." not in authority_hostname
        and ":" not in authority_hostname
        and parsed_authority.port is None
        and authority_hostname != "localhost"
    ):
        raise ValueError("repository registry must be localhost or contain a dot or port")
    if len(name) > 255 or not all(_REPOSITORY_COMPONENT.fullmatch(part) for part in name.split("/")):
        raise ValueError("repository contains an invalid repository-name component")
    return value, authority
