.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.


.. _howto/connection:oras:

ORAS Connection
===============

The ORAS Connection stores credentials for one OCI Registry Authority. The ORAS Hook uses
``oras_default`` by default. The ORAS Dag Bundle defaults to anonymous access; set its
``oras_conn_id`` argument when authentication is required.

Configuring the Connection
--------------------------

Registry Authority
    The registry hostname, with an optional port in the separate Port field. Examples are
    ``ghcr.io`` and ``registry.example.com`` with port ``5443``. IPv4 and bracketed IPv6
    authorities are supported. Do not include credentials or a repository path.

Username (optional)
    The username for basic authentication. A username requires a password.

Password or identity token (optional)
    With a username, this is the basic-auth password. Without a username, the value is sent to
    ORAS as an identity token.

Extra
    ORAS Connections must not define extras. Transport settings are explicit Hook or Dag bundle
    arguments, and the provider rejects non-empty Connection extras at runtime.

Credential authority binding
----------------------------

The provider normalizes the Connection Registry Authority and the repository Registry Authority
before using credentials. They must match exactly after normalization. A missing or invalid
configured Connection stops the operation; it does not fall back to anonymous access.

Explicit anonymous access
-------------------------

Pass ``oras_conn_id=None`` to :class:`~airflow.providers.oras.hooks.oras.OrasHook` to bypass
Connection lookup. The :class:`~airflow.providers.oras.bundles.oras.OrasDagBundle` uses this
anonymous mode by default.
