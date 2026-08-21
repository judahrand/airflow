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

Security
========

Trusting executable Dag content
-------------------------------

Files pulled by this provider become executable Dag content. The configured registry repository and
its Trusted Publishers are therefore part of the Airflow deployment's code trust boundary. Restrict
write and tag-mutation permissions, protect publishing credentials, review publisher pipelines, and
prefer immutable digest promotion when policy requires it.

The initial provider does **not** verify artifact signatures or attestations. Registry
authentication, TLS, access controls, and digest-addressed retrieval do not establish who published
an artifact. Deployment Managers that require cryptographic provenance must enforce verification in
the publishing or promotion workflow before an artifact reference becomes available to Airflow.

Credentials and subprocess isolation
------------------------------------

An ORAS Connection is bound to one normalized Registry Authority. The provider refuses to send its
credentials when that authority differs from the repository authority, rejects Connection extras,
and fails closed when a configured Connection cannot be loaded or validated.

Passwords and identity tokens are passed to ORAS over standard input. Each invocation uses a private
temporary ORAS registry configuration file with mode ``0600``; it does not reuse a user's global
ORAS login configuration. The provider invokes the executable without a shell and redacts known
secrets from bounded error detail. Operators should still restrict process tracing, crash dumps, and
access to task and component logs according to their deployment's credential policy.

Transport Configuration
-----------------------

TLS certificate verification is enabled by default. Configure a private registry CA with
``ca_file``; configure mutual TLS with ``cert_file`` and ``key_file``, and limit filesystem
access to those files.

.. warning::

    ``insecure=True`` disables server certificate verification and permits man-in-the-middle
    attacks. ``plain_http=True`` sends registry traffic without TLS. Use either option only for an
    explicitly isolated development environment. They are mutually exclusive.

Filesystem protections and limits
---------------------------------

The bundle pulls into a staging directory and publishes Tracking Materializations by atomic rename.
It validates expected directories and digest markers, refuses symbolic links at security-sensitive
paths, and does not replace a usable tracking directory until the new artifact is complete.

These checks do not isolate the bundle from another local principal that can modify its storage
directory. Give Dag processors and workers dedicated identities, limit write access to their bundle
storage, avoid sharing that storage with untrusted workloads, and use the usual Airflow component
isolation controls.

.. include:: /../../../devel-common/src/sphinx_exts/includes/security.rst
