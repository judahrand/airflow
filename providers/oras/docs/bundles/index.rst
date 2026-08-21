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

ORAS Dag Bundle
===============

Use :class:`~airflow.providers.oras.bundles.oras.OrasDagBundle` in Airflow's
``[dag_processor] dag_bundle_config_list`` to expose the files from an ORAS-pullable OCI artifact
as a versioned Dag bundle.

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
      {
        "name": "published-dags",
        "classpath": "airflow.providers.oras.bundles.oras.OrasDagBundle",
        "kwargs": {
          "repository": "registry.example.com/airflow/dags",
          "tracking_ref": "production",
          "oras_conn_id": "oras_default",
          "refresh_interval": 300,
          "ca_file": "/etc/airflow/registry-ca.pem",
          "view_url_template": "https://registry.example.com/ui/artifacts/{version}"
        }
      }
    ]'

Repository and Tracking Reference
---------------------------------

``repository`` is a tagless repository such as ``registry.example.com/airflow/dags``. It must not
contain a scheme, embedded credentials, tag, or digest. ``tracking_ref`` accepts:

* a moving tag such as ``production``;
* an immutable ``sha256:...`` or ``sha512:...`` manifest digest; or
* a pinned ``tag@digest`` reference, where the digest is used directly.

For a tag, the Dag processor resolves the current manifest digest on initialization and refresh.
A digest or ``tag@digest`` Tracking Reference is already pinned, so resolving it does not contact
the registry. Pulling content still requires registry access unless that immutable materialization
is already present locally.

Versioned materialization
-------------------------

The Dag processor reads a stable Tracking Materialization at ``tracking/content/``. A successful
refresh stages the new artifact and atomically replaces that directory, so parsing never observes
a partially pulled artifact. A failed refresh leaves the previous usable content in place.

Workers use the manifest digest recorded for the Dag run. Each Pinned Reference is materialized at
``versions/<digest>/content/`` and is never refreshed in place, allowing Dag runs assigned to
different versions to execute concurrently. The cache is local to each component; shared storage
is not required.

Transport Configuration
-----------------------

Network and TLS behavior is configured explicitly on the bundle:

* ``plain_http`` uses unencrypted HTTP.
* ``insecure`` disables TLS certificate verification.
* ``ca_file`` selects a custom certificate authority bundle.
* ``cert_file`` and ``key_file`` configure mutual TLS and must be supplied together.

``plain_http`` and ``insecure`` are mutually exclusive. These settings are not read from the ORAS
Connection. See :doc:`../security` before weakening the transport defaults.

Artifact contents and media types
---------------------------------

The provider accepts any artifact that ``oras pull`` can materialize. It does not impose a media
type or an archive/filesystem layout. Publishers may use a custom artifact type such as
``application/vnd.apache.airflow.dag-bundle.v1`` as an organizational convention, but the provider
does not require or validate it. Publish files so their pulled paths form the intended Dag bundle.

Artifact view links
-------------------

Registry web interfaces do not share a portable artifact URL. Set ``view_url_template`` only when
the registry has a suitable URL, using ``{version}`` for the immutable digest. When the option is
omitted, Airflow does not synthesize a view URL.
