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
or an OCI image index containing multiple artifacts as a versioned Dag bundle.

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

Compose a bundle from multiple artifacts
----------------------------------------

An OCI image index can combine independently published artifacts into one Dag bundle. For example,
publish each Dag and its supporting Python modules as a separate artifact, then create an index
referencing those artifacts. The provider pulls the contents of all referenced artifacts into the
bundle directory. Use distinct paths for each artifact so their files do not overlap.

This lets you update one Dag without rebuilding or republishing the other artifacts in the bundle.
Only the changed artifact and a new index need to be published. All artifacts referenced by the
index must be in the same registry repository.

For example, start with two local directories, ``orders/`` and ``customers/``, each containing its
Dag files and supporting modules. Publish both directories, then create an index tagged
``production``:

.. code-block:: bash

    repository=registry.example.com/airflow/dags
    artifact_type=application/vnd.apache.airflow.dag-bundle.v1

    oras push --artifact-type "$artifact_type" "$repository:orders-v1" orders
    oras push --artifact-type "$artifact_type" "$repository:customers-v1" customers
    oras manifest index create "$repository:production" orders-v1 customers-v1

With ``repository`` and ``tracking_ref="production"`` configured as above, Airflow exposes both
directories in the same bundle. After changing files in ``orders/``, publish the updated artifact
and replace the index, reusing the existing ``customers-v1`` artifact:

.. code-block:: bash

    oras push --artifact-type "$artifact_type" "$repository:orders-v2" orders
    oras manifest index create "$repository:production" orders-v2 customers-v1

The index records the child manifest digests when it is created. Moving a child artifact's tag
alone does not change an existing index; publish a new index to change the bundle contents.
Airflow uses the **index digest** as the bundle version, so Dag runs pinned to the previous index
continue to use its original artifacts. Retain previous indexes and their referenced artifacts in
the registry for as long as those bundle versions may be needed.

On refresh, Airflow stages and replaces the complete bundle directory. Independent artifact
publishing does not make the provider's local materialization incremental.

See the ORAS documentation for `creating an index
<https://oras.land/docs/commands/oras_manifest_index_create/>`__ and `updating an index
<https://oras.land/docs/commands/oras_manifest_index_update/>`__.

Artifact view links
-------------------

Registry web interfaces do not share a portable artifact URL. Set ``view_url_template`` only when
the registry has a suitable URL, using ``{version}`` for the immutable digest. When the option is
omitted, Airflow does not synthesize a view URL.
