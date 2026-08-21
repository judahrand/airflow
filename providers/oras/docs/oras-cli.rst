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

ORAS CLI prerequisite
=====================

The provider invokes an external `ORAS CLI <https://oras.land/docs/>`__ to resolve and pull
artifacts. Install ORAS 1.3.2 or newer on every Dag processor and worker that can initialize this
bundle, and make the ``oras`` executable available on that component's ``PATH``.

The Python package does not contain, download, or update the executable at runtime. The Deployment
Manager owns the installation, version pin, release-checksum verification, patching, and rollout of
the ORAS CLI. Follow the `official ORAS installation documentation
<https://oras.land/docs/installation/>`__ for the target platform.

Verify each runtime image or host before deploying:

.. code-block:: console

    $ oras version
    Version:        1.3.2

The command output must report version 1.3.2 or newer. If the executable is unavailable, the
provider raises an actionable error instead of downloading a replacement.
