<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Container deployment

Use Linux 5.14 or newer with cgroup v2 and the CPU and memory controllers available
from the container's parent. Each Runner needs its own container and state volume.
The container runtime must provide a private cgroup namespace; mounting the host's
cgroup hierarchy into the container is not a supported substitute.

## Docker

Build from an extracted **Linux Runner binary distribution** for the container's
architecture. Keep its legal files together with its binary.
From the repository root:

```sh
docker build -f deploy/container/Dockerfile -t tenon:local /path/to/extracted-runner
docker compose -f deploy/container/compose.yaml up -d
```

The supplied Compose configuration grants `SYS_ADMIN` so Tenon can make the
container's private cgroup mount writable. This broad permission is suitable only for
the [trusted native workloads](../../SECURITY.md) supported by Tenon, and is
inherited by child programs. This is not a security sandbox.

`init: true` supplies Docker's small init for signal forwarding and orphan reaping.
Tenon handles both this layout and direct execution as PID 1. With init enabled,
the entrypoint must execute Tenon directly: a wrapper between init and Runner,
extra background processes or a full service manager needs a separately delegated
Runner scope. Disabling `init` also makes the Runner PID 1;
use an init when plugins may leave orphaned descendants.

The management port is published only on host loopback. Configure TLS and access
control before exposing it elsewhere. The state volume holds installed plugins
and Documents. Adjust `stop_grace_period` if the Runner's configured shutdown
deadlines need longer. Container-level limits remain upper bounds over the whole
container, including the Runner and init.

Keep the runtime's normal seccomp and LSM policies initially. A host policy can
still deny `mount_setattr` or cgroup writes even with `SYS_ADMIN`; Tenon reports
that failure. Adapt only the relevant host policy or use a runtime-provided
writable delegation. Do not solve it by mounting the host hierarchy or disabling
all security profiles. Rootless Docker and arbitrary Kubernetes Pod configurations
are not guaranteed to supply this delegation.

## Podman

Podman's systemd mode can supply a writable private cgroup mount without adding
`SYS_ADMIN`. It does not require running systemd inside the container:

```sh
podman volume create tenon-state
podman run --name tenon --init --systemd=always --cgroupns=private \
  --stop-signal=SIGTERM --stop-timeout=90 \
  -p 127.0.0.1:8080:8080 \
  -v "$PWD/deploy/container/runner.json:/etc/tenon/runner.json:ro" \
  -v tenon-state:/var/lib/tenon tenon:local
```

The parent must delegate the requested controllers, including for rootless
operation. SELinux deployments must also permit container cgroup management and
label the configuration volume appropriately. Use the enforcement check below to
confirm that the host configuration allows Document limits.

## Confirm enforcement

Install a plugin and apply a Document with `resourceLimits`. Wait for Pipeline
detail to show matching `documentEtag` and `appliedDocumentEtag` values, a running
plugin and
`resourceLimits.state: "enforced"`; see the [HTTP API](../../guide/http-api.md).

`runner.resource_limits_unavailable` means the Runner could not obtain writable
delegation. Unlimited Documents can still run; a limited Document reports
`resource_limits_apply_failed` and is never silently started without its limits.
A competing Runner or ambiguous scope prevents Runner startup.
