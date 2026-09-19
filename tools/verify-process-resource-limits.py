#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Run real Linux limits tests inside a systemd-delegated test service."""

import os
from pathlib import Path
import subprocess
import sys


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("Usage: verify-process-resource-limits.py TEST_BINARY RUNNER_BINARY")
    membership = next(
        line.removeprefix("0::")
        for line in Path("/proc/self/cgroup").read_text().splitlines()
        if line.startswith("0::")
    )
    # This deployment fixture uses the host's standard cgroup2 mount. The
    # production backend separately supports mount roots and namespaces.
    current = Path("/sys/fs/cgroup") / membership.lstrip("/")
    scope = current
    manager = scope / "test.manager"
    manager.mkdir()
    # The fixture owns this disposable scope. Move its driver and optional
    # container init out before delegating independent ranges to real Runners.
    members = (scope / "cgroup.procs").read_text().splitlines()
    allowed = {str(os.getpid())}
    if os.getppid() == 1 and Path("/.dockerenv").exists():
        allowed.add("1")
    if not set(members) <= allowed:
        raise SystemExit(f"The test scope contains unrelated processes: {members}")
    for pid in members:
        (manager / "cgroup.procs").write_text(pid)
    (scope / "cgroup.subtree_control").write_text("+cpu +memory")
    root = scope / "tenon.tests"
    root.mkdir()
    (root / "cgroup.subtree_control").write_text("+cpu +memory")
    # Restrict only this disposable test subtree, not the deployment swap policy.
    (root / "memory.swap.max").write_text("0")
    environment = dict(os.environ, TENON_TEST_CGROUP_ROOT=str(root),
                       TENON_TEST_RUNNER_BINARY=str(Path(sys.argv[2]).resolve()))
    try:
        subprocess.run([str(Path(sys.argv[1]).resolve()), "--include-ignored",
                        "--skip", "linux::scope::export_deployment_fixture",
                        "--test-threads=1", "--nocapture"], env=environment, check=True)
    finally:
        # A leftover child group is a failed cleanup assertion. systemd owns
        # final service teardown even when this fixture exits with an error.
        root.rmdir()
        # The service/container manager removes our occupied test.manager group.


if __name__ == "__main__":
    main()
