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

"""Compare platform-independent bytes from successful native Java black-box runs."""

import argparse
import json
from pathlib import Path


def compare(paths: list[Path], expected_platforms: set[str]) -> None:
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    platforms = [report["platform"] for report in reports]
    if len(platforms) != len(set(platforms)) or set(platforms) != expected_platforms:
        raise ValueError(f"Expected exactly {sorted(expected_platforms)}, got {platforms}")
    baseline = reports[0]["contracts"]
    if set(baseline) != {"source", "sink", "source-and-sink"}:
        raise ValueError("Native report does not contain all three Plugin interfaces")
    external = reports[0]["externalJvm"]
    if set(external) != {"source", "replaySink"}:
        raise ValueError("Native report does not contain both external JVM packages")
    for report in reports[1:]:
        if report["externalJvm"] != external:
            raise ValueError(f"External JVM package bytes differ on {report['platform']}")
        if report["contracts"] != baseline:
            raise ValueError(
                f"Platform-independent bundle material differs: {platforms[0]} vs {report['platform']}"
            )
    print(f"Java bundle contracts match across {', '.join(sorted(platforms))}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path, nargs="+")
    parser.add_argument("--platforms", nargs="+", default=[
        "linux-amd64", "linux-arm64", "macos-amd64", "macos-arm64"
    ])
    arguments = parser.parse_args()
    compare(arguments.reports, set(arguments.platforms))


if __name__ == "__main__":
    main()
