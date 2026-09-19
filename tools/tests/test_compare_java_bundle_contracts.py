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

"""Exercise missing, duplicate and divergent native bundle evidence."""

import contextlib
import io
import json
from pathlib import Path
import runpy
import tempfile
import unittest


compare = runpy.run_path(
    str(Path(__file__).resolve().parents[1] / "compare-java-bundle-contracts.py")
)["compare"]
PLATFORMS = {"linux-amd64", "linux-arm64", "macos-amd64", "macos-arm64"}


class JavaBundleEvidenceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.paths = []
        for platform in sorted(PLATFORMS):
            path = Path(self.directory.name) / f"{platform}.json"
            path.write_text(json.dumps({
                "platform": platform,
                "externalJvm": {"source": "same-source", "replaySink": "same-sink"},
                "contracts": {interface: {"programName": interface, "sha256": "same-bytes"}
                              for interface in ("source", "sink", "source-and-sink")},
            }), encoding="utf-8")
            self.paths.append(path)

    def test_all_native_platforms_match(self) -> None:
        with contextlib.redirect_stdout(io.StringIO()) as output:
            compare(self.paths, PLATFORMS)
        self.assertIn("match across", output.getvalue())

    def test_explicit_two_platform_scope_matches(self) -> None:
        paths = [path for path in self.paths if "arm64" in path.name]
        with contextlib.redirect_stdout(io.StringIO()):
            compare(paths, {"linux-arm64", "macos-arm64"})

    def test_missing_platform_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "Expected exactly"):
            compare(self.paths[:-1], PLATFORMS)

    def test_duplicate_platform_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "Expected exactly"):
            compare(self.paths + self.paths[:1], PLATFORMS)

    def test_changed_bundle_material_is_rejected(self) -> None:
        path = self.paths[-1]
        report = json.loads(path.read_text(encoding="utf-8"))
        report["contracts"]["sink"]["sha256"] = "different-bytes"
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "material differs"):
            compare(self.paths, PLATFORMS)

    def test_changed_external_jvm_package_is_rejected(self) -> None:
        path = self.paths[-1]
        report = json.loads(path.read_text(encoding="utf-8"))
        report["externalJvm"]["source"] = "different-bytes"
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "External JVM package bytes differ"):
            compare(self.paths, PLATFORMS)

    def test_missing_external_jvm_package_is_rejected(self) -> None:
        for path in self.paths:
            report = json.loads(path.read_text(encoding="utf-8"))
            del report["externalJvm"]["source"]
            path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "both external JVM packages"):
            compare(self.paths, PLATFORMS)

    def test_missing_interface_is_rejected(self) -> None:
        for path in self.paths:
            report = json.loads(path.read_text(encoding="utf-8"))
            del report["contracts"]["sink"]
            path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "all three Plugin interfaces"):
            compare(self.paths, PLATFORMS)


if __name__ == "__main__":
    unittest.main()
