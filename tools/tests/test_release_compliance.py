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

import hashlib
import importlib.util
from pathlib import Path
import tarfile
import tempfile
import unittest
import sys
from unittest.mock import patch


TOOLS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOLS))


def load(name):
    spec = importlib.util.spec_from_file_location(name, TOOLS / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


headers = load("validate-license-headers")
release = load("package-release")
sources = load("source_files")


class LicenseHeadersTest(unittest.TestCase):
    def test_extracted_source_inventory_ignores_build_products(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / "src/lib.rs").write_text("source")
            (root / "src/output").mkdir()
            (root / "src/output/mod.rs").write_text("source")
            (root / "src/target").mkdir()
            (root / "src/target/mod.rs").write_text("source")
            (root / "output").mkdir()
            (root / "output/artifact.txt").write_text("generated")
            (root / "target").mkdir()
            (root / "target/generated.rs").write_text("generated")
            (root / ".flattened-pom.xml").write_text("generated")
            self.assertEqual(
                [Path("src/lib.rs"), Path("src/output/mod.rs"), Path("src/target/mod.rs")],
                sources.source_files(root),
            )

    def test_maven_wrapper_original_headers_are_retained(self):
        for name in ("mvnw", "mvnw.cmd"):
            headers.validate(TOOLS.parent / "sdk/java" / name)

    def test_missing_and_incomplete_headers_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lib.rs"
            for content in ("fn main() {}\n", "// SPDX-License-Identifier: Apache-2.0\n"):
                path.write_text(content)
                with self.assertRaisesRegex(ValueError, "missing ASF"):
                    headers.validate(path)
            path.write_text((TOOLS.parent / "src/lib.rs").read_text())
            headers.validate(path)

    def test_machine_data_is_exempt_but_unknown_source_is_not(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "schema.json"
            path.write_text('{}\n')
            headers.validate(path)
            path = path.with_suffix(".new-language")
            path.write_text("new source\n")
            with self.assertRaisesRegex(ValueError, "classify"):
                headers.validate(path)


class ReleaseArchiveTest(unittest.TestCase):
    def test_source_archive_materializes_internal_links_and_rejects_external_links(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "source"
            root.mkdir()
            for name in ("LICENSE", "NOTICE", "DISCLAIMER"):
                (root / name).write_text(name)
            (root / "contract.json").write_text('{}\n')
            (root / "linked.json").symlink_to("contract.json")
            with patch.object(release, "ROOT", root):
                self.assertEqual((b'{}\n', False), release.source_files()["linked.json"])
                (Path(directory) / "private.json").write_text('{}\n')
                (root / "external.json").symlink_to("../private.json")
                with self.assertRaisesRegex(ValueError, "inside the source tree"):
                    release.source_files()

    def test_archive_preserves_bytes_modes_and_has_a_verifiable_checksum(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            files = {"LICENSE": (b"terms\n", False), "bin/tenon": (b"program", True)}
            path = release.archive(output, "test-incubating", files)
            first = path.read_bytes()
            with tarfile.open(path) as archive:
                self.assertEqual(b"terms\n", archive.extractfile("test-incubating/LICENSE").read())
                self.assertEqual(0o755, archive.getmember("test-incubating/bin/tenon").mode)
                self.assertTrue(all(member.uid == 0 and member.mtime == 0 for member in archive))
            digest, name = path.with_name(path.name + ".sha512").read_text().split()
            self.assertEqual(hashlib.sha512(first).hexdigest(), digest)
            self.assertEqual(path.name, name)
            release.archive(output, "test-incubating", dict(reversed(list(files.items()))))
            self.assertEqual(first, path.read_bytes())


if __name__ == "__main__":
    unittest.main()
