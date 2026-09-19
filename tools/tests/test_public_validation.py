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

import importlib.util
from pathlib import Path
import tempfile
import unittest
import sys
from unittest.mock import patch


TOOLS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOLS))


def load(name):
    spec = importlib.util.spec_from_file_location(name, TOOLS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


docs = load("validate-public-docs")
contracts = load("validate-contracts")


class PublicDocumentationTest(unittest.TestCase):
    def test_local_links_and_external_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "source"
            root.mkdir()
            source = root / "README.md"
            target = root / "with space.md"
            target.write_text("# Details\n")
            source.write_text("[Read](with%20space.md?view=1#details)\n")
            docs.validate_file(root, source)
            for body in ("![Image](missing.png)\n", "[Read](missing.md)\n", "[Read](with%20space.md#missing)\n"):
                source.write_text(body)
                with self.assertRaises(ValueError):
                    docs.validate_file(root, source)
            (parent / "outside.md").write_text("Outside\n")
            (root / "escape.md").symlink_to(parent / "outside.md")
            for target in ("../outside.md", "escape.md", "file:///outside/private.md", "file:../../outside.md"):
                source.write_text(f"[Read]({target})\n")
                with self.assertRaises(ValueError):
                    docs.validate_file(root, source)

    def test_contract_and_tool_text_format(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("contract.json", "contract.proto", "check.py"):
                path = root / name
                for content in (b'{}', b'{} \n', b'\xef\xbb\xbf{}\n', b'\xff\n'):
                    path.write_bytes(content)
                    with self.assertRaises(ValueError):
                        docs.validate_file(root, path)


class ContractValidationTest(unittest.TestCase):
    def test_duplicate_keys_and_external_schema_reference(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "contract.json"
            path.write_text('{"field": 1, "field": 2}')
            with patch.object(contracts, "ROOT", root), self.assertRaises(contracts.ValidationFailure):
                contracts.load_json(path)
        with self.assertRaises(contracts.ValidationFailure):
            contracts.validate_internal_schema_refs("Schema", {"$ref": "other.json"})

    def test_description_property_is_not_schema_annotation(self):
        contracts.validate_schema_explanatory_text("Schema", {
            "properties": {"description": {"type": "string", "description": "Plugin text."}}
        })
        for annotation in ("", 42, "描述"):
            with self.assertRaises(contracts.ValidationFailure):
                contracts.validate_schema_explanatory_text("Schema", {
                    "properties": {"description": {"description": annotation}}
                })

    def test_multiple_constraints_may_reject_the_same_field(self):
        from jsonschema import Draft202012Validator
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "vectors.json"
            path.write_text('{"valid":[{"name":"valid","value":{"name":"A"}}],"invalid":[{"name":"empty","value":{"name":""},"expectedInstancePointer":"/name"}]}')
            schema = {"properties": {"name": {"minLength": 1, "pattern": "[A-Z]"}}}
            contracts.validate_schema_vectors("Schema", schema, path, Draft202012Validator, "value")

    def test_unregistered_schema_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "extra.schema.json").write_text('{}')
            with patch.object(contracts, "ROOT", root), patch.object(contracts, "CONTRACTS", root), patch.object(contracts, "SCHEMA_CASES", ()), self.assertRaises(contracts.ValidationFailure):
                contracts.validate_json_and_schemas()

    def test_wrong_invalid_vector_location_is_rejected(self):
        from jsonschema import Draft202012Validator
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "vectors.json"
            path.write_text('{"valid":[{"name":"valid","value":{"count":1}}],"invalid":[{"name":"bad","value":{"count":"one"},"expectedInstancePointer":"/wrong"}]}')
            schema = {"type": "object", "properties": {"count": {"type": "integer"}}}
            with patch.object(contracts, "ROOT", root), self.assertRaises(contracts.ValidationFailure):
                contracts.validate_schema_vectors("Schema", schema, path, Draft202012Validator, "value")


if __name__ == "__main__":
    unittest.main()
