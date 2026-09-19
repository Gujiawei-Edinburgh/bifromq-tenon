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
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'validate-rust-test-layout.py'
spec = importlib.util.spec_from_file_location('test_layout', SCRIPT)
layout = importlib.util.module_from_spec(spec)
spec.loader.exec_module(layout)


class TestModuleBoundary(unittest.TestCase):
    def validate(self, source):
        return layout.validate_source(source, Path('src/example.rs'))

    def test_unconditional_test_modules_are_rejected(self):
        for declaration in ['mod tests;', 'pub mod runner_test_support;',
                            'pub(crate) mod contract_test_support {}']:
            with self.subTest(declaration=declaration):
                self.assertTrue(self.validate(declaration))

    def test_non_test_conditions_do_not_hide_test_modules(self):
        for condition in ['not(test)', 'unix', 'any(test, unix)']:
            with self.subTest(condition=condition):
                self.assertTrue(self.validate(f'#[cfg({condition})]\nmod tests;'))

    def test_explicit_test_gates_are_accepted(self):
        for condition in ['test', 'all(test, not(feature = "loom-model"))',
                          'feature = "repository-test-support"',
                          'any(test, feature = "repository-test-support")']:
            with self.subTest(condition=condition):
                self.assertEqual([], self.validate(f'#[cfg({condition})]\nmod tests;'))

    def test_path_attribute_and_comments_preserve_gate(self):
        self.assertEqual([], self.validate(
            '#[cfg(test)]\n// Shared fixture.\n#[path = "fixture.rs"]\nmod tests;'))

    def test_nested_test_injection_remains_rejected(self):
        self.assertTrue(self.validate('impl Owner {\n    #[cfg(test)]\n    fn new_test() {}\n}'))

    def test_production_modules_are_unaffected(self):
        self.assertEqual([], self.validate('mod pipeline;\npub(crate) mod runner;'))


if __name__ == '__main__':
    unittest.main()
