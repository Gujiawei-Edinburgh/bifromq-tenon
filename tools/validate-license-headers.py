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

"""Check ASF source headers without modifying third-party or machine data files."""

from pathlib import Path
import sys
from source_files import source_files


ROOT = Path(__file__).resolve().parents[1]
SOURCE_SUFFIXES = {
    ".rs", ".java", ".proto", ".c", ".py", ".sh", ".cmd", ".toml",
    ".yml", ".yaml", ".xml", ".groovy", ".properties", ".md", ".service",
}


def validate(path: Path) -> None:
    if path.name in {"LICENSE", "NOTICE", "DISCLAIMER"}:
        return
    suffix = Path(path.stem).suffix if path.suffix == ".liquid" else path.suffix
    # JSON contracts and templates do not permit comments. Lockfiles are generated;
    # PEM fixtures and service/goal entries have no creative source content.
    if suffix in {".json", ".lock", ".pem"}:
        return
    if "META-INF/services" in path.as_posix() or path.name == "goal.txt":
        return
    if suffix not in SOURCE_SUFFIXES and path.name not in {"mvnw", ".gitignore", "Dockerfile"}:
        raise ValueError(f"{path}: classify this file before excluding it from header checks")
    text = path.read_text(encoding="utf-8").lstrip()
    if text.startswith(("#!", "<?xml", "<# : batch portion")):
        text = text.split("\n", 1)[1].lstrip()
    if text.startswith("/*"):
        header = text.split("*/", 1)[0]
    elif text.startswith("<!--"):
        header = text.split("-->", 1)[0]
    else:
        comments = []
        for line in text.splitlines():
            if line.strip() and not line.lstrip().lower().startswith(("#", "@rem", "rem ")):
                break
            comments.append(line)
        header = "\n".join(comments)
    standard = all(part in header for part in (
        "Licensed to the Apache Software Foundation (ASF)",
        "contributor license agreements", "See the NOTICE file",
        "Apache License, Version 2.0", "licenses/LICENSE-2.0",
    ))
    spdx = all(part in header for part in (
        "SPDX-License-Identifier: Apache-2.0",
        "SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership",
        "SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements",
    ))
    if not (standard or spdx):
        raise ValueError(f"{path}: missing ASF source header")


def main() -> int:
    errors = []
    checked = 0
    for name in source_files(ROOT):
        path = ROOT / name
        if not path.exists():
            continue  # A tracked file deleted in the current change.
        try:
            validate(path)
            checked += 1
        except (ValueError, OSError) as error:
            errors.append(str(error))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"License header validation passed: {checked} files, including documented exceptions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
