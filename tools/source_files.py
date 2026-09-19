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

"""Enumerate public sources in a checkout or an extracted source release."""

import os
from pathlib import Path
import subprocess


def source_files(root: Path):
    if (root / ".git").exists():
        names = subprocess.check_output(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root
        ).decode().split("\0")
        return sorted({Path(name) for name in names if name and (
            (root / name).is_file() or (root / name).is_symlink()
        )})
    # Source archives have no Git metadata. Exclude the build outputs from .gitignore
    # so validation also works after compiling an extracted release.
    paths = []
    for directory, directories, files in os.walk(root):
        parent = Path(directory)
        build_root = parent == root or (parent / "Cargo.toml").is_file() or (parent / "pom.xml").is_file()
        directories[:] = [name for name in directories if name not in {
            ".git", ".idea", "__pycache__",
        } and not (build_root and name == "target")
          and not (parent == root and name == "output")]
        for name in files:
            if name not in {".DS_Store", ".flattened-pom.xml", "dependency-reduced-pom.xml"}:
                paths.append((Path(directory) / name).relative_to(root))
    return sorted(paths)
