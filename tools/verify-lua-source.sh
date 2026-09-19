#!/usr/bin/env bash
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

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cd "${repo_root}"

lua_source_dir="$(cargo metadata --locked --format-version 1 | python3 -c '
import json
from pathlib import Path
import sys

metadata = json.load(sys.stdin)
package, = (item for item in metadata["packages"] if item["name"] == "lua-src")
source, = Path(package["manifest_path"]).parent.glob("lua-5.5.*")
print(source)
')"

output_dir="$(mktemp -d "${TMPDIR:-/tmp}/tenon-lua-source.XXXXXX")"
trap 'rm -rf -- "${output_dir}"' EXIT

platform_libraries=(-lm)
case "$(uname -s)" in
  Linux) platform_define=-DLUA_USE_LINUX; platform_libraries+=(-ldl) ;;
  Darwin) platform_define=-DLUA_USE_MACOSX ;;
  *) echo "Lua source verification requires Linux or macOS" >&2; exit 2 ;;
esac

"${CC:-clang}" -std=c99 -O1 -g \
  -fsanitize=address,undefined -fno-omit-frame-pointer \
  -DLUA_COMPAT_GLOBAL=0 "${platform_define}" \
  -I"${lua_source_dir}" tests/lua-source/regressions.c \
  "${lua_source_dir}"/*.c "${platform_libraries[@]}" -o "${output_dir}/regressions"
UBSAN_OPTIONS="${UBSAN_OPTIONS:-halt_on_error=1:print_stacktrace=1}" \
  "${output_dir}/regressions"
