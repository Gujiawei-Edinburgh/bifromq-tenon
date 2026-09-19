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

"""Check source documentation encoding, language, local links and resources."""

from html.parser import HTMLParser
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote, urlsplit
from source_files import source_files


ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r'!?\[[^\]\n]*\]\(\s*(<[^>]+>|[^\s)]+)(?:\s+["\'][^\n]*?["\'])?\s*\)')
REFERENCE = re.compile(r'^\s{0,3}\[[^\]]+\]:\s*(<[^>]+>|\S+)', re.MULTILINE)


def prose(text: str) -> str:
    return re.sub(r'^\s*(`{3,}|~{3,}).*?^\s*\1\s*$', '', text, flags=re.MULTILINE | re.DOTALL)


class Resources(HTMLParser):
    def __init__(self):
        super().__init__()
        self.targets = []

    def handle_starttag(self, tag, attrs):
        for key, value in attrs:
            if key in {"src", "href", "poster"} and value:
                self.targets.append(value)


def validate_target(root: Path, source: Path, raw: str) -> None:
    parsed = urlsplit(raw.strip("<>"))
    if parsed.scheme == "file":
        raise ValueError(f"{source}: file URLs are not portable source links: {raw}")
    if parsed.scheme or parsed.netloc:
        return
    target = (source.parent / unquote(parsed.path)).resolve() if parsed.path else source.resolve()
    if not target.is_relative_to(root.resolve()):
        raise ValueError(f"{source}: local link leaves the source tree: {raw}")
    if not target.exists():
        raise ValueError(f"{source}: missing local target: {raw}")
    if parsed.fragment and target.suffix == ".md":
        text = target.read_text(encoding="utf-8")
        anchors = set(re.findall(r'\bid=["\']([^"\']+)', text))
        counts = {}
        for heading in re.findall(r'^#{1,6}\s+(.+?)\s*#*$', prose(text), re.MULTILINE):
            slug = re.sub(r'[^\w\- ]', '', heading.lower()).replace(' ', '-')
            count = counts.get(slug, 0)
            counts[slug] = count + 1
            anchors.add(slug if not count else f"{slug}-{count}")
        if unquote(parsed.fragment) not in anchors:
            raise ValueError(f"{source}: missing Markdown anchor: {raw}")


def validate_file(root: Path, path: Path) -> None:
    data = path.read_bytes()
    text = data.decode("utf-8")
    if data.startswith(b"\xef\xbb\xbf") or (text and not text.endswith("\n")):
        raise ValueError(f"{path}: expected UTF-8 without BOM and a final newline")
    if any(line.endswith((' ', '\t')) for line in text.splitlines()):
        raise ValueError(f"{path}: trailing whitespace")
    if path.suffix not in {".md", ".html"}:
        return
    visible = prose(text)
    if re.search(r'[\u3400-\u9fff]', visible):
        raise ValueError(f"{path}: contributor documentation must use English")
    parser = Resources()
    parser.feed(visible)
    for raw in [*LINK.findall(visible), *REFERENCE.findall(visible), *parser.targets]:
        validate_target(root, path, raw)


def main() -> int:
    try:
        names = source_files(ROOT)
        text_suffixes = {".md", ".json", ".html", ".css", ".js", ".svg", ".py", ".proto"}
        paths = sorted({ROOT / name for name in names if name and (
            Path(name).suffix in {".md", ".html"}
            or (Path(name).parts[0] in {"contracts", "tools"} and Path(name).suffix in text_suffixes)
        )})
        for path in paths:
            validate_file(ROOT, path)
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"Documentation validation failed: {error}", file=sys.stderr)
        return 1
    print(f"Public documentation validation passed: {len(paths)} files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
