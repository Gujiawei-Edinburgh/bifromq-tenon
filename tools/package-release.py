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

"""Build source and Runner archives with their legal files and SHA-512 checksums."""

import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
from source_files import source_files as list_source_files


ROOT = Path(__file__).resolve().parents[1]
TARGETS = (
    "aarch64-apple-darwin", "x86_64-apple-darwin",
    "aarch64-unknown-linux-gnu", "x86_64-unknown-linux-gnu",
)


def run(*args):
    return subprocess.check_output(args, cwd=ROOT).decode()


def archive(output: Path, name: str, files: dict) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    path = output / (name + ".tar.gz")
    with tempfile.NamedTemporaryFile(dir=output, delete=False) as temporary:
        temporary_path = Path(temporary.name)
        try:
            with gzip.GzipFile(filename="", mode="wb", fileobj=temporary, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w") as tar:
                    for relative, (data, executable) in sorted(files.items()):
                        member = tarfile.TarInfo(name + "/" + relative)
                        member.size = len(data)
                        member.mode = 0o755 if executable else 0o644
                        tar.addfile(member, io.BytesIO(data))
            temporary.flush()
            temporary_path.replace(path)
        finally:
            temporary_path.unlink(missing_ok=True)
    digest = hashlib.sha512(path.read_bytes()).hexdigest()
    path.with_name(path.name + ".sha512").write_text(digest + "  " + path.name + "\n")
    return path


def source_files():
    files = {}
    for relative in list_source_files(ROOT):
        name = relative.as_posix()
        path = ROOT / name
        if path.is_symlink() and (not path.is_file() or not path.resolve().is_relative_to(ROOT.resolve())):
            raise ValueError(f"Source link must resolve to a file inside the source tree: {name}")
        if path.is_file():
            files[name] = (path.read_bytes(), bool(path.stat().st_mode & 0o111))
    for required in ("LICENSE", "NOTICE", "DISCLAIMER"):
        if required not in files:
            raise ValueError(f"Missing source release file: {required}")
    return files


def dependency_licenses(target: str, about: str):
    version = run(about, "--version").strip()
    if version != "cargo-about 0.9.2":
        raise ValueError("Install cargo-about 0.9.2 with --locked --features cli")
    report = json.loads(run(
        about, "generate", "--locked", "--fail", "--format", "json",
        "--target", target, "--config", str(ROOT / "tools/release-about.toml"),
    ))
    sections = []
    dependencies = set()
    for license in report["licenses"]:
        crates = [entry["crate"] for entry in license["used_by"] if entry["crate"]["source"]]
        if not crates:
            continue
        if not license.get("source_path") and license["id"] != "Apache-2.0":
            raise ValueError(f"Unverified license text for {crates[0]['name']}: {license['id']}")
        names = sorted({crate["name"] + " " + crate["version"] for crate in crates})
        dependencies.update(names)
        sections.append("\n".join(names) + "\nLicense: " + license["id"] + "\n\n" + license["text"])
    metadata = json.loads(run("cargo", "metadata", "--locked", "--format-version", "1", "--filter-platform", target))
    lua, = (package for package in metadata["packages"] if package["name"] == "lua-src")
    lua_header, = Path(lua["manifest_path"]).parent.glob("lua-5.5.*/lua.h")
    native_license = re.search(r"/\*{3,}\n\* Copyright.*?\*+/", lua_header.read_text(), re.DOTALL)
    if native_license is None:
        raise ValueError("Cannot identify the embedded Lua license in lua.h")
    # lua-src is a build dependency, but its Lua C library is linked into the Runner.
    sections.append(lua_header.parent.name + " (via lua-src " + lua["version"] + ")\n\n" + native_license.group())
    dependencies.add(lua_header.parent.name)
    notices = []
    for package in metadata["packages"]:
        if package["name"] + " " + package["version"] not in dependencies:
            continue
        for path in sorted(Path(package["manifest_path"]).parent.glob("NOTICE*")):
            if path.is_file():
                notices.append(package["name"] + " " + package["version"] + "\n" + path.read_text())
    return (
        "\n\n".join(sections).encode() + b"\n",
        ("\n".join(sorted(dependencies)) + "\n").encode(),
        "\n\n".join(notices).encode(),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("source", "runner"))
    parser.add_argument("--target", choices=TARGETS)
    parser.add_argument("--output", type=Path, default=ROOT / "target/dist")
    parser.add_argument("--cargo-about", default="cargo-about")
    args = parser.parse_args()
    if args.kind == "runner" and not args.target:
        parser.error("runner requires --target")
    subprocess.run(["python3", "tools/validate-license-headers.py"], cwd=ROOT, check=True)
    metadata = json.loads(run("cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"))
    package, = (package for package in metadata["packages"] if package["name"] == "tenon")
    name = "apache-bifromq-tenon-" + package["version"] + "-incubating"
    if args.kind == "source":
        files = source_files()
        name += "-src"
    else:
        subprocess.run(["cargo", "build", "--locked", "--release", "--bin", "tenon", "--target", args.target], cwd=ROOT, check=True)
        licenses, dependencies, notices = dependency_licenses(args.target, args.cargo_about)
        binary = Path(metadata["target_directory"]) / args.target / "release/tenon"
        files = {
            "bin/tenon": (binary.read_bytes(), True),
            "LICENSE": ((ROOT / "LICENSE").read_bytes() + b"\nBundled third-party licenses: licenses/third-party.txt\n", False),
            "NOTICE": ((ROOT / "release/runner/NOTICE").read_bytes() + notices, False),
            "DISCLAIMER": ((ROOT / "DISCLAIMER").read_bytes(), False),
            "README.md": ((ROOT / "release/runner/README.md").read_bytes(), False),
            "licenses/third-party.txt": (licenses, False),
            "DEPENDENCIES": (dependencies, False),
        }
        name += "-" + args.target + "-bin"
    path = archive(args.output.resolve(), name, files)
    print(f"Prepared {path} and SHA-512 checksum.")


if __name__ == "__main__":
    main()
