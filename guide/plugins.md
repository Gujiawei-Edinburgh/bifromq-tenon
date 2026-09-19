<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Plugin packages and development

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

A Program is identified by `programName` and an exact semantic version. It implements `source`, `sink` or `source-and-sink`. An Instance runs the Program with its own configuration. Choose a language SDK and generator from the links below to write a plugin.

## Package structure

Each distributable is a gzip tar containing these fixed root files:

```text
manifest.json
config.schema.json
payload.descriptor.pb
```

Other ordinary files contain the launcher, executable, libraries, optional runtime and resources. The [manifest Schema](../contracts/plugin/manifest.schema.json) defines identity, display metadata, interface, a nonempty `platforms` list and an argv `command`. The descriptor must include the standard top-level `SourceRecordPayload` and/or `SinkRecordPayload` roots exactly for the interfaces implemented. Include source information in the descriptor; the standard packaging tools generate it.

Tar paths must be relative and canonical. Absolute paths, `.`/`..`, duplicate normalized paths, symlinks, hard links and special files are rejected. Directory entries and archived permission bits are not package identity. Installed directories use `0700` and ordinary files `0500`.

`command` is executed without a shell, with the installation directory as cwd. Bare executable names use the inherited PATH. Tenon appends its reserved `--sdk-config` argument; do not include it yourself. Use stdout and stderr for diagnostic text.

## Display metadata

Every manifest requires `displayName` and `description`. The display name is 1-80 Unicode code points of single-line plain text; the description is 1-1024 code points of plain text and may contain LF or CR line breaks. Both must contain a non-whitespace character. C0/C1 control characters are forbidden except LF and CR in descriptions; display names also forbid Unicode line and paragraph separators. The [manifest Schema](../contracts/plugin/manifest.schema.json) defines the exact character rules.

Values are preserved without trimming, normalization, truncation, or inferred defaults. They are package metadata, not HTML or Markdown. They do not participate in `programName + exactVersion` identity, Document references, interface selection, or launch behavior. The Runner returns both fields in list and single-Program responses. Console uses the display name as the title, a shortened description on cards, and the complete description on the inspection page.

Platform variants of the same Program version must carry identical display metadata. Editing either field changes the immutable package content; uploading changed text under an already installed identity conflicts just like any other package change. Rebuild plugins and generators together when updating the manifest contract; packages missing either field are invalid.

## Platform and immutability

Manifest platforms use `os` = `linux` or `darwin` and `architecture` = `amd64` or `arm64`. No aliases, wildcard or duplicate entries are accepted. Installation rejects packages that do not support the Runner's platform. One package has one command regardless of the number of declared platforms.

Package authors must declare genuine compatibility and document external dependencies. A portable JAR may rely on an external JVM; the Runner does not execute a package during installation to probe its dependencies. Missing dependencies become startup failures.

On the same Runner, the normalized ordinary-file paths and every file's original bytes determine duplicate installation. Equal content is idempotent; any difference for the same identity conflicts. Across platform variants of one logical version, interface, Config Schema, payload descriptors and business semantics must remain identical, while launcher/runtime/platform material may differ. Change the exact version for a semantic change.

Install through POST `/plugins`, then refer to the exact identity from a Document. To upgrade, install the new version, update the Document, observe application and shutdown of the old instance, then uninstall the old version. Do not modify the installed tree. Damaged installed packages can stop the Runner or leave dependent Documents unready; restore them by reinstalling the original bundle.

## SDKs and generators

- [Rust SDK](../sdk/rust/plugin-sdk/README.md), [scaffold](../sdk/rust/rust-plugin-scaffold/README.md), and [cargo-tenon](../sdk/rust/cargo-tenon/README.md).
- [Java SDK](../sdk/java/README.md), [archetype](../sdk/java/README.md#generate-a-plugin), and [Maven packaging plugin](../sdk/java/README.md#packaging).
- [Repository plugins and local debugging](../plugin/README.md), including the Dummy Source, Stdout Sink and MQTT plugin.

Use the generators' standard build and bundle operations. Document how the plugin acknowledges, retries and rejects records. A Sink should report success only after delivery reaches its documented guarantee. Test normal shutdown, failures, replay and actual external delivery as well as startup.

The [quickstart](quickstart.md) builds an entirely local generated plugin and exercises the normal installation API. [Contributing](../CONTRIBUTING.md) describes repository-wide and cross-platform validation.

## Build artifacts for distribution

Use `cargo tenon bundle --release` for a Rust distribution; the quickstart's default debug bundle is for local testing. Rust binaries can contain build-machine paths in diagnostics and debug information. For a distributable, set compiler `--remap-path-prefix` mappings for the checkout, Cargo cache and any local dependencies through `RUSTFLAGS` or `CARGO_ENCODED_RUSTFLAGS`, then inspect the finished archive. A release profile alone does not remove every embedded source path. Keep build logs and local test results outside the published package.
