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

# Building a Runner distribution

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

A Rust executable can reuse Tenon's Runner and Pipeline with three hooks: HTTP request authorization, execution admission, and complete-Document storage protection. Tenon does not supply licensing, billing, encryption algorithms or credential management. The public interfaces are exported by the `tenon` library.

The [security policy and threat model](../SECURITY.md#extension-responsibilities) describes the limits of each hook and the responsibilities of a custom distribution. Hook availability alone does not enable access control or turn plugin processes into security sandboxes.

```rust
fn main() -> std::process::ExitCode {
    tenon::run_main_with(|_config| Ok(tenon::RunnerHooks::default()))
}
```

`run_main_with` receives a one-time initializer over validated `RunnerConfig` and returns `ExitCode`. It runs once at Runner startup, before saved Documents are loaded or HTTP requests are accepted. Distribution settings belong to the optional `extra` object. Sanitize errors returned during initialization.

`RunnerHooks::new(policy, protection, authorization)` installs concrete implementations. Defaults are `AllowAll`, `Plaintext` and `NoHttpAuth`.

## DocumentProtection

`DocumentProtection: Send + Sync` exposes synchronous `protect(source: &[u8], output: &mut dyn Write)` and `unprotect(stored: &[u8], output: &mut dyn Write)`, both returning `io::Result<()>`. Implement the byte transformation; Tenon handles file storage.

A failed `protect` leaves the previous saved Document intact. A failed `unprotect` prevents startup and preserves the file. Methods may be called concurrently for different Documents. Bound their memory and computation costs; cancellation does not roll back a file operation already underway.

ETags, GET responses and plugin configuration always use original plaintext bytes. Randomized protected bytes do not change the ETag. Storage protection is not API authorization, field-level secret expansion or encryption of runtime memory.

## ExecutionPolicy

`authorize(ExecutionScope)` synchronously receives the complete proposed set of admitted, statically verified Documents and returns `Result<ExecutionPermit, ExecutionDenied>`. Scope references are borrowed for that call and have no ordering guarantee. Quota decisions use the proposed Document set. Missing-plugin Documents can still consume admission quota.

Calls happen on the Runner thread; the policy need not be Send/Sync and must return promptly without blocking I/O. Do not maintain an independent quota ledger by counting callbacks. An allowed Document can still fail to save.

Startup tries Documents one by one in id byte order. A denied Document remains readable and deletable but has no admitted Pipeline; retrying identical bytes through PUT rechecks admission. PUT evaluates a replacement in the full desired set after static and conditional checks and before persistence. Denial returns the hook's sanitized public code/message as HTTP 403. DELETE frees desired quota without another policy call. Plugin installation, automatic restart and reconfiguration do not reauthorize unchanged desired configuration.

Both accepted and denied results carry `entitlement_until: Option<Instant>`. Every result immediately replaces the current deadline, including `None` to remove it, independently of whether a later file operation succeeds. Expiry initiates bounded Runner shutdown even without API traffic. It does not promise instantaneous process disappearance. Once shutdown begins, later results cannot reopen admission. Update policy inputs in your distribution; `authorize` runs only during startup admission and Document PUT requests.

## HttpApiAuthorization

`HttpApiAuthorization: Send + Sync` asynchronously checks the original HTTP `Method`, URI path and `HeaderMap`, returning a Send future of `Result<(), HttpAuthRejection>`. It sees all methods and routes, including unknown routes, before endpoint body processing. It does not receive the query, request body, trailers or TLS client identity. Paths retain percent encoding; decode each segment once when interpreting resource ids. Preserve repeated and non-text header values.

`Unauthorized` requires a `WWW-Authenticate` challenge and yields 401; `Forbidden` yields 403 with an optional challenge. Core responses contain stable public messages and do not echo credentials or internal errors. HEAD responses have no body. Denial does not modify execution admission, entitlement deadlines or running Pipelines.

The hook may await external services but must not block executor threads. Implement timeouts and failures in the distribution. A cancelled request can drop the future before completion; release any resources it holds. Do not rely on completion of an abandoned callback for an irreversible operation. Requests already rejected by shutdown receive 503 without invoking authorization. With mutual TLS, transport authentication happens first; each new SSE connection is authorized, but an existing stream is not continuously reauthenticated.
