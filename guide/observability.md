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

# Metrics and live diagnostics

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

The Runner collects core metrics on request through `GET /metrics`. The default representation is Tenon's structured JSON, not OTLP JSON. `?format=prometheus` selects Prometheus text; `?format=json` explicitly selects JSON. The [metric catalog](../contracts/metrics/catalog.json) defines exact names, units, attributes, measurements and temporality. Use the running OpenAPI for filters and response fields.

Metrics are cumulative where the catalog says cumulative. Collect metrics periodically with your monitoring system. Tenon does not calculate percentiles. Collection is bounded by the configured metrics deadline; a busy, disconnected or slow Pipeline may be absent. Treat missing data as missing, not zero. Process identity and restarts matter when calculating rates. Filters use raw catalog names rather than exporter-specific renamed forms.

Pipeline and plugin state metrics use the same management state as the HTTP status resources. They do not establish external broker health or successful end-to-end delivery. Optional Runner labels and collection timing are configured once at startup through the [Runner Schema](../contracts/runner/config.schema.json).

## Diagnostic streams

Connect to `/pipelines/{id}/diagnostics` with one URL-encoded `target` and `Accept: text/event-stream`:

- `flow:<flowId>/channel:<channelIndex>` observes one channel's Lua output and errors.
- `plugin:<pluginInstanceId>` observes one plugin process's stdout and stderr.

The stream begins with `attached`, carries `diagnostic` records, and may end with `closed`; EOF is also authoritative termination. It may be attached before the selected object starts. Reconnection is a new subscription and repeats HTTP authorization.

Records include process/channel/VM identities and decimal-string sequences. Lua streams are `lua-print` and `lua-error`; error records additionally carry stable `phase` and `code`. Error text is an author-oriented hint, not a stable parser input or full native traceback. Plugin streams are `stdout` and `stderr` with one shared process sequence for both interfaces.

Text is capped at 16,384 UTF-8 bytes per record; truncation and invalid UTF-8 are reported. Sequence gaps can reveal loss. Delivery is best effort: there is no historical query, replay or Last-Event-ID resume. Slow subscribers must tolerate dropped diagnostics.

Keep Runner stderr for startup, persistence and process cleanup failures. A stopped or failed Runner may no longer serve HTTP, so API observation alone is insufficient for those failures.
