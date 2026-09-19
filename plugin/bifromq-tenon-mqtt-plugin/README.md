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

# BifroMQ Tenon MQTT Plugin

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../../DISCLAIMER).

The BifroMQ Tenon MQTT Plugin bridges MQTT brokers and Tenon Flows. One plugin instance owns one MQTT client per Tenon channel. Client `i` uses the configured ID `clientIdPrefix + i`; the Source and Sink sides share these clients.

## Configuration

See [`config.schema.json`](config.schema.json) for all settings. Omit `source`, or set an empty `source.subscriptions`, to disable incoming MQTT messages while retaining Sink publishing.

```json
{
  "endpoint": "mqtt://broker.example.com:1883",
  "clientIdPrefix": "site-a-",
  "eventLoopThreads": 1,
  "auth": {
    "username": "bridge-a",
    "password": "your-password"
  },
  "source": {
    "subscriptions": [
      {
        "filter": "site/a/#",
        "qos": 1
      }
    ],
    "maxPendingMessages": 32
  },
  "sink": {
    "defaultQos": 1,
    "defaultRetain": false
  }
}
```

`endpoint` accepts `mqtt://` and `mqtts://` URLs. The plugin uses MQTT 5; TLS uses the platform trust store. Set `clientIdPrefix` so that all channel client IDs are unique on the broker. Sink records use the client with the same channel index.

A new plugin process starts a clean MQTT session. Reconnects within that process use `session.cleanStart` (default `false`) and the configured session expiry (default 24 hours). A process restart discards the prior broker session and its queued offline messages.

Source subscriptions apply to every channel client. After reconnecting, the plugin restores them when the broker has no session, or retries subscriptions that were not confirmed. A rejected subscription or a SUBACK timeout of 30 seconds fails the plugin. Check broker connectivity and actual message delivery separately from Tenon's `running` status.

Source subscriptions support MQTT QoS 0, 1 and 2.

`eventLoopThreads` defaults to 1 and controls the threads used for MQTT connections. The count is capped at the number of channels.

`source.maxPendingMessages` defaults to 32 and limits unacknowledged MQTT messages per channel. It is also advertised to the broker as the MQTT 5 receive maximum. Messages exceeding the local window receive `QuotaExceeded` for QoS 1 or 2; QoS 0 messages are dropped.

## Payloads

- `SourceRecordPayload` contains the received topic, body, QoS, retain flag, DUP flag, and receive timestamp.
- `SinkRecordPayload` contains the topic, body, and optional QoS and retain values.

Source acknowledgements follow successful Tenon completion. Uncompleted records remain unacknowledged; further input on the affected channel stops, and replay depends on MQTT QoS and session behavior. Design downstream processing to tolerate duplicates.

The body is binary-safe and is passed through without content-type conversion.

## Packaging

The plugin appears as **MQTT** in Console. Its required `display-name` and `description` are declared under `[package.metadata.tenon]` in Cargo.toml and written into the bundle manifest.

Install the generated bundle through the standard Tenon Plugin installation API. The bundle contains the schema, payload descriptor, manifest, and program executable. Build one bundle per Tenon target (`aarch64-apple-darwin`, `x86_64-apple-darwin`, `aarch64-unknown-linux-gnu`, or `x86_64-unknown-linux-gnu`) with the standard `cargo tenon bundle --target <target>` command. The plugin is started and managed by the standard Tenon Plugin lifecycle.

Build the MQTT plugin from a Tenon source checkout:

```sh
cargo install --path sdk/rust/cargo-tenon --locked
cargo tenon bundle --release --locked \
  --manifest-path plugin/bifromq-tenon-mqtt-plugin/Cargo.toml \
  --target aarch64-apple-darwin
```

Choose the target matching your Runner. The generated bundle is a local build;
redistributors must supply the licensing materials for the plugin and its bundled
dependencies before distributing it.
