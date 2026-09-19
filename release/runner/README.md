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

# Apache BifroMQ Tenon Runner

Tenon is a satellite project of Apache BifroMQ (Incubating).
Read the accompanying DISCLAIMER for its incubation status.

This archive is built for the operating system and CPU named in its filename.
It does not include a JVM or any Java plugin runtime.

Create a UTF-8 JSON configuration file. Replace the state directory below with
an absolute, writable path and choose an available local port:

```json
{
  "stateDirectory": "/absolute/path/tenon-state",
  "http": {"listenAddress": "127.0.0.1:8080"},
  "pipeline": {"retryBackoff": {"initialDelayMs": 100, "maximumDelayMs": 30000}},
  "lua": {"cpuTimeLimitMs": 50, "memoryLimitBytes": 16777216}
}
```

Start with `bin/tenon --config /absolute/path/runner.jsonc`. Once started,
`http://127.0.0.1:8080/openapi.json` describes its management API. Stop with
SIGINT or SIGTERM. The default listener has no authorization; keep it on a
trusted management interface.

The Runner is licensed under Apache License 2.0. The accompanying LICENSE,
NOTICE, DEPENDENCIES and licenses/third-party.txt describe this distribution.

The Runner uses cryptographic software, including rustls and ring, for TLS.
Local rules may apply to the import, use and redistribution of cryptographic
software. Consult the Apache Software Foundation export information at
https://www.apache.org/licenses/exports/ and applicable local requirements.
