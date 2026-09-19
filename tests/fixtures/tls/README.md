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

# HTTPS test identities

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../../../DISCLAIMER).

These are public test materials and must never be used in a deployment. A fixed test root CA signs an intermediate CA, which issues two distinct localhost server certificates. Each server PEM contains the server certificate followed by the intermediate. Server certificates are valid from 2020-01-01 to 2120-01-01. Clients trust only the test root and do not disable TLS verification.

Mutual TLS uses independent client CAs A and B. A issues through an intermediate; B issues directly. Client certificates share one public test private key. Normal certificates are valid through 2120; `expired` ended in 2021, `future` begins in 2110, `server-only` permits only server use, and `no-eku` omits extended key usage. CA private keys are not retained. None of these identities or trust roots is suitable for production.
