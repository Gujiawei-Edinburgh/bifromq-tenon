/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! The Plugin's actual Source worker must recognize the SDK admission boundary.
#[path = "../../../sdk/rust/plugin-sdk/tests/support/process_peer.rs"]
mod peer;
use std::path::Path;
use tenon_plugin_sdk::Error;

#[tokio::test]
async fn sdk_admission_closes_before_mqtt_business_quiesce_without_panic_ack_or_retry()
-> Result<(), Error> {
    let mut peer = peer::Peer::start_source_binary(
        serde_json::json!({}),
        Path::new(&std::env::var("TENON_TEST_MQTT_SOURCE_BINARY")?),
    )
    .await?;
    peer.ready().await?;
    peer.quiesce().await?;
    peer.quiesced().await?;
    peer.event("admission-race-checked").await?;
    assert!(
        matches!(
            peer.reader()?.try_read()?,
            tenon_ipc::queue::ReadOutcome::Empty
        ),
        "closed admission must not commit a message"
    );
    peer.finish().await?;
    Ok(())
}
