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

use super::*;
use proptest::test_runner::TestCaseError;

#[test]
fn limits_preserve_authored_numbers_and_omission_through_runner_transport() -> io::Result<()> {
    let verifier = verifier()?;
    for (pending, bytes) in [
        (None, None),
        (Some("1e2"), Some("262144.0")),
        (Some("3"), Some("4096")),
    ] {
        let mut document = self_route();
        for (field, number) in [("maxPendingRecords", pending), ("maxRecordBytes", bytes)] {
            if let Some(number) = number {
                document["flows"]["main"][field] = serde_json::from_str(number)?;
            }
        }
        let verified = verifier
            .verify(parse_value(&document)?)
            .map_err(io::Error::other)?;
        let reconstructed = VerifiedTenonDocument::from_runner_json(&verified.strict_json());
        assert_eq!(serde_json::to_value(&reconstructed)?, document);
        let flow = reconstructed
            .flows()
            .values()
            .next()
            .ok_or_else(|| io::Error::other("missing Flow"))?;
        assert_eq!(
            flow.max_pending_records().get(),
            if pending == Some("3") { 3 } else { 100 }
        );
        assert_eq!(
            flow.max_record_bytes().get(),
            if bytes == Some("4096") { 4096 } else { 262_144 }
        );
    }
    Ok(())
}

proptest! {
    #[test]
    fn independently_configured_flows_keep_their_own_limits(
        pending in 1_u64..=10_000, bytes in 1024_u64..=16_777_216,
        second_pending in 1_u64..=10_000, second_bytes in 1024_u64..=16_777_216,
    ) {
        let mut document = self_route();
        document["pluginInstances"]["second"] = document["pluginInstances"]["device"].clone();
        document["flows"]["other"] = document["flows"]["main"].clone();
        document["flows"]["other"]["source"] = json!("second");
        document["flows"]["other"]["sinks"] = json!(["second"]);
        for (id, count, size) in [("main", pending, bytes), ("other", second_pending, second_bytes)] {
            document["flows"][id]["maxPendingRecords"] = json!(count);
            document["flows"][id]["maxRecordBytes"] = json!(size);
        }
        let verified = verifier()?.verify(parse_value(&document)?).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let reconstructed = VerifiedTenonDocument::from_runner_json(&verified.strict_json());
        for (id, count, size) in [("main", pending, bytes), ("other", second_pending, second_bytes)] {
            let flow = &reconstructed.flows()[&crate::identifiers::FlowId::try_from(id.to_owned()).map_err(|error| TestCaseError::fail(error.to_string()))?];
            prop_assert_eq!(flow.max_pending_records().get(), count);
            prop_assert_eq!(flow.max_record_bytes().get(), size);
        }
    }
}
