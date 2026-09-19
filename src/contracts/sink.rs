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

//! Sink wire contracts and Egress framing shared by Lua and Queue delivery.
//!
//! Lua checks the complete encoded size before allocating or accepting output.
//! Pipeline delivery uses the same size calculation and always writes field 1,
//! including an empty payload, because an empty Queue body is a wrap marker.

include!(concat!(env!("OUT_DIR"), "/tenon.sink.rs"));

pub(crate) struct EncodedEgressRecord {
    bytes: Vec<u8>,
}

impl EncodedEgressRecord {
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl From<&EgressRecord> for EncodedEgressRecord {
    fn from(record: &EgressRecord) -> Self {
        let mut bytes = Vec::with_capacity(encoded_len(record.payload.len()));
        // Emit even an empty payload: a zero-length Queue record is reserved for wrap markers.
        prost::encoding::bytes::encode(1, &record.payload, &mut bytes);
        Self { bytes }
    }
}

pub(crate) fn encoded_len(payload_len: usize) -> usize {
    prost::encoding::key_len(1)
        + prost::encoding::encoded_len_varint(payload_len as u64)
        + payload_len
}

#[cfg(test)]
mod tests {
    use super::{EgressRecord, EncodedEgressRecord, encoded_len};

    #[test]
    fn length_matches_encoding_across_varint_boundaries() {
        for payload_len in [0, 1, 127, 128, 16383, 16384] {
            let record = EgressRecord {
                payload: vec![0; payload_len],
            };
            let encoded = EncodedEgressRecord::from(&record);
            assert_eq!(encoded.len(), encoded_len(payload_len));
            if payload_len == 0 {
                assert_eq!(encoded.as_bytes(), [0x0a, 0x00]);
            }
        }
    }
}
