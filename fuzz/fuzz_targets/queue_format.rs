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

#![no_main]

use std::num::NonZeroU64;

use libfuzzer_sys::fuzz_target;
use tenon_ipc::queue::{
    DataCapacity, Frame, Header, LogicalPosition, decode_frame, encode_record_frame,
    inspect_frame_header, plan_append,
};

fuzz_target!(|input: &[u8]| {
    let capacity = DataCapacity::try_from(4096).expect("fixed capacity is valid");
    let limit = NonZeroU64::new(4088).expect("fixed payload limit is nonzero");
    let _ = Header::decode(capacity, input);
    let _ = inspect_frame_header(capacity, limit, input);
    let _ = decode_frame(capacity, limit, input);

    // Starting from a valid header reaches every padding and position check,
    // without requiring the fuzzer to rediscover the magic before each mutation.
    let mut header = Header::new(
        limit,
        LogicalPosition::try_from(0).unwrap(),
        1,
        LogicalPosition::try_from(0).unwrap(),
        1,
        capacity,
    )
    .unwrap()
    .encode();
    for pair in input.chunks_exact(2) {
        header[usize::from(pair[0]) % header.len()] ^= pair[1];
    }
    if let Ok(decoded) = Header::decode(capacity, &header) {
        assert_eq!(decoded.encode(), header);
        let _ = plan_append(
            capacity,
            limit,
            decoded.commit(),
            decoded.release(),
            input.len(),
        );
    }

    if !input.is_empty() && input.len() <= limit.get() as usize {
        let mut encoded = vec![0; 4096];
        let length = encode_record_frame(capacity, limit, input, &mut encoded)
            .expect("bounded nonempty payload must encode");
        assert_eq!(
            decode_frame(capacity, limit, &encoded[..length]).unwrap(),
            Frame::Record(input)
        );
    }
});
