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

use std::{
    io,
    num::{NonZeroU32, NonZeroU64},
    path::Path,
    sync::Arc,
};
use tenon_ipc::{
    bell::{BellRegion, create_bell_region},
    queue::{DataCapacity, create_queue_file},
};
pub(crate) fn region(path: &Path, slots: u32) -> io::Result<Arc<BellRegion>> {
    let slots =
        NonZeroU32::new(slots).ok_or_else(|| io::Error::other("a test region needs slots"))?;
    create_bell_region(path, slots, 0)?;
    BellRegion::open(path).map_err(io::Error::from)
}
pub(crate) fn create(path: &Path, capacity: usize, maximum: usize) -> io::Result<()> {
    create_queue_file(
        path,
        DataCapacity::try_from(capacity as u64).map_err(io::Error::other)?,
        NonZeroU64::new(maximum as u64)
            .ok_or_else(|| io::Error::other("a test queue needs a payload limit"))?,
    )
    .map_err(io::Error::from)
}
