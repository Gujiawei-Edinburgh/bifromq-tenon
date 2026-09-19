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

mod payload;

use payload::SourceRecordPayload;
use tenon_plugin_sdk::{SourceProgram, TenonSource};

struct DummySource;

impl TenonSource for DummySource {
    fn start(&mut self) {}
    fn quiesce(&mut self) {}
    fn close(&mut self) {}
}

fn main() {
    SourceProgram::run::<SourceRecordPayload>(|_config, _parallelism, _sender| Ok(DummySource))
        .await_shutdown();
}
