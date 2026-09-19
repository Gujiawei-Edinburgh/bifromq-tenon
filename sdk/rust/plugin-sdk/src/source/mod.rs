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

//! Source author interface, one business Source lifecycle, and typed sends.

pub(crate) mod program;
pub(crate) mod session;

pub use program::SourceProgram;
pub use session::{AckCode, Completion, InvalidChannel, PayloadSender, SendError};

/// One business Source created and started by the SDK.
pub trait TenonSource {
    /// Starts producing. Panics terminate the plugin process.
    fn start(&mut self);
    /// Stops new production without waiting for outstanding send results.
    fn quiesce(&mut self);
    /// Reclaims business resources after the SDK has resolved its send results.
    fn close(&mut self);
}
