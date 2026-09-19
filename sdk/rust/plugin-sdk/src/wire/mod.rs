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

//! Generated, language-neutral lifecycle and Source/Sink record bytes.
//!
//! This module has no runtime state. Author code uses the Source API, never
//! these private messages. Repository tests can inspect the same generated
//! definitions through the explicit test feature.

#[allow(missing_docs)]
pub mod plugin;

#[allow(missing_docs)]
pub mod source;

#[allow(missing_docs)]
pub mod sink;

#[cfg(test)]
mod tests;
