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

//! Repository test access to internal contract adapters and shared Runner fixtures.
//!
//! Production callers enter Tenon through `run_main`; this module exists only so
//! integration tests can exercise the same protocol and Queue implementations as
//! the executable without making every implementation module public.

/// Production path derivation for an exclusive Sink input Queue.
pub use crate::pipeline::contract_test_support::egress_queue_path;

/// Production path derivation for the Bell Regions a plugin loop binds.
pub use crate::pipeline::contract_test_support::{flow_channel_bell_path, loops_bell_path};

/// Production side-directory names inside one plugin Instance directory.
pub use crate::pipeline::contract_test_support::{sink_directory_name, source_directory_name};

/// Generated protocol types used by cross-process contract tests.
pub mod contracts {
    pub use crate::contracts::{core, sink, source};
}

/// Source Queue sizing and validation functions used by contract tests.
pub mod ingress_queue {
    pub use crate::pipeline::contract_test_support::ingress_queue::{
        COMPLETION_MAX_PAYLOAD_SIZE, completion_capacity, max_pending_records, submission_capacity,
        validate_pair,
    };
}

/// Production Plugin Control lifecycle exercised by cross-language repository tests.
#[cfg(feature = "repository-test-support")]
pub mod plugin_program {
    pub use crate::pipeline::contract_test_support::plugin_program::{
        PluginProgramLaunch, force_stop_sink_program, run_sink_program,
        run_sink_program_with_control_stream_loss, run_sink_program_with_owner_loss,
        run_source_and_sink_program, run_source_program,
    };
}

/// Tenon Document parsing and validation used by contract tests.
pub mod tenon_document {
    pub use crate::tenon_document::verified::TenonDocumentVerifier;
    pub use crate::tenon_document::{TenonDocumentVerificationError, UnverifiedTenonDocument};
}
