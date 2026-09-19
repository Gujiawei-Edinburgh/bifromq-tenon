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

use crate::contracts::core::{LuaLimits, PipelineBootstrap, PipelineEnvironment, RetryBackoff};
use std::error::Error;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

#[allow(clippy::expect_used, reason = "the shared fixture has four CPUs")]
pub(in crate::pipeline::reconfigure) const AVAILABLE_CPU_COUNT: NonZeroUsize =
    NonZeroUsize::new(4).expect("the fixture CPU count is nonzero");

pub(in crate::pipeline) fn instance_directory(
    root: &Path,
    id: &crate::identifiers::PluginInstanceId,
) -> PathBuf {
    super::runtime_files::instance_working_directory(&root.join("instances"), id)
}

pub(in crate::pipeline::reconfigure) fn bootstrap(
    root: &Path,
) -> Result<PipelineBootstrap, Box<dyn Error>> {
    Ok(PipelineBootstrap {
        revision_plan: Some(super::plan::tests::base_revision("environment")?),
        environment: Some(PipelineEnvironment {
            metrics_node_id: None,
            pipeline_working_directory: root
                .to_str()
                .ok_or("Fixture path is not UTF-8")?
                .to_owned(),
            lua_limits: Some(LuaLimits {
                cpu_time_limit_ms: 100,
                memory_limit_bytes: 16_777_216,
            }),
            retry_backoff: Some(RetryBackoff {
                initial_delay_ms: 100,
                maximum_delay_ms: 30_000,
            }),
            reconfigure_timeout_ms: 15_000,
            available_cpu_count: AVAILABLE_CPU_COUNT.get() as u32,
        }),
    })
}

pub(in crate::pipeline::reconfigure) fn environment(
    root: &Path,
) -> Result<PipelineEnvironment, Box<dyn Error>> {
    let environment = bootstrap(root)?
        .environment
        .ok_or("Fixture environment is missing")?;
    Ok(environment)
}

#[cfg(not(feature = "loom-model"))]
pub(in crate::pipeline) mod clock;

#[cfg(not(feature = "loom-model"))]
pub(in crate::pipeline) mod controller {
    use super::*;
    use crate::identifiers::FlowId;
    use crate::pipeline::reconfigure::revision::PipelineRevision;
    use crate::pipeline::reconfigure::{PipelineReconfigurer, ReconfigureShutdown};
    use crate::pipeline::runtime::test_support::{HeldWorkerExit, hold_worker_exit};
    use std::future::Future;
    use tokio::sync::watch;

    pub(in crate::pipeline) use crate::pipeline::reconfigure::resource_stage::test_support::revision;

    pub(in crate::pipeline) async fn with_pipeline_environment(
        check: impl AsyncFnOnce(
            &mut PipelineReconfigurer,
            &Path,
            PipelineRevision,
        ) -> Result<(), Box<dyn Error>>,
    ) -> Result<(), Box<dyn Error>> {
        crate::pipeline::reconfigure::resource_stage::test_support::with_environment(
            "normal",
            &[],
            async |reconfigurer, _, root, target| check(reconfigurer, root, target).await,
        )
        .await
    }

    pub(in crate::pipeline) fn hold_flow_exit(
        reconfigurer: &mut PipelineReconfigurer,
        flow: &FlowId,
    ) -> Result<(HeldWorkerExit, impl Future<Output = ()> + Send + use<>), Box<dyn Error>> {
        let current = reconfigurer
            .current
            .as_mut()
            .ok_or("Pipeline has no applied runtime")?;
        Ok(hold_worker_exit(&mut current.runtime, flow)?)
    }

    pub(in crate::pipeline) fn observe_shutdown(
        reconfigurer: &PipelineReconfigurer,
    ) -> watch::Receiver<Option<ReconfigureShutdown>> {
        reconfigurer.shutdown_sender.subscribe()
    }
}
