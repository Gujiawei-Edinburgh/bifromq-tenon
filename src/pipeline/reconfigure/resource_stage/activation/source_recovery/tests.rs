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

//! Completed recovery work must not restart while its batch still owns control.

use super::*;
use crate::contracts::core::PluginInstanceState;
use crate::pipeline::plugin::test_support::{assert_reaped, recorded_pid};
use crate::pipeline::reconfigure::resource_stage::activation::tests::with_environment;

#[tokio::test(flavor = "current_thread")]
async fn rebuilt_source_waits_for_batch_return_even_when_its_retry_is_due()
-> Result<(), Box<dyn std::error::Error>> {
    with_environment("normal", &[], async |reconfigurer, _, root, target| {
        reconfigurer.apply(target, None).await?;
        loop {
            let status = reconfigurer
                .current
                .as_ref()
                .ok_or("Current runtime is missing")?
                .status_snapshot();
            if status
                .plugin_instances
                .iter()
                .all(|instance| instance.state() == PluginInstanceState::Running)
            {
                break;
            }
            let DataPlaneOperation::Completed(event) = reconfigurer.observe_runtime().await? else {
                return Err("Runtime stopped during startup".into());
            };
            reconfigurer.handle_runtime_event(event).await?;
        }
        let id = PluginInstanceId::try_from("dual-b")?;
        let directory = instance_working_directory(&root.join(INSTANCES_DIRECTORY_NAME), &id);
        let pid = recorded_pid(&directory)?;
        rustix::process::kill_process(pid, rustix::process::Signal::KILL)?;
        let DataPlaneOperation::Completed(event) = reconfigurer.observe_runtime().await? else {
            return Err("Runtime stopped before Source recovery".into());
        };
        assert!(matches!(&event, PluginInstanceEvent::ProcessFailed(failed) if failed == &id));
        reconfigurer.handle_runtime_event(event).await?;
        assert_reaped(pid)?;

        // Resume the batch wait with a genuinely rebuilt Source and an expired
        // retry. Even already-complete sibling work cannot allow another launch.
        let mut batch = BTreeMap::from([(id.clone(), RecoveryStep::Rebuilt)]);
        tokio::time::pause();
        tokio::time::advance(
            reconfigurer.environment.retry_backoff().initial_delay()
                + std::time::Duration::from_millis(1),
        )
        .await;
        let outcome = reconfigurer
            .wait_source_recovery(std::future::ready(()), &mut batch)
            .await;
        tokio::time::resume();
        assert!(matches!(outcome?, DataPlaneOperation::Completed(())));
        let status = reconfigurer
            .current
            .as_ref()
            .ok_or("Recovery lost current")?
            .status_snapshot();
        assert!(
            status
                .plugin_instances
                .iter()
                .any(|instance| instance.id == id.as_str()
                    && instance.state() == PluginInstanceState::RestartBackoff)
        );
        assert_eq!(recorded_pid(&directory)?, pid);
        Ok(())
    })
    .await
}
