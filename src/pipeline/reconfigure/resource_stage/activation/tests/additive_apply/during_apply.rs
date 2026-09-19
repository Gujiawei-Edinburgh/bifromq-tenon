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

//! Keeps the actual apply future alive when a concurrent test check fails or panics.

use super::*;
use crate::pipeline::reconfigure::ReconfigureShutdown;

pub(super) async fn during_apply(
    reconfigurer: &mut Reconfigurer,
    target: PipelineRevision,
    check: impl std::future::Future<Output = TestResult>,
) -> TestResult<Result<PipelineApplyOutcome, PipelineReconfigureError>> {
    let shutdown = reconfigurer.shutdown_handle();
    let mut applying = Box::pin(reconfigurer.apply(target, None));
    let mut checking = Box::pin(check);
    let checked = {
        let observing = poll_fn(|context| {
            match catch_unwind(AssertUnwindSafe(|| checking.as_mut().poll(context))) {
                Ok(result) => result.map(Ok),
                Err(panic) => Poll::Ready(Err(panic)),
            }
        });
        tokio::select! {
            biased;
            result = &mut applying => {
                result?;
                return Err("Apply finished before the concurrent check completed".into());
            }
            checked = timeout(TEST_DEADLINE, observing) => {
                checked
            }
        }
    };
    drop(checking);
    if !matches!(&checked, Ok(Ok(Ok(())))) {
        shutdown.request(ReconfigureShutdown::Force);
    }
    let applied = timeout(TEST_DEADLINE, &mut applying).await;
    if applied.is_err() {
        shutdown.request(ReconfigureShutdown::Force);
        let _cleanup = applying.as_mut().await;
    }
    drop(applying);
    match checked? {
        Ok(result) => result?,
        Err(panic) => resume_unwind(panic),
    }
    Ok(applied?)
}

#[tokio::test(flavor = "current_thread")]
async fn check_error_waits_for_actual_lua_apply_cleanup_before_returning() -> TestResult {
    use crate::pipeline::diagnostics::test_support::interested_flow_channel;
    with_environment("normal", &[("dual-to-archive", COUNTING_LUA)], async |reconfigurer, _, root, target| {
        super::failure::extend_initialization_budget(reconfigurer, root)?;
        let (diagnostics, mut records) = interested_flow_channel(&FlowId::try_from(String::from("dual-to-archive"))?, 0);
        reconfigurer.diagnostics = diagnostics;
        reconfigurer.apply(target, None).await?;
        let next = next_revision(root, |document| document["flows"]["dual-to-archive"]["process"]["script"] = json!("print('initializing'); while true do end"))?;
        let result = during_apply(reconfigurer, next, async {
            records.recv().await.ok_or("Initialization diagnostic is missing")?;
            Err("Injected concurrent check failure".into())
        }).await;
        assert!(matches!(result, Err(error) if error.to_string() == "Injected concurrent check failure"));
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::panic,
    reason = "the fixture verifies cleanup before propagating a test panic"
)]
async fn check_panic_waits_for_actual_lua_apply_cleanup_before_unwinding() -> TestResult {
    use crate::pipeline::diagnostics::test_support::interested_flow_channel;
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            super::failure::extend_initialization_budget(reconfigurer, root)?;
            let (diagnostics, mut records) =
                interested_flow_channel(&FlowId::try_from(String::from("dual-to-archive"))?, 0);
            reconfigurer.diagnostics = diagnostics;
            reconfigurer.apply(target, None).await?;
            let next = next_revision(root, |document| {
                document["flows"]["dual-to-archive"]["process"]["script"] =
                    json!("print('initializing'); while true do end")
            })?;
            let mut guarded = Box::pin(during_apply(reconfigurer, next, async {
                records
                    .recv()
                    .await
                    .ok_or("Initialization diagnostic is missing")?;
                panic!("Injected concurrent check panic");
            }));
            let outcome = poll_fn(|context| {
                match catch_unwind(AssertUnwindSafe(|| guarded.as_mut().poll(context))) {
                    Ok(result) => result.map(Ok),
                    Err(panic) => Poll::Ready(Err(panic)),
                }
            })
            .await;
            drop(guarded);
            assert!(outcome.is_err());
            assert!(reconfigurer.current.is_none());
            assert!(!root.exists());
            Ok(())
        },
    )
    .await
}
