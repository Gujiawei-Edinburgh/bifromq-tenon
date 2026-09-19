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

use super::PublishedPipelineRevisions;
use crate::contracts::core::{
    PipelineStatusSnapshot, PipelineToRunner, PluginInstanceState, PluginInstanceStatus,
    pipeline_to_runner,
};
use crate::payload_contract::PluginInterface;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::pipeline::test_support::{
    RevisionFixture, TargetReferenceProbe, revision_status as snapshot,
};
use crate::runner::pipeline::{PipelineLifecycleTarget, RunnerPipelineControlSessionError};
use crate::runner::test_support::{install_plugin, load_config, target};
use std::io;
use std::sync::Arc;
use tokio::sync::watch;

#[test]
fn unknown_status_panics_before_releasing_any_published_program() -> io::Result<()> {
    let mut fixture = RevisionFixture::new(&["com.example.first", "com.example.second"])?;
    let first = fixture.target("com.example.first", "first")?;
    let second = fixture.target("com.example.second", "second")?;
    let mut published = PublishedPipelineRevisions::new(first);
    publish(&mut published, second)?;
    let unknown = TenonDocumentEtag::for_source(b"unknown").strong_value();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        published.observe(envelope(snapshot(&unknown, PluginInstanceState::Running)))
    }));
    assert!(result.is_err());
    fixture.assert_in_use("com.example.first")?;
    fixture.assert_in_use("com.example.second")?;
    drop(published);
    fixture.uninstall("com.example.first")?;
    fixture.uninstall("com.example.second")
}

#[test]
fn complete_applied_status_releases_only_older_programs() -> io::Result<()> {
    let mut fixture = RevisionFixture::new(&[
        "com.example.first",
        "com.example.second",
        "com.example.third",
    ])?;
    let first = fixture.target("com.example.first", "first")?;
    let second = fixture.target("com.example.second", "second")?;
    let third = fixture.target("com.example.third", "third")?;
    let second_etag = second.document_etag().strong_value();
    let third_etag = third.document_etag().strong_value();
    let mut published = PublishedPipelineRevisions::new(first);
    publish(&mut published, second)?;
    publish(&mut published, third)?;
    let second_status = published.observe(envelope(snapshot(
        &second_etag,
        PluginInstanceState::Starting,
    )));
    assert_eq!(second_status.snapshot().document_etag, second_etag);
    assert_eq!(
        second_status.document().plugin_instances()["left"]
            .program_name()
            .as_str(),
        "com.example.second"
    );
    fixture.uninstall("com.example.first")?;
    fixture.assert_in_use("com.example.second")?;
    fixture.assert_in_use("com.example.third")?;

    let third_status = published.observe(envelope(snapshot(
        &third_etag,
        PluginInstanceState::StartFailed,
    )));
    fixture.uninstall("com.example.second")?;
    fixture.assert_in_use("com.example.third")?;
    drop(published);
    fixture.uninstall("com.example.third")?;
    // Status views retain only the applied Document, not executable Program owners.
    assert_eq!(second_status.document().flows().len(), 2);
    assert_eq!(third_status.snapshot().plugin_instances.len(), 2);
    Ok(())
}

#[test]
fn repeated_etag_uses_its_earliest_retained_occurrence() -> io::Result<()> {
    let mut fixture = RevisionFixture::new(&["com.example.first", "com.example.second"])?;
    let first = fixture.target("com.example.first", "first")?;
    let second = fixture.target("com.example.second", "second")?;
    let first_etag = first.document_etag().strong_value();
    let second_etag = second.document_etag().strong_value();
    let mut published = PublishedPipelineRevisions::new(Arc::clone(&first));
    publish(&mut published, second)?;
    publish(&mut published, first)?;
    for etag in [&first_etag, &second_etag] {
        published.observe(envelope(snapshot(etag, PluginInstanceState::Running)));
        fixture.assert_in_use("com.example.first")?;
        fixture.assert_in_use("com.example.second")?;
    }
    published.observe(envelope(snapshot(
        &first_etag,
        PluginInstanceState::Running,
    )));
    fixture.uninstall("com.example.second")?;
    fixture.assert_in_use("com.example.first")?;
    drop(published);
    fixture.uninstall("com.example.first")
}

#[test]
fn latest_slot_and_successful_publication_have_distinct_retention_boundaries() -> io::Result<()> {
    let mut fixture = RevisionFixture::new(&[
        "com.example.bootstrap",
        "com.example.replaced",
        "com.example.sent",
        "com.example.failed",
    ])?;
    let bootstrap = fixture.target("com.example.bootstrap", "bootstrap")?;
    let mut published = PublishedPipelineRevisions::new(bootstrap);
    let (targets, mut updates) =
        watch::channel(Some(fixture.target("com.example.replaced", "replaced")?));
    fixture.assert_in_use("com.example.replaced")?;
    drop(targets.send_replace(Some(fixture.target("com.example.sent", "sent")?)));
    fixture.uninstall("com.example.replaced")?;
    let mut wire = None;
    published
        .publish(
            updates
                .borrow_and_update()
                .clone()
                .ok_or_else(|| io::Error::other("Test target slot is populated"))?,
            |target| {
                wire = Some(target.revision());
                Ok(())
            },
        )
        .map_err(io::Error::other)?;
    published
        .publish(
            updates
                .borrow_and_update()
                .clone()
                .ok_or_else(|| io::Error::other("Test target slot is populated"))?,
            |_| Err(RunnerPipelineControlSessionError::Disconnected),
        )
        .map_err(io::Error::other)?;
    // No target remains in the latest slot, but the successful send still owns it.
    drop(targets.send_replace(None));
    fixture.assert_in_use("com.example.sent")?;
    drop(targets.send_replace(Some(fixture.target("com.example.failed", "failed")?)));
    assert!(
        published
            .publish(
                updates
                    .borrow_and_update()
                    .clone()
                    .ok_or_else(|| io::Error::other("Test target slot is populated"))?,
                |_| Err(RunnerPipelineControlSessionError::Disconnected)
            )
            .is_err()
    );
    fixture.assert_in_use("com.example.failed")?;
    drop(targets.send_replace(None));
    fixture.uninstall("com.example.failed")?;
    fixture.assert_in_use("com.example.sent")?;
    drop(published);
    fixture.uninstall("com.example.bootstrap")?;
    fixture.uninstall("com.example.sent")?;
    // Wire bytes are a transport value, not another installation owner.
    assert!(wire.is_some());
    Ok(())
}

#[test]
fn shared_program_survives_until_both_chains_and_latest_target_release_it() -> io::Result<()> {
    let mut fixture = RevisionFixture::new(&["com.example.shared", "com.example.replacement"])?;
    let first = fixture.target("com.example.shared", "first")?;
    let second = fixture.target("com.example.shared", "second")?;
    let replacement = fixture.target("com.example.replacement", "replacement")?;
    let replacement_etag = replacement.document_etag().strong_value();
    let (targets, _updates) = watch::channel(Some(Arc::clone(&second)));
    let mut first_chain = PublishedPipelineRevisions::new(first);
    let second_chain = PublishedPipelineRevisions::new(second);
    publish(&mut first_chain, replacement)?;
    first_chain.observe(envelope(snapshot(
        &replacement_etag,
        PluginInstanceState::Running,
    )));
    fixture.assert_in_use("com.example.shared")?;
    drop(second_chain);
    fixture.assert_in_use("com.example.shared")?;
    drop(targets.send_replace(None));
    fixture.uninstall("com.example.shared")?;
    drop(first_chain);
    fixture.uninstall("com.example.replacement")
}

#[test]
fn applied_status_releases_only_older_published_targets() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    let config = load_config(state_directory.path())?;
    let first = Arc::new(target(
        state_directory.path(),
        &config,
        "revision-owner",
        "function main(event) emit() end",
    )?);
    let second = Arc::new(target(
        state_directory.path(),
        &config,
        "revision-owner",
        "function main(event) emit() emit() end",
    )?);
    let third = Arc::new(target(
        state_directory.path(),
        &config,
        "revision-owner",
        "function main(event) local value = event emit() end",
    )?);
    let second_etag = second.revision().document_etag.clone();
    let third_etag = third.revision().document_etag.clone();
    let first_targets = TargetReferenceProbe::new(&first);
    let second_targets = TargetReferenceProbe::new(&second);
    let third_targets = TargetReferenceProbe::new(&third);

    let mut published = PublishedPipelineRevisions::new(Arc::clone(&first));
    publish(&mut published, Arc::clone(&second))?;
    publish(&mut published, Arc::clone(&third))?;
    drop(first);
    drop(second);
    drop(third);
    first_targets.assert_retained()?;
    second_targets.assert_retained()?;
    third_targets.assert_retained()?;

    published.observe(status(&second_etag, "primary"));
    first_targets.assert_released()?;
    second_targets.assert_retained()?;
    third_targets.assert_retained()?;

    published.observe(status(&third_etag, "primary"));
    second_targets.assert_released()?;
    third_targets.assert_retained()?;
    drop(published);
    third_targets.assert_released()
}

#[test]
fn repeated_etag_retains_each_intervening_published_target() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    let config = load_config(state_directory.path())?;
    let first = Arc::new(target(
        state_directory.path(),
        &config,
        "reverted-revision",
        "function main(event) emit() end",
    )?);
    let second = Arc::new(target(
        state_directory.path(),
        &config,
        "reverted-revision",
        "function main(event) local value = event emit() end",
    )?);
    let first_etag = first.revision().document_etag.clone();
    let second_etag = second.revision().document_etag.clone();
    let first_targets = TargetReferenceProbe::new(&first);
    let second_targets = TargetReferenceProbe::new(&second);
    let mut published = PublishedPipelineRevisions::new(Arc::clone(&first));
    publish(&mut published, Arc::clone(&second))?;
    publish(&mut published, Arc::clone(&first))?;
    drop(first);
    drop(second);

    published.observe(status(&first_etag, "primary"));
    first_targets.assert_retained()?;
    second_targets.assert_retained()?;

    published.observe(status(&second_etag, "primary"));
    first_targets.assert_retained()?;
    second_targets.assert_retained()?;

    published.observe(status(&first_etag, "primary"));
    first_targets.assert_retained()?;
    second_targets.assert_released()?;

    drop(published);
    first_targets.assert_released()
}

#[test]
fn revision_publication_registers_only_the_latest_successful_send() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    let config = load_config(state_directory.path())?;
    let bootstrap = Arc::new(target(
        state_directory.path(),
        &config,
        "revision-send",
        "function main(event) emit() end",
    )?);
    let replaced_before_send = Arc::new(target(
        state_directory.path(),
        &config,
        "revision-send",
        "function main(event) emit() emit() end",
    )?);
    let sent = Arc::new(target(
        state_directory.path(),
        &config,
        "revision-send",
        "function main(event) local value = event emit() end",
    )?);
    let failed = Arc::new(target(
        state_directory.path(),
        &config,
        "revision-send",
        "function main(event) local value = event.payload emit() end",
    )?);
    let sent_etag = sent.revision().document_etag.clone();
    let replaced_targets = TargetReferenceProbe::new(&replaced_before_send);
    let sent_targets = TargetReferenceProbe::new(&sent);
    let failed_targets = TargetReferenceProbe::new(&failed);
    let mut published = PublishedPipelineRevisions::new(Arc::clone(&bootstrap));
    let (targets, mut updates) = watch::channel(None);
    drop(targets.send_replace(Some(Arc::clone(&replaced_before_send))));
    drop(replaced_before_send);
    replaced_targets.assert_retained()?;
    drop(targets.send_replace(Some(Arc::clone(&sent))));
    replaced_targets.assert_released()?;

    let mut captured = None;
    published
        .publish(
            updates
                .borrow_and_update()
                .clone()
                .ok_or_else(|| io::Error::other("Test target slot is populated"))?,
            |target| {
                captured = Some(target.revision().document_etag.clone());
                Ok::<_, RunnerPipelineControlSessionError>(())
            },
        )
        .map_err(io::Error::other)?;
    assert_eq!(captured.as_deref(), Some(sent_etag.as_str()));
    assert!(published.latest_is(&sent));

    let bootstrap_etag = bootstrap.revision().document_etag.clone();
    drop(targets.send_replace(Some(Arc::clone(&bootstrap))));
    drop(sent);
    sent_targets.assert_retained()?;
    let mut reverted = None;
    published
        .publish(
            updates
                .borrow_and_update()
                .clone()
                .ok_or_else(|| io::Error::other("Test target slot is populated"))?,
            |target| {
                reverted = Some(target.revision().document_etag.clone());
                Ok::<_, RunnerPipelineControlSessionError>(())
            },
        )
        .map_err(io::Error::other)?;
    assert_eq!(reverted.as_deref(), Some(bootstrap_etag.as_str()));
    assert!(published.latest_is(&bootstrap));

    drop(targets.send_replace(Some(Arc::clone(&failed))));
    let error = published
        .publish(
            updates
                .borrow_and_update()
                .clone()
                .ok_or_else(|| io::Error::other("Test target slot is populated"))?,
            |_| Err(RunnerPipelineControlSessionError::Disconnected),
        )
        .err()
        .ok_or_else(|| io::Error::other("Failed revision publication was accepted"))?;
    assert!(matches!(
        error,
        RunnerPipelineControlSessionError::Disconnected
    ));
    assert!(!published.latest_is(&failed));
    drop(failed);
    failed_targets.assert_retained()?;

    drop(targets.send_replace(None));
    failed_targets.assert_released()?;
    Ok(())
}

fn publish(
    published: &mut PublishedPipelineRevisions,
    target: Arc<PipelineLifecycleTarget>,
) -> io::Result<()> {
    published
        .publish(target, |_| Ok(()))
        .map_err(io::Error::other)
}

fn status(document_etag: &str, sink_id: &str) -> PipelineToRunner {
    PipelineToRunner {
        message: Some(pipeline_to_runner::Message::StatusSnapshot(
            PipelineStatusSnapshot {
                document_etag: document_etag.to_owned(),
                plugin_instances: ["source", sink_id]
                    .into_iter()
                    .map(|id| PluginInstanceStatus {
                        id: id.to_owned(),
                        state: PluginInstanceState::Running as i32,
                        last_error: None,
                    })
                    .collect(),
            },
        )),
    }
}

fn envelope(status: PipelineStatusSnapshot) -> PipelineToRunner {
    PipelineToRunner {
        message: Some(pipeline_to_runner::Message::StatusSnapshot(status)),
    }
}
