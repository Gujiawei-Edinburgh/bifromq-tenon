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

use super::*;
use crate::runner::state_directory::prepare_runner_state_directory;
use crate::runner::test_support::captured_runner_executable;
use std::ffi::OsStr;

#[test]
fn failed_runtime_image_copy_removes_the_partial_directory() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let image = tempfile::NamedTempFile::new()?;
    let write_only = fs::OpenOptions::new().write(true).open(image.path())?;
    let result = RunnerRuntimeResources::prepare(
        parent.path(),
        CapturedRunnerExecutable::from_file(write_only)?,
    );
    assert!(matches!(
        result,
        Err(RunnerRuntimeResourcesError::Executable(_))
    ));
    assert_eq!(fs::read_dir(parent.path())?.count(), 0);
    Ok(())
}

#[test]
fn failed_incomplete_marker_rename_preserves_the_runtime_directory() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let runtime = RunnerRuntimeResources::prepare(parent.path(), captured_runner_executable()?)
        .map_err(io::Error::other)?;
    let original = runtime.directory().to_path_buf();
    let suffix = runner_runtime_suffix(&original).map_err(io::Error::other)?;
    let marker = parent
        .path()
        .join(format!(".tenon-cleanup-incomplete-{suffix}"));
    fs::create_dir_all(marker.join("occupied"))?;

    let result = runtime.cleanup(RunnerRuntimeCleanup::MarkOwnerIncomplete);
    assert!(
        matches!(result, Err(RunnerRuntimeResourcesError::Directory { path, .. }) if path == marker)
    );
    assert!(original.is_dir());
    assert!(original.join("tenon").is_file());
    Ok(())
}

#[test]
fn runtime_creation_does_not_recreate_the_fixed_pipeline_root() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let pipelines = state_layout.pipeline_runtime_directory().to_path_buf();
    fs::remove_dir(&pipelines)?;

    let error = RunnerRuntimeResources::prepare(&pipelines, captured_runner_executable()?)
        .err()
        .ok_or_else(|| io::Error::other("runtime creation recreated the fixed root"))?;

    assert!(matches!(
        error,
        RunnerRuntimeResourcesError::Directory { .. }
    ));
    assert!(!pipelines.exists());
    Ok(())
}

#[test]
fn unrecovered_pipeline_owner_preserves_its_runtime_directory() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let pipeline_runtime_directory = state_layout.pipeline_runtime_directory();
    let runtime =
        RunnerRuntimeResources::prepare(&pipeline_runtime_directory, captured_runner_executable()?)
            .map_err(io::Error::other)?;
    let runtime_path = runtime.directory().to_path_buf();
    runtime
        .cleanup(RunnerRuntimeCleanup::MarkOwnerIncomplete)
        .map_err(io::Error::other)?;
    let preserved = fs::read_dir(&pipeline_runtime_directory)?
        .next()
        .ok_or_else(|| io::Error::other("Incomplete runtime directory is missing"))??
        .path();
    assert_ne!(preserved, runtime_path);
    assert!(!runtime_path.exists());
    assert!(
        preserved
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|name| name.starts_with(".tenon-cleanup-incomplete-"))
    );

    let error =
        RunnerRuntimeResources::prepare(&pipeline_runtime_directory, captured_runner_executable()?)
            .err()
            .ok_or_else(|| io::Error::other("unrecovered runtime directory was removed"))?;
    assert!(matches!(
        error,
        RunnerRuntimeResourcesError::IncompletePreviousCleanup(ref path) if path == &preserved
    ));
    assert!(preserved.is_dir());
    fs::remove_dir_all(preserved)
}
