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

//! Bounded bridge from asynchronous API bodies to the synchronous Plugin Store.
//!
//! The producer copies transport frames into fixed-size chunks, so the queue is
//! bounded by bytes as well as messages. The blocking reader first spools a
//! complete body under the already-initialized private Plugin directory. Only
//! an explicit end marker permits Store validation and atomic publication;
//! cancellation or truncation leaves no Plugin fact.

use super::{PluginOperationFailure, RunnerUnavailable};
use crate::runner::plugin::store::PluginProgramInstallResult;
use bytes::{Buf as _, Bytes};
use std::io::{self, Read};
use tokio::sync::{mpsc, oneshot};

const UPLOAD_CHUNK_CAPACITY: usize = 4;
const UPLOAD_CHUNK_BYTES: usize = 64 * 1024;
type PluginInstallReply =
    Result<Result<PluginProgramInstallResult, PluginOperationFailure>, RunnerUnavailable>;
type PluginInstallResponse = oneshot::Sender<PluginInstallReply>;

/// The adapter-owned producer half of one Plugin package upload.
pub(crate) struct RunnerPluginUpload {
    chunks: mpsc::Sender<PluginUploadChunk>,
    result: oneshot::Receiver<PluginInstallReply>,
}

impl RunnerPluginUpload {
    pub(super) fn channel() -> (Self, PluginUploadReader, PluginInstallResponse) {
        let (chunks, receiver) = mpsc::channel(UPLOAD_CHUNK_CAPACITY);
        let (response, result) = oneshot::channel();
        let upload = Self { chunks, result };
        (
            upload,
            PluginUploadReader {
                receiver,
                current: Bytes::new(),
                ended: false,
            },
            response,
        )
    }

    /// Queues one transport frame using byte-bounded chunks and backpressure.
    ///
    /// # Errors
    ///
    /// Returns `RunnerUnavailable` when the consumer has stopped. Dropping the
    /// upload rejects an unfinished body.
    pub(crate) async fn write(&mut self, chunk: Bytes) -> Result<(), RunnerUnavailable> {
        for part in chunk.chunks(UPLOAD_CHUNK_BYTES) {
            self.chunks
                .send(PluginUploadChunk::Data(Bytes::copy_from_slice(part)))
                .await
                .map_err(|_| RunnerUnavailable)?;
        }
        Ok(())
    }

    /// Marks the request body complete and waits for the owned installation.
    ///
    /// # Errors
    ///
    /// The outer error reports unavailable management; the inner result
    /// preserves an ordinary package rejection or conflict.
    pub(crate) async fn finish(
        self,
    ) -> Result<Result<PluginProgramInstallResult, PluginOperationFailure>, RunnerUnavailable> {
        let _ = self.chunks.send(PluginUploadChunk::End).await;
        drop(self.chunks);
        self.result.await.map_err(|_| RunnerUnavailable)?
    }
}

/// The blocking Store-owned consumer half of one Plugin package upload.
pub(super) struct PluginUploadReader {
    receiver: mpsc::Receiver<PluginUploadChunk>,
    current: Bytes,
    ended: bool,
}

impl Read for PluginUploadReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.ended {
            return Ok(0);
        }
        while self.current.is_empty() {
            let Some(chunk) = self.receiver.blocking_recv() else {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Plugin upload ended without a complete request body",
                ));
            };
            match chunk {
                PluginUploadChunk::Data(chunk) => self.current = chunk,
                PluginUploadChunk::End => {
                    self.ended = true;
                    return Ok(0);
                }
            }
        }
        let length = output.len().min(self.current.len());
        output[..length].copy_from_slice(&self.current[..length]);
        self.current.advance(length);
        Ok(length)
    }
}

enum PluginUploadChunk {
    Data(Bytes),
    End,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn upload_chunks_apply_backpressure_and_stream_in_order() -> io::Result<()> {
        let (mut upload, mut reader, _response) = RunnerPluginUpload::channel();
        for chunk in [
            b"one".as_slice(),
            b"two".as_slice(),
            b"three".as_slice(),
            b"four".as_slice(),
        ] {
            upload
                .write(Bytes::copy_from_slice(chunk))
                .await
                .map_err(|_| io::Error::other("Upload channel closed"))?;
        }
        assert!(matches!(
            upload
                .chunks
                .try_send(PluginUploadChunk::Data(Bytes::from_static(b"five"))),
            Err(mpsc::error::TrySendError::Full(_))
        ));

        let read = tokio::task::spawn_blocking(move || {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes)?;
            Ok::<_, io::Error>(bytes)
        });
        upload
            .write(Bytes::from_static(b"five"))
            .await
            .map_err(|_| io::Error::other("Upload channel closed"))?;
        upload
            .chunks
            .send(PluginUploadChunk::End)
            .await
            .map_err(|_| io::Error::other("Upload channel closed"))?;
        drop(upload.chunks);

        let bytes = read.await.map_err(io::Error::other)??;
        assert_eq!(bytes, b"onetwothreefourfive");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn transport_frames_are_split_into_byte_bounded_chunks() -> io::Result<()> {
        let (mut upload, mut reader, _response) = RunnerPluginUpload::channel();
        let bytes = Bytes::from(vec![7; UPLOAD_CHUNK_BYTES * 3 + 1]);
        let writer = tokio::spawn(async move { upload.write(bytes).await });

        for expected in [
            UPLOAD_CHUNK_BYTES,
            UPLOAD_CHUNK_BYTES,
            UPLOAD_CHUNK_BYTES,
            1,
        ] {
            let Some(PluginUploadChunk::Data(chunk)) = reader.receiver.recv().await else {
                return Err(io::Error::other("Upload chunk was not produced"));
            };
            assert_eq!(chunk.len(), expected);
        }
        writer.await.map_err(io::Error::other)?.map_err(|_| {
            io::Error::other("Byte-bounded upload channel closed before the frame was split")
        })?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropped_request_body_is_not_treated_as_a_complete_package() -> io::Result<()> {
        let (mut upload, mut reader, _response) = RunnerPluginUpload::channel();
        upload
            .write(Bytes::from_static(b"valid-looking-prefix"))
            .await
            .map_err(|_| io::Error::other("Upload channel closed"))?;
        drop(upload);

        let error = tokio::task::spawn_blocking(move || {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).err()
        })
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Incomplete upload was accepted as EOF"))?;

        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        Ok(())
    }
}
