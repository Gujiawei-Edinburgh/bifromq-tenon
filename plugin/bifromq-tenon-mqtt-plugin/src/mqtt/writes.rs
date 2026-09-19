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

//! Admits batches in call order; the connection task owns their work and results.
use super::{Config, Error, SinkRecordPayload, publish_message};
use futures_util::{StreamExt, stream::FuturesUnordered};
use rumqttc::{AsyncClient, PubAckReason, PubRecReason, PublishNotice, PublishNoticeError};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{mpsc, oneshot};

const CAPACITY: usize = 32;

pub(super) struct Writes {
    sender: mpsc::UnboundedSender<Batch>,
    active: AtomicUsize,
    failure: Mutex<Option<String>>,
}

pub(super) struct Dispatcher {
    client: AsyncClient,
    received: mpsc::UnboundedReceiver<Batch>,
}

struct Batch {
    records: Box<[SinkRecordPayload]>,
    default_qos: u8,
    default_retain: bool,
    completed: oneshot::Sender<Result<(), String>>,
    active: ActiveWrite,
}

impl Writes {
    pub(super) fn new(client: AsyncClient) -> (Arc<Self>, Dispatcher) {
        let (sender, received) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                sender,
                active: AtomicUsize::new(0),
                failure: Mutex::new(None),
            }),
            Dispatcher { client, received },
        )
    }

    pub(super) fn has_pending(&self) -> bool {
        self.active.load(Ordering::Acquire) != 0
    }

    pub(super) fn fail(&self, error: String) {
        *self.failure.lock().expect("write failure lock") = Some(error);
    }

    pub(super) fn write(
        self: &Arc<Self>,
        records: Box<[SinkRecordPayload]>,
        config: &Config,
    ) -> impl Future<Output = Result<(), Error>> {
        let (completed, completion) = oneshot::channel();
        self.active.fetch_add(1, Ordering::AcqRel);
        // The SDK retains each batch's Queue space until completion. Moving its
        // owned records here is bounded by all associated Egress Queue capacities.
        // Admission must return so SDK Shutdown can join its Queue threads before
        // closing this owner; a second blocking queue would create a close cycle.
        let admitted = self.sender.send(Batch {
            records,
            default_qos: config.sink.default_qos,
            default_retain: config.sink.default_retain,
            completed,
            active: ActiveWrite(self.clone()),
        });
        async move {
            if admitted.is_err() {
                return Err(self.closed_error());
            }
            completion
                .await
                .map_err(|_| self.closed_error())?
                .map_err(Into::into)
        }
    }

    fn closed_error(&self) -> Error {
        self.failure
            .lock()
            .expect("write failure lock")
            .clone()
            .unwrap_or_else(|| "MQTT publish owner stopped".to_owned())
            .into()
    }
}

impl Dispatcher {
    pub(super) async fn run(mut self) -> Result<(), Error> {
        let mut confirming = FuturesUnordered::new();
        loop {
            let batch = tokio::select! {
                biased;
                Some(result) = confirming.next() => { result?; continue; }
                batch = self.received.recv(), if confirming.len() < CAPACITY => {
                    let Some(batch) = batch else { return Ok(()) };
                    batch
                }
            };
            let Batch {
                records,
                default_qos,
                default_retain,
                completed,
                active,
            } = batch;
            let admit = admit(&self.client, records, default_qos, default_retain);
            tokio::pin!(admit);
            let notices = loop {
                tokio::select! {
                    biased;
                    Some(result) = confirming.next() => result?,
                    result = &mut admit => break result,
                }
            };
            let mut notices = match notices {
                Ok(notices) => notices,
                Err(error) => {
                    let message = error.to_string();
                    let _ = completed.send(Err(message));
                    return Err(error);
                }
            };
            confirming.push(async move {
                let result = async {
                    while let Some(result) = notices.next().await {
                        result?;
                    }
                    Ok::<(), PublishNoticeError>(())
                }
                .await
                .map_err(|error| error.to_string());
                // Network work is finished even when nobody polls its observer.
                drop(active);
                let failure = result.as_ref().err().cloned();
                let _ = completed.send(result);
                match failure {
                    Some(error) => Err(Error::from(error)),
                    None => Ok(()),
                }
            });
        }
    }
}

async fn admit(
    client: &AsyncClient,
    records: Box<[SinkRecordPayload]>,
    default_qos: u8,
    default_retain: bool,
) -> Result<FuturesUnordered<impl Future<Output = Result<(), PublishNoticeError>>>, Error> {
    let mut notices = FuturesUnordered::new();
    for record in records {
        let message = record.message.ok_or("sink message is required")?;
        let topic = message.topic.clone();
        let publish = publish_message(client, message, default_qos, default_retain);
        tokio::pin!(publish);
        loop {
            tokio::select! {
                biased;
                Some(result) = notices.next() => result?,
                result = &mut publish => {
                    notices.push(confirm(result?, topic));
                    break;
                }
            }
        }
    }
    Ok(notices)
}

async fn confirm(notice: PublishNotice, topic: String) -> Result<(), PublishNoticeError> {
    let Err(error) = notice.wait_completion_async().await else {
        return Ok(());
    };
    let rejected = match &error {
        PublishNoticeError::V5PubAck(
            reason @ (PubAckReason::NotAuthorized
            | PubAckReason::TopicNameInvalid
            | PubAckReason::PayloadFormatInvalid),
        ) => Some(*reason as u8),
        PublishNoticeError::V5PubRec(
            reason @ (PubRecReason::NotAuthorized
            | PubRecReason::TopicNameInvalid
            | PubRecReason::PayloadFormatInvalid),
        ) => Some(*reason as u8),
        _ => None,
    };
    if let Some(code) = rejected {
        // The broker has terminated this publication. Retrying the same invalid
        // record would block its Queue forever; report the loss and release it.
        eprintln!(
            "mqtt.publish_rejected topic={topic:?} code=0x{code:02x} retry=false reason={error}"
        );
        Ok(())
    } else {
        Err(error)
    }
}

struct ActiveWrite(Arc<Writes>);

impl Drop for ActiveWrite {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}
