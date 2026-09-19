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

//! Owns subscription intent; rumqttc owns packet identifiers and reconnect replay.
use super::{Error, Subscription, qos};
use rumqttc::{AsyncClient, ClientError, Event, EventLoop, Incoming, Outgoing};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;

const ACK_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct SubscriptionControl {
    client: AsyncClient,
    filters: Vec<Subscription>,
    phase: Phase,
    online: bool,
    subscribe_index: usize,
    remote_subscriptions: bool,
    unsubscribe_index: usize,
    pending: Option<Pending>,
    failure: Option<String>,
    disconnect_queued: bool,
}

impl SubscriptionControl {
    pub(crate) fn new(client: AsyncClient, subscriptions: &[Subscription]) -> Self {
        Self {
            client,
            filters: subscriptions.to_vec(),
            phase: Phase::Dormant,
            online: false,
            subscribe_index: 0,
            remote_subscriptions: !subscriptions.is_empty(),
            unsubscribe_index: 0,
            pending: None,
            failure: None,
            disconnect_queued: false,
        }
    }

    pub(crate) fn activate(&mut self) {
        if self.phase == Phase::Dormant && !self.filters.is_empty() {
            self.phase = Phase::Active;
            self.drive();
        }
    }

    pub(crate) fn quiesce(&mut self) {
        if self.phase != Phase::Closed {
            self.phase = Phase::Quiesced;
            self.drive();
        }
    }

    pub(crate) fn close(&mut self) {
        self.phase = Phase::Closed;
        self.drive();
    }

    /// Reports whether a Publish arriving now belongs to this Source.
    ///
    /// Only an activated subscription set admits records, so a Source without
    /// configured subscriptions and a Source that already stopped producing
    /// both refuse every message.
    pub(crate) fn admits(&self) -> bool {
        self.phase == Phase::Active
    }

    /// Reports whether this Source has been asked to close. The Connection
    /// that owns it bounds its own closing wait with this.
    pub(super) fn closing(&self) -> bool {
        self.phase == Phase::Closed
    }

    // Every caller holds the same short-lived mutex; enqueue never waits for capacity.
    pub(super) fn drive(&mut self) {
        if self.failure.is_some() {
            return;
        }
        let result = if self.phase == Phase::Closed {
            if self.disconnect_queued {
                return;
            }
            self.client
                .try_disconnect_with_timeout(super::CLOSE_WINDOW)
                .map(|()| self.disconnect_queued = true)
        } else if !self.online || self.pending.is_some() {
            return;
        } else if self.phase == Phase::Active {
            let Some(filter) = self.filters.get(self.subscribe_index) else {
                return;
            };
            self.client
                .try_subscribe_tracked(filter.filter.clone(), qos(filter.qos))
                .map(|notice| {
                    self.remote_subscriptions = true;
                    self.pending = Some(Pending {
                        operation: Operation::Subscribe,
                        deadline: None,
                        completion: Box::pin(async move {
                            notice.wait_completion_async().await.map_err(Into::into)
                        }),
                    });
                })
        } else if self.phase == Phase::Quiesced && self.remote_subscriptions {
            let Some(filter) = self.filters.get(self.unsubscribe_index) else {
                return;
            };
            self.client
                .try_unsubscribe_tracked(filter.filter.clone())
                .map(|notice| {
                    self.pending = Some(Pending {
                        operation: Operation::Unsubscribe,
                        deadline: None,
                        completion: Box::pin(async move {
                            notice.wait_completion_async().await.map_err(Into::into)
                        }),
                    });
                })
        } else {
            return;
        };
        if let Err(error) = result
            && !matches!(error, ClientError::RequestChannelFull(_))
        {
            self.failure = Some(error.to_string());
        }
    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.drive();
        if let Some(error) = self.failure.take() {
            return Poll::Ready(Err(error.into()));
        }
        if !self.online {
            return Poll::Pending;
        }
        let Some(pending) = &mut self.pending else {
            return Poll::Pending;
        };
        match pending.completion.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                let operation = pending.operation;
                self.pending = None;
                if let Err(error) = result {
                    let (operation, index) = if operation == Operation::Subscribe {
                        ("SUBACK", self.subscribe_index)
                    } else {
                        ("UNSUBACK", self.unsubscribe_index)
                    };
                    return Poll::Ready(Err(format!(
                        "MQTT {operation} rejected subscription {index}: {error}"
                    )
                    .into()));
                }
                if operation == Operation::Subscribe {
                    self.subscribe_index += 1;
                } else {
                    self.unsubscribe_index += 1;
                    if self.unsubscribe_index == self.filters.len() {
                        self.remote_subscriptions = false;
                        self.subscribe_index = 0;
                    }
                }
                self.drive();
                Poll::Ready(Ok(()))
            }
        }
    }

    pub(super) fn event(&mut self, event: &Event) -> Result<(), Error> {
        match event {
            Event::Incoming(Incoming::ConnAck(ack)) => {
                self.online = true;
                if !ack.session_present {
                    // rumqttc has reset the old session and failed its notices.
                    self.pending = None;
                    self.subscribe_index = 0;
                    self.unsubscribe_index = 0;
                    self.remote_subscriptions = false;
                }
            }
            Event::Outgoing(Outgoing::Subscribe(_) | Outgoing::Unsubscribe(_)) => {
                if let Some(pending) = &mut self.pending {
                    pending.deadline = Some(Instant::now() + ACK_TIMEOUT);
                }
            }
            _ => {}
        }
        self.drive();
        Ok(())
    }

    pub(super) fn disconnected(&mut self, eventloop: &mut EventLoop) {
        self.online = false;
        // Include control requests admitted concurrently with poll's first cleanup.
        // The library preserves their identifiers and notices when replaying them.
        eventloop.clean();
        if let Some(pending) = &mut self.pending {
            pending.deadline = None;
        }
        self.disconnect_queued = false;
    }

    pub(super) fn deadline(&self) -> Option<Instant> {
        if self.phase == Phase::Closed || !self.online {
            None
        } else {
            self.pending.as_ref().and_then(|pending| pending.deadline)
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Dormant,
    Active,
    Quiesced,
    Closed,
}

struct Pending {
    operation: Operation,
    deadline: Option<Instant>,
    completion: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Subscribe,
    Unsubscribe,
}
