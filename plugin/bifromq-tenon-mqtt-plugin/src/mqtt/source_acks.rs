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

//! Keeps Source completions attached to the MQTT session that delivered them.
use super::Error;
use rumqttc::{
    AsyncClient, ClientError, Event, Incoming, ManualAck, Outgoing, PubAckReason, PubRecReason,
    Publish,
};
use std::collections::{HashMap, VecDeque};

#[derive(Clone, Copy, Debug)]
pub(crate) struct SourceReceipt {
    session: u64,
    pkid: u16,
}

pub(super) struct SourceAcks {
    client: AsyncClient,
    session: u64,
    online: bool,
    accepted: HashMap<u16, Accepted>,
    ready: VecDeque<Settlement>,
}

struct Accepted {
    ack: ManualAck,
    completion: Completion,
    seen_on_connection: bool,
}

/// One acknowledgement the drive loop must still send.
enum Settlement {
    /// A business completion released this delivery's acknowledgement.
    Completed(u16),
    /// Local admission refused this record; the broker is told it was rejected.
    Rejected(ManualAck),
}

impl SourceAcks {
    pub(super) fn new(client: AsyncClient) -> Self {
        Self {
            client,
            session: 0,
            online: false,
            accepted: HashMap::new(),
            ready: VecDeque::new(),
        }
    }

    pub(super) fn event(&mut self, event: &Event) {
        match event {
            Event::Incoming(Incoming::ConnAck(ack)) => {
                self.online = true;
                if !ack.session_present {
                    self.session += 1;
                    self.accepted.clear();
                    self.ready.clear();
                }
            }
            Event::Outgoing(Outgoing::PubAck(pkid) | Outgoing::PubRec(pkid)) => {
                self.accepted.remove(pkid);
            }
            _ => {}
        }
    }

    pub(super) fn duplicate(&mut self, publish: &Publish) -> bool {
        if let Some(accepted) = self.accepted.get_mut(&publish.pkid) {
            accepted.seen_on_connection = true;
            if accepted.completion == Completion::Completed {
                accepted.completion = Completion::Ready;
                self.ready.push_back(Settlement::Completed(publish.pkid));
            }
            true
        } else {
            false
        }
    }

    pub(super) fn accept(&mut self, publish: &Publish) -> Option<SourceReceipt> {
        let ack = self.client.prepare_ack(publish)?;
        self.accepted.insert(
            publish.pkid,
            Accepted {
                ack,
                completion: Completion::Waiting,
                seen_on_connection: true,
            },
        );
        Some(SourceReceipt {
            session: self.session,
            pkid: publish.pkid,
        })
    }

    /// Refuses a record the SDK would not admit.
    ///
    /// The record never entered this state machine, so a broker replay of it is
    /// admitted afresh rather than taken for a delivery that is still running.
    /// The broker is told the local window refused the record, which ends its
    /// hold on that packet. A QoS 0 record carries no packet identifier, so it
    /// has no acknowledgement to answer with and is dropped.
    pub(super) fn refuse(&mut self, publish: &Publish) {
        let Some(mut ack) = self.client.prepare_ack(publish) else {
            return;
        };
        match &mut ack {
            ManualAck::PubAck(ack) => ack.reason = PubAckReason::QuotaExceeded,
            ManualAck::PubRec(ack) => ack.reason = PubRecReason::QuotaExceeded,
        }
        self.ready.push_back(Settlement::Rejected(ack));
    }

    pub(super) fn disconnected(&mut self) {
        self.online = false;
        // A flush failure can discard Outgoing events after the library has
        // advanced its handshake. Never retain that uncertain identifier as a
        // business delivery: a later PUBLISH may legally reuse it. If the ACK
        // was not sent, broker replay may repeat the completed business work.
        self.accepted
            .retain(|_, accepted| accepted.completion != Completion::Submitted);
        for accepted in self.accepted.values_mut() {
            accepted.seen_on_connection = false;
            if accepted.completion == Completion::Ready {
                accepted.completion = Completion::Completed;
            }
        }
        // A rejection queued on the lost connection never reached the broker.
        // The broker replays the record it still holds, and a fresh rejection
        // is prepared if admission still refuses it then.
        self.ready.retain(|settlement| match settlement {
            Settlement::Completed(_) => true,
            Settlement::Rejected(_) => false,
        });
    }

    /// Drops a tracked delivery without acknowledging it.
    ///
    /// The record reached no completion boundary, so the plugin leaves it
    /// unacknowledged and lets the broker replay it. Releasing the identity is
    /// what makes that replay a fresh admission instead of a duplicate of a
    /// delivery that is no longer running.
    pub(super) fn abandon(&mut self, receipt: SourceReceipt) {
        if receipt.session == self.session {
            self.accepted.remove(&receipt.pkid);
        }
    }

    pub(super) fn complete(&mut self, receipt: SourceReceipt) {
        if receipt.session == self.session
            && let Some(accepted) = self.accepted.get_mut(&receipt.pkid)
        {
            if accepted.completion != Completion::Waiting {
                return;
            }
            accepted.completion = Completion::Completed;
            if self.online && accepted.seen_on_connection {
                accepted.completion = Completion::Ready;
                self.ready.push_back(Settlement::Completed(receipt.pkid));
            }
        }
    }

    pub(super) fn drive(&mut self) -> Result<(), Error> {
        while let Some(settlement) = self.ready.front() {
            let ack = match settlement {
                Settlement::Completed(pkid) => {
                    let accepted = self
                        .accepted
                        .get_mut(pkid)
                        .expect("ready ACK has an accepted delivery");
                    accepted.ack.clone()
                }
                Settlement::Rejected(ack) => ack.clone(),
            };
            match self.client.try_manual_ack(ack) {
                Ok(()) => {
                    if let Settlement::Completed(pkid) = self.ready.front().expect("checked front")
                        && let Some(accepted) = self.accepted.get_mut(pkid)
                    {
                        accepted.completion = Completion::Submitted;
                    }
                    self.ready.pop_front();
                }
                Err(ClientError::RequestChannelFull(_)) => break,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rumqttc::{AckMode, ConnAck, ConnectReturnCode, MqttState, Packet, PubRel, QoS};

    #[test]
    fn full_native_queue_keeps_known_unsent_completion_across_reconnect() {
        let (requests, received) = flume::bounded(1);
        let client = AsyncClient::from_senders(requests);
        client.try_unsubscribe("busy").unwrap();
        let mut acks = SourceAcks::new(client);
        acks.event(&Event::Incoming(Incoming::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success,
            properties: None,
        })));
        let mut publish = Publish::new("input", QoS::ExactlyOnce, Vec::new(), None);
        publish.pkid = 42;
        let receipt = acks.accept(&publish).unwrap();
        acks.complete(receipt);
        acks.drive().unwrap();
        acks.disconnected();
        received.recv().unwrap();
        acks.event(&Event::Incoming(Incoming::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::Success,
            properties: None,
        })));
        assert!(
            acks.duplicate(&publish),
            "a known unsent ACK must not repeat business"
        );
        acks.drive().unwrap();
        assert!(
            matches!(received.recv().unwrap(), rumqttc::Request::PubRec(ack) if ack.pkid == 42)
        );
    }

    #[test]
    fn lost_outgoing_event_does_not_preserve_a_completed_packet_identifier() {
        let (requests, received) = flume::bounded(1);
        let client = AsyncClient::from_senders(requests);
        let mut acks = SourceAcks::new(client);
        let mut state = MqttState::builder(32).ack_mode(AckMode::Manual).build();
        acks.event(&Event::Incoming(Incoming::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success,
            properties: None,
        })));
        let mut publish = Publish::new("input", QoS::ExactlyOnce, b"old".to_vec(), None);
        publish.pkid = 42;
        state
            .handle_incoming_packet(Packet::Publish(publish.clone()))
            .unwrap();
        let receipt = acks.accept(&publish).unwrap();
        acks.complete(receipt);
        acks.drive().unwrap();
        assert!(matches!(
            state
                .handle_outgoing_packet(received.recv().unwrap())
                .unwrap(),
            Some(Packet::PubRec(_))
        ));
        // The real failure window: state already advanced, but a failed flush
        // invokes clean() before Outgoing::PubRec is returned to the plugin.
        state.clean();
        acks.disconnected();
        acks.event(&Event::Incoming(Incoming::ConnAck(ConnAck {
            session_present: true,
            code: ConnectReturnCode::Success,
            properties: None,
        })));
        assert!(matches!(
            state
                .handle_incoming_packet(Packet::PubRel(PubRel::new(42, None)))
                .unwrap(),
            Some(Packet::PubComp(_))
        ));
        // Even if the PUBCOMP event is lost too, this new delivery is independent.
        state.clean();
        acks.disconnected();
        publish.payload = b"new".as_slice().into();
        state
            .handle_incoming_packet(Packet::Publish(publish.clone()))
            .unwrap();
        assert!(
            !acks.duplicate(&publish),
            "new business must not be acknowledged as an old delivery"
        );
        assert!(acks.accept(&publish).is_some());
        assert!(received.is_empty());
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Completion {
    Waiting,
    Completed,
    Ready,
    Submitted,
}
