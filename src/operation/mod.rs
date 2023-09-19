/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::client::*;
use crate::client::implementation::*;
use crate::decode::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::spec::*;

use std::collections::*;
use std::time::*;

pub(crate) struct MqttOperation {
    id: u64,
    packet: Box<MqttPacket>
}

pub(crate) enum NetworkEvent<'a> {
    ConnectionOpened,
    ConnectionClosed,
    IncomingData(&'a [u8]),
    WriteCompletion
}

pub(crate) struct NetworkEventContext<'a> {
    event: NetworkEvent<'a>,
    timestamp: Instant,
}

pub(crate) enum UserEvent {
    Start,
    Stop,
    Publish(PublishOptionsInternal),
    Subscribe(SubscribeOptionsInternal),
    Unsubscribe(UnsubscribeOptionsInternal)
}

pub(crate) struct UserEventContext {
    event: UserEvent,
    timestamp: Instant,
}

pub(crate) struct ServiceContext<'a> {
    to_socket: &'a mut Vec<u8>
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum OperationalStateType {
    Disconnected,
    PendingConnack,
    Connected,
    PendingDisconnect,
}

pub(crate) struct OperationalStateConfig {
    connect: Box<ConnectPacket>
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum OperationalQueueType {
    User,
    Resubmit,
    HighPriority,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum OperationalEnqueuePosition {
    Front,
    Back
}

pub(crate) struct OperationalState {
    config: OperationalStateConfig,

    state: OperationalStateType,

    pending_write_completion: bool,

    operations: HashMap<u64, MqttOperation>,

    user_operation_queue: VecDeque<u64>,
    resubmit_operation_queue: VecDeque<u64>,
    high_priority_operation_queue: VecDeque<u64>,

    pending_ack_operations: HashMap<u16, u64>,

    current_settings: Option<NegotiatedSettings>,

    next_operation_id: u64,

    encoder: Encoder,
    decoder: Decoder,
}

impl OperationalState {
    pub(crate) fn new(config: OperationalStateConfig) -> OperationalState {
        OperationalState {
            config,
            state: OperationalStateType::Disconnected,
            pending_write_completion : false,
            operations: HashMap::new(),
            user_operation_queue: VecDeque::new(),
            resubmit_operation_queue: VecDeque::new(),
            high_priority_operation_queue: VecDeque::new(),
            pending_ack_operations: HashMap::new(),
            current_settings: None,
            next_operation_id : 1,
            encoder: Encoder::new(),
            decoder: Decoder::new()
        }
    }

    pub(crate) fn handle_network_event(&mut self, context: &NetworkEventContext) -> Mqtt5Result<()> {
        let event = &context.event;

        match &event {
            NetworkEvent::ConnectionOpened => {
                if self.state != OperationalStateType::Disconnected {
                    return Err(Mqtt5Error::InternalStateError);
                }

                self.state = OperationalStateType::PendingConnack;
                self.pending_write_completion = false;

                // Queue up a Connect packet
                let connect = self.create_connect();
                let connect_op_id = self.create_operation(connect);

                self.enqueue_operation(connect_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Front)?;

                Ok(())
            }

            NetworkEvent::ConnectionClosed => {
                self.high_priority_operation_queue.clear();
                self.state = OperationalStateType::Disconnected;
                Ok(())
            }

            NetworkEvent::WriteCompletion => {
                self.pending_write_completion = false;
                Ok(())
            }

            NetworkEvent::IncomingData(data) => {
                if self.state == OperationalStateType::Disconnected {
                    return Err(Mqtt5Error::InternalStateError);
                }

                let mut decoded_packets = VecDeque::new();
                let mut context = DecodingContext {
                    maximum_packet_size: self.get_maximum_incoming_packet_size(),
                    decoded_packets: &mut decoded_packets
                };

                self.decoder.decode_bytes(data, &mut context)?;

                for packet in decoded_packets {
                    self.handle_packet_by_state(packet)?;
                }

                Ok(())
            }
        }
    }

    pub(crate) fn service(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        Ok(())
    }

    pub(crate) fn handle_user_event(&mut self, context: &UserEventContext) -> Mqtt5Result<()> {
        let event = &context.event;
        match event {
            _ => {
                Err(Mqtt5Error::Unimplemented)
            }
        }
    }

    fn handle_packet_by_state(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        match self.state {
            OperationalStateType::Disconnected => { Err(Mqtt5Error::InternalStateError) }
            OperationalStateType::PendingConnack => {
                match &*packet {
                    MqttPacket::Connack(connack) => {
                        self.on_connack_received(connack)?;
                        Ok(())
                    }
                    _ => { Err(Mqtt5Error::ProtocolError) }
                }
            }
            _ => {
                self.on_connected_packet_received(packet)?;
                Ok(())
            }
        }
    }

    fn on_connack_received(&mut self, packet: &ConnackPacket) -> Mqtt5Result<()> {
        // if failed reason code
        if packet.reason_code != ConnectReasonCode::Success {
            // TODO stuff
            return Err(Mqtt5Error::ConnectionRejected);
        }

        self.state = OperationalStateType::Connected;

        // TODO stuff
        Ok(())
    }

    fn on_connected_packet_received(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        match &*packet {
            MqttPacket::Publish(publish) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Suback(suback) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Unsuback(unsuback) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Puback(puback) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Pingresp(pingresp) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Pubcomp(pubcomp) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Pubrel(pubrel) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Pubrec(pubrel) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Disconnect(diuconnect) => { Err(Mqtt5Error::Unimplemented) }

            _ => { Err(Mqtt5Error::ProtocolError) }
        }
    }

    fn get_maximum_incoming_packet_size(&self) -> u32 {
        if let Some(maximum_packet_size) = &self.config.connect.maximum_packet_size_bytes {
            return *maximum_packet_size;
        }

        return MAXIMUM_VARIABLE_LENGTH_INTEGER as u32;
    }

    fn get_queue(&mut self, queue_type: OperationalQueueType) -> &mut VecDeque<u64> {
        match queue_type {
            OperationalQueueType::User => { &mut self.user_operation_queue }
            OperationalQueueType::Resubmit => { &mut self.resubmit_operation_queue }
            OperationalQueueType::HighPriority => { &mut self.high_priority_operation_queue }
        }
    }

    fn enqueue_operation(&mut self, id: u64, queue_type: OperationalQueueType, position: OperationalEnqueuePosition) -> Mqtt5Result<()> {
        if !self.operations.contains_key(&id) {
            return Err(Mqtt5Error::InternalStateError);
        }

        let queue = self.get_queue(queue_type);
        match position {
            OperationalEnqueuePosition::Front => { queue.push_front(id); }
            OperationalEnqueuePosition::Back => { queue.push_back(id); }
        }

        Ok(())
    }

    // Internal APIs
    fn create_operation(&mut self, packet: Box<MqttPacket>) -> u64 {
        let id = self.next_operation_id;
        self.next_operation_id += 1;

        let operation = MqttOperation {
            id,
            packet
        };

        self.operations.insert(id, operation);

        id
    }

    fn create_connect(&self) -> Box<MqttPacket> {
        let mut connect = (*self.config.connect).clone();

        if connect.client_id.is_none() {
            if let Some(settings) = &self.current_settings {
                connect.client_id = Some(settings.client_id.clone());
            }
        }

        // TODO: session resumption based on config properties

        return Box::new(MqttPacket::Connect(connect));
    }
}