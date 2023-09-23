/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::cmp::min;
use crate::*;
use crate::client::*;
use crate::client::implementation::*;
use crate::decode::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::spec::*;

use std::collections::*;
use std::time::*;
use crate::alias::{InboundAliasResolver, OutboundAliasResolution, OutboundAliasResolver};
use crate::spec::connack::validate_connack_packet_inbound_internal;
use crate::validate::*;

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
    current_time: Instant,
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
    current_time: Instant,
}

pub(crate) struct ServiceContext<'a> {
    to_socket: &'a mut Vec<u8>,
    current_time: Instant,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum OperationalStateType {
    Disconnected,
    PendingConnack,
    Connected,
    PendingDisconnect,
}

pub(crate) struct OperationalStateConfig {
    connect: Box<ConnectPacket>,

    base_timestamp: Instant,

    connack_timeout_millis: u32,

    ping_timeout_millis: u32,

    outbound_resolver_factory_fn: fn () -> Box<dyn OutboundAliasResolver>,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum OperationalQueueType {
    User,
    Resubmit,
    HighPriority,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum OperationalQueueServiceMode {
    All,
    HighPriorityOnly,
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
    current_operation: Option<u64>,

    pending_ack_operations: HashMap<u16, u64>,

    current_settings: Option<NegotiatedSettings>,

    next_operation_id: u64,

    encoder: Encoder,
    decoder: Decoder,

    next_ping_timepoint: Option<Instant>,
    ping_timeout_timepoint: Option<Instant>,

    connack_timeout_timepoint: Option<Instant>,

    outbound_alias_resolver: Box<dyn OutboundAliasResolver>,
    inbound_alias_resolver: InboundAliasResolver
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
            current_operation: None,
            pending_ack_operations: HashMap::new(),
            current_settings: None,
            next_operation_id : 1,
            encoder: Encoder::new(),
            decoder: Decoder::new(),
            next_ping_timepoint: None,
            ping_timeout_timepoint: None,
            connack_timeout_timepoint: None,
            outbound_alias_resolver: (&config.outbound_resolver_factory_fn)(),
            inbound_alias_resolver: InboundAliasResolver::new(config.connect.topic_alias_maximum.unwrap_or(0))
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
                self.current_operation = None;
                self.pending_write_completion = false;
                self.decoder.reset_for_new_connection();

                // Queue up a Connect packet
                let connect = self.create_connect();
                let connect_op_id = self.create_operation(connect);

                self.enqueue_operation(connect_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Front)?;

                self.connack_timeout_timepoint = Some(context.current_time + Duration::from_millis(self.config.connack_timeout_millis as u64));

                Ok(())
            }

            NetworkEvent::ConnectionClosed => {
                self.high_priority_operation_queue.clear();
                self.state = OperationalStateType::Disconnected;

                self.connack_timeout_timepoint = None;
                self.next_ping_timepoint = None;
                self.ping_timeout_timepoint = None;

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
                let mut decode_context = DecodingContext {
                    maximum_packet_size: self.get_maximum_incoming_packet_size(),
                    decoded_packets: &mut decoded_packets
                };

                self.decoder.decode_bytes(data, &mut decode_context)?;

                for packet in decoded_packets {
                    self.handle_packet(packet, &context.current_time)?;
                }

                Ok(())
            }
        }
    }

    fn dequeue_operation(&mut self, context: &mut ServiceContext, mode: OperationalQueueServiceMode) -> Option<u64> {
        if !self.high_priority_operation_queue.is_empty() {
            return Some(self.high_priority_operation_queue.pop_front().unwrap());
        }

        if mode != OperationalQueueServiceMode::HighPriorityOnly {
            if !self.resubmit_operation_queue.is_empty() {
                return Some(self.resubmit_operation_queue.pop_front().unwrap());
            }

            if !self.user_operation_queue.is_empty() {
                return Some(self.user_operation_queue.pop_front().unwrap());
            }
        }

        None
    }

    fn compute_outbound_alias_resolution(&mut self, packet: &MqttPacket) -> OutboundAliasResolution {
        if let MqttPacket::Publish(publish) = packet {
            return self.outbound_alias_resolver.resolve_and_apply_topic_alias(&publish.topic_alias, &publish.topic);
        }

        OutboundAliasResolution{ ..Default::default() }
    }

    fn service_queue(&mut self, context: &mut ServiceContext, mode: OperationalQueueServiceMode) -> Mqtt5Result<()> {
        loop {
            if self.current_operation.is_none() {
                self.current_operation = self.dequeue_operation(context, mode);
                if self.current_operation.is_none() {
                    return Ok(())
                }

                let operation_option = self.operations.get(&self.current_operation.unwrap());
                if let Some(operation) = operation_option {
                    let packet = &*operation.packet;
                    let encode_context = EncodingContext {
                        outbound_alias_resolution: self.compute_outbound_alias_resolution(packet)
                    };

                    self.encoder.reset(packet, &encode_context)?;
                } else {
                    self.current_operation = None;
                    return Err(Mqtt5Error::InternalStateError);
                }
            }

            let packet = &self.operations.get(&self.current_operation.unwrap()).unwrap().packet;

            let encode_result = self.encoder.encode(&*packet, &mut context.to_socket)?;
            if encode_result == EncodeResult::Complete {
                self.current_operation = None;
            } else {
                return Ok(())
            }
        }
    }

    fn service_pending_connack(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        if context.current_time >= self.connack_timeout_timepoint.unwrap() {
            return Err(Mqtt5Error::ConnackTimeout);
        }

        self.service_queue(context, OperationalQueueServiceMode::HighPriorityOnly)?;

        Ok(())
    }

    fn service_keep_alive(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        if let Some(ping_timeout) = &self.ping_timeout_timepoint {
            if &context.current_time >= ping_timeout {
                return Err(Mqtt5Error::PingTimeout);
            }
        } else if let Some(next_ping) = &self.next_ping_timepoint {
            if &context.current_time >= next_ping {
                let ping = Box::new(MqttPacket::Pingreq(PingreqPacket{}));
                let ping_op_id = self.create_operation(ping);

                self.enqueue_operation(ping_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Front)?;

                self.ping_timeout_timepoint = Some(context.current_time + Duration::from_millis(self.config.ping_timeout_millis as u64));
            }
        }

        Ok(())
    }

    fn service_connected(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        self.service_keep_alive(context)?;

        self.service_queue(context, OperationalQueueServiceMode::All)?;

        Ok(())
    }

    fn service_pending_disconnect(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        self.service_queue(context, OperationalQueueServiceMode::HighPriorityOnly)?;

        Ok(())
    }

    pub(crate) fn service(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        match self.state {
            OperationalStateType::Disconnected => { Ok(()) }
            OperationalStateType::PendingConnack => { self.service_pending_connack(context) }
            OperationalStateType::Connected => { self.service_connected(context) }
            OperationalStateType::PendingDisconnect => { self.service_pending_disconnect(context) }
        }
    }

    pub(crate) fn handle_user_event(&mut self, context: &UserEventContext) -> Mqtt5Result<()> {
        let event = &context.event;
        match event {
            // TODO
            _ => {
                Err(Mqtt5Error::Unimplemented)
            }
        }
    }

    fn get_next_service_timepoint_operational_queue(&self, current_time: &Instant, mode: OperationalQueueServiceMode) -> Instant {
        let forever = *current_time + Duration::from_secs(u64::MAX);
        if self.pending_write_completion {
            return forever;
        }

        if !self.high_priority_operation_queue.is_empty() {
            return *current_time;
        }

        if mode == OperationalQueueServiceMode::All {
            if !self.resubmit_operation_queue.is_empty() || !self.user_operation_queue.is_empty() {
                return *current_time;
            }
        }

        forever
    }

    fn get_next_service_timepoint_disconnected(&self, current_time: &Instant) -> Instant {
        *current_time + Duration::from_secs(u64::MAX)
    }

    fn get_next_service_timepoint_pending_connack(&self, current_time: &Instant) -> Instant {
        min(self.get_next_service_timepoint_operational_queue(current_time, OperationalQueueServiceMode::HighPriorityOnly), self.connack_timeout_timepoint.unwrap())
    }

    fn get_next_service_timepoint_connected(&self, current_time: &Instant) -> Instant {
        let mut next_service_time = *current_time + Duration::from_secs(u64::MAX);

        if let Some(ping_timeout) = &self.ping_timeout_timepoint {
            next_service_time = min(next_service_time, *ping_timeout);
        }

        // TODO simplify?

        if self.pending_write_completion {
            return next_service_time;
        }

        if let Some(next_ping_timepoint) = &self.next_ping_timepoint {
            next_service_time = min(next_service_time, *next_ping_timepoint);
        }

        min(self.get_next_service_timepoint_operational_queue(current_time, OperationalQueueServiceMode::All), next_service_time)
    }

    fn get_next_service_timepoint_pending_disconnect(&self, current_time: &Instant) -> Instant {
        let mut next_service_time = *current_time + Duration::from_secs(u64::MAX);

        min(self.get_next_service_timepoint_operational_queue(current_time, OperationalQueueServiceMode::HighPriorityOnly), next_service_time)
    }

    pub(crate) fn get_next_service_timepoint(&self, current_time: &Instant) -> Instant {
        match self.state {
            OperationalStateType::Disconnected => { self.get_next_service_timepoint_disconnected(current_time) }
            OperationalStateType::PendingConnack => { self.get_next_service_timepoint_pending_connack(current_time) }
            OperationalStateType::Connected => { self.get_next_service_timepoint_connected(current_time) }
            OperationalStateType::PendingDisconnect => { self.get_next_service_timepoint_pending_disconnect(current_time) }
        }
    }

    fn schedule_ping(&mut self, current_time: &Instant) -> () {
        self.ping_timeout_timepoint = None;
        self.next_ping_timepoint = Some(*current_time + Duration::from_secs(self.current_settings.as_ref().unwrap().server_keep_alive as u64));
    }

    fn handle_connack(&mut self, packet: &ConnackPacket, current_time: &Instant) -> Mqtt5Result<()> {
        if self.state != OperationalStateType::PendingConnack {
            return Err(Mqtt5Error::ProtocolError);
        }

        if packet.reason_code != ConnectReasonCode::Success {
            return Err(Mqtt5Error::ConnectionRejected);
        }

        validate_connack_packet_inbound_internal(packet)?;

        self.state = OperationalStateType::Connected;
        self.current_settings = Some(build_negotiated_settings(&self.config, packet, &self.current_settings));
        self.connack_timeout_timepoint = None;
        self.outbound_alias_resolver.reset_for_new_connection(packet.topic_alias_maximum.unwrap_or(0));
        self.inbound_alias_resolver.reset_for_new_connection();

        self.schedule_ping(current_time);

        Ok(())
    }

    fn handle_pingresp(&mut self) -> Mqtt5Result<()> {
        match self.state {
            OperationalStateType::Connected |  OperationalStateType::PendingDisconnect => {
                if let Some(ping_timeout) = &self.ping_timeout_timepoint {
                    self.ping_timeout_timepoint = None;
                    self.schedule_ping(&self.next_ping_timepoint.unwrap());
                    Ok(())
                } else {
                    Err(Mqtt5Error::ProtocolError)
                }
            }
            _ => {
                Err(Mqtt5Error::ProtocolError)
            }
        }
    }

    fn handle_packet(&mut self, packet: Box<MqttPacket>, current_time: &Instant) -> Mqtt5Result<()> {
        match &*packet {
            MqttPacket::Connack(connack) => { self.handle_connack(connack, current_time) }
            MqttPacket::Publish(publish) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Suback(suback) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Unsuback(unsuback) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Puback(puback) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Pingresp(pingresp) => { self.handle_pingresp() }
            MqttPacket::Pubcomp(pubcomp) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Pubrel(pubrel) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Pubrec(pubrel) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Disconnect(disconnect) => { Err(Mqtt5Error::Unimplemented) }

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

fn build_negotiated_settings(config: &OperationalStateConfig, packet: &ConnackPacket, existing_settings: &Option<NegotiatedSettings>) -> NegotiatedSettings {
    let connect = &*config.connect;
    let final_client_id_ref = packet.assigned_client_identifier.as_ref().unwrap_or(connect.client_id.as_ref().unwrap_or(&existing_settings.as_ref().unwrap().client_id));

    NegotiatedSettings {
        maximum_qos : packet.maximum_qos.unwrap_or(QualityOfService::ExactlyOnce),
        session_expiry_interval : packet.session_expiry_interval.unwrap_or(connect.session_expiry_interval_seconds.unwrap_or(0)),
        receive_maximum_from_server : packet.receive_maximum.unwrap_or(65535),
        maximum_packet_size_to_server : packet.maximum_packet_size.unwrap_or(MAXIMUM_VARIABLE_LENGTH_INTEGER as u32),
        topic_alias_maximum_to_server : packet.topic_alias_maximum.unwrap_or(0),
        server_keep_alive : packet.server_keep_alive.unwrap_or(connect.keep_alive_interval_seconds),
        retain_available : packet.retain_available.unwrap_or(true),
        wildcard_subscriptions_available : packet.wildcard_subscriptions_available.unwrap_or(true),
        subscription_identifiers_available : packet.subscription_identifiers_available.unwrap_or(true),
        shared_subscriptions_available : packet.shared_subscriptions_available.unwrap_or(true),
        rejoined_session : packet.session_present,
        client_id : final_client_id_ref.clone()
    }
}