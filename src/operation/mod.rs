/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


use crate::*;
use crate::alias::*;
use crate::client::*;
use crate::client::implementation::*;
use crate::decode::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::spec::*;
use crate::spec::connack::validate_connack_packet_inbound_internal;

use std::cell::RefCell;
use std::cmp::min;
use std::collections::*;
use std::mem;
use std::time::*;

enum MqttOperationOptions {
    Publish(PublishOptionsInternal),
    Subscribe(SubscribeOptionsInternal),
    Unsubscribe(UnsubscribeOptionsInternal),
    Disconnect(DisconnectOptionsInternal),
}

pub(crate) struct MqttOperation {
    id: u64,
    packet: Box<MqttPacket>,
    packet_id: Option<u16>,
    options: Option<MqttOperationOptions>,
    timeout: Option<Instant>,
}

impl MqttOperation {
    pub fn bind_packet_id(&mut self, packet_id: u16) -> () {
        self.packet_id = Some(packet_id);
        match &mut *self.packet {
            MqttPacket::Subscribe(subscribe) => {
                subscribe.packet_id = packet_id;
            }
            MqttPacket::Unsubscribe(unsubscribe) => {
                unsubscribe.packet_id = packet_id;
            }
            MqttPacket::Publish(publish) => {
                publish.packet_id = packet_id;
            }
            _ => {
                panic!("Invalid packet type for packet id binding");
            }
        }
    }
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
    Publish(Box<MqttPacket>, PublishOptionsInternal),
    Subscribe(Box<MqttPacket>, SubscribeOptionsInternal),
    Unsubscribe(Box<MqttPacket>, UnsubscribeOptionsInternal),
    Disconnect(Box<MqttPacket>, DisconnectOptionsInternal)
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

enum OperationResponse {
    Publish(PublishResponse),
    Subscribe(SubackPacket),
    Unsubscribe(UnsubackPacket),
    Disconnect
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

    allocated_packet_ids: HashMap<u16, u64>,
    pending_ack_operations: HashMap<u16, u64>,
    pending_write_completion_operations: Vec<u64>,
    pending_publish_count: u16,

    current_settings: Option<NegotiatedSettings>,

    next_operation_id: u64,
    next_packet_id: u16,

    encoder: Encoder,
    decoder: Decoder,

    next_ping_timepoint: Option<Instant>,
    ping_timeout_timepoint: Option<Instant>,

    connack_timeout_timepoint: Option<Instant>,

    outbound_alias_resolver: RefCell<Box<dyn OutboundAliasResolver>>,
    inbound_alias_resolver: InboundAliasResolver
}

impl OperationalState {

    // Crate-public API

    pub(crate) fn new(config: OperationalStateConfig) -> OperationalState {
        let outbound_resolver = (config.outbound_resolver_factory_fn)();
        let inbound_resolver = InboundAliasResolver::new((&config).connect.topic_alias_maximum.unwrap_or(0));

        OperationalState {
            config,
            state: OperationalStateType::Disconnected,
            pending_write_completion : false,
            operations: HashMap::new(),
            user_operation_queue: VecDeque::new(),
            resubmit_operation_queue: VecDeque::new(),
            high_priority_operation_queue: VecDeque::new(),
            current_operation: None,
            allocated_packet_ids: HashMap::new(),
            pending_ack_operations: HashMap::new(),
            pending_write_completion_operations: Vec::new(),
            pending_publish_count: 0,
            current_settings: None,
            next_operation_id : 1,
            next_packet_id : 1,
            encoder: Encoder::new(),
            decoder: Decoder::new(),
            next_ping_timepoint: None,
            ping_timeout_timepoint: None,
            connack_timeout_timepoint: None,
            outbound_alias_resolver: RefCell::new(outbound_resolver),
            inbound_alias_resolver: inbound_resolver
        }
    }

    pub(crate) fn handle_network_event(&mut self, context: &NetworkEventContext) -> Mqtt5Result<()> {
        let event = &context.event;

        match &event {
            NetworkEvent::ConnectionOpened => { self.handle_network_event_connection_opened(context) }
            NetworkEvent::ConnectionClosed => { self.handle_network_event_connection_closed() }
            NetworkEvent::WriteCompletion => { self.handle_network_event_write_completion() }
            NetworkEvent::IncomingData(data) => { self.handle_network_event_incoming_data(context, data) }
        }
    }

    pub(crate) fn service(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        match self.state {
            OperationalStateType::Disconnected => { Ok(()) }
            OperationalStateType::PendingConnack => { self.service_pending_connack(context) }
            OperationalStateType::Connected => { self.service_connected(context) }
            OperationalStateType::PendingDisconnect => { self.service_pending_disconnect(context) }
        }
    }

    pub(crate) fn handle_user_event(&mut self, context: UserEventContext) -> Mqtt5Result<()> {
        let mut op_id = 0;
        let event = context.event;
        match event {
            UserEvent::Subscribe(packet, subscribe_options) => {
                op_id = self.create_operation(context.current_time, packet, Some(MqttOperationOptions::Subscribe(subscribe_options)));
            }
            UserEvent::Unsubscribe(packet, unsubscribe_options) => {
                op_id = self.create_operation(context.current_time, packet, Some(MqttOperationOptions::Unsubscribe(unsubscribe_options)));
            }
            UserEvent::Publish(packet, publish_options) => {
                op_id = self.create_operation(context.current_time, packet, Some(MqttOperationOptions::Publish(publish_options)));
            }
            UserEvent::Disconnect(packet, disconnect_options) => {
                op_id = self.create_operation(context.current_time, packet, Some(MqttOperationOptions::Disconnect(disconnect_options)));
            }
        }

        self.enqueue_operation(op_id, OperationalQueueType::User, OperationalEnqueuePosition::Back)
    }

    pub(crate) fn get_next_service_timepoint(&self, current_time: &Instant) -> Instant {
        match self.state {
            OperationalStateType::Disconnected => { self.get_next_service_timepoint_disconnected(current_time) }
            OperationalStateType::PendingConnack => { self.get_next_service_timepoint_pending_connack(current_time) }
            OperationalStateType::Connected => { self.get_next_service_timepoint_connected(current_time) }
            OperationalStateType::PendingDisconnect => { self.get_next_service_timepoint_pending_disconnect(current_time) }
        }
    }

    // Private Implementation

    fn partition_operation_queue_by_queue_policy(&self, queue: &VecDeque<u64>, policy: &OfflineQueuePolicy) -> (Vec<u64>, Vec<u64>) {
        partition_operations_by_queue_policy(queue.into_iter().filter(|id| {
            self.operations.get(*id).is_some()
        }).map(|id| {
            (*id, &*self.operations.get(id).unwrap().packet)
        }), policy)
    }

    fn complete_operation_as_success(&mut self, id : u64, completion_result: Option<OperationResponse>) -> Mqtt5Result<()> {
        let operation_option = self.operations.remove(&id);
        if operation_option.is_none() {
            return Err(Mqtt5Error::InternalStateError);
        }

        let operation = operation_option.unwrap();
        if let Some(packet_id) = operation.packet_id {
            self.allocated_packet_ids.remove(&packet_id);
        }

        if operation.options.is_none() {
            return Ok(())
        }

        complete_operation_with_result(&mut operation.options.unwrap(), completion_result)
    }

    fn complete_operation_as_failure(&mut self, id : u64, error: Mqtt5Error) -> Mqtt5Result<()> {
        let operation_option = self.operations.remove(&id);
        if operation_option.is_none() {
            return Err(Mqtt5Error::InternalStateError);
        }

        let operation = operation_option.unwrap();
        if let Some(packet_id) = operation.packet_id {
            self.allocated_packet_ids.remove(&packet_id);
        }

        if operation.options.is_none() {
            return Ok(())
        }

        complete_operation_with_error(&mut operation.options.unwrap(), error)
    }

    fn complete_operation_sequence_as_failure<T>(&mut self, iterator: T, error: Mqtt5Error) -> Mqtt5Result<()> where T : Iterator<Item = u64> {
        iterator.fold(
            Ok(()),
            |res, item| {
                fold_mqtt5_result(res, self.complete_operation_as_failure(item, error))
            }
        )
    }

    fn complete_operation_sequence_as_empty_success<T>(&mut self, iterator: T) -> Mqtt5Result<()> where T : Iterator<Item = u64> {
        iterator.fold(
            Ok(()),
            |res, item| {
                fold_mqtt5_result(res, self.complete_operation_as_success(item, None))
            }
        )
    }

    fn handle_network_event_connection_opened(&mut self, context: &NetworkEventContext) -> Mqtt5Result<()> {
        if self.state != OperationalStateType::Disconnected {
            return Err(Mqtt5Error::InternalStateError);
        }

        self.state = OperationalStateType::PendingConnack;
        self.current_operation = None;
        self.pending_write_completion = false;
        self.pending_publish_count = 0;
        self.decoder.reset_for_new_connection();

        // Queue up a Connect packet
        let connect = self.create_connect();
        let connect_op_id = self.create_operation(context.current_time, connect, None);

        self.enqueue_operation(connect_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Front)?;

        self.connack_timeout_timepoint = Some(context.current_time + Duration::from_millis(self.config.connack_timeout_millis as u64));

        Ok(())
    }

    fn handle_network_event_connection_closed(&mut self) -> Mqtt5Result<()> {
        self.high_priority_operation_queue.clear();
        self.state = OperationalStateType::Disconnected;

        self.connack_timeout_timepoint = None;
        self.next_ping_timepoint = None;
        self.ping_timeout_timepoint = None;

        let mut completions : Vec<u64> = Vec::new();
        mem::swap(&mut completions, &mut self.pending_write_completion_operations);
        let result : Mqtt5Result<()> = self.complete_operation_sequence_as_failure(completions.iter().copied(), Mqtt5Error::ConnectionClosed);

        /*
          TODO:
            unacked_operations: apply offline queue policy to subscribe/unsubscribe
         */

        result
    }

    fn handle_network_event_write_completion(&mut self) -> Mqtt5Result<()> {
        self.pending_write_completion = false;

        let mut completions : Vec<u64> = Vec::new();
        mem::swap(&mut completions, &mut self.pending_write_completion_operations);
        let result : Mqtt5Result<()> = self.complete_operation_sequence_as_empty_success(completions.iter().copied());

        result
    }

    fn handle_network_event_incoming_data(&mut self, context: &NetworkEventContext, data: &[u8]) -> Mqtt5Result<()> {
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

    fn compute_outbound_alias_resolution(&self, packet: &MqttPacket) -> OutboundAliasResolution {
        if let MqttPacket::Publish(publish) = packet {
            return self.outbound_alias_resolver.borrow_mut().resolve_and_apply_topic_alias(&publish.topic_alias, &publish.topic);
        }

        OutboundAliasResolution{ ..Default::default() }
    }

    fn on_current_operation_fully_written(&mut self) -> () {
        let operation = self.operations.get(&self.current_operation.unwrap()).unwrap();
        let packet = &*operation.packet;
        match packet {
            MqttPacket::Subscribe(subscribe) => {
                self.pending_ack_operations.insert(subscribe.packet_id, operation.id);
            }
            MqttPacket::Unsubscribe(unsubscribe) => {
                self.pending_ack_operations.insert(unsubscribe.packet_id, operation.id);
            }
            MqttPacket::Publish(publish) => {
                if publish.qos == QualityOfService::AtMostOnce {
                    self.pending_write_completion_operations.push(operation.id);
                } else {
                    self.pending_ack_operations.insert(publish.packet_id, operation.id);
                    self.pending_publish_count += 1;
                }
            }
            MqttPacket::Pubrel(pubrel) => {
                self.pending_ack_operations.insert(pubrel.packet_id, operation.id);
            }
            _ => {
                self.pending_write_completion_operations.push(operation.id);
            }
        }

        self.current_operation = None;
    }

    fn service_queue(&mut self, context: &mut ServiceContext, mode: OperationalQueueServiceMode) -> Mqtt5Result<()> {
        loop {
            if self.current_operation.is_none() {
                self.current_operation = self.dequeue_operation(context, mode);
                if self.current_operation.is_none() {
                    return Ok(())
                }

                let current_operation_id = self.current_operation.unwrap();
                if !self.operations.contains_key(&current_operation_id) {
                    self.current_operation = None;
                    continue;
                }

                self.acquire_packet_id_for_operation(current_operation_id);

                let operation = self.operations.get(&current_operation_id).unwrap();
                let packet = &*operation.packet;
                let encode_context = EncodingContext {
                    outbound_alias_resolution: self.compute_outbound_alias_resolution(packet)
                };

                self.encoder.reset(packet, &encode_context)?;
            }

            let packet = &self.operations.get(&self.current_operation.unwrap()).unwrap().packet;

            let encode_result = self.encoder.encode(&*packet, &mut context.to_socket)?;
            if encode_result == EncodeResult::Complete {
                self.on_current_operation_fully_written();
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
                let ping_op_id = self.create_operation(context.current_time, ping, None);

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
        let next_service_time = *current_time + Duration::from_secs(u64::MAX);

        min(self.get_next_service_timepoint_operational_queue(current_time, OperationalQueueServiceMode::HighPriorityOnly), next_service_time)
    }

    fn schedule_ping(&mut self, current_time: &Instant) -> () {
        self.ping_timeout_timepoint = None;
        self.next_ping_timepoint = Some(*current_time + Duration::from_secs(self.current_settings.as_ref().unwrap().server_keep_alive as u64));
    }

    fn apply_session_present_to_connection(&mut self, session_present: bool) -> Mqtt5Result<()> {
        let mut unacked_operations : Vec<(u16, u64)> = self.pending_ack_operations.iter().map(|(key, val)| (*key, *val)).collect();
        self.pending_ack_operations.clear();

        unacked_operations.sort_by(|a, b| a.1.cmp(&(b.1)));

        if session_present {
            // TODO: unacked operations: qos 1+ publishes, pubrel to resubmit queue maintaining order
        } else {
            // TODO: unacked operations: apply offline queue policy
        }

        /*
          TODO: unacked operations: remaining to front of user queue maintaining order
          TODO: user operations: unbind packet ids
         */

        Ok(())
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
        self.outbound_alias_resolver.borrow_mut().reset_for_new_connection(packet.topic_alias_maximum.unwrap_or(0));
        self.inbound_alias_resolver.reset_for_new_connection();

        self.schedule_ping(current_time);

        self.apply_session_present_to_connection(packet.session_present)?;

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

    fn handle_suback(&mut self, packet: Box<MqttPacket>, packet_id: u16) -> Mqtt5Result<()> {
        let operation_id_option = self.pending_ack_operations.get(&packet_id);
        if let Some(operation_id) = operation_id_option {
            if let MqttPacket::Suback(suback) = *packet {
                let operation_option = self.operations.remove(&operation_id);
                if let Some(mut operation) = operation_option {
                    if let MqttOperationOptions::Subscribe(mut subscribe_options) = operation.options.unwrap() {
                        let response_channel = subscribe_options.response_sender.take();
                        if response_channel.unwrap().send(Ok(suback)).is_err() {
                            return Err(Mqtt5Error::OperationChannelSendError);
                        }

                        self.pending_ack_operations.remove(&packet_id);
                        self.allocated_packet_ids.remove(&packet_id);
                        return Ok(());
                    }
                }

                return Err(Mqtt5Error::InternalStateError);
            }
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_unsuback(&mut self, packet: Box<MqttPacket>, packet_id: u16) -> Mqtt5Result<()> {
        let operation_id_option = self.pending_ack_operations.get(&packet_id);
        if let Some(operation_id) = operation_id_option {
            if let MqttPacket::Unsuback(unsuback) = *packet {
                let operation_option = self.operations.remove(&operation_id);
                if let Some(mut operation) = operation_option {
                    if let MqttOperationOptions::Unsubscribe(mut unsubscribe_options) = operation.options.unwrap() {
                        let response_channel = unsubscribe_options.response_sender.take();
                        if response_channel.unwrap().send(Ok(unsuback)).is_err() {
                            return Err(Mqtt5Error::OperationChannelSendError);
                        }

                        self.pending_ack_operations.remove(&packet_id);
                        self.allocated_packet_ids.remove(&packet_id);
                        return Ok(());
                    }
                }

                return Err(Mqtt5Error::InternalStateError);
            }
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_puback(&mut self, packet: Box<MqttPacket>, packet_id: u16) -> Mqtt5Result<()> {
        Err(Mqtt5Error::Unimplemented)
    }

    fn handle_pubrec(&mut self, packet: Box<MqttPacket>, packet_id: u16) -> Mqtt5Result<()> {
        Err(Mqtt5Error::Unimplemented)
    }

    fn handle_pubrel(&mut self, packet: Box<MqttPacket>, packet_id: u16) -> Mqtt5Result<()> {
        Err(Mqtt5Error::Unimplemented)
    }

    fn handle_pubcomp(&mut self, packet: Box<MqttPacket>, packet_id: u16) -> Mqtt5Result<()> {
        Err(Mqtt5Error::Unimplemented)
    }

    fn handle_packet(&mut self, packet: Box<MqttPacket>, current_time: &Instant) -> Mqtt5Result<()> {
        let id_option = get_ack_packet_id(&packet);

        match &*packet {
            MqttPacket::Connack(connack) => { self.handle_connack(connack, current_time) }
            MqttPacket::Publish(_) => { Err(Mqtt5Error::Unimplemented) }
            MqttPacket::Pingresp(_) => { self.handle_pingresp() }
            MqttPacket::Disconnect(_) => { Err(Mqtt5Error::ServerSideDisconnect) }
            MqttPacket::Suback(_) => { self.handle_suback(packet, id_option.unwrap_or(0)) }
            MqttPacket::Unsuback(_) => { self.handle_unsuback(packet, id_option.unwrap_or(0)) }
            MqttPacket::Puback(_) => { self.handle_puback(packet, id_option.unwrap_or(0)) }
            MqttPacket::Pubcomp(_) => { self.handle_pubcomp(packet, id_option.unwrap_or(0)) }
            MqttPacket::Pubrel(_) => { self.handle_pubrel(packet, id_option.unwrap_or(0)) }
            MqttPacket::Pubrec(_) => { self.handle_pubrec(packet, id_option.unwrap_or(0)) }

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
    fn create_operation(&mut self, current_time: Instant, packet: Box<MqttPacket>, options: Option<MqttOperationOptions>) -> u64 {
        let id = self.next_operation_id;
        self.next_operation_id += 1;

        let timeout = calculate_operation_timeout_from_options(current_time, &options);

        let operation = MqttOperation {
            id,
            packet,
            packet_id: None,
            options,
            timeout
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

    fn acquire_packet_id_for_operation(&mut self, operation_id: u64) -> () {
        let operation = self.operations.get_mut(&operation_id).unwrap();

        if operation.packet_id.is_some() {
            return;
        }

        match &*operation.packet {
            MqttPacket::Subscribe(_) | MqttPacket::Unsubscribe(_) => { }
            MqttPacket::Publish(publish) => {
                if publish.qos == QualityOfService::AtMostOnce {
                    return;
                }
            }
            _ => { return; }
        }

        let start_id = self.next_packet_id;
        let mut check_id = start_id;

        loop {
            self.next_packet_id += 1;
            if self.next_packet_id == 0 {
                self.next_packet_id = 1;
            }

            if !self.allocated_packet_ids.contains_key(&check_id) {
                operation.bind_packet_id(check_id);
                self.allocated_packet_ids.insert(check_id, operation.id);
                return;
            }

            if self.next_packet_id == start_id {
                panic!("Packet Id space exhausted which should be impossible")
            }

            check_id = self.next_packet_id;
        }
    }

    fn release_packet_id_for_operation(&mut self, operation_id: u64) -> () {
        let operation = self.operations.get_mut(&operation_id).unwrap();

        if let Some(packet_id) = operation.packet_id {
            self.allocated_packet_ids.remove(&packet_id);
            operation.packet_id = None;
        }
    }
}

fn get_ack_packet_id(packet: &MqttPacket) -> Option<u16> {
    match packet {
        MqttPacket::Suback(suback) => { Some(suback.packet_id) }
        MqttPacket::Unsuback(unsuback) => { Some(unsuback.packet_id) }
        MqttPacket::Puback(puback) => { Some(puback.packet_id) }
        MqttPacket::Pubcomp(pubcomp) => { Some(pubcomp.packet_id) }
        MqttPacket::Pubrel(pubrel) => { Some(pubrel.packet_id) }
        MqttPacket::Pubrec(pubrec) => { Some(pubrec.packet_id) }
        _ => { None }
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

fn calculate_operation_timeout_from_options(current_time: Instant, options: &Option<MqttOperationOptions>) -> Option<Instant> {
    if let Some(operation_options) = options {
        let mut timeout_duration_millis : Option<u32> = None;

        match operation_options {
            MqttOperationOptions::Subscribe(subscribe_options) => {
                timeout_duration_millis = subscribe_options.options.timeout_in_millis;
            }
            MqttOperationOptions::Unsubscribe(unsubscribe_options) => {
                timeout_duration_millis = unsubscribe_options.options.timeout_in_millis;
            }
            MqttOperationOptions::Publish(publish_options) => {
                timeout_duration_millis = publish_options.options.timeout_in_millis;
            }
            _ => {}
        }

        if timeout_duration_millis.is_none() {
            return None;
        }

        return Some(current_time + Duration::from_millis(timeout_duration_millis.unwrap() as u64));
    }

    None
}

fn complete_operation_with_result(operation_options: &mut MqttOperationOptions, completion_result: Option<OperationResponse>) -> Mqtt5Result<()> {
    match operation_options {
        MqttOperationOptions::Publish(publish_options) => {
            if let OperationResponse::Publish(publish_result) = completion_result.unwrap() {
                let sender = publish_options.response_sender.take().unwrap();
                let _ = sender.send(Ok(publish_result));
                return Ok(());
            }
        }
        MqttOperationOptions::Subscribe(subscribe_options) => {
            if let OperationResponse::Subscribe(suback) = completion_result.unwrap() {
                let sender = subscribe_options.response_sender.take().unwrap();
                let _ = sender.send(Ok(suback));
                return Ok(());
            }
        }
        MqttOperationOptions::Unsubscribe(unsubscribe_options) => {
            if let OperationResponse::Unsubscribe(unsuback) = completion_result.unwrap() {
                let sender = unsubscribe_options.response_sender.take().unwrap();
                let _ = sender.send(Ok(unsuback));
                return Ok(());
            }
        }
        MqttOperationOptions::Disconnect(disconnect_options) => {
            let sender = disconnect_options.response_sender.take().unwrap();
            let _ = sender.send(Ok(()));
            return Ok(());
        }
    }

    Err(Mqtt5Error::InternalStateError)
}

fn complete_operation_with_error(operation_options: &mut MqttOperationOptions, error: Mqtt5Error) ->Mqtt5Result<()> {
    match operation_options {
        MqttOperationOptions::Publish(publish_options) => {
            let sender = publish_options.response_sender.take().unwrap();
            let _ = sender.send(Err(error));
        }
        MqttOperationOptions::Subscribe(subscribe_options) => {
            let sender = subscribe_options.response_sender.take().unwrap();
            let _ = sender.send(Err(error));
        }
        MqttOperationOptions::Unsubscribe(unsubscribe_options) => {
            let sender = unsubscribe_options.response_sender.take().unwrap();
            let _ = sender.send(Err(error));
        }
        MqttOperationOptions::Disconnect(disconnect_options) => {
            let sender = disconnect_options.response_sender.take().unwrap();
            let _ = sender.send(Err(error));
        }
    }

    Ok(())
}

fn does_packet_pass_offline_queue_policy(packet: &MqttPacket, policy: &OfflineQueuePolicy) -> bool {
    match packet {
        MqttPacket::Subscribe(_) | MqttPacket::Unsubscribe(_) => {
            match policy {
                OfflineQueuePolicy::PreserveQos1PlusPublishes | OfflineQueuePolicy::PreserveNothing => { false }
                OfflineQueuePolicy::Custom(predicate) => { predicate(packet) }
                _ => { true }
            }
        }
        MqttPacket::Publish(publish) => {
            match policy {
                OfflineQueuePolicy::PreserveNothing => { false }
                OfflineQueuePolicy::PreserveQos1PlusPublishes | OfflineQueuePolicy::PreserveAcknowledged => {
                    publish.qos != QualityOfService::AtMostOnce
                }
                OfflineQueuePolicy::Custom(predicate) => { predicate(packet) }
                _ => { true }
            }
        }
        _ => { true }
    }
}

fn partition_operations_by_queue_policy<'a, T>(iterator: T, policy: &OfflineQueuePolicy) -> (Vec<u64>, Vec<u64>) where T : Iterator<Item = (u64, &'a MqttPacket)> {
    let mut retained : Vec<u64> = Vec::new();
    let mut filtered : Vec<u64> = Vec::new();

    iterator.for_each(|(id, packet)| {
        if does_packet_pass_offline_queue_policy(packet, policy) {
            retained.push(id);
        } else {
            filtered.push(id);
        }
    });

    (retained, filtered)
}