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
use std::cmp::{min, Ordering, Reverse};
use std::collections::*;
use std::mem;
use std::sync::Arc;
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
    connection_packets: &'a mut VecDeque<Box<MqttPacket>>,
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
    pub connect: Box<ConnectPacket>,

    pub base_timestamp: Instant,

    pub offline_queue_policy: OfflineQueuePolicy,
    pub rejoin_session_policy: RejoinSessionPolicy,

    pub connack_timeout_millis: u32,
    pub ping_timeout_millis: u32,

    pub outbound_resolver: Option<Box<dyn OutboundAliasResolver + Send>>,
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

#[derive(Copy, Clone, PartialEq, Eq)]
struct OperationTimeoutRecord {
    id: u64,
    timeout: Instant
}

impl PartialOrd for OperationTimeoutRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.timeout.cmp(&other.timeout))
    }    
}

impl Ord for OperationTimeoutRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timeout.cmp(&other.timeout)
    }
}

pub(crate) struct OperationalState {
    config: OperationalStateConfig,

    state: OperationalStateType,

    pending_write_completion: bool,

    operations: HashMap<u64, MqttOperation>,

    operation_ack_timeouts: BinaryHeap<Reverse<OperationTimeoutRecord>>,

    user_operation_queue: VecDeque<u64>,
    resubmit_operation_queue: VecDeque<u64>,
    high_priority_operation_queue: VecDeque<u64>,
    current_operation: Option<u64>,

    qos2_incomplete_incoming_publishes: HashSet<u16>,
    allocated_packet_ids: HashMap<u16, u64>,
    pending_ack_operations: HashMap<u16, u64>,
    pending_write_completion_operations: VecDeque<u64>,
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

    pub(crate) fn new(mut config: OperationalStateConfig) -> OperationalState {
        let outbound_resolver = config.outbound_resolver.take().unwrap_or(Box::new(NullOutboundAliasResolver::new()));
        let inbound_resolver = InboundAliasResolver::new((&config).connect.topic_alias_maximum.unwrap_or(0));

        OperationalState {
            config,
            state: OperationalStateType::Disconnected,
            pending_write_completion : false,
            operations: HashMap::new(),
            operation_ack_timeouts: BinaryHeap::new(),
            user_operation_queue: VecDeque::new(),
            resubmit_operation_queue: VecDeque::new(),
            high_priority_operation_queue: VecDeque::new(),
            current_operation: None,
            qos2_incomplete_incoming_publishes: HashSet::new(),
            allocated_packet_ids: HashMap::new(),
            pending_ack_operations: HashMap::new(),
            pending_write_completion_operations: VecDeque::new(),
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

    pub(crate) fn handle_network_event(&mut self, context: &mut NetworkEventContext) -> Mqtt5Result<()> {
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
        let event = context.event;
        let op_id = match event {
            UserEvent::Subscribe(packet, subscribe_options) => {
                self.create_operation(packet, Some(MqttOperationOptions::Subscribe(subscribe_options)))
            }
            UserEvent::Unsubscribe(packet, unsubscribe_options) => {
                self.create_operation(packet, Some(MqttOperationOptions::Unsubscribe(unsubscribe_options)))
            }
            UserEvent::Publish(packet, publish_options) => {
                self.create_operation(packet, Some(MqttOperationOptions::Publish(publish_options)))
            }
            UserEvent::Disconnect(packet, disconnect_options) => {
                self.create_operation(packet, Some(MqttOperationOptions::Disconnect(disconnect_options)))
            }
        };

        assert_ne!(op_id, 0);

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

    fn partition_operation_queue_by_queue_policy(&self, queue: &VecDeque<u64>, policy: &OfflineQueuePolicy) -> (VecDeque<u64>, VecDeque<u64>) {
        partition_operations_by_queue_policy(queue.iter().filter(|id| {
            self.operations.get(*id).is_some()
        }).map(|id| {
            (*id, &*self.operations.get(id).unwrap().packet)
        }), policy)
    }

    fn should_retain_high_priority_operation(&self, id: u64) -> bool {
        if let Some(operation) = self.operations.get(&id) {
            if let MqttPacket::Pubrel(packet) = &*operation.packet {
                return true;
            }
        }

        false
    }

    fn partition_high_priority_queue_for_disconnect<T>(&self, iterator: T) -> (VecDeque<u64>, VecDeque<u64>) where T : Iterator<Item = u64> {
        let mut retained = VecDeque::new();
        let mut rejected = VecDeque::new();

        iterator.for_each(|id| {
            if self.should_retain_high_priority_operation(id) {
                retained.push_back(id);
            } else {
                rejected.push_back(id);
            }
        });

        (retained, rejected)
    }

    fn should_resubmit_post_disconnection(&self, id: u64) -> bool {
        if let Some(operation) = self.operations.get(&id) {
            match &*operation.packet {
                MqttPacket::Publish(_) | MqttPacket::Pubrel(_) => { return true; }
                _ => {}
            }
        }

        false
    }

    fn partition_unacked_operations_for_disconnect<T>(&self, iterator: T) -> (VecDeque<u64>, VecDeque<u64>) where T : Iterator<Item = u64> {
        let mut resubmit = VecDeque::new();
        let mut offline = VecDeque::new();

        iterator.for_each(|id| {
            if self.should_resubmit_post_disconnection(id) {
                resubmit.push_back(id);
            } else {
                offline.push_back(id);
            }
        });

        (resubmit, offline)
    }

    fn complete_operation_as_success(&mut self, id : u64, completion_result: Option<OperationResponse>) -> Mqtt5Result<()> {
        let operation_option = self.operations.remove(&id);
        if operation_option.is_none() {
            return Err(Mqtt5Error::InternalStateError);
        }

        let operation = operation_option.unwrap();
        if let Some(packet_id) = operation.packet_id {
            self.allocated_packet_ids.remove(&packet_id);
            self.pending_ack_operations.remove(&packet_id);
        }

        if operation.options.is_none() {
            return Ok(())
        }

        complete_operation_with_result(&mut operation.options.unwrap(), completion_result)
    }

    fn complete_operation_as_failure(&mut self, id : u64, error: Mqtt5Error) -> Mqtt5Result<()> {
        let operation_option = self.operations.remove(&id);
        if operation_option.is_none() {
            return Ok(())
        }

        let operation = operation_option.unwrap();
        if let Some(packet_id) = operation.packet_id {
            self.allocated_packet_ids.remove(&packet_id);
            self.pending_ack_operations.remove(&packet_id);
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
        let connect_op_id = self.create_operation(connect, None);

        self.enqueue_operation(connect_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Front)?;

        self.connack_timeout_timepoint = Some(context.current_time + Duration::from_millis(self.config.connack_timeout_millis as u64));

        Ok(())
    }

    fn handle_network_event_connection_closed(&mut self) -> Mqtt5Result<()> {
        self.state = OperationalStateType::Disconnected;

        self.connack_timeout_timepoint = None;
        self.next_ping_timepoint = None;
        self.ping_timeout_timepoint = None;

        let mut result : Mqtt5Result<()> = Ok(());
        let mut completions : VecDeque<u64> = VecDeque::new();

        /*
         * high priority operations are processed as follows:
         *
         *   puback, pingreq, pubrec, pubcomp, disconnect can all be failed without consequence
         *
         *   pubrel is moved to the resubmit queue
         */
        mem::swap(&mut completions, &mut self.high_priority_operation_queue);
        let (mut pubrels, mut failures) = self.partition_high_priority_queue_for_disconnect(completions.into_iter());

        result = fold_mqtt5_result(result, self.complete_operation_sequence_as_failure(failures.into_iter(), Mqtt5Error::ConnectionClosed));
        self.resubmit_operation_queue.append(&mut pubrels);

        /*
         * write completion pending operations can be processed immediately and either failed
         * if they fail the offline queue policy or re-queued
         */
        let mut completions : VecDeque<u64> = VecDeque::new();
        mem::swap(&mut completions, &mut self.pending_write_completion_operations);

        let (mut retained, mut rejected) = self.partition_operation_queue_by_queue_policy(&completions, &self.config.offline_queue_policy);

        /* keep the ones that pass policy (qos 0 publish under once case) */
        self.user_operation_queue.append(&mut retained);

        /* fail everything else */
        result = fold_mqtt5_result(result, self.complete_operation_sequence_as_failure(rejected.into_iter(), Mqtt5Error::ConnectionClosed));

        /*
         * unacked operations are processed as follows:
         *
         *   subscribes and unsubscribes have the offline queue policy applied.  If they fail, the
         *   operation is failed, otherwise it gets put back in the user queue
         *
         *   publish and pubrel get moved to the resubmit queue.  They'll be re-checked on the
         *   next successful connection and either have the offline queue policy applied (if no
         *   session is found) or stay in the resubmit queue.
         */
        let mut unacked_table = HashMap::new();
        mem::swap(&mut unacked_table, &mut self.pending_ack_operations);
        self.operation_ack_timeouts.clear();

        let (mut resubmit, mut offline) = self.partition_unacked_operations_for_disconnect(unacked_table.into_iter().map(|(key, val)| { val }));
        self.resubmit_operation_queue.append(&mut resubmit);

        let (mut retained_unacked, mut rejected_unacked) = self.partition_operation_queue_by_queue_policy(&offline, &self.config.offline_queue_policy);
        result = fold_mqtt5_result(result, self.complete_operation_sequence_as_failure(rejected_unacked.into_iter(), Mqtt5Error::ConnectionClosed));
        self.user_operation_queue.append(&mut retained_unacked);

        result
    }

    fn handle_network_event_write_completion(&mut self) -> Mqtt5Result<()> {
        self.pending_write_completion = false;

        let mut completions : VecDeque<u64> = VecDeque::new();
        mem::swap(&mut completions, &mut self.pending_write_completion_operations);
        let result : Mqtt5Result<()> = self.complete_operation_sequence_as_empty_success(completions.iter().copied());

        result
    }

    fn handle_network_event_incoming_data(&mut self, context: &mut NetworkEventContext, data: &[u8]) -> Mqtt5Result<()> {
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
            self.handle_packet(packet, context)?;
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

    fn get_next_ack_timeout(&mut self, now: Instant) -> Option<u64> {
        if let Some(reverse_record) = self.operation_ack_timeouts.peek() {
            let record = &reverse_record.0;
            if record.timeout <= now {
                return Some(record.id);
            }
        }

        None
    }

    fn process_ack_timeouts(&mut self, now: Instant) -> Mqtt5Result<()> {
        let mut result = Ok(());

        while let Some(id) = self.get_next_ack_timeout(now) {
            result = fold_mqtt5_result(result, self.complete_operation_as_failure(id, Mqtt5Error::AckTimeout));
        }

        result
    }

    fn get_operation_timeout_duration(&self, operation: &MqttOperation, now: Instant) -> Option<Duration> {
        match &operation.options {
            Some(MqttOperationOptions::Unsubscribe(unsubscribe_options)) => {
                if let Some(timeout) = unsubscribe_options.options.timeout_in_millis {
                    return Some(Duration::from_millis(timeout as u64));
                }
            }
            Some(MqttOperationOptions::Subscribe(subscribe_options)) => {
                if let Some(timeout) = subscribe_options.options.timeout_in_millis {
                    return Some(Duration::from_millis(timeout as u64));
                }
            }
            Some(MqttOperationOptions::Publish(publish_options)) => {
                if let Some(timeout) = publish_options.options.timeout_in_millis {
                    return Some(Duration::from_millis(timeout as u64));
                }
            }
            _ => {}
        }

        None
    }

    fn start_operation_ack_timeout(&mut self, id: u64, now: Instant) {
        let mut timeout_duration_option : Option<Duration> = None;
        if let Some(operation) = self.operations.get(&id) {
            timeout_duration_option = self.get_operation_timeout_duration(operation, now);
        }

        if let Some(operation) = self.operations.get_mut(&id) {
            if let Some(timeout_duration) = timeout_duration_option {
                let timeout = now + timeout_duration;

                let timeout_record = OperationTimeoutRecord {
                    id,
                    timeout
                };

                self.operation_ack_timeouts.push(Reverse(timeout_record));
            }
        }
    }

    fn on_current_operation_fully_written(&mut self, now: Instant) -> () {
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
                    self.pending_write_completion_operations.push_back(operation.id);
                } else {
                    self.pending_ack_operations.insert(publish.packet_id, operation.id);
                    self.pending_publish_count += 1;
                }
            }
            MqttPacket::Pubrel(pubrel) => {
                self.pending_ack_operations.insert(pubrel.packet_id, operation.id);
            }
            _ => {
                self.pending_write_completion_operations.push_back(operation.id);
            }
        }

        let id = operation.id;
        self.start_operation_ack_timeout(id, now);

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
                self.on_current_operation_fully_written(context.current_time);
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
                let ping_op_id = self.create_operation(ping, None);

                self.enqueue_operation(ping_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Front)?;

                self.ping_timeout_timepoint = Some(context.current_time + Duration::from_millis(self.config.ping_timeout_millis as u64));
            }
        }

        Ok(())
    }

    fn service_connected(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        self.service_keep_alive(context)?;

        self.service_queue(context, OperationalQueueServiceMode::All)?;

        self.process_ack_timeouts(context.current_time)?;

        Ok(())
    }

    fn service_pending_disconnect(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        self.service_queue(context, OperationalQueueServiceMode::HighPriorityOnly)?;
        self.process_ack_timeouts(context.current_time)?;

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

        if let Some(ack_timeout) = self.operation_ack_timeouts.peek() {
            next_service_time = min(next_service_time, ack_timeout.0.timeout);
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

        if let Some(ack_timeout) = self.operation_ack_timeouts.peek() {
            next_service_time = min(next_service_time, ack_timeout.0.timeout);
        }

        min(self.get_next_service_timepoint_operational_queue(current_time, OperationalQueueServiceMode::HighPriorityOnly), next_service_time)
    }

    fn schedule_ping(&mut self, current_time: &Instant) -> () {
        self.ping_timeout_timepoint = None;
        self.next_ping_timepoint = Some(*current_time + Duration::from_secs(self.current_settings.as_ref().unwrap().server_keep_alive as u64));
    }

    fn unbind_operation_packet_id(&mut self, id: u64) {
        if let Some(operation) = self.operations.get_mut(&id) {
            if let Some(packet_id) = operation.packet_id {
                self.allocated_packet_ids.remove(&packet_id);
                operation.packet_id = None;

                match &mut *operation.packet {
                    MqttPacket::Subscribe(subscribe) => {
                        subscribe.packet_id = 0;
                    }
                    MqttPacket::Unsubscribe(unsubscribe) => {
                        unsubscribe.packet_id = 0;
                    }
                    MqttPacket::Publish(publish) => {
                        publish.packet_id = 0;
                    }
                    _ => {}
                }
            }
        }
    }

    fn apply_session_present_to_connection(&mut self, session_present: bool) -> Mqtt5Result<()> {
        let mut result = Ok(());

        if !session_present {
            /*
             * No session.  Everything in the resubmit queue should be checked against the offline
             * policy and either failed or moved to the user queue.
             */
            let mut resubmit = VecDeque::new();
            std::mem::swap(&mut resubmit, &mut self.resubmit_operation_queue);

            let (mut retained, mut rejected) = self.partition_operation_queue_by_queue_policy(&resubmit, &self.config.offline_queue_policy);

            /* keep the ones that pass policy */
            self.user_operation_queue.append(&mut retained);

            /* fail everything else */
            result = self.complete_operation_sequence_as_failure(rejected.into_iter(), Mqtt5Error::OfflineQueuePolicyFailed);

            self.qos2_incomplete_incoming_publishes.clear();
            self.allocated_packet_ids.clear();
        }

        let mut user_queue = VecDeque::new();
        std::mem::swap(&mut user_queue, &mut self.user_operation_queue);
        user_queue.iter().for_each(|id| { self.unbind_operation_packet_id(*id)});
        self.user_operation_queue = user_queue;

        sort_operation_deque(&mut self.resubmit_operation_queue);
        sort_operation_deque(&mut self.user_operation_queue);

        assert!(self.high_priority_operation_queue.is_empty());
        assert!(self.pending_ack_operations.is_empty());
        assert!(self.operation_ack_timeouts.is_empty());
        assert!(self.pending_write_completion_operations.is_empty());

        result
    }

    fn handle_connack(&mut self, packet: Box<MqttPacket>, context: &mut NetworkEventContext) -> Mqtt5Result<()> {
        if let MqttPacket::Connack(connack) = &*packet {
            if self.state != OperationalStateType::PendingConnack {
                return Err(Mqtt5Error::ProtocolError);
            }

            if connack.reason_code != ConnectReasonCode::Success {
                context.connection_packets.push_back(packet);
                return Err(Mqtt5Error::ConnectionRejected);
            }

            validate_connack_packet_inbound_internal(connack)?;

            self.state = OperationalStateType::Connected;
            self.current_settings = Some(build_negotiated_settings(&self.config, connack, &self.current_settings));
            self.connack_timeout_timepoint = None;
            self.outbound_alias_resolver.borrow_mut().reset_for_new_connection(connack.topic_alias_maximum.unwrap_or(0));
            self.inbound_alias_resolver.reset_for_new_connection();

            self.schedule_ping(&context.current_time);

            self.apply_session_present_to_connection(connack.session_present)?;

            context.connection_packets.push_back(packet);
            return Ok(());
        }

        Err(Mqtt5Error::InternalStateError)
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

    fn handle_suback(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Suback(suback) = *packet {
            let packet_id = suback.packet_id;
            let operation_id_option = self.pending_ack_operations.get(&packet_id);
            if let Some(operation_id) = operation_id_option {
                return self.complete_operation_as_success(*operation_id, Some(OperationResponse::Subscribe(suback)));
            }
        }

        // No record of the packet id or associated operation wasn't a Subscribe
        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_unsuback(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Unsuback(unsuback) = *packet {
            let packet_id = unsuback.packet_id;
            let operation_id_option = self.pending_ack_operations.get(&packet_id);
            if let Some(operation_id) = operation_id_option {
                return self.complete_operation_as_success(*operation_id, Some(OperationResponse::Unsubscribe(unsuback)));
            }
        }

        // No record of the packet id or associated operation wasn't an Unsubscribe
        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_puback(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Puback(puback) = *packet {
            let packet_id = puback.packet_id;
            let operation_id_option = self.pending_ack_operations.get(&packet_id);
            if let Some(operation_id) = operation_id_option {
                return self.complete_operation_as_success(*operation_id, Some(OperationResponse::Publish(PublishResponse::Qos1(puback))));
            }
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_pubrec(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Pubrec(pubrec) = *packet {
            let packet_id = pubrec.packet_id;
            let operation_id_option = self.pending_ack_operations.get(&packet_id);
            if let Some(operation_id) = operation_id_option {
                if pubrec.reason_code as u8 >= 128 {
                    return self.complete_operation_as_success(*operation_id, Some(OperationResponse::Publish(PublishResponse::Qos2(Qos2Response::Pubrec(pubrec)))));
                } else {
                    let operation_option = self.operations.get_mut(operation_id);
                    if let Some(mut operation) = operation_option {
                        if let MqttPacket::Publish(publish) = &*operation.packet {
                            if publish.qos == QualityOfService::ExactlyOnce {
                                operation.packet = Box::new(MqttPacket::Pubrel(PubrelPacket {
                                    packet_id: pubrec.packet_id,
                                    ..Default::default()
                                }));

                                self.enqueue_operation(*operation_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Back)?;

                                return Ok(());
                            }
                        }
                    }
                }
            }

            return Err(Mqtt5Error::ProtocolError);
        }

        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_pubrel(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Pubrel(pubrel) = &*packet {
            self.qos2_incomplete_incoming_publishes.remove(&pubrel.packet_id);

            let pubcomp = Box::new(MqttPacket::Pubcomp(PubcompPacket{
                packet_id: pubrel.packet_id,
                ..Default::default()
            }));
            let pubcomp_op_id = self.create_operation(pubcomp, None);

            self.enqueue_operation(pubcomp_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Back)?;

            return Ok(());
        }

        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_pubcomp(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Pubcomp(pubcomp) = *packet {
            let packet_id = pubcomp.packet_id;
            let operation_id_option = self.pending_ack_operations.get(&packet_id);
            if let Some(operation_id) = operation_id_option {
                return self.complete_operation_as_success(*operation_id, Some(OperationResponse::Publish(PublishResponse::Qos2(Qos2Response::Pubcomp(pubcomp)))));
            }
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn publish_received_callback_stub(&self, publish: Arc<PublishPacket>) {
        // TODO, possibly wait until we refactor MqttPacket again =/
    }

    fn handle_publish(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Publish(publish) = *packet {
            let arc_publish = Arc::new(publish);
            let packet_id = arc_publish.packet_id;
            let qos = arc_publish.qos;
            match qos {
                QualityOfService::AtMostOnce => {
                    self.publish_received_callback_stub(arc_publish);
                    return Ok(());
                }

                QualityOfService::AtLeastOnce => {
                    self.publish_received_callback_stub(arc_publish);

                    let puback = Box::new(MqttPacket::Puback(PubackPacket{
                        packet_id,
                        ..Default::default()
                    }));
                    let puback_op_id = self.create_operation(puback, None);

                    self.enqueue_operation(puback_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Back)?;

                    return Ok(());
                }

                QualityOfService::ExactlyOnce => {
                    if !self.qos2_incomplete_incoming_publishes.contains(&packet_id) {
                        self.publish_received_callback_stub(arc_publish);
                        self.qos2_incomplete_incoming_publishes.insert(packet_id);
                    }

                    let pubrec = Box::new(MqttPacket::Pubrec(PubrecPacket{
                        packet_id,
                        ..Default::default()
                    }));
                    let pubrec_op_id = self.create_operation(pubrec, None);

                    self.enqueue_operation(pubrec_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Back)?;

                    return Ok(());
                }
            }
        }

        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_disconnect(&mut self, packet: Box<MqttPacket>, context: &mut NetworkEventContext) -> Mqtt5Result<()> {
        if let MqttPacket::Disconnect(disconnect) = &*packet {
            context.connection_packets.push_back(packet);

            return Err(Mqtt5Error::ServerSideDisconnect);
        }

        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_packet(&mut self, packet: Box<MqttPacket>, context: &mut NetworkEventContext) -> Mqtt5Result<()> {
        match &*packet {
            MqttPacket::Connack(_) => { self.handle_connack(packet, context) }
            MqttPacket::Publish(_) => { self.handle_publish(packet) }
            MqttPacket::Pingresp(_) => { self.handle_pingresp() }
            MqttPacket::Disconnect(_) => { self.handle_disconnect(packet, context) }
            MqttPacket::Suback(_) => { self.handle_suback(packet) }
            MqttPacket::Unsuback(_) => { self.handle_unsuback(packet) }
            MqttPacket::Puback(_) => { self.handle_puback(packet) }
            MqttPacket::Pubcomp(_) => { self.handle_pubcomp(packet) }
            MqttPacket::Pubrel(_) => { self.handle_pubrel(packet) }
            MqttPacket::Pubrec(_) => { self.handle_pubrec(packet) }
            MqttPacket::Auth(_) => { Err(Mqtt5Error::Unimplemented) }
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

    fn create_operation(&mut self, packet: Box<MqttPacket>, options: Option<MqttOperationOptions>) -> u64 {
        let id = self.next_operation_id;
        self.next_operation_id += 1;

        let operation = MqttOperation {
            id,
            packet,
            packet_id: None,
            options
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

fn complete_operation_with_result(operation_options: &mut MqttOperationOptions, completion_result: Option<OperationResponse>) -> Mqtt5Result<()> {
    match operation_options {
        MqttOperationOptions::Publish(publish_options) => {
            if let OperationResponse::Publish(publish_result) = completion_result.unwrap() {
                let sender = publish_options.response_sender.take().unwrap();
                if sender.send(Ok(publish_result)).is_err() {
                    return Err(Mqtt5Error::OperationChannelSendError);
                }

                return Ok(());
            }
        }
        MqttOperationOptions::Subscribe(subscribe_options) => {
            if let OperationResponse::Subscribe(suback) = completion_result.unwrap() {
                let sender = subscribe_options.response_sender.take().unwrap();
                if sender.send(Ok(suback)).is_err() {
                    return Err(Mqtt5Error::OperationChannelSendError);
                }

                return Ok(());
            }
        }
        MqttOperationOptions::Unsubscribe(unsubscribe_options) => {
            if let OperationResponse::Unsubscribe(unsuback) = completion_result.unwrap() {
                let sender = unsubscribe_options.response_sender.take().unwrap();
                if sender.send(Ok(unsuback)).is_err() {
                    return Err(Mqtt5Error::OperationChannelSendError);
                }
                return Ok(());
            }
        }
        MqttOperationOptions::Disconnect(disconnect_options) => {
            let sender = disconnect_options.response_sender.take().unwrap();
            if sender.send(Ok(())).is_err() {
                return Err(Mqtt5Error::OperationChannelSendError);
            }

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
                _ => { true }
            }
        }
        MqttPacket::Publish(publish) => {
            match policy {
                OfflineQueuePolicy::PreserveNothing => { false }
                OfflineQueuePolicy::PreserveQos1PlusPublishes | OfflineQueuePolicy::PreserveAcknowledged => {
                    publish.qos != QualityOfService::AtMostOnce
                }
                _ => { true }
            }
        }
        _ => { false }
    }
}

fn partition_operations_by_queue_policy<'a, T>(iterator: T, policy: &OfflineQueuePolicy) -> (VecDeque<u64>, VecDeque<u64>) where T : Iterator<Item = (u64, &'a MqttPacket)> {
    let mut retained : VecDeque<u64> = VecDeque::new();
    let mut filtered : VecDeque<u64> = VecDeque::new();

    iterator.for_each(|(id, packet)| {
        if does_packet_pass_offline_queue_policy(packet, policy) {
            retained.push_back(id);
        } else {
            filtered.push_back(id);
        }
    });

    (retained, filtered)
}

fn sort_operation_deque(operations: &mut VecDeque<u64>) {
    operations.rotate_right(operations.as_slices().1.len());
    operations.as_mut_slices().0.sort();
}