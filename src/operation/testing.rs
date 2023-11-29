/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate tokio;

use tokio::sync::oneshot;

#[cfg(test)]
mod operational_state_tests {
    use super::*;
    use crate::operation::*;

    fn build_standard_test_config() -> OperationalStateConfig {
        OperationalStateConfig {
            connect : Box::new(ConnectPacket{
                client_id: Some("DefaultTesting".to_string()),
                ..Default::default()
            }),
            base_timestamp: Instant::now(),
            offline_queue_policy: OfflineQueuePolicy::PreserveAll,
            rejoin_session_policy: RejoinSessionPolicy::RejoinAlways,
            connack_timeout_millis: 10000,
            ping_timeout_millis: 30000,
            outbound_resolver: None,
        }
    }

    type PacketHandler = Box<dyn Fn(&Box<MqttPacket>, &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> + 'static>;

    fn handle_connect_with_successful_connack(packet: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        if let MqttPacket::Connect(connect) = &**packet {
            let mut assigned_client_identifier = None;
            if connect.client_id.is_none() {
                assigned_client_identifier = Some("auto-assigned-client-id".to_string());
            }

            let response = Box::new(MqttPacket::Connack(ConnackPacket {
                assigned_client_identifier,
                ..Default::default()
            }));
            response_packets.push_back(response);

            return Ok(());
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn create_connack_rejection() -> ConnackPacket {
        ConnackPacket {
            reason_code : ConnectReasonCode::Banned,
            ..Default::default()
        }
    }

    fn handle_connect_with_failure_connack(packet: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        if let MqttPacket::Connect(_) = &**packet {
            let response = Box::new(MqttPacket::Connack(create_connack_rejection()));
            response_packets.push_back(response);

            return Ok(());
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_connect_with_tiny_maximum_packet_size(packet: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        if let MqttPacket::Connect(_) = &**packet {
            let response = Box::new(MqttPacket::Connack(ConnackPacket {
                maximum_packet_size: Some(10),
                ..Default::default()
            }));
            response_packets.push_back(response);

            return Ok(());
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_pingreq_with_pingresp(_: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        let response = Box::new(MqttPacket::Pingresp(PingrespPacket{}));
        response_packets.push_back(response);

        Ok(())
    }

    fn handle_publish_with_success_no_relay(packet: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        if let MqttPacket::Publish(publish) = &**packet {
            match publish.qos {
                QualityOfService::AtMostOnce => {}
                QualityOfService::AtLeastOnce => {
                    let response = Box::new(MqttPacket::Puback(PubackPacket{
                        packet_id : publish.packet_id,
                        ..Default::default()
                    }));
                    response_packets.push_back(response);
                }
                QualityOfService::ExactlyOnce => {
                    let response = Box::new(MqttPacket::Pubrec(PubrecPacket{
                        packet_id : publish.packet_id,
                        ..Default::default()
                    }));
                    response_packets.push_back(response);
                }
            }

            return Ok(());
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_pubrec_with_success(packet: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        if let MqttPacket::Pubrec(pubrec) = &**packet {
            let response = Box::new(MqttPacket::Pubrel(PubrelPacket{
                packet_id : pubrec.packet_id,
                ..Default::default()
            }));
            response_packets.push_back(response);

            return Ok(());
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_pubrel_with_success(packet: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        if let MqttPacket::Pubrel(pubrel) = &**packet {
            let response = Box::new(MqttPacket::Pubcomp(PubcompPacket{
                packet_id : pubrel.packet_id,
                ..Default::default()
            }));
            response_packets.push_back(response);

            return Ok(());
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_subscribe_with_success(packet: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        if let MqttPacket::Subscribe(subscribe) = &**packet {
            let mut reason_codes = Vec::new();
            for subscription in &subscribe.subscriptions {
                match subscription.qos {
                    QualityOfService::AtMostOnce => { reason_codes.push(SubackReasonCode::GrantedQos0); }
                    QualityOfService::AtLeastOnce => { reason_codes.push(SubackReasonCode::GrantedQos1); }
                    QualityOfService::ExactlyOnce => { reason_codes.push(SubackReasonCode::GrantedQos2); }
                }
            }

            let response = Box::new(MqttPacket::Suback(SubackPacket{
                packet_id : subscribe.packet_id,
                reason_codes,
                ..Default::default()
            }));
            response_packets.push_back(response);

            return Ok(());
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_subscribe_with_failure(packet: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        if let MqttPacket::Subscribe(subscribe) = &**packet {
            let mut reason_codes = Vec::new();
            for subscription in &subscribe.subscriptions {
                reason_codes.push(SubackReasonCode::NotAuthorized);
            }

            let response = Box::new(MqttPacket::Suback(SubackPacket{
                packet_id : subscribe.packet_id,
                reason_codes,
                ..Default::default()
            }));
            response_packets.push_back(response);

            return Ok(());
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_unsubscribe_with_success(packet: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        if let MqttPacket::Unsubscribe(unsubscribe) = &**packet {
            let mut reason_codes = Vec::new();
            for _ in &unsubscribe.topic_filters {
                reason_codes.push(UnsubackReasonCode::Success);
            }

            let response = Box::new(MqttPacket::Unsuback(UnsubackPacket{
                packet_id : unsubscribe.packet_id,
                reason_codes,
                ..Default::default()
            }));
            response_packets.push_back(response);

            return Ok(());
        }

        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_with_protocol_error(_: &Box<MqttPacket>, _: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_with_nothing(_: &Box<MqttPacket>, _: &mut VecDeque<Box<MqttPacket>>) -> Mqtt5Result<()> {
        Ok(())
    }

    fn create_default_packet_handlers() -> HashMap<PacketType, PacketHandler> {
        let mut handlers : HashMap<PacketType, PacketHandler> = HashMap::new();

        handlers.insert(PacketType::Connect, Box::new(handle_connect_with_successful_connack));
        handlers.insert(PacketType::Pingreq, Box::new(handle_pingreq_with_pingresp));
        handlers.insert(PacketType::Publish, Box::new(handle_publish_with_success_no_relay));
        handlers.insert(PacketType::Pubrec, Box::new(handle_pubrec_with_success));
        handlers.insert(PacketType::Pubrel, Box::new(handle_pubrel_with_success));
        handlers.insert(PacketType::Subscribe, Box::new(handle_subscribe_with_success));
        handlers.insert(PacketType::Unsubscribe, Box::new(handle_unsubscribe_with_success));

        handlers.insert(PacketType::Disconnect, Box::new(handle_with_nothing));
        handlers.insert(PacketType::Auth, Box::new(handle_with_nothing));
        handlers.insert(PacketType::Puback, Box::new(handle_with_nothing));
        handlers.insert(PacketType::Pubcomp, Box::new(handle_with_nothing));

        handlers.insert(PacketType::Connack, Box::new(handle_with_protocol_error));
        handlers.insert(PacketType::Suback, Box::new(handle_with_protocol_error));
        handlers.insert(PacketType::Unsuback, Box::new(handle_with_protocol_error));
        handlers.insert(PacketType::Pingresp, Box::new(handle_with_protocol_error));

        handlers
    }

    struct OperationalStateTestFixture {
        base_timestamp: Instant,

        broker_decoder: Decoder,
        broker_encoder: Encoder,

        pub client_state: OperationalState,

        pub client_event_stream: VecDeque<Arc<ClientEvent>>,

        pub to_broker_packet_stream: VecDeque<Box<MqttPacket>>,
        pub to_client_packet_stream: VecDeque<Box<MqttPacket>>,

        pub broker_packet_handlers: HashMap<PacketType, PacketHandler>,
    }

    impl OperationalStateTestFixture {


        pub(crate) fn new(config : OperationalStateConfig) -> Self {
            Self {
                base_timestamp : config.base_timestamp.clone(),
                broker_decoder: Decoder::new(),
                broker_encoder: Encoder::new(),
                client_state: OperationalState::new(config),
                client_event_stream : VecDeque::new(),
                to_broker_packet_stream : VecDeque::new(),
                to_client_packet_stream : VecDeque::new(),
                broker_packet_handlers : create_default_packet_handlers(),
            }
        }

        fn handle_to_broker_packet(&mut self, packet: &Box<MqttPacket>, response_bytes: &mut Vec<u8>) -> Mqtt5Result<()> {
            let mut response_packets = VecDeque::new();
            let packet_type = mqtt_packet_to_packet_type(&*packet);

            if let Some(handler) = self.broker_packet_handlers.get(&packet_type) {
                (*handler)(packet, &mut response_packets)?;

                let mut encode_buffer = Vec::with_capacity(4096);

                for response_packet in &response_packets {
                    let encoding_context = EncodingContext {
                        outbound_alias_resolution: OutboundAliasResolution {
                            skip_topic: false,
                            alias: None,
                        }
                    };

                    self.broker_encoder.reset(&*response_packet, &encoding_context)?;

                    let mut encode_result = EncodeResult::Full;
                    while encode_result == EncodeResult::Full {
                        encode_result = self.broker_encoder.encode(&*response_packet, &mut encode_buffer)?;
                        response_bytes.append(&mut encode_buffer);
                        encode_buffer.clear(); // redundant probably
                    }
                }

                self.to_client_packet_stream.append(&mut response_packets);
            }

            Ok(())
        }

        pub(crate) fn service_once(&mut self, elapsed_millis: u64) -> Mqtt5Result<()> {
            let current_time = self.base_timestamp + Duration::from_millis(elapsed_millis);

            let mut to_socket = Vec::with_capacity(4096);

            let mut service_context = ServiceContext {
                to_socket: &mut to_socket,
                current_time,
            };

            self.client_state.service(&mut service_context)
        }

        pub(crate) fn service_with_drain(&mut self, elapsed_millis: u64) -> Mqtt5Result<Vec<u8>> {
            let current_time = self.base_timestamp + Duration::from_millis(elapsed_millis);
            let mut done = false;
            let mut response_bytes = Vec::new();

            while !done {
                let mut to_socket = Vec::with_capacity(4096);
                let mut broker_packets = VecDeque::new();

                let mut service_context = ServiceContext {
                    to_socket: &mut to_socket,
                    current_time,
                };

                self.client_state.service(&mut service_context)?;
                if to_socket.len() > 0 {
                    let mut network_event = NetworkEventContext {
                        event: NetworkEvent::WriteCompletion,
                        current_time,
                        client_events: &mut self.client_event_stream,
                    };

                    let completion_result = self.client_state.handle_network_event(&mut network_event);
                    let mut maximum_packet_size_to_server : u32 = MAXIMUM_VARIABLE_LENGTH_INTEGER as u32;
                    if let Some(settings) = self.client_state.get_negotiated_settings() {
                        maximum_packet_size_to_server = settings.maximum_packet_size_to_server;
                    }

                    let mut decode_context = DecodingContext {
                        maximum_packet_size : maximum_packet_size_to_server,
                        decoded_packets: &mut broker_packets,
                    };

                    if self.broker_decoder.decode_bytes(to_socket.as_slice(), &mut decode_context).is_err() {
                        panic!("Test triggered broker decode failure");
                    }

                    for packet in &broker_packets {
                        if self.handle_to_broker_packet(packet, &mut response_bytes).is_err() {
                            panic!("Test triggered broker packet handling failure");
                        }
                    }

                    self.to_broker_packet_stream.append(&mut broker_packets);

                    if let Err(error) = completion_result {
                        return Err(error);
                    }
                } else {
                    done = true;
                }
            }

            Ok(response_bytes)
        }

        pub(crate) fn service_round_trip(&mut self, service_time: u64, response_time: u64) -> Mqtt5Result<()> {
            let server_bytes = self.service_with_drain(service_time)?;

            self.on_incoming_bytes(response_time, server_bytes.as_slice())?;

            Ok(())
        }

        pub(crate) fn on_connection_opened(&mut self, elapsed_millis: u64) -> Mqtt5Result<()> {
            let mut context = NetworkEventContext {
                current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
                event: NetworkEvent::ConnectionOpened,
                client_events: &mut self.client_event_stream,
            };

            self.client_state.handle_network_event(&mut context)
        }

        pub(crate) fn on_write_completion(&mut self, elapsed_millis: u64) -> Mqtt5Result<()> {
            let mut context = NetworkEventContext {
                current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
                event: NetworkEvent::WriteCompletion,
                client_events: &mut self.client_event_stream,
            };

            self.client_state.handle_network_event(&mut context)
        }

        pub(crate) fn on_connection_closed(&mut self, elapsed_millis: u64) -> Mqtt5Result<()> {
            let mut context = NetworkEventContext {
                current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
                event: NetworkEvent::ConnectionClosed,
                client_events: &mut self.client_event_stream,
            };

            self.client_state.handle_network_event(&mut context)
        }

        pub(crate) fn on_incoming_bytes(&mut self, elapsed_millis: u64, bytes: &[u8]) -> Mqtt5Result<()> {
            let mut context = NetworkEventContext {
                current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
                event: NetworkEvent::IncomingData(bytes),
                client_events: &mut self.client_event_stream,
            };

            self.client_state.handle_network_event(&mut context)
        }

        pub(crate) fn get_next_service_time(&mut self, elapsed_millis: u64) -> Option<u64> {
            let current_time = self.base_timestamp + Duration::from_millis(elapsed_millis);
            let next_service_timepoint = self.client_state.get_next_service_timepoint(&current_time);

            if let Some(service_timepoint) = &next_service_timepoint {
                let next_service_millis = (*service_timepoint - self.base_timestamp).as_millis();
                return Some(next_service_millis as u64);
            }

            None
        }

        pub(crate) fn subscribe(&mut self, elapsed_millis: u64, subscribe: SubscribePacket, options: SubscribeOptions) -> Mqtt5Result<oneshot::Receiver<SubscribeResult>> {
            let (sender, receiver) = oneshot::channel();
            let packet = Box::new(MqttPacket::Subscribe(subscribe));
            let subscribe_options = SubscribeOptionsInternal {
                options,
                response_sender : Some(sender)
            };

            let subscribe_event = UserEvent::Subscribe(packet, subscribe_options);

            self.client_state.handle_user_event(UserEventContext {
                event: subscribe_event,
                current_time: self.base_timestamp + Duration::from_millis(elapsed_millis)
            })?;

            Ok(receiver)
        }

        pub(crate) fn unsubscribe(&mut self, elapsed_millis: u64, unsubscribe: UnsubscribePacket, options: UnsubscribeOptions) -> Mqtt5Result<oneshot::Receiver<UnsubscribeResult>> {
            let (sender, receiver) = oneshot::channel();
            let packet = Box::new(MqttPacket::Unsubscribe(unsubscribe));
            let unsubscribe_options = UnsubscribeOptionsInternal {
                options,
                response_sender : Some(sender)
            };

            let unsubscribe_event = UserEvent::Unsubscribe(packet, unsubscribe_options);

            self.client_state.handle_user_event(UserEventContext {
                event: unsubscribe_event,
                current_time: self.base_timestamp + Duration::from_millis(elapsed_millis)
            })?;

            Ok(receiver)
        }

        pub(crate) fn publish(&mut self, elapsed_millis: u64, publish: PublishPacket, options: PublishOptions) -> Mqtt5Result<oneshot::Receiver<PublishResult>> {
            let (sender, receiver) = oneshot::channel();
            let packet = Box::new(MqttPacket::Publish(publish));
            let publish_options = PublishOptionsInternal {
                options,
                response_sender : Some(sender)
            };

            let publish_event = UserEvent::Publish(packet, publish_options);

            self.client_state.handle_user_event(UserEventContext {
                event: publish_event,
                current_time: self.base_timestamp + Duration::from_millis(elapsed_millis)
            })?;

            Ok(receiver)
        }

        pub(crate) fn disconnect(&mut self, elapsed_millis: u64, disconnect: DisconnectPacket, options: DisconnectOptions) -> Mqtt5Result<oneshot::Receiver<DisconnectResult>> {
            let (sender, receiver) = oneshot::channel();
            let packet = Box::new(MqttPacket::Disconnect(disconnect));
            let disconnect_options = DisconnectOptionsInternal {
                options,
                response_sender : Some(sender)
            };

            let disconnect_event = UserEvent::Disconnect(packet, disconnect_options);

            self.client_state.handle_user_event(UserEventContext {
                event: disconnect_event,
                current_time: self.base_timestamp + Duration::from_millis(elapsed_millis)
            })?;

            Ok(receiver)
        }

        pub(crate) fn advance_disconnected_to_state(&mut self, state: OperationalStateType, elapsed_millis: u64) -> Mqtt5Result<()> {
            assert_eq!(OperationalStateType::Disconnected, self.client_state.state);

            let result = match state {
                OperationalStateType::PendingConnack => {
                    self.on_connection_opened(elapsed_millis)
                }
                OperationalStateType::Connected => {
                    self.on_connection_opened(elapsed_millis)?;
                    let server_bytes = self.service_with_drain(elapsed_millis)?;
                    self.on_incoming_bytes(elapsed_millis, server_bytes.as_slice())
                }
                OperationalStateType::PendingDisconnect => {
                    panic!("Not supported");
                }
                OperationalStateType::Halted => {
                    self.on_connection_opened(elapsed_millis)?;
                    self.on_connection_opened(elapsed_millis).unwrap_or(());
                    Ok(())
                }
                OperationalStateType::Disconnected => { Ok(()) }
            };

            assert_eq!(state, self.client_state.state);

            result
        }
    }

    fn find_nth_packet_of_type<'a, T>(packet_sequence : T, packet_type : PacketType, count: usize, start_position : Option<usize>, end_position : Option<usize>) -> Option<(usize, &'a Box<MqttPacket>)> where T : Iterator<Item = &'a Box<MqttPacket>> {
        let start = start_position.unwrap_or(0);
        let mut index = start;
        let mut seen = 0;

        for packet in packet_sequence.skip(start) {
            if mqtt_packet_to_packet_type(&*packet) == packet_type {
                seen += 1;
                if seen == count {
                    return Some((index, packet));
                }
            }

            index += 1;
            if let Some(end) = end_position {
                if index >= end {
                    return None;
                }
            }
        }

        None
    }

    fn verify_packet_type_sequence<'a, T, U>(packet_sequence : T, expected_sequence : U, start_position : Option<usize>) where T : Iterator<Item = &'a Box<MqttPacket>>, U : Iterator<Item = PacketType> {
        let start = start_position.unwrap_or(0);
        let type_sequence = packet_sequence.skip(start).map(|packet|{ mqtt_packet_to_packet_type(packet) });

        assert!(expected_sequence.eq(type_sequence));
    }

    fn verify_packet_sequence<'a, T>(packet_sequence : T, expected_sequence : T, start_position : Option<usize>) where T : Iterator<Item = &'a Box<MqttPacket>> {
        let start = start_position.unwrap_or(0);
        assert!(expected_sequence.eq(packet_sequence.skip(start)));
    }

    fn find_nth_client_event_of_type<'a, T>(client_event_sequence : T, event_type : ClientEventType, count: usize, start_position : Option<usize>, end_position : Option<usize>) -> Option<(usize, &'a Arc<ClientEvent>)> where T : Iterator<Item = &'a Arc<ClientEvent>> {
        let start = start_position.unwrap_or(0);
        let mut index = start;
        let mut seen = 0;

        for event in client_event_sequence.skip(start) {
            if client_event_to_client_event_type(&**event) == event_type {
                seen += 1;
                if seen == count {
                    return Some((index, event));
                }
            }

            index += 1;
            if let Some(end) = end_position {
                if index >= end {
                    return None;
                }
            }
        }

        None
    }

    fn verify_client_event_type_sequence<'a, T, U>(client_event_sequence : T, expected_sequence : U, start_position : Option<usize>) where T : Iterator<Item = &'a Arc<ClientEvent>>, U : Iterator<Item = ClientEventType> {
        let start = start_position.unwrap_or(0);
        let type_sequence = client_event_sequence.skip(start).map(|event|{ client_event_to_client_event_type(event) });

        assert!(expected_sequence.eq(type_sequence));
    }

    fn verify_client_event_sequence<'a, T>(client_event_sequence : T, expected_sequence : T, start_position : Option<usize>) where T : Iterator<Item = &'a Arc<ClientEventType>> {
        let start = start_position.unwrap_or(0);
        assert!(expected_sequence.eq(client_event_sequence.skip(start)));
    }

    fn verify_operational_state_empty(fixture: &OperationalStateTestFixture) {
        assert_eq!(0, fixture.client_state.operations.len());
        assert_eq!(0, fixture.client_state.user_operation_queue.len());
        assert_eq!(0, fixture.client_state.resubmit_operation_queue.len());
        assert_eq!(0, fixture.client_state.high_priority_operation_queue.len());
        assert_eq!(0, fixture.client_state.operation_ack_timeouts.len());
        assert_eq!(0, fixture.client_state.allocated_packet_ids.len());
        assert_eq!(0, fixture.client_state.pending_ack_operations.len());
        assert_eq!(0, fixture.client_state.pending_write_completion_operations.len());
        assert_eq!(0, fixture.client_state.pending_publish_count);
    }

    #[test]
    fn disconnected_state_network_event_handler_fails() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(OperationalStateType::Disconnected, fixture.client_state.state);

        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_connection_closed(0).err().unwrap());
        assert!(fixture.client_event_stream.is_empty());

        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_write_completion(0).err().unwrap());
        assert!(fixture.client_event_stream.is_empty());

        let bytes : Vec<u8> = vec!(0, 1, 2, 3, 4, 5);
        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_incoming_bytes(0, bytes.as_slice()).err().unwrap());
        assert!(fixture.client_event_stream.is_empty());
    }

    #[test]
    fn disconnected_state_next_service_time_never() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(OperationalStateType::Disconnected, fixture.client_state.state);

        assert_eq!(None, fixture.get_next_service_time(0));
    }

    fn verify_service_does_nothing(fixture : &mut OperationalStateTestFixture) {
        let client_event_stream_length = fixture.client_event_stream.len();
        let to_broker_packet_stream_length = fixture.to_broker_packet_stream.len();
        let to_client_packet_stream_length = fixture.to_client_packet_stream.len();

        let publish_receiver = fixture.publish(0, PublishPacket {
            topic: "derp".to_string(),
            qos: QualityOfService::AtLeastOnce,
            ..Default::default()
        }, PublishOptions {
            timeout_in_millis: None
        });
        assert!(publish_receiver.is_ok());
        assert!(fixture.client_state.operations.len() > 0);
        assert!(fixture.client_state.user_operation_queue.len() > 0);

        if let Ok(bytes) = fixture.service_with_drain(0) {
            assert_eq!(0, bytes.len());
        }

        assert_eq!(client_event_stream_length, fixture.client_event_stream.len());
        assert_eq!(to_broker_packet_stream_length, fixture.to_broker_packet_stream.len());
        assert_eq!(to_client_packet_stream_length, fixture.to_client_packet_stream.len());
    }

    #[test]
    fn disconnected_state_service_does_nothing() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(OperationalStateType::Disconnected, fixture.client_state.state);

        verify_service_does_nothing(&mut fixture);
    }

    #[test]
    fn halted_state_network_event_handler_fails() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Halted, 0));

        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_connection_opened(0).err().unwrap());
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        assert!(fixture.client_event_stream.is_empty());

        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_write_completion(0).err().unwrap());
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        assert!(fixture.client_event_stream.is_empty());

        let bytes : Vec<u8> = vec!(0, 1, 2, 3, 4, 5);
        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_incoming_bytes(0, bytes.as_slice()).err().unwrap());
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        assert!(fixture.client_event_stream.is_empty());
    }

    #[test]
    fn halted_state_next_service_time_never() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Halted, 0));

        assert_eq!(None, fixture.get_next_service_time(0));
    }

    #[test]
    fn halted_state_service_does_nothing() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Halted, 0));

        verify_service_does_nothing(&mut fixture);
    }

    #[test]
    fn halted_state_transition_out_on_connection_closed() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Halted, 0));

        assert_eq!(Ok(()), fixture.on_connection_closed(0));
        assert_eq!(OperationalStateType::Disconnected, fixture.client_state.state);
    }

    #[test]
    fn pending_connack_state_network_event_connection_opened_fails() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.on_connection_opened(0));
        assert_eq!(OperationalStateType::PendingConnack, fixture.client_state.state);

        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_connection_opened(0).err().unwrap());
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        assert!(fixture.client_event_stream.is_empty());
    }

    #[test]
    fn pending_connack_state_network_event_write_completion_fails() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.on_connection_opened(0));
        assert_eq!(OperationalStateType::PendingConnack, fixture.client_state.state);

        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_write_completion(0).err().unwrap());
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        assert!(fixture.client_event_stream.is_empty());
    }

    #[test]
    fn pending_connack_state_connack_timeout() {
        let config = build_standard_test_config();
        let connack_timeout_millis = config.connack_timeout_millis;

        let mut fixture = OperationalStateTestFixture::new(config);
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::PendingConnack, 1));

        assert_eq!(Some(1), fixture.get_next_service_time(1));

        let service_result = fixture.service_with_drain(1);
        assert!(service_result.is_ok());

        assert_eq!(Some(1 + connack_timeout_millis as u64), fixture.get_next_service_time(1));

        // service post-timeout
        assert_eq!(Err(Mqtt5Error::ConnackTimeout), fixture.service_with_drain(1 + connack_timeout_millis as u64));
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        assert!(fixture.client_event_stream.is_empty());
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn pending_connack_state_failure_connack() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        fixture.broker_packet_handlers.insert(PacketType::Connect, Box::new(handle_connect_with_failure_connack));

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::PendingConnack, 0));

        let server_bytes = fixture.service_with_drain(0).unwrap();

        assert_eq!(Err(Mqtt5Error::ConnectionRejected), fixture.on_incoming_bytes(0, server_bytes.as_slice()));
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);

        // connack rejection is the only time we generate a connection failure event, everything else
        // is the responsibility of the caller
        let expected_events = vec!(ClientEvent::ConnectionFailure(ConnectionFailureEvent{
            error: Mqtt5Error::ConnectionRejected,
            connack: Some(create_connack_rejection())
        }));
        assert!(expected_events.iter().eq(fixture.client_event_stream.iter().map(|event| { &**event })));
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn pending_connack_state_connection_closed() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::PendingConnack, 0));

        let _ = fixture.service_with_drain(0).unwrap();

        assert_eq!(Ok(()), fixture.on_connection_closed(0));
        assert_eq!(OperationalStateType::Disconnected, fixture.client_state.state);
        assert!(fixture.client_event_stream.is_empty());
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn pending_connack_state_incoming_garbage_data() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::PendingConnack, 0));

        let mut server_bytes = fixture.service_with_drain(0).unwrap();
        server_bytes.clear();
        let mut garbage = vec!(1, 2, 3, 4, 5, 6, 7, 8);
        server_bytes.append(&mut garbage);

        assert_eq!(Err(Mqtt5Error::MalformedPacket), fixture.on_incoming_bytes(0, server_bytes.as_slice()));
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        assert!(fixture.client_event_stream.is_empty());
        verify_operational_state_empty(&fixture);
    }

    fn encode_packet_to_buffer(packet: MqttPacket, buffer: &mut Vec<u8>) -> Mqtt5Result<()> {
        let mut encode_buffer = Vec::with_capacity(4096);
        let encoding_context = EncodingContext {
            outbound_alias_resolution: OutboundAliasResolution {
                skip_topic: false,
                alias: None,
            }
        };

        let mut encoder = Encoder::new();
        encoder.reset(&packet, &encoding_context)?;

        let mut encode_result = EncodeResult::Full;
        while encode_result == EncodeResult::Full {
            encode_result = encoder.encode(&packet, &mut encode_buffer)?;
            buffer.append(&mut encode_buffer);
        }

        Ok(())
    }

    fn do_pending_connack_state_non_connack_packet_test(packet: MqttPacket) {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::PendingConnack, 0));

        let mut server_bytes = fixture.service_with_drain(0).unwrap();
        server_bytes.clear();

        assert_eq!(Ok(()), encode_packet_to_buffer(packet, &mut server_bytes));

        assert_eq!(Err(Mqtt5Error::ProtocolError), fixture.on_incoming_bytes(0, server_bytes.as_slice()));
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        assert!(fixture.client_event_stream.is_empty());
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn pending_connack_state_unexpected_packets() {
        // Not a protocol error: Connack, Auth
        let packets = vec!(
            MqttPacket::Connect(ConnectPacket {
                ..Default::default()
            }),
            MqttPacket::Pingreq(PingreqPacket {}),
            MqttPacket::Pingresp(PingrespPacket {}),
            MqttPacket::Publish(PublishPacket {
                topic: "hello/there".to_string(),
                ..Default::default()
            }),
            MqttPacket::Puback(PubackPacket {
                packet_id : 1,
                ..Default::default()
            }),
            MqttPacket::Pubrec(PubrecPacket {
                packet_id : 1,
                ..Default::default()
            }),
            MqttPacket::Pubrel(PubrelPacket {
                packet_id : 1,
                ..Default::default()
            }),
            MqttPacket::Pubcomp(PubcompPacket {
                packet_id : 1,
                ..Default::default()
            }),
            MqttPacket::Subscribe(SubscribePacket {
                packet_id : 1,
                ..Default::default()
            }),
            MqttPacket::Suback(SubackPacket {
                packet_id : 1,
                ..Default::default()
            }),
            MqttPacket::Unsubscribe(UnsubscribePacket {
                packet_id : 1,
                ..Default::default()
            }),
            MqttPacket::Unsuback(UnsubackPacket {
                packet_id : 1,
                ..Default::default()
            }),
            MqttPacket::Disconnect(DisconnectPacket {
                ..Default::default()
            }),
        );

        for packet in packets {
            do_pending_connack_state_non_connack_packet_test(packet);
        }
    }

    #[test]
    fn pending_connack_state_connack_received_too_soon() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::PendingConnack, 0));

        let mut server_bytes = Vec::new();

        assert_eq!(Ok(()), encode_packet_to_buffer(MqttPacket::Connack(ConnackPacket{
            ..Default::default()
        }), &mut server_bytes));

        assert_eq!(Err(Mqtt5Error::ProtocolError), fixture.on_incoming_bytes(0, server_bytes.as_slice()));
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        assert!(fixture.client_event_stream.is_empty());
    }

    #[test]
    fn pending_connack_state_transition_to_disconnected() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::PendingConnack, 0));

        assert_eq!(Ok(()), fixture.on_connection_closed(0));
        assert_eq!(OperationalStateType::Disconnected, fixture.client_state.state);
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn cconnected_state_transition_to_success() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        let connack = ConnackPacket {
            ..Default::default()
        };

        let settings = build_negotiated_settings(&fixture.client_state.config, &connack,&None);
        let expected_events = vec!(ClientEvent::ConnectionSuccess(ConnectionSuccessEvent{
            connack,
            settings
        }));
        assert!(expected_events.iter().eq(fixture.client_event_stream.iter().map(|event| { &**event })));
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn connected_state_network_event_connection_opened_fails() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        let client_event_count = fixture.client_event_stream.len();

        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_connection_opened(0).err().unwrap());
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        assert_eq!(client_event_count, fixture.client_event_stream.len());
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn connected_state_network_event_write_completion_fails() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_write_completion(0).err().unwrap());
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn cconnected_state_transition_to_disconnected() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        assert_eq!(Ok(()), fixture.on_connection_closed(0));
        assert_eq!(OperationalStateType::Disconnected, fixture.client_state.state);
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn connected_state_incoming_garbage_data() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        let mut garbage = vec!(1, 2, 3, 4, 5, 6, 7, 8);

        assert_eq!(Err(Mqtt5Error::MalformedPacket), fixture.on_incoming_bytes(0, garbage.as_slice()));
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        verify_operational_state_empty(&fixture);
    }

    fn do_connected_state_unexpected_packet_test(packet : MqttPacket) {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        let mut buffer = Vec::new();
        assert_eq!(Ok(()), encode_packet_to_buffer(packet, &mut buffer));

        assert_eq!(Err(Mqtt5Error::ProtocolError), fixture.on_incoming_bytes(0, buffer.as_slice()));
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn cconnected_state_unexpected_packets() {
        let packets = vec!(
            MqttPacket::Connect(ConnectPacket{
                ..Default::default()
            }),
            MqttPacket::Connack(ConnackPacket{
                ..Default::default()
            }),
            MqttPacket::Subscribe(SubscribePacket{
                ..Default::default()
            }),
            MqttPacket::Unsubscribe(UnsubscribePacket{
                ..Default::default()
            }),
            MqttPacket::Pingreq(PingreqPacket{})
        );

        for packet in packets {
            do_connected_state_unexpected_packet_test(packet);
        }
    }

    fn do_connected_state_invalid_ack_packet_id_test(packet : MqttPacket, error: Mqtt5Error) {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        let mut buffer = Vec::new();
        assert_eq!(Ok(()), encode_packet_to_buffer(packet, &mut buffer));

        assert_eq!(Err(error), fixture.on_incoming_bytes(0, buffer.as_slice()));
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn cconnected_state_unknown_ack_packet_id() {
        let packets = vec!(
            MqttPacket::Puback(PubackPacket{
                packet_id : 5,
                ..Default::default()
            }),
            MqttPacket::Suback(SubackPacket{
                packet_id : 42,
                ..Default::default()
            }),
            MqttPacket::Unsuback(UnsubackPacket{
                packet_id : 47,
                ..Default::default()
            }),
            MqttPacket::Pubrec(PubrecPacket{
                packet_id : 666,
                ..Default::default()
            }),
            MqttPacket::Pubcomp(PubcompPacket{
                packet_id : 1023,
                ..Default::default()
            }),
        );

        for packet in packets {
            do_connected_state_invalid_ack_packet_id_test(packet, Mqtt5Error::ProtocolError);
        }
    }

    #[test]
    fn cconnected_state_invalid_ack_packet_id() {
        let packets = vec!(
            (MqttPacket::Publish(PublishPacket{
                qos: QualityOfService::AtLeastOnce,
                ..Default::default()
            }), Mqtt5Error::PublishPacketValidation),
            (MqttPacket::Puback(PubackPacket{
                ..Default::default()
            }), Mqtt5Error::PubackPacketValidation),
            (MqttPacket::Suback(SubackPacket{
                ..Default::default()
            }), Mqtt5Error::SubackPacketValidation),
            (MqttPacket::Unsuback(UnsubackPacket{
                ..Default::default()
            }), Mqtt5Error::UnsubackPacketValidation),
            (MqttPacket::Pubrec(PubrecPacket{
                ..Default::default()
            }), Mqtt5Error::PubrecPacketValidation),
            (MqttPacket::Pubcomp(PubcompPacket{
                ..Default::default()
            }), Mqtt5Error::PubcompPacketValidation),
        );

        for (packet, expected_error) in packets {
            do_connected_state_invalid_ack_packet_id_test(packet, expected_error);
        }
    }

    fn do_ping_sequence_test(connack_delay: u64, response_delay_millis: u64, request_delay_millis: u64) {
        const ping_timeout_millis : u32 = 10000;
        const keep_alive_seconds : u16 = 20;
        const keep_alive_milliseconds : u64 = (keep_alive_seconds as u64) * 1000;

        assert!(response_delay_millis < ping_timeout_millis as u64);

        let mut config = build_standard_test_config();
        config.ping_timeout_millis = ping_timeout_millis;
        config.connect.keep_alive_interval_seconds = keep_alive_seconds;

        let mut fixture = OperationalStateTestFixture::new(config);

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, connack_delay));

        let mut current_time = connack_delay;
        let mut rolling_ping_time = current_time + keep_alive_milliseconds;
        for i in 0..5 {
            // verify next service time the outbound ping time and nothing happens until then
            assert!(fixture.client_state.ping_timeout_timepoint.is_none());
            assert!(fixture.client_state.next_ping_timepoint.is_some());
            assert_eq!(rolling_ping_time, fixture.get_next_service_time(current_time).unwrap());
            assert_eq!(1 + i, fixture.to_broker_packet_stream.len());

            // trigger a ping, verify it goes out and a pingresp comes back
            current_time = rolling_ping_time + request_delay_millis;
            let server_bytes = fixture.service_with_drain(current_time).unwrap();
            assert_eq!(2 + i, fixture.to_broker_packet_stream.len());
            let (index, _) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Pingreq, i + 1, None, None).unwrap();
            assert_eq!(1 + i, index);

            // verify next service time is the ping timeout
            let ping_timeout = current_time + ping_timeout_millis as u64;
            assert!(fixture.client_state.ping_timeout_timepoint.is_some());
            assert_eq!(ping_timeout, fixture.get_next_service_time(current_time).unwrap());

            // receive pingresp, verify timeout reset
            assert_eq!(2 + i, fixture.to_client_packet_stream.len());
            let (index, _) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Pingresp, i + 1, None, None).unwrap();
            assert_eq!(1 + i, index);

            assert_eq!(Ok(()), fixture.on_incoming_bytes(current_time + response_delay_millis, server_bytes.as_slice()));
            assert_eq!(None, fixture.client_state.ping_timeout_timepoint);

            rolling_ping_time = current_time + keep_alive_milliseconds;
        }

        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn connected_state_ping_sequence_instant() {
        do_ping_sequence_test(0, 0, 0);
    }

    #[test]
    fn connected_state_ping_sequence_response_delayed() {
        do_ping_sequence_test(1,2500, 0);
    }

    #[test]
    fn connected_state_ping_sequence_request_delayed() {
        do_ping_sequence_test(3, 0, 1000);
    }

    #[test]
    fn connected_state_ping_sequence_both_delayed() {
        do_ping_sequence_test(7, 999, 131);
    }

    fn do_connected_state_ping_push_out_test(operation_function: Box<dyn Fn(&mut OperationalStateTestFixture, u64, u64) -> ()>, transmission_time: u64, response_time: u64, expected_push_out: u64) {
        const ping_timeout_millis : u32 = 10000;
        const keep_alive_seconds : u16 = 20;
        const keep_alive_milliseconds : u64 = (keep_alive_seconds as u64) * 1000;

        let mut config = build_standard_test_config();
        config.ping_timeout_millis = ping_timeout_millis;
        config.connect.keep_alive_interval_seconds = keep_alive_seconds;

        let mut fixture = OperationalStateTestFixture::new(config);

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        operation_function(&mut fixture, transmission_time, response_time);

        assert_eq!(Some(expected_push_out + keep_alive_milliseconds), fixture.get_next_service_time(response_time));
        verify_operational_state_empty(&fixture);
    }

    fn do_subscribe_success(fixture : &mut OperationalStateTestFixture, transmission_time: u64, response_time: u64, expected_reason_code: SubackReasonCode) {
        let subscribe = SubscribePacket {
            subscriptions: vec!(
                Subscription{
                    topic_filter : "hello/world".to_string(),
                    qos : QualityOfService::AtLeastOnce,
                    ..Default::default()
                }
            ),
            ..Default::default()
        };

        let subscribe_result_receiver = fixture.subscribe(0, subscribe.clone(), SubscribeOptions{ ..Default::default() }).unwrap();
        assert_eq!(Ok(()), fixture.service_round_trip(transmission_time, response_time));

        let (index, to_broker_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Subscribe, 1, None, None).unwrap();
        assert_eq!(1, index);
        if let MqttPacket::Subscribe(to_broker_subscribe) = &**to_broker_packet {
            assert_eq!(subscribe.subscriptions, to_broker_subscribe.subscriptions);
        } else {
            panic!("Expected subscribe");
        }

        let result = subscribe_result_receiver.blocking_recv();
        assert!(!result.is_err());

        let subscribe_result = result.unwrap();
        assert!(!subscribe_result.is_err());

        let suback_result = subscribe_result.unwrap();
        assert_eq!(1, suback_result.reason_codes.len());
        assert_eq!(expected_reason_code, suback_result.reason_codes[0]);

        let (index, to_client_packet) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Suback, 1, None, None).unwrap();
        assert_eq!(1, index);
        if let MqttPacket::Suback(to_client_suback) = &**to_client_packet {
            assert_eq!(suback_result, *to_client_suback);
        } else {
            panic!("Expected suback");
        }

        verify_operational_state_empty(&fixture);
    }

    fn do_publish_success(fixture : &mut OperationalStateTestFixture, qos: QualityOfService, transmission_time: u64, response_time: u64) {
        let publish = PublishPacket {
            topic: "hello/world".to_string(),
            qos,
            ..Default::default()
        };

        let publish_result_receiver = fixture.publish(0, publish.clone(), PublishOptions{ ..Default::default() }).unwrap();
        assert_eq!(Ok(()), fixture.service_round_trip(transmission_time, response_time));

        let (index, to_broker_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Publish, 1, None, None).unwrap();
        assert_eq!(1, index);
        if let MqttPacket::Publish(to_broker_publish) = &**to_broker_packet {
            assert_eq!(publish.qos, to_broker_publish.qos);
            assert_eq!(publish.topic, to_broker_publish.topic);
        } else {
            panic!("Expected publish");
        }

        // pubrel/pubcomp needs another full service cycle
        if qos == QualityOfService::ExactlyOnce {
            assert_eq!(Ok(()), fixture.service_round_trip(response_time, response_time));
        }

        let result = publish_result_receiver.blocking_recv();
        assert!(!result.is_err());

        let op_result = result.unwrap();
        assert!(!op_result.is_err());

        let publish_response = op_result.unwrap();
        match publish_response {
            PublishResponse::Qos0 => {
                assert_eq!(QualityOfService::AtMostOnce, qos);
            }
            PublishResponse::Qos1(puback) => {
                assert_eq!(QualityOfService::AtLeastOnce, qos);
                let (index, _) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Puback, 1, None, None).unwrap();
                assert_eq!(1, index);
            }
            PublishResponse::Qos2(_) => {
                assert_eq!(QualityOfService::ExactlyOnce, qos);
                let (index, _) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Pubrec, 1, None, None).unwrap();
                assert_eq!(1, index);
                let (index, _) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Pubcomp, 1, None, None).unwrap();
                assert_eq!(2, index);

                let (index, _) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Pubrel, 1, None, None).unwrap();
                assert_eq!(2, index);
            }
        }

        verify_operational_state_empty(&fixture);
    }

    fn do_unsubscribe_success(fixture : &mut OperationalStateTestFixture, transmission_time: u64, response_time: u64) {
        let unsubscribe = UnsubscribePacket {
            topic_filters: vec!("hello/world".to_string()),
            ..Default::default()
        };

        let unsubscribe_result_receiver = fixture.unsubscribe(0, unsubscribe.clone(), UnsubscribeOptions{ ..Default::default() }).unwrap();
        assert_eq!(Ok(()), fixture.service_round_trip(transmission_time, response_time));

        let (index, to_broker_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Unsubscribe, 1, None, None).unwrap();
        assert_eq!(1, index);
        if let MqttPacket::Unsubscribe(to_broker_unsubscribe) = &**to_broker_packet {
            assert_eq!(unsubscribe.topic_filters, to_broker_unsubscribe.topic_filters);
        } else {
            panic!("Expected unsubscribe");
        }

        let result = unsubscribe_result_receiver.blocking_recv();
        assert!(!result.is_err());

        let unsubscribe_result = result.unwrap();
        assert!(!unsubscribe_result.is_err());

        let unsuback_result = unsubscribe_result.unwrap();
        assert_eq!(1, unsuback_result.reason_codes.len());
        assert_eq!(UnsubackReasonCode::Success, unsuback_result.reason_codes[0]);

        let (index, to_client_packet) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Unsuback, 1, None, None).unwrap();
        assert_eq!(1, index);
        if let MqttPacket::Unsuback(to_client_unsuback) = &**to_client_packet {
            assert_eq!(unsuback_result, *to_client_unsuback);
        } else {
            panic!("Expected unsuback");
        }

        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn connected_state_ping_push_out_by_subscribe_completion() {
        do_connected_state_ping_push_out_test(Box::new(
            |transmission_time, response_time, expected_push_out| {
                do_subscribe_success(transmission_time, response_time, expected_push_out, SubackReasonCode::GrantedQos1)
            }
        ), 666, 1337, 666);
    }

    #[test]
    fn connected_state_ping_push_out_by_unsubscribe_completion() {
        do_connected_state_ping_push_out_test(Box::new(do_unsubscribe_success), 1337, 1551, 1337);
    }

    #[test]
    fn connected_state_ping_no_push_out_by_qos0_publish_completion() {
        do_connected_state_ping_push_out_test(Box::new(
            |fixture, transmission_time, response_time|{
                do_publish_success(fixture, QualityOfService::AtMostOnce, transmission_time, response_time);
            }
        ), 666, 1336, 0);
    }

    #[test]
    fn connected_state_ping_push_out_by_qos1_publish_completion() {
        do_connected_state_ping_push_out_test(Box::new(
            |fixture, transmission_time, response_time|{
                do_publish_success(fixture, QualityOfService::AtLeastOnce, transmission_time, response_time);
            }
        ), 333, 777, 333);
    }

    #[test]
    fn connected_state_ping_push_out_by_qos2_publish_completion() {
        do_connected_state_ping_push_out_test(Box::new(
            |fixture, transmission_time, response_time|{
                do_publish_success(fixture, QualityOfService::ExactlyOnce, transmission_time, response_time);
            }
        ), 444, 888, 888);
    }

    #[test]
    fn connected_state_ping_pingresp_timeout() {
        const connack_time : u64 = 11;
        const ping_timeout_millis : u32 = 10000;
        const keep_alive_seconds : u16 = 20;
        const keep_alive_milliseconds : u64 = (keep_alive_seconds as u64) * 1000;

        let mut config = build_standard_test_config();
        config.ping_timeout_millis = ping_timeout_millis;
        config.connect.keep_alive_interval_seconds = keep_alive_seconds;

        let mut fixture = OperationalStateTestFixture::new(config);

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, connack_time));
        let expected_ping_time = connack_time + keep_alive_milliseconds;
        assert_eq!(Some(expected_ping_time), fixture.get_next_service_time(connack_time));

        // trigger a ping
        let response_bytes = fixture.service_with_drain(expected_ping_time).unwrap();
        let ping_timeout_timepoint = connack_time + keep_alive_milliseconds + ping_timeout_millis as u64;
        assert_eq!(ping_timeout_timepoint, fixture.get_next_service_time(expected_ping_time).unwrap());
        let (index, _) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Pingreq, 1, None, None).unwrap();
        assert_eq!(1, index); // [connect, pingreq]

        // right before timeout, nothing should happen
        let outbound_packet_count = fixture.to_broker_packet_stream.len();
        assert_eq!(2, outbound_packet_count);

        assert_eq!(Ok(()), fixture.service_once(ping_timeout_timepoint - 1));
        assert_eq!(outbound_packet_count, fixture.to_broker_packet_stream.len());

        // invoke service after timeout, verify failure and halt
        assert_eq!(Err(Mqtt5Error::PingTimeout), fixture.service_once(ping_timeout_timepoint));
        assert_eq!(OperationalStateType::Halted, fixture.client_state.state);
        assert_eq!(outbound_packet_count, fixture.to_broker_packet_stream.len());
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn connected_state_ping_no_pings_on_zero_keep_alive() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));
        assert_eq!(None, fixture.get_next_service_time(0));

        for i in 0..3600 {
            let elapsed_millis : u64 = i * 1000;
            assert_eq!(Ok(()), fixture.service_round_trip(elapsed_millis, elapsed_millis));
            assert_eq!(None, fixture.get_next_service_time(0));
            assert_eq!(1, fixture.to_broker_packet_stream.len());
            assert_eq!(1, fixture.to_client_packet_stream.len());
        }

        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn connected_state_subscribe_success() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        do_subscribe_success(&mut fixture, 1, 2, SubackReasonCode::GrantedQos1);
    }

    #[test]
    fn connected_state_subscribe_validate_failure() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        fixture.broker_packet_handlers.insert(PacketType::Connect, Box::new(handle_connect_with_tiny_maximum_packet_size));

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        let subscribe = SubscribePacket {
            subscriptions: vec!(
                Subscription {
                    topic_filter : "hello/world".to_string(),
                    qos : QualityOfService::AtLeastOnce,
                    ..Default::default()
                }
            ),
            ..Default::default()
        };

        let subscribe_result_receiver = fixture.subscribe(0, subscribe.clone(), SubscribeOptions{ ..Default::default() }).unwrap();
        assert_eq!(Ok(()), fixture.service_round_trip(0, 0));

        let result = subscribe_result_receiver.blocking_recv();
        assert!(!result.is_err());

        let subscribe_result = result.unwrap();
        assert!(subscribe_result.is_err());

        assert_eq!(Mqtt5Error::SubscribePacketValidation, subscribe_result.unwrap_err());
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn connected_state_subscribe_suback_failure() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        fixture.broker_packet_handlers.insert(PacketType::Subscribe, Box::new(handle_subscribe_with_failure));

        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        do_subscribe_success(&mut fixture, 0, 0, SubackReasonCode::NotAuthorized);
    }

    #[test]
    fn connected_state_subscribe_timeout_failure() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        fixture.broker_packet_handlers.insert(PacketType::Subscribe, Box::new(handle_with_nothing));
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        let subscribe = SubscribePacket {
            subscriptions: vec!(
                Subscription{
                    topic_filter : "hello/world".to_string(),
                    qos : QualityOfService::AtLeastOnce,
                    ..Default::default()
                }
            ),
            ..Default::default()
        };

        let mut subscribe_result_receiver = fixture.subscribe(0, subscribe.clone(), SubscribeOptions{
            timeout_in_millis : Some(30000)
        }).unwrap();
        assert_eq!(Ok(()), fixture.service_round_trip(0, 0));

        let (index, to_broker_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Subscribe, 1, None, None).unwrap();
        assert_eq!(1, index);
        if let MqttPacket::Subscribe(to_broker_subscribe) = &**to_broker_packet {
            assert_eq!(subscribe.subscriptions, to_broker_subscribe.subscriptions);
        } else {
            panic!("Expected subscribe");
        }

        assert_eq!(Some(30000), fixture.get_next_service_time(0));

        for i in 0..30 {
            let elapsed_millis = i * 1000;
            assert_eq!(Some(30000), fixture.get_next_service_time(elapsed_millis));
            assert_eq!(Ok(()), fixture.service_round_trip(elapsed_millis, elapsed_millis));
            assert_eq!(Err(oneshot::error::TryRecvError::Empty), subscribe_result_receiver.try_recv());
        }

        assert_eq!(Ok(()), fixture.service_round_trip(30000, 30000));
        let result = subscribe_result_receiver.blocking_recv();
        assert_eq!(Mqtt5Error::AckTimeout, result.unwrap().unwrap_err());
        verify_operational_state_empty(&fixture);
    }

    #[test]
    fn connected_state_unsubscribe_success() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        do_unsubscribe_success(&mut fixture, 3, 7);
    }

    #[test]
    fn connected_state_publish_qos0_success() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        do_publish_success(&mut fixture, QualityOfService::AtMostOnce, 11, 13);
    }

    #[test]
    fn connected_state_publish_qos1_success() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        do_publish_success(&mut fixture, QualityOfService::AtLeastOnce, 17, 23);
    }

    #[test]
    fn connected_state_publish_qos2_success() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());
        assert_eq!(Ok(()), fixture.advance_disconnected_to_state(OperationalStateType::Connected, 0));

        do_publish_success(&mut fixture, QualityOfService::ExactlyOnce, 29, 31);
    }
}