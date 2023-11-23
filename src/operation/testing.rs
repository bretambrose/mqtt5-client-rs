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
            connack_timeout_millis: 30,
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


        pub fn new(config : OperationalStateConfig) -> Self {
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

        pub fn service_with_drain(&mut self, elapsed_millis: u64) -> Mqtt5Result<Vec<u8>> {
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

        pub fn on_connection_opened(&mut self, elapsed_millis: u64) -> Mqtt5Result<()> {
            let mut context = NetworkEventContext {
                current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
                event: NetworkEvent::ConnectionOpened,
                client_events: &mut self.client_event_stream,
            };

            self.client_state.handle_network_event(&mut context)
        }

        pub fn on_write_completion(&mut self, elapsed_millis: u64) -> Mqtt5Result<()> {
            let mut context = NetworkEventContext {
                current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
                event: NetworkEvent::WriteCompletion,
                client_events: &mut self.client_event_stream,
            };

            self.client_state.handle_network_event(&mut context)
        }

        pub fn on_connection_closed(&mut self, elapsed_millis: u64) -> Mqtt5Result<()> {
            let mut context = NetworkEventContext {
                current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
                event: NetworkEvent::ConnectionClosed,
                client_events: &mut self.client_event_stream,
            };

            self.client_state.handle_network_event(&mut context)
        }

        pub fn on_incoming_bytes(&mut self, elapsed_millis: u64, bytes: &[u8]) -> Mqtt5Result<()> {
            let mut context = NetworkEventContext {
                current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
                event: NetworkEvent::IncomingData(bytes),
                client_events: &mut self.client_event_stream,
            };

            self.client_state.handle_network_event(&mut context)
        }

        pub fn subscribe(&mut self, elapsed_millis: u64, subscribe: SubscribePacket, options: SubscribeOptions) -> Mqtt5Result<oneshot::Receiver<SubscribeResult>> {
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

        pub fn unsubscribe(&mut self, elapsed_millis: u64, unsubscribe: UnsubscribePacket, options: UnsubscribeOptions) -> Mqtt5Result<oneshot::Receiver<UnsubscribeResult>> {
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

        pub fn publish(&mut self, elapsed_millis: u64, publish: PublishPacket, options: PublishOptions) -> Mqtt5Result<oneshot::Receiver<PublishResult>> {
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

        pub fn disconnect(&mut self, elapsed_millis: u64, disconnect: DisconnectPacket, options: DisconnectOptions) -> Mqtt5Result<oneshot::Receiver<DisconnectResult>> {
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

    #[test]
    fn network_event_handler_fails_while_disconnected() {
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
    fn network_event_handler_fails_while_pending_connack() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), fixture.on_connection_opened(0));
        assert_eq!(OperationalStateType::PendingConnack, fixture.client_state.state);

        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_connection_opened(0).err().unwrap());
        assert!(fixture.client_event_stream.is_empty());

        // write completion isn't invalid per se, but it is if nothing has been written yet
        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_write_completion(0).err().unwrap());
        assert!(fixture.client_event_stream.is_empty());
    }

    fn advance_fixture_to_connected(fixture: &mut OperationalStateTestFixture, elapsed_millis: u64) -> Mqtt5Result<()> {
        fixture.on_connection_opened(elapsed_millis)?;

        let server_bytes = fixture.service_with_drain(elapsed_millis)?;

        fixture.on_incoming_bytes(elapsed_millis, server_bytes.as_slice())?;

        Ok(())
    }

    #[test]
    fn can_connect_successfully() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), advance_fixture_to_connected(&mut fixture, 0));
        assert_eq!(OperationalStateType::Connected, fixture.client_state.state);

        let connack = ConnackPacket {
            ..Default::default()
        };

        let settings = build_negotiated_settings(&fixture.client_state.config, &connack,&None);
        let expected_events = vec!(ClientEvent::ConnectionSuccess(ConnectionSuccessEvent{
            connack,
            settings
        }));
        assert!(expected_events.iter().eq(fixture.client_event_stream.iter().map(|event| { &**event })));
    }

    #[test]
    fn network_event_handler_fails_while_connected() {
        let mut fixture = OperationalStateTestFixture::new(build_standard_test_config());

        assert_eq!(Ok(()), advance_fixture_to_connected(&mut fixture, 0));
        assert_eq!(OperationalStateType::Connected, fixture.client_state.state);
        let client_event_count = fixture.client_event_stream.len();

        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_connection_opened(0).err().unwrap());
        assert_eq!(client_event_count, fixture.client_event_stream.len());

        // write completion isn't invalid per se, but it is if nothing has been written yet
        assert_eq!(Mqtt5Error::InternalStateError, fixture.on_write_completion(0).err().unwrap());
        assert_eq!(client_event_count, fixture.client_event_stream.len());
    }

/*

    #[test]
    fn next_service_time_while_disconnected() {
        let config = build_standard_test_config();
        let state = OperationalState::new(config);
    }

 */
}