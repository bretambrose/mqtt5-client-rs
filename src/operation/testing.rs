/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#[cfg(test)]
mod operational_state_tests {
    use super::*;
    use crate::operation::*;

    fn build_standard_test_config() -> OperationalStateConfig {
        OperationalStateConfig {
            connect : Box::new(ConnectPacket{ ..Default::default() }),
            base_timestamp: Instant::now(),
            offline_queue_policy: OfflineQueuePolicy::PreserveAll,
            rejoin_session_policy: RejoinSessionPolicy::RejoinAlways,
            connack_timeout_millis: 30,
            ping_timeout_millis: 30000,
            outbound_resolver: None,
        }
    }

    #[test]
    fn network_event_handler_fails_while_disconnected() {
        let config = build_standard_test_config();
        let mut state = OperationalState::new(config);

        assert_eq!(OperationalStateType::Disconnected, state.state);

        let mut events  = VecDeque::new();
        let mut context = NetworkEventContext {
            event: NetworkEvent::ConnectionClosed,
            events: &mut events,
            current_time: Instant::now()
        };

        assert_eq!(Mqtt5Error::InternalStateError, state.handle_network_event(&mut context).err().unwrap());
        assert!(context.events.is_empty());

        context.event = NetworkEvent::WriteCompletion;

        assert_eq!(Mqtt5Error::InternalStateError, state.handle_network_event(&mut context).err().unwrap());
        assert!(context.events.is_empty());

        let bytes : Vec<u8> = vec!(0, 1, 2, 3, 4, 5);
        context.event = NetworkEvent::IncomingData(bytes.as_slice());

        assert_eq!(Mqtt5Error::InternalStateError, state.handle_network_event(&mut context).err().unwrap());
        assert!(context.events.is_empty());
    }

    #[test]
    fn next_service_time_while_disconnected() {
        let config = build_standard_test_config();
        let state = OperationalState::new(config);
    }
}