/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::decode::utils::*;
use crate::encode::utils::*;
use crate::spec::*;
use crate::spec::utils::*;

use std::collections::VecDeque;

/// Data model of an [MQTT5 PUBACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901121) packet
#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct PubackPacket {

    // packet id is modeled but internal to the client
    pub(crate) packet_id: u16,

    /// Success indicator or failure reason for the associated PUBLISH packet.
    ///
    /// See [MQTT5 PUBACK Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901124)
    pub reason_code: PubackReasonCode,

    /// Additional diagnostic information about the result of the PUBLISH attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901127)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901128)
    pub user_properties: Option<Vec<UserProperty>>,
}

#[rustfmt::skip]
define_ack_packet_lengths_function!(compute_puback_packet_length_properties, PubackPacket, PubackReasonCode);
define_ack_packet_reason_string_accessor!(get_puback_packet_reason_string, Puback);
define_ack_packet_user_property_accessor!(get_puback_packet_user_property, Puback);

#[rustfmt::skip]
define_ack_packet_encoding_impl!(write_puback_encoding_steps, PubackPacket, PubackReasonCode, PACKET_TYPE_PUBACK, compute_puback_packet_length_properties, get_puback_packet_reason_string, get_puback_packet_user_property);

define_ack_packet_decode_properties_function!(decode_puback_properties, PubackPacket);
define_ack_packet_decode_function!(decode_puback_packet, PubackPacket, PACKET_TYPE_PUBACK, convert_u8_to_puback_reason_code, decode_puback_properties);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn puback_round_trip_encode_decode_default() {
        let packet = PubackPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_success_no_props() {

        let packet = PubackPacket {
            packet_id: 123,
            reason_code: PubackReasonCode::Success,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_failure_no_props() {

        let packet = PubackPacket {
            packet_id: 16384,
            reason_code: PubackReasonCode::NotAuthorized,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_success_with_props() {

        let packet = PubackPacket {
            packet_id: 1025,
            reason_code: PubackReasonCode::Success,
            reason_string: Some("This was the best publish I've ever seen.  Take a bow.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "puback1".to_string(), value: "value1".to_string()},
                UserProperty{name: "puback2".to_string(), value: "value2".to_string()},
                UserProperty{name: "puback2".to_string(), value: "value3".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_failure_with_props() {

        let packet = PubackPacket {
            packet_id: 1025,
            reason_code: PubackReasonCode::ImplementationSpecificError,
            reason_string: Some("Wow!  What a terrible publish.  You should be ashamed.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "puback1".to_string(), value: "value1".to_string()},
                UserProperty{name: "puback2".to_string(), value: "value2".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }
}