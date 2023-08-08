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

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct PubrecPacket {
    pub packet_id: u16,

    pub reason_code: PubrecReasonCode,
    pub reason_string: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,
}

#[rustfmt::skip]
define_ack_packet_lengths_function!(compute_pubrec_packet_length_properties, PubrecPacket, PubrecReasonCode);
define_ack_packet_reason_string_accessor!(get_pubrec_packet_reason_string, Pubrec);
define_ack_packet_user_property_accessor!(get_pubrec_packet_user_property, Pubrec);

#[rustfmt::skip]
define_ack_packet_encoding_impl!(write_pubrec_encoding_steps, PubrecPacket, PubrecReasonCode, PACKET_TYPE_PUBREC, compute_pubrec_packet_length_properties, get_pubrec_packet_reason_string, get_pubrec_packet_user_property);

define_ack_packet_decode_properties_function!(decode_pubrec_properties, PubrecPacket);
define_ack_packet_decode_function!(decode_pubrec_packet, PubrecPacket, PACKET_TYPE_PUBREC, convert_u8_to_pubrec_reason_code, decode_pubrec_properties);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pubrec_round_trip_encode_decode_default() {
        let packet = PubrecPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_success_no_props() {

        let packet = PubrecPacket {
            packet_id: 1234,
            reason_code: PubrecReasonCode::Success,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_failure_no_props() {

        let packet = PubrecPacket {
            packet_id: 8191,
            reason_code: PubrecReasonCode::PacketIdentifierInUse,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_success_with_props() {

        let packet = PubrecPacket {
            packet_id: 10253,
            reason_code: PubrecReasonCode::Success,
            reason_string: Some("Whoa, qos2.  Brave and inspired.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubrec1".to_string(), value: "value1".to_string()},
                UserProperty{name: "pubrec2".to_string(), value: "value2".to_string()},
                UserProperty{name: "pubrec2".to_string(), value: "value3".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_failure_with_props() {

        let packet = PubrecPacket {
            packet_id: 125,
            reason_code: PubrecReasonCode::UnspecifiedError,
            reason_string: Some("Qos2?  Get that nonsense outta here.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubwreck1".to_string(), value: "krabbypatty".to_string()},
                UserProperty{name: "pubwreck2".to_string(), value: "spongebob".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }
}