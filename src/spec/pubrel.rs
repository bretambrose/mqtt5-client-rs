/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::decode::utils::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::spec::*;
use crate::spec::utils::*;

use std::collections::VecDeque;

/// Data model of an [MQTT5 PUBREL](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901141) packet
#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct PubrelPacket {

    // packet id is modeled but internal to the client
    pub(crate) packet_id: u16,

    /// Success indicator or failure reason for the middle step of the QoS 2 PUBLISH delivery process.
    ///
    /// See [MQTT5 PUBREL Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901144)
    pub reason_code: PubrelReasonCode,

    /// Additional diagnostic information about the ongoing QoS 2 PUBLISH delivery process.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901147)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901148)
    pub user_properties: Option<Vec<UserProperty>>,
}

#[rustfmt::skip]
define_ack_packet_lengths_function!(compute_pubrel_packet_length_properties, PubrelPacket, PubrelReasonCode);
define_ack_packet_reason_string_accessor!(get_pubrel_packet_reason_string, Pubrel);
define_ack_packet_user_property_accessor!(get_pubrel_packet_user_property, Pubrel);

#[rustfmt::skip]
define_ack_packet_encoding_impl!(write_pubrel_encoding_steps, PubrelPacket, PubrelReasonCode, PACKET_TYPE_PUBREL, compute_pubrel_packet_length_properties, get_pubrel_packet_reason_string, get_pubrel_packet_user_property);

define_ack_packet_decode_properties_function!(decode_pubrel_properties, PubrelPacket);
define_ack_packet_decode_function!(decode_pubrel_packet, PubrelPacket, PACKET_TYPE_PUBREL, convert_u8_to_pubrel_reason_code, decode_pubrel_properties);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pubrel_round_trip_encode_decode_default() {
        let packet = Box::new(PubrelPacket {
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_success_no_props() {

        let packet = Box::new(PubrelPacket {
            packet_id: 12,
            reason_code: PubrelReasonCode::Success,
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_failure_no_props() {

        let packet = Box::new(PubrelPacket {
            packet_id: 8193,
            reason_code: PubrelReasonCode::PacketIdentifierNotFound,
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_success_with_props() {

        let packet = Box::new(PubrelPacket {
            packet_id: 10253,
            reason_code: PubrelReasonCode::Success,
            reason_string: Some("Qos2, I can do this.  Believe in me.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubrel1".to_string(), value: "value1".to_string()},
                UserProperty{name: "pubrel2".to_string(), value: "value2".to_string()},
                UserProperty{name: "pubrel2".to_string(), value: "value3".to_string()},
            ))
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_failure_with_props() {

        let packet = Box::new(PubrelPacket {
            packet_id: 12500,
            reason_code: PubrelReasonCode::PacketIdentifierNotFound,
            reason_string: Some("Aw shucks, I forgot what I was doing.  Sorry!".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "hello1".to_string(), value: "squidward".to_string()},
                UserProperty{name: "patrick".to_string(), value: "star".to_string()},
            ))
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_decode_failure_bad_fixed_header() {
        let packet = Box::new(PubrelPacket {
            packet_id: 8193,
            reason_code: PubrelReasonCode::PacketIdentifierNotFound,
            ..Default::default()
        });

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubrel(packet), 15);
    }
}