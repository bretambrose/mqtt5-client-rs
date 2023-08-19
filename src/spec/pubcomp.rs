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

/// Data model of an [MQTT5 PUBCOMP](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901151) packet
#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct PubcompPacket {

    /// Id of the QoS 2 publish this packet corresponds to
    pub packet_id: u16,

    /// Success indicator or failure reason for the final step of a QoS 2 PUBLISH delivery.
    ///
    /// See [MQTT5 PUBCOMP Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901154)
    pub reason_code: PubcompReasonCode,

    /// Additional diagnostic information about the final step of a QoS 2 PUBLISH delivery.
    ///
    /// See [MQTT5 PUBCOMP Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901157)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901158)
    pub user_properties: Option<Vec<UserProperty>>,
}

#[rustfmt::skip]
define_ack_packet_lengths_function!(compute_pubcomp_packet_length_properties, PubcompPacket, PubcompReasonCode);
define_ack_packet_reason_string_accessor!(get_pubcomp_packet_reason_string, Pubcomp);
define_ack_packet_user_property_accessor!(get_pubcomp_packet_user_property, Pubcomp);

#[rustfmt::skip]
define_ack_packet_encoding_impl!(write_pubcomp_encoding_steps, PubcompPacket, PubcompReasonCode, PACKET_TYPE_PUBCOMP, compute_pubcomp_packet_length_properties, get_pubcomp_packet_reason_string, get_pubcomp_packet_user_property);

define_ack_packet_decode_properties_function!(decode_pubcomp_properties, PubcompPacket);
define_ack_packet_decode_function!(decode_pubcomp_packet, PubcompPacket, PACKET_TYPE_PUBCOMP, convert_u8_to_pubcomp_reason_code, decode_pubcomp_properties);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pubcomp_round_trip_encode_decode_default() {
        let packet = Box::new(PubcompPacket {
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet)));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_success_no_props() {

        let packet = Box::new(PubcompPacket {
            packet_id: 132,
            reason_code: PubcompReasonCode::Success,
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet)));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_failure_no_props() {

        let packet = Box::new(PubcompPacket {
            packet_id: 4095,
            reason_code: PubcompReasonCode::PacketIdentifierNotFound,
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet)));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_success_with_props() {

        let packet = Box::new(PubcompPacket {
            packet_id: 1253,
            reason_code: PubcompReasonCode::Success,
            reason_string: Some("We did it!  High five.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubcomp1".to_string(), value: "value1".to_string()},
                UserProperty{name: "pubcomp2".to_string(), value: "value2".to_string()},
                UserProperty{name: "pubcomp2".to_string(), value: "value3".to_string()},
            ))
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet)));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_failure_with_props() {

        let packet = Box::new(PubcompPacket {
            packet_id: 1500,
            reason_code: PubcompReasonCode::PacketIdentifierNotFound,
            reason_string: Some("I tried so hard, and got so far, but in the end, we totally face-planted".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "uf".to_string(), value: "dah".to_string()},
                UserProperty{name: "velkomen".to_string(), value: "stanwood".to_string()},
            ))
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet)));
    }

    #[test]
    fn pubcomp_decode_failure_bad_fixed_header() {
        let packet = Box::new(PubcompPacket {
            packet_id: 4095,
            reason_code: PubcompReasonCode::PacketIdentifierNotFound,
            ..Default::default()
        });

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubcomp(packet), 10);
    }
}