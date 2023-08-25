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
use crate::validate::*;
use crate::validate::utils::*;

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

pub(crate) fn validate_pubrel_packet_fixed(packet: &PubrelPacket) -> Mqtt5Result<()> {

    validate_optional_string_length!(reason, &packet.reason_string, PubrelPacketValidation);
    validate_user_properties!(properties, &packet.user_properties, PubrelPacketValidation);

    Ok(())
}

pub(crate) fn validate_pubrel_packet_context_specific(packet: &PubrelPacket, context: &ValidationContext) -> Mqtt5Result<()> {

    // validate packet size against the negotiated maximum
    let (total_remaining_length, _) = compute_pubrel_packet_length_properties(packet)?;
    if total_remaining_length > context.negotiated_settings.maximum_packet_size_to_server {
        return Err(Mqtt5Error::PubrelPacketValidation);
    }

    Ok(())
}

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

    fn create_pubrel_with_all_properties() -> Box<PubrelPacket> {
        Box::new(PubrelPacket {
            packet_id: 12500,
            reason_code: PubrelReasonCode::PacketIdentifierNotFound,
            reason_string: Some("Aw shucks, I forgot what I was doing.  Sorry!".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "hello1".to_string(), value: "squidward".to_string()},
                UserProperty{name: "patrick".to_string(), value: "star".to_string()},
            ))
        })
    }

    #[test]
    fn pubrel_round_trip_encode_decode_failure_with_props() {

        let packet = create_pubrel_with_all_properties();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_decode_failure_bad_fixed_header() {
        let packet = create_pubrel_with_all_properties();

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubrel(packet), 15);
    }

    #[test]
    fn pubrel_decode_failure_bad_reason_code() {
        let packet = create_pubrel_with_all_properties();

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for acks, the reason code is in byte 4
            clone[4] = 15;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pubrel(packet), corrupt_reason_code);
    }

    #[test]
    fn pubrel_decode_failure_duplicate_reason_string() {
        let packet = create_pubrel_with_all_properties();

        let duplicate_reason_string = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length
            clone[1] += 5;

            // increase property section length
            clone[5] += 5;

            // add the duplicate property
            clone.push(PROPERTY_KEY_REASON_STRING);
            clone.push(0);
            clone.push(2);
            clone.push(67);
            clone.push(67);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pubrel(packet), duplicate_reason_string);
    }

    use crate::validate::testing::*;

    #[test]
    fn pubrel_validate_success() {
        let packet = create_pubrel_with_all_properties();

        assert_eq!(validate_pubrel_packet_fixed(&packet), Ok(()));
    }

    #[test]
    fn pubrel_validate_failure_reason_string_length() {
        let mut packet = create_pubrel_with_all_properties();
        packet.reason_string = Some("A".repeat(128 * 1024).to_string());

        assert_eq!(validate_pubrel_packet_fixed(&packet), Err(Mqtt5Error::PubrelPacketValidation));
    }

    #[test]
    fn pubrel_validate_failure_invalid_user_properties() {
        let mut packet = create_pubrel_with_all_properties();
        packet.user_properties = Some(create_invalid_user_properties());

        assert_eq!(validate_pubrel_packet_fixed(&packet), Err(Mqtt5Error::PubrelPacketValidation));
    }

    #[test]
    fn pubrel_validate_success_context_specific() {
        let packet = create_pubrel_with_all_properties();

        let test_validation_context = create_pinned_validation_context();
        let validation_context = create_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_pubrel_packet_context_specific(&packet, &validation_context), Ok(()));
    }

    #[test]
    fn pubrel_validate_failure_context_packet_size() {
        let packet = create_pubrel_with_all_properties();

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.maximum_packet_size_to_server = 10;
        let validation_context = create_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_pubrel_packet_context_specific(&packet, &validation_context), Err(Mqtt5Error::PubrelPacketValidation));
    }
}