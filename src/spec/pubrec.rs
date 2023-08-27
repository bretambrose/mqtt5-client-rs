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

/// Data model of an [MQTT5 PUBREC](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901131) packet
#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct PubrecPacket {

    /// Id of the QoS 2 publish this packet corresponds to
    pub packet_id: u16,

    /// Success indicator or failure reason for the initial step of the QoS 2 PUBLISH delivery process.
    ///
    /// See [MQTT5 PUBREC Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901134)
    pub reason_code: PubrecReasonCode,

    /// Additional diagnostic information about the result of the PUBLISH attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901147)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901138)
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

pub(crate) fn validate_pubrec_packet_fixed(packet: &PubrecPacket) -> Mqtt5Result<()> {

    validate_optional_string_length!(reason, &packet.reason_string, PubrecPacketValidation);
    validate_user_properties!(properties, &packet.user_properties, PubrecPacketValidation);

    Ok(())
}

pub(crate) fn validate_pubrec_packet_context_specific(packet: &PubrecPacket, context: &ValidationContext) -> Mqtt5Result<()> {

    /* inbound size is checked on decode, outbound size is checked here */
    if context.is_outbound {
        let (total_remaining_length, _) = compute_pubrec_packet_length_properties(packet)?;
        let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
        if total_packet_length > context.negotiated_settings.maximum_packet_size_to_server {
            return Err(Mqtt5Error::PubrecPacketValidation);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pubrec_round_trip_encode_decode_default() {
        let packet = Box::new(PubrecPacket {
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_success_no_props() {

        let packet = Box::new(PubrecPacket {
            packet_id: 1234,
            reason_code: PubrecReasonCode::Success,
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_failure_no_props() {

        let packet = Box::new(PubrecPacket {
            packet_id: 8191,
            reason_code: PubrecReasonCode::PacketIdentifierInUse,
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_success_with_props() {

        let packet = Box::new(PubrecPacket {
            packet_id: 10253,
            reason_code: PubrecReasonCode::Success,
            reason_string: Some("Whoa, qos2.  Brave and inspired.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubrec1".to_string(), value: "value1".to_string()},
                UserProperty{name: "pubrec2".to_string(), value: "value2".to_string()},
                UserProperty{name: "pubrec2".to_string(), value: "value3".to_string()},
            ))
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    fn create_pubrec_with_all_properties() -> Box<PubrecPacket> {
        Box::new(PubrecPacket {
            packet_id: 125,
            reason_code: PubrecReasonCode::UnspecifiedError,
            reason_string: Some("Qos2?  Get that nonsense outta here.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubwreck1".to_string(), value: "krabbypatty".to_string()},
                UserProperty{name: "pubwreck2".to_string(), value: "spongebob".to_string()},
            ))
        })
    }

    #[test]
    fn pubrec_round_trip_encode_decode_failure_with_props() {

        let packet = create_pubrec_with_all_properties();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_decode_failure_bad_fixed_header() {
        let packet = create_pubrec_with_all_properties();

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubrec(packet), 12);
    }

    #[test]
    fn pubrec_decode_failure_bad_reason_code() {
        let packet = create_pubrec_with_all_properties();

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for acks, the reason code is in byte 4
            clone[4] = 3;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pubrec(packet), corrupt_reason_code);
    }

    #[test]
    fn pubrec_decode_failure_duplicate_reason_string() {
        let packet = create_pubrec_with_all_properties();

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

        do_mutated_decode_failure_test(&MqttPacket::Pubrec(packet), duplicate_reason_string);
    }

    #[test]
    fn pubrec_decode_failure_packet_size() {
        let packet = create_pubrec_with_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Pubrec(packet));
    }

    use crate::validate::testing::*;

    #[test]
    fn pubrec_validate_success() {
        let packet = create_pubrec_with_all_properties();

        assert_eq!(validate_packet_fixed(&MqttPacket::Pubrec(packet)), Ok(()));
    }

    #[test]
    fn pubrec_validate_failure_reason_string_length() {
        let mut packet = create_pubrec_with_all_properties();
        packet.reason_string = Some("A".repeat(128 * 1024).to_string());

        assert_eq!(validate_packet_fixed(&MqttPacket::Pubrec(packet)), Err(Mqtt5Error::PubrecPacketValidation));
    }

    #[test]
    fn pubrec_validate_failure_invalid_user_properties() {
        let mut packet = create_pubrec_with_all_properties();
        packet.user_properties = Some(create_invalid_user_properties());

        assert_eq!(validate_packet_fixed(&MqttPacket::Pubrec(packet)), Err(Mqtt5Error::PubrecPacketValidation));
    }

    #[test]
    fn pubrec_validate_success_context_specific() {
        let packet = create_pubrec_with_all_properties();

        let test_validation_context = create_pinned_validation_context();
        let validation_context = create_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_packet_context_specific(&MqttPacket::Pubrec(packet), &validation_context), Ok(()));
    }

    #[test]
    fn pubrec_validate_failure_context_specific_outbound_size() {
        let packet = create_pubrec_with_all_properties();

        do_outbound_size_validate_failure_test(&MqttPacket::Pubrec(packet), Mqtt5Error::PubrecPacketValidation);
    }
}