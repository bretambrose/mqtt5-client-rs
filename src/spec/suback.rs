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

/// Data model of an [MQTT5 SUBACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901171) packet.
#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct SubackPacket {

    /// Id of the unsubscribe this packet is acknowledging
    pub packet_id: u16,

    /// Additional diagnostic information about the result of the SUBSCRIBE attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901176)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901177)
    pub user_properties: Option<Vec<UserProperty>>,

    /// A list of reason codes indicating the result of each individual subscription entry in the
    /// associated SUBSCRIBE packet.
    ///
    /// See [MQTT5 Suback Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901178)
    pub reason_codes: Vec<SubackReasonCode>,
}

#[rustfmt::skip]
fn compute_suback_packet_length_properties(packet: &SubackPacket) -> Mqtt5Result<(u32, u32)> {
    let mut suback_property_section_length = compute_user_properties_length(&packet.user_properties);
    add_optional_string_property_length!(suback_property_section_length, packet.reason_string);

    let mut total_remaining_length : usize = 2 + compute_variable_length_integer_encode_size(suback_property_section_length)?;
    total_remaining_length += suback_property_section_length;

    total_remaining_length += packet.reason_codes.len();

    Ok((total_remaining_length as u32, suback_property_section_length as u32))
}

fn get_suback_packet_reason_string(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Suback, reason_string)
}

fn get_suback_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Suback(suback) = packet {
        if let Some(properties) = &suback.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

#[rustfmt::skip]
pub(crate) fn write_suback_encoding_steps(packet: &SubackPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<()> {
    let (total_remaining_length, suback_property_length) = compute_suback_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, PACKET_TYPE_SUBACK << 4);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    encode_integral_expression!(steps, Uint16, packet.packet_id);
    encode_integral_expression!(steps, Vli, suback_property_length);

    encode_optional_string_property!(steps, get_suback_packet_reason_string, PROPERTY_KEY_REASON_STRING, packet.reason_string);
    encode_user_properties!(steps, get_suback_packet_user_property, packet.user_properties);

    let reason_codes = &packet.reason_codes;
    for reason_code in reason_codes {
        encode_enum!(steps, Uint8, u8, *reason_code);
    }

    Ok(())
}

fn decode_suback_properties(property_bytes: &[u8], packet : &mut SubackPacket) -> Mqtt5Result<()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => { return Err(Mqtt5Error::MalformedPacket); }
        }
    }

    Ok(())
}

pub(crate) fn decode_suback_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<MqttPacket>> {
    if first_byte != (PACKET_TYPE_SUBACK << 4) {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let mut box_packet = Box::new(MqttPacket::Suback(SubackPacket { ..Default::default() }));

    if let MqttPacket::Suback(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

        let mut properties_length: usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
        if properties_length > mutable_body.len() {
            return Err(Mqtt5Error::MalformedPacket);
        }

        let properties_bytes = &mutable_body[..properties_length];
        let payload_bytes = &mutable_body[properties_length..];

        decode_suback_properties(properties_bytes, packet)?;

        let reason_code_count = payload_bytes.len();
        packet.reason_codes.reserve(reason_code_count);

        for i in 0..reason_code_count {
            packet.reason_codes.push(convert_u8_to_suback_reason_code(payload_bytes[i])?);
        }

        return Ok(box_packet);
    }

    Err(Mqtt5Error::Unknown)
}

pub(crate) fn validate_suback_packet_fixed(packet: &SubackPacket) -> Mqtt5Result<()> {

    validate_user_properties!(properties, &packet.user_properties, SubackPacketValidation);
    validate_optional_string_length!(reason_string, &packet.reason_string, SubackPacketValidation);

    Ok(())
}

pub(crate) fn validate_suback_packet_context_specific(packet: &SubackPacket, context: &ValidationContext) -> Mqtt5Result<()> {

    if context.is_outbound {
        /* inbound size is checked on decode, outbound size is checked here */
        let (total_remaining_length, _) = compute_suback_packet_length_properties(packet)?;
        let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
        if total_packet_length > context.negotiated_settings.maximum_packet_size_to_server {
            return Err(Mqtt5Error::SubackPacketValidation);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;
    use crate::validate::testing::create_invalid_user_properties;

    #[test]
    fn suback_round_trip_encode_decode_default() {
        let packet = SubackPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Suback(packet)));
    }

    #[test]
    fn suback_round_trip_encode_decode_required() {
        let packet = SubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                SubackReasonCode::GrantedQos1,
                SubackReasonCode::QuotaExceeded,
                SubackReasonCode::SubscriptionIdentifiersNotSupported,
            ],
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Suback(packet)));
    }

    fn create_suback_all_properties() -> SubackPacket {
        SubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                SubackReasonCode::GrantedQos2,
                SubackReasonCode::UnspecifiedError,
                SubackReasonCode::SharedSubscriptionsNotSupported
            ],
            reason_string : Some("Maybe tomorrow".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "This".to_string(), value: "wasfast".to_string()},
                UserProperty{name: "Onepacket".to_string(), value: "left".to_string()},
            ))
        }
    }

    #[test]
    fn suback_round_trip_encode_decode_all() {
        let packet = create_suback_all_properties();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Suback(packet)));
    }

    #[test]
    fn suback_decode_failure_bad_fixed_header() {
        let packet = SubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                SubackReasonCode::GrantedQos1,
                SubackReasonCode::QuotaExceeded,
                SubackReasonCode::SubscriptionIdentifiersNotSupported,
            ],
            ..Default::default()
        };

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Suback(packet), 15);
    }

    #[test]
    fn suback_decode_failure_reason_code_invalid() {
        let packet = SubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                SubackReasonCode::GrantedQos1
            ],
            ..Default::default()
        };

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for acks, the reason code is in byte 4
            clone[5] = 196;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Suback(packet), corrupt_reason_code);
    }

    const SUBACK_PACKET_TEST_PROPERTY_LENGTH_INDEX : usize = 4;
    const SUBACK_PACKET_TEST_PAYLOAD_INDEX : usize = 12;

    #[test]
    fn suback_decode_failure_duplicate_reason_string() {

        let packet = SubackPacket {
            packet_id : 1023,
            reason_string: Some("derp".to_string()),
            reason_codes : vec![
                SubackReasonCode::GrantedQos1
            ],
            ..Default::default()
        };

        let duplicate_reason_string = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[1] += 4;
            clone[SUBACK_PACKET_TEST_PROPERTY_LENGTH_INDEX] += 4;

            clone.insert(SUBACK_PACKET_TEST_PAYLOAD_INDEX, 65);
            clone.insert(SUBACK_PACKET_TEST_PAYLOAD_INDEX, 1);
            clone.insert(SUBACK_PACKET_TEST_PAYLOAD_INDEX, 0);
            clone.insert(SUBACK_PACKET_TEST_PAYLOAD_INDEX, PROPERTY_KEY_REASON_STRING);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Suback(packet), duplicate_reason_string);
    }

    #[test]
    fn suback_validate_success() {
        let packet = create_suback_all_properties();

        assert_eq!(Ok(()), validate_packet_fixed(&MqttPacket::Suback(packet)));
    }

    #[test]
    fn suback_validate_failure_reason_string_length() {
        let mut packet = create_suback_all_properties();
        packet.reason_string = Some("Derp".repeat(20000).to_string());

        assert_eq!(Err(Mqtt5Error::SubackPacketValidation), validate_packet_fixed(&MqttPacket::Suback(packet)));
    }

    #[test]
    fn suback_validate_failure_user_properties_invalid() {
        let mut packet = create_suback_all_properties();
        packet.user_properties = Some(create_invalid_user_properties());

        assert_eq!(Err(Mqtt5Error::SubackPacketValidation), validate_packet_fixed(&MqttPacket::Suback(packet)));
    }

    use crate::validate::testing::*;

    #[test]
    fn suback_validate_context_specific_success() {
        let packet = create_suback_all_properties();

        let test_validation_context = create_pinned_validation_context();
        let validation_context = create_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_packet_context_specific(&MqttPacket::Suback(packet), &validation_context), Ok(()));
    }

    #[test]
    fn suback_validate_context_specific_failure_outbound_size() {
        let packet = create_suback_all_properties();

        do_outbound_size_validate_failure_test(&MqttPacket::Suback(packet), Mqtt5Error::SubackPacketValidation);
    }

}
