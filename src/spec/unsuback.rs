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

/// Data model of an [MQTT5 UNSUBACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901187) packet.
#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct UnsubackPacket {

    /// Id of the unsubscribe this packet is acknowledging
    pub packet_id: u16,

    /// Additional diagnostic information about the result of the UNSUBSCRIBE attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901192)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901193)
    pub user_properties: Option<Vec<UserProperty>>,

    /// A list of reason codes indicating the result of unsubscribing from each individual topic filter entry in the
    /// associated UNSUBSCRIBE packet.
    ///
    /// See [MQTT5 Unsuback Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901194)
    pub reason_codes: Vec<UnsubackReasonCode>,
}

#[rustfmt::skip]
fn compute_unsuback_packet_length_properties(packet: &UnsubackPacket) -> Mqtt5Result<(u32, u32)> {
    let mut unsuback_property_section_length = compute_user_properties_length(&packet.user_properties);
    add_optional_string_property_length!(unsuback_property_section_length, packet.reason_string);

    let mut total_remaining_length : usize = 2 + compute_variable_length_integer_encode_size(unsuback_property_section_length)?;
    total_remaining_length += unsuback_property_section_length;

    total_remaining_length += packet.reason_codes.len();

    Ok((total_remaining_length as u32, unsuback_property_section_length as u32))
}

fn get_unsuback_packet_reason_string(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Unsuback, reason_string)
}

fn get_unsuback_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Unsuback(unsuback) = packet {
        if let Some(properties) = &unsuback.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

#[rustfmt::skip]
pub(crate) fn write_unsuback_encoding_steps(packet: &UnsubackPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<()> {
    let (total_remaining_length, unsuback_property_length) = compute_unsuback_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, PACKET_TYPE_UNSUBACK << 4);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    encode_integral_expression!(steps, Uint16, packet.packet_id);
    encode_integral_expression!(steps, Vli, unsuback_property_length);

    encode_optional_string_property!(steps, get_unsuback_packet_reason_string, PROPERTY_KEY_REASON_STRING, packet.reason_string);
    encode_user_properties!(steps, get_unsuback_packet_user_property, packet.user_properties);

    let reason_codes = &packet.reason_codes;
    for reason_code in reason_codes {
        encode_enum!(steps, Uint8, u8, *reason_code);
    }

    Ok(())
}

fn decode_unsuback_properties(property_bytes: &[u8], packet : &mut UnsubackPacket) -> Mqtt5Result<()> {
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

pub(crate) fn decode_unsuback_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<MqttPacket>> {

    if first_byte != (PACKET_TYPE_UNSUBACK << 4) {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let mut box_packet = Box::new(MqttPacket::Unsuback(UnsubackPacket { ..Default::default() }));

    if let MqttPacket::Unsuback(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

        let mut properties_length: usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
        if properties_length > mutable_body.len() {
            return Err(Mqtt5Error::MalformedPacket);
        }

        let properties_bytes = &mutable_body[..properties_length];
        let payload_bytes = &mutable_body[properties_length..];

        decode_unsuback_properties(properties_bytes, packet)?;

        let reason_code_count = payload_bytes.len();
        packet.reason_codes.reserve(reason_code_count);

        for i in 0..reason_code_count {
            packet.reason_codes.push(convert_u8_to_unsuback_reason_code(payload_bytes[i])?);
        }

        return Ok(box_packet);
    }

    Err(Mqtt5Error::Unknown)
}

pub(crate) fn validate_unsuback_packet_fixed(packet: &UnsubackPacket) -> Mqtt5Result<()> {

    validate_user_properties!(properties, &packet.user_properties, UnsubackPacketValidation);
    validate_optional_string_length!(reason_string, &packet.reason_string, UnsubackPacketValidation);

    Ok(())
}

pub(crate) fn validate_unsuback_packet_context_specific(packet: &UnsubackPacket, context: &ValidationContext) -> Mqtt5Result<()> {

    if context.is_outbound {
        /* inbound size is checked on decode, outbound size is checked here */
        let (total_remaining_length, _) = compute_unsuback_packet_length_properties(packet)?;
        let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
        if total_packet_length > context.negotiated_settings.maximum_packet_size_to_server {
            return Err(Mqtt5Error::UnsubackPacketValidation);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn unsuback_round_trip_encode_decode_default() {
        let packet = UnsubackPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsuback(packet)));
    }

    #[test]
    fn unsuback_round_trip_encode_decode_required() {
        let packet = UnsubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                UnsubackReasonCode::ImplementationSpecificError,
                UnsubackReasonCode::Success,
                UnsubackReasonCode::TopicNameInvalid
            ],
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsuback(packet)));
    }

    fn create_unsuback_all_properties() -> UnsubackPacket {
        UnsubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                UnsubackReasonCode::NotAuthorized,
                UnsubackReasonCode::PacketIdentifierInUse,
                UnsubackReasonCode::Success
            ],
            reason_string : Some("Didn't feel like it".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "Time".to_string(), value: "togohome".to_string()},
                UserProperty{name: "Ouch".to_string(), value: "backhurts".to_string()},
            ))
        }
    }

    #[test]
    fn unsuback_round_trip_encode_decode_all() {
        let packet = create_unsuback_all_properties();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsuback(packet)));
    }

    #[test]
    fn unsuback_decode_failure_bad_fixed_header() {
        let packet = UnsubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                UnsubackReasonCode::ImplementationSpecificError,
                UnsubackReasonCode::Success,
                UnsubackReasonCode::TopicNameInvalid
            ],
            ..Default::default()
        };

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Unsuback(packet), 9);
    }

    #[test]
    fn unsuback_decode_failure_reason_code_invalid() {
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

    const UNSUBACK_PACKET_TEST_PROPERTY_LENGTH_INDEX : usize = 4;
    const UNSUBACK_PACKET_TEST_PAYLOAD_INDEX : usize = 12;

    #[test]
    fn unsuback_decode_failure_duplicate_reason_string() {

        let packet = UnsubackPacket {
            packet_id : 1023,
            reason_string: Some("derp".to_string()),
            reason_codes : vec![
                UnsubackReasonCode::UnspecifiedError
            ],
            ..Default::default()
        };

        let duplicate_reason_string = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[1] += 6;
            clone[UNSUBACK_PACKET_TEST_PROPERTY_LENGTH_INDEX] += 6;

            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, 67);
            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, 67);
            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, 67);
            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, 3);
            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, 0);
            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, PROPERTY_KEY_REASON_STRING);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Unsuback(packet), duplicate_reason_string);
    }

    #[test]
    fn unsuback_validate_success() {
        let packet = create_unsuback_all_properties();

        assert_eq!(Ok(()), validate_packet_fixed(&MqttPacket::Unsuback(packet)));
    }

    #[test]
    fn unsuback_validate_failure_reason_string_length() {
        let mut packet = create_unsuback_all_properties();
        packet.reason_string = Some("Derp".repeat(20000).to_string());

        assert_eq!(Err(Mqtt5Error::UnsubackPacketValidation), validate_packet_fixed(&MqttPacket::Unsuback(packet)));
    }

    #[test]
    fn unsuback_validate_failure_user_properties_invalid() {
        let mut packet = create_unsuback_all_properties();
        packet.user_properties = Some(create_invalid_user_properties());

        assert_eq!(Err(Mqtt5Error::UnsubackPacketValidation), validate_packet_fixed(&MqttPacket::Unsuback(packet)));
    }

    use crate::validate::testing::*;

    #[test]
    fn unsuback_validate_context_specific_success() {
        let packet = create_unsuback_all_properties();

        let test_validation_context = create_pinned_validation_context();
        let validation_context = create_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_packet_context_specific(&MqttPacket::Unsuback(packet), &validation_context), Ok(()));
    }

    #[test]
    fn unsuback_validate_context_specific_failure_outbound_size() {
        let packet = create_unsuback_all_properties();

        do_outbound_size_validate_failure_test(&MqttPacket::Unsuback(packet), Mqtt5Error::UnsubackPacketValidation);
    }
}