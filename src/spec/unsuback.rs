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
pub(crate) fn write_unsuback_encoding_steps(packet: &UnsubackPacket, _: &mut EncodingContext, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<()> {
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

pub(crate) fn decode_unsuback_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<UnsubackPacket>> {
    let mut packet = Box::new(UnsubackPacket { ..Default::default() });

    if first_byte != (PACKET_TYPE_UNSUBACK << 4) {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let mut mutable_body = packet_body;
    mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

    let mut properties_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
    if properties_length > mutable_body.len() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let properties_bytes = &mutable_body[..properties_length];
    let payload_bytes = &mutable_body[properties_length..];

    decode_unsuback_properties(properties_bytes, &mut packet)?;

    let reason_code_count = payload_bytes.len();
    packet.reason_codes.reserve(reason_code_count);

    for i in 0..reason_code_count {
        packet.reason_codes.push(convert_u8_to_unsuback_reason_code(payload_bytes[i])?);
    }

    Ok(packet)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn unsuback_round_trip_encode_decode_default() {
        let packet = Box::new(UnsubackPacket {
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsuback(packet)));
    }

    #[test]
    fn unsuback_round_trip_encode_decode_required() {
        let packet = Box::new(UnsubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                UnsubackReasonCode::ImplementationSpecificError,
                UnsubackReasonCode::Success,
                UnsubackReasonCode::TopicNameInvalid
            ],
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsuback(packet)));
    }

    #[test]
    fn unsuback_round_trip_encode_decode_all() {
        let packet = Box::new(UnsubackPacket {
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
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsuback(packet)));
    }

    #[test]
    fn unsuback_decode_failure_bad_fixed_header() {
        let packet = Box::new(UnsubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                UnsubackReasonCode::ImplementationSpecificError,
                UnsubackReasonCode::Success,
                UnsubackReasonCode::TopicNameInvalid
            ],
            ..Default::default()
        });

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Unsuback(packet), 9);
    }
}