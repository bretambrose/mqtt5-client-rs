/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::decoding_utils::*;
use crate::encoding_utils::*;
use crate::spec::*;
use crate::spec_impl::*;

use std::collections::VecDeque;

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct DisconnectPacket {
    pub reason_code: DisconnectReasonCode,

    pub session_expiry_interval_seconds: Option<u32>,
    pub reason_string: Option<String>,
    pub user_properties: Option<Vec<UserProperty>>,
    pub server_reference: Option<String>,
}

#[rustfmt::skip]
fn compute_disconnect_packet_length_properties(packet: &DisconnectPacket) -> Mqtt5Result<(u32, u32), ()> {
    let mut disconnect_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_u32_property_length!(disconnect_property_section_length, packet.session_expiry_interval_seconds);
    add_optional_string_property_length!(disconnect_property_section_length, packet.reason_string);
    add_optional_string_property_length!(disconnect_property_section_length, packet.server_reference);

    if disconnect_property_section_length == 0 && packet.reason_code == DisconnectReasonCode::NormalDisconnection {
        return Ok((0, 0));
    }

    let mut total_remaining_length : usize = 1 + compute_variable_length_integer_encode_size(disconnect_property_section_length)?;
    total_remaining_length += disconnect_property_section_length;

    Ok((total_remaining_length as u32, disconnect_property_section_length as u32))
}

fn get_disconnect_packet_reason_string(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Disconnect, reason_string)
}

fn get_disconnect_packet_server_reference(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Disconnect, server_reference)
}

fn get_disconnect_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Disconnect(disconnect) = packet {
        if let Some(properties) = &disconnect.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

#[rustfmt::skip]
pub(crate) fn write_disconnect_encoding_steps(packet: &DisconnectPacket, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
    let (total_remaining_length, disconnect_property_length) = compute_disconnect_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, PACKET_TYPE_DISCONNECT << 4);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    if disconnect_property_length == 0 && packet.reason_code == DisconnectReasonCode::NormalDisconnection {
        assert_eq!(0, total_remaining_length);
        return Ok(());
    }

    encode_enum!(steps, Uint8, u8, packet.reason_code);
    encode_integral_expression!(steps, Vli, disconnect_property_length);

    encode_optional_property!(steps, Uint32, PROPERTY_KEY_SESSION_EXPIRY_INTERVAL, packet.session_expiry_interval_seconds);
    encode_optional_string_property!(steps, get_disconnect_packet_reason_string, PROPERTY_KEY_REASON_STRING, packet.reason_string);
    encode_optional_string_property!(steps, get_disconnect_packet_server_reference, PROPERTY_KEY_SERVER_REFERENCE, packet.server_reference);
    encode_user_properties!(steps, get_disconnect_packet_user_property, packet.user_properties);

    Ok(())
}

fn decode_disconnect_properties(property_bytes: &[u8], packet : &mut DisconnectPacket) -> Mqtt5Result<(), ()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_SESSION_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.session_expiry_interval_seconds)?; }
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            PROPERTY_KEY_SERVER_REFERENCE => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.server_reference)?; }
            _ => { return Err(Mqtt5Error::MalformedPacket); }
        }
    }

    Ok(())
}

pub(crate) fn decode_disconnect_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<DisconnectPacket, ()> {
    let mut packet = DisconnectPacket { ..Default::default() };

    if first_byte != (PACKET_TYPE_DISCONNECT << 4) {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let mut mutable_body = packet_body;
    if mutable_body.len() == 0 {
        return Ok(packet);
    }

    mutable_body = decode_u8_as_enum(mutable_body, &mut packet.reason_code, convert_u8_to_disconnect_reason_code)?;

    let mut properties_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
    if properties_length != mutable_body.len() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    decode_disconnect_properties(mutable_body, &mut packet)?;

    Ok(packet)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decoder::testing::*;

    #[test]
    fn disconnect_round_trip_encode_decode_default() {
        let packet = DisconnectPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Disconnect(packet)));
    }

    #[test]
    fn disconnect_round_trip_encode_decode_normal_reason_code() {
        let packet = DisconnectPacket {
            reason_code : DisconnectReasonCode::NormalDisconnection,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Disconnect(packet)));
    }

    #[test]
    fn disconnect_round_trip_encode_decode_abnormal_reason_code() {
        let packet = DisconnectPacket {
            reason_code : DisconnectReasonCode::ConnectionRateExceeded,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Disconnect(packet)));
    }

    #[test]
    fn disconnect_round_trip_encode_decode_all_properties() {
        let packet = DisconnectPacket {
            reason_code : DisconnectReasonCode::ConnectionRateExceeded,
            reason_string : Some("I don't like you".to_string()),
            server_reference : Some("far.far.away.com".to_string()),
            session_expiry_interval_seconds : Some(14400),
            user_properties: Some(vec!(
                UserProperty{name: "Super".to_string(), value: "Meatboy".to_string()},
                UserProperty{name: "Minsc".to_string(), value: "Boo".to_string()},
            )),
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Disconnect(packet)));
    }
}