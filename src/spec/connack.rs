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
pub struct ConnackPacket {
    pub session_present: bool,
    pub reason_code: ConnectReasonCode,

    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_qos: Option<QualityOfService>,
    pub retain_available: Option<bool>,
    pub maximum_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub topic_alias_maximum: Option<u16>,
    pub reason_string: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,

    pub wildcard_subscriptions_available: Option<bool>,
    pub subscription_identifiers_available: Option<bool>,
    pub shared_subscriptions_available: Option<bool>,

    pub server_keep_alive: Option<u16>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
}

#[rustfmt::skip]
fn compute_connack_packet_length_properties(packet: &ConnackPacket) -> Mqtt5Result<(u32, u32), ()> {

    let mut connack_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_u32_property_length!(connack_property_section_length, packet.session_expiry_interval);
    add_optional_u16_property_length!(connack_property_section_length, packet.receive_maximum);
    add_optional_u8_property_length!(connack_property_section_length, packet.maximum_qos);
    add_optional_u8_property_length!(connack_property_section_length, packet.retain_available);
    add_optional_u32_property_length!(connack_property_section_length, packet.maximum_packet_size);
    add_optional_string_property_length!(connack_property_section_length, packet.assigned_client_identifier);
    add_optional_u16_property_length!(connack_property_section_length, packet.topic_alias_maximum);
    add_optional_string_property_length!(connack_property_section_length, packet.reason_string);
    add_optional_u8_property_length!(connack_property_section_length, packet.wildcard_subscriptions_available);
    add_optional_u8_property_length!(connack_property_section_length, packet.subscription_identifiers_available);
    add_optional_u8_property_length!(connack_property_section_length, packet.shared_subscriptions_available);
    add_optional_u16_property_length!(connack_property_section_length, packet.server_keep_alive);
    add_optional_string_property_length!(connack_property_section_length, packet.response_information);
    add_optional_string_property_length!(connack_property_section_length, packet.server_reference);
    add_optional_string_property_length!(connack_property_section_length, packet.authentication_method);
    add_optional_bytes_property_length!(connack_property_section_length, packet.authentication_data);

    let mut total_remaining_length : usize = compute_variable_length_integer_encode_size(connack_property_section_length)?;

    total_remaining_length += 2;
    total_remaining_length += connack_property_section_length;

    Ok((total_remaining_length as u32, connack_property_section_length as u32))
}

fn get_connack_packet_assigned_client_identifier(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connack, assigned_client_identifier)
}

fn get_connack_packet_reason_string(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connack, reason_string)
}

fn get_connack_packet_response_information(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connack, response_information)
}

fn get_connack_packet_server_reference(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connack, server_reference)
}

fn get_connack_packet_authentication_method(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connack, authentication_method)
}

fn get_connack_packet_authentication_data(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Connack, authentication_data)
}

fn get_connack_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Connack(connack) = packet {
        if let Some(properties) = &connack.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

#[rustfmt::skip]
pub(crate) fn write_connack_encoding_steps(packet: &ConnackPacket, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
    let (total_remaining_length, connack_property_length) = compute_connack_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, PACKET_TYPE_CONNACK << 4);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    /*
     * Variable Header
     * 1 byte flags
     * 1 byte reason code
     * 1-4 byte Property Length as Variable Byte Integer
     * n bytes Properties
     */
    encode_integral_expression!(steps, Uint8, if packet.session_present { 1 } else { 0 });
    encode_enum!(steps, Uint8, u8, packet.reason_code);
    encode_integral_expression!(steps, Vli, connack_property_length);

    encode_optional_property!(steps, Uint32, PROPERTY_KEY_SESSION_EXPIRY_INTERVAL, packet.session_expiry_interval);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_RECEIVE_MAXIMUM, packet.receive_maximum);
    encode_optional_enum_property!(steps, Uint8, PROPERTY_KEY_MAXIMUM_QOS, u8, packet.maximum_qos);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_RETAIN_AVAILABLE, packet.retain_available);
    encode_optional_property!(steps, Uint32, PROPERTY_KEY_MAXIMUM_PACKET_SIZE, packet.maximum_packet_size);
    encode_optional_string_property!(steps, get_connack_packet_assigned_client_identifier, PROPERTY_KEY_ASSIGNED_CLIENT_IDENTIFIER, packet.assigned_client_identifier);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM, packet.topic_alias_maximum);
    encode_optional_string_property!(steps, get_connack_packet_reason_string, PROPERTY_KEY_REASON_STRING, packet.reason_string);
    encode_user_properties!(steps, get_connack_packet_user_property, packet.user_properties);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_WILDCARD_SUBSCRIPTIONS_AVAILABLE, packet.wildcard_subscriptions_available);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE, packet.subscription_identifiers_available);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_SHARED_SUBSCRIPTIONS_AVAILABLE, packet.shared_subscriptions_available);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_SERVER_KEEP_ALIVE, packet.server_keep_alive);
    encode_optional_string_property!(steps, get_connack_packet_response_information, PROPERTY_KEY_RESPONSE_INFORMATION, packet.response_information);
    encode_optional_string_property!(steps, get_connack_packet_server_reference, PROPERTY_KEY_SERVER_REFERENCE, packet.server_reference);
    encode_optional_string_property!(steps, get_connack_packet_authentication_method, PROPERTY_KEY_AUTHENTICATION_METHOD, packet.authentication_method);
    encode_optional_bytes_property!(steps, get_connack_packet_authentication_data, PROPERTY_KEY_AUTHENTICATION_DATA, packet.authentication_data);

    Ok(())
}


fn decode_connack_properties(property_bytes: &[u8], packet : &mut ConnackPacket) -> Mqtt5Result<(), ()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_SESSION_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.session_expiry_interval)?; }
            PROPERTY_KEY_RECEIVE_MAXIMUM => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.receive_maximum)?; }
            PROPERTY_KEY_MAXIMUM_QOS => { mutable_property_bytes = decode_optional_u8_as_enum(mutable_property_bytes, &mut packet.maximum_qos, convert_u8_to_quality_of_service)?; }
            PROPERTY_KEY_RETAIN_AVAILABLE => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.retain_available)?; }
            PROPERTY_KEY_MAXIMUM_PACKET_SIZE => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.maximum_packet_size)?; }
            PROPERTY_KEY_ASSIGNED_CLIENT_IDENTIFIER => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.assigned_client_identifier)?; }
            PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.topic_alias_maximum)?; }
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            PROPERTY_KEY_WILDCARD_SUBSCRIPTIONS_AVAILABLE => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.wildcard_subscriptions_available)?; }
            PROPERTY_KEY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.subscription_identifiers_available)?; }
            PROPERTY_KEY_SHARED_SUBSCRIPTIONS_AVAILABLE => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.shared_subscriptions_available)?; }
            PROPERTY_KEY_SERVER_KEEP_ALIVE => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.server_keep_alive)?; }
            PROPERTY_KEY_RESPONSE_INFORMATION => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.response_information)?; }
            PROPERTY_KEY_SERVER_REFERENCE => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.server_reference)?; }
            PROPERTY_KEY_AUTHENTICATION_METHOD => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.authentication_method)?; }
            PROPERTY_KEY_AUTHENTICATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut packet.authentication_data)?; }
            _ => { return Err(Mqtt5Error::MalformedPacket); }
        }
    }

    Ok(())
}

pub(crate) fn decode_connack_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<ConnackPacket, ()> {
    let mut packet = ConnackPacket { ..Default::default() };

    if first_byte != (PACKET_TYPE_CONNACK << 4) {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let mut mutable_body = packet_body;
    if mutable_body.len() == 0 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let flags : u8 = mutable_body[0];
    mutable_body = &mutable_body[1..];

    if flags == 1 {
        packet.session_present = true;
    } else if flags != 0 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    mutable_body = decode_u8_as_enum(mutable_body, &mut packet.reason_code, convert_u8_to_connect_reason_code)?;

    let mut properties_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
    if properties_length != mutable_body.len() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    decode_connack_properties(mutable_body, &mut packet)?;

    Ok(packet)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::testing::*;


    #[test]
    fn connack_round_trip_encode_decode_default() {
        let packet = ConnackPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connack(packet)));
    }

    #[test]
    fn connack_round_trip_encode_decode_required() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Banned,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connack(packet)));
    }

    #[test]
    fn connack_round_trip_encode_decode_all() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::NotAuthorized,

            session_expiry_interval: Some(7200),
            receive_maximum: Some(200),
            maximum_qos: Some(QualityOfService::AtLeastOnce),
            retain_available: Some(true),
            maximum_packet_size: Some(256 * 1024),
            assigned_client_identifier: Some("I dub thee Stinky".to_string()),
            topic_alias_maximum: Some(30),
            reason_string: Some("You're sketchy.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "Go".to_string(), value: "Away".to_string()},
                UserProperty{name: "".to_string(), value: "Uff da".to_string()},
            )),
            wildcard_subscriptions_available: Some(true),
            subscription_identifiers_available:Some(false),
            shared_subscriptions_available: Some(true),
            server_keep_alive: Some(1600),
            response_information: Some("We/care/a/lot".to_string()),
            server_reference: Some("lolcats.com".to_string()),
            authentication_method: Some("Sekrit".to_string()),
            authentication_data: Some("TopSekrit".as_bytes().to_vec()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connack(packet)));
    }
}
