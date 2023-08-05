/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::{Mqtt5Error, Mqtt5Result};
use crate::decoding_utils::*;
use crate::spec::*;
use crate::spec_impl::*;

const DECODE_BUFFER_DEFAULT_SIZE : usize = 16 * 1024;

#[derive(Copy, Clone, Eq, PartialEq)]
enum DecoderState {
    ReadPacketType,
    ReadTotalRemainingLength,
    ReadPacketBody,
    ProtocolError
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum DecoderDirective {
    OutOfData,
    Continue,
    ProtocolError
}

pub struct DecoderOptions {
    packet_stream: std::sync::mpsc::Sender<MqttPacket>
}

pub struct Decoder {
    config: DecoderOptions,

    state: DecoderState,

    scratch: Vec<u8>,

    first_byte: Option<u8>,

    remaining_length : Option<usize>,
}

fn decode_connect_properties(property_bytes: &[u8], packet : &mut ConnectPacket) -> Mqtt5Result<(), ()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_SESSION_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.session_expiry_interval_seconds)?; }
            PROPERTY_KEY_RECEIVE_MAXIMUM => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.receive_maximum)?; }
            PROPERTY_KEY_MAXIMUM_PACKET_SIZE => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.maximum_packet_size_bytes)?; }
            PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.topic_alias_maximum)?; }
            PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.request_response_information)?; }
            PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.request_problem_information)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            PROPERTY_KEY_AUTHENTICATION_METHOD => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.authentication_method)?; }
            PROPERTY_KEY_AUTHENTICATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut packet.authentication_data)?; }
            _ => { return Err(Mqtt5Error::ProtocolError); }
        }
    }

    Ok(())
}

fn decode_will_properties(property_bytes: &[u8], will: &mut PublishPacket, connect : &mut ConnectPacket) -> Mqtt5Result<(), ()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_WILL_DELAY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut connect.will_delay_interval_seconds)?; }
            PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR => { mutable_property_bytes = decode_optional_u8_as_enum(mutable_property_bytes, &mut will.payload_format, convert_u8_to_payload_format_indicator)?; }
            PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut will.message_expiry_interval_seconds)?; }
            PROPERTY_KEY_CONTENT_TYPE => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut will.content_type)?; }
            PROPERTY_KEY_RESPONSE_TOPIC => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut will.response_topic)?; }
            PROPERTY_KEY_CORRELATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut will.correlation_data)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut will.user_properties)?; }
            _ => { return Err(Mqtt5Error::ProtocolError); }
        }
    }

    Ok(())
}

const CONNECT_HEADER_PROTOCOL_LENGTH : usize = 7;

fn decode_connect_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<ConnectPacket, ()> {
    let mut packet = ConnectPacket { ..Default::default() };

    if first_byte != (PACKET_TYPE_CONNECT << 4)  {
        return Err(Mqtt5Error::ProtocolError);
    }

    let mut mutable_body = packet_body;
    if mutable_body.len() < CONNECT_HEADER_PROTOCOL_LENGTH {
        return Err(Mqtt5Error::ProtocolError);
    }

    let protocol_bytes = &mutable_body[..CONNECT_HEADER_PROTOCOL_LENGTH];
    mutable_body = &mutable_body[CONNECT_HEADER_PROTOCOL_LENGTH..];

    match protocol_bytes {
        [0u8, 4u8, 77u8, 81u8, 84u8, 84u8, 5u8] => { }
        _ => { return Err(Mqtt5Error::ProtocolError); }
    }

    /*
    if protocol_bytes == CONNECT_HEADER_PROTOCOL_BYTES.as_slice() {
        return Err(Mqtt5Error::ProtocolError);
    }*/

    let mut connect_flags : u8 = 0;
    mutable_body = decode_u8(mutable_body, &mut connect_flags)?;

    packet.clean_start = (connect_flags & CONNECT_PACKET_CLEAN_START_FLAG_MASK) != 0;
    let has_will = (connect_flags & CONNECT_PACKET_HAS_WILL_FLAG_MASK) != 0;
    let will_retain = (connect_flags & CONNECT_PACKET_WILL_RETAIN_FLAG_MASK) != 0;
    let will_qos = convert_u8_to_quality_of_service((connect_flags >> CONNECT_PACKET_WILL_QOS_FLAG_SHIFT) & QOS_MASK)?;

    if !has_will {
        /* indirectly check bits of connect flags vs. spec */
        if will_retain || will_qos != QualityOfService::AtMostOnce {
            return Err(Mqtt5Error::ProtocolError);
        }
    }

    let has_username = (connect_flags & CONNECT_PACKET_HAS_USERNAME_FLAG_MASK) != 0;
    let has_password = (connect_flags & CONNECT_PACKET_HAS_PASSWORD_FLAG_MASK) != 0;

    mutable_body = decode_u16(mutable_body, &mut packet.keep_alive_interval_seconds)?;

    let mut connect_property_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut connect_property_length)?;

    if mutable_body.len() < connect_property_length {
        return Err(Mqtt5Error::ProtocolError);
    }

    let property_body = &mutable_body[..connect_property_length];
    mutable_body = &mutable_body[connect_property_length..];

    decode_connect_properties(property_body, &mut packet)?;

    mutable_body = decode_length_prefixed_optional_string(mutable_body, &mut packet.client_id)?;

    if has_will {
        let mut will_property_length : usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut will_property_length)?;

        if mutable_body.len() < will_property_length {
            return Err(Mqtt5Error::ProtocolError);
        }

        let will_property_body = &mutable_body[..will_property_length];
        mutable_body = &mutable_body[will_property_length..];

        let mut will : PublishPacket = PublishPacket {
            qos : will_qos,
            retain : will_retain,
            ..Default::default()
        };

        decode_will_properties(will_property_body, &mut will, &mut packet)?;

        mutable_body = decode_length_prefixed_string(mutable_body, &mut will.topic)?;
        mutable_body = decode_length_prefixed_optional_bytes(mutable_body, &mut will.payload)?;

        packet.will = Some(will);
    }

    if has_username {
        mutable_body = decode_optional_length_prefixed_string(mutable_body, &mut packet.username)?;
    }

    if has_password {
        mutable_body = decode_optional_length_prefixed_bytes(mutable_body, &mut packet.password)?;
    }

    if mutable_body.len() > 0 {
        return Err(Mqtt5Error::ProtocolError);
    }

    Ok(packet)
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
            _ => { return Err(Mqtt5Error::ProtocolError); }
        }
    }

    if let Some(qos) = packet.maximum_qos {
        if qos == QualityOfService::ExactlyOnce {
            return Err(Mqtt5Error::ProtocolError);
        }
    }

    Ok(())
}

fn decode_connack_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<ConnackPacket, ()> {
    let mut packet = ConnackPacket { ..Default::default() };

    if first_byte != (PACKET_TYPE_CONNACK << 4) {
        return Err(Mqtt5Error::ProtocolError);
    }

    let mut mutable_body = packet_body;
    if mutable_body.len() == 0 {
        return Err(Mqtt5Error::ProtocolError);
    }

    let flags : u8 = mutable_body[0];
    mutable_body = &mutable_body[1..];

    if flags == 1 {
        packet.session_present = true;
    } else if flags != 0 {
        return Err(Mqtt5Error::ProtocolError);
    }

    mutable_body = decode_u8_as_enum(mutable_body, &mut packet.reason_code, convert_u8_to_connect_reason_code)?;

    let mut properties_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
    if properties_length != mutable_body.len() {
        return Err(Mqtt5Error::ProtocolError);
    }

    decode_connack_properties(mutable_body, &mut packet)?;

    Ok(packet)
}

fn decode_publish_properties(property_bytes: &[u8], packet : &mut PublishPacket) -> Mqtt5Result<(), ()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR => { mutable_property_bytes = decode_optional_u8_as_enum(mutable_property_bytes, &mut packet.payload_format, convert_u8_to_payload_format_indicator)?; }
            PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.message_expiry_interval_seconds)?; }
            PROPERTY_KEY_TOPIC_ALIAS => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.topic_alias)?; }
            PROPERTY_KEY_RESPONSE_TOPIC => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.response_topic)?; }
            PROPERTY_KEY_CORRELATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut packet.correlation_data)?; }
            PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER => {
                let mut subscription_id : usize = 0;
                mutable_property_bytes = decode_vli_into_mutable(mutable_property_bytes, &mut subscription_id)?;
                if packet.subscription_identifiers.is_none() {
                    packet.subscription_identifiers = Some(Vec::new());
                }

                let ids : &mut Vec<u32> = &mut packet.subscription_identifiers.as_mut().unwrap();
                ids.push(subscription_id as u32);
            }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            PROPERTY_KEY_CONTENT_TYPE => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.content_type)?; }
            _ => { return Err(Mqtt5Error::ProtocolError); }
        }
    }

    Ok(())
}

fn decode_publish_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PublishPacket, ()> {
    let mut packet = PublishPacket { ..Default::default() };

    if (first_byte & PUBLISH_PACKET_FIXED_HEADER_DUPLICATE_FLAG) != 0 {
        packet.duplicate = true;
    }

    if (first_byte & PUBLISH_PACKET_FIXED_HEADER_RETAIN_FLAG) != 0 {
        packet.retain = true;
    }

    packet.qos = convert_u8_to_quality_of_service((first_byte >> 1) & QOS_MASK)?;

    let mut mutable_body = packet_body;
    let mut properties_length : usize = 0;

    mutable_body = decode_length_prefixed_string(mutable_body, &mut packet.topic)?;

    if packet.qos != QualityOfService::AtMostOnce {
        mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;
    }

    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;

    let properties_bytes = &mutable_body[..properties_length];
    let payload_bytes = &mutable_body[properties_length..];

    decode_publish_properties(properties_bytes, &mut packet)?;

    if payload_bytes.len() > 0 {
        packet.payload = Some(payload_bytes.to_vec());
    }

    Ok(packet)
}

define_ack_packet_decode_properties_function!(decode_puback_properties, PubackPacket);
define_ack_packet_decode_function!(decode_puback_packet, PubackPacket, PACKET_TYPE_PUBACK, convert_u8_to_puback_reason_code, decode_puback_properties);

define_ack_packet_decode_properties_function!(decode_pubrec_properties, PubrecPacket);
define_ack_packet_decode_function!(decode_pubrec_packet, PubrecPacket, PACKET_TYPE_PUBREC, convert_u8_to_pubrec_reason_code, decode_pubrec_properties);

define_ack_packet_decode_properties_function!(decode_pubrel_properties, PubrelPacket);
define_ack_packet_decode_function!(decode_pubrel_packet, PubrelPacket, PACKET_TYPE_PUBREL, convert_u8_to_pubrel_reason_code, decode_pubrel_properties);

define_ack_packet_decode_properties_function!(decode_pubcomp_properties, PubcompPacket);
define_ack_packet_decode_function!(decode_pubcomp_packet, PubcompPacket, PACKET_TYPE_PUBCOMP, convert_u8_to_pubcomp_reason_code, decode_pubcomp_properties);

fn decode_subscribe_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<SubscribePacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_suback_properties(property_bytes: &[u8], packet : &mut SubackPacket) -> Mqtt5Result<(), ()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => { return Err(Mqtt5Error::ProtocolError); }
        }
    }

    Ok(())
}

fn decode_suback_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<SubackPacket, ()> {
    let mut packet = SubackPacket { ..Default::default() };

    if first_byte != (PACKET_TYPE_SUBACK << 4) {
        return Err(Mqtt5Error::ProtocolError);
    }

    let mut mutable_body = packet_body;
    mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

    let mut properties_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;

    let properties_bytes = &mutable_body[..properties_length];
    let payload_bytes = &mutable_body[properties_length..];

    decode_suback_properties(properties_bytes, &mut packet)?;

    let reason_code_count = payload_bytes.len();
    packet.reason_codes.reserve(reason_code_count);

    for i in 0..reason_code_count {
        packet.reason_codes.push(convert_u8_to_suback_reason_code(payload_bytes[i])?);
    }

    Ok(packet)
}

fn decode_unsubscribe_properties(property_bytes: &[u8], packet : &mut UnsubscribePacket) -> Mqtt5Result<(), ()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => { return Err(Mqtt5Error::ProtocolError); }
        }
    }

    Ok(())
}

fn decode_unsubscribe_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<UnsubscribePacket, ()> {
    let mut packet = UnsubscribePacket { ..Default::default() };

    if first_byte != UNSUBSCRIBE_FIRST_BYTE {
        return Err(Mqtt5Error::ProtocolError);
    }

    let mut mutable_body = packet_body;
    mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

    let mut properties_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;

    let properties_bytes = &mutable_body[..properties_length];
    let mut payload_bytes = &mutable_body[properties_length..];

    decode_unsubscribe_properties(properties_bytes, &mut packet)?;

    while payload_bytes.len() > 0 {
        let mut topic_filter = String::new();
        payload_bytes = decode_length_prefixed_string(payload_bytes, &mut topic_filter)?;

        packet.topic_filters.push(topic_filter);
    }

    Ok(packet)
}

fn decode_unsuback_properties(property_bytes: &[u8], packet : &mut UnsubackPacket) -> Mqtt5Result<(), ()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => { return Err(Mqtt5Error::ProtocolError); }
        }
    }

    Ok(())
}

fn decode_unsuback_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<UnsubackPacket, ()> {
    let mut packet = UnsubackPacket { ..Default::default() };

    if first_byte != (PACKET_TYPE_UNSUBACK << 4) {
        return Err(Mqtt5Error::ProtocolError);
    }

    let mut mutable_body = packet_body;
    mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

    let mut properties_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;

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

const PINGREQ_FIRST_BYTE : u8 = PACKET_TYPE_PINGREQ << 4;

fn decode_pingreq_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PingreqPacket, ()> {
    if packet_body.len() != 0 {
        return Err(Mqtt5Error::ProtocolError);
    }

    if first_byte != PINGREQ_FIRST_BYTE {
        return Err(Mqtt5Error::ProtocolError);
    }

    return Ok(PingreqPacket{});
}

const PINGRESP_FIRST_BYTE : u8 = PACKET_TYPE_PINGRESP << 4;

fn decode_pingresp_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PingrespPacket, ()> {
    if packet_body.len() != 0 {
        return Err(Mqtt5Error::ProtocolError);
    }

    if first_byte != PINGRESP_FIRST_BYTE {
        return Err(Mqtt5Error::ProtocolError);
    }

    return Ok(PingrespPacket{});
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
            _ => { return Err(Mqtt5Error::ProtocolError); }
        }
    }

    Ok(())
}

fn decode_disconnect_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<DisconnectPacket, ()> {
    let mut packet = DisconnectPacket { ..Default::default() };

    if first_byte != (PACKET_TYPE_DISCONNECT << 4) {
        return Err(Mqtt5Error::ProtocolError);
    }

    let mut mutable_body = packet_body;
    if mutable_body.len() == 0 {
        return Ok(packet);
    }

    mutable_body = decode_u8_as_enum(mutable_body, &mut packet.reason_code, convert_u8_to_disconnect_reason_code)?;

    let mut properties_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
    if properties_length != mutable_body.len() {
        return Err(Mqtt5Error::ProtocolError);
    }

    decode_disconnect_properties(mutable_body, &mut packet)?;

    Ok(packet)
}

fn decode_auth_properties(property_bytes: &[u8], packet : &mut AuthPacket) -> Mqtt5Result<(), ()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_AUTHENTICATION_METHOD => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.authentication_method)?; }
            PROPERTY_KEY_AUTHENTICATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut packet.authentication_data)?; }
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => { return Err(Mqtt5Error::ProtocolError); }
        }
    }

    Ok(())
}

fn decode_auth_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<AuthPacket, ()> {
    let mut packet = AuthPacket { ..Default::default() };

    if first_byte != (PACKET_TYPE_AUTH << 4) {
        return Err(Mqtt5Error::ProtocolError);
    }

    let mut mutable_body = packet_body;

    mutable_body = decode_u8_as_enum(mutable_body, &mut packet.reason_code, convert_u8_to_authenticate_reason_code)?;

    let mut properties_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
    if properties_length != mutable_body.len() {
        return Err(Mqtt5Error::ProtocolError);
    }

    decode_auth_properties(mutable_body, &mut packet)?;

    Ok(packet)
}

macro_rules! decode_packet_by_type {
    ($decode_function: ident, $packet_type: ident, $first_byte: ident, $packet_body: ident) => {
        match $decode_function($first_byte, $packet_body) {
            Ok(packet) => { return Ok(MqttPacket::$packet_type(packet)); }
            Err(err) => { return Err(err); }
        }
    };
}

fn decode_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<MqttPacket, ()> {
    let packet_type = first_byte >> 4;

    match packet_type {
        PACKET_TYPE_CONNECT => { decode_packet_by_type!(decode_connect_packet, Connect, first_byte, packet_body) }
        PACKET_TYPE_CONNACK => { decode_packet_by_type!(decode_connack_packet, Connack, first_byte, packet_body) }
        PACKET_TYPE_PUBLISH => { decode_packet_by_type!(decode_publish_packet, Publish, first_byte, packet_body) }
        PACKET_TYPE_PUBACK => { decode_packet_by_type!(decode_puback_packet, Puback, first_byte, packet_body) }
        PACKET_TYPE_PUBREC => { decode_packet_by_type!(decode_pubrec_packet, Pubrec, first_byte, packet_body) }
        PACKET_TYPE_PUBREL => { decode_packet_by_type!(decode_pubrel_packet, Pubrel, first_byte, packet_body) }
        PACKET_TYPE_PUBCOMP => { decode_packet_by_type!(decode_pubcomp_packet, Pubcomp, first_byte, packet_body) }
        PACKET_TYPE_SUBSCRIBE => { decode_packet_by_type!(decode_subscribe_packet, Subscribe, first_byte, packet_body) }
        PACKET_TYPE_SUBACK => { decode_packet_by_type!(decode_suback_packet, Suback, first_byte, packet_body) }
        PACKET_TYPE_UNSUBSCRIBE => { decode_packet_by_type!(decode_unsubscribe_packet, Unsubscribe, first_byte, packet_body) }
        PACKET_TYPE_UNSUBACK => { decode_packet_by_type!(decode_unsuback_packet, Unsuback, first_byte, packet_body) }
        PACKET_TYPE_PINGREQ => { decode_packet_by_type!(decode_pingreq_packet, Pingreq, first_byte, packet_body) }
        PACKET_TYPE_PINGRESP => { decode_packet_by_type!(decode_pingresp_packet, Pingresp, first_byte, packet_body) }
        PACKET_TYPE_DISCONNECT => { decode_packet_by_type!(decode_disconnect_packet, Disconnect, first_byte, packet_body) }
        PACKET_TYPE_AUTH => { decode_packet_by_type!(decode_auth_packet, Auth, first_byte, packet_body) }
        _ => {
            return Err(Mqtt5Error::Unimplemented(()));
        }
    }
}

impl Decoder {
    pub fn new(options: DecoderOptions) -> Decoder {
        Decoder {
            config: options,
            state: DecoderState::ReadPacketType,
            scratch : Vec::<u8>::with_capacity(DECODE_BUFFER_DEFAULT_SIZE),
            first_byte : None,
            remaining_length : None,
        }
    }

    pub fn reset_for_new_connection(&mut self) {
        self.reset();
    }

    fn process_read_packet_type<'a>(&mut self, bytes: &'a [u8]) -> (DecoderDirective, &'a[u8]) {
        if bytes.len() == 0 {
            return (DecoderDirective::OutOfData, bytes);
        }

        self.first_byte = Some(bytes[0]);
        self.state = DecoderState::ReadTotalRemainingLength;

        return  (DecoderDirective::Continue, &bytes[1..]);
    }

    fn process_read_total_remaining_length<'a>(&mut self, bytes: &'a[u8]) -> (DecoderDirective, &'a[u8]) {
        if bytes.len() == 0 {
            return (DecoderDirective::OutOfData, bytes);
        }

        self.scratch.push(bytes[0]);
        let remaining_bytes = &bytes[1..];

        let decode_vli_result = decode_vli(&self.scratch);
        if let Ok(DecodeVliResult::Value(remaining_length, _)) = decode_vli_result {
            self.remaining_length = Some(remaining_length as usize);
            self.state = DecoderState::ReadPacketBody;
            self.scratch.clear();
            return (DecoderDirective::Continue, remaining_bytes);
        } else if self.scratch.len() >= 4 {
            return (DecoderDirective::ProtocolError, remaining_bytes);
        } else if remaining_bytes.len() > 0 {
            return (DecoderDirective::Continue, remaining_bytes);
        } else {
            return (DecoderDirective::OutOfData, remaining_bytes);
        }
    }

    fn process_read_packet_body<'a>(&mut self, bytes: &'a[u8]) -> (DecoderDirective, &'a[u8]) {
        let read_so_far = self.scratch.len();
        let bytes_needed = self.remaining_length.unwrap() - read_so_far;
        if bytes_needed > bytes.len() {
            self.scratch.extend_from_slice(bytes);
            return (DecoderDirective::OutOfData, &[]);
        }

        let packet_slice : &[u8];
        if self.scratch.len() > 0 {
            self.scratch.extend_from_slice(&bytes[..bytes_needed]);
            packet_slice = &self.scratch;
        } else {
            packet_slice = &bytes[..bytes_needed];
        }

        if let Ok(packet) = decode_packet(self.first_byte.unwrap(), packet_slice) {
            if self.config.packet_stream.send(packet).is_err() {
                return (DecoderDirective::ProtocolError, &[]);
            }

            self.reset_for_new_packet();
            return (DecoderDirective::Continue, &bytes[bytes_needed..]);
        }

        return (DecoderDirective::ProtocolError, &[]);
    }

    pub fn decode_bytes(&mut self, bytes: &[u8]) -> Mqtt5Result<(), ()> {
        let mut current_slice = bytes;

        let mut decode_result = DecoderDirective::Continue;
        while decode_result == DecoderDirective::Continue {
            match self.state {
                DecoderState::ReadPacketType => {
                    (decode_result, current_slice) = self.process_read_packet_type(current_slice);
                }

                DecoderState::ReadTotalRemainingLength => {
                    (decode_result, current_slice) = self.process_read_total_remaining_length(current_slice);
                }

                DecoderState::ReadPacketBody => {
                    (decode_result, current_slice) = self.process_read_packet_body(current_slice);
                }

                _ => {
                    decode_result = DecoderDirective::ProtocolError;
                }
            }
        }

        if decode_result == DecoderDirective::ProtocolError {
            self.state = DecoderState::ProtocolError;
            return Err(Mqtt5Error::ProtocolError);
        }

        Ok(())
    }

    fn reset_for_new_packet(&mut self) {
        if self.state != DecoderState::ProtocolError {
            self.reset();
        }
    }

    fn reset(&mut self) {
        self.state = DecoderState::ReadPacketType;
        self.scratch.clear();
        self.first_byte = None;
        self.remaining_length = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::TryRecvError;
    use super::*;
    use crate::encoder::*;

    #[test]
    fn create_decoder() {
        let (packet_sender, _) = std::sync::mpsc::channel();

        let options = DecoderOptions {
            packet_stream : packet_sender
        };

        let mut decoder = Decoder::new(options);
        decoder.reset_for_new_connection();

    }

    fn do_single_encode_decode_test(packet : &MqttPacket, encode_size : usize, decode_size : usize, encode_repetitions : u32) -> bool {

        let mut encoder = Encoder::new();

        let mut full_encoded_stream = Vec::with_capacity( 128 * 1024);
        let mut encode_buffer = Vec::with_capacity(encode_size);

        /* encode 5 copies of the packet */
        for _ in 0..encode_repetitions {
            assert!(!encoder.reset(&packet).is_err());

            let mut cumulative_result : EncodeResult = EncodeResult::Full;
            while cumulative_result == EncodeResult::Full {
                encode_buffer.clear();
                let encode_result = encoder.encode(packet, &mut encode_buffer);
                if let Err(_) = encode_result {
                    break;
                }

                cumulative_result = encode_result.unwrap();
                full_encoded_stream.extend_from_slice(encode_buffer.as_slice());
            }

            assert_eq!(cumulative_result, EncodeResult::Complete);
        }

        let (packet_sender, packet_receiver) = std::sync::mpsc::channel();

        let options = DecoderOptions {
            packet_stream : packet_sender
        };

        let mut decoder = Decoder::new(options);
        decoder.reset_for_new_connection();

        let mut decode_stream_slice = full_encoded_stream.as_slice();
        while decode_stream_slice.len() > 0 {
            let fragment_size : usize = usize::min(decode_size, decode_stream_slice.len());
            let decode_slice = &decode_stream_slice[..fragment_size];
            decode_stream_slice = &decode_stream_slice[fragment_size..];

            let decode_result = decoder.decode_bytes(decode_slice);
            assert!(!decode_result.is_err());
        }

        let mut matching_packets : u32 = 0;

        loop {
            let receive_result = packet_receiver.try_recv();
            if let Err(error) = receive_result {
                assert_eq!(TryRecvError::Empty, error);
                break;
            }

            let received_packet = receive_result.unwrap();
            matching_packets += 1;

            assert_eq!(*packet, received_packet);
        }

        assert_eq!(encode_repetitions, matching_packets);

        return true;
    }

    fn do_round_trip_encode_decode_test(packet : &MqttPacket) -> bool {
        let encode_buffer_sizes : Vec<usize> = vec!(4, 5, 7, 11, 17, 31, 47, 71, 131);
        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7, 11, 17, 31, 47, 71, 131, 1023);

        for encode_size in encode_buffer_sizes.iter() {
            for decode_size in decode_fragment_sizes.iter() {
                assert!(do_single_encode_decode_test(&packet, *encode_size, *decode_size, 5));
            }
        }

        return true;
    }

    #[test]
    fn publish_round_trip_encode_decode_default() {
        let packet = PublishPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Publish(packet)));
    }

    #[test]
    fn publish_round_trip_encode_decode_basic() {

        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            payload: Some("a payload".as_bytes().to_vec()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Publish(packet)));
    }

    fn create_publish_with_all_fields() -> PublishPacket {
        return PublishPacket {
            packet_id: 47,
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            duplicate: true,
            retain: true,
            payload: Some("a payload".as_bytes().to_vec()),
            payload_format: Some(PayloadFormatIndicator::Utf8),
            message_expiry_interval_seconds : Some(3600),
            topic_alias: Some(10),
            response_topic: Some("Respond/to/me".to_string()),
            correlation_data: Some(vec!(1, 2, 3, 4, 5)),
            subscription_identifiers: Some(vec!(10, 20, 256, 32768)),
            content_type: Some("rest/json".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "name1".to_string(), value: "value1".to_string()},
                UserProperty{name: "name2".to_string(), value: "value2".to_string()},
                UserProperty{name: "name3".to_string(), value: "value3".to_string()},
            ))
        };
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields() {

        let packet = create_publish_with_all_fields();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Publish(packet)));
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields_2byte_payload() {
        let mut publish = create_publish_with_all_fields();
        publish.payload = Some(vec![0; 257]);

        let packet = &MqttPacket::Publish(publish);

        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7);

        for decode_size in decode_fragment_sizes.iter() {
            assert!(do_single_encode_decode_test(&packet, 1024, *decode_size, 5));
        }
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields_3byte_payload() {
        let mut publish = create_publish_with_all_fields();
        publish.payload = Some(vec![0; 32768]);

        let packet = &MqttPacket::Publish(publish);

        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7);

        for decode_size in decode_fragment_sizes.iter() {
            assert!(do_single_encode_decode_test(&packet, 1024, *decode_size, 5));
        }
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields_4byte_payload() {
        let mut publish = create_publish_with_all_fields();
        publish.payload = Some(vec![0; 128 * 128 * 128]);

        let packet = &MqttPacket::Publish(publish);

        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7);

        for decode_size in decode_fragment_sizes.iter() {
            assert!(do_single_encode_decode_test(&packet, 1024, *decode_size, 5));
        }
    }

    #[test]
    fn pingreq_round_trip_encode_decode() {
        let packet = PingreqPacket {};
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingreq(packet)));
    }

    #[test]
    fn pingresp_round_trip_encode_decode() {
        let packet = PingrespPacket {};
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingresp(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_default() {
        let packet = PubackPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_success_no_props() {

        let packet = PubackPacket {
            packet_id: 123,
            reason_code: PubackReasonCode::Success,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_failure_no_props() {

        let packet = PubackPacket {
            packet_id: 16384,
            reason_code: PubackReasonCode::NotAuthorized,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_success_with_props() {

        let packet = PubackPacket {
            packet_id: 1025,
            reason_code: PubackReasonCode::Success,
            reason_string: Some("This was the best publish I've ever seen.  Take a bow.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "puback1".to_string(), value: "value1".to_string()},
                UserProperty{name: "puback2".to_string(), value: "value2".to_string()},
                UserProperty{name: "puback2".to_string(), value: "value3".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_failure_with_props() {

        let packet = PubackPacket {
            packet_id: 1025,
            reason_code: PubackReasonCode::ImplementationSpecificError,
            reason_string: Some("Wow!  What a terrible publish.  You should be ashamed.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "puback1".to_string(), value: "value1".to_string()},
                UserProperty{name: "puback2".to_string(), value: "value2".to_string()},
            ))
        };

        assert!(do_single_encode_decode_test(&MqttPacket::Puback(packet), 1024, 1024, 1));
        //assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

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

    #[test]
    fn pubrel_round_trip_encode_decode_default() {
        let packet = PubrelPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_success_no_props() {

        let packet = PubrelPacket {
            packet_id: 12,
            reason_code: PubrelReasonCode::Success,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_failure_no_props() {

        let packet = PubrelPacket {
            packet_id: 8193,
            reason_code: PubrelReasonCode::PacketIdentifierNotFound,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_success_with_props() {

        let packet = PubrelPacket {
            packet_id: 10253,
            reason_code: PubrelReasonCode::Success,
            reason_string: Some("Qos2, I can do this.  Believe in me.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubrel1".to_string(), value: "value1".to_string()},
                UserProperty{name: "pubrel2".to_string(), value: "value2".to_string()},
                UserProperty{name: "pubrel2".to_string(), value: "value3".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_failure_with_props() {

        let packet = PubrelPacket {
            packet_id: 12500,
            reason_code: PubrelReasonCode::PacketIdentifierNotFound,
            reason_string: Some("Aw shucks, I forgot what I was doing.  Sorry!".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "hello1".to_string(), value: "squidward".to_string()},
                UserProperty{name: "patrick".to_string(), value: "star".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_default() {
        let packet = PubcompPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet)));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_success_no_props() {

        let packet = PubcompPacket {
            packet_id: 132,
            reason_code: PubcompReasonCode::Success,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet)));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_failure_no_props() {

        let packet = PubcompPacket {
            packet_id: 4095,
            reason_code: PubcompReasonCode::PacketIdentifierNotFound,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet)));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_success_with_props() {

        let packet = PubcompPacket {
            packet_id: 1253,
            reason_code: PubcompReasonCode::Success,
            reason_string: Some("We did it!  High five.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubcomp1".to_string(), value: "value1".to_string()},
                UserProperty{name: "pubcomp2".to_string(), value: "value2".to_string()},
                UserProperty{name: "pubcomp2".to_string(), value: "value3".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet)));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_failure_with_props() {

        let packet = PubcompPacket {
            packet_id: 1500,
            reason_code: PubcompReasonCode::PacketIdentifierNotFound,
            reason_string: Some("I tried so hard, and got so far, but in the end, we totally face-planted".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "uf".to_string(), value: "dah".to_string()},
                UserProperty{name: "velkomen".to_string(), value: "stanwood".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_default() {
        let packet = ConnectPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_basic() {
        let packet = ConnectPacket {
            keep_alive_interval_seconds : 1200,
            clean_start : true,
            client_id : Some("MyClient".to_string()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_no_flags_all_optional_properties() {
        let packet = ConnectPacket {
            keep_alive_interval_seconds : 3600,
            clean_start : true,
            client_id : Some("MyClient2".to_string()),
            session_expiry_interval_seconds: Some(0xFFFFFFFFu32),
            request_response_information: Some(true),
            request_problem_information: Some(false),
            receive_maximum: Some(100),
            topic_alias_maximum: Some(20),
            maximum_packet_size_bytes: Some(128 * 1024),
            authentication_method: Some("Kerberos".to_string()),
            authentication_data: Some(vec![5, 4, 3, 2, 1]),
            user_properties: Some(vec!(
                UserProperty{name: "connecting".to_string(), value: "future".to_string()},
                UserProperty{name: "Iamabanana".to_string(), value: "Hamizilla".to_string()},
            )),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_username_only() {
        let packet = ConnectPacket {
            username : Some("SpaceUnicorn".to_string()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_password_only() {
        let packet = ConnectPacket {
            password : Some("Marshmallow Lasers".as_bytes().to_vec()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_all_non_will_properties() {
        let packet = ConnectPacket {
            keep_alive_interval_seconds : 3600,
            clean_start : true,
            client_id : Some("NotAHaxxor".to_string()),
            session_expiry_interval_seconds: Some(0x1234ABCDu32),
            request_response_information: Some(false),
            request_problem_information: Some(true),
            receive_maximum: Some(1000),
            topic_alias_maximum: Some(2),
            maximum_packet_size_bytes: Some(512 * 1024 - 1),
            authentication_method: Some("GSSAPI".to_string()),
            authentication_data: Some(vec![15, 14, 13, 12, 11]),
            user_properties: Some(vec!(
                UserProperty{name: "Another".to_string(), value: "brick".to_string()},
                UserProperty{name: "WhenIwas".to_string(), value: "ayoungboy".to_string()},
            )),
            username: Some("Gluten-free armada".to_string()),
            password: Some("PancakeRobot".as_bytes().to_vec()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_default_will() {
        let packet = ConnectPacket {
            will : Some(PublishPacket {
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_simple_will() {
        let packet = ConnectPacket {
            will : Some(PublishPacket {
                topic : "in/rememberance".to_string(),
                qos: QualityOfService::ExactlyOnce,
                payload: Some("I'llbealright".as_bytes().to_vec()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_all_will_fields() {
        let packet = ConnectPacket {
            will_delay_interval_seconds : Some(60),
            will : Some(PublishPacket {
                topic : "in/rememberance/of/mrkrabs".to_string(),
                qos: QualityOfService::ExactlyOnce,
                payload: Some("Arrrrrrrrrrrrrrr".as_bytes().to_vec()),
                retain: true,
                payload_format : Some(PayloadFormatIndicator::Utf8),
                message_expiry_interval_seconds : Some(1800),
                content_type : Some("QueryXML".to_string()),
                response_topic : Some("forever/today".to_string()),
                correlation_data : Some("Request1".as_bytes().to_vec()),
                user_properties: Some(vec!(
                    UserProperty{name: "WillProp1".to_string(), value: "WillValue1".to_string()},
                )),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_everything() {
        let packet = ConnectPacket {
            keep_alive_interval_seconds : 3600,
            clean_start : true,
            client_id : Some("NotAHaxxor".to_string()),
            session_expiry_interval_seconds: Some(0x1234ABCDu32),
            request_response_information: Some(false),
            request_problem_information: Some(true),
            receive_maximum: Some(1000),
            topic_alias_maximum: Some(2),
            maximum_packet_size_bytes: Some(512 * 1024 - 1),
            authentication_method: Some("GSSAPI".to_string()),
            authentication_data: Some(vec![15, 14, 13, 12, 11]),
            user_properties: Some(vec!(
                UserProperty{name: "Another".to_string(), value: "brick".to_string()},
                UserProperty{name: "WhenIwas".to_string(), value: "ayoungboy".to_string()},
            )),
            will_delay_interval_seconds : Some(60),
            will : Some(PublishPacket {
                topic : "in/rememberance/of/mrkrabs".to_string(),
                qos: QualityOfService::ExactlyOnce,
                payload: Some("Arrrrrrrrrrrrrrr".as_bytes().to_vec()),
                retain: true,
                payload_format : Some(PayloadFormatIndicator::Utf8),
                message_expiry_interval_seconds : Some(1800),
                content_type : Some("QueryXML".to_string()),
                response_topic : Some("forever/today".to_string()),
                correlation_data : Some("Request1".as_bytes().to_vec()),
                user_properties: Some(vec!(
                    UserProperty{name: "WillProp1".to_string(), value: "WillValue1".to_string()},
                )),
                ..Default::default()
            }),
            username: Some("Gluten-free armada".to_string()),
            password: Some("PancakeRobot".as_bytes().to_vec()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

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

    #[test]
    fn auth_round_trip_encode_decode_default() {
        let packet = AuthPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Auth(packet)));
    }

    #[test]
    fn auth_round_trip_encode_decode_required() {
        let packet = AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Auth(packet)));
    }

    #[test]
    fn auth_round_trip_encode_decode_all_properties() {
        let packet = AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            authentication_method : Some("UnbreakableAuthExchange".to_string()),
            authentication_data : Some("Noonewillguessthis".as_bytes().to_vec()),
            reason_string : Some("Myfavoritebroker".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "Roblox".to_string(), value: "Wheredidmymoneygo".to_string()},
                UserProperty{name: "Beeswarmsimulator".to_string(), value: "Lootbox".to_string()},
            )),
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Auth(packet)));
    }

    #[test]
    fn unsubscribe_round_trip_encode_decode_default() {
        let packet = UnsubscribePacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsubscribe(packet)));
    }

    #[test]
    fn unsubscribe_round_trip_encode_decode_basic() {
        let packet = UnsubscribePacket {
            packet_id : 123,
            topic_filters : vec![ "hello/world".to_string() ],
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsubscribe(packet)));
    }

    #[test]
    fn unsubscribe_round_trip_encode_decode_all_properties() {
        let packet = UnsubscribePacket {
            packet_id : 123,
            topic_filters : vec![
                "hello/world".to_string(),
                "calvin/is/a/goof".to_string(),
                "wild/+/card".to_string()
            ],
            user_properties: Some(vec!(
                UserProperty{name: "Clickergames".to_string(), value: "arelame".to_string()},
            )),
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsubscribe(packet)));
    }

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

    #[test]
    fn unsuback_round_trip_encode_decode_all() {
        let packet = UnsubackPacket {
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
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsuback(packet)));
    }

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

    #[test]
    fn suback_round_trip_encode_decode_all() {
        let packet = SubackPacket {
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
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Suback(packet)));
    }
}