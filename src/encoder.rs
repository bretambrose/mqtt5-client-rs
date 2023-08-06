/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::collections::VecDeque;

use crate::encoding_utils::*;
use crate::spec::*;
use crate::spec_impl::*;
use crate::{Mqtt5Error, Mqtt5Result};

fn get_connect_packet_client_id(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connect, client_id)
}

fn get_connect_packet_authentication_method(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connect, authentication_method)
}

fn get_connect_packet_authentication_data(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Connect, authentication_data)
}

fn get_connect_packet_username(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connect, username)
}

fn get_connect_packet_password(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Connect, password)
}

fn get_connect_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(properties) = &connect.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

fn get_connect_packet_will_content_type(packet: &MqttPacket) -> &str {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(content_type) = &will.content_type {
                return content_type;
            }
        }
    }

    panic!("Encoder: will content type accessor invoked in an invalid state");
}

fn get_connect_packet_will_response_topic(packet: &MqttPacket) -> &str {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(response_topic) = &will.response_topic {
                return response_topic;
            }
        }
    }

    panic!("Will response topic accessor invoked in an invalid state");
}

fn get_connect_packet_will_correlation_data(packet: &MqttPacket) -> &[u8] {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(correlation_data) = &will.correlation_data {
                return correlation_data;
            }
        }
    }

    panic!("Will correlation data accessor invoked in an invalid state");
}

fn get_connect_packet_will_topic(packet: &MqttPacket) -> &str {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            return &will.topic;
        }
    }

    panic!("Will topic accessor invoked in an invalid state");
}

fn get_connect_packet_will_payload(packet: &MqttPacket) -> &[u8] {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(payload) = &will.payload {
                return payload;
            }
        }
    }

    panic!("Will payload accessor invoked in an invalid state");
}

fn get_connect_packet_will_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(publish) = &connect.will {
            if let Some(properties) = &publish.user_properties {
                return &properties[index];
            }
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

static MQTT5_CONNECT_PROTOCOL_BYTES: [u8; 7] = [0, 4, 77, 81, 84, 84, 5];
fn get_connect_protocol_bytes(_: &MqttPacket) -> &'static [u8] {
    return &MQTT5_CONNECT_PROTOCOL_BYTES;
}

fn compute_connect_flags(packet: &ConnectPacket) -> u8 {
    let mut flags: u8 = 0;
    if packet.clean_start {
        flags |= 1u8 << 1;
    }

    if let Some(will) = &packet.will {
        flags |= 1u8 << 2;
        flags |= (will.qos as u8) << 3;
        if will.retain {
            flags |= 1u8 << 5;
        }
    }

    if packet.password.is_some() {
        flags |= 1u8 << 6;
    }

    if packet.username.is_some() {
        flags |= 1u8 << 7;
    }

    flags
}

#[rustfmt::skip]
fn compute_connect_packet_length_properties(packet: &ConnectPacket) -> Mqtt5Result<(u32, u32, u32), ()> {
    let mut connect_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_u32_property_length!(connect_property_section_length, packet.session_expiry_interval_seconds);
    add_optional_u16_property_length!(connect_property_section_length, packet.receive_maximum);
    add_optional_u32_property_length!(connect_property_section_length, packet.maximum_packet_size_bytes);
    add_optional_u16_property_length!(connect_property_section_length, packet.topic_alias_maximum);
    add_optional_u8_property_length!(connect_property_section_length, packet.request_response_information);
    add_optional_u8_property_length!(connect_property_section_length, packet.request_problem_information);
    add_optional_string_property_length!(connect_property_section_length, packet.authentication_method);
    add_optional_bytes_property_length!(connect_property_section_length, packet.authentication_data);

    /* variable header length =
     *    10 bytes (6 for mqtt string, 1 for protocol version, 1 for flags, 2 for keep alive)
     *  + # bytes(variable_length_encoding(connect_property_section_length))
     *  + connect_property_section_length
     */
    let variable_header_length_result = compute_variable_length_integer_encode_size(connect_property_section_length);
    if let Err(error) = variable_header_length_result {
        return Err(error);
    }

    let mut variable_header_length: usize = variable_header_length_result.unwrap();
    variable_header_length += 10 + connect_property_section_length;

    let mut payload_length : usize = 0;
    add_optional_string_length!(payload_length, packet.client_id);

    let mut will_property_length : usize = 0;
    if let Some(will) = &packet.will {
        will_property_length = compute_user_properties_length(&will.user_properties);

        add_optional_u32_property_length!(will_property_length, packet.will_delay_interval_seconds);
        add_optional_u8_property_length!(will_property_length, will.payload_format);
        add_optional_u32_property_length!(will_property_length, will.message_expiry_interval_seconds);
        add_optional_string_property_length!(will_property_length, will.content_type);
        add_optional_string_property_length!(will_property_length, will.response_topic);
        add_optional_bytes_property_length!(will_property_length, will.correlation_data);

        let will_properties_length_encode_size_result = compute_variable_length_integer_encode_size(will_property_length);
        if let Err(error) = will_properties_length_encode_size_result {
            return Err(error);
        }

        let will_properties_length_encode_size : usize = will_properties_length_encode_size_result.unwrap();

        payload_length += will_property_length;
        payload_length += will_properties_length_encode_size;
        payload_length += 2 + will.topic.len();
        add_optional_bytes_length!(payload_length, will.payload);
    }

    if let Some(username) = &packet.username {
        payload_length += 2 + username.len();
    }

    if let Some(password) = &packet.password {
        payload_length += 2 + password.len();
    }

    let total_remaining_length : usize = payload_length + variable_header_length;

    if total_remaining_length > MAXIMUM_VARIABLE_LENGTH_INTEGER {
        return Err(Mqtt5Error::VariableLengthIntegerMaximumExceeded);
    }

    Ok((total_remaining_length as u32, connect_property_section_length as u32, will_property_length as u32))
}

#[rustfmt::skip]
impl Encodable for ConnectPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        let length_result = compute_connect_packet_length_properties(self);
        if let Err(error) = length_result {
            return Err(error);
        }

        let (total_remaining_length, connect_property_length, will_property_length) = length_result.unwrap();

        encode_integral_expression!(steps, Uint8, 1u8 << 4);
        encode_integral_expression!(steps, Vli, total_remaining_length);
        encode_raw_bytes!(steps, get_connect_protocol_bytes);
        encode_integral_expression!(steps, Uint8, compute_connect_flags(self));
        encode_integral_expression!(steps, Uint16, self.keep_alive_interval_seconds);

        encode_integral_expression!(steps, Vli, connect_property_length);
        encode_optional_property!(steps, Uint32, PROPERTY_KEY_SESSION_EXPIRY_INTERVAL, self.session_expiry_interval_seconds);
        encode_optional_property!(steps, Uint16, PROPERTY_KEY_RECEIVE_MAXIMUM, self.receive_maximum);
        encode_optional_property!(steps, Uint32, PROPERTY_KEY_MAXIMUM_PACKET_SIZE, self.maximum_packet_size_bytes);
        encode_optional_property!(steps, Uint16, PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM, self.topic_alias_maximum);
        encode_optional_boolean_property!(steps, PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION, self.request_response_information);
        encode_optional_boolean_property!(steps, PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION, self.request_problem_information);
        encode_optional_string_property!(steps, get_connect_packet_authentication_method, PROPERTY_KEY_AUTHENTICATION_METHOD, self.authentication_method);
        encode_optional_bytes_property!(steps, get_connect_packet_authentication_data, PROPERTY_KEY_AUTHENTICATION_DATA, self.authentication_data);
        encode_user_properties!(steps, get_connect_packet_user_property, self.user_properties);

        encode_length_prefixed_optional_string!(steps, get_connect_packet_client_id, self.client_id);

        if let Some(will) = &self.will {
            encode_integral_expression!(steps, Vli, will_property_length);
            encode_optional_property!(steps, Uint32, PROPERTY_KEY_WILL_DELAY_INTERVAL, self.will_delay_interval_seconds);
            encode_optional_enum_property!(steps, Uint8, PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR, u8, will.payload_format);
            encode_optional_property!(steps, Uint32, PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL, will.message_expiry_interval_seconds);
            encode_optional_string_property!(steps, get_connect_packet_will_content_type, PROPERTY_KEY_CONTENT_TYPE, &will.content_type);
            encode_optional_string_property!(steps, get_connect_packet_will_response_topic, PROPERTY_KEY_RESPONSE_TOPIC, &will.response_topic);
            encode_optional_bytes_property!(steps, get_connect_packet_will_correlation_data, PROPERTY_KEY_CORRELATION_DATA, will.correlation_data);
            encode_user_properties!(steps, get_connect_packet_will_user_property, will.user_properties);

            encode_length_prefixed_string!(steps, get_connect_packet_will_topic, will.topic);
            encode_length_prefixed_optional_bytes!(steps, get_connect_packet_will_payload, will.payload);
        }

        if self.username.is_some() {
            encode_length_prefixed_optional_string!(steps, get_connect_packet_username, self.username);
        }

        if self.password.is_some() {
            encode_length_prefixed_optional_bytes!(steps, get_connect_packet_password, self.password);
        }

        Ok(())
    }
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
impl Encodable for ConnackPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        let (total_remaining_length, connack_property_length) = compute_connack_packet_length_properties(self)?;

        encode_integral_expression!(steps, Uint8, PACKET_TYPE_CONNACK << 4);
        encode_integral_expression!(steps, Vli, total_remaining_length);

        /*
         * Variable Header
         * 1 byte flags
         * 1 byte reason code
         * 1-4 byte Property Length as Variable Byte Integer
         * n bytes Properties
         */
        encode_integral_expression!(steps, Uint8, if self.session_present { 1 } else { 0 });
        encode_enum!(steps, Uint8, u8, self.reason_code);
        encode_integral_expression!(steps, Vli, connack_property_length);

        encode_optional_property!(steps, Uint32, PROPERTY_KEY_SESSION_EXPIRY_INTERVAL, self.session_expiry_interval);
        encode_optional_property!(steps, Uint16, PROPERTY_KEY_RECEIVE_MAXIMUM, self.receive_maximum);
        encode_optional_enum_property!(steps, Uint8, PROPERTY_KEY_MAXIMUM_QOS, u8, self.maximum_qos);
        encode_optional_boolean_property!(steps, PROPERTY_KEY_RETAIN_AVAILABLE, self.retain_available);
        encode_optional_property!(steps, Uint32, PROPERTY_KEY_MAXIMUM_PACKET_SIZE, self.maximum_packet_size);
        encode_optional_string_property!(steps, get_connack_packet_assigned_client_identifier, PROPERTY_KEY_ASSIGNED_CLIENT_IDENTIFIER, self.assigned_client_identifier);
        encode_optional_property!(steps, Uint16, PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM, self.topic_alias_maximum);
        encode_optional_string_property!(steps, get_connack_packet_reason_string, PROPERTY_KEY_REASON_STRING, self.reason_string);
        encode_user_properties!(steps, get_connack_packet_user_property, self.user_properties);
        encode_optional_boolean_property!(steps, PROPERTY_KEY_WILDCARD_SUBSCRIPTIONS_AVAILABLE, self.wildcard_subscriptions_available);
        encode_optional_boolean_property!(steps, PROPERTY_KEY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE, self.subscription_identifiers_available);
        encode_optional_boolean_property!(steps, PROPERTY_KEY_SHARED_SUBSCRIPTIONS_AVAILABLE, self.shared_subscriptions_available);
        encode_optional_property!(steps, Uint16, PROPERTY_KEY_SERVER_KEEP_ALIVE, self.server_keep_alive);
        encode_optional_string_property!(steps, get_connack_packet_response_information, PROPERTY_KEY_RESPONSE_INFORMATION, self.response_information);
        encode_optional_string_property!(steps, get_connack_packet_server_reference, PROPERTY_KEY_SERVER_REFERENCE, self.server_reference);
        encode_optional_string_property!(steps, get_connack_packet_authentication_method, PROPERTY_KEY_AUTHENTICATION_METHOD, self.authentication_method);
        encode_optional_bytes_property!(steps, get_connack_packet_authentication_data, PROPERTY_KEY_AUTHENTICATION_DATA, self.authentication_data);

        Ok(())
    }
}

#[rustfmt::skip]
fn compute_publish_packet_length_properties(packet: &PublishPacket) -> Mqtt5Result<(u32, u32), ()> {
    let mut publish_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_u8_property_length!(publish_property_section_length, packet.payload_format);
    add_optional_u32_property_length!(publish_property_section_length, packet.message_expiry_interval_seconds);
    add_optional_u16_property_length!(publish_property_section_length, packet.topic_alias);
    add_optional_string_property_length!(publish_property_section_length, packet.content_type);
    add_optional_string_property_length!(publish_property_section_length, packet.response_topic);
    add_optional_bytes_property_length!(publish_property_section_length, packet.correlation_data);

    /* should never happen on the client, but just to be complete */
    if let Some(subscription_identifiers) = &packet.subscription_identifiers {
        for val in subscription_identifiers.iter() {
            let encoding_size = compute_variable_length_integer_encode_size(*val as usize)?;
            publish_property_section_length += 1 + encoding_size;
        }
    }

    /*
     * Remaining Length:
     * Variable Header
     *  - Topic Name
     *  - Packet Identifier
     *  - Property Length as VLI x
     *  - All Properties x
     * Payload
     */

    let publish_property_section_length_encode_size_result = compute_variable_length_integer_encode_size(publish_property_section_length);
    if let Err(error) = publish_property_section_length_encode_size_result {
        return Err(error);
    }

    let mut total_remaining_length : usize = publish_property_section_length_encode_size_result.unwrap();

    /* Topic name */
    total_remaining_length += 2 + packet.topic.len();

    /* Optional (qos1+) packet id */
    if packet.qos != QualityOfService::AtMostOnce {
        total_remaining_length += 2;
    }

    total_remaining_length += publish_property_section_length;

    if let Some(payload) = &packet.payload {
        total_remaining_length += payload.len();
    }

    Ok((total_remaining_length as u32, publish_property_section_length as u32))
}

/*
 * Fixed Header
 * byte 1:
 *  bits 4-7: MQTT Control Packet Type
 *  bit 3: DUP flag
 *  bit 1-2: QoS level
 *  bit 0: RETAIN
 * byte 2-x: Remaining Length as Variable Byte Integer (1-4 bytes)
 */
fn compute_publish_fixed_header_first_byte(packet: &PublishPacket) -> u8 {
    let mut first_byte: u8 = PACKET_TYPE_PUBLISH << 4;

    if packet.duplicate {
        first_byte |= 1u8 << 3;
    }

    first_byte |= (packet.qos as u8) << 1;

    if packet.retain {
        first_byte |= 1u8;
    }

    first_byte
}

fn get_publish_packet_response_topic(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Publish, response_topic)
}

fn get_publish_packet_correlation_data(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Publish, correlation_data)
}

fn get_publish_packet_content_type(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Publish, content_type)
}

fn get_publish_packet_topic(packet: &MqttPacket) -> &str {
    get_packet_field!(packet, MqttPacket::Publish, topic)
}

fn get_publish_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Publish(publish) = packet {
        if let Some(properties) = &publish.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

fn get_publish_packet_payload(packet: &MqttPacket) -> &[u8] {
    if let MqttPacket::Publish(publish) = packet {
        if let Some(bytes) = &publish.payload {
            return bytes;
        }
    }

    panic!("Internal encoding error: invalid publish payload state");
}

#[rustfmt::skip]
impl Encodable for PublishPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        let length_result = compute_publish_packet_length_properties(self);
        if let Err(error) = length_result {
            return Err(error);
        }

        let (total_remaining_length, publish_property_length) = length_result.unwrap();

        encode_integral_expression!(steps, Uint8, compute_publish_fixed_header_first_byte(self));
        encode_integral_expression!(steps, Vli, total_remaining_length);

        /*
         * Variable Header
         * UTF-8 Encoded Topic Name
         * 2 byte Packet Identifier
         * 1-4 byte Property Length as Variable Byte Integer
         * n bytes Properties
         */
        encode_length_prefixed_string!(steps, get_publish_packet_topic, self.topic);
        if self.qos != QualityOfService::AtMostOnce {
            encode_integral_expression!(steps, Uint16, self.packet_id);
        }
        encode_integral_expression!(steps, Vli, publish_property_length);

        encode_optional_enum_property!(steps, Uint8, PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR, u8, self.payload_format);
        encode_optional_property!(steps, Uint32, PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL, self.message_expiry_interval_seconds);
        encode_optional_property!(steps, Uint16, PROPERTY_KEY_TOPIC_ALIAS, self.topic_alias);
        encode_optional_string_property!(steps, get_publish_packet_response_topic, PROPERTY_KEY_RESPONSE_TOPIC, self.response_topic);
        encode_optional_bytes_property!(steps, get_publish_packet_correlation_data, PROPERTY_KEY_CORRELATION_DATA, self.correlation_data);

        if let Some(subscription_identifiers) = &self.subscription_identifiers {
            for val in subscription_identifiers {
                encode_integral_expression!(steps, Uint8, PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER);
                encode_integral_expression!(steps, Vli, *val);
            }
        }

        encode_optional_string_property!(steps, get_publish_packet_content_type, PROPERTY_KEY_CONTENT_TYPE, &self.content_type);
        encode_user_properties!(steps, get_publish_packet_user_property, self.user_properties);

        if self.payload.is_some() {
            encode_raw_bytes!(steps, get_publish_packet_payload);
        }

        Ok(())
    }
}

#[rustfmt::skip]
define_ack_packet_lengths_function!(compute_puback_packet_length_properties, PubackPacket, PubackReasonCode);
define_ack_packet_reason_string_accessor!(get_puback_packet_reason_string, Puback);
define_ack_packet_user_property_accessor!(get_puback_packet_user_property, Puback);

#[rustfmt::skip]
define_ack_packet_encoding_impl!(PubackPacket, PubackReasonCode, PACKET_TYPE_PUBACK, compute_puback_packet_length_properties, get_puback_packet_reason_string, get_puback_packet_user_property);


#[rustfmt::skip]
define_ack_packet_lengths_function!(compute_pubrec_packet_length_properties, PubrecPacket, PubrecReasonCode);
define_ack_packet_reason_string_accessor!(get_pubrec_packet_reason_string, Pubrec);
define_ack_packet_user_property_accessor!(get_pubrec_packet_user_property, Pubrec);

#[rustfmt::skip]
define_ack_packet_encoding_impl!(PubrecPacket, PubrecReasonCode, PACKET_TYPE_PUBREC, compute_pubrec_packet_length_properties, get_pubrec_packet_reason_string, get_pubrec_packet_user_property);

#[rustfmt::skip]
define_ack_packet_lengths_function!(compute_pubrel_packet_length_properties, PubrelPacket, PubrelReasonCode);
define_ack_packet_reason_string_accessor!(get_pubrel_packet_reason_string, Pubrel);
define_ack_packet_user_property_accessor!(get_pubrel_packet_user_property, Pubrel);

#[rustfmt::skip]
define_ack_packet_encoding_impl!(PubrelPacket, PubrelReasonCode, PACKET_TYPE_PUBREL, compute_pubrel_packet_length_properties, get_pubrel_packet_reason_string, get_pubrel_packet_user_property);

#[rustfmt::skip]
define_ack_packet_lengths_function!(compute_pubcomp_packet_length_properties, PubcompPacket, PubcompReasonCode);
define_ack_packet_reason_string_accessor!(get_pubcomp_packet_reason_string, Pubcomp);
define_ack_packet_user_property_accessor!(get_pubcomp_packet_user_property, Pubcomp);

#[rustfmt::skip]
define_ack_packet_encoding_impl!(PubcompPacket, PubcompReasonCode, PACKET_TYPE_PUBCOMP, compute_pubcomp_packet_length_properties, get_pubcomp_packet_reason_string, get_pubcomp_packet_user_property);

#[rustfmt::skip]
fn compute_subscribe_packet_length_properties(packet: &SubscribePacket) -> Mqtt5Result<(u32, u32), ()> {
    let mut subscribe_property_section_length = compute_user_properties_length(&packet.user_properties);
    add_optional_u32_property_length!(subscribe_property_section_length, packet.subscription_identifier);

    let mut total_remaining_length : usize = 2 + compute_variable_length_integer_encode_size(subscribe_property_section_length)?;
    total_remaining_length += subscribe_property_section_length;

    total_remaining_length += packet.subscriptions.len() * 3;
    for subscription in &packet.subscriptions {
        total_remaining_length += subscription.topic_filter.len();
    }

    Ok((total_remaining_length as u32, subscribe_property_section_length as u32))
}

fn get_subscribe_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Subscribe(subscribe) = packet {
        if let Some(properties) = &subscribe.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

fn get_subscribe_packet_topic_filter(packet: &MqttPacket, index: usize) -> &str {
    if let MqttPacket::Subscribe(subscribe) = packet {
        return &subscribe.subscriptions[index].topic_filter;
    }

    panic!("Internal encoding error: invalid subscribe topic filter state");
}

fn compute_subscription_options_byte(subscription: &Subscription) -> u8 {
    let mut options_byte = subscription.qos as u8;

    if subscription.no_local {
        options_byte |= SUBSCRIPTION_OPTIONS_NO_LOCAL_MASK;
    }

    if subscription.retain_as_published {
        options_byte |= SUBSCRIPTION_OPTIONS_RETAIN_AS_PUBLISHED_MASK;
    }

    options_byte |= (subscription.retain_handling_type as u8) << SUBSCRIPTION_OPTIONS_RETAIN_HANDLING_SHIFT;

    options_byte
}

#[rustfmt::skip]
impl Encodable for SubscribePacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        let (total_remaining_length, subscribe_property_length) = compute_subscribe_packet_length_properties(self)?;

        encode_integral_expression!(steps, Uint8, SUBSCRIBE_FIRST_BYTE);
        encode_integral_expression!(steps, Vli, total_remaining_length);

        encode_integral_expression!(steps, Uint16, self.packet_id);
        encode_integral_expression!(steps, Vli, subscribe_property_length);
        encode_optional_property!(steps, Uint32, PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER, self.subscription_identifier);
        encode_user_properties!(steps, get_subscribe_packet_user_property, self.user_properties);

        let subscriptions = &self.subscriptions;
        for (i, subscription) in subscriptions.iter().enumerate() {
            encode_indexed_string!(steps, get_subscribe_packet_topic_filter, subscription.topic_filter, i);
            encode_integral_expression!(steps, Uint8, compute_subscription_options_byte(subscription));
        }

        Ok(())
    }
}

#[rustfmt::skip]
fn compute_suback_packet_length_properties(packet: &SubackPacket) -> Mqtt5Result<(u32, u32), ()> {
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
impl Encodable for SubackPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        let (total_remaining_length, suback_property_length) = compute_suback_packet_length_properties(self)?;

        encode_integral_expression!(steps, Uint8, PACKET_TYPE_SUBACK << 4);
        encode_integral_expression!(steps, Vli, total_remaining_length);

        encode_integral_expression!(steps, Uint16, self.packet_id);
        encode_integral_expression!(steps, Vli, suback_property_length);

        encode_optional_string_property!(steps, get_suback_packet_reason_string, PROPERTY_KEY_REASON_STRING, self.reason_string);
        encode_user_properties!(steps, get_suback_packet_user_property, self.user_properties);

        let reason_codes = &self.reason_codes;
        for reason_code in reason_codes {
            encode_enum!(steps, Uint8, u8, *reason_code);
        }

        Ok(())
    }
}

#[rustfmt::skip]
fn compute_unsubscribe_packet_length_properties(packet: &UnsubscribePacket) -> Mqtt5Result<(u32, u32), ()> {
    let unsubscribe_property_section_length = compute_user_properties_length(&packet.user_properties);

    let mut total_remaining_length : usize = 2 + compute_variable_length_integer_encode_size(unsubscribe_property_section_length)?;
    total_remaining_length += unsubscribe_property_section_length;

    total_remaining_length += packet.topic_filters.len() * 2;
    for filter in &packet.topic_filters {
        total_remaining_length += filter.len();
    }

    Ok((total_remaining_length as u32, unsubscribe_property_section_length as u32))
}

fn get_unsubscribe_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Unsubscribe(unsubscribe) = packet {
        if let Some(properties) = &unsubscribe.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

fn get_unsubscribe_packet_topic_filter(packet: &MqttPacket, index: usize) -> &str {
    if let MqttPacket::Unsubscribe(unsubscribe) = packet {
        return &unsubscribe.topic_filters[index];
    }

    panic!("Internal encoding error: invalid unsubscribe topic filter state");
}

#[rustfmt::skip]
impl Encodable for UnsubscribePacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        let (total_remaining_length, unsubscribe_property_length) = compute_unsubscribe_packet_length_properties(self)?;

        encode_integral_expression!(steps, Uint8, UNSUBSCRIBE_FIRST_BYTE);
        encode_integral_expression!(steps, Vli, total_remaining_length);

        encode_integral_expression!(steps, Uint16, self.packet_id);
        encode_integral_expression!(steps, Vli, unsubscribe_property_length);
        encode_user_properties!(steps, get_unsubscribe_packet_user_property, self.user_properties);

        let topic_filters = &self.topic_filters;
        for (i, topic_filter) in topic_filters.iter().enumerate() {
            encode_indexed_string!(steps, get_unsubscribe_packet_topic_filter, topic_filter, i);
        }

        Ok(())
    }
}

#[rustfmt::skip]
fn compute_unsuback_packet_length_properties(packet: &UnsubackPacket) -> Mqtt5Result<(u32, u32), ()> {
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
impl Encodable for UnsubackPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        let (total_remaining_length, unsuback_property_length) = compute_unsuback_packet_length_properties(self)?;

        encode_integral_expression!(steps, Uint8, PACKET_TYPE_UNSUBACK << 4);
        encode_integral_expression!(steps, Vli, total_remaining_length);

        encode_integral_expression!(steps, Uint16, self.packet_id);
        encode_integral_expression!(steps, Vli, unsuback_property_length);

        encode_optional_string_property!(steps, get_unsuback_packet_reason_string, PROPERTY_KEY_REASON_STRING, self.reason_string);
        encode_user_properties!(steps, get_unsuback_packet_user_property, self.user_properties);

        let reason_codes = &self.reason_codes;
        for reason_code in reason_codes {
            encode_enum!(steps, Uint8, u8, *reason_code);
        }

        Ok(())
    }
}

#[rustfmt::skip]
impl Encodable for PingreqPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        encode_integral_expression!(steps, Uint8, PACKET_TYPE_PINGREQ << 4);
        encode_integral_expression!(steps, Uint8, 0);

        Ok(())
    }
}

#[rustfmt::skip]
impl Encodable for PingrespPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        encode_integral_expression!(steps, Uint8, PACKET_TYPE_PINGRESP << 4);
        encode_integral_expression!(steps, Uint8, 0);

        Ok(())
    }
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
impl Encodable for DisconnectPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        let (total_remaining_length, disconnect_property_length) = compute_disconnect_packet_length_properties(self)?;

        encode_integral_expression!(steps, Uint8, PACKET_TYPE_DISCONNECT << 4);
        encode_integral_expression!(steps, Vli, total_remaining_length);

        if disconnect_property_length == 0 && self.reason_code == DisconnectReasonCode::NormalDisconnection {
            assert_eq!(0, total_remaining_length);
            return Ok(());
        }

        encode_enum!(steps, Uint8, u8, self.reason_code);
        encode_integral_expression!(steps, Vli, disconnect_property_length);

        encode_optional_property!(steps, Uint32, PROPERTY_KEY_SESSION_EXPIRY_INTERVAL, self.session_expiry_interval_seconds);
        encode_optional_string_property!(steps, get_disconnect_packet_reason_string, PROPERTY_KEY_REASON_STRING, self.reason_string);
        encode_optional_string_property!(steps, get_disconnect_packet_server_reference, PROPERTY_KEY_SERVER_REFERENCE, self.server_reference);
        encode_user_properties!(steps, get_disconnect_packet_user_property, self.user_properties);

        Ok(())
    }
}

#[rustfmt::skip]
fn compute_auth_packet_length_properties(packet: &AuthPacket) -> Mqtt5Result<(u32, u32), ()> {
    let mut auth_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_string_property_length!(auth_property_section_length, packet.authentication_method);
    add_optional_bytes_property_length!(auth_property_section_length, packet.authentication_data);
    add_optional_string_property_length!(auth_property_section_length, packet.reason_string);

    let mut total_remaining_length : usize = 1 + compute_variable_length_integer_encode_size(auth_property_section_length)?;
    total_remaining_length += auth_property_section_length;

    Ok((total_remaining_length as u32, auth_property_section_length as u32))
}

fn get_auth_packet_authentication_method(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Auth, authentication_method)
}

fn get_auth_packet_authentication_data(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Auth, authentication_data)
}

fn get_auth_packet_reason_string(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Auth, reason_string)
}

fn get_auth_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Auth(auth) = packet {
        if let Some(properties) = &auth.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

#[rustfmt::skip]
impl Encodable for AuthPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        let (total_remaining_length, auth_property_length) = compute_auth_packet_length_properties(self)?;

        encode_integral_expression!(steps, Uint8, PACKET_TYPE_AUTH << 4);
        encode_integral_expression!(steps, Vli, total_remaining_length);

        encode_enum!(steps, Uint8, u8, self.reason_code);
        encode_integral_expression!(steps, Vli, auth_property_length);

        encode_optional_string_property!(steps, get_auth_packet_authentication_method, PROPERTY_KEY_AUTHENTICATION_METHOD, self.authentication_method);
        encode_optional_bytes_property!(steps, get_auth_packet_authentication_data, PROPERTY_KEY_AUTHENTICATION_DATA, self.authentication_data);
        encode_optional_string_property!(steps, get_auth_packet_reason_string, PROPERTY_KEY_REASON_STRING, self.reason_string);
        encode_user_properties!(steps, get_auth_packet_user_property, self.user_properties);

        Ok(())
    }
}

impl Encodable for MqttPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        match self {
            MqttPacket::Connect(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Connack(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Publish(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Puback(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Pubrec(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Pubrel(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Pubcomp(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Subscribe(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Suback(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Unsubscribe(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Unsuback(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Pingreq(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Pingresp(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Disconnect(packet) => {
                return packet.write_encoding_steps(steps);
            }
            MqttPacket::Auth(packet) => {
                return packet.write_encoding_steps(steps);
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum EncodeResult {
    Complete,
    Full,
}

pub(crate) struct Encoder {
    steps: VecDeque<EncodingStep>,
}

impl Encoder {
    pub fn new() -> Encoder {
        Encoder {
            steps: VecDeque::new(),
        }
    }

    pub fn reset(&mut self, packet: &MqttPacket) -> Mqtt5Result<(), ()> {
        self.steps.clear();

        packet.write_encoding_steps(&mut self.steps)
    }

    pub fn encode(
        &mut self,
        packet: &MqttPacket,
        dest: &mut Vec<u8>,
    ) -> Mqtt5Result<EncodeResult, ()> {
        let capacity = dest.capacity();
        if capacity < 4 {
            return Err(Mqtt5Error::EncodeBufferTooSmall);
        }

        while !self.steps.is_empty() {
            let step = self.steps.pop_front().unwrap();
            let result = process_encoding_step(&mut self.steps, step, packet, dest);
            if let Err(error) = result {
                return Err(error);
            }

            // Always want to have at least 4 bytes available to start a new step
            if dest.len() + 4 > dest.capacity() {
                break;
            }
        }

        if capacity != dest.capacity() {
            panic!("Internal error: encoding logic resized dest buffer");
        }

        if dest.len() + 4 > dest.capacity() {
            Ok(EncodeResult::Full)
        } else {
            Ok(EncodeResult::Complete)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoder_hand_check() {
        let mut encoder = Encoder::new();

        let packet = MqttPacket::Connect(ConnectPacket {
            keep_alive_interval_seconds: 1200,
            clean_start: true,
            client_id: Some("A-client-id".to_owned()),
            username: Some("A-username".to_owned()),
            password: Some([1u8, 2u8, 3u8, 4u8].to_vec()),
            session_expiry_interval_seconds: Some(3600),
            request_response_information: Some(true),
            request_problem_information: Some(true),
            receive_maximum: Some(100),
            topic_alias_maximum: Some(25),
            maximum_packet_size_bytes: Some(128 * 1024),
            authentication_method: Some("GSSAPI".to_owned()),
            authentication_data: Some([1u8, 2u8, 3u8, 4u8].to_vec()),
            will_delay_interval_seconds: Some(1u32 << 24),
            will: Some(PublishPacket {
                topic: "oh/no".to_owned(),
                qos: QualityOfService::AtLeastOnce,
                retain: true,
                payload: Some(vec![0u8; 1024]),
                payload_format: Some(PayloadFormatIndicator::Bytes),
                message_expiry_interval_seconds: Some(32768),
                response_topic: Some("here/lies/a/packet".to_owned()),
                correlation_data: Some([1u8, 2u8, 3u8, 4u8, 5u8].to_vec()),
                content_type: Some("application/json".to_owned()),
                user_properties: Some(
                    [UserProperty {
                        name: "WillTerb".to_owned(),
                        value: "WillBlah".to_owned(),
                    }]
                    .to_vec(),
                ),

                ..Default::default()
            }),
            user_properties: Some(
                [UserProperty {
                    name: "Terb".to_owned(),
                    value: "Blah".to_owned(),
                }]
                .to_vec(),
            ),
        });

        let reset_result = encoder.reset(&packet);
        assert!(reset_result.is_ok());

        let mut buffer = Vec::<u8>::with_capacity(16384);

        let encoding_result = encoder.encode(&packet, &mut buffer);
        assert!(encoding_result.is_ok());
    }
}
