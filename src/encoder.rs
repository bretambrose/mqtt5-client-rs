/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::collections::VecDeque;
use crate::packet::*;
use crate::packet::MqttPacket;

static PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR : u8 = 1;
static PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL : u8 = 2;
static PROPERTY_KEY_CONTENT_TYPE : u8 = 3;
static PROPERTY_KEY_RESPONSE_TOPIC : u8 = 8;
static PROPERTY_KEY_CORRELATION_DATA : u8 = 9;
static PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER : u8 = 11;
static PROPERTY_KEY_SESSION_EXPIRY_INTERVAL : u8 = 17;
static PROPERTY_KEY_ASSIGNED_CLIENT_IDENTIFIER : u8 = 18;
static PROPERTY_KEY_SERVER_KEEP_ALIVE : u8 = 19;
static PROPERTY_KEY_AUTHENTICATION_METHOD : u8 = 21;
static PROPERTY_KEY_AUTHENTICATION_DATA : u8 = 22;
static PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION : u8 = 23;
static PROPERTY_KEY_WILL_DELAY_INTERVAL : u8 = 24;
static PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION : u8 = 25;
static PROPERTY_KEY_RESPONSE_INFORMATION : u8 = 26;
static PROPERTY_KEY_SERVER_REFERENCE : u8 = 28;
static PROPERTY_KEY_REASON_STRING : u8 = 31;
static PROPERTY_KEY_RECEIVE_MAXIMUM : u8 = 33;
static PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM : u8 = 34;
static PROPERTY_KEY_TOPIC_ALIAS : u8 = 35;
static PROPERTY_KEY_MAXIMUM_QOS : u8 = 36;
static PROPERTY_KEY_RETAIN_AVAILABLE : u8 = 37;
static PROPERTY_KEY_USER_PROPERTY : u8 = 38;
static PROPERTY_KEY_MAXIMUM_PACKET_SIZE : u8 = 39;
static PROPERTY_KEY_WILDCARD_SUBSCRIPTIONS_AVAILABLE : u8 = 40;
static PROPERTY_KEY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE : u8 = 41;
static PROPERTY_KEY_SHARED_SUBSCRIPTIONS_AVAILABLE : u8 = 42;

pub enum EncodingStep<T> {
    Uint8(u8),
    Uint16(u16),
    Uint32(u32),
    Vli(u32),
    StringSlice(fn (&T) -> &str, u32),
    BytesSlice(fn (&T) -> &[u8], u32),
    UserPropertyName(fn (&T, usize) -> &UserProperty, usize, u32),
    UserPropertyValue(fn (&T, usize) -> &UserProperty, usize, u32),
}

pub trait Encodable {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep<MqttPacket>>);
}

macro_rules! get_packet_field {
    ($target: expr, $pat: path, $field_name: ident) => {
        if let $pat(a) = $target { // #1
            &a.$field_name
        } else {
            panic!("Packet variant mismatch");
        }
    };
}

macro_rules! get_optional_packet_field {
    ($target: expr, $pat: path, $field_name: ident) => {
        if let $pat(a) = $target { // #1
            &a.$field_name.as_ref().unwrap()
        } else {
            panic!("Packet variant mismatch");
        }
    };
}

macro_rules! encode_expression {
    ($target: ident, $enum_variant: ident, $value: expr) => {
        $target.push_back(EncodingStep::$enum_variant($value));
    };
}

macro_rules! encode_optional_expression {
    ($target: ident, $enum_variant: ident, $optional_value: expr) => {
        if let Some(val) = $optional_value {
            $target.push_back(EncodingStep::$enum_variant(val));
        }
    };
}

macro_rules! encode_property {
    ($target: ident, $enum_variant: ident, $property_key: expr, $value: expr) => {
        $target.push_back(EncodingStep::Uint8($property_key));
        $target.push_back(EncodingStep::$enum_variant($value));
    };
}

macro_rules! encode_optional_property {
    ($target: ident, $enum_variant: ident, $property_key: expr, $optional_value: expr) => {
        if let Some(val) = $optional_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::$enum_variant(val));
        }
    };
}

macro_rules! encode_optional_enum_property {
    ($target: ident, $enum_variant: ident, $property_key: expr, $int_type: ty, $optional_value: expr) => {
        if let Some(val) = $optional_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::$enum_variant(val as $int_type));
        }
    };
}


macro_rules! encode_optional_boolean_property {
    ($target: ident, $property_key: expr, $optional_value: expr) => {
        if let Some(val) = $optional_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::Uint8(if val { 1u8 } else { 0u8 }));
        }
    };
}

macro_rules! encode_length_prefixed_string {
    ($target: ident, $getter: ident, $value: expr) => {
        $target.push_back(EncodingStep::Uint16($value.len() as u16));
        $target.push_back(EncodingStep::StringSlice($getter as fn(&MqttPacket) -> &str, 0));
    };
}

macro_rules! encode_optional_length_prefixed_string {
    ($target: ident, $getter: ident, $optional_value: expr) => {
        if let Some(val) = &$optional_value {
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::StringSlice($getter as fn(&MqttPacket) -> &str, 0));
        } else {
            $target.push_back(EncodingStep::Uint16(0));
        }
    };
}

macro_rules! encode_optional_string_property {
    ($target: ident, $getter: ident, $property_key: expr, $field_value: expr) => {
        if let Some(val) = &$field_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::StringSlice($getter as fn(&MqttPacket) -> &str, 0));
        }
    };
}

macro_rules! encode_raw_bytes {
    ($target: ident, $getter: ident) => {
        $target.push_back(EncodingStep::BytesSlice($getter as fn(&MqttPacket) -> &[u8], 0));
    };
}

macro_rules! encode_optional_length_prefixed_bytes {
    ($target: ident, $getter: ident, $optional_value: expr) => {
        if let Some(val) = &$optional_value {
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::BytesSlice($getter as fn(&MqttPacket) -> &[u8], 0));
        } else {
            $target.push_back(EncodingStep::Uint16(0));
        }
    };
}

macro_rules! encode_optional_bytes_property {
    ($target: ident, $getter: ident, $property_key: expr, $field_value: expr) => {
        if let Some(val) = &$field_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::BytesSlice($getter as fn(&MqttPacket) -> &[u8], 0));
        }
    };
}

macro_rules! encode_user_property {
    ($target: ident, $user_property_getter: ident, $value: ident, $index: expr) => {
        {
            $target.push_back(EncodingStep::Uint16($value.name.len() as u16));
            $target.push_back(EncodingStep::UserPropertyName($user_property_getter as fn(&MqttPacket, usize) -> &UserProperty, $index, 0));
            $target.push_back(EncodingStep::Uint16($value.value.len() as u16));
            $target.push_back(EncodingStep::UserPropertyValue($user_property_getter as fn(&MqttPacket, usize) -> &UserProperty, $index, 0));
        }
    };
}

macro_rules! encode_user_properties {
    ($target: ident, $user_property_getter: ident, $properties_ref: expr) => {
        {
            if let Some(properties) = &$properties_ref {
                for (i, user_property) in properties.iter().enumerate() {
                    encode_user_property!($target, $user_property_getter, user_property, i);
                }
            }
        }
    };
}

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

fn get_publish_packet_content_type(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Publish, content_type)
}

fn get_publish_packet_response_topic(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Publish, response_topic)
}

fn get_publish_packet_correlation_data(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Publish, correlation_data)
}

fn get_publish_packet_topic(packet: &MqttPacket) -> &str {
    get_packet_field!(packet, MqttPacket::Publish, topic)
}

fn get_publish_packet_payload(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Publish, payload)
}

fn get_publish_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Publish(publish) = packet {
        if let Some(properties) = &publish.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}


static MQTT5_CONNECT_PROTOCOL_BYTES: [u8; 7] = [0, 4, 77, 81, 84, 84, 5];
fn get_connect_protocol_bytes(_ : &MqttPacket) -> &'static[u8] {
    return &MQTT5_CONNECT_PROTOCOL_BYTES;
}

fn compute_connect_flags(packet: &ConnectPacket) -> u8 {
    let mut flags : u8 = 0;
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

fn compute_connect_packet_length_properties(packet: &ConnectPacket) -> (u32, u32, u32) {
    (0, 0, 0)
}

impl Encodable for ConnectPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep<MqttPacket>>) {
        let (total_remaining_length, connect_property_length, will_property_length) = compute_connect_packet_length_properties(self);

        encode_expression!(steps, Uint8, 1u8 << 4);
        encode_expression!(steps, Vli, total_remaining_length);
        encode_raw_bytes!(steps, get_connect_protocol_bytes);
        encode_expression!(steps, Uint8, compute_connect_flags(self));
        encode_expression!(steps, Uint16, self.keep_alive_interval_seconds);

        encode_expression!(steps, Vli, connect_property_length);
        encode_optional_property!(steps, Uint32, PROPERTY_KEY_SESSION_EXPIRY_INTERVAL, self.session_expiry_interval_seconds);
        encode_optional_property!(steps, Uint16, PROPERTY_KEY_RECEIVE_MAXIMUM, self.receive_maximum);
        encode_optional_property!(steps, Uint32, PROPERTY_KEY_MAXIMUM_PACKET_SIZE, self.maximum_packet_size_bytes);
        encode_optional_property!(steps, Uint16, PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM, self.topic_alias_maximum);
        encode_optional_boolean_property!(steps, PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION, self.request_response_information);
        encode_optional_boolean_property!(steps, PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION, self.request_problem_information);
        encode_optional_string_property!(steps, get_connect_packet_authentication_method, PROPERTY_KEY_AUTHENTICATION_METHOD, self.authentication_method);
        encode_optional_bytes_property!(steps, get_connect_packet_authentication_data, PROPERTY_KEY_AUTHENTICATION_DATA, self.authentication_data);
        encode_user_properties!(steps, get_connect_packet_user_property, self.user_properties);

        encode_optional_length_prefixed_string!(steps, get_connect_packet_client_id, self.client_id);

        if let Some(will) = &self.will {
            encode_expression!(steps, Vli, will_property_length);
            encode_optional_property!(steps, Uint32, PROPERTY_KEY_WILL_DELAY_INTERVAL, self.will_delay_interval_seconds);
            encode_optional_enum_property!(steps, Uint8, PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR, u8, will.payload_format);
            encode_optional_property!(steps, Uint32, PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL, will.message_expiry_interval_seconds);
            encode_optional_string_property!(steps, get_publish_packet_content_type, PROPERTY_KEY_CONTENT_TYPE, &will.content_type);
            encode_optional_string_property!(steps, get_publish_packet_response_topic, PROPERTY_KEY_RESPONSE_TOPIC, &will.response_topic);
            encode_optional_bytes_property!(steps, get_publish_packet_correlation_data, PROPERTY_KEY_CORRELATION_DATA, will.correlation_data);
            encode_user_properties!(steps, get_publish_packet_user_property, will.user_properties);

            encode_length_prefixed_string!(steps, get_publish_packet_topic, will.topic);
            encode_optional_length_prefixed_bytes!(steps, get_publish_packet_payload, will.payload);
        }

        encode_optional_length_prefixed_string!(steps, get_connect_packet_username, self.username);
        encode_optional_length_prefixed_bytes!(steps, get_connect_packet_password, self.password);
    }
}

impl Encodable for MqttPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep<MqttPacket>>) {
        match self {
            MqttPacket::Connect(packet) => { packet.write_encoding_steps(steps); }
            _ => {}
        }
    }
}

pub struct Encoder {
    steps : VecDeque<EncodingStep<MqttPacket>>
}

impl Encoder {

    pub fn new() -> Encoder {
        Encoder { steps: VecDeque::new() }
    }

    pub fn reset(&mut self, packet: &MqttPacket) {
        self.steps.clear();
        packet.write_encoding_steps(&mut self.steps);
    }

    pub fn write_debug(&mut self, packet: &MqttPacket, dest: &mut Vec<u8>) {
        while !self.steps.is_empty() {
            let step = self.steps.pop_front().unwrap();
            handle_step(step, packet, dest);
        }
    }


}

fn handle_step(step: EncodingStep<MqttPacket>, packet: &MqttPacket, dest: &mut Vec<u8>) {
    match step {
        EncodingStep::Uint8(val) => {
            dest.push(val);
        }
        EncodingStep::Uint16(val) => {
            dest.extend_from_slice(&val.to_le_bytes());
        }
        EncodingStep::Uint32(val) => {
            dest.extend_from_slice(&val.to_le_bytes());
        }
        EncodingStep::Vli(val) => {
            // TODO Fix
            dest.extend_from_slice(&val.to_le_bytes());
        }
        EncodingStep::StringSlice(getter, _) => {
            let str_slice = getter(packet);
            dest.extend_from_slice(str_slice.as_bytes());
        }
        EncodingStep::BytesSlice(getter, _) => {
            let slice = getter(packet);
            dest.extend_from_slice(slice);
        }
        EncodingStep::UserPropertyName(getter, index, _) => {
            let property = getter(packet, index);
            dest.extend_from_slice(property.name.as_bytes());
        }
        EncodingStep::UserPropertyValue(getter, index, _) => {
            let property = getter(packet, index);
            dest.extend_from_slice(property.value.as_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoder_step_lifetime() {

        let mut encoder  = Encoder::new();

        let packet = MqttPacket::Connect(ConnectPacket { keep_alive_interval_seconds: 1200, ..Default::default() });
        encoder.reset(&packet);
    }

}
