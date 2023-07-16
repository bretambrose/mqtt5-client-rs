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

        encode_length_prefixed_optional_string!(steps, get_connect_packet_username, self.username);
        encode_length_prefixed_optional_bytes!(steps, get_connect_packet_password, self.password);

        Ok(())
    }
}

impl Encodable for MqttPacket {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
        match self {
            MqttPacket::Connect(packet) => {
                return packet.write_encoding_steps(steps);
            }
            _ => Err(Mqtt5Error::Unimplemented(())),
        }
    }
}

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
            process_encoding_step(&mut self.steps, step, packet, dest);

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
