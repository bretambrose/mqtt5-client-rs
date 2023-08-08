/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::decode::utils::*;
use crate::encode::utils::*;
use crate::spec::*;
use crate::spec::utils::*;

use std::collections::VecDeque;

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct PublishPacket {
    pub packet_id: u16,

    pub topic: String,

    pub qos: QualityOfService,
    pub duplicate: bool,
    pub retain: bool,

    pub payload: Option<Vec<u8>>,

    pub payload_format: Option<PayloadFormatIndicator>,
    pub message_expiry_interval_seconds: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,

    pub subscription_identifiers: Option<Vec<u32>>,

    pub content_type: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,
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

    let mut total_remaining_length = compute_variable_length_integer_encode_size(publish_property_section_length)?;

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
pub(crate) fn write_publish_encoding_steps(packet: &PublishPacket, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
    let (total_remaining_length, publish_property_length) = compute_publish_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, compute_publish_fixed_header_first_byte(packet));
    encode_integral_expression!(steps, Vli, total_remaining_length);

    /*
     * Variable Header
     * UTF-8 Encoded Topic Name
     * 2 byte Packet Identifier
     * 1-4 byte Property Length as Variable Byte Integer
     * n bytes Properties
     */
    encode_length_prefixed_string!(steps, get_publish_packet_topic, packet.topic);
    if packet.qos != QualityOfService::AtMostOnce {
        encode_integral_expression!(steps, Uint16, packet.packet_id);
    }
    encode_integral_expression!(steps, Vli, publish_property_length);

    encode_optional_enum_property!(steps, Uint8, PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR, u8, packet.payload_format);
    encode_optional_property!(steps, Uint32, PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL, packet.message_expiry_interval_seconds);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_TOPIC_ALIAS, packet.topic_alias);
    encode_optional_string_property!(steps, get_publish_packet_response_topic, PROPERTY_KEY_RESPONSE_TOPIC, packet.response_topic);
    encode_optional_bytes_property!(steps, get_publish_packet_correlation_data, PROPERTY_KEY_CORRELATION_DATA, packet.correlation_data);

    if let Some(subscription_identifiers) = &packet.subscription_identifiers {
        for val in subscription_identifiers {
            encode_integral_expression!(steps, Uint8, PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER);
            encode_integral_expression!(steps, Vli, *val);
        }
    }

    encode_optional_string_property!(steps, get_publish_packet_content_type, PROPERTY_KEY_CONTENT_TYPE, &packet.content_type);
    encode_user_properties!(steps, get_publish_packet_user_property, packet.user_properties);

    if packet.payload.is_some() {
        encode_raw_bytes!(steps, get_publish_packet_payload);
    }

    Ok(())
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
            _ => { return Err(Mqtt5Error::MalformedPacket); }
        }
    }

    Ok(())
}

pub(crate) fn decode_publish_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PublishPacket, ()> {
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
    if properties_length > mutable_body.len() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let properties_bytes = &mutable_body[..properties_length];
    let payload_bytes = &mutable_body[properties_length..];

    decode_publish_properties(properties_bytes, &mut packet)?;

    if payload_bytes.len() > 0 {
        packet.payload = Some(payload_bytes.to_vec());
    }

    Ok(packet)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

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
}
