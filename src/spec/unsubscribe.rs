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
pub struct UnsubscribePacket {
    pub packet_id: u16,

    pub topic_filters: Vec<String>,

    pub user_properties: Option<Vec<UserProperty>>,
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
pub(crate) fn write_unsubscribe_encoding_steps(packet: &UnsubscribePacket, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
    let (total_remaining_length, unsubscribe_property_length) = compute_unsubscribe_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, UNSUBSCRIBE_FIRST_BYTE);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    encode_integral_expression!(steps, Uint16, packet.packet_id);
    encode_integral_expression!(steps, Vli, unsubscribe_property_length);
    encode_user_properties!(steps, get_unsubscribe_packet_user_property, packet.user_properties);

    let topic_filters = &packet.topic_filters;
    for (i, topic_filter) in topic_filters.iter().enumerate() {
        encode_indexed_string!(steps, get_unsubscribe_packet_topic_filter, topic_filter, i);
    }

    Ok(())
}

fn decode_unsubscribe_properties(property_bytes: &[u8], packet : &mut UnsubscribePacket) -> Mqtt5Result<(), ()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => { return Err(Mqtt5Error::MalformedPacket); }
        }
    }

    Ok(())
}

pub(crate) fn decode_unsubscribe_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<UnsubscribePacket, ()> {
    let mut packet = UnsubscribePacket { ..Default::default() };

    if first_byte != UNSUBSCRIBE_FIRST_BYTE {
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
    let mut payload_bytes = &mutable_body[properties_length..];

    decode_unsubscribe_properties(properties_bytes, &mut packet)?;

    while payload_bytes.len() > 0 {
        let mut topic_filter = String::new();
        payload_bytes = decode_length_prefixed_string(payload_bytes, &mut topic_filter)?;

        packet.topic_filters.push(topic_filter);
    }

    Ok(packet)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decoder::testing::*;

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
}
