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

/// Data model of an [MQTT5 SUBSCRIBE](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901161) packet.
#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct SubscribePacket {

    /// Packet Id of the subscribe.  Setting this value on an outbound subscribe has no effect on the
    /// actual packet id used by the client.
    pub packet_id: u16,

    /// List of topic filter subscriptions that the client wishes to listen to
    ///
    /// See [MQTT5 Subscribe Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901168)
    pub subscriptions: Vec<Subscription>,

    /// A positive integer to associate with all subscriptions in this request.  Publish packets that match
    /// a subscription in this request should include this identifier in the resulting message.
    ///
    /// See [MQTT5 Subscription Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901166)
    pub subscription_identifier: Option<u32>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901167)
    pub user_properties: Option<Vec<UserProperty>>,
}


#[rustfmt::skip]
fn compute_subscribe_packet_length_properties(packet: &SubscribePacket) -> Mqtt5Result<(u32, u32)> {
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
pub(crate) fn write_subscribe_encoding_steps(packet: &SubscribePacket, _: &mut EncodingContext, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<()> {
    let (total_remaining_length, subscribe_property_length) = compute_subscribe_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, SUBSCRIBE_FIRST_BYTE);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    encode_integral_expression!(steps, Uint16, packet.packet_id);
    encode_integral_expression!(steps, Vli, subscribe_property_length);
    encode_optional_property!(steps, Uint32, PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER, packet.subscription_identifier);
    encode_user_properties!(steps, get_subscribe_packet_user_property, packet.user_properties);

    let subscriptions = &packet.subscriptions;
    for (i, subscription) in subscriptions.iter().enumerate() {
        encode_indexed_string!(steps, get_subscribe_packet_topic_filter, subscription.topic_filter, i);
        encode_integral_expression!(steps, Uint8, compute_subscription_options_byte(subscription));
    }

    Ok(())
}

fn decode_subscribe_properties(property_bytes: &[u8], packet : &mut SubscribePacket) -> Mqtt5Result<()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.subscription_identifier)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => { return Err(Mqtt5Error::MalformedPacket); }
        }
    }

    Ok(())
}

pub(crate) fn decode_subscribe_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<SubscribePacket>> {
    let mut packet = Box::new(SubscribePacket { ..Default::default() });

    if first_byte != SUBSCRIBE_FIRST_BYTE {
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

    decode_subscribe_properties(properties_bytes, &mut packet)?;

    while payload_bytes.len() > 0 {
        let mut subscription = Subscription {
            ..Default::default()
        };

        payload_bytes = decode_length_prefixed_string(payload_bytes, &mut subscription.topic_filter)?;

        let mut subscription_options : u8 = 0;
        payload_bytes = decode_u8(payload_bytes, &mut subscription_options)?;

        subscription.qos = convert_u8_to_quality_of_service(subscription_options & 0x03)?;

        if (subscription_options & SUBSCRIPTION_OPTIONS_NO_LOCAL_MASK) != 0 {
            subscription.no_local = true;
        }

        if (subscription_options & SUBSCRIPTION_OPTIONS_RETAIN_AS_PUBLISHED_MASK) != 0 {
            subscription.retain_as_published = true;
        }

        subscription.retain_handling_type = convert_u8_to_retain_handling_type((subscription_options >> SUBSCRIPTION_OPTIONS_RETAIN_HANDLING_SHIFT) & 0x03)?;

        packet.subscriptions.push(subscription);
    }

    Ok(packet)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn subscribe_round_trip_encode_decode_default() {
        let packet = Box::new(SubscribePacket {
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Subscribe(packet)));
    }

    #[test]
    fn subscribe_round_trip_encode_decode_basic() {
        let packet = Box::new(SubscribePacket {
            packet_id : 123,
            subscriptions : vec![ Subscription { topic_filter: "hello/world".to_string(), qos: QualityOfService::AtLeastOnce, ..Default::default() } ],
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Subscribe(packet)));
    }

    #[test]
    fn subscribe_round_trip_encode_decode_all_properties() {
        let packet = Box::new(SubscribePacket {
            packet_id : 123,
            subscriptions : vec![
                Subscription {
                    topic_filter: "a/b/c/d/e".to_string(),
                    qos: QualityOfService::ExactlyOnce,
                    retain_as_published: true,
                    no_local: false,
                    retain_handling_type: RetainHandlingType::DontSend
                },
                Subscription {
                    topic_filter: "the/best/+/filter/*".to_string(),
                    qos: QualityOfService::AtMostOnce,
                    retain_as_published: false,
                    no_local: true,
                    retain_handling_type: RetainHandlingType::SendOnSubscribeIfNew
                }
            ],
            subscription_identifier : Some(41),
            user_properties: Some(vec!(
                UserProperty{name: "Worms".to_string(), value: "inmyhead".to_string()},
            )),
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Subscribe(packet)));
    }
}