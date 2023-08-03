/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::spec::*;
use crate::*;

pub const PACKET_TYPE_CONNECT: u8 = 1;
pub const PACKET_TYPE_CONNACK: u8 = 2;
pub const PACKET_TYPE_PUBLISH: u8 = 3;
pub const PACKET_TYPE_PUBACK: u8 = 4;
pub const PACKET_TYPE_PUBREC: u8 = 5;
pub const PACKET_TYPE_PUBREL: u8 = 6;
pub const PACKET_TYPE_PUBCOMP: u8 = 7;
pub const PACKET_TYPE_SUBSCRIBE: u8 = 8;
pub const PACKET_TYPE_SUBACK: u8 = 9;
pub const PACKET_TYPE_UNSUBSCRIBE: u8 = 10;
pub const PACKET_TYPE_UNSUBACK: u8 = 11;
pub const PACKET_TYPE_PINGREQ: u8 = 12;
pub const PACKET_TYPE_PINGRESP: u8 = 13;
pub const PACKET_TYPE_DISCONNECT: u8 = 14;
pub const PACKET_TYPE_AUTH: u8 = 15;

pub const PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR: u8 = 1;
pub const PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL: u8 = 2;
pub const PROPERTY_KEY_CONTENT_TYPE: u8 = 3;
pub const PROPERTY_KEY_RESPONSE_TOPIC: u8 = 8;
pub const PROPERTY_KEY_CORRELATION_DATA: u8 = 9;
pub const PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER: u8 = 11;
pub const PROPERTY_KEY_SESSION_EXPIRY_INTERVAL: u8 = 17;
pub const PROPERTY_KEY_ASSIGNED_CLIENT_IDENTIFIER: u8 = 18;
pub const PROPERTY_KEY_SERVER_KEEP_ALIVE: u8 = 19;
pub const PROPERTY_KEY_AUTHENTICATION_METHOD: u8 = 21;
pub const PROPERTY_KEY_AUTHENTICATION_DATA: u8 = 22;
pub const PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION: u8 = 23;
pub const PROPERTY_KEY_WILL_DELAY_INTERVAL: u8 = 24;
pub const PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION: u8 = 25;
pub const PROPERTY_KEY_RESPONSE_INFORMATION: u8 = 26;
pub const PROPERTY_KEY_SERVER_REFERENCE: u8 = 28;
pub const PROPERTY_KEY_REASON_STRING: u8 = 31;
pub const PROPERTY_KEY_RECEIVE_MAXIMUM: u8 = 33;
pub const PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM: u8 = 34;
pub const PROPERTY_KEY_TOPIC_ALIAS: u8 = 35;
pub const PROPERTY_KEY_MAXIMUM_QOS: u8 = 36;
pub const PROPERTY_KEY_RETAIN_AVAILABLE: u8 = 37;
pub const PROPERTY_KEY_USER_PROPERTY: u8 = 38;
pub const PROPERTY_KEY_MAXIMUM_PACKET_SIZE: u8 = 39;
pub const PROPERTY_KEY_WILDCARD_SUBSCRIPTIONS_AVAILABLE: u8 = 40;
pub const PROPERTY_KEY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE: u8 = 41;
pub const PROPERTY_KEY_SHARED_SUBSCRIPTIONS_AVAILABLE: u8 = 42;

pub const PUBLISH_PACKET_FIXED_HEADER_DUPLICATE_FLAG : u8 = 8;
pub const PUBLISH_PACKET_FIXED_HEADER_RETAIN_FLAG : u8 = 1;
pub const PUBLISH_PACKET_FIXED_HEADER_QOS_MASK : u8 = 3;

pub(crate) fn convert_u8_to_quality_of_service(value: u8) -> Mqtt5Result<QualityOfService, ()> {
    match value {
        0 => { Ok(QualityOfService::AtMostOnce) }
        1 => { Ok(QualityOfService::AtLeastOnce) }
        2 => { Ok(QualityOfService::ExactlyOnce) }
        _ => { Err(Mqtt5Error::ProtocolError) }
    }
}

pub(crate) fn convert_u8_to_payload_format_indicator(value: u8) -> Mqtt5Result<PayloadFormatIndicator, ()> {
    match value {
        0 => { Ok(PayloadFormatIndicator::Bytes) }
        1 => { Ok(PayloadFormatIndicator::Utf8) }
        _ => { Err(Mqtt5Error::ProtocolError) }
    }
}

pub(crate) fn convert_u8_to_puback_reason_code(value: u8) -> Mqtt5Result<PubackReasonCode, ()> {
    match value {
        0 => { Ok(PubackReasonCode::Success) }
        16 => { Ok(PubackReasonCode::NoMatchingSubscribers) }
        128 => { Ok(PubackReasonCode::UnspecifiedError) }
        131 => { Ok(PubackReasonCode::ImplementationSpecificError) }
        135 => { Ok(PubackReasonCode::NotAuthorized) }
        144 => { Ok(PubackReasonCode::TopicNameInvalid) }
        145 => { Ok(PubackReasonCode::PacketIdentifierInUse) }
        151 => { Ok(PubackReasonCode::QuotaExceeded) }
        153 => { Ok(PubackReasonCode::PayloadFormatInvalid) }
        _ => { Err(Mqtt5Error::ProtocolError) }
    }
}

pub(crate) fn convert_u8_to_pubrec_reason_code(value: u8) -> Mqtt5Result<PubrecReasonCode, ()> {
    match value {
        0 => { Ok(PubrecReasonCode::Success) }
        16 => { Ok(PubrecReasonCode::NoMatchingSubscribers) }
        128 => { Ok(PubrecReasonCode::UnspecifiedError) }
        131 => { Ok(PubrecReasonCode::ImplementationSpecificError) }
        135 => { Ok(PubrecReasonCode::NotAuthorized) }
        144 => { Ok(PubrecReasonCode::TopicNameInvalid) }
        145 => { Ok(PubrecReasonCode::PacketIdentifierInUse) }
        151 => { Ok(PubrecReasonCode::QuotaExceeded) }
        153 => { Ok(PubrecReasonCode::PayloadFormatInvalid) }
        _ => { Err(Mqtt5Error::ProtocolError) }
    }
}

pub(crate) fn convert_u8_to_pubrel_reason_code(value: u8) -> Mqtt5Result<PubrelReasonCode, ()> {
    match value {
        0 => { Ok(PubrelReasonCode::Success) }
        146 => { Ok(PubrelReasonCode::PacketIdentifierNotFound) }
        _ => { Err(Mqtt5Error::ProtocolError) }
    }
}

pub(crate) fn convert_u8_to_pubcomp_reason_code(value: u8) -> Mqtt5Result<PubcompReasonCode, ()> {
    match value {
        0 => { Ok(PubcompReasonCode::Success) }
        146 => { Ok(PubcompReasonCode::PacketIdentifierNotFound) }
        _ => { Err(Mqtt5Error::ProtocolError) }
    }
}
