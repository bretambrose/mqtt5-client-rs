/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::packet::*;
use crate::client::*;

pub static PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR : u8 = 1;
pub static PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL : u8 = 2;
pub static PROPERTY_KEY_CONTENT_TYPE : u8 = 3;
pub static PROPERTY_KEY_RESPONSE_TOPIC : u8 = 8;
pub static PROPERTY_KEY_CORRELATION_DATA : u8 = 9;
pub static PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER : u8 = 11;
pub static PROPERTY_KEY_SESSION_EXPIRY_INTERVAL : u8 = 17;
pub static PROPERTY_KEY_ASSIGNED_CLIENT_IDENTIFIER : u8 = 18;
pub static PROPERTY_KEY_SERVER_KEEP_ALIVE : u8 = 19;
pub static PROPERTY_KEY_AUTHENTICATION_METHOD : u8 = 21;
pub static PROPERTY_KEY_AUTHENTICATION_DATA : u8 = 22;
pub static PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION : u8 = 23;
pub static PROPERTY_KEY_WILL_DELAY_INTERVAL : u8 = 24;
pub static PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION : u8 = 25;
pub static PROPERTY_KEY_RESPONSE_INFORMATION : u8 = 26;
pub static PROPERTY_KEY_SERVER_REFERENCE : u8 = 28;
pub static PROPERTY_KEY_REASON_STRING : u8 = 31;
pub static PROPERTY_KEY_RECEIVE_MAXIMUM : u8 = 33;
pub static PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM : u8 = 34;
pub static PROPERTY_KEY_TOPIC_ALIAS : u8 = 35;
pub static PROPERTY_KEY_MAXIMUM_QOS : u8 = 36;
pub static PROPERTY_KEY_RETAIN_AVAILABLE : u8 = 37;
pub static PROPERTY_KEY_USER_PROPERTY : u8 = 38;
pub static PROPERTY_KEY_MAXIMUM_PACKET_SIZE : u8 = 39;
pub static PROPERTY_KEY_WILDCARD_SUBSCRIPTIONS_AVAILABLE : u8 = 40;
pub static PROPERTY_KEY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE : u8 = 41;
pub static PROPERTY_KEY_SHARED_SUBSCRIPTIONS_AVAILABLE : u8 = 42;


macro_rules! get_packet_field {
    ($target: expr, $pat: path, $field_name: ident) => {
        if let $pat(a) = $target { // #1
            &a.$field_name
        } else {
            panic!("Packet variant mismatch");
        }
    };
}

pub(crate) use get_packet_field;

macro_rules! get_optional_packet_field {
    ($target: expr, $pat: path, $field_name: ident) => {
        if let $pat(a) = $target { // #1
            &a.$field_name.as_ref().unwrap()
        } else {
            panic!("Packet variant mismatch");
        }
    };
}

pub(crate) use get_optional_packet_field;

macro_rules! encode_expression {
    ($target: ident, $enum_variant: ident, $value: expr) => {
        $target.push_back(EncodingStep::$enum_variant($value));
    };
}

pub(crate) use encode_expression;

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

pub(crate) use encode_optional_property;

macro_rules! encode_optional_enum_property {
    ($target: ident, $enum_variant: ident, $property_key: expr, $int_type: ty, $optional_value: expr) => {
        if let Some(val) = $optional_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::$enum_variant(val as $int_type));
        }
    };
}

pub(crate) use encode_optional_enum_property;

macro_rules! encode_optional_boolean_property {
    ($target: ident, $property_key: expr, $optional_value: expr) => {
        if let Some(val) = $optional_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::Uint8(if val { 1u8 } else { 0u8 }));
        }
    };
}

pub(crate) use encode_optional_boolean_property;

macro_rules! encode_length_prefixed_string {
    ($target: ident, $getter: ident, $value: expr) => {
        $target.push_back(EncodingStep::Uint16($value.len() as u16));
        $target.push_back(EncodingStep::StringSlice($getter as fn(&MqttPacket) -> &str, 0));
    };
}

pub(crate) use encode_length_prefixed_string;

macro_rules! encode_length_prefixed_optional_string {
    ($target: ident, $getter: ident, $optional_value: expr) => {
        if let Some(val) = &$optional_value {
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::StringSlice($getter as fn(&MqttPacket) -> &str, 0));
        } else {
            $target.push_back(EncodingStep::Uint16(0));
        }
    };
}

pub(crate) use encode_length_prefixed_optional_string;

macro_rules! encode_optional_string_property {
    ($target: ident, $getter: ident, $property_key: expr, $field_value: expr) => {
        if let Some(val) = &$field_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::StringSlice($getter as fn(&MqttPacket) -> &str, 0));
        }
    };
}

pub(crate) use encode_optional_string_property;

macro_rules! encode_raw_bytes {
    ($target: ident, $getter: ident) => {
        $target.push_back(EncodingStep::BytesSlice($getter as fn(&MqttPacket) -> &[u8], 0));
    };
}

pub(crate) use encode_raw_bytes;

macro_rules! encode_length_prefixed_optional_bytes {
    ($target: ident, $getter: ident, $optional_value: expr) => {
        if let Some(val) = &$optional_value {
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::BytesSlice($getter as fn(&MqttPacket) -> &[u8], 0));
        } else {
            $target.push_back(EncodingStep::Uint16(0));
        }
    };
}

pub(crate) use encode_length_prefixed_optional_bytes;

macro_rules! encode_optional_bytes_property {
    ($target: ident, $getter: ident, $property_key: expr, $field_value: expr) => {
        if let Some(val) = &$field_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::BytesSlice($getter as fn(&MqttPacket) -> &[u8], 0));
        }
    };
}

pub(crate) use encode_optional_bytes_property;

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

pub(crate) use encode_user_property;

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

pub(crate) use encode_user_properties;

/*****************************************************/

macro_rules! add_optional_u8_property_length {
    ($target: ident, $optional_value: expr) => {
        if $optional_value.is_some() {
            $target += 2;
        }
    };
}

pub(crate) use add_optional_u8_property_length;

macro_rules! add_optional_u16_property_length {
    ($target: ident, $optional_value: expr) => {
        if $optional_value.is_some() {
            $target += 3;
        }
    };
}

pub(crate) use add_optional_u16_property_length;

macro_rules! add_optional_u32_property_length {
    ($target: ident, $optional_value: expr) => {
        if $optional_value.is_some() {
            $target += 5;
        }
    };
}

pub(crate) use add_optional_u32_property_length;

macro_rules! add_optional_string_property_length {
    ($target: ident, $optional_value: expr) => {
        if let Some(val) = &$optional_value {
            $target += 3 + val.len();
        }
    };
}

pub(crate) use add_optional_string_property_length;

macro_rules! add_optional_bytes_property_length {
    ($target: ident, $optional_value: expr) => {
        if let Some(val) = &$optional_value {
            $target += 3 + val.len();
        }
    };
}

pub(crate) use add_optional_bytes_property_length;

macro_rules! add_optional_string_length {
    ($target: ident, $optional_value: expr) => {
        $target += 2;
        if let Some(val) = &$optional_value {
            $target += val.len();
        }
    };
}

pub(crate) use add_optional_string_length;

macro_rules! add_optional_bytes_length {
    ($target: ident, $optional_value: expr) => {
        $target += 2;
        if let Some(val) = &$optional_value {
            $target += val.len();
        }
    };
}

pub(crate) use add_optional_bytes_length;

pub static MAXIMUM_VARIABLE_LENGTH_INTEGER : usize = (1 << 28) - 1;

pub fn compute_user_properties_length(properties: &Option<Vec<UserProperty>>) -> usize {
    let mut total  = 0;
    if let Some(props) = properties {
        let property_count = props.len();
        total += property_count * 4; // 4 bytes of length-prefix per property
        for property in props {
            total += property.name.len();
            total += property.value.len();
        }
    }

    total
}

pub fn compute_variable_length_integer_encode_size(value : usize) -> Mqtt5Result<usize, ()> {
    if value < 1usize << 7 {
        Ok(1)
    } else if value < 1usize << 14 {
        Ok(2)
    } else if value < 1usize << 21 {
        Ok(3)
    } else if value < 1usize << 28 {
        Ok(4)
    } else {
        Err(Mqtt5Error::VariableLengthIntegerMaximumExceeded)
    }
}

pub fn encode_vli(value: u32, dest: &mut Vec<u8>) {
    let mut done = false;
    let mut val = value;
    while !done {
        let mut byte : u8 = (val & 0x7F) as u8;
        val /= 128;

        if val != 0 {
            byte |= 128;
        }

        dest.push(byte);

        done = val == 0;
    }
}
