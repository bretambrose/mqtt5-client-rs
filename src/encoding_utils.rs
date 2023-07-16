/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

///
/// Internal utilities to encode MQTT5 packets, based on the MQTT5 spec

use std::collections::VecDeque;

use crate::spec::{UserProperty, MqttPacket};
use crate::{Mqtt5Error, Mqtt5Result};

pub(crate) enum EncodingStep {
    Uint8(u8),
    Uint16(u16),
    Uint32(u32),
    Vli(u32),
    StringSlice(fn (&MqttPacket) -> &str, usize),
    BytesSlice(fn (&MqttPacket) -> &[u8], usize),
    UserPropertyName(fn (&MqttPacket, usize) -> &UserProperty, usize, usize),
    UserPropertyValue(fn (&MqttPacket, usize) -> &UserProperty, usize, usize),
}

pub(crate) trait Encodable {
    fn write_encoding_steps(&self, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()>;
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

pub(crate) use get_optional_packet_field;

macro_rules! encode_integral_expression {
    ($target: ident, $enum_variant: ident, $value: expr) => {
        $target.push_back(EncodingStep::$enum_variant($value));
    };
}

pub(crate) use encode_integral_expression;

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

fn encode_vli(value: u32, dest: &mut Vec<u8>) {
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


fn process_byte_slice_encoding(bytes: &[u8], offset: usize, dest: &mut Vec<u8>) -> usize {
    let dest_space_in_bytes = dest.capacity() - dest.len();
    let remaining_slice_bytes = bytes.len() - offset;
    let encodable_length = usize::min(dest_space_in_bytes, remaining_slice_bytes);
    let end_offset = offset + encodable_length;
    let encodable_slice = bytes.get(offset..end_offset).unwrap();
    dest.extend_from_slice(encodable_slice);

    if encodable_length < remaining_slice_bytes {
        end_offset
    } else {
        0
    }
}

pub(crate) fn process_encoding_step(steps : &mut VecDeque<EncodingStep>, step : EncodingStep, packet: &MqttPacket, dest: &mut Vec<u8>) {
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
            encode_vli(val, dest);
        }
        EncodingStep::StringSlice(getter, offset) => {
            let slice = getter(packet).as_bytes();
            let end_offset = process_byte_slice_encoding(slice, offset, dest);
            if end_offset > 0 {
                steps.push_front(EncodingStep::StringSlice(getter, end_offset));
            }
        }
        EncodingStep::BytesSlice(getter, offset) => {
            let slice = getter(packet);
            let end_offset = process_byte_slice_encoding(slice, offset, dest);
            if end_offset > 0 {
                steps.push_front(EncodingStep::BytesSlice(getter, end_offset));
            }
        }
        EncodingStep::UserPropertyName(getter, index, offset) => {
            let slice = getter(packet, index).name.as_bytes();
            let end_offset = process_byte_slice_encoding(slice, offset, dest);
            if end_offset > 0 {
                steps.push_front(EncodingStep::UserPropertyName(getter, index,end_offset));
            }
        }
        EncodingStep::UserPropertyValue(getter, index, offset) => {
            let slice = getter(packet, index).value.as_bytes();
            let end_offset = process_byte_slice_encoding(slice, offset, dest);
            if end_offset > 0 {
                steps.push_front(EncodingStep::UserPropertyValue(getter, index,end_offset));
            }
        }
    }
}
