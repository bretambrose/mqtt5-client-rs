/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::{Mqtt5Error, Mqtt5Result};
use crate::spec::UserProperty;

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum DecodeVliResult<'a> {
    InsufficientData,
    Value(u32, &'a[u8]), /* (decoded value, remaining bytes) */
}

pub(crate) fn decode_vli(buffer: &[u8]) -> Mqtt5Result<DecodeVliResult> {
    let mut value: u32 = 0;
    let mut needs_data: bool;
    let mut shift: u32 = 0;
    let data_len = buffer.len();

    for i in 0..4 {
        if i >= data_len {
            return Ok(DecodeVliResult::InsufficientData);
        }

        let byte = buffer[i];
        value |= ((byte & 0x7F) as u32) << shift;
        shift += 7;

        needs_data = (byte & 0x80) != 0;
        if !needs_data {
            return Ok(DecodeVliResult::Value(value, &buffer[(i + 1)..]));
        }
    }

    Err(Mqtt5Error::DecoderInvalidVli)
}

pub(crate) fn decode_vli_into_mutable<'a>(buffer: &'a[u8], value: &mut usize) -> Mqtt5Result<&'a[u8]> {
    let decode_result = decode_vli(buffer);
    match decode_result {
        Ok(DecodeVliResult::InsufficientData) => { Err(Mqtt5Error::MalformedPacket)}
        Ok(DecodeVliResult::Value(vli, remaining_slice)) => {
            *value = vli as usize;
            Ok(remaining_slice)
        }
        Err(_) => {
            Err(Mqtt5Error::MalformedPacket)
        }
    }
}

fn map_utf8_err_to_malformed_packet(_: std::str::Utf8Error) -> Mqtt5Error {
    return Mqtt5Error::MalformedPacket;
}

pub(crate) fn decode_length_prefixed_string<'a>(bytes: &'a[u8], value: &mut String) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let value_length : usize = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];
    if value_length > mutable_bytes.len() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let decode_utf8_result = std::str::from_utf8(&mutable_bytes[..value_length]).map_err(map_utf8_err_to_malformed_packet)?;
    *value = decode_utf8_result.to_string();
    Ok(&mutable_bytes[(value_length)..])
}

pub(crate) fn decode_optional_length_prefixed_string<'a>(bytes: &'a[u8], value: &mut Option<String>) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if value.is_some() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let value_length : usize = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];
    if value_length > mutable_bytes.len() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let decode_utf8_result = std::str::from_utf8(&mutable_bytes[..value_length]).map_err(map_utf8_err_to_malformed_packet)?;
    *value = Some(decode_utf8_result.to_string());
    Ok(&mutable_bytes[(value_length)..])
}

pub(crate) fn decode_length_prefixed_optional_string<'a>(bytes: &'a[u8], value: &mut Option<String>) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if value.is_some() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let value_length : usize = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];

    if value_length == 0 {
        *value = None;
        return Ok(mutable_bytes);
    }

    if value_length > mutable_bytes.len() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let decode_utf8_result = std::str::from_utf8(&mutable_bytes[..value_length]).map_err(map_utf8_err_to_malformed_packet)?;
    *value = Some(decode_utf8_result.to_string());
    Ok(&mutable_bytes[(value_length)..])
}

pub(crate) fn decode_optional_length_prefixed_bytes<'a>(bytes: &'a[u8], value: &mut Option<Vec<u8>>) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if value.is_some() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let value_length : usize = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];
    if value_length > mutable_bytes.len() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    *value = Some(Vec::from(&mutable_bytes[..value_length]));
    Ok(&mutable_bytes[(value_length)..])
}

pub(crate) fn decode_length_prefixed_optional_bytes<'a>(bytes: &'a[u8], value: &mut Option<Vec<u8>>) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if value.is_some() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let value_length : usize = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];

    if value_length == 0 {
        *value = None;
        return Ok(mutable_bytes);
    }

    if value_length > mutable_bytes.len() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    *value = Some(Vec::from(&mutable_bytes[..value_length]));
    Ok(&mutable_bytes[(value_length)..])
}

pub(crate) fn decode_user_property<'a>(bytes: &'a[u8], properties: &mut Option<Vec<UserProperty>>) -> Mqtt5Result<&'a[u8]> {
    let mut property : UserProperty = UserProperty { ..Default::default() };

    let mut mutable_bytes = bytes;
    mutable_bytes = decode_length_prefixed_string(mutable_bytes, &mut property.name)?;
    mutable_bytes = decode_length_prefixed_string(mutable_bytes, &mut property.value)?;

    if properties.is_none() {
        *properties = Some(Vec::new());
    }

    properties.as_mut().unwrap().push(property);

    Ok(mutable_bytes)
}

pub(crate) fn decode_u8<'a>(bytes: &'a[u8], value: &mut u8) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 1 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    *value = bytes[0];

    Ok(&bytes[1..])
}

pub(crate) fn decode_optional_u8_as_bool<'a>(bytes: &'a[u8], value: &mut Option<bool>) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 1 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if value.is_some() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if bytes[0] == 0 {
        *value = Some(false);
    } else if bytes[0] == 1 {
        *value = Some(true);
    } else {
        return Err(Mqtt5Error::MalformedPacket);
    }

    Ok(&bytes[1..])
}

pub(crate) fn decode_u8_as_enum<'a, T>(bytes: &'a[u8], value: &mut T, converter: fn(u8) ->Mqtt5Result<T>) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 1 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    *value = converter(bytes[0])?;

    Ok(&bytes[1..])
}

pub(crate) fn decode_optional_u8_as_enum<'a, T>(bytes: &'a[u8], value: &mut Option<T>, converter: fn(u8) -> Mqtt5Result<T>) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 1 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if value.is_some() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    *value = Some(converter(bytes[0])?);

    Ok(&bytes[1..])
}

pub(crate) fn decode_u16<'a>(bytes: &'a[u8], value: &mut u16) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    *value = u16::from_le_bytes(bytes[..2].try_into().unwrap());

    Ok(&bytes[2..])
}

pub(crate) fn decode_optional_u16<'a>(bytes: &'a[u8], value: &mut Option<u16>) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if value.is_some() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    *value = Some(u16::from_le_bytes(bytes[..2].try_into().unwrap()));

    Ok(&bytes[2..])
}

pub(crate) fn decode_optional_u32<'a>(bytes: &'a[u8], value: &mut Option<u32>) -> Mqtt5Result<&'a[u8]> {
    if bytes.len() < 4 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if value.is_some() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    *value = Some(u32::from_le_bytes(bytes[..4].try_into().unwrap()));

    Ok(&bytes[4..])
}

macro_rules! define_ack_packet_decode_properties_function {
    ($function_name: ident, $packet_type: ident) => {
        fn $function_name(property_bytes: &[u8], packet : &mut $packet_type) -> Mqtt5Result<()> {
            let mut mutable_property_bytes = property_bytes;

            while mutable_property_bytes.len() > 0 {
                let property_key = mutable_property_bytes[0];
                mutable_property_bytes = &mutable_property_bytes[1..];

                match property_key {
                    PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
                    PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
                    _ => { return Err(Mqtt5Error::MalformedPacket); }
                }
            }

            Ok(())
        }
    };
}

pub(crate) use define_ack_packet_decode_properties_function;

macro_rules! define_ack_packet_decode_function {
    ($function_name: ident, $packet_type: ident, $packet_type_value: expr, $reason_code_converter_function_name: ident, $decode_properties_function_name: ident) => {
        pub(crate) fn $function_name(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<$packet_type>> {
            let mut packet = Box::new($packet_type { ..Default::default() });

            if first_byte != ($packet_type_value << 4) {
                return Err(Mqtt5Error::MalformedPacket);
            }

            let mut mutable_body = packet_body;
            mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;
            if mutable_body.len() == 0 {
                /* Success is the default, so nothing to do */
                return Ok(packet);
            }

            mutable_body = decode_u8_as_enum(mutable_body, &mut packet.reason_code, $reason_code_converter_function_name)?;
            if mutable_body.len() == 0 {
                return Ok(packet);
            }

            /* it's a mystery why the specification adds this field; it's completely unnecessary */
            let mut properties_length = 0;
            mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
            if properties_length != mutable_body.len() {
                return Err(Mqtt5Error::MalformedPacket);
            }

            $decode_properties_function_name(mutable_body, &mut packet)?;

            Ok(packet)
        }
    };
}

pub(crate) use define_ack_packet_decode_function;