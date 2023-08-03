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

pub(crate) fn decode_vli(buffer: &[u8]) -> Mqtt5Result<DecodeVliResult, ()> {
    let mut value: u32 = 0;
    let mut needs_data: bool = false;
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

pub(crate) fn decode_vli_into_mutable<'a>(buffer: &'a[u8], value: &mut usize) -> Mqtt5Result<&'a[u8], ()> {
    let decode_result = decode_vli(buffer);
    match decode_result {
        Ok(DecodeVliResult::InsufficientData) => { Err(Mqtt5Error::ProtocolError)}
        Ok(DecodeVliResult::Value(vli, remaining_slice)) => {
            *value = vli as usize;
            Ok(remaining_slice)
        }
        Err(_) => {
            Err(Mqtt5Error::ProtocolError)
        }
    }
}

pub(crate) fn decode_length_prefixed_string<'a>(bytes: &'a[u8], value: &mut String) -> Mqtt5Result<&'a[u8], ()> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::ProtocolError);
    }

    let value_length : usize = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];
    if value_length > mutable_bytes.len() {
        return Err(Mqtt5Error::ProtocolError);
    }

    let value_result = std::str::from_utf8(&mutable_bytes[..value_length]);
    match value_result {
        Ok(string_value) => {
            *value = string_value.to_string();
            Ok(&mutable_bytes[(value_length)..])
        }
        Err(_) => { Err(Mqtt5Error::ProtocolError) }
    }
}

pub(crate) fn decode_optional_length_prefixed_string<'a>(bytes: &'a[u8], value: &mut Option<String>) -> Mqtt5Result<&'a[u8], ()> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::ProtocolError);
    }
    let value_length : usize = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];
    if value_length > mutable_bytes.len() {
        return Err(Mqtt5Error::ProtocolError);
    }

    let value_result = std::str::from_utf8(&mutable_bytes[..value_length]);
    match value_result {
        Ok(string_value) => {
            *value = Some(string_value.to_string());
            Ok(&mutable_bytes[(value_length)..])
        }
        Err(_) => { Err(Mqtt5Error::ProtocolError) }
    }
}

pub(crate) fn decode_optional_length_prefixed_bytes<'a>(bytes: &'a[u8], value: &mut Option<Vec<u8>>) -> Mqtt5Result<&'a[u8], ()> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::ProtocolError);
    }
    let value_length : usize = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];
    if value_length > mutable_bytes.len() {
        return Err(Mqtt5Error::ProtocolError);
    }

    *value = Some(Vec::from(&mutable_bytes[..value_length]));
    Ok(&mutable_bytes[(value_length)..])
}

pub(crate) fn decode_user_property<'a>(bytes: &'a[u8], properties: &mut Option<Vec<UserProperty>>) -> Mqtt5Result<&'a[u8], ()> {
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

pub(crate) fn decode_optional_u8<'a>(bytes: &'a[u8], value: &mut Option<u8>) -> Mqtt5Result<&'a[u8], ()> {
    if bytes.len() < 1 {
        return Err(Mqtt5Error::ProtocolError);
    }

    *value = Some(bytes[0]);

    Ok(&bytes[1..])
}

pub(crate) fn decode_optional_u8_as_enum<'a, T>(bytes: &'a[u8], value: &mut Option<T>, converter: fn(u8) ->Mqtt5Result<T, ()>) -> Mqtt5Result<&'a[u8], ()> {
    if bytes.len() < 1 {
        return Err(Mqtt5Error::ProtocolError);
    }

    *value = Some(converter(bytes[0])?);

    Ok(&bytes[1..])
}

pub(crate) fn decode_u16<'a>(bytes: &'a[u8], value: &mut u16) -> Mqtt5Result<&'a[u8], ()> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::ProtocolError);
    }

    *value = u16::from_le_bytes(bytes[..2].try_into().unwrap());

    Ok(&bytes[2..])
}

pub(crate) fn decode_optional_u16<'a>(bytes: &'a[u8], value: &mut Option<u16>) -> Mqtt5Result<&'a[u8], ()> {
    if bytes.len() < 2 {
        return Err(Mqtt5Error::ProtocolError);
    }

    *value = Some(u16::from_le_bytes(bytes[..2].try_into().unwrap()));

    Ok(&bytes[2..])
}

pub(crate) fn decode_optional_u32<'a>(bytes: &'a[u8], value: &mut Option<u32>) -> Mqtt5Result<&'a[u8], ()> {
    if bytes.len() < 4 {
        return Err(Mqtt5Error::ProtocolError);
    }

    *value = Some(u32::from_le_bytes(bytes[..4].try_into().unwrap()));

    Ok(&bytes[4..])
}