/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::{Mqtt5Error, Mqtt5Result};

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum DecodeVliResult {
    InsufficientData,
    Value(u32, u8), /* (decoded value, bytes read) */
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
            return Ok(DecodeVliResult::Value(value, i as u8 + 1));
        }
    }

    Err(Mqtt5Error::DecoderInvalidVli)
}
