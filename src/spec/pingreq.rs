/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::encode::utils::*;
use crate::spec::*;
use crate::spec::utils::*;

use std::collections::VecDeque;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct PingreqPacket {}

#[rustfmt::skip]
pub(crate) fn write_pingreq_encoding_steps(_: &PingreqPacket, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
    encode_integral_expression!(steps, Uint8, PACKET_TYPE_PINGREQ << 4);
    encode_integral_expression!(steps, Uint8, 0);

    Ok(())
}

const PINGREQ_FIRST_BYTE : u8 = PACKET_TYPE_PINGREQ << 4;

pub(crate) fn decode_pingreq_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PingreqPacket, ()> {
    if packet_body.len() != 0 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if first_byte != PINGREQ_FIRST_BYTE {
        return Err(Mqtt5Error::MalformedPacket);
    }

    return Ok(PingreqPacket{});
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pingreq_round_trip_encode_decode() {
        let packet = PingreqPacket {};
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingreq(packet)));
    }
}