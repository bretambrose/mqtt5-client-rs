/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::spec::*;
use crate::spec::utils::*;

use std::collections::VecDeque;

/// Data model of an [MQTT5 PINGREQ](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901195) packet.
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct PingreqPacket {}

#[rustfmt::skip]
pub(crate) fn write_pingreq_encoding_steps(_: &PingreqPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<()> {
    encode_integral_expression!(steps, Uint8, PACKET_TYPE_PINGREQ << 4);
    encode_integral_expression!(steps, Uint8, 0);

    Ok(())
}

const PINGREQ_FIRST_BYTE : u8 = PACKET_TYPE_PINGREQ << 4;

pub(crate) fn decode_pingreq_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<PingreqPacket>> {
    if packet_body.len() != 0 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if first_byte != PINGREQ_FIRST_BYTE {
        return Err(Mqtt5Error::MalformedPacket);
    }

    return Ok(Box::new(PingreqPacket{}));
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pingreq_round_trip_encode_decode() {
        let packet = Box::new(PingreqPacket {});
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingreq(packet)));
    }

    #[test]
    fn pingreq_decode_failure_bad_fixed_header() {
        let packet = Box::new(PingreqPacket {});

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pingreq(packet), 1);
    }

    #[test]
    fn pingreq_decode_failure_bad_length() {
        let packet = Box::new(PingreqPacket {});

        let extend_length = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for this packet, the reason code is in byte 2
            clone[1] = 2;
            clone.push(5);
            clone.push(6);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pingreq(packet), extend_length);
    }

}