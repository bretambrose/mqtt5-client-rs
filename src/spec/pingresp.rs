/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::encode::utils::*;
use crate::spec::*;
use crate::spec::utils::*;

use std::collections::VecDeque;

/// Data model of an [MQTT5 PINGRESP](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901200) packet.
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct PingrespPacket {}

#[rustfmt::skip]
pub(crate) fn write_pingresp_encoding_steps(_: &PingrespPacket, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<()> {
    encode_integral_expression!(steps, Uint8, PACKET_TYPE_PINGRESP << 4);
    encode_integral_expression!(steps, Uint8, 0);

    Ok(())
}

const PINGRESP_FIRST_BYTE : u8 = PACKET_TYPE_PINGRESP << 4;

pub(crate) fn decode_pingresp_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<PingrespPacket>> {
    if packet_body.len() != 0 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    if first_byte != PINGRESP_FIRST_BYTE {
        return Err(Mqtt5Error::MalformedPacket);
    }

    return Ok(Box::new(PingrespPacket{}));
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pingresp_round_trip_encode_decode() {
        let packet = Box::new(PingrespPacket {});
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingresp(packet)));
    }
}