/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::{Mqtt5Error, Mqtt5Result};
use crate::decoding_utils::*;
use crate::spec::*;
use crate::spec_impl::*;

const DECODE_BUFFER_DEFAULT_SIZE : usize = 16 * 1024;

#[derive(Copy, Clone, Eq, PartialEq)]
enum DecoderState {
    ReadPacketType,
    ReadTotalRemainingLength,
    ReadPacketBody,
    ProtocolError
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum DecoderDirective {
    OutOfData,
    Continue,
    ProtocolError
}

pub struct DecoderOptions {
    on_packet_decoded: fn(&MqttPacket) -> Mqtt5Result<(), ()>
}

pub struct Decoder {
    config: DecoderOptions,

    state: DecoderState,

    scratch: Vec<u8>,

    first_byte: Option<u8>,

    remaining_length : Option<usize>,
}

fn decode_connect_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<ConnectPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_connack_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<ConnackPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_publish_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PublishPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_puback_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PubackPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_pubrec_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PubrecPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_pubrel_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PubrelPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_pubcomp_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PubcompPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_subscribe_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<SubscribePacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_suback_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<SubackPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_unsubscribe_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<UnsubscribePacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_unsuback_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<UnsubackPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_pingreq_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PingreqPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_pingresp_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PingrespPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_disconnect_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<DisconnectPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

fn decode_auth_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<AuthPacket, ()> {
    Err(Mqtt5Error::Unimplemented(()))
}

macro_rules! decode_packet_by_type {
    ($decode_function: ident, $packet_type: ident, $first_byte: ident, $packet_body: ident) => {
        match $decode_function($first_byte, $packet_body) {
            Ok(packet) => { return Ok(MqttPacket::$packet_type(packet)); }
            Err(err) => { return Err(err); }
        }
    };
}

fn decode_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<MqttPacket, ()> {
    let packet_type = first_byte >> 4;

    match packet_type {
        PACKET_TYPE_CONNECT => { decode_packet_by_type!(decode_connect_packet, Connect, first_byte, packet_body) }
        PACKET_TYPE_CONNACK => { decode_packet_by_type!(decode_connack_packet, Connack, first_byte, packet_body) }
        PACKET_TYPE_PUBLISH => { decode_packet_by_type!(decode_publish_packet, Publish, first_byte, packet_body) }
        PACKET_TYPE_PUBACK => { decode_packet_by_type!(decode_puback_packet, Puback, first_byte, packet_body) }
        PACKET_TYPE_PUBREC => { decode_packet_by_type!(decode_pubrec_packet, Pubrec, first_byte, packet_body) }
        PACKET_TYPE_PUBREL => { decode_packet_by_type!(decode_pubrel_packet, Pubrel, first_byte, packet_body) }
        PACKET_TYPE_PUBCOMP => { decode_packet_by_type!(decode_pubcomp_packet, Pubcomp, first_byte, packet_body) }
        PACKET_TYPE_SUBSCRIBE => { decode_packet_by_type!(decode_subscribe_packet, Subscribe, first_byte, packet_body) }
        PACKET_TYPE_SUBACK => { decode_packet_by_type!(decode_suback_packet, Suback, first_byte, packet_body) }
        PACKET_TYPE_UNSUBSCRIBE => { decode_packet_by_type!(decode_unsubscribe_packet, Unsubscribe, first_byte, packet_body) }
        PACKET_TYPE_UNSUBACK => { decode_packet_by_type!(decode_unsuback_packet, Unsuback, first_byte, packet_body) }
        PACKET_TYPE_PINGREQ => { decode_packet_by_type!(decode_pingreq_packet, Pingreq, first_byte, packet_body) }
        PACKET_TYPE_PINGRESP => { decode_packet_by_type!(decode_pingresp_packet, Pingresp, first_byte, packet_body) }
        PACKET_TYPE_DISCONNECT => { decode_packet_by_type!(decode_disconnect_packet, Disconnect, first_byte, packet_body) }
        PACKET_TYPE_AUTH => { decode_packet_by_type!(decode_auth_packet, Auth, first_byte, packet_body) }
        _ => {
            return Err(Mqtt5Error::Unimplemented(()));
        }
    }
}

impl Decoder {
    pub fn new(options: DecoderOptions) -> Decoder {
        Decoder {
            config: options,
            state: DecoderState::ReadPacketType,
            scratch : Vec::<u8>::with_capacity(DECODE_BUFFER_DEFAULT_SIZE),
            first_byte : None,
            remaining_length : None,
        }
    }

    pub fn reset_for_new_connection(&mut self) {
        self.reset();
    }

    fn process_read_packet_type<'a>(&mut self, bytes: &'a [u8]) -> (DecoderDirective, &'a[u8]) {
        if bytes.len() == 0 {
            return (DecoderDirective::OutOfData, bytes);
        }

        self.first_byte = Some(bytes[0]);
        self.state = DecoderState::ReadTotalRemainingLength;

        return  (DecoderDirective::Continue, &bytes[1..]);
    }

    fn process_read_total_remaining_length<'a>(&mut self, bytes: &'a[u8]) -> (DecoderDirective, &'a[u8]) {
        if bytes.len() == 0 {
            return (DecoderDirective::OutOfData, bytes);
        }

        self.scratch.push(bytes[0]);
        let remaining_bytes = &bytes[1..];

        let decode_vli_result = decode_vli(&self.scratch);
        if let Ok(DecodeVliResult::Value(remaining_length, _)) = decode_vli_result {
            self.remaining_length = Some(remaining_length as usize);
            self.state = DecoderState::ReadPacketBody;
            self.scratch.clear();
            return (DecoderDirective::Continue, remaining_bytes);
        } else if self.scratch.len() >= 4 {
            return (DecoderDirective::ProtocolError, remaining_bytes);
        } else if remaining_bytes.len() > 0 {
            return (DecoderDirective::Continue, remaining_bytes);
        } else {
            return (DecoderDirective::OutOfData, remaining_bytes);
        }
    }

    fn process_read_packet_body<'a>(&mut self, bytes: &'a[u8]) -> (DecoderDirective, &'a[u8]) {
        let read_so_far = self.scratch.len();
        let bytes_needed = self.remaining_length.unwrap() - read_so_far;
        if bytes_needed > bytes.len() {
            self.scratch.extend_from_slice(bytes);
            return (DecoderDirective::OutOfData, &[]);
        }

        let packet_slice : &[u8];
        if self.scratch.len() > 0 {
            self.scratch.extend_from_slice(&bytes[..bytes_needed]);
            packet_slice = &self.scratch;
        } else {
            packet_slice = &bytes[..bytes_needed];
        }

        if let Ok(packet) = decode_packet(self.first_byte.unwrap(), packet_slice) {
            if let Ok(_) = (self.config.on_packet_decoded)(&packet) {
                self.reset_for_new_packet();
                return (DecoderDirective::Continue, &bytes[bytes_needed..]);
            }
        }

        return (DecoderDirective::ProtocolError, &[]);
    }

    pub fn decode_bytes(&mut self, bytes: &[u8]) -> Mqtt5Result<(), ()> {
        let mut current_slice = bytes;

        let mut decode_result = DecoderDirective::Continue;
        while decode_result == DecoderDirective::Continue {
            match self.state {
                DecoderState::ReadPacketType => {
                    (decode_result, current_slice) = self.process_read_packet_type(current_slice);
                }

                DecoderState::ReadTotalRemainingLength => {
                    (decode_result, current_slice) = self.process_read_total_remaining_length(current_slice);
                }

                DecoderState::ReadPacketBody => {
                    (decode_result, current_slice) = self.process_read_packet_body(current_slice);
                }

                _ => {
                    decode_result = DecoderDirective::ProtocolError;
                }
            }
        }

        if decode_result == DecoderDirective::ProtocolError {
            self.state = DecoderState::ProtocolError;
            return Err(Mqtt5Error::ProtocolError);
        }

        Ok(())
    }

    fn reset_for_new_packet(&mut self) {
        if self.state != DecoderState::ProtocolError {
            self.reset();
        }
    }

    fn reset(&mut self) {
        self.state = DecoderState::ReadPacketType;
        self.scratch.clear();
        self.first_byte = None;
        self.remaining_length = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoding_utils::*;

    fn on_packet_decoded_test(packet : &MqttPacket) -> Mqtt5Result<(), ()> {
        Err(Mqtt5Error::Unimplemented(()))
    }

    #[test]
    fn create_decoder() {
        let options = DecoderOptions {
            on_packet_decoded : on_packet_decoded_test
        };

        let mut decoder = Decoder::new(options);
        decoder.reset_for_new_connection();

    }
}