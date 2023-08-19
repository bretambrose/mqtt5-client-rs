/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod utils;

use crate::*;
use crate::alias::*;
use crate::decode::utils::*;
use crate::encode::*;
use crate::spec::*;
use crate::spec::utils::*;

use crate::spec::auth::*;
use crate::spec::connack::*;
use crate::spec::connect::*;
use crate::spec::disconnect::*;
use crate::spec::pingreq::*;
use crate::spec::pingresp::*;
use crate::spec::puback::*;
use crate::spec::pubcomp::*;
use crate::spec::publish::*;
use crate::spec::pubrec::*;
use crate::spec::pubrel::*;
use crate::spec::suback::*;
use crate::spec::subscribe::*;
use crate::spec::unsuback::*;
use crate::spec::unsubscribe::*;

const DECODE_BUFFER_DEFAULT_SIZE : usize = 16 * 1024;

#[derive(Copy, Clone, Eq, PartialEq)]
enum DecoderState {
    ReadPacketType,
    ReadTotalRemainingLength,
    ReadPacketBody,
    TerminalError
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum DecoderDirective {
    OutOfData,
    Continue,
    TerminalError
}

pub struct DecoderOptions {
    packet_stream: std::sync::mpsc::Sender<MqttPacket>
}

pub struct Decoder {
    config: DecoderOptions,

    state: DecoderState,

    scratch: Vec<u8>,

    first_byte: Option<u8>,

    remaining_length : Option<usize>,
}

macro_rules! decode_packet_by_type {
    ($decode_function: ident, $packet_type: ident, $first_byte: ident, $packet_body: ident) => {
        match $decode_function($first_byte, $packet_body) {
            Ok(packet) => { return Ok(MqttPacket::$packet_type(packet)); }
            Err(err) => { return Err(err); }
        }
    };
}

fn decode_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<MqttPacket> {
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
            return Err(Mqtt5Error::MalformedPacket);
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

        return (DecoderDirective::Continue, &bytes[1..]);
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
            return (DecoderDirective::TerminalError, remaining_bytes);
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
            if self.config.packet_stream.send(packet).is_err() {
                return (DecoderDirective::TerminalError, &[]);
            }

            self.reset_for_new_packet();
            return (DecoderDirective::Continue, &bytes[bytes_needed..]);
        }

        return (DecoderDirective::TerminalError, &[]);
    }

    pub fn decode_bytes(&mut self, bytes: &[u8]) -> Mqtt5Result<()> {
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
                    decode_result = DecoderDirective::TerminalError;
                }
            }
        }

        if decode_result == DecoderDirective::TerminalError {
            self.state = DecoderState::TerminalError;
            return Err(Mqtt5Error::MalformedPacket);
        }

        Ok(())
    }

    fn reset_for_new_packet(&mut self) {
        if self.state != DecoderState::TerminalError {
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
pub(crate) mod testing {
    use std::sync::mpsc::TryRecvError;
    use super::*;

    pub(crate) fn do_single_encode_decode_test(packet : &MqttPacket, encode_size : usize, decode_size : usize, encode_repetitions : u32) -> bool {

        let mut encoder = Encoder::new();

        let mut full_encoded_stream = Vec::with_capacity( 128 * 1024);
        let mut encode_buffer = Vec::with_capacity(encode_size);

        let mut outbound_resolver : Box<dyn OutboundAliasResolver> = Box::new(ManualOutboundAliasResolver::new(65535));
        let mut encoding_context = EncodingContext {
            outbound_alias_resolver : &mut outbound_resolver
        };

        /* encode 5 copies of the packet */
        for _ in 0..encode_repetitions {
            assert!(!encoder.reset(&packet, &mut encoding_context).is_err());

            let mut cumulative_result : EncodeResult = EncodeResult::Full;
            while cumulative_result == EncodeResult::Full {
                encode_buffer.clear();
                let encode_result = encoder.encode(packet, &mut encode_buffer);
                if let Err(_) = encode_result {
                    break;
                }

                cumulative_result = encode_result.unwrap();
                full_encoded_stream.extend_from_slice(encode_buffer.as_slice());
            }

            assert_eq!(cumulative_result, EncodeResult::Complete);
        }

        let (packet_sender, packet_receiver) = std::sync::mpsc::channel();

        let options = DecoderOptions {
            packet_stream : packet_sender
        };

        let mut decoder = Decoder::new(options);
        decoder.reset_for_new_connection();

        let mut decode_stream_slice = full_encoded_stream.as_slice();
        while decode_stream_slice.len() > 0 {
            let fragment_size : usize = usize::min(decode_size, decode_stream_slice.len());
            let decode_slice = &decode_stream_slice[..fragment_size];
            decode_stream_slice = &decode_stream_slice[fragment_size..];

            let decode_result = decoder.decode_bytes(decode_slice);
            assert!(!decode_result.is_err());
        }

        let mut matching_packets : u32 = 0;

        let mut inbound_alias_resolver = InboundAliasResolver::new(65535);

        loop {
            let receive_result = packet_receiver.try_recv();
            if let Err(error) = receive_result {
                assert_eq!(TryRecvError::Empty, error);
                break;
            }

            let mut received_packet = receive_result.unwrap();
            matching_packets += 1;

            if let MqttPacket::Publish(publish) = &mut received_packet {
                if let Err(_) = inbound_alias_resolver.resolve_topic_alias(&publish.topic_alias, &mut publish.topic) {
                    return false;
                }
            }

            assert_eq!(*packet, received_packet);
        }

        assert_eq!(encode_repetitions, matching_packets);

        return true;
    }

    pub(crate) fn do_round_trip_encode_decode_test(packet : &MqttPacket) -> bool {
        let encode_buffer_sizes : Vec<usize> = vec!(4, 5, 7, 11, 17, 31, 47, 71, 131);
        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7, 11, 17, 31, 47, 71, 131, 1023);

        for encode_size in encode_buffer_sizes.iter() {
            for decode_size in decode_fragment_sizes.iter() {
                assert!(do_single_encode_decode_test(&packet, *encode_size, *decode_size, 5));
            }
        }

        return true;
    }
}