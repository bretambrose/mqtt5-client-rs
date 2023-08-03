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
    packet_stream: std::sync::mpsc::Sender<MqttPacket>
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

fn decode_publish_properties(property_bytes: &[u8], packet : &mut PublishPacket) -> Mqtt5Result<(), ()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR => { mutable_property_bytes = decode_optional_u8_as_enum(mutable_property_bytes, &mut packet.payload_format, convert_u8_to_payload_format_indicator)?; }
            PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.message_expiry_interval_seconds)?; }
            PROPERTY_KEY_TOPIC_ALIAS => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.topic_alias)?; }
            PROPERTY_KEY_RESPONSE_TOPIC => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.response_topic)?; }
            PROPERTY_KEY_CORRELATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut packet.correlation_data)?; }
            PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER => {
                let mut subscription_id : usize = 0;
                mutable_property_bytes = decode_vli_into_mutable(mutable_property_bytes, &mut subscription_id)?;
                if packet.subscription_identifiers.is_none() {
                    packet.subscription_identifiers = Some(Vec::new());
                }

                let ids : &mut Vec<u32> = &mut packet.subscription_identifiers.as_mut().unwrap();
                ids.push(subscription_id as u32);
            }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            PROPERTY_KEY_CONTENT_TYPE => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.content_type)?; }
            _ => { return Err(Mqtt5Error::ProtocolError); }
        }
    }

    Ok(())
}

fn decode_publish_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PublishPacket, ()> {
    let mut packet = PublishPacket { ..Default::default() };

    if (first_byte & PUBLISH_PACKET_FIXED_HEADER_DUPLICATE_FLAG) != 0 {
        packet.duplicate = true;
    }

    if (first_byte & PUBLISH_PACKET_FIXED_HEADER_RETAIN_FLAG) != 0 {
        packet.retain = true;
    }

    packet.qos = convert_u8_to_quality_of_service((first_byte >> 1) & PUBLISH_PACKET_FIXED_HEADER_QOS_MASK)?;

    let mut mutable_body = packet_body;
    let mut properties_length : usize = 0;

    mutable_body = decode_length_prefixed_string(mutable_body, &mut packet.topic)?;

    if packet.qos != QualityOfService::AtMostOnce {
        mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;
    }

    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;

    let properties_bytes = &mutable_body[..properties_length];
    let payload_bytes = &mutable_body[properties_length..];

    decode_publish_properties(properties_bytes, &mut packet)?;

    if payload_bytes.len() > 0 {
        packet.payload = Some(payload_bytes.to_vec());
    }

    Ok(packet)
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

const PINGREQ_FIRST_BYTE : u8 = PACKET_TYPE_PINGREQ << 4;

fn decode_pingreq_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PingreqPacket, ()> {
    if packet_body.len() != 0 {
        return Err(Mqtt5Error::ProtocolError);
    }

    if first_byte != PINGREQ_FIRST_BYTE {
        return Err(Mqtt5Error::ProtocolError);
    }

    return Ok(PingreqPacket{});
}

const PINGRESP_FIRST_BYTE : u8 = PACKET_TYPE_PINGRESP << 4;

fn decode_pingresp_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<PingrespPacket, ()> {
    if packet_body.len() != 0 {
        return Err(Mqtt5Error::ProtocolError);
    }

    if first_byte != PINGRESP_FIRST_BYTE {
        return Err(Mqtt5Error::ProtocolError);
    }

    return Ok(PingrespPacket{});
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
            if self.config.packet_stream.send(packet).is_err() {
                return (DecoderDirective::ProtocolError, &[]);
            }

            self.reset_for_new_packet();
            return (DecoderDirective::Continue, &bytes[bytes_needed..]);
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
    use std::sync::mpsc::TryRecvError;
    use super::*;
    use crate::encoder::*;

    #[test]
    fn create_decoder() {
        let (packet_sender, _) = std::sync::mpsc::channel();

        let options = DecoderOptions {
            packet_stream : packet_sender
        };

        let mut decoder = Decoder::new(options);
        decoder.reset_for_new_connection();

    }

    fn do_single_encode_decode_test(packet : &MqttPacket, encode_size : usize, decode_size : usize, encode_repetitions : u32) -> bool {

        let mut encoder = Encoder::new();

        let mut full_encoded_stream = Vec::with_capacity( 128 * 1024);
        let mut encode_buffer = Vec::with_capacity(encode_size);

        /* encode 5 copies of the packet */
        for i in 0..encode_repetitions {
            assert!(!encoder.reset(&packet).is_err());

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

        loop {
            let receive_result = packet_receiver.try_recv();
            if let Err(error) = receive_result {
                assert_eq!(TryRecvError::Empty, error);
                break;
            }

            let received_packet = receive_result.unwrap();
            matching_packets += 1;

            assert_eq!(*packet, received_packet);
        }

        assert_eq!(encode_repetitions, matching_packets);

        return true;
    }

    fn do_round_trip_encode_decode_test(packet : &MqttPacket) -> bool {
        let encode_buffer_sizes : Vec<usize> = vec!(4, 5, 7, 11, 17, 31, 47, 71, 131);
        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7, 11, 17, 31, 47, 71, 131, 1023);

        for encode_size in encode_buffer_sizes.iter() {
            for decode_size in decode_fragment_sizes.iter() {
                assert!(do_single_encode_decode_test(&packet, *encode_size, *decode_size, 5));
            }
        }

        return true;
    }

    #[test]
    fn publish_round_trip_encode_decode_default() {
        let packet = PublishPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Publish(packet)));
    }

    #[test]
    fn publish_round_trip_encode_decode_basic() {

        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            payload: Some("a payload".as_bytes().to_vec()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Publish(packet)));
    }

    fn create_publish_with_all_fields() -> PublishPacket {
        return PublishPacket {
            packet_id: 47,
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            duplicate: true,
            retain: true,
            payload: Some("a payload".as_bytes().to_vec()),
            payload_format: Some(PayloadFormatIndicator::Utf8),
            message_expiry_interval_seconds : Some(3600),
            topic_alias: Some(10),
            response_topic: Some("Respond/to/me".to_string()),
            correlation_data: Some(vec!(1, 2, 3, 4, 5)),
            subscription_identifiers: Some(vec!(10, 20, 256, 32768)),
            content_type: Some("rest/json".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "name1".to_string(), value: "value1".to_string()},
                UserProperty{name: "name2".to_string(), value: "value2".to_string()},
                UserProperty{name: "name3".to_string(), value: "value3".to_string()},
            ))
        };
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields() {

        let packet = create_publish_with_all_fields();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Publish(packet)));
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields_2byte_payload() {
        let mut publish = create_publish_with_all_fields();
        publish.payload = Some(vec![0; 257]);

        let packet = &MqttPacket::Publish(publish);

        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7);

        for decode_size in decode_fragment_sizes.iter() {
            assert!(do_single_encode_decode_test(&packet, 1024, *decode_size, 5));
        }
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields_3byte_payload() {
        let mut publish = create_publish_with_all_fields();
        publish.payload = Some(vec![0; 32768]);

        let packet = &MqttPacket::Publish(publish);

        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7);

        for decode_size in decode_fragment_sizes.iter() {
            assert!(do_single_encode_decode_test(&packet, 1024, *decode_size, 5));
        }
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields_4byte_payload() {
        let mut publish = create_publish_with_all_fields();
        publish.payload = Some(vec![0; 128 * 128 * 128]);

        let packet = &MqttPacket::Publish(publish);

        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7);

        for decode_size in decode_fragment_sizes.iter() {
            assert!(do_single_encode_decode_test(&packet, 1024, *decode_size, 5));
        }
    }

    #[test]
    fn pingreq_round_trip_encode_decode() {
        let packet = PingreqPacket {};
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingreq(packet)));
    }

    #[test]
    fn pingresp_round_trip_encode_decode() {
        let packet = PingrespPacket {};
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingresp(packet)));
    }
}