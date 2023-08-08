/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::collections::VecDeque;

use crate::encoding_utils::*;
use crate::spec::*;
use crate::{Mqtt5Error, Mqtt5Result};

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

fn write_encoding_steps(mqtt_packet: &MqttPacket, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<(), ()> {
    match mqtt_packet {
        MqttPacket::Connect(packet) => {
            write_connect_encoding_steps(packet, steps)
        }
        MqttPacket::Connack(packet) => {
            write_connack_encoding_steps(packet, steps)
        }
        MqttPacket::Publish(packet) => {
            write_publish_encoding_steps(packet, steps)
        }
        MqttPacket::Puback(packet) => {
            write_puback_encoding_steps(packet, steps)
        }
        MqttPacket::Pubrec(packet) => {
            write_pubrec_encoding_steps(packet, steps)
        }
        MqttPacket::Pubrel(packet) => {
            write_pubrel_encoding_steps(packet, steps)
        }
        MqttPacket::Pubcomp(packet) => {
            write_pubcomp_encoding_steps(packet, steps)
        }
        MqttPacket::Subscribe(packet) => {
            write_subscribe_encoding_steps(packet, steps)
        }
        MqttPacket::Suback(packet) => {
            write_suback_encoding_steps(packet, steps)
        }
        MqttPacket::Unsubscribe(packet) => {
            write_unsubscribe_encoding_steps(packet, steps)
        }
        MqttPacket::Unsuback(packet) => {
            write_unsuback_encoding_steps(packet, steps)
        }
        MqttPacket::Pingreq(packet) => {
            write_pingreq_encoding_steps(packet, steps)
        }
        MqttPacket::Pingresp(packet) => {
            write_pingresp_encoding_steps(packet, steps)
        }
        MqttPacket::Disconnect(packet) => {
            write_disconnect_encoding_steps(packet, steps)
        }
        MqttPacket::Auth(packet) => {
            write_auth_encoding_steps(packet, steps)
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum EncodeResult {
    Complete,
    Full,
}

pub(crate) struct Encoder {
    steps: VecDeque<EncodingStep>,
}

impl Encoder {
    pub fn new() -> Encoder {
        Encoder {
            steps: VecDeque::new(),
        }
    }

    pub fn reset(&mut self, packet: &MqttPacket) -> Mqtt5Result<(), ()> {
        self.steps.clear();

        write_encoding_steps(packet, &mut self.steps)
    }

    pub fn encode(
        &mut self,
        packet: &MqttPacket,
        dest: &mut Vec<u8>,
    ) -> Mqtt5Result<EncodeResult, ()> {
        let capacity = dest.capacity();
        if capacity < 4 {
            return Err(Mqtt5Error::EncodeBufferTooSmall);
        }

        while !self.steps.is_empty() && dest.len() + 4 <= dest.capacity() {
            let step = self.steps.pop_front().unwrap();
            process_encoding_step(&mut self.steps, step, packet, dest)?;
        }

        if capacity != dest.capacity() {
            panic!("Internal error: encoding logic resized dest buffer");
        }

        if self.steps.is_empty() {
            Ok(EncodeResult::Complete)
        } else {
            Ok(EncodeResult::Full)
        }
    }
}
