/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate core;

pub mod client;
mod client_impl;
mod decoder;
mod decoding_utils;
mod encoder;
mod encoding_utils;
pub mod spec;
mod spec_impl;

#[derive(Debug, Eq, PartialEq)]
pub enum Mqtt5Error<T> {
    Unknown,
    Unimplemented(T),
    OperationChannelReceiveError,
    OperationChannelSendError(T),
    VariableLengthIntegerMaximumExceeded,
    EncodeBufferTooSmall,
    DecoderInvalidVli,
    MalformedPacket,
    ProtocolError,
}

pub type Mqtt5Result<T, E> = Result<T, Mqtt5Error<E>>;
