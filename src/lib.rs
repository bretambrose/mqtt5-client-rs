/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

mod encoder;
mod encoding_utils;
mod spec_impl;
pub mod spec;
pub mod client;
mod client_impl;

#[derive(Debug)]
pub enum Mqtt5Error<T> {
    Unknown,
    Unimplemented(T),
    OperationChannelReceiveError,
    OperationChannelSendError(T),
    VariableLengthIntegerMaximumExceeded,
    EncodeBufferTooSmall,
}

pub type Mqtt5Result<T, E> = Result<T, Mqtt5Error<E>>;

