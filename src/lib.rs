/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate core;

pub mod client;
mod decode;
mod encode;
pub mod spec;

pub use spec::auth::AuthPacket;
pub use spec::connack::ConnackPacket;
pub use spec::connect::ConnectPacket;
pub use spec::disconnect::DisconnectPacket;
pub use spec::pingreq::PingreqPacket;
pub use spec::pingresp::PingrespPacket;
pub use spec::puback::PubackPacket;
pub use spec::pubcomp::PubcompPacket;
pub use spec::publish::PublishPacket;
pub use spec::pubrec::PubrecPacket;
pub use spec::pubrel::PubrelPacket;
pub use spec::suback::SubackPacket;
pub use spec::subscribe::SubscribePacket;
pub use spec::unsuback::UnsubackPacket;
pub use spec::unsubscribe::UnsubscribePacket;

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
