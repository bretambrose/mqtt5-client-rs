/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate core;

mod alias;
pub mod client;
mod decode;
mod encode;
pub mod spec;
mod validate;

/* Re-export all spec types at the root level */

pub use spec::QualityOfService;
pub use spec::PayloadFormatIndicator;
pub use spec::RetainHandlingType;
pub use spec::ConnectReasonCode;
pub use spec::PubackReasonCode;
pub use spec::PubrecReasonCode;
pub use spec::PubrelReasonCode;
pub use spec::PubcompReasonCode;
pub use spec::DisconnectReasonCode;
pub use spec::SubackReasonCode;
pub use spec::UnsubackReasonCode;
pub use spec::AuthenticateReasonCode;

pub use spec::UserProperty;
pub use spec::Subscription;

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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Mqtt5Error {
    Unknown,
    Unimplemented,
    OperationChannelReceiveError,
    OperationChannelSendError,
    VariableLengthIntegerMaximumExceeded,
    EncodeBufferTooSmall,
    DecoderInvalidVli,
    MalformedPacket,
    ProtocolError,
    InboundTopicAliasNotAllowed,
    InboundTopicAliasNotValid,
    OutboundTopicAliasNotAllowed,
    OutboundTopicAliasInvalid,
    UserPropertyValidation,
    AuthPacketValidation,
    ConnackPacketValidation,
}

pub type Mqtt5Result<T> = Result<T, Mqtt5Error>;
