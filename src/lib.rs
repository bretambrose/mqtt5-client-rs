/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

mod alias;
pub mod client;
mod decode;
mod encode;
mod logging;
mod operation;
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

pub use client::*;

use std::fmt;

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
    ConnectPacketValidation,
    DisconnectPacketValidation,
    PubackPacketValidation,
    PubcompPacketValidation,
    PubrecPacketValidation,
    PubrelPacketValidation,
    PublishPacketValidation,
    SubackPacketValidation,
    UnsubackPacketValidation,
    SubscribePacketValidation,
    UnsubscribePacketValidation,
    InternalStateError,
    ConnectionRejected,
    ConnackTimeout,
    PingTimeout,
    ConnectionClosed,
    OfflineQueuePolicyFailed,
    ServerSideDisconnect,
    AckTimeout,
    PacketIdSpaceExhausted,
    OperationalStateReset
}

impl fmt::Display for Mqtt5Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Mqtt5Error::Unknown => { write!(f, "Unknown") }
            Mqtt5Error::Unimplemented => { write!(f, "Unimplemented") }
            Mqtt5Error::OperationChannelReceiveError => { write!(f, "OperationChannelReceiveError") }
            Mqtt5Error::OperationChannelSendError => { write!(f, "OperationChannelSendError") }
            Mqtt5Error::VariableLengthIntegerMaximumExceeded => { write!(f, "VariableLengthIntegerMaximumExceeded") }
            Mqtt5Error::EncodeBufferTooSmall => { write!(f, "EncodeBufferTooSmall") }
            Mqtt5Error::DecoderInvalidVli => { write!(f, "DecoderInvalidVli") }
            Mqtt5Error::MalformedPacket => { write!(f, "MalformedPacket") }
            Mqtt5Error::ProtocolError => { write!(f, "ProtocolError") }
            Mqtt5Error::InboundTopicAliasNotAllowed => { write!(f, "InboundTopicAliasNotAllowed") }
            Mqtt5Error::InboundTopicAliasNotValid => { write!(f, "InboundTopicAliasNotValid") }
            Mqtt5Error::OutboundTopicAliasNotAllowed => { write!(f, "OutboundTopicAliasNotAllowed") }
            Mqtt5Error::OutboundTopicAliasInvalid => { write!(f, "OutboundTopicAliasInvalid") }
            Mqtt5Error::UserPropertyValidation => { write!(f, "UserPropertyValidation") }
            Mqtt5Error::AuthPacketValidation => { write!(f, "AuthPacketValidation") }
            Mqtt5Error::ConnackPacketValidation => { write!(f, "ConnackPacketValidation") }
            Mqtt5Error::ConnectPacketValidation => { write!(f, "ConnectPacketValidation") }
            Mqtt5Error::DisconnectPacketValidation => { write!(f, "DisconnectPacketValidation") }
            Mqtt5Error::PubackPacketValidation => { write!(f, "PubackPacketValidation") }
            Mqtt5Error::PubcompPacketValidation => { write!(f, "PubcompPacketValidation") }
            Mqtt5Error::PubrecPacketValidation => { write!(f, "PubrecPacketValidation") }
            Mqtt5Error::PubrelPacketValidation => { write!(f, "PubrelPacketValidation") }
            Mqtt5Error::PublishPacketValidation => { write!(f, "PublishPacketValidation") }
            Mqtt5Error::SubackPacketValidation => { write!(f, "SubackPacketValidation") }
            Mqtt5Error::UnsubackPacketValidation => { write!(f, "UnsubackPacketValidation") }
            Mqtt5Error::SubscribePacketValidation => { write!(f, "SubscribePacketValidation") }
            Mqtt5Error::UnsubscribePacketValidation => { write!(f, "UnsubscribePacketValidation") }
            Mqtt5Error::InternalStateError => { write!(f, "InternalStateError") }
            Mqtt5Error::ConnectionRejected => { write!(f, "ConnectionRejected") }
            Mqtt5Error::ConnackTimeout => { write!(f, "ConnackTimeout") }
            Mqtt5Error::PingTimeout => { write!(f, "PingTimeout") }
            Mqtt5Error::ConnectionClosed => { write!(f, "ConnectionClosed") }
            Mqtt5Error::OfflineQueuePolicyFailed => { write!(f, "OfflineQueuePolicyFailed") }
            Mqtt5Error::ServerSideDisconnect => { write!(f, "ServerSideDisconnect") }
            Mqtt5Error::AckTimeout => { write!(f, "AckTimeout") }
            Mqtt5Error::PacketIdSpaceExhausted => { write!(f, "PacketIdSpaceExhausted") }
            Mqtt5Error::OperationalStateReset => { write!(f, "OperationalStateReset") }
        }
    }
}

pub type Mqtt5Result<T> = Result<T, Mqtt5Error>;

fn fold_mqtt5_result<T>(base: Mqtt5Result<T>, new_result: Mqtt5Result<T>) -> Mqtt5Result<T> {
    if new_result.is_err() {
        return new_result;
    }

    base
}
