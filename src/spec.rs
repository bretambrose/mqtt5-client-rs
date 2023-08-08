/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod auth;
pub(crate) mod connack;
pub(crate) mod connect;
pub(crate) mod disconnect;
pub(crate) mod pingreq;
pub(crate) mod pingresp;
pub(crate) mod puback;
pub(crate) mod pubcomp;
pub(crate) mod publish;
pub(crate) mod pubrec;
pub(crate) mod pubrel;
pub(crate) mod suback;
pub(crate) mod subscribe;
pub(crate) mod unsuback;
pub(crate) mod unsubscribe;

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

#[derive(Default, Debug, Copy, Clone, Eq, PartialEq)]
pub enum QualityOfService {
    #[default]
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum PayloadFormatIndicator {
    #[default]
    Bytes = 0,
    Utf8 = 1,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum RetainHandlingType {
    #[default]
    SendOnSubscribe = 0,
    SendOnSubscribeIfNew = 1,
    DontSend = 2,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectReasonCode {
    #[default]
    Success = 0,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    UnsupportedProtocolVersion = 132,
    ClientIdentifierNotValid = 133,
    BadUsernameOrPassword = 134,
    NotAuthorized = 135,
    ServerUnavailable = 136,
    ServerBusy = 137,
    Banned = 138,
    BadAuthenticationMethod = 140,
    TopicNameInvalid = 144,
    PacketTooLarge = 149,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QosNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    ConnectionRateExceeeded = 159,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum PubackReasonCode {
    #[default]
    Success = 0,
    NoMatchingSubscribers = 16,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum PubrecReasonCode {
    #[default]
    Success = 0,
    NoMatchingSubscribers = 16,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    PayloadFormatInvalid = 153,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum PubrelReasonCode {
    #[default]
    Success = 0,
    PacketIdentifierNotFound = 146,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum PubcompReasonCode {
    #[default]
    Success = 0,
    PacketIdentifierNotFound = 146,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum DisconnectReasonCode {
    #[default]
    NormalDisconnection = 0,
    DisconnectWithWillMessage = 4,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    ServerBusy = 137,
    ServerShuttingDown = 139,
    KeepAliveTimeout = 141,
    SessionTakenOver = 142,
    TopicFilterInvalid = 143,
    TopicNameInvalid = 144,
    ReceiveMaximumExceeded = 147,
    TopicAliasInvalid = 148,
    PacketTooLarge = 149,
    MessageRateTooHigh = 150,
    QuotaExceeded = 151,
    AdministrativeAction = 152,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QosNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    SharedSubscriptionsNotSupported = 158,
    ConnectionRateExceeded = 159,
    MaximumConnectTime = 160,
    SubscriptionIdentifiersNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum SubackReasonCode {
    #[default]
    GrantedQos0 = 0,
    GrantedQos1 = 1,
    GrantedQos2 = 2,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicFilterInvalid = 143,
    PacketIdentifierInUse = 145,
    QuotaExceeded = 151,
    SharedSubscriptionsNotSupported = 158,
    SubscriptionIdentifiersNotSupported = 161,
    WildcaredSubscriptionsNotSupported = 162,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum UnsubackReasonCode {
    #[default]
    Success = 0,
    NoSubscriptionExisted = 17,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuthenticateReasonCode {
    #[default]
    Success = 0,
    ContinueAuthentication = 24,
    ReAuthenticate = 25,
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct UserProperty {
    pub name: String,
    pub value: String,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct Subscription {
    pub topic_filter: String,
    pub qos: QualityOfService,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling_type: RetainHandlingType,
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) enum MqttPacket {
    Connect(ConnectPacket),
    Connack(ConnackPacket),
    Publish(PublishPacket),
    Puback(PubackPacket),
    Pubrec(PubrecPacket),
    Pubrel(PubrelPacket),
    Pubcomp(PubcompPacket),
    Subscribe(SubscribePacket),
    Suback(SubackPacket),
    Unsubscribe(UnsubscribePacket),
    Unsuback(UnsubackPacket),
    Pingreq(PingreqPacket),
    Pingresp(PingrespPacket),
    Disconnect(DisconnectPacket),
    Auth(AuthPacket),
}
