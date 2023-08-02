/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

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

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct ConnectPacket {
    pub keep_alive_interval_seconds: u16,
    pub clean_start: bool,

    pub client_id: Option<String>,

    pub username: Option<String>,
    pub password: Option<Vec<u8>>,

    pub session_expiry_interval_seconds: Option<u32>,

    pub request_response_information: Option<bool>,
    pub request_problem_information: Option<bool>,
    pub receive_maximum: Option<u16>,
    pub topic_alias_maximum: Option<u16>,
    pub maximum_packet_size_bytes: Option<u32>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,

    pub will_delay_interval_seconds: Option<u32>,
    pub will: Option<PublishPacket>,

    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct ConnackPacket {
    pub session_present: bool,
    pub reason_code: ConnectReasonCode,

    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_qos: Option<QualityOfService>,
    pub retain_available: Option<bool>,
    pub maximum_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub topic_alias_maximum: Option<u16>,
    pub reason_string: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,

    pub wildcard_subscriptions_available: Option<bool>,
    pub subscription_identifiers_available: Option<bool>,
    pub shared_subscriptions_available: Option<bool>,

    pub server_keep_alive: Option<u16>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct PublishPacket {
    pub packet_id: u16,

    pub topic: String,

    pub qos: QualityOfService,
    pub duplicate: bool,
    pub retain: bool,

    pub payload: Option<Vec<u8>>,

    pub payload_format: Option<PayloadFormatIndicator>,
    pub message_expiry_interval_seconds: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,

    pub subscription_identifiers: Option<Vec<u32>>,

    pub content_type: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct PubackPacket {
    pub packet_id: u16,

    pub reason_code: PubackReasonCode,
    pub reason_string: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct PubrecPacket {
    pub packet_id: u16,

    pub reason_code: PubrecReasonCode,
    pub reason_string: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct PubrelPacket {
    pub packet_id: u16,

    pub reason_code: PubrelReasonCode,
    pub reason_string: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct PubcompPacket {
    pub packet_id: u16,

    pub reason_code: PubcompReasonCode,
    pub reason_string: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct SubscribePacket {
    pub packet_id: u16,

    pub subscriptions: Vec<Subscription>,

    pub subscription_identifier: Option<u32>,

    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct SubackPacket {
    pub packet_id: u16,

    pub reason_string: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,

    pub reason_codes: Vec<SubackReasonCode>,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct UnsubscribePacket {
    pub packet_id: u16,

    pub topic_filters: Vec<String>,

    pub user_properties: Option<Vec<UserProperty>>,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct UnsubackPacket {
    pub packet_id: u16,

    pub reason_string: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,

    pub reason_codes: Vec<UnsubackReasonCode>,
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct PingreqPacket {}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct PingrespPacket {}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct DisconnectPacket {
    pub reason_code: DisconnectReasonCode,

    pub session_expiry_interval_seconds: Option<u32>,
    pub reason_string: Option<String>,
    pub user_properties: Option<Vec<UserProperty>>,
    pub server_reference: Option<String>,
}

#[derive(Default, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct AuthPacket {
    pub reason_code: AuthenticateReasonCode,

    pub method: Option<String>,
    pub data: Option<Vec<u8>>,
    pub reason_string: Option<String>,

    pub user_properties: Option<Vec<UserProperty>>,
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
