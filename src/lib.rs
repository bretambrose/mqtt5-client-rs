use std::ops::Sub;

struct UserProperty {
    name : String,
    Value : String,
}

enum QualityOfService {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

enum PayloadFormatIndicator {
    Bytes = 0,
    Utf8 = 1,
}

enum RetainHandlingType {
    SendOnSubscribe = 0,
    SendOnSubscribeIfNew = 1,
    DontSend = 2,
}

enum ConnectReasonCode {
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

enum PubackReasonCode {
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

enum PubrecReasonCode {
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

enum PubrelReasonCode {
    Success = 0,
    PacketIdentifierNotFound = 146,
}

enum PubcompReasonCode {
    Success = 0,
    PacketIdentifierNotFound = 146,
}

enum DisconnectReasonCode {
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

enum SubackReasonCode {
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

enum UnsubackReasonCode {
    Success = 0,
    NoSubscriptionExisted = 17,
    UnspecifiedError = 128,
    ImplementationSpecificError = 131,
    NotAuthorized = 135,
    TopicNameInvalid = 144,
    PacketIdentifierInUse = 145,
}

enum AuthenticateReasonCode {
    Success = 0,
    ContinueAuthentication = 24,
    ReAuthenticate = 25,
}

struct Subscription {
    topic_filter : String,
    qos : QualityOfService,
    no_local : bool,
    retain_as_published : bool,
    retain_handling_type : RetainHandlingType,
}

struct ConnectPacket {
    keep_alive_interval_seconds : u16,
    clean_start : bool,

    client_id : Option<String>,

    username : Option<String>,
    password : Option<Vec<u8>>,

    session_expiry_interval_seconds : Option<u32>,

    request_response_information : Option<bool>,
    request_problem_information : Option<bool>,
    receive_maximum : Option<u16>,
    topic_alias_maximum : Option<u16>,
    maximum_packet_size_bytes : Option<u32>,

    will_delay_interval_seconds : Option<u32>,
    will : Option<PublishPacket>,

    user_properties : Option<Vec<UserProperty>>,
}

struct ConnackPacket {
    session_present : bool,
    reason_code : ConnectReasonCode,

    session_expiry_interval : Option<u32>,
    receive_maximum : Option<u16>,
    maximum_qos : Option<QualityOfService>,
    retain_available : Option<bool>,
    maximum_packet_size : Option<u32>,
    assigned_client_identifier : Option<String>,
    topic_alias_maximum : Option<u16>,
    reason_string : Option<String>,

    user_properties : Option<Vec<UserProperty>>,

    wildcard_subscriptions_available : Option<bool>,
    subscription_identifiers_available : Option<bool>,
    shared_subscriptions_available : Option<bool>,

    server_keep_alive : Option<u16>,
    response_information : Option<String>,
    server_reference : Option<String>,
    authentication_method : Option<String>,
    authentication_data : Option<Vec<u8>>,
}

struct PublishPacket {
    packet_id : u16,

    topic : String,

    qos : QualityOfService,
    duplicate : bool,
    retain : bool,

    payload : Vec<u8>,

    payload_format : Option<PayloadFormatIndicator>,
    message_expiry_interval_seconds : Option<u32>,
    topic_alias : Option<u16>,
    response_topic : Option<String>,
    correlation_data : Option<String>,

    subscription_identifiers : Option<Vec<u32>>,

    content_type : Option<String>,

    user_properties : Option<Vec<UserProperty>>,
}

struct PubackPacket {
    packet_id : u16,

    reason_code : PubackReasonCode,
    reason_string : Option<String>,

    user_properties : Option<Vec<UserProperty>>,
}

struct PubrecPacket {
    packet_id : u16,

    reason_code : PubrecReasonCode,
    reason_string : Option<String>,

    user_properties : Option<Vec<UserProperty>>,
}

struct PubrelPacket {
    packet_id : u16,

    reason_code : PubrelReasonCode,
    reason_string : Option<String>,

    user_properties : Option<Vec<UserProperty>>,
}

struct PubcompPacket {
    packet_id : u16,

    reason_code : PubcompReasonCode,
    reason_string : Option<String>,

    user_properties : Option<Vec<UserProperty>>,
}

struct SubscribePacket {
    packet_id : u16,

    subscriptions : Vec<Subscription>,

    subscription_identifier : Option<u32>,

    user_properties : Option<Vec<UserProperty>>,
}

struct SubackPacket {
    packet_id : u16,

    reason_string : Option<String>,

    user_properties : Option<Vec<UserProperty>>,

    reason_codes : Vec<SubackReasonCode>,
}

struct UnsubscribePacket {
    packet_id : u16,

    topic_filters : Vec<String>,

    user_properties : Option<Vec<UserProperty>>,
}

struct UnsubackPacket {
    packet_id : u16,

    reason_string : Option<String>,

    user_properties : Option<Vec<UserProperty>>,

    reason_codes : Vec<UnsubackReasonCode>,
}

struct PingreqPacket {
}

struct PingrespPacket {
}

struct DisconnectPacket {
    reason_code : DisconnectReasonCode,

    session_expiry_interval_seconds : Option<u32>,
    reason_string : Option<String>,
    user_properties : Option<Vec<UserProperty>>,
    server_reference : Option<String>,
}

struct AuthPacket {
    reason_code : AuthenticateReasonCode,

    method : Option<String>,
    data : Option<Vec<u8>>,
    reason_string : Option<String>,

    user_properties : Option<Vec<UserProperty>>,
}

enum MqttPacket {
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
