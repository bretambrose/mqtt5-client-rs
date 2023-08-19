/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::decode::utils::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::spec::*;
use crate::spec::utils::*;

use std::collections::VecDeque;

/// Data model of an [MQTT5 CONNECT](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901033) packet.
#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ConnectPacket {

    /// The maximum time interval, in seconds, that is permitted to elapse between the point at which the client
    /// finishes transmitting one MQTT packet and the point it starts sending the next.  The client will use
    /// PINGREQ packets to maintain this property.
    ///
    /// If the responding CONNACK contains a keep alive property value, then that is the negotiated keep alive value.
    /// Otherwise, the keep alive sent by the client is the negotiated value.
    ///
    /// See [MQTT5 Keep Alive](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901045)
    pub keep_alive_interval_seconds: u16,

    /// Clean start is modeled but not under direct user control.  Instead it is controlled by client
    /// configuration that is outside the scope of the MQTT5 spec.
    pub(crate) clean_start: bool,

    /// A unique string identifying the client to the server.  Used to restore session state between connections.
    ///
    /// If left empty, the broker will auto-assign a unique client id.  When reconnecting, the mqtt5 client will
    /// always use the auto-assigned client id.
    ///
    /// See [MQTT5 Client Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901059)
    pub client_id: Option<String>,

    /// A string value that the server may use for client authentication and authorization.
    ///
    /// See [MQTT5 User Name](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901071)
    pub username: Option<String>,

    /// Opaque binary data that the server may use for client authentication and authorization.
    ///
    /// See [MQTT5 Password](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901072)
    pub password: Option<Vec<u8>>,

    /// A time interval, in seconds, that the client requests the server to persist this connection's MQTT session state
    /// for.  Has no meaning if the client has not been configured to rejoin sessions.  Must be non-zero in order to
    /// successfully rejoin a session.
    ///
    /// If the responding CONNACK contains a session expiry property value, then that is the negotiated session expiry
    /// value.  Otherwise, the session expiry sent by the client is the negotiated value.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901048)
    pub session_expiry_interval_seconds: Option<u32>,

    /// If set to true, requests that the server send response information in the subsequent CONNACK.  This response
    /// information may be used to set up request-response implementations over MQTT, but doing so is outside
    /// the scope of the MQTT5 spec and client.
    ///
    /// See [MQTT5 Request Response Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901052)
    pub request_response_information: Option<bool>,

    /// If set to true, requests that the server send additional diagnostic information (via response string or
    /// user properties) in DISCONNECT or CONNACK packets from the server.
    ///
    /// See [MQTT5 Request Problem Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901053)
    pub request_problem_information: Option<bool>,

    /// Notifies the server of the maximum number of in-flight Qos 1 and 2 messages the client is willing to handle.  If
    /// omitted, then no limit is requested.
    ///
    /// See [MQTT5 Receive Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901049)
    pub receive_maximum: Option<u16>,

    /// Maximum number of topic aliases that the client will accept for incoming publishes.  An inbound topic alias larger than
    /// this number is a protocol error.  If this value is not specified, the client does not support inbound topic
    /// aliasing.
    ///
    /// See [MQTT5 Topic Alias Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901051)
    pub topic_alias_maximum: Option<u16>,

    /// Notifies the server of the maximum packet size the client is willing to handle.  If
    /// omitted, then no limit beyond the natural limits of MQTT packet size is requested.
    ///
    /// See [MQTT5 Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901050)
    pub maximum_packet_size_bytes: Option<u32>,

    /// Notifies the server that the client wishes to use a specific authentication method as part of the connection
    /// process.  If this field is left empty, no authentication exchange should be performed as part of the connection
    /// process.
    ///
    /// See [MQTT5 Authentication Method](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901055)
    pub authentication_method: Option<String>,

    /// Additional authentication method specific binary data supplied as part of kicking off an authentication
    /// exchange.  This field may only be set if `authentication_method` is also set.
    ///
    /// See [MQTT5 Authentication Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901056)
    pub authentication_data: Option<Vec<u8>>,

    /// A time interval, in seconds, that the server should wait (for a session reconnection) before sending the
    /// will message associated with the connection's session.  If omitted, the server will send the will when the
    /// associated session is destroyed.  If the session is destroyed before a will delay interval has elapsed, then
    /// the will must be sent at the time of session destruction.
    ///
    /// See [MQTT5 Will Delay Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901062)
    pub will_delay_interval_seconds: Option<u32>,

    /// The definition of a message to be published when the connection's session is destroyed by the server or when
    /// the will delay interval has elapsed, whichever comes first.  If undefined, then nothing will be sent.
    ///
    /// See [MQTT5 Will](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901040)
    pub will: Option<PublishPacket>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901054)
    pub user_properties: Option<Vec<UserProperty>>,
}

fn get_connect_packet_client_id(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connect, client_id)
}

fn get_connect_packet_authentication_method(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connect, authentication_method)
}

fn get_connect_packet_authentication_data(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Connect, authentication_data)
}

fn get_connect_packet_username(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connect, username)
}

fn get_connect_packet_password(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Connect, password)
}

fn get_connect_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(properties) = &connect.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

fn get_connect_packet_will_content_type(packet: &MqttPacket) -> &str {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(content_type) = &will.content_type {
                return content_type;
            }
        }
    }

    panic!("Encoder: will content type accessor invoked in an invalid state");
}

fn get_connect_packet_will_response_topic(packet: &MqttPacket) -> &str {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(response_topic) = &will.response_topic {
                return response_topic;
            }
        }
    }

    panic!("Will response topic accessor invoked in an invalid state");
}

fn get_connect_packet_will_correlation_data(packet: &MqttPacket) -> &[u8] {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(correlation_data) = &will.correlation_data {
                return correlation_data;
            }
        }
    }

    panic!("Will correlation data accessor invoked in an invalid state");
}

fn get_connect_packet_will_topic(packet: &MqttPacket) -> &str {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            return &will.topic;
        }
    }

    panic!("Will topic accessor invoked in an invalid state");
}

fn get_connect_packet_will_payload(packet: &MqttPacket) -> &[u8] {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(payload) = &will.payload {
                return payload;
            }
        }
    }

    panic!("Will payload accessor invoked in an invalid state");
}

fn get_connect_packet_will_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(publish) = &connect.will {
            if let Some(properties) = &publish.user_properties {
                return &properties[index];
            }
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

static MQTT5_CONNECT_PROTOCOL_BYTES: [u8; 7] = [0, 4, 77, 81, 84, 84, 5];
fn get_connect_protocol_bytes(_: &MqttPacket) -> &'static [u8] {
    return &MQTT5_CONNECT_PROTOCOL_BYTES;
}

fn compute_connect_flags(packet: &ConnectPacket) -> u8 {
    let mut flags: u8 = 0;
    if packet.clean_start {
        flags |= 1u8 << 1;
    }

    if let Some(will) = &packet.will {
        flags |= 1u8 << 2;
        flags |= (will.qos as u8) << 3;
        if will.retain {
            flags |= 1u8 << 5;
        }
    }

    if packet.password.is_some() {
        flags |= 1u8 << 6;
    }

    if packet.username.is_some() {
        flags |= 1u8 << 7;
    }

    flags
}

#[rustfmt::skip]
fn compute_connect_packet_length_properties(packet: &ConnectPacket) -> Mqtt5Result<(u32, u32, u32)> {
    let mut connect_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_u32_property_length!(connect_property_section_length, packet.session_expiry_interval_seconds);
    add_optional_u16_property_length!(connect_property_section_length, packet.receive_maximum);
    add_optional_u32_property_length!(connect_property_section_length, packet.maximum_packet_size_bytes);
    add_optional_u16_property_length!(connect_property_section_length, packet.topic_alias_maximum);
    add_optional_u8_property_length!(connect_property_section_length, packet.request_response_information);
    add_optional_u8_property_length!(connect_property_section_length, packet.request_problem_information);
    add_optional_string_property_length!(connect_property_section_length, packet.authentication_method);
    add_optional_bytes_property_length!(connect_property_section_length, packet.authentication_data);

    /* variable header length =
     *    10 bytes (6 for mqtt string, 1 for protocol version, 1 for flags, 2 for keep alive)
     *  + # bytes(variable_length_encoding(connect_property_section_length))
     *  + connect_property_section_length
     */
    let mut variable_header_length = compute_variable_length_integer_encode_size(connect_property_section_length)?;
    variable_header_length += 10 + connect_property_section_length;

    let mut payload_length : usize = 0;
    add_optional_string_length!(payload_length, packet.client_id);

    let mut will_property_length : usize = 0;
    if let Some(will) = &packet.will {
        will_property_length = compute_user_properties_length(&will.user_properties);

        add_optional_u32_property_length!(will_property_length, packet.will_delay_interval_seconds);
        add_optional_u8_property_length!(will_property_length, will.payload_format);
        add_optional_u32_property_length!(will_property_length, will.message_expiry_interval_seconds);
        add_optional_string_property_length!(will_property_length, will.content_type);
        add_optional_string_property_length!(will_property_length, will.response_topic);
        add_optional_bytes_property_length!(will_property_length, will.correlation_data);

        let will_properties_length_encode_size = compute_variable_length_integer_encode_size(will_property_length)?;

        payload_length += will_property_length;
        payload_length += will_properties_length_encode_size;
        payload_length += 2 + will.topic.len();
        add_optional_bytes_length!(payload_length, will.payload);
    }

    if let Some(username) = &packet.username {
        payload_length += 2 + username.len();
    }

    if let Some(password) = &packet.password {
        payload_length += 2 + password.len();
    }

    let total_remaining_length : usize = payload_length + variable_header_length;

    if total_remaining_length > MAXIMUM_VARIABLE_LENGTH_INTEGER {
        return Err(Mqtt5Error::VariableLengthIntegerMaximumExceeded);
    }

    Ok((total_remaining_length as u32, connect_property_section_length as u32, will_property_length as u32))
}

#[rustfmt::skip]
pub(crate) fn write_connect_encoding_steps(packet: &ConnectPacket, _: &mut EncodingContext, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<()> {
    let (total_remaining_length, connect_property_length, will_property_length) = compute_connect_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, 1u8 << 4);
    encode_integral_expression!(steps, Vli, total_remaining_length);
    encode_raw_bytes!(steps, get_connect_protocol_bytes);
    encode_integral_expression!(steps, Uint8, compute_connect_flags(packet));
    encode_integral_expression!(steps, Uint16, packet.keep_alive_interval_seconds);

    encode_integral_expression!(steps, Vli, connect_property_length);
    encode_optional_property!(steps, Uint32, PROPERTY_KEY_SESSION_EXPIRY_INTERVAL, packet.session_expiry_interval_seconds);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_RECEIVE_MAXIMUM, packet.receive_maximum);
    encode_optional_property!(steps, Uint32, PROPERTY_KEY_MAXIMUM_PACKET_SIZE, packet.maximum_packet_size_bytes);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM, packet.topic_alias_maximum);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION, packet.request_response_information);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION, packet.request_problem_information);
    encode_optional_string_property!(steps, get_connect_packet_authentication_method, PROPERTY_KEY_AUTHENTICATION_METHOD, packet.authentication_method);
    encode_optional_bytes_property!(steps, get_connect_packet_authentication_data, PROPERTY_KEY_AUTHENTICATION_DATA, packet.authentication_data);
    encode_user_properties!(steps, get_connect_packet_user_property, packet.user_properties);

    encode_length_prefixed_optional_string!(steps, get_connect_packet_client_id, packet.client_id);

    if let Some(will) = &packet.will {
        encode_integral_expression!(steps, Vli, will_property_length);
        encode_optional_property!(steps, Uint32, PROPERTY_KEY_WILL_DELAY_INTERVAL, packet.will_delay_interval_seconds);
        encode_optional_enum_property!(steps, Uint8, PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR, u8, will.payload_format);
        encode_optional_property!(steps, Uint32, PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL, will.message_expiry_interval_seconds);
        encode_optional_string_property!(steps, get_connect_packet_will_content_type, PROPERTY_KEY_CONTENT_TYPE, &will.content_type);
        encode_optional_string_property!(steps, get_connect_packet_will_response_topic, PROPERTY_KEY_RESPONSE_TOPIC, &will.response_topic);
        encode_optional_bytes_property!(steps, get_connect_packet_will_correlation_data, PROPERTY_KEY_CORRELATION_DATA, will.correlation_data);
        encode_user_properties!(steps, get_connect_packet_will_user_property, will.user_properties);

        encode_length_prefixed_string!(steps, get_connect_packet_will_topic, will.topic);
        encode_length_prefixed_optional_bytes!(steps, get_connect_packet_will_payload, will.payload);
    }

    if packet.username.is_some() {
        encode_length_prefixed_optional_string!(steps, get_connect_packet_username, packet.username);
    }

    if packet.password.is_some() {
        encode_length_prefixed_optional_bytes!(steps, get_connect_packet_password, packet.password);
    }

    Ok(())
}

fn decode_connect_properties(property_bytes: &[u8], packet : &mut ConnectPacket) -> Mqtt5Result<()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_SESSION_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.session_expiry_interval_seconds)?; }
            PROPERTY_KEY_RECEIVE_MAXIMUM => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.receive_maximum)?; }
            PROPERTY_KEY_MAXIMUM_PACKET_SIZE => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.maximum_packet_size_bytes)?; }
            PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.topic_alias_maximum)?; }
            PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.request_response_information)?; }
            PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.request_problem_information)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            PROPERTY_KEY_AUTHENTICATION_METHOD => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.authentication_method)?; }
            PROPERTY_KEY_AUTHENTICATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut packet.authentication_data)?; }
            _ => { return Err(Mqtt5Error::MalformedPacket); }
        }
    }

    Ok(())
}

fn decode_will_properties(property_bytes: &[u8], will: &mut PublishPacket, connect : &mut ConnectPacket) -> Mqtt5Result<()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_WILL_DELAY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut connect.will_delay_interval_seconds)?; }
            PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR => { mutable_property_bytes = decode_optional_u8_as_enum(mutable_property_bytes, &mut will.payload_format, convert_u8_to_payload_format_indicator)?; }
            PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut will.message_expiry_interval_seconds)?; }
            PROPERTY_KEY_CONTENT_TYPE => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut will.content_type)?; }
            PROPERTY_KEY_RESPONSE_TOPIC => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut will.response_topic)?; }
            PROPERTY_KEY_CORRELATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut will.correlation_data)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut will.user_properties)?; }
            _ => { return Err(Mqtt5Error::MalformedPacket); }
        }
    }

    Ok(())
}

const CONNECT_HEADER_PROTOCOL_LENGTH : usize = 7;

pub(crate) fn decode_connect_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<ConnectPacket>> {
    let mut packet = Box::new(ConnectPacket { ..Default::default() } );

    if first_byte != (PACKET_TYPE_CONNECT << 4)  {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let mut mutable_body = packet_body;
    if mutable_body.len() < CONNECT_HEADER_PROTOCOL_LENGTH {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let protocol_bytes = &mutable_body[..CONNECT_HEADER_PROTOCOL_LENGTH];
    mutable_body = &mutable_body[CONNECT_HEADER_PROTOCOL_LENGTH..];

    match protocol_bytes {
        [0u8, 4u8, 77u8, 81u8, 84u8, 84u8, 5u8] => { }
        _ => { return Err(Mqtt5Error::MalformedPacket); }
    }

    /*
    if protocol_bytes == CONNECT_HEADER_PROTOCOL_BYTES.as_slice() {
        return Err(Mqtt5Error::ProtocolError);
    }*/

    let mut connect_flags : u8 = 0;
    mutable_body = decode_u8(mutable_body, &mut connect_flags)?;

    packet.clean_start = (connect_flags & CONNECT_PACKET_CLEAN_START_FLAG_MASK) != 0;
    let has_will = (connect_flags & CONNECT_PACKET_HAS_WILL_FLAG_MASK) != 0;
    let will_retain = (connect_flags & CONNECT_PACKET_WILL_RETAIN_FLAG_MASK) != 0;
    let will_qos = convert_u8_to_quality_of_service((connect_flags >> CONNECT_PACKET_WILL_QOS_FLAG_SHIFT) & QOS_MASK)?;

    if !has_will {
        /* indirectly check bits of connect flags vs. spec */
        if will_retain || will_qos != QualityOfService::AtMostOnce {
            return Err(Mqtt5Error::MalformedPacket);
        }
    }

    let has_username = (connect_flags & CONNECT_PACKET_HAS_USERNAME_FLAG_MASK) != 0;
    let has_password = (connect_flags & CONNECT_PACKET_HAS_PASSWORD_FLAG_MASK) != 0;

    mutable_body = decode_u16(mutable_body, &mut packet.keep_alive_interval_seconds)?;

    let mut connect_property_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut connect_property_length)?;

    if mutable_body.len() < connect_property_length {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let property_body = &mutable_body[..connect_property_length];
    mutable_body = &mutable_body[connect_property_length..];

    decode_connect_properties(property_body, &mut packet)?;

    mutable_body = decode_length_prefixed_optional_string(mutable_body, &mut packet.client_id)?;

    if has_will {
        let mut will_property_length : usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut will_property_length)?;

        if mutable_body.len() < will_property_length {
            return Err(Mqtt5Error::MalformedPacket);
        }

        let will_property_body = &mutable_body[..will_property_length];
        mutable_body = &mutable_body[will_property_length..];

        let mut will : PublishPacket = PublishPacket {
            qos : will_qos,
            retain : will_retain,
            ..Default::default()
        };

        decode_will_properties(will_property_body, &mut will, &mut packet)?;

        mutable_body = decode_length_prefixed_string(mutable_body, &mut will.topic)?;
        mutable_body = decode_length_prefixed_optional_bytes(mutable_body, &mut will.payload)?;

        packet.will = Some(will);
    }

    if has_username {
        mutable_body = decode_optional_length_prefixed_string(mutable_body, &mut packet.username)?;
    }

    if has_password {
        mutable_body = decode_optional_length_prefixed_bytes(mutable_body, &mut packet.password)?;
    }

    if mutable_body.len() > 0 {
        return Err(Mqtt5Error::MalformedPacket);
    }

    Ok(packet)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn connect_round_trip_encode_decode_default() {
        let packet = Box::new(ConnectPacket {
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_basic() {
        let packet = Box::new(ConnectPacket {
            keep_alive_interval_seconds : 1200,
            clean_start : true,
            client_id : Some("MyClient".to_string()),
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_no_flags_all_optional_properties() {
        let packet = Box::new(ConnectPacket {
            keep_alive_interval_seconds : 3600,
            clean_start : true,
            client_id : Some("MyClient2".to_string()),
            session_expiry_interval_seconds: Some(0xFFFFFFFFu32),
            request_response_information: Some(true),
            request_problem_information: Some(false),
            receive_maximum: Some(100),
            topic_alias_maximum: Some(20),
            maximum_packet_size_bytes: Some(128 * 1024),
            authentication_method: Some("Kerberos".to_string()),
            authentication_data: Some(vec![5, 4, 3, 2, 1]),
            user_properties: Some(vec!(
                UserProperty{name: "connecting".to_string(), value: "future".to_string()},
                UserProperty{name: "Iamabanana".to_string(), value: "Hamizilla".to_string()},
            )),
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_username_only() {
        let packet = Box::new(ConnectPacket {
            username : Some("SpaceUnicorn".to_string()),
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_password_only() {
        let packet = Box::new(ConnectPacket {
            password : Some("Marshmallow Lasers".as_bytes().to_vec()),
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_all_non_will_properties() {
        let packet = Box::new(ConnectPacket {
            keep_alive_interval_seconds : 3600,
            clean_start : true,
            client_id : Some("NotAHaxxor".to_string()),
            session_expiry_interval_seconds: Some(0x1234ABCDu32),
            request_response_information: Some(false),
            request_problem_information: Some(true),
            receive_maximum: Some(1000),
            topic_alias_maximum: Some(2),
            maximum_packet_size_bytes: Some(512 * 1024 - 1),
            authentication_method: Some("GSSAPI".to_string()),
            authentication_data: Some(vec![15, 14, 13, 12, 11]),
            user_properties: Some(vec!(
                UserProperty{name: "Another".to_string(), value: "brick".to_string()},
                UserProperty{name: "WhenIwas".to_string(), value: "ayoungboy".to_string()},
            )),
            username: Some("Gluten-free armada".to_string()),
            password: Some("PancakeRobot".as_bytes().to_vec()),
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_default_will() {
        let packet = Box::new(ConnectPacket {
            will : Some(PublishPacket {
                ..Default::default()
            }),
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_simple_will() {
        let packet = Box::new(ConnectPacket {
            will : Some(PublishPacket {
                topic : "in/rememberance".to_string(),
                qos: QualityOfService::ExactlyOnce,
                payload: Some("I'llbealright".as_bytes().to_vec()),
                ..Default::default()
            }),
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_all_will_fields() {
        let packet = Box::new(ConnectPacket {
            will_delay_interval_seconds : Some(60),
            will : Some(PublishPacket {
                topic : "in/rememberance/of/mrkrabs".to_string(),
                qos: QualityOfService::ExactlyOnce,
                payload: Some("Arrrrrrrrrrrrrrr".as_bytes().to_vec()),
                retain: true,
                payload_format : Some(PayloadFormatIndicator::Utf8),
                message_expiry_interval_seconds : Some(1800),
                content_type : Some("QueryXML".to_string()),
                response_topic : Some("forever/today".to_string()),
                correlation_data : Some("Request1".as_bytes().to_vec()),
                user_properties: Some(vec!(
                    UserProperty{name: "WillProp1".to_string(), value: "WillValue1".to_string()},
                )),
                ..Default::default()
            }),
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_everything() {
        let packet = Box::new(ConnectPacket {
            keep_alive_interval_seconds : 3600,
            clean_start : true,
            client_id : Some("NotAHaxxor".to_string()),
            session_expiry_interval_seconds: Some(0x1234ABCDu32),
            request_response_information: Some(false),
            request_problem_information: Some(true),
            receive_maximum: Some(1000),
            topic_alias_maximum: Some(2),
            maximum_packet_size_bytes: Some(512 * 1024 - 1),
            authentication_method: Some("GSSAPI".to_string()),
            authentication_data: Some(vec![15, 14, 13, 12, 11]),
            user_properties: Some(vec!(
                UserProperty{name: "Another".to_string(), value: "brick".to_string()},
                UserProperty{name: "WhenIwas".to_string(), value: "ayoungboy".to_string()},
            )),
            will_delay_interval_seconds : Some(60),
            will : Some(PublishPacket {
                topic : "in/rememberance/of/mrkrabs".to_string(),
                qos: QualityOfService::ExactlyOnce,
                payload: Some("Arrrrrrrrrrrrrrr".as_bytes().to_vec()),
                retain: true,
                payload_format : Some(PayloadFormatIndicator::Utf8),
                message_expiry_interval_seconds : Some(1800),
                content_type : Some("QueryXML".to_string()),
                response_topic : Some("forever/today".to_string()),
                correlation_data : Some("Request1".as_bytes().to_vec()),
                user_properties: Some(vec!(
                    UserProperty{name: "WillProp1".to_string(), value: "WillValue1".to_string()},
                )),
                ..Default::default()
            }),
            username: Some("Gluten-free armada".to_string()),
            password: Some("PancakeRobot".as_bytes().to_vec()),
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_decode_failure_bad_fixed_header() {
        let packet = Box::new(ConnectPacket {
            will : Some(PublishPacket {
                topic : "in/rememberance".to_string(),
                qos: QualityOfService::ExactlyOnce,
                payload: Some("I'llbealright".as_bytes().to_vec()),
                ..Default::default()
            }),
            ..Default::default()
        });

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Connect(packet), 6);
    }
}