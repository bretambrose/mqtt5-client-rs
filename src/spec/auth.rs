/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::decode::utils::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::validate::*;
use crate::spec::*;
use crate::spec::utils::*;

use std::collections::VecDeque;

/// Data model of an [MQTT5 AUTH](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901217) packet.
#[derive(Clone, Debug, Default)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct AuthPacket {

    /// Specifies an endpoint's response to a previously-received AUTH packet as part of an authentication exchange.
    ///
    /// See [MQTT5 Authenticate Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901220)
    pub reason_code: AuthenticateReasonCode,

    /// Authentication method this packet corresponds to.  The authentication method must remain the
    /// same for the entirety of an authentication exchange.
    ///
    /// See [MQTT5 Authentication Method](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901223)
    pub authentication_method: Option<String>,

    /// Method-specific binary data included in this step of an authentication exchange.
    ///
    /// See [MQTT5 Authentication Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901224)
    pub authentication_data: Option<Vec<u8>>,

    /// Additional diagnostic information or context.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901225)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901226)
    pub user_properties: Option<Vec<UserProperty>>,
}

#[rustfmt::skip]
fn compute_auth_packet_length_properties(packet: &AuthPacket) -> Mqtt5Result<(u32, u32)> {
    let mut auth_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_string_property_length!(auth_property_section_length, packet.authentication_method);
    add_optional_bytes_property_length!(auth_property_section_length, packet.authentication_data);
    add_optional_string_property_length!(auth_property_section_length, packet.reason_string);

    /* 2-byte auth packets are allowed by the spec when there are no properties and the reason code is success */
    if auth_property_section_length == 0 && packet.reason_code == AuthenticateReasonCode::Success {
        return Ok((0, 0));
    }

    let mut total_remaining_length : usize = 1 + compute_variable_length_integer_encode_size(auth_property_section_length)?;
    total_remaining_length += auth_property_section_length;

    Ok((total_remaining_length as u32, auth_property_section_length as u32))
}

fn get_auth_packet_authentication_method(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Auth, authentication_method)
}

fn get_auth_packet_authentication_data(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Auth, authentication_data)
}

fn get_auth_packet_reason_string(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Auth, reason_string)
}

fn get_auth_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Auth(auth) = packet {
        if let Some(properties) = &auth.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

#[rustfmt::skip]
pub(crate) fn write_auth_encoding_steps(packet: &AuthPacket, _: &mut EncodingContext, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<()> {
    let (total_remaining_length, auth_property_length) = compute_auth_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, PACKET_TYPE_AUTH << 4);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    if total_remaining_length == 0 {
        return Ok(());
    }

    encode_enum!(steps, Uint8, u8, packet.reason_code);
    encode_integral_expression!(steps, Vli, auth_property_length);

    encode_optional_string_property!(steps, get_auth_packet_authentication_method, PROPERTY_KEY_AUTHENTICATION_METHOD, packet.authentication_method);
    encode_optional_bytes_property!(steps, get_auth_packet_authentication_data, PROPERTY_KEY_AUTHENTICATION_DATA, packet.authentication_data);
    encode_optional_string_property!(steps, get_auth_packet_reason_string, PROPERTY_KEY_REASON_STRING, packet.reason_string);
    encode_user_properties!(steps, get_auth_packet_user_property, packet.user_properties);

    Ok(())
}


fn decode_auth_properties(property_bytes: &[u8], packet : &mut AuthPacket) -> Mqtt5Result<()> {
    let mut mutable_property_bytes = property_bytes;

    while mutable_property_bytes.len() > 0 {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_AUTHENTICATION_METHOD => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.authentication_method)?; }
            PROPERTY_KEY_AUTHENTICATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut packet.authentication_data)?; }
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => { return Err(Mqtt5Error::MalformedPacket); }
        }
    }

    Ok(())
}

pub(crate) fn decode_auth_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<AuthPacket>> {
    let mut packet = Box::new(AuthPacket { ..Default::default() });

    if first_byte != (PACKET_TYPE_AUTH << 4) {
        return Err(Mqtt5Error::MalformedPacket);
    }

    let mut mutable_body = packet_body;
    if mutable_body.len() == 0 {
        return Ok(packet);
    }

    mutable_body = decode_u8_as_enum(mutable_body, &mut packet.reason_code, convert_u8_to_authenticate_reason_code)?;

    let mut properties_length : usize = 0;
    mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
    if properties_length != mutable_body.len() {
        return Err(Mqtt5Error::MalformedPacket);
    }

    decode_auth_properties(mutable_body, &mut packet)?;

    Ok(packet)
}

pub(crate) fn validate_auth_packet_fixed(packet: &AuthPacket) -> Mqtt5Result<()> {

    // validate packet size against the theoretical maximum since this could be used
    // before the connack establishes a different bound
    let (total_remaining_length, _) = compute_auth_packet_length_properties(packet)?;
    if total_remaining_length > MAXIMUM_VARIABLE_LENGTH_INTEGER as u32 {
        return Err(Mqtt5Error::AuthPacketValidation);
    }

    if let Some(method) = &packet.authentication_method {
        if method.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
            return Err(Mqtt5Error::AuthPacketValidation);
        }
    }

    if let Some(data) = &packet.authentication_data {
        if data.len() > MAXIMUM_BINARY_PROPERTY_LENGTH {
            return Err(Mqtt5Error::AuthPacketValidation);
        }
    }

    if let Some(reason) = &packet.reason_string {
        if reason.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
            return Err(Mqtt5Error::AuthPacketValidation);
        }
    }

    if let Some(properties) = &packet.user_properties {
        if let Err(_) = validate_user_properties(&properties) {
            return Err(Mqtt5Error::AuthPacketValidation);
        }
    }

    Ok(())
}

pub(crate) fn validate_auth_packet_context_specific(packet: &AuthPacket, context: &ValidationContext) -> Mqtt5Result<()> {

    // validate packet size against the negotiated maximum
    let (total_remaining_length, _) = compute_auth_packet_length_properties(packet)?;
    if total_remaining_length > context.negotiated_settings.maximum_packet_size_to_server {
        return Err(Mqtt5Error::AuthPacketValidation);
    }

    Ok(())
}


#[cfg(test)]
mod tests {
    use crate::decode::testing::*;
    use super::*;

    #[test]
    fn auth_round_trip_encode_decode_default() {
        let packet = Box::new(AuthPacket {
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Auth(packet)));
    }

    #[test]
    fn auth_round_trip_encode_decode_required() {
        let packet = Box::new(AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            ..Default::default()
        });

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Auth(packet)));
    }

    fn create_all_properties_auth_packet() -> Box<AuthPacket> {
        Box::new(AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            authentication_method : Some("UnbreakableAuthExchange".to_string()),
            authentication_data : Some("Noonewillguessthis".as_bytes().to_vec()),
            reason_string : Some("Myfavoritebroker".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "Roblox".to_string(), value: "Wheredidmymoneygo".to_string()},
                UserProperty{name: "Beeswarmsimulator".to_string(), value: "Lootbox".to_string()},
            )),
        })
    }

    #[test]
    fn auth_round_trip_encode_decode_all_properties() {
        let packet = create_all_properties_auth_packet();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Auth(packet)));
    }

    #[test]
    fn auth_decode_failure_bad_fixed_header() {
        let packet = Box::new(AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            ..Default::default()
        });

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Auth(packet), 1);
    }

    use crate::validate::testing::*;

    #[test]
    fn auth_validate_success_all_properties() {
        let packet = create_all_properties_auth_packet();

        assert_eq!(validate_auth_packet_fixed(&packet), Ok(()));

        let test_validation_context = create_pinned_validation_context();
        let validation_context = create_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_auth_packet_context_specific(&packet, &validation_context), Ok(()));
    }

    #[test]
    fn auth_validate_failure_authentication_method_length() {
        let mut packet = create_all_properties_auth_packet();
        packet.authentication_method = Some("a".repeat(65537));

        assert_eq!(validate_auth_packet_fixed(&packet), Err(Mqtt5Error::AuthPacketValidation));
    }

    #[test]
    fn auth_validate_failure_authentication_data_length() {
        let mut packet = create_all_properties_auth_packet();
        packet.authentication_data = Some(vec![0; 128 * 1024]);

        assert_eq!(validate_auth_packet_fixed(&packet), Err(Mqtt5Error::AuthPacketValidation));
    }

    #[test]
    fn auth_validate_failure_reason_string_length() {
        let mut packet = create_all_properties_auth_packet();
        packet.reason_string = Some("a".repeat(199000));

        assert_eq!(validate_auth_packet_fixed(&packet), Err(Mqtt5Error::AuthPacketValidation));
    }

    #[test]
    fn auth_validate_failure_user_properties() {
        let mut packet = create_all_properties_auth_packet();
        packet.user_properties = Some(create_invalid_user_properties());

        assert_eq!(validate_auth_packet_fixed(&packet), Err(Mqtt5Error::AuthPacketValidation));
    }

    #[test]
    fn auth_validate_failure_context_packet_size() {
        let packet = create_all_properties_auth_packet();

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.maximum_packet_size_to_server = 50;
        let validation_context = create_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_auth_packet_context_specific(&packet, &validation_context), Err(Mqtt5Error::AuthPacketValidation));
    }
}