/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod utils;

use crate::spec::*;
use crate::*;
use crate::spec::auth::*;
use crate::spec::connack::*;
use crate::spec::connect::*;
use crate::spec::disconnect::*;
use crate::spec::puback::*;
use crate::spec::pubcomp::*;
use crate::spec::pubrec::*;
use crate::spec::pubrel::*;
use crate::client::*;
use crate::alias::*;

pub(crate) const MAXIMUM_STRING_PROPERTY_LENGTH : usize = 65535;
pub(crate) const MAXIMUM_BINARY_PROPERTY_LENGTH : usize = 65535;

pub(crate) struct ValidationContext<'a> {

    // Maximum packet size, maximum qos, incoming topic alias, retained, wildcard, sub ids, shared subs
    pub negotiated_settings : &'a NegotiatedSettings,

    // inbound manual topic alias validation
    // We don't bother with outbound alias validation because the outbound alias resolution
    // contract is to just send the topic if an oubtound mapping is invalid.
    pub inbound_alias_resolver: &'a InboundAliasResolver,

    // session_expiry_interval for disconnect constraints
    pub client_config: &'a Mqtt5ClientOptions,

    // true if this is destined for a broker, false if it is from a broker
    pub is_outbound: bool
}

fn validate_user_property(property: &UserProperty) -> Mqtt5Result<()> {
    if property.name.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        return Err(Mqtt5Error::UserPropertyValidation);
    }

    if property.value.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        return Err(Mqtt5Error::UserPropertyValidation);
    }

    Ok(())
}

pub(crate) fn validate_user_properties(properties: &Vec<UserProperty>) -> Mqtt5Result<()> {
    for property in properties {
        validate_user_property(property)?;
    }

    Ok(())
}

/// Validates all intrinsic packet properties against the MQTT5 spec requirements.
/// The only property skipped is the total packet size (against the maximum variable length
/// integer + 5) since we check packet size against the protocol-negotiated maximum later
/// in the context_specific variant.  Packets that can be used before negotiation completes
/// (auth, connect, connack) are the exception and do get a size check here.
pub(crate) fn validate_packet_fixed(packet: &MqttPacket) -> Mqtt5Result<()> {
    match packet {
        MqttPacket::Auth(auth) => { validate_auth_packet_fixed(auth) }
        MqttPacket::Connack(connack) => { validate_connack_packet_fixed(connack) }
        MqttPacket::Connect(connect) => { validate_connect_packet_fixed(connect) }
        MqttPacket::Disconnect(disconnect) => { validate_disconnect_packet_fixed(disconnect) }
        MqttPacket::Pingreq(_) => { Ok(()) }
        MqttPacket::Pingresp(_) => { Ok(()) }
        MqttPacket::Puback(puback) => { validate_puback_packet_fixed(puback) }
        MqttPacket::Pubcomp(pubcomp) => { validate_pubcomp_packet_fixed(pubcomp) }
        MqttPacket::Pubrec(pubrec) => { validate_pubrec_packet_fixed(pubrec) }
        MqttPacket::Pubrel(pubrel) => { validate_pubrel_packet_fixed(pubrel) }
        _ => {
            Err(Mqtt5Error::Unimplemented)
        }
    }
}

/// Validates various context specific properties against the MQTT5 spec based on the current
/// internal state of the client and its negotiated settings.
///
/// For example, validates against the negotiated maximum packet size, topic alias, etc...
pub(crate) fn validate_packet_context_specific(packet: &MqttPacket, context: &ValidationContext) -> Mqtt5Result<()> {
    match packet {
        MqttPacket::Auth(auth) => { validate_auth_packet_context_specific(auth, context) }
        MqttPacket::Connack(connack) => { validate_connack_packet_context_specific(connack, context) }
        MqttPacket::Connect(connect) => { validate_connect_packet_context_specific(connect, context) }
        MqttPacket::Disconnect(disconnect) => { validate_disconnect_packet_context_specific(disconnect, context) }
        MqttPacket::Pingreq(_) => { Ok(()) }
        MqttPacket::Pingresp(_) => { Ok(()) }
        MqttPacket::Puback(puback) => { validate_puback_packet_context_specific(puback, context) }
        MqttPacket::Pubcomp(pubcomp) => { validate_pubcomp_packet_context_specific(pubcomp, context) }
        MqttPacket::Pubrec(pubrec) => { validate_pubrec_packet_context_specific(pubrec, context) }
        MqttPacket::Pubrel(pubrel) => { validate_pubrel_packet_context_specific(pubrel, context) }
        _ => {
            Err(Mqtt5Error::Unimplemented)
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::encode::utils::MAXIMUM_VARIABLE_LENGTH_INTEGER;

    pub(crate) struct PinnedValidationContext{
        pub settings : NegotiatedSettings,
        inbound_resolver : InboundAliasResolver,
        pub config : Mqtt5ClientOptions,
    }

    pub(crate) fn create_pinned_validation_context() -> PinnedValidationContext {
        let mut pinned_context = PinnedValidationContext {
            settings : NegotiatedSettings {..Default::default() },
            inbound_resolver : InboundAliasResolver::new(0),
            config : Mqtt5ClientOptions{ ..Default::default() },
        };

        pinned_context.settings.maximum_packet_size_to_server = MAXIMUM_VARIABLE_LENGTH_INTEGER as u32;

        pinned_context
    }

    pub(crate) fn create_validation_context_from_pinned(pinned: &PinnedValidationContext) -> ValidationContext {
        ValidationContext {
            negotiated_settings : &pinned.settings,
            inbound_alias_resolver : &pinned.inbound_resolver,
            client_config : &pinned.config,
            is_outbound : true,
        }
    }

    pub(crate) fn create_invalid_user_properties() -> Vec<UserProperty> {
        vec!(
            UserProperty{name: "GoodName".to_string(), value: "badvalue".repeat(20000)},
            UserProperty{name: "badname".repeat(10000), value: "goodvalue".to_string()},
        )
    }

}