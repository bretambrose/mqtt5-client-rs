/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::spec::*;
use crate::*;
use crate::spec::auth::*;
use crate::client::*;
use crate::alias::*;

pub(crate) const MAXIMUM_STRING_PROPERTY_LENGTH : usize = 65535;
pub(crate) const MAXIMUM_BINARY_PROPERTY_LENGTH : usize = 65535;

pub(crate) struct ValidationContext<'a> {

    // Maximum packet size, maximum qos, incoming topic alias, retained, wildcard, sub ids, shared subs
    pub negotiated_settings : &'a NegotiatedSettings,

    // outbound manual topic alias
    pub outbound_alias_resolver: &'a dyn OutboundAliasResolver,

    // inbound manual topic alias
    pub inbound_alias_resolver: &'a dyn InboundAliasResolver,

    // session_expiry_interval for disconnect constraints
    pub client_config: &'a Mqtt5ClientOptions
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
        _ => {
            Err(Mqtt5Error::Unimplemented)
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use std::rc::Rc;
    use super::*;
    use crate::client::*;
    use crate::alias::*;
    use crate::encode::utils::MAXIMUM_VARIABLE_LENGTH_INTEGER;

    pub(crate) struct PinnedValidationContext<'a> {
        pub settings : NegotiatedSettings,
        outbound_resolver : Box<dyn OutboundAliasResolver + 'a>,
        inbound_resolver : Box<dyn InboundAliasResolver + 'a>,
        config : Mqtt5ClientOptions,
    }

    pub(crate) fn create_pinned_validation_context<'a>() -> PinnedValidationContext<'a> {
        let mut pinned_context = PinnedValidationContext {
            settings : NegotiatedSettings {..Default::default() },
            outbound_resolver : Box::new( NullOutboundAliasResolver::new()),
            inbound_resolver : Box::new( NullInboundAliasResolver::new() ),
            config : Mqtt5ClientOptions{ ..Default::default() },
        };

        pinned_context.settings.maximum_packet_size_to_server = MAXIMUM_VARIABLE_LENGTH_INTEGER as u32;

        pinned_context
    }

    pub(crate) fn create_validation_context_from_pinned<'a>(pinned: &'a PinnedValidationContext<'a>) -> ValidationContext<'a> {
        ValidationContext {
            negotiated_settings : &pinned.settings,
            outbound_alias_resolver : &pinned.outbound_resolver,
            inbound_alias_resolver : &pinned.inbound_resolver,
            client_config : &pinned.config,
        }
    }

    pub(crate) fn create_invalid_user_properties() -> Vec<UserProperty> {
        vec!(
            UserProperty{name: "GoodName".to_string(), value: "badvalue".repeat(20000)},
            UserProperty{name: "badname".repeat(10000), value: "goodvalue".to_string()},
        )
    }

}