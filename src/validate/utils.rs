/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate log;

use crate::validate::*;

use log::*;

pub(crate) fn validate_string_length(value: &String, error: Mqtt5Error, packet_type: &str, field_name: &str) -> Mqtt5Result<()> {
    if value.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        error!("{}Packet Validation - {} string field too long", packet_type, field_name);
        return Err(error);
    }

    Ok(())
}

pub(crate) fn validate_optional_string_length(optional_string: &Option<String>, error: Mqtt5Error, packet_type: &str, field_name: &str) -> Mqtt5Result<()> {
    if let Some(value) = &optional_string {
        if value.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
            error!("{}Packet Validation - {} string field too long", packet_type, field_name);
            return Err(error);
        }
    }

    Ok(())
}

macro_rules! validate_optional_binary_length {
    ($value_name: ident, $optional_binary_expr: expr, $error: ident) => {
        if let Some($value_name) = $optional_binary_expr {
            if $value_name.len() > MAXIMUM_BINARY_PROPERTY_LENGTH {
                return Err(Mqtt5Error::$error);
            }
        }
    };
}

pub(crate) use validate_optional_binary_length;

macro_rules! validate_optional_integer_non_zero {
    ($value_name: ident, $optional_integer_expr: expr, $error: ident) => {
        if let Some($value_name) = $optional_integer_expr {
            if $value_name == 0 {
                return Err(Mqtt5Error::$error);
            }
        }
    };
}

pub(crate) use validate_optional_integer_non_zero;

macro_rules! validate_ack_outbound {
    ($function_name: ident, $packet_type: ident, $error: expr, $packet_type_string: expr) => {
        pub(crate) fn $function_name(packet: &$packet_type) -> Mqtt5Result<()> {

            validate_optional_string_length(&packet.reason_string, $error, $packet_type_string, "reason_string")?;
            validate_user_properties(&packet.user_properties, $error, $packet_type_string)?;

            Ok(())
        }
    };
}

pub(crate) use validate_ack_outbound;

macro_rules! validate_ack_outbound_internal {
    ($function_name: ident, $packet_type: ident, $error: ident, $packet_length_function_name: ident) => {
        pub(crate) fn $function_name(packet: &$packet_type, context: &OutboundValidationContext) -> Mqtt5Result<()> {

            let (total_remaining_length, _) = $packet_length_function_name(packet)?;
            let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
            if total_packet_length > context.negotiated_settings.maximum_packet_size_to_server {
                return Err(Mqtt5Error::$error);
            }

            if packet.packet_id == 0 {
                return Err(Mqtt5Error::$error);
            }

            Ok(())
        }
    };
}

pub(crate) use validate_ack_outbound_internal;

macro_rules! validate_ack_inbound_internal {
    ($function_name: ident, $packet_type: ident, $error: ident) => {
        pub(crate) fn $function_name(packet: &$packet_type, _: &InboundValidationContext) -> Mqtt5Result<()> {

            if packet.packet_id == 0 {
                return Err(Mqtt5Error::$error);
            }

            Ok(())
        }
    };
}

pub(crate) use validate_ack_inbound_internal;

macro_rules! test_ack_validate_success {
    ($function_name: ident, $packet_type: ident, $packet_factory_function: ident) => {
        #[test]
        fn $function_name() {
            let packet = MqttPacket::$packet_type($packet_factory_function());

            assert_eq!(validate_packet_outbound(&packet), Ok(()));

            let test_validation_context = create_pinned_validation_context();

            let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
            assert_eq!(validate_packet_outbound_internal(&packet, &outbound_validation_context), Ok(()));

            let inbound_validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);
            assert_eq!(validate_packet_inbound_internal(&packet, &inbound_validation_context), Ok(()));
        }
    };
}

pub(crate) use test_ack_validate_success;

macro_rules! test_ack_validate_failure_reason_string_length {
    ($function_name: ident, $packet_type: ident, $packet_factory_function: ident, $error: ident) => {
        #[test]
        fn $function_name() {
            let mut packet = $packet_factory_function();
            packet.reason_string = Some("A".repeat(128 * 1024).to_string());

            assert_eq!(validate_packet_outbound(&MqttPacket::$packet_type(packet)), Err(Mqtt5Error::$error));
        }
    };
}

pub(crate) use test_ack_validate_failure_reason_string_length;

macro_rules! test_ack_validate_failure_invalid_user_properties {
    ($function_name: ident, $packet_type: ident, $packet_factory_function: ident, $error: ident) => {
        #[test]
        fn $function_name() {
            let mut packet = $packet_factory_function();
            packet.user_properties = Some(create_invalid_user_properties());

            assert_eq!(validate_packet_outbound(&MqttPacket::$packet_type(packet)), Err(Mqtt5Error::$error));
        }
    };
}

pub(crate) use test_ack_validate_failure_invalid_user_properties;

macro_rules! test_ack_validate_failure_outbound_size {
    ($function_name: ident, $packet_type: ident, $packet_factory_function: ident, $error: ident) => {
        #[test]
        fn $function_name() {
            let packet = $packet_factory_function();

            do_outbound_size_validate_failure_test(&MqttPacket::$packet_type(packet), Mqtt5Error::$error);
        }
    };
}

pub(crate) use test_ack_validate_failure_outbound_size;

macro_rules! test_ack_validate_failure_packet_id_zero {
    ($function_name: ident, $packet_type: ident, $packet_factory_function: ident, $error: ident) => {
        #[test]
        fn $function_name() {
            let mut ack = $packet_factory_function();
            ack.packet_id = 0;

            let packet = MqttPacket::$packet_type(ack);

            let test_validation_context = create_pinned_validation_context();

            let outbound_context = create_outbound_validation_context_from_pinned(&test_validation_context);
            assert_eq!(validate_packet_outbound_internal(&packet, &outbound_context), Err(Mqtt5Error::$error));

            let inbound_context = create_inbound_validation_context_from_pinned(&test_validation_context);
            assert_eq!(validate_packet_inbound_internal(&packet, &inbound_context), Err(Mqtt5Error::$error));
        }
    };
}

pub(crate) use test_ack_validate_failure_packet_id_zero;

macro_rules! test_ack_validate_failure_inbound_packet_id_zero {
    ($function_name: ident, $packet_type: ident, $packet_factory_function: ident, $error: ident) => {
        #[test]
        fn $function_name() {
            let mut ack = $packet_factory_function();
            ack.packet_id = 0;

            let packet = MqttPacket::$packet_type(ack);

            let test_validation_context = create_pinned_validation_context();
            let inbound_context = create_inbound_validation_context_from_pinned(&test_validation_context);
            assert_eq!(validate_packet_inbound_internal(&packet, &inbound_context), Err(Mqtt5Error::$error));
        }
    };
}

pub(crate) use test_ack_validate_failure_inbound_packet_id_zero;

pub(crate) fn is_valid_topic(topic: &str) -> bool {
    if topic.len() == 0 || topic.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        return false;
    }

    if topic.contains(['#', '+']) {
        return false;
    }

    true
}

pub(crate) fn is_valid_topic_filter(topic: &str) -> bool {
    if topic.len() == 0 || topic.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        return false;
    }

    let mut seen_mlw = false;
    for segment in  topic.split('/') {
        if seen_mlw {
            return false;
        }

        if segment.len() == 1 {
            if segment == "#" {
                seen_mlw = true;
            }
        } else if segment.contains(['#', '+']) {
            return false;
        }
    }
    true
}

// if the topic filter is not valid, then the other fields are not to be trusted
pub(crate) struct TopicFilterProperties {
    pub is_valid: bool,
    pub is_shared: bool,
    pub has_wildcard: bool
}

fn compute_topic_filter_properties(topic: &str) -> TopicFilterProperties {
    let mut properties = TopicFilterProperties {
        is_valid: true,
        is_shared: false,
        has_wildcard: false
    };

    if topic.len() == 0 || topic.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        properties.is_valid = false;
        return properties;
    }

    let mut has_share_prefix = false;
    let mut has_share_name = false;
    let mut seen_mlw = false;
    for (index, segment) in  topic.split('/').enumerate() {
        if seen_mlw {
            properties.is_valid = false;
            break;
        }

        let has_wildcard = segment.contains(['#', '+']);
        properties.has_wildcard |= has_wildcard;

        if index == 0 && segment == "$share" {
            has_share_prefix = true;
        }

        if index == 1 && has_share_prefix && segment.len() > 0 && !has_wildcard {
            has_share_name = true;
        }

        if has_share_name {
            if (index == 2 && segment.len() > 0) || index > 2 {
                properties.is_shared = true;
            }
        }

        if segment.len() == 1 {
            if segment == "#" {
                seen_mlw = true;
            }
        } else if has_wildcard {
            properties.is_valid = false;
            break;
        }
    }

    properties
}

pub(crate) fn is_topic_filter_valid_internal(filter: &str, context: &OutboundValidationContext, no_local: Option<bool>) -> bool {
    let topic_filter_properties = compute_topic_filter_properties(filter);

    if !topic_filter_properties.is_valid {
        return false;
    }

    if topic_filter_properties.is_shared {
        if !context.negotiated_settings.shared_subscriptions_available {
            return false;
        }

        if let Some(no_local_value) = no_local {
            if no_local_value {
                return false;
            }
        }
    }

    if topic_filter_properties.has_wildcard && !context.negotiated_settings.wildcard_subscriptions_available {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn check_valid_topics() {
        assert_eq!(true, is_valid_topic("/"));
        assert_eq!(true, is_valid_topic("a/"));
        assert_eq!(true, is_valid_topic("/b"));
        assert_eq!(true, is_valid_topic("a/b/c"));
        assert_eq!(true, is_valid_topic("hippopotamus"));
    }

    #[test]
    fn check_invalid_topics() {
        assert_eq!(false, is_valid_topic("#"));
        assert_eq!(false, is_valid_topic("sport/tennis/#"));
        assert_eq!(false, is_valid_topic("sport/tennis#"));
        assert_eq!(false, is_valid_topic("sport/tennis/#/ranking"));
        assert_eq!(false, is_valid_topic(""));
        assert_eq!(false, is_valid_topic("+"));
        assert_eq!(false, is_valid_topic("+/tennis/#"));
        assert_eq!(false, is_valid_topic("sport/+/player1"));
        assert_eq!(false, is_valid_topic("sport+"));
        assert_eq!(false, is_valid_topic(&"s".repeat(70000).to_string()));
    }

    #[test]
    fn check_valid_topic_filters() {
        assert_eq!(true, is_valid_topic_filter("a/b/c"));
        assert_eq!(true, is_valid_topic_filter("#"));
        assert_eq!(true, is_valid_topic_filter("/#"));
        assert_eq!(true, is_valid_topic_filter("sports/tennis/#"));
        assert_eq!(true, is_valid_topic_filter("+"));
        assert_eq!(true, is_valid_topic_filter("/+"));
        assert_eq!(true, is_valid_topic_filter("+/a"));
        assert_eq!(true, is_valid_topic_filter("+/tennis/#"));
        assert_eq!(true, is_valid_topic_filter("port/+/player1"));
    }

    #[test]
    fn check_invalid_topic_filters() {
        assert_eq!(false, is_valid_topic_filter(""));
        assert_eq!(false, is_valid_topic_filter("derp+"));
        assert_eq!(false, is_valid_topic_filter("derp+/"));
        assert_eq!(false, is_valid_topic_filter("derp#/"));
        assert_eq!(false, is_valid_topic_filter("#/a"));
        assert_eq!(false, is_valid_topic_filter("sport/tennis#"));
        assert_eq!(false, is_valid_topic_filter("sport/tennis/#/ranking"));
        assert_eq!(false, is_valid_topic_filter("sport+"));
        assert_eq!(false, is_valid_topic_filter(&"s".repeat(70000).to_string()));
    }

    #[test]
    fn check_topic_properties() {
        assert_eq!(true, compute_topic_filter_properties("a/b/c").is_valid);
        assert_eq!(true, compute_topic_filter_properties("#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("/#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("sports/tennis/#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("+").is_valid);
        assert_eq!(true, compute_topic_filter_properties("/+").is_valid);
        assert_eq!(true, compute_topic_filter_properties("+/a").is_valid);
        assert_eq!(true, compute_topic_filter_properties("+/tennis/#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("port/+/player1").is_valid);
        assert_eq!(false, compute_topic_filter_properties("").is_valid);
        assert_eq!(false, compute_topic_filter_properties("derp+").is_valid);
        assert_eq!(false, compute_topic_filter_properties("derp+/").is_valid);
        assert_eq!(false, compute_topic_filter_properties("derp#/").is_valid);
        assert_eq!(false, compute_topic_filter_properties("#/a").is_valid);
        assert_eq!(false, compute_topic_filter_properties("sport/tennis#").is_valid);
        assert_eq!(false, compute_topic_filter_properties("sport/tennis/#/ranking").is_valid);
        assert_eq!(false, compute_topic_filter_properties("sport+").is_valid);
        assert_eq!(false, compute_topic_filter_properties(&"s".repeat(70000).to_string()).is_valid);

        assert_eq!(false, compute_topic_filter_properties("a/b/c").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share//c").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/a").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/+/a").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/#/a").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/b/").is_shared);
        assert_eq!(true, compute_topic_filter_properties("$share/b//").is_shared);
        assert_eq!(true, compute_topic_filter_properties("$share/a/b").is_shared);
        assert_eq!(true, compute_topic_filter_properties("$share/a/b/c").is_shared);

        assert_eq!(false, compute_topic_filter_properties("a/b/c").has_wildcard);
        assert_eq!(false, compute_topic_filter_properties("/").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("#").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("+").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("a/+/+").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("a/b/#").has_wildcard);
    }
}
