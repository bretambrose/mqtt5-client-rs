/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

macro_rules! validate_string_length {
    ($string_expr: expr, $error: ident) => {
        if $string_expr.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
            return Err(Mqtt5Error::$error);
        }
    };
}

pub(crate) use validate_string_length;

macro_rules! validate_optional_string_length {
    ($value_name: ident, $optional_string_expr: expr, $error: ident) => {
        if let Some($value_name) = $optional_string_expr {
            if $value_name.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
                return Err(Mqtt5Error::$error);
            }
        }
    };
}

pub(crate) use validate_optional_string_length;

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

macro_rules! validate_user_properties {
    ($value_name: ident, $optional_properties_expr: expr, $error: ident) => {
        if let Some($value_name) = $optional_properties_expr {
            if let Err(_) = validate_user_properties(&$value_name) {
                return Err(Mqtt5Error::$error);
            }
        }
    };
}

pub(crate) use validate_user_properties;

macro_rules! validate_ack_outbound {
    ($function_name: ident, $packet_type: ident, $error: ident) => {
        pub(crate) fn $function_name(packet: &$packet_type) -> Mqtt5Result<()> {

            validate_optional_string_length!(reason, &packet.reason_string, $error);
            validate_user_properties!(properties, &packet.user_properties, $error);

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

macro_rules! test_ack_validate_failure_internal_packet_id_zero {
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

pub(crate) use test_ack_validate_failure_internal_packet_id_zero;

pub(crate) fn is_valid_topic(topic: &str) -> bool {
    if topic.len() == 0 {
        return false;
    }

    if topic.contains(['#', '+']) {
        return false;
    }

    true
}

pub(crate) fn is_valid_topic_filter(topic: &str) -> bool {
    if topic.len() == 0 {
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
    }
}
