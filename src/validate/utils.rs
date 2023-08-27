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
