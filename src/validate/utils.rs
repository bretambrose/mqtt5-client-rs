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

macro_rules! validate_binary_length {
    ($binary_expr: expr, $error: ident) => {
        if $binary_expr.len() > MAXIMUM_BINARY_PROPERTY_LENGTH {
            return Err(Mqtt5Error::$error);
        }
    };
}

pub(crate) use validate_binary_length;

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
