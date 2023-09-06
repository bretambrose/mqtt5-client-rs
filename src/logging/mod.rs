/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::spec::*;

use std::fmt;
use std::fmt::Write;

impl fmt::Display for UserProperty {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {}) ", self.name, self.value)
    }
}

pub(crate) fn create_user_properties_log_string(properties: &Vec<UserProperty>) -> String {
    let mut val : String = "Some([ ".to_string();
    for property in properties {
        write!(&mut val, "(\"{}\", \"{}\") ", property.name, property.value).ok();
    }
    write!(&mut val, "])").ok();
    val
}

macro_rules! log_primitive_value {
    ($integral_value: expr, $formatter: expr, $log_field: expr) => {
        write!($formatter, "  {}: {}\n", $log_field, $integral_value)?;
    };
}

pub(crate) use log_primitive_value;

macro_rules! log_optional_primitive_value {
    ($optional_integral_value: expr, $formatter: expr, $log_field: expr, $value: ident) => {
        if let Some($value) = &$optional_integral_value {
            write!($formatter, "  {}: {}\n", $log_field, $value)?;
        }
    };
}

pub(crate) use log_optional_primitive_value;

macro_rules! log_enum {
    ($enum_value: expr, $formatter: expr, $log_field: expr, $converter: ident) => {
        write!($formatter, "  {}: {}\n", $log_field, $converter($enum_value))?;
    };
}

pub(crate) use log_enum;

macro_rules! log_optional_enum {
    ($optional_enum_value: expr, $formatter: expr, $log_field: expr, $value:ident, $converter: ident) => {
        if let Some($value) = &$optional_enum_value {
            write!($formatter, "  {}: {}\n", $log_field, $converter(*$value))?;
        }
    };
}

pub(crate) use log_optional_enum;

macro_rules! log_string {
    ($value: expr, $formatter: expr, $log_field: expr) => {
        write!($formatter, "  {}: {}\n", $log_field, $value)?;
    };
}

pub(crate) use log_string;

macro_rules! log_optional_string {
    ($optional_string: expr, $formatter: expr, $log_field: expr, $value:ident) => {
        if let Some($value) = &$optional_string {
            write!($formatter, "  {}: {}\n", $log_field, $value)?;
        }
    };
}

pub(crate) use log_optional_string;

macro_rules! log_optional_string_sensitive {
    ($optional_string: expr, $formatter: expr, $log_field: expr) => {
        if let Some(_) = &$optional_string {
            write!($formatter, "  {}: <...redacted>\n", $log_field)?;
        }
    };
}

pub(crate) use log_optional_string_sensitive;

macro_rules! log_optional_binary_data {
    ($optional_data: expr, $formatter: expr, $log_field: expr, $value:ident) => {
        if let Some($value) = &$optional_data {
            write!($formatter, "  {}: <{} Bytes>\n",  $log_field, $value.len())?;
        }
    };
}

pub(crate) use log_optional_binary_data;

macro_rules! log_optional_binary_data_sensitive {
    ($optional_data: expr, $formatter: expr, $log_field: expr) => {
        if let Some(_) = &$optional_data {
            write!($formatter, "  {}: <...redacted>\n", $log_field)?;
        }
    };
}

pub(crate) use log_optional_binary_data_sensitive;

macro_rules! log_user_properties {
    ($user_properties: expr, $formatter: expr, $value:ident) => {
        if let Some($value) = &$user_properties {
            write!($formatter, "  UserProperties: {}\n", create_user_properties_log_string($value))?;
        }
    };
}

pub(crate) use log_user_properties;