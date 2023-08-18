/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;

pub trait OutboundAliasResolver {
    fn get_maximum_alias_value(&self) -> u32;

    fn reset_for_new_connection(&mut self, max_aliases : u16);

    fn compute_alias_for_outbound_publish(&mut self, packet: &PublishPacket) -> Mqtt5Result<(Option<String>, Option<u16>)>;

    fn validate_alias(&self, packet: &PublishPacket) -> Mqtt5Result<()>;
}

impl<T: OutboundAliasResolver + ?Sized> OutboundAliasResolver for Box<T> {
    fn get_maximum_alias_value(&self) -> u32 { self.get_maximum_alias_value() }

    fn reset_for_new_connection(&mut self, max_aliases : u16) { self.reset_for_new_connection(max_aliases); }

    fn compute_alias_for_outbound_publish(&mut self, packet: &PublishPacket) -> Mqtt5Result<(Option<String>, Option<u16>)> {
        self.compute_alias_for_outbound_publish(packet)
    }

    fn validate_alias(&self, packet: &PublishPacket) -> Mqtt5Result<()> {
        self.validate_alias(packet)
    }
}


pub struct NullOutboundAliasResolver {

}

impl NullOutboundAliasResolver {
    pub fn new() -> NullOutboundAliasResolver {
        NullOutboundAliasResolver {}
    }
}

impl OutboundAliasResolver for NullOutboundAliasResolver {
    fn get_maximum_alias_value(&self) -> u32 { 0 }

    fn reset_for_new_connection(&mut self, _ : u16) {}

    fn compute_alias_for_outbound_publish(&mut self, packet: &PublishPacket) -> Mqtt5Result<(Option<String>, Option<u16>)> {
        Ok((Some(packet.topic.clone()), None))
    }

    fn validate_alias(&self, packet: &PublishPacket) -> Mqtt5Result<()> {
        if let Some(_) = packet.topic_alias {
            return Err(Mqtt5Error::OutboundTopicAliasNotAllowed);
        }

        Ok(())
    }
}

pub trait InboundAliasResolver {
    fn get_maximum_alias_value(&self) -> u32;

    fn reset_for_new_connection(&mut self);

    fn on_publish_received(&mut self, packet: &mut PublishPacket) -> Mqtt5Result<()>;
}

impl<T: InboundAliasResolver + ?Sized> InboundAliasResolver for Box<T> {
    fn get_maximum_alias_value(&self) -> u32 { self.get_maximum_alias_value() }

    fn reset_for_new_connection(&mut self) { self.reset_for_new_connection() }

    fn on_publish_received(&mut self, packet: &mut PublishPacket) -> Mqtt5Result<()> {
        self.on_publish_received(packet)
    }
}

pub struct NullInboundAliasResolver {

}

impl NullInboundAliasResolver {
    pub fn new() -> NullInboundAliasResolver {
        NullInboundAliasResolver {}
    }
}

impl InboundAliasResolver for NullInboundAliasResolver {
    fn get_maximum_alias_value(&self) -> u32 { 0 }

    fn reset_for_new_connection(&mut self) {}

    fn on_publish_received(&mut self, packet: &mut PublishPacket) -> Mqtt5Result<()> {
        if let Some(_) = packet.topic_alias {
            return Err(Mqtt5Error::InboundTopicAliasNotAllowed);
        }

        Ok(())
    }
}
