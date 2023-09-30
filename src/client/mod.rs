/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod implementation;

extern crate tokio;

use crate::*;
use crate::client::implementation::*;
use crate::spec::*;

use std::future::Future;
use std::pin::Pin;
use tokio::runtime;
use tokio::sync::oneshot;

use crate::spec::connect::ConnectPacket;
use crate::spec::puback::PubackPacket;
use crate::spec::pubrec::PubrecPacket;
use crate::spec::pubcomp::PubcompPacket;
use crate::spec::publish::PublishPacket;
use crate::spec::suback::SubackPacket;
use crate::spec::subscribe::SubscribePacket;
use crate::spec::unsuback::UnsubackPacket;
use crate::spec::unsubscribe::UnsubscribePacket;

#[derive(Debug, Default)]
pub struct PublishOptions {
}

#[derive(Debug)]
pub enum Qos2Response {
    Pubrec(PubrecPacket),
    Pubcomp(PubcompPacket),
}

#[derive(Debug)]
pub enum PublishResponse {
    Qos0,
    Qos1(PubackPacket),
    Qos2(Qos2Response),
}

pub type PublishResult = Mqtt5Result<PublishResponse>;

pub type PublishResultFuture = dyn Future<Output = PublishResult>;

#[derive(Debug, Default)]
pub struct SubscribeOptions {
}

pub type SubscribeResult = Mqtt5Result<SubackPacket>;

pub type SubscribeResultFuture = dyn Future<Output = SubscribeResult>;

#[derive(Debug, Default)]
pub struct UnsubscribeOptions {
}

pub type UnsubscribeResult = Mqtt5Result<UnsubackPacket>;

pub type UnsubscribeResultFuture = dyn Future<Output = UnsubscribeResult>;

#[derive(Debug, Default)]
pub struct DisconnectOptions {
}

pub type DisconnectResult = Mqtt5Result<()>;

pub type DisconnectResultFuture = dyn Future<Output = DisconnectResult>;

#[derive(Default)]
pub struct NegotiatedSettings {

    /// The maximum QoS allowed between the server and client.
    pub maximum_qos : QualityOfService,

    /// The amount of time in seconds the server will retain the session after a disconnect.
    pub session_expiry_interval : u32,

    /// The number of QoS 1 and QoS2 publications the server is willing to process concurrently.
    pub receive_maximum_from_server : u16,

    /// The maximum packet size the server is willing to accept.
    pub maximum_packet_size_to_server : u32,

    /// The highest value that the server will accept as a Topic Alias sent by the client.
    pub topic_alias_maximum_to_server : u16,

    /// The amount of time in seconds before the server will disconnect the client for inactivity.
    pub server_keep_alive : u16,

    /// Whether or not the server supports retained messages.
    pub retain_available : bool,

    /// Whether or not the server supports wildcard subscriptions.
    pub wildcard_subscriptions_available : bool,

    /// Whether or not the server supports subscription identifiers.
    pub subscription_identifiers_available : bool,

    /// Whether or not the server supports shared subscriptions.
    pub shared_subscriptions_available : bool,

    /// Whether or not the client has rejoined an existing session.
    pub rejoined_session : bool,

    /// Client id in use for the current connection
    pub client_id : String
}

impl From<oneshot::error::RecvError> for Mqtt5Error {
    fn from(_: oneshot::error::RecvError) -> Self {
        Mqtt5Error::OperationChannelReceiveError
    }
}

#[derive(Default)]
pub struct Mqtt5ClientOptions {
    pub connect : Option<Box<ConnectPacket>>
}

pub struct Mqtt5Client {
    operation_sender: tokio::sync::mpsc::Sender<OperationOptions>,
}

impl Mqtt5Client {
    pub fn new(config: Mqtt5ClientOptions, runtime_handle: &runtime::Handle) -> Mqtt5Client {
        let (operation_sender, operation_receiver) = tokio::sync::mpsc::channel(100);

        spawn_client_impl(config, operation_receiver, &runtime_handle);

        Mqtt5Client { operation_sender }
    }

    pub fn start(&self) -> Mqtt5Result<()> {
        client_lifecycle_operation_body!(Start, self)
    }

    pub fn stop(&self, packet: &DisconnectPacket, options: DisconnectOptions) -> Pin<Box<DisconnectResultFuture>> {
        client_mqtt_operation_body!(self, Stop, DisconnectOptionsInternal, packet, Disconnect, options)
    }

    pub fn close(&self) -> Mqtt5Result<()> {
        client_lifecycle_operation_body!(Shutdown, self)
    }

    pub fn publish(&self, packet: &PublishPacket, options: PublishOptions) -> Pin<Box<PublishResultFuture>> {
        client_mqtt_operation_body!(self, Publish, PublishOptionsInternal, packet, Publish, options)
    }

    pub fn subscribe(&self, packet: &SubscribePacket, options: SubscribeOptions) -> Pin<Box<SubscribeResultFuture>> {
        client_mqtt_operation_body!(self, Subscribe, SubscribeOptionsInternal, packet, Subscribe, options)
    }

    pub fn unsubscribe(&self, packet: &UnsubscribePacket, options: UnsubscribeOptions) -> Pin<Box<UnsubscribeResultFuture>> {
        client_mqtt_operation_body!(self, Unsubscribe, UnsubscribeOptionsInternal, packet, Unsubscribe, options)
    }
}
