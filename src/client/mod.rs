/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod implementation;

extern crate tokio;

use crate::*;
use crate::client::implementation::*;
use crate::spec::*;
use crate::spec::utils::*;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::runtime;
use tokio::sync::oneshot;
use crate::alias::OutboundAliasResolver;

use crate::spec::connect::ConnectPacket;
use crate::spec::puback::PubackPacket;
use crate::spec::pubrec::PubrecPacket;
use crate::spec::pubcomp::PubcompPacket;
use crate::spec::publish::PublishPacket;
use crate::spec::suback::SubackPacket;
use crate::spec::subscribe::SubscribePacket;
use crate::spec::unsuback::UnsubackPacket;
use crate::spec::unsubscribe::UnsubscribePacket;
use crate::validate::*;

#[derive(Debug, Default)]
pub struct PublishOptions {
    pub timeout_in_millis: Option<u32>,
}

#[derive(Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum Qos2Response {
    Pubrec(PubrecPacket),
    Pubcomp(PubcompPacket),
}

#[derive(Debug)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum PublishResponse {
    Qos0,
    Qos1(PubackPacket),
    Qos2(Qos2Response),
}

pub type PublishResult = Mqtt5Result<PublishResponse>;

pub type PublishResultFuture = dyn Future<Output = PublishResult>;

#[derive(Debug, Default)]
pub struct SubscribeOptions {
    pub timeout_in_millis: Option<u32>,
}

pub type SubscribeResult = Mqtt5Result<SubackPacket>;

pub type SubscribeResultFuture = dyn Future<Output = SubscribeResult>;

#[derive(Debug, Default)]
pub struct UnsubscribeOptions {
    pub timeout_in_millis: Option<u32>,
}

pub type UnsubscribeResult = Mqtt5Result<UnsubackPacket>;

pub type UnsubscribeResultFuture = dyn Future<Output = UnsubscribeResult>;

#[derive(Debug, Default)]
pub struct DisconnectOptions {
}

pub type DisconnectResult = Mqtt5Result<()>;

pub type DisconnectResultFuture = dyn Future<Output = DisconnectResult>;

#[derive(Default, Clone, PartialEq, Eq, Debug)]
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

impl fmt::Display for NegotiatedSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "  maximum_qos: {}", quality_of_service_to_str(self.maximum_qos))?;
        write!(f, "  session_expiry_interval: {}", self.session_expiry_interval)?;
        write!(f, "  receive_maximum_from_server: {}", self.receive_maximum_from_server)?;
        write!(f, "  maximum_packet_size_to_server: {}", self.maximum_packet_size_to_server)?;
        write!(f, "  topic_alias_maximum_to_server: {}", self.topic_alias_maximum_to_server)?;
        write!(f, "  server_keep_alive: {}", self.server_keep_alive)?;
        write!(f, "  retain_available: {}", self.retain_available)?;
        write!(f, "  wildcard_subscriptions_available: {}", self.wildcard_subscriptions_available)?;
        write!(f, "  subscription_identifiers_available: {}", self.subscription_identifiers_available)?;
        write!(f, "  shared_subscriptions_available: {}", self.shared_subscriptions_available)?;
        write!(f, "  rejoined_session: {}", self.rejoined_session)?;
        write!(f, "  client_id: {}", self.client_id)?;

        Ok(())
    }
}

impl From<oneshot::error::RecvError> for Mqtt5Error {
    fn from(_: oneshot::error::RecvError) -> Self {
        Mqtt5Error::OperationChannelReceiveError
    }
}

#[derive(Default)]
pub enum OfflineQueuePolicy {
    #[default]
    PreserveAll,
    PreserveAcknowledged,
    PreserveQos1PlusPublishes,
    PreserveNothing,
}

#[derive(Default)]
pub enum RejoinSessionPolicy {
    #[default]
    RejoinPostSuccess,
    RejoinAlways,
    RejoinNever
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ConnectionAttemptEvent {}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ConnectionSuccessEvent {
    pub connack: ConnackPacket,
    pub settings: NegotiatedSettings
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ConnectionFailureEvent {
    pub error: Mqtt5Error,
    pub connack: Option<ConnackPacket>,
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct DisconnectionEvent {
    pub error: Mqtt5Error,
    pub disconnect: Option<DisconnectPacket>,
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct StoppedEvent {}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct PublishReceivedEvent {
    pub publish: PublishPacket
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub(crate) enum ClientEventType {
    ConnectionAttempt,
    ConnectionSuccess,
    ConnectionFailure,
    Disconnection,
    Stopped,
    PublishReceived,
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum ClientEvent {
    ConnectionAttempt(ConnectionAttemptEvent),
    ConnectionSuccess(ConnectionSuccessEvent),
    ConnectionFailure(ConnectionFailureEvent),
    Disconnection(DisconnectionEvent),
    Stopped(StoppedEvent),
    PublishReceived(PublishReceivedEvent),
}

pub(crate) fn client_event_to_client_event_type(event: &ClientEvent) -> ClientEventType {
    match event {
        ClientEvent::ConnectionAttempt(_) => { ClientEventType::ConnectionAttempt },
        ClientEvent::ConnectionSuccess(_) => { ClientEventType::ConnectionSuccess },
        ClientEvent::ConnectionFailure(_) => { ClientEventType::ConnectionFailure },
        ClientEvent::Disconnection(_) => { ClientEventType::Disconnection },
        ClientEvent::Stopped(_) => { ClientEventType::Stopped },
        ClientEvent::PublishReceived(_) => { ClientEventType::PublishReceived },
    }
}

pub enum ClientEventListener {
    Channel(std::sync::mpsc::Sender<Arc<ClientEvent>>),
    Callback(Box<dyn Fn(Arc<ClientEvent>) -> () + Send + Sync>)
}

#[derive(Default)]
pub struct Mqtt5ClientOptions {
    pub connect : Option<Box<ConnectPacket>>,

    pub offline_queue_policy: OfflineQueuePolicy,
    pub rejoin_session_policy: RejoinSessionPolicy,

    pub connack_timeout_millis: u32,
    pub ping_timeout_millis: u32,

    pub default_event_listener: Option<ClientEventListener>,

    pub outbound_resolver: Option<Box<dyn OutboundAliasResolver + Send>>,
}

pub struct Mqtt5Client {
    operation_sender: tokio::sync::mpsc::Sender<OperationOptions>,

    listener_id_allocator: Mutex<u64>,
}

impl Mqtt5Client {
    pub fn new(config: Mqtt5ClientOptions, runtime_handle: &runtime::Handle) -> Arc<Mqtt5Client> {
        let (operation_sender, operation_receiver) = tokio::sync::mpsc::channel(100);

        spawn_client_impl(config, operation_receiver, &runtime_handle);

        Arc::new(Mqtt5Client {
            operation_sender,
            listener_id_allocator: Mutex::new(1),
        })
    }

    pub fn start(&self) -> Mqtt5Result<()> {
        client_lifecycle_operation_body!(Start, self)
    }

    pub fn stop(&self, packet: DisconnectPacket, options: DisconnectOptions) -> Pin<Box<DisconnectResultFuture>> {
        client_mqtt_operation_body!(self, Stop, DisconnectOptionsInternal, packet, Disconnect, options)
    }

    pub fn close(&self) -> Mqtt5Result<()> {
        client_lifecycle_operation_body!(Shutdown, self)
    }

    pub fn publish(&self, packet: PublishPacket, options: PublishOptions) -> Pin<Box<PublishResultFuture>> {
        client_mqtt_operation_body!(self, Publish, PublishOptionsInternal, packet, Publish, options)
    }

    pub fn subscribe(&self, packet: SubscribePacket, options: SubscribeOptions) -> Pin<Box<SubscribeResultFuture>> {
        client_mqtt_operation_body!(self, Subscribe, SubscribeOptionsInternal, packet, Subscribe, options)
    }

    pub fn unsubscribe(&self, packet: UnsubscribePacket, options: UnsubscribeOptions) -> Pin<Box<UnsubscribeResultFuture>> {
        client_mqtt_operation_body!(self, Unsubscribe, UnsubscribeOptionsInternal, packet, Unsubscribe, options)
    }

    pub fn add_event_listener(&self, listener: ClientEventListener) -> Mqtt5Result<u64> {
        let mut listener_id : u64 = 1;
        let mut current_id = self.listener_id_allocator.lock().unwrap();
        listener_id = *current_id;
        *current_id += 1;

        match self.operation_sender.try_send(OperationOptions::AddListener(listener_id, listener))
        {
            Err(_) => Err(Mqtt5Error::OperationChannelSendError),
            _ => Ok(listener_id),
        }
    }

    pub fn remove_event_listener(&self, listener_id: u64) -> Mqtt5Result<()> {
        match self.operation_sender.try_send(OperationOptions::RemoveListener(listener_id))
        {
            Err(_) => Err(Mqtt5Error::OperationChannelSendError),
            _ => Ok(()),
        }
    }
}
