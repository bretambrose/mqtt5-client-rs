/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod internal;

extern crate tokio;

use crate::*;
use crate::client::internal::*;
use crate::client::internal::tokio_impl::*;
use crate::spec::*;
use crate::spec::utils::*;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::runtime;
use tokio::sync::oneshot;
use crate::alias::OutboundAliasResolver;
use crate::operation::OperationalStateConfig;

use crate::spec::connect::ConnectPacket;
use crate::spec::disconnect::validate_disconnect_packet_outbound;
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
pub enum StopMode {
    #[default]
    Soft,
    Hard
}

#[derive(Debug, Default)]
pub struct StopOptions {
    pub disconnect: Option<DisconnectPacket>,
    pub mode: StopMode
}

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
#[cfg_attr(test, derive(Eq, PartialEq, Copy, Clone))]
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
    PostSuccess,
    Always,
    Never
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
pub enum ClientEvent {
    ConnectionAttempt(ConnectionAttemptEvent),
    ConnectionSuccess(ConnectionSuccessEvent),
    ConnectionFailure(ConnectionFailureEvent),
    Disconnection(DisconnectionEvent),
    Stopped(StoppedEvent),
    PublishReceived(PublishReceivedEvent),
}

pub enum ClientEventListener {
    Channel(std::sync::mpsc::Sender<Arc<ClientEvent>>),
    Callback(Box<dyn Fn(Arc<ClientEvent>) -> () + Send + Sync>)
}

macro_rules! client_lifecycle_operation_body {
    ($lifeycle_operation:ident, $self:ident) => {{
        $self.user_state.try_send(OperationOptions::$lifeycle_operation())
    }};
}

macro_rules! client_mqtt_operation_body {
    ($self:ident, $operation_type:ident, $options_internal_type: ident, $packet_name: ident, $packet_type: ident, $options_value: expr) => ({
        let boxed_packet = Box::new(MqttPacket::$packet_type($packet_name));
        if let Err(error) = validate_packet_outbound(&*boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        let (response_sender, rx) = oneshot::channel();
        let internal_options = $options_internal_type {
            options : $options_value,
            response_sender : Some(response_sender)
        };
        let send_result = $self.user_state.try_send(OperationOptions::$operation_type(boxed_packet, internal_options));
        Box::pin(async move {
            match send_result {
                Err(error) => { Err(error) }
                _ => {
                    rx.await?
                }
            }
        })
    })
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
    user_state: UserRuntimeState,

    listener_id_allocator: Mutex<u64>,
}

impl Mqtt5Client {
    pub fn new(mut config: Mqtt5ClientOptions, runtime_handle: &runtime::Handle) -> Mqtt5Client {
        let (user_state, internal_state) = create_runtime_states();

        let connect = config.connect.take().unwrap_or(Box::new(ConnectPacket{ ..Default::default() }));

        let state_config = OperationalStateConfig {
            connect,
            base_timestamp: Instant::now(),
            offline_queue_policy: config.offline_queue_policy,
            rejoin_session_policy: config.rejoin_session_policy,
            connack_timeout_millis: config.connack_timeout_millis,
            ping_timeout_millis: config.ping_timeout_millis,
            outbound_resolver: config.outbound_resolver.take(),
        };

        let default_listener = config.default_event_listener.take();
        let client_impl = Mqtt5ClientImpl::new(state_config, default_listener);

        spawn_client_impl(client_impl, internal_state, &runtime_handle);

        Mqtt5Client {
            user_state,
            listener_id_allocator: Mutex::new(1),
        }
    }

    pub fn start(&self) -> Mqtt5Result<()> {
        client_lifecycle_operation_body!(Start, self)
    }

    pub fn stop(&self, options: StopOptions) -> Mqtt5Result<()> {
        if let Some(disconnect) = &options.disconnect {
            validate_disconnect_packet_outbound(disconnect)?;
        }

        let mut stop_options_internal = StopOptionsInternal {
            mode: options.mode,
            ..Default::default()
        };

        if options.disconnect.is_some() {
            stop_options_internal.disconnect = Some(Box::new(MqttPacket::Disconnect(options.disconnect.unwrap())));
        }

        self.user_state.try_send(OperationOptions::Stop(stop_options_internal))
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
        let mut current_id = self.listener_id_allocator.lock().unwrap();
        let listener_id = *current_id;
        *current_id += 1;

        self.user_state.try_send(OperationOptions::AddListener(listener_id, listener))?;

        Ok(listener_id)
    }

    pub fn remove_event_listener(&self, listener_id: u64) -> Mqtt5Result<()> {
        self.user_state.try_send(OperationOptions::RemoveListener(listener_id))
    }
}

