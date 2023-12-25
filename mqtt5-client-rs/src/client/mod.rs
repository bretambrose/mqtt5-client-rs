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
use std::time::Duration;
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

#[derive(Debug, Default, Copy, Clone)]
pub struct PublishOptions {
    pub(crate) timeout: Option<Duration>,
}

pub struct PublishOptionsBuilder {
    options: PublishOptions
}

impl PublishOptionsBuilder {
    pub fn new() -> Self {
        PublishOptionsBuilder {
            options: PublishOptions{
                ..Default::default()
            }
        }
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.options.timeout = Some(timeout);
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = Some(timeout);
        self
    }

    pub fn build(&self) -> PublishOptions {
        self.options
    }
}

impl Default for PublishOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
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

#[derive(Debug, Default, Copy, Clone)]
pub struct SubscribeOptions {
    pub(crate) timeout: Option<Duration>,
}

pub struct SubscribeOptionsBuilder {
    options: SubscribeOptions
}

impl SubscribeOptionsBuilder {
    pub fn new() -> Self {
        SubscribeOptionsBuilder {
            options: SubscribeOptions {
                ..Default::default()
            }
        }
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.options.timeout = Some(timeout);
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = Some(timeout);
        self
    }

    pub fn build(&self) -> SubscribeOptions {
        self.options
    }
}

impl Default for SubscribeOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub type SubscribeResult = Mqtt5Result<SubackPacket>;

pub type SubscribeResultFuture = dyn Future<Output = SubscribeResult>;

#[derive(Debug, Default, Copy, Clone)]
pub struct UnsubscribeOptions {
    pub(crate) timeout: Option<Duration>,
}

pub struct UnsubscribeOptionsBuilder {
    options: UnsubscribeOptions
}

impl UnsubscribeOptionsBuilder {
    pub fn new() -> Self {
        UnsubscribeOptionsBuilder {
            options: UnsubscribeOptions {
                ..Default::default()
            }
        }
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.options.timeout = Some(timeout);
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = Some(timeout);
        self
    }

    pub fn build(&self) -> UnsubscribeOptions {
        self.options
    }
}

impl Default for UnsubscribeOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub type UnsubscribeResult = Mqtt5Result<UnsubackPacket>;

pub type UnsubscribeResultFuture = dyn Future<Output = UnsubscribeResult>;

#[derive(Debug, Default, Clone)]
pub struct StopOptions {
    pub(crate) disconnect: Option<DisconnectPacket>,
}

pub struct StopOptionsBuilder {
    options: StopOptions
}

impl StopOptionsBuilder {
    pub fn new() -> Self {
        StopOptionsBuilder {
            options: StopOptions {
                ..Default::default()
            }
        }
    }

    pub fn set_disconnect_packet(&mut self, disconnect: DisconnectPacket)  {
        self.options.disconnect = Some(disconnect);
    }

    pub fn with_disconnect_packet(mut self, disconnect: DisconnectPacket) -> Self {
        self.options.disconnect = Some(disconnect);
        self
    }

    pub fn build(&self) -> StopOptions {
        self.options.clone()
    }
}

impl Default for StopOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
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

#[derive(Default, Clone)]
#[cfg_attr(test, derive(Eq, PartialEq, Copy))]
pub enum OfflineQueuePolicy {
    #[default]
    PreserveAll,
    PreserveAcknowledged,
    PreserveQos1PlusPublishes,
    PreserveNothing,
}

#[derive(Default, Clone)]
pub enum RejoinSessionPolicy {
    #[default]
    PostSuccess,
    Always,
    Never
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ConnectionAttemptEvent {}

#[cfg_attr(test, derive(Eq, PartialEq))]
#[derive(Debug)]
pub struct ConnectionSuccessEvent {
    pub connack: ConnackPacket,
    pub settings: NegotiatedSettings
}

#[cfg_attr(test, derive(Eq, PartialEq))]
#[derive(Debug)]
pub struct ConnectionFailureEvent {
    pub error: Mqtt5Error,
    pub connack: Option<ConnackPacket>,
}

#[cfg_attr(test, derive(Eq, PartialEq))]
#[derive(Debug)]
pub struct DisconnectionEvent {
    pub error: Mqtt5Error,
    pub disconnect: Option<DisconnectPacket>,
}

#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct StoppedEvent {}

#[cfg_attr(test, derive(Eq, PartialEq))]
#[derive(Debug)]
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
    Callback(Box<dyn Fn(Arc<ClientEvent>) + Send + Sync>)
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
                Err(error) => {
                    Err(error)
                }
                _ => {
                    rx.await?
                }
            }
        })
    })
}

/// Configuration options that will determine packet field values for the CONNECT packet sent out
/// by the client on each connection attempt.  Almost equivalent to ConnectPacket, but there are a
/// few differences that make exposing a ConnectPacket directly awkward and potentially misleading.
///
/// Auth-related fields are not yet exposed because we don't support authentication exchanges yet.
#[derive(Default, Clone)]
pub struct ConnectOptions {

    /// The maximum time interval, in seconds, that is permitted to elapse between the point at which the client
    /// finishes transmitting one MQTT packet and the point it starts sending the next.  The client will use
    /// PINGREQ packets to maintain this property.
    ///
    /// If the responding CONNACK contains a keep alive property value, then that is the negotiated keep alive value.
    /// Otherwise, the keep alive sent by the client is the negotiated value.
    ///
    /// See [MQTT5 Keep Alive](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901045)
    ///
    /// If the final negotiated value is 0, then that means no keep alive will be used.  Such a
    /// state is not advised due to scenarios where TCP connections can be invisibly dropped by
    /// routers/firewalls within the full connection circuit.
    pub(crate) keep_alive_interval_seconds: Option<u16>,

    /// Configuration value that determines how the client will attempt to rejoin sessions
    pub(crate) rejoin_session_policy: RejoinSessionPolicy,

    /// A unique string identifying the client to the server.  Used to restore session state between connections.
    ///
    /// If left empty, the broker will auto-assign a unique client id.  When reconnecting, the mqtt5 client will
    /// always use the auto-assigned client id.
    ///
    /// See [MQTT5 Client Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901059)
    pub(crate) client_id: Option<String>,

    /// A string value that the server may use for client authentication and authorization.
    ///
    /// See [MQTT5 User Name](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901071)
    username: Option<String>,

    /// Opaque binary data that the server may use for client authentication and authorization.
    ///
    /// See [MQTT5 Password](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901072)
    password: Option<Vec<u8>>,

    /// A time interval, in seconds, that the client requests the server to persist this connection's MQTT session state
    /// for.  Has no meaning if the client has not been configured to rejoin sessions.  Must be non-zero in order to
    /// successfully rejoin a session.
    ///
    /// If the responding CONNACK contains a session expiry property value, then that is the negotiated session expiry
    /// value.  Otherwise, the session expiry sent by the client is the negotiated value.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901048)
    pub(crate) session_expiry_interval_seconds: Option<u32>,

    /// If set to true, requests that the server send response information in the subsequent CONNACK.  This response
    /// information may be used to set up request-response implementations over MQTT, but doing so is outside
    /// the scope of the MQTT5 spec and client.
    ///
    /// See [MQTT5 Request Response Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901052)
    request_response_information: Option<bool>,

    /// If set to true, requests that the server send additional diagnostic information (via response string or
    /// user properties) in DISCONNECT or CONNACK packets from the server.
    ///
    /// See [MQTT5 Request Problem Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901053)
    request_problem_information: Option<bool>,

    /// Notifies the server of the maximum number of in-flight Qos 1 and 2 messages the client is willing to handle.  If
    /// omitted, then no limit is requested.
    ///
    /// See [MQTT5 Receive Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901049)
    receive_maximum: Option<u16>,

    /// Maximum number of topic aliases that the client will accept for incoming publishes.  An inbound topic alias larger than
    /// this number is a protocol error.  If this value is not specified, the client does not support inbound topic
    /// aliasing.
    ///
    /// See [MQTT5 Topic Alias Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901051)
    pub(crate) topic_alias_maximum: Option<u16>,

    /// Notifies the server of the maximum packet size the client is willing to handle.  If
    /// omitted, then no limit beyond the natural limits of MQTT packet size is requested.
    ///
    /// See [MQTT5 Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901050)
    pub(crate) maximum_packet_size_bytes: Option<u32>,

    /// A time interval, in seconds, that the server should wait (for a session reconnection) before sending the
    /// will message associated with the connection's session.  If omitted, the server will send the will when the
    /// associated session is destroyed.  If the session is destroyed before a will delay interval has elapsed, then
    /// the will must be sent at the time of session destruction.
    ///
    /// See [MQTT5 Will Delay Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901062)
    will_delay_interval_seconds: Option<u32>,

    /// The definition of a message to be published when the connection's session is destroyed by the server or when
    /// the will delay interval has elapsed, whichever comes first.  If undefined, then nothing will be sent.
    ///
    /// See [MQTT5 Will](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901040)
    ///
    /// TODO: consider making this a builder function to allow dynamic will construction
    will: Option<PublishPacket>,

    /// Set of MQTT5 user properties to include with all CONNECT packets.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901054)
    user_properties: Option<Vec<UserProperty>>,
}

impl ConnectOptions {
    pub(crate) fn to_connect_packet(&self, connected_previously: bool) -> ConnectPacket {
        let clean_start =
            match self.rejoin_session_policy {
                RejoinSessionPolicy::PostSuccess => {
                    !connected_previously
                }
                RejoinSessionPolicy::Always => {
                    false
                }
                RejoinSessionPolicy::Never => {
                    true
                }
            };

        ConnectPacket {
            keep_alive_interval_seconds: self.keep_alive_interval_seconds.unwrap_or(0),
            clean_start,
            client_id: self.client_id.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            session_expiry_interval_seconds: self.session_expiry_interval_seconds,
            request_response_information: self.request_response_information,
            request_problem_information: self.request_problem_information,
            receive_maximum: self.receive_maximum,
            topic_alias_maximum: self.topic_alias_maximum,
            maximum_packet_size_bytes: self.maximum_packet_size_bytes,
            authentication_method: None,
            authentication_data: None,
            will_delay_interval_seconds: self.will_delay_interval_seconds,
            will: self.will.clone(),
            user_properties: self.user_properties.clone(),
        }
    }
}

pub struct ConnectOptionsBuilder {
    options: ConnectOptions
}

impl ConnectOptionsBuilder {
    pub fn new() -> Self {
        ConnectOptionsBuilder {
            options: ConnectOptions {
                ..Default::default()
            }
        }
    }

    pub fn with_keep_alive_interval_seconds(mut self, keep_alive: u16) -> Self {
        self.options.keep_alive_interval_seconds = Some(keep_alive);
        self
    }

    pub fn set_keep_alive_interval_seconds(&mut self, keep_alive: u16) {
        self.options.keep_alive_interval_seconds = Some(keep_alive);
    }

    pub fn with_rejoin_session_policy(mut self, policy: RejoinSessionPolicy) -> Self {
        self.options.rejoin_session_policy = policy;
        self
    }

    pub fn set_rejoin_session_policy(&mut self, policy: RejoinSessionPolicy) {
        self.options.rejoin_session_policy = policy;
    }

    pub fn with_client_id(mut self, client_id: &str) -> Self {
        self.options.client_id = Some(client_id.to_string());
        self
    }

    pub fn set_client_id(&mut self, client_id: &str) {
        self.options.client_id = Some(client_id.to_string());
    }

    pub fn with_username(mut self, username: &str) -> Self {
        self.options.username = Some(username.to_string());
        self
    }

    pub fn set_username(&mut self, username: &str) {
        self.options.username = Some(username.to_string());
    }

    pub fn with_password(mut self, password: &[u8]) -> Self {
        self.options.password = Some(password.to_vec());
        self
    }

    pub fn set_password(&mut self, password: &[u8]) {
        self.options.password = Some(password.to_vec());
    }

    pub fn with_session_expiry_interval_seconds(mut self, session_expiry_interval_seconds: u32) -> Self {
        self.options.session_expiry_interval_seconds = Some(session_expiry_interval_seconds);
        self
    }

    pub fn set_session_expiry_interval_seconds(&mut self, session_expiry_interval_seconds: u32) {
        self.options.session_expiry_interval_seconds = Some(session_expiry_interval_seconds);
    }

    pub fn with_request_response_information(mut self, request_response_information: bool) -> Self {
        self.options.request_response_information = Some(request_response_information);
        self
    }

    pub fn set_request_response_information(&mut self, request_response_information: bool) {
        self.options.request_response_information = Some(request_response_information);
    }

    pub fn with_request_problem_information(mut self, request_problem_information: bool) -> Self {
        self.options.request_problem_information = Some(request_problem_information);
        self
    }

    pub fn set_request_problem_information(&mut self, request_problem_information: bool) {
        self.options.request_problem_information = Some(request_problem_information);
    }

    pub fn with_receive_maximum(mut self, receive_maximum: u16) -> Self {
        self.options.receive_maximum = Some(receive_maximum);
        self
    }

    pub fn set_receive_maximum(&mut self, receive_maximum: u16) {
        self.options.receive_maximum = Some(receive_maximum);
    }

    pub fn with_topic_alias_maximum(mut self, topic_alias_maximum: u16) -> Self {
        self.options.topic_alias_maximum = Some(topic_alias_maximum);
        self
    }

    pub fn set_topic_alias_maximum(&mut self, topic_alias_maximum: u16) {
        self.options.topic_alias_maximum = Some(topic_alias_maximum);
    }

    pub fn with_maximum_packet_size_bytes(mut self, maximum_packet_size_bytes: u32) -> Self {
        self.options.maximum_packet_size_bytes = Some(maximum_packet_size_bytes);
        self
    }

    pub fn set_maximum_packet_size_bytes(&mut self, maximum_packet_size_bytes: u32) {
        self.options.maximum_packet_size_bytes = Some(maximum_packet_size_bytes);
    }

    pub fn with_will_delay_interval_seconds(mut self, will_delay_interval_seconds: u32) -> Self {
        self.options.will_delay_interval_seconds = Some(will_delay_interval_seconds);
        self
    }

    pub fn set_will_delay_interval_seconds(&mut self, will_delay_interval_seconds: u32) {
        self.options.will_delay_interval_seconds = Some(will_delay_interval_seconds);
    }

    pub fn with_will(mut self, will: PublishPacket) -> Self {
        self.options.will = Some(will);
        self
    }

    pub fn set_will(&mut self, will: PublishPacket) {
        self.options.will = Some(will);
    }

    pub fn with_user_properties(mut self, user_properties: Vec<UserProperty>) -> Self {
        self.options.user_properties = Some(user_properties);
        self
    }

    pub fn set_user_properties(&mut self, user_properties: Vec<UserProperty>) {
        self.options.user_properties = Some(user_properties);
    }

    pub fn build(&self) -> ConnectOptions {
        self.options.clone()
    }
}

impl Default for ConnectOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
pub struct Mqtt5ClientOptions {
    pub(crate) connect_options : Option<ConnectOptions>,

    pub(crate) offline_queue_policy: OfflineQueuePolicy,

    pub(crate) connack_timeout: Duration,
    pub(crate) ping_timeout: Duration,

    pub(crate) default_event_listener: Option<ClientEventListener>,

    pub(crate) outbound_alias_resolver: Option<Box<dyn OutboundAliasResolver + Send>>,
}

pub struct Mqtt5ClientOptionsBuilder {
    options: Mqtt5ClientOptions
}

impl Mqtt5ClientOptionsBuilder {
    pub fn new() -> Self {
        Mqtt5ClientOptionsBuilder {
            options: Mqtt5ClientOptions {
                ..Default::default()
            }
        }
    }

    pub fn with_connect_options(mut self, connect_options: ConnectOptions) -> Self {
        self.options.connect_options = Some(connect_options);
        self
    }

    pub fn set_user_properties(&mut self, connect_options: ConnectOptions) {
        self.options.connect_options = Some(connect_options);
    }

    pub fn with_offline_queue_policy(mut self, offline_queue_policy: OfflineQueuePolicy) -> Self {
        self.options.offline_queue_policy = offline_queue_policy;
        self
    }

    pub fn set_offline_queue_policy(&mut self, offline_queue_policy: OfflineQueuePolicy) {
        self.options.offline_queue_policy = offline_queue_policy;
    }

    pub fn with_connack_timeout(mut self, connack_timeout: Duration) -> Self {
        self.options.connack_timeout = connack_timeout;
        self
    }

    pub fn set_connack_timeout(&mut self, connack_timeout: Duration) {
        self.options.connack_timeout = connack_timeout;
    }

    pub fn with_ping_timeout(mut self, ping_timeout: Duration) -> Self {
        self.options.ping_timeout = ping_timeout;
        self
    }

    pub fn set_ping_timeout(&mut self, ping_timeout: Duration) {
        self.options.ping_timeout = ping_timeout;
    }

    pub fn with_default_event_listener(mut self, default_event_listener: ClientEventListener) -> Self {
        self.options.default_event_listener = Some(default_event_listener);
        self
    }

    pub fn set_default_event_listener(&mut self, default_event_listener: ClientEventListener) {
        self.options.default_event_listener = Some(default_event_listener);
    }

    pub fn with_outbound_alias_resolver(mut self, outbound_alias_resolver: Box<dyn OutboundAliasResolver + Send>) -> Self {
        self.options.outbound_alias_resolver = Some(outbound_alias_resolver);
        self
    }

    pub fn set_outbound_alias_resolver(&mut self, outbound_alias_resolver: Box<dyn OutboundAliasResolver + Send>) {
        self.options.outbound_alias_resolver = Some(outbound_alias_resolver);
    }

    pub fn build(self) -> Mqtt5ClientOptions {
        self.options
    }
}

impl Default for Mqtt5ClientOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Mqtt5Client {
    user_state: UserRuntimeState,

    listener_id_allocator: Mutex<u64>,
}

impl Mqtt5Client {
    pub fn new(mut config: Mqtt5ClientOptions, runtime_handle: &runtime::Handle) -> Mqtt5Client {
        let (user_state, internal_state) = create_runtime_states();

        let state_config = OperationalStateConfig {
            connect_options: config.connect_options.unwrap_or(ConnectOptions { ..Default::default() }),
            base_timestamp: Instant::now(),
            offline_queue_policy: config.offline_queue_policy,
            connack_timeout: config.connack_timeout,
            ping_timeout: config.ping_timeout,
            outbound_alias_resolver: config.outbound_alias_resolver.take(),
        };

        let default_listener = config.default_event_listener.take();
        let client_impl = Mqtt5ClientImpl::new(state_config, default_listener);

        spawn_client_impl(client_impl, internal_state, runtime_handle);

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
        //client_mqtt_operation_body!(self, Subscribe, SubscribeOptionsInternal, packet, Subscribe, options)

        let boxed_packet = Box::new(MqttPacket::Subscribe(packet));
        if let Err(error) = validate_packet_outbound(&*boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        let (response_sender, rx) = oneshot::channel();
        let internal_options = SubscribeOptionsInternal {
            options,
            response_sender : Some(response_sender)
        };
        let send_result = self.user_state.try_send(OperationOptions::Subscribe(boxed_packet, internal_options));
        Box::pin(async move {
            match send_result {
                Err(_) => {
                    Err(Mqtt5Error::OperationChannelSendError)
                }
                _ => {
                    rx.await?
                }
            }
        })
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

