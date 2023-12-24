/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod tokio_impl;

extern crate tokio;

use std::collections::{HashMap, VecDeque};
use std::mem;
use std::time::{Duration, Instant};
use crate::*;
use crate::client::*;
use crate::operation::*;
use crate::spec::*;

use tokio::runtime;
use tokio::sync::oneshot;

pub(crate) struct PublishOptionsInternal {
    pub options: PublishOptions,
    pub response_sender: Option<oneshot::Sender<PublishResult>>,
}

pub(crate) struct SubscribeOptionsInternal {
    pub options: SubscribeOptions,
    pub response_sender: Option<oneshot::Sender<SubscribeResult>>,
}

pub(crate) struct UnsubscribeOptionsInternal {
    pub options: UnsubscribeOptions,
    pub response_sender: Option<oneshot::Sender<UnsubscribeResult>>,
}

#[derive(Debug, Default)]
pub(crate) struct StopOptionsInternal {
    pub disconnect: Option<Box<MqttPacket>>,
    pub mode: StopMode
}

pub(crate) enum OperationOptions {
    Publish(Box<MqttPacket>, PublishOptionsInternal),
    Subscribe(Box<MqttPacket>, SubscribeOptionsInternal),
    Unsubscribe(Box<MqttPacket>, UnsubscribeOptionsInternal),
    Start(),
    Stop(StopOptionsInternal),
    Shutdown(),
    AddListener(u64, ClientEventListener),
    RemoveListener(u64)
}

#[derive(Eq, PartialEq, Copy, Clone)]
pub(crate) enum ClientImplState {
    Stopped,
    Connecting,
    Connected,
    PendingReconnect,
    Shutdown,
    // possibly need a pending stopped state for async connection shutdown
}

pub(crate) struct Mqtt5ClientImpl {
    operational_state: OperationalState,
    listeners: HashMap<u64, ClientEventListener>,

    current_state: ClientImplState,
    desired_state: ClientImplState,

    desired_stop_options: Option<StopOptionsInternal>,

    packet_events: VecDeque<PacketEvent>,

    last_connack: Option<ConnackPacket>,
    last_disconnect: Option<DisconnectPacket>,
    last_error: Option<Mqtt5Error>
}


impl Mqtt5ClientImpl {

    pub(crate) fn new(config: OperationalStateConfig, default_listener: Option<ClientEventListener>) -> Self {
        let mut client_impl = Mqtt5ClientImpl {
            operational_state: OperationalState::new(config),
            listeners: HashMap::new(),
            current_state: ClientImplState::Stopped,
            desired_state: ClientImplState::Stopped,
            desired_stop_options: None,
            packet_events: VecDeque::new(),
            last_connack: None,
            last_disconnect: None,
            last_error: None,
        };

        if let Some(listener) = default_listener {
            client_impl.listeners.insert(0, listener);
        }

        client_impl
    }

    pub(crate) fn get_current_state(&self) -> ClientImplState {
        self.current_state
    }

    pub(crate) fn add_listener(&mut self, id: u64, listener: ClientEventListener) {
        self.listeners.insert(id, listener);
    }

    pub(crate) fn remove_listener(&mut self, id: u64) {
        self.listeners.remove(&id);
    }

    pub(crate) fn broadcast_event(&self, event: Arc<ClientEvent>) {
        for (_, listener) in &self.listeners {
            match listener {
                ClientEventListener::Channel(channel) => {
                    channel.send(event.clone()).unwrap();
                }
                ClientEventListener::Callback(callback) => {
                    callback(event.clone());
                }
            }
        }
    }

    pub(crate) fn apply_error(&mut self, error: Mqtt5Error) {
        if self.last_error.is_none() {
            self.last_error = Some(error);
        }
    }

    pub(crate) fn handle_incoming_operation(&mut self, operation: OperationOptions) {
        match operation {
            OperationOptions::Publish(packet, internal_options) => {
                let user_event_context = UserEventContext {
                    event: UserEvent::Publish(packet, internal_options),
                    current_time: Instant::now()
                };

                self.operational_state.handle_user_event(user_event_context);
            }
            OperationOptions::Subscribe(packet, internal_options) => {
                let user_event_context = UserEventContext {
                    event: UserEvent::Subscribe(packet, internal_options),
                    current_time: Instant::now()
                };

                self.operational_state.handle_user_event(user_event_context);
            }
            OperationOptions::Unsubscribe(packet, internal_options) => {
                let user_event_context = UserEventContext {
                    event: UserEvent::Unsubscribe(packet, internal_options),
                    current_time: Instant::now()
                };

                self.operational_state.handle_user_event(user_event_context);
            }
            OperationOptions::Start() => {
                self.desired_stop_options = None;
                self.desired_state = ClientImplState::Connected;
            }
            OperationOptions::Stop(options) => {
                if let Some(disconnect) = &options.disconnect {
                    let disconnect_context = UserEventContext {
                        event: UserEvent::Disconnect(disconnect.clone()),
                        current_time: Instant::now()
                    };

                    self.operational_state.handle_user_event(disconnect_context);
                }

                self.desired_stop_options = Some(options);
                self.desired_state = ClientImplState::Stopped;
            }
            OperationOptions::Shutdown() => {
                self.operational_state.reset(&Instant::now());
                self.desired_state = ClientImplState::Shutdown;
            }
            OperationOptions::AddListener(id, listener) => {
                self.add_listener(id, listener);
            }
            OperationOptions::RemoveListener(id) => {
                self.remove_listener(id);
            }
        }
    }

    fn handle_packet_events(&mut self) {
        let mut events = VecDeque::new();
        mem::swap(&mut events, &mut self.packet_events);

        for event in events {
            match event {
                PacketEvent::Publish(publish) => {
                    let publish_event = PublishReceivedEvent {
                        publish,
                    };

                    let publish_client_event = Arc::new(ClientEvent::PublishReceived(publish_event));
                    self.broadcast_event(publish_client_event);
                }
                PacketEvent::Disconnect(disconnect) => {
                    self.last_disconnect = Some(disconnect);
                }
                PacketEvent::Connack(connack) => {
                    let reason_code = connack.reason_code;
                    self.last_connack = Some(connack);
                    if reason_code == ConnectReasonCode::Success {
                        self.emit_connection_success_event();
                    }
                }
            }
        }

        self.packet_events.clear();
    }

    pub(crate) fn handle_incoming_bytes(&mut self, bytes: &[u8]) -> Mqtt5Result<()> {
        let mut context = NetworkEventContext {
            event: NetworkEvent::IncomingData(bytes),
            current_time: Instant::now(),
            packet_events: &mut self.packet_events
        };

        let result = self.operational_state.handle_network_event(&mut context);
        if result.is_err() {
            self.apply_error(result.unwrap_err());
        }

        self.handle_packet_events();

        result
    }

    pub(crate) fn handle_write_completion(&mut self) -> Mqtt5Result<()> {
        let mut context = NetworkEventContext {
            event: NetworkEvent::WriteCompletion,
            current_time: Instant::now(),
            packet_events: &mut self.packet_events
        };

        let result = self.operational_state.handle_network_event(&mut context);
        if result.is_err() {
            self.apply_error(result.unwrap_err());
        }

        result
    }

    pub(crate) fn handle_service(&mut self, outbound_data: &mut Vec<u8>) -> Mqtt5Result<()> {
        let mut context = ServiceContext {
            to_socket: outbound_data,
            current_time: Instant::now(),
        };

        let result = self.operational_state.service(&mut context);
        if result.is_err() {
            self.apply_error(result.unwrap_err());
        }

        result
    }

    fn compute_optional_state_transition(&self) -> Option<ClientImplState> {
        match self.current_state {
            ClientImplState::Stopped => {
                match self.desired_state {
                    ClientImplState::Connected => {
                        return Some(ClientImplState::Connecting)
                    }
                    ClientImplState::Shutdown => {
                        return Some(ClientImplState::Shutdown)
                    }
                    _ => {}
                }
            }

            ClientImplState::Connecting | ClientImplState::PendingReconnect => {
                if self.desired_state != ClientImplState::Connected {
                    return Some(ClientImplState::Stopped)
                }
            }

            ClientImplState::Connected => {
                if self.desired_state != ClientImplState::Connected {
                    if let Some(stop_options) = &self.desired_stop_options {
                        if stop_options.disconnect.is_none() {
                            return Some(ClientImplState::Stopped);
                        }
                    }
                }
            }

            _ => { }
        }

        None
    }

    fn get_next_connected_service_time(&mut self) -> Option<Instant> {
        if self.current_state == ClientImplState::Connected {
            return self.operational_state.get_next_service_timepoint(&Instant::now());
        }

        None
    }

    fn emit_connection_attempt_event(&self) {
        let connection_attempt_event = ConnectionAttemptEvent {
        };

        self.broadcast_event(Arc::new(ClientEvent::ConnectionAttempt(connection_attempt_event)));
    }

    fn emit_connection_success_event(&self) {
        let settings = self.operational_state.get_negotiated_settings().as_ref().unwrap();

        let connection_success_event = ConnectionSuccessEvent {
            connack: self.last_connack.as_ref().unwrap().clone(),
            settings: settings.clone(),
        };

        self.broadcast_event(Arc::new(ClientEvent::ConnectionSuccess(connection_success_event)));
    }

    fn emit_connection_failure_event(&self) {
        let mut connection_failure_event = ConnectionFailureEvent {
            error: self.last_error.unwrap_or(Mqtt5Error::Unknown),
            connack: None,
        };

        if let Some(connack) = &self.last_connack {
            connection_failure_event.connack = Some(connack.clone());
        }

        self.broadcast_event(Arc::new(ClientEvent::ConnectionFailure(connection_failure_event)));
    }

    fn emit_disconnection_event(&self) {
        let mut disconnection_event = DisconnectionEvent {
            error: self.last_error.unwrap_or(Mqtt5Error::Unknown),
            disconnect: None,
        };

        if let Some(disconnect) = &self.last_disconnect {
            disconnection_event.disconnect = Some(disconnect.clone());
        }

        self.broadcast_event(Arc::new(ClientEvent::Disconnection(disconnection_event)));
    }

    fn emit_stopped_event(&self) {
        let stopped_event = StoppedEvent {
        };

        self.broadcast_event(Arc::new(ClientEvent::Stopped(stopped_event)));
    }

    fn transition_to_state(&mut self, mut new_state: ClientImplState) -> Mqtt5Result<()> {
        let old_state = self.current_state;
        if old_state == new_state {
            return Ok(());
        }

        if new_state == ClientImplState::PendingReconnect {
            if self.desired_state != ClientImplState::Connected {
                new_state = ClientImplState::Stopped;
            }
        }

        if new_state == ClientImplState::Connected {
            let mut connection_opened_context = NetworkEventContext {
                event: NetworkEvent::ConnectionOpened,
                current_time: Instant::now(),
                packet_events: &mut self.packet_events
            };

            self.operational_state.handle_network_event(&mut connection_opened_context)?;
        } else if old_state == ClientImplState::Connected {
            let mut connection_closed_context = NetworkEventContext {
                event: NetworkEvent::ConnectionClosed,
                current_time: Instant::now(),
                packet_events: &mut self.packet_events
            };

            self.operational_state.handle_network_event(&mut connection_closed_context)?;
        }

        if new_state == ClientImplState::Connecting {
            self.last_error = None;
            self.last_connack = None;
            self.last_disconnect = None;
            self.emit_connection_attempt_event();
        }

        if old_state == ClientImplState::Connecting && new_state != ClientImplState::Connected {
            self.emit_connection_failure_event();
        }

        if old_state == ClientImplState::Connected {
            if let Some(connack) = &self.last_connack {
                if connack.reason_code == ConnectReasonCode::Success {
                    self.emit_disconnection_event();
                } else {
                    self.emit_connection_failure_event();
                }
            } else {
                self.emit_connection_failure_event();
            }
        }

        if new_state == ClientImplState::Stopped {
            self.desired_stop_options = None;
            self.emit_stopped_event();
        }

        self.current_state = new_state;

        Ok(())
    }
}

async fn client_event_loop(client_impl: &mut Mqtt5ClientImpl, async_state: &mut ClientRuntimeState) {
    let mut done = false;
    while !done {
        let current_state = client_impl.get_current_state();
        let next_state_result =
            match current_state {
                ClientImplState::Stopped => { async_state.process_stopped(client_impl).await }
                ClientImplState::Connecting => { async_state.process_connecting(client_impl).await }
                ClientImplState::Connected => { async_state.process_connected(client_impl).await }
                ClientImplState::PendingReconnect => { async_state.process_pending_reconnect(client_impl, Duration::from_secs(5)).await }
                _ => { Ok(ClientImplState::Shutdown) }
            };

        done = true;
        if let Ok(next_state) = next_state_result {
            if let Ok(_) = client_impl.transition_to_state(next_state) {
                if next_state != ClientImplState::Shutdown {
                    done = false;
                }
            }
        }
    }
}

pub(crate) fn spawn_client_impl(
    mut client_impl: Mqtt5ClientImpl,
    mut runtime_state: ClientRuntimeState,
    runtime_handle: &runtime::Handle,
) {
    runtime_handle.spawn(async move {
        client_event_loop(&mut client_impl, &mut runtime_state).await;
    });
}
