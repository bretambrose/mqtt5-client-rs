/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod tokio_impl;

extern crate tokio;

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use crate::*;
use crate::client::*;
use crate::operation::*;
use crate::spec::*;

use tokio::runtime;
use tokio::sync::oneshot;
use tokio::net::TcpStream;

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

    connection: Option<TcpStream>,

    packet_events: VecDeque<PacketEvent>
}

impl Mqtt5ClientImpl {

    pub(crate) fn new(config: OperationalStateConfig, default_listener: Option<ClientEventListener>) -> Self {
        let mut client_impl = Mqtt5ClientImpl {
            operational_state: OperationalState::new(config),
            listeners: HashMap::new(),
            current_state: ClientImplState::Stopped,
            desired_state: ClientImplState::Stopped,
            connection: None,
            packet_events: VecDeque::new()
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

    pub(crate) fn handle_incoming_operation(&mut self, operation: OperationOptions) -> Mqtt5Result<()> {
        match operation {
            OperationOptions::Publish(packet, internal_options) => {
                let user_event_context = UserEventContext {
                    event: UserEvent::Publish(packet, internal_options),
                    current_time: Instant::now()
                };

                return self.operational_state.handle_user_event(user_event_context);
            }
            OperationOptions::Subscribe(packet, internal_options) => {
                let user_event_context = UserEventContext {
                    event: UserEvent::Subscribe(packet, internal_options),
                    current_time: Instant::now()
                };

                return self.operational_state.handle_user_event(user_event_context);
            }
            OperationOptions::Unsubscribe(packet, internal_options) => {
                let user_event_context = UserEventContext {
                    event: UserEvent::Unsubscribe(packet, internal_options),
                    current_time: Instant::now()
                };

                return self.operational_state.handle_user_event(user_event_context);
            }
            OperationOptions::Start() => {
                self.desired_state = ClientImplState::Connected;
            }
            OperationOptions::Stop(_) => {
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

        Ok(())
    }

    fn compute_optional_state_transition(&self) -> Option<ClientImplState> {
        match self.current_state {
            ClientImplState::Stopped => {
                match self.desired_state {
                    ClientImplState::Connected => {
                        Some(ClientImplState::Connecting)
                    }
                    ClientImplState::Shutdown => {
                        Some(ClientImplState::Shutdown)
                    }
                    _ => { None }
                }
            }

            ClientImplState::Connecting | ClientImplState::Connected | ClientImplState::PendingReconnect => {
                if self.desired_state != ClientImplState::Connected {
                    Some(ClientImplState::Stopped)
                } else {
                    None
                }
            }

            _ => { None }
        }
    }

    fn apply_connection_result(&mut self, connection_result: std::io::Result<TcpStream>) -> Mqtt5Result<ClientImplState> {
        if let Ok(stream) = connection_result {
            self.connection = Some(stream);
            Ok(ClientImplState::Connected)
        } else {
            Ok(ClientImplState::PendingReconnect)
        }
    }

    fn get_next_connected_service_time(&mut self) -> Option<Instant> {
        if self.current_state == ClientImplState::Connected {
            return self.operational_state.get_next_service_timepoint(&Instant::now());
        }

        None
    }

    fn transition_to_state(&mut self, next_state: ClientImplState) -> Mqtt5Result<()> {
        if next_state == ClientImplState::Connected {
            let mut connection_opened_context = NetworkEventContext {
                event: NetworkEvent::ConnectionOpened,
                current_time: Instant::now(),
                packet_events: &mut self.packet_events
            };

            self.operational_state.handle_network_event(&mut connection_opened_context)?;
        } else if self.current_state == ClientImplState::Connected {
            let mut connection_closed_context = NetworkEventContext {
                event: NetworkEvent::ConnectionClosed,
                current_time: Instant::now(),
                packet_events: &mut self.packet_events
            };

            self.operational_state.handle_network_event(&mut connection_closed_context)?;
        }

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
