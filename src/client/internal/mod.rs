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

    packet_events: VecDeque<PacketEvent>,

    last_connack: Option<ConnackPacket>,
    last_disconnect: Option<DisconnectPacket>,
    last_error: Option<Mqtt5Error>
}

/*

On Enter Connecting: Emit AttemptingConnect, clear all last fields

On Leave Connecting:
    If not going to Connected, emit ConnectionFailure

On Leave Connected:
    If successful connack exists, emit Disconnected
    Else emit Connection Failure

On Successful connack received:
    Emit ConnectionSuccess

When do we set last error?
  Pass through wrapper functions to operational state

How do we propagate errors from async runtime logic?
  Give a set function that only sets if None?

What do we do if there's a disconnect or failure and no error code?
  Add an unknown entry
 */

impl Mqtt5ClientImpl {

    pub(crate) fn new(config: OperationalStateConfig, default_listener: Option<ClientEventListener>) -> Self {
        let mut client_impl = Mqtt5ClientImpl {
            operational_state: OperationalState::new(config),
            listeners: HashMap::new(),
            current_state: ClientImplState::Stopped,
            desired_state: ClientImplState::Stopped,
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
                    self.last_connack = Some(connack);
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

        result
    }

    pub(crate) fn handle_service(&mut self, outbound_data: &mut Vec<u8>) -> Mqtt5Result<()> {
        let mut context = ServiceContext {
            to_socket: outbound_data,
            current_time: Instant::now(),
        };

        return self.operational_state.service(&mut context);
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
