/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate tokio;

use std::collections::HashMap;
use std::time::Instant;
use crate::*;
use crate::client::*;
use crate::operation::*;
use crate::spec::*;

use tokio::runtime;
use tokio::sync::mpsc;
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

#[derive(Eq, PartialEq)]
enum ClientImplState {
    Stopped,
    Connecting,
    Connected,
    PendingReconnect,
    Shutdown,
    // possibly need a pending stopped state for async connection shutdown
}

struct Mqtt5ClientImpl {
    operational_state: OperationalState,
    operation_receiver: mpsc::Receiver<OperationOptions>,
    listeners: HashMap<u64, ClientEventListener>,

    current_state: ClientImplState,
    desired_state: ClientImplState
}

impl Mqtt5ClientImpl {

    pub(crate) fn new(config: OperationalStateConfig, operation_receiver: mpsc::Receiver<OperationOptions>) -> Self {
        Mqtt5ClientImpl {
            operational_state: OperationalState::new(config),
            operation_receiver,
            listeners: HashMap::new(),
            current_state: ClientImplState::Stopped,
            desired_state: ClientImplState::Stopped,
        }
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

    fn change_state_to_connecting(&mut self) -> Mqtt5Result<()> {
        self.current_state = ClientImplState::Connecting;
        Ok(())
    }

    async fn service_stopped(&mut self) -> Mqtt5Result<()> {
        tokio::select! {
            result = self.operation_receiver.recv() => {
                if let Some(operation) = result {
                    self.handle_incoming_operation(operation)?;
                }
            }
        }

        match self.desired_state {
            ClientImplState::Connected => {
                self.change_state_to_connecting()?;
            }
            ClientImplState::Shutdown => {
                // If we're already stopped, there's nothing to clean up
                self.current_state = ClientImplState::Shutdown;
            }
            _ => {}
        }

        Ok(())
    }

    async fn service_connecting(&mut self) -> Mqtt5Result<()> {
        tokio::select! {
            result = self.operation_receiver.recv() => {
                if let Some(operation) = result {
                    self.handle_incoming_operation(operation)?;
                }
            }
        }

        Ok(())
    }

    async fn service_connected(&mut self) -> Mqtt5Result<()> {
        tokio::select! {
            result = self.operation_receiver.recv() => {
                if let Some(operation) = result {
                    self.handle_incoming_operation(operation)?;
                }
            }
        }

        Ok(())
    }

    async fn service_pending_reconnect(&mut self) -> Mqtt5Result<()> {
        tokio::select! {
            result = self.operation_receiver.recv() => {
                if let Some(operation) = result {
                    self.handle_incoming_operation(operation)?;
                }
            }
        }

        Ok(())
    }

    async fn service_shutdown(&mut self) -> Mqtt5Result<()> {
        Err(Mqtt5Error::ClientClosed)
    }

    pub(crate) async fn service(&mut self) -> Mqtt5Result<()> {
        match self.current_state {
            ClientImplState::Stopped => {
                self.service_stopped().await?
            }
            ClientImplState::Connecting => {
                self.service_stopped().await?
            }
            ClientImplState::Connected => {
                self.service_stopped().await?
            }
            ClientImplState::PendingReconnect => {
                self.service_stopped().await?
            }
            ClientImplState::Shutdown => {
                self.service_stopped().await?
            }
        }

        Ok(())
    }

}

async fn client_event_loop(client_impl: &mut Mqtt5ClientImpl) {
    let mut done = false;
    while !done {
        if let Err(_) = client_impl.service().await {
            done = true;
        }
    }
}

pub(crate) fn spawn_client_impl(
    mut config: Mqtt5ClientOptions,
    operation_receiver: mpsc::Receiver<OperationOptions>,
    runtime_handle: &runtime::Handle,
) {
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

    let mut client_impl = Mqtt5ClientImpl::new(state_config, operation_receiver);

    if let Some(listener) = config.default_event_listener {
        client_impl.listeners.insert(0, listener);
    }

    runtime_handle.spawn(async move {
        client_event_loop(&mut client_impl).await;
    });
}
