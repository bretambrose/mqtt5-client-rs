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

macro_rules! client_lifecycle_operation_body {
    ($lifeycle_operation:ident, $self:ident) => {{
        match $self
            .operation_sender
            .try_send(OperationOptions::$lifeycle_operation())
        {
            Err(_) => Err(Mqtt5Error::OperationChannelSendError),
            _ => Ok(()),
        }
    }};
}

pub(crate) use client_lifecycle_operation_body;

macro_rules! client_mqtt_operation_body {
    ($self:ident, $operation_type:ident, $options_internal_type: ident, $packet_name: ident, $packet_type: ident, $options_value: expr) => ({
        let (response_sender, rx) = oneshot::channel();
        let boxed_packet = Box::new(MqttPacket::$packet_type($packet_name));
        let internal_options = $options_internal_type {
            options : $options_value,
            response_sender : Some(response_sender)
        };
        let send_result = $self.operation_sender.try_send(OperationOptions::$operation_type(boxed_packet, internal_options));
        Box::pin(async move {
            match send_result {
                Err(tokio::sync::mpsc::error::TrySendError::Full(val)) | Err(tokio::sync::mpsc::error::TrySendError::Closed(val)) => {
                    match val {
                        OperationOptions::$operation_type(_, _) => {
                            Err(Mqtt5Error::OperationChannelSendError)
                        }
                        _ => {
                            panic!("Illegal MQTT operation options type encountered in channel send error processing");
                        }
                    }
                }
                _ => {
                    rx.await?
                }
            }
        })
    })
}

pub(crate) use client_mqtt_operation_body;

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

pub(crate) struct DisconnectOptionsInternal {
    pub options: DisconnectOptions,
    pub response_sender: Option<oneshot::Sender<DisconnectResult>>,
}

pub(crate) enum OperationOptions {
    Publish(Box<MqttPacket>, PublishOptionsInternal),
    Subscribe(Box<MqttPacket>, SubscribeOptionsInternal),
    Unsubscribe(Box<MqttPacket>, UnsubscribeOptionsInternal),
    Start(),
    Stop(Box<MqttPacket>, DisconnectOptionsInternal),
    Shutdown(),
    AddListener(u64, ClientEventListener),
    RemoveListener(u64)
}

struct Mqtt5ClientImpl {
    state: OperationalState,
    operation_receiver: mpsc::Receiver<OperationOptions>,
    listeners: HashMap<u64, ClientEventListener>,
}

impl Mqtt5ClientImpl {
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
}

async fn client_event_loop(client_impl: &mut Mqtt5ClientImpl) {
    let mut done = false;
    while !done {
        tokio::select! {
            result = client_impl.operation_receiver.recv() => {
                match result {
                    Some(value) => {
                        match value {
                            OperationOptions::Publish(_, internal_options) => {
                                println!("Got a publish!");
                                println!("Raising a client event to test callbacks");

                                let event = Arc::new(ClientEvent::ConnectionAttempt(ConnectionAttemptEvent{}));
                                client_impl.broadcast_event(event);

                                let failure_result : PublishResult = Err(Mqtt5Error::Unimplemented);
                                internal_options.response_sender.unwrap().send(failure_result).unwrap();
                            }
                            OperationOptions::Subscribe(_, internal_options) => {
                                println!("Got a subscribe!");
                                let failure_result : SubscribeResult = Err(Mqtt5Error::Unimplemented);
                                internal_options.response_sender.unwrap().send(failure_result).unwrap();
                            }
                            OperationOptions::Unsubscribe(_, internal_options) => {
                                println!("Got an unsubscribe!");
                                let failure_result : UnsubscribeResult = Err(Mqtt5Error::Unimplemented);
                                internal_options.response_sender.unwrap().send(failure_result).unwrap();
                            }
                            OperationOptions::Start() => {
                                println!("Received start!");
                            }
                            OperationOptions::Stop(_, internal_options) => {
                                println!("Received stop!");
                                let failure_result : DisconnectResult = Err(Mqtt5Error::Unimplemented);
                                internal_options.response_sender.unwrap().send(failure_result).unwrap();
                            }
                            OperationOptions::Shutdown() => {
                                println!("Received shutdown!");
                                done = true;
                            }
                            OperationOptions::AddListener(id, listener) => {
                                client_impl.add_listener(id, listener);
                            }
                            OperationOptions::RemoveListener(id) => {
                                client_impl.remove_listener(id);
                            }
                        }
                    }
                    _ => {
                    }
                }
            }
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

    let mut client_impl = Mqtt5ClientImpl {
        state: OperationalState::new(state_config),
        operation_receiver,
        listeners: HashMap::new()
    };

    if let Some(listener) = config.default_event_listener {
        client_impl.listeners.insert(0, listener);
    }

    runtime_handle.spawn(async move {
        client_event_loop(&mut client_impl).await;
    });
}
