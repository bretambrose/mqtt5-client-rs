/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate tokio;

use crate::client::*;
use crate::Mqtt5Error;
use tokio::runtime;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

macro_rules! client_lifecycle_operation_body {
    ($lifeycle_operation:ident, $self:ident) => {{
        match $self
            .operation_sender
            .try_send(OperationOptions::$lifeycle_operation())
        {
            Err(_) => Err(Mqtt5Error::OperationChannelSendError(())),
            _ => Ok(()),
        }
    }};
}

pub(crate) use client_lifecycle_operation_body;

macro_rules! client_mqtt_operation_body {
    ($self:ident, $operation_type:ident, $options_internal_type: ident, $options_value: expr) => ({
        let (response_sender, rx) = oneshot::channel();
        let internal_options = $options_internal_type { options : $options_value, response_sender };
        let send_result = $self.operation_sender.try_send(OperationOptions::$operation_type(internal_options));
        Box::pin(async move {
            match send_result {
                Err(tokio::sync::mpsc::error::TrySendError::Full(val)) | Err(tokio::sync::mpsc::error::TrySendError::Closed(val)) => {
                    match val {
                        OperationOptions::$operation_type(options) => {
                            Err(Mqtt5Error::OperationChannelSendError(options.options))
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

pub struct PublishOptionsInternal {
    pub options: PublishOptions,

    pub response_sender: oneshot::Sender<PublishResult>,
}

pub struct SubscribeOptionsInternal {
    pub options: SubscribeOptions,

    pub response_sender: oneshot::Sender<SubscribeResult>,
}

pub struct UnsubscribeOptionsInternal {
    pub options: UnsubscribeOptions,

    pub response_sender: oneshot::Sender<UnsubscribeResult>,
}

pub enum OperationOptions {
    Publish(PublishOptionsInternal),
    Subscribe(SubscribeOptionsInternal),
    Unsubscribe(UnsubscribeOptionsInternal),
    Start(),
    Stop(),
    Shutdown(),
}

struct Mqtt5ClientImpl {
    config: Mqtt5ClientOptions,
    operation_receiver: mpsc::Receiver<OperationOptions>,
}

async fn client_event_loop(client_impl: &mut Mqtt5ClientImpl) {
    let mut done = false;
    while !done {
        tokio::select! {
            result = client_impl.operation_receiver.recv() => {
                match result {
                    Some(value) => {
                        match value {
                            OperationOptions::Publish(internal_options) => {
                                println!("Got a publish!");
                                let failure_result : PublishResult = Err(Mqtt5Error::<PublishOptions>::Unimplemented(internal_options.options));
                                internal_options.response_sender.send(failure_result).unwrap();
                            }
                            OperationOptions::Subscribe(internal_options) => {
                                println!("Got a subscribe!");
                                let failure_result : SubscribeResult = Err(Mqtt5Error::<SubscribeOptions>::Unimplemented(internal_options.options));
                                internal_options.response_sender.send(failure_result).unwrap();
                            }
                            OperationOptions::Unsubscribe(internal_options) => {
                                println!("Got an unsubscribe!");
                                let failure_result : UnsubscribeResult = Err(Mqtt5Error::<UnsubscribeOptions>::Unimplemented(internal_options.options));
                                internal_options.response_sender.send(failure_result).unwrap();
                            }
                            OperationOptions::Start() => {
                                println!("Received start!");
                            }
                            OperationOptions::Stop() => {
                                println!("Received stop!");
                            }
                            OperationOptions::Shutdown() => {
                                println!("Received shutdown!");
                                done = true;
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

pub fn spawn_client_impl(
    config: Mqtt5ClientOptions,
    operation_receiver: mpsc::Receiver<OperationOptions>,
    runtime_handle: &runtime::Handle,
) {
    let mut client_impl = Mqtt5ClientImpl {
        config,
        operation_receiver,
    };
    runtime_handle.spawn(async move {
        client_event_loop(&mut client_impl).await;
    });
}
