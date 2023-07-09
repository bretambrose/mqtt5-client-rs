/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate tokio;

use std::future::Future;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::runtime;
use std::pin::Pin;

use crate::packet::*;

pub struct PublishOptions {
    pub publish: PublishPacket
}

pub enum SuccessfulPublish {
    Qos0Success(PublishPacket),
    Qos1Success(PubackPacket, PublishPacket),
    Qos2Success(PubcompPacket, PublishPacket),
}

pub type PublishResult = Result<SuccessfulPublish, Mqtt5Error<()>>;

struct PublishOptionsInternal {
    pub options: PublishOptions,

    pub response_sender: oneshot::Sender<PublishResult>
}

type PublishResultFuture = dyn Future<Output = PublishResult>;

pub struct SubscribeOptions {
    pub subscribe: SubscribePacket
}

pub type SubscribeResult = Result<SubackPacket, Mqtt5Error<()>>;

struct SubscribeOptionsInternal {
    pub options: SubscribeOptions,

    pub response_sender: oneshot::Sender<SubscribeResult>
}

pub struct UnsubscribeOptions {
    pub unsubscribe: UnsubscribePacket
}

pub type UnsubscribeResult = Result<UnsubackPacket, Mqtt5Error<()>>;

struct UnsubscribeOptionsInternal {
    pub options: UnsubscribeOptions,

    pub response_sender: oneshot::Sender<UnsubscribeResult>
}

enum OperationOptions {
    Publish(PublishOptionsInternal),
    Subscribe(SubscribeOptionsInternal),
    Unsubscribe(UnsubscribeOptionsInternal),
    Start(),
    Stop(),
    Shutdown()
}

pub struct Mqtt5ClientOptions {

}

pub struct Mqtt5Client {
    operation_sender : tokio::sync::mpsc::Sender<OperationOptions>,
}

struct Mqtt5ClientImpl {
    config: Mqtt5ClientOptions,
    operation_receiver : tokio::sync::mpsc::Receiver<OperationOptions>,
}

pub enum Mqtt5Error<T> {
    Unknown,
    OperationChannelReceiveError,
    OperationChannelSendError(T),
}

impl<T> From<tokio::sync::oneshot::error::RecvError> for Mqtt5Error<T> {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        Mqtt5Error::<T>::OperationChannelReceiveError
    }
}

pub type Mqtt5Result<T, E> = Result<T, Mqtt5Error<E>>;

macro_rules! extract_operation_subclass_options {
    ($send_result:expr, $option_type1:ident, $future_expr:expr) => ({
        match $send_result {
            Err(tokio::sync::mpsc::error::TrySendError::Full(val)) | Err(tokio::sync::mpsc::error::TrySendError::Closed(val)) => {
                match val {
                    OperationOptions::$option_type1(options) => {
                        Err(Mqtt5Error::OperationChannelSendError(options.options))
                    }
                    _ => {
                        panic!("Derp");
                    }
                }
            }
            _ => {
                Ok($future_expr)
            }
        }
    });
    ($send_result:expr, $option_type1:ident) => ({
        match $send_result {
            Err(tokio::sync::mpsc::error::TrySendError::Full(val)) | Err(tokio::sync::mpsc::error::TrySendError::Closed(val)) => {
                match val {
                    OperationOptions::$option_type1(options) => {
                        Err(Mqtt5Error::OperationChannelSendError(options.options))
                    }
                    _ => {
                        panic!("Derp");
                    }
                }
            }
            _ => {
                Ok(())
            }
        }
    });
    ($send_result:expr) => ({
        match $send_result {
            Err(error) => {
                Err(Mqtt5Error::OperationChannelSendError(()))
            }
            _ => {
                Ok(())
            }
        }
    })
}

/*
pub enum SuccessfulPublish {
    Qos0Success(PublishPacket),
    Qos1Success(PubackPacket, PublishPacket),
    Qos2Success(PubcompPacket, PublishPacket),
}

pub type PublishResult = Result<SuccessfulPublish, Mqtt5Error<()>>;
 */
impl Mqtt5Client {

    pub fn new(config: Mqtt5ClientOptions, runtime_handle : &runtime::Handle) -> Mqtt5Client {
        let (operation_sender, operation_receiver) = tokio::sync::mpsc::channel(100);

        let mut client_impl = Mqtt5ClientImpl { config, operation_receiver };
        runtime_handle.spawn(async move {
            loop {
                tokio::select! {
                    result = client_impl.operation_receiver.recv() => {
                        match result {
                            None => {
                                println!("Oh crap");
                            }
                            Some(value) => {
                                match value {
                                    OperationOptions::Publish(internal_options) => {
                                        println!("Got a publish!");
                                        let fake_result : PublishResult = Ok(SuccessfulPublish::Qos0Success(internal_options.options.publish));
                                        internal_options.response_sender.send(fake_result);
                                    }
                                    OperationOptions::Subscribe(_) => {
                                        println!("Got a subscribe!");
                                    }
                                    OperationOptions::Unsubscribe(_) => {
                                        println!("Got an unsubscribe!");
                                    }
                                    _ => {
                                        println!("Got some lifecycle thing");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        let client = Mqtt5Client { operation_sender };

        client
    }

    pub fn start(&self) -> Mqtt5Result<(), ()> {
        extract_operation_subclass_options!(self.operation_sender.try_send(OperationOptions::Start()))
    }

    pub fn stop(&self) -> Mqtt5Result<(), ()> {
        extract_operation_subclass_options!(self.operation_sender.try_send(OperationOptions::Stop()))
    }

    pub fn close(&self) -> Mqtt5Result<(), ()> {
        extract_operation_subclass_options!(self.operation_sender.try_send(OperationOptions::Shutdown()))
    }

    pub fn publish(&self, options: PublishOptions) -> Mqtt5Result<Pin<Box<PublishResultFuture>>, PublishOptions> {
        let (response_sender, rx) = oneshot::channel();
        let internal_options = PublishOptionsInternal { options, response_sender };
        let send_result = self.operation_sender.try_send(OperationOptions::Publish(internal_options));

        extract_operation_subclass_options!(send_result, Publish, Box::pin(async { rx.await? }))
    }

    pub fn subscribe(&self, options: SubscribeOptions) -> Mqtt5Result<(), SubscribeOptions> {
        let (response_sender, _) = oneshot::channel();
        let internal_options = SubscribeOptionsInternal { options, response_sender };
        let send_result = self.operation_sender.try_send(OperationOptions::Subscribe(internal_options));
        extract_operation_subclass_options!(send_result, Subscribe)
    }

    pub fn unsubscribe(&self, options: UnsubscribeOptions) -> Mqtt5Result<(), UnsubscribeOptions> {
        let (response_sender, _) = oneshot::channel();
        let internal_options = UnsubscribeOptionsInternal { options, response_sender };
        let send_result = self.operation_sender.try_send(OperationOptions::Unsubscribe(internal_options));
        extract_operation_subclass_options!(send_result, Unsubscribe)
    }
}