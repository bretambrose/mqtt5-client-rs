/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate tokio;

use std::future::Future;
use tokio::sync::oneshot;
use tokio::runtime;
use std::pin::Pin;

use crate::packet::*;

#[derive(Debug)]
pub struct PublishOptions {
    pub publish: PublishPacket
}

#[derive(Debug)]
pub enum SuccessfulPublish {
    Qos0Success(PublishPacket),
    Qos1Success(PubackPacket, PublishPacket),
    Qos2Success(PubcompPacket, PublishPacket),
}

pub type PublishResult = Result<SuccessfulPublish, Mqtt5Error<PublishOptions>>;

struct PublishOptionsInternal {
    pub options: PublishOptions,

    pub response_sender: oneshot::Sender<PublishResult>
}

type PublishResultFuture = dyn Future<Output = PublishResult>;

#[derive(Debug)]
pub struct SubscribeOptions {
    pub subscribe: SubscribePacket
}

pub type SubscribeResult = Result<SubackPacket, Mqtt5Error<SubscribeOptions>>;

struct SubscribeOptionsInternal {
    pub options: SubscribeOptions,

    pub response_sender: oneshot::Sender<SubscribeResult>
}

type SubscribeResultFuture = dyn Future<Output = SubscribeResult>;

#[derive(Debug)]
pub struct UnsubscribeOptions {
    pub unsubscribe: UnsubscribePacket
}

pub type UnsubscribeResult = Result<UnsubackPacket, Mqtt5Error<UnsubscribeOptions>>;

struct UnsubscribeOptionsInternal {
    pub options: UnsubscribeOptions,

    pub response_sender: oneshot::Sender<UnsubscribeResult>
}

type UnsubscribeResultFuture = dyn Future<Output = UnsubscribeResult>;

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

#[derive(Debug)]
pub enum Mqtt5Error<T> {
    Unknown,
    Unimplemented(T),
    OperationChannelReceiveError,
    OperationChannelSendError(T),
}

impl<T> From<oneshot::error::RecvError> for Mqtt5Error<T> {
    fn from(_: oneshot::error::RecvError) -> Self {
        Mqtt5Error::<T>::OperationChannelReceiveError
    }
}

pub type Mqtt5Result<T, E> = Result<T, Mqtt5Error<E>>;

/*
pub struct Mqtt5ResultAsync<F, E> {
    pub fut : Pin<Box<F>>,
    pub sync_error: Mqtt5Error<E>
}
*/

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
    ($send_result:expr) => ({
        match $send_result {
            Err(_) => {
                Err(Mqtt5Error::OperationChannelSendError(()))
            }
            _ => {
                Ok(())
            }
        }
    })
}

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
                                    _ => {
                                        println!("Got some lifecycle operation");
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

    pub fn subscribe(&self, options: SubscribeOptions) -> Mqtt5Result<Pin<Box<SubscribeResultFuture>>, SubscribeOptions> {
        let (response_sender, rx) = oneshot::channel();
        let internal_options = SubscribeOptionsInternal { options, response_sender };
        let send_result = self.operation_sender.try_send(OperationOptions::Subscribe(internal_options));
        extract_operation_subclass_options!(send_result, Subscribe, Box::pin(async { rx.await? }))
    }

    pub fn unsubscribe(&self, options: UnsubscribeOptions) -> Mqtt5Result<Pin<Box<UnsubscribeResultFuture>>, UnsubscribeOptions> {
        let (response_sender, rx) = oneshot::channel();
        let internal_options = UnsubscribeOptionsInternal { options, response_sender };
        let send_result = self.operation_sender.try_send(OperationOptions::Unsubscribe(internal_options));
        extract_operation_subclass_options!(send_result, Unsubscribe, Box::pin(async { rx.await? }))
    }
}