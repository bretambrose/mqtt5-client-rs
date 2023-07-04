/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::sync::mpsc::{Receiver, Sender, SendError, channel};
use crate::packet::*;

pub struct PublishOptions {
    publish: PublishPacket
}

pub struct SubscribeOptions {
    subscribe: SubscribePacket
}

pub struct UnsubscribeOptions {
    unsubscribe: UnsubscribePacket
}

enum OperationOptions {
    Publish(PublishOptions),
    Subscribe(SubscribeOptions),
    Unsubscribe(UnsubscribeOptions),
    Start(),
    Stop(),
    Shutdown()
}

pub struct Mqtt5ClientOptions {

}

pub struct Mqtt5Client {
    config: Mqtt5ClientOptions,
    operation_channel : Sender<OperationOptions>,
}

struct Mqtt5ClientImpl {
    operation_channel : Receiver<OperationOptions>,
}

pub enum Mqtt5Error<T> {
    OperationChannelSendError(T),
}

pub type Mqtt5Result<T, U> = Result<T, Mqtt5Error<U>>;

fn send_result_to_mqtt5_result<T, U>(send_result: Result<(), SendError<OperationOptions>>, success_value : T) -> Mqtt5Result<T, U> {
    match send_result {
        Ok(_) => {
            Ok(success_value)
        }
        Err(error) => {
            match error {
                OperationOptions::Publish(options) => {
                    Err(Mqtt5Error::<PublishOptions>::OperationChannelSendError(options))
                }
                OperationOptions::Subscribe(options) => {
                    Err(Mqtt5Error::<SubscribeOptions>::OperationChannelSendError(options))
                }
                OperationOptions::Unsubscribe(options) => {
                    Err(Mqtt5Error::<UnsubscribeOptions>::OperationChannelSendError(options))
                }
                _ => {
                    Err(Mqtt5Error::OperationChannelSendError(()))
                }
            }
        }
    }
}

impl Mqtt5Client {

    pub fn new(options: Mqtt5ClientOptions) -> Mqtt5Client {
        let (operation_sender, _) = channel();

        let client = Mqtt5Client { config : options, operation_channel : operation_sender };

        client
    }

    pub fn start(&self) -> Mqtt5Result<(), ()> {
        return send_result_to_mqtt5_result(self.operation_channel.send(OperationOptions::Start()), ());
    }

    pub fn stop(&self) -> Mqtt5Result<(), ()> {
        return send_result_to_mqtt5_result(self.operation_channel.send(OperationOptions::Stop()), ());
    }

    pub fn close(&self) -> Mqtt5Result<(), ()> {
        return send_result_to_mqtt5_result(self.operation_channel.send(OperationOptions::Shutdown()), ());
    }

    pub fn publish(&self, options: PublishOptions) -> Mqtt5Result<(), PublishOptions> {
        return send_result_to_mqtt5_result(self.operation_channel.send(OperationOptions::Publish(options)), ());
    }

    pub fn subscribe(&self, options: SubscribeOptions) -> Mqtt5Result<(), SubscribeOptions> {
        return send_result_to_mqtt5_result(self.operation_channel.send(OperationOptions::Subscribe(options)), ());
    }

    pub fn unsubscribe(&self, options: UnsubscribeOptions) -> Mqtt5Result<(), UnsubscribeOptions> {
        return send_result_to_mqtt5_result(self.operation_channel.send(OperationOptions::Unsubscribe(options)), ());
    }
}