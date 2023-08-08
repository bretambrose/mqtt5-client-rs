/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

mod implementation;

extern crate tokio;

use crate::client::implementation::*;
use crate::{Mqtt5Error, Mqtt5Result};
use std::future::Future;
use std::pin::Pin;
use tokio::runtime;
use tokio::sync::oneshot;

use crate::spec::puback::PubackPacket;
use crate::spec::pubcomp::PubcompPacket;
use crate::spec::publish::PublishPacket;
use crate::spec::suback::SubackPacket;
use crate::spec::subscribe::SubscribePacket;
use crate::spec::unsuback::UnsubackPacket;
use crate::spec::unsubscribe::UnsubscribePacket;

#[derive(Debug)]
pub struct PublishOptions {
    pub publish: PublishPacket,
}

#[derive(Debug)]
pub enum SuccessfulPublish {
    Qos0Success(PublishPacket),
    Qos1Success(PubackPacket, PublishPacket),
    Qos2Success(PubcompPacket, PublishPacket),
}

pub type PublishResult = Mqtt5Result<SuccessfulPublish, PublishOptions>;

pub type PublishResultFuture = dyn Future<Output = PublishResult>;

#[derive(Debug)]
pub struct SubscribeOptions {
    pub subscribe: SubscribePacket,
}

pub type SubscribeResult = Mqtt5Result<SubackPacket, SubscribeOptions>;

pub type SubscribeResultFuture = dyn Future<Output = SubscribeResult>;

#[derive(Debug)]
pub struct UnsubscribeOptions {
    pub unsubscribe: UnsubscribePacket,
}

pub type UnsubscribeResult = Mqtt5Result<UnsubackPacket, UnsubscribeOptions>;

pub type UnsubscribeResultFuture = dyn Future<Output = UnsubscribeResult>;

impl<T> From<oneshot::error::RecvError> for Mqtt5Error<T> {
    fn from(_: oneshot::error::RecvError) -> Self {
        Mqtt5Error::<T>::OperationChannelReceiveError
    }
}

pub struct Mqtt5ClientOptions {}

pub struct Mqtt5Client {
    operation_sender: tokio::sync::mpsc::Sender<OperationOptions>,
}

impl Mqtt5Client {
    pub fn new(config: Mqtt5ClientOptions, runtime_handle: &runtime::Handle) -> Mqtt5Client {
        let (operation_sender, operation_receiver) = tokio::sync::mpsc::channel(100);

        spawn_client_impl(config, operation_receiver, &runtime_handle);

        Mqtt5Client { operation_sender }
    }

    pub fn start(&self) -> Mqtt5Result<(), ()> {
        client_lifecycle_operation_body!(Start, self)
    }

    pub fn stop(&self) -> Mqtt5Result<(), ()> {
        client_lifecycle_operation_body!(Stop, self)
    }

    pub fn close(&self) -> Mqtt5Result<(), ()> {
        client_lifecycle_operation_body!(Shutdown, self)
    }

    pub fn publish(&self, options: PublishOptions) -> Pin<Box<PublishResultFuture>> {
        client_mqtt_operation_body!(self, Publish, PublishOptionsInternal, options)
    }

    pub fn subscribe(&self, options: SubscribeOptions) -> Pin<Box<SubscribeResultFuture>> {
        client_mqtt_operation_body!(self, Subscribe, SubscribeOptionsInternal, options)
    }

    pub fn unsubscribe(&self, options: UnsubscribeOptions) -> Pin<Box<UnsubscribeResultFuture>> {
        client_mqtt_operation_body!(self, Unsubscribe, UnsubscribeOptionsInternal, options)
    }
}
