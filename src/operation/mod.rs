/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::client::implementation::*;
use crate::spec::*;

use std::collections::*;

pub(crate) struct MqttOperation {
    id: u64,
    packet: Box<MqttPacket>
}

pub(crate) enum NetworkEvent<'a> {
    ConnectionOpened,
    ConnectionClosed,
    IncomingData(&'a [u8]),
    WriteCompletion
}

pub(crate) enum UserEvent {
    Start,
    Stop,
    Publish(PublishOptionsInternal),
    Subscribe(SubscribeOptionsInternal),
    Unsubscribe(UnsubscribeOptionsInternal)
}

pub(crate) struct ServiceContext<'a> {
    to_socket: &'a mut Vec<u8>
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum OperationalStateType {
    Disconnected,
    PendingConnack,
    Connected,
    PendingDisconnect,
}

pub(crate) struct OperationalStateConfig {

}

#[derive(Copy, Clone, PartialEq, Eq)]
enum OperationalQueueType {
    User,
    Resubmit,
    HighPriority,
}

pub(crate) struct OperationalState {
    config: OperationalStateConfig,

    state: OperationalStateType,

    pending_write_completion: bool,

    operations: HashMap<u64, MqttOperation>,

    user_operation_queue: VecDeque<u64>,
    resubmit_operation_queue: VecDeque<u64>,
    high_priority_operation_queue: VecDeque<u64>,

    next_operation_id: u64,
}

impl OperationalState {
    pub(crate) fn new(config: OperationalStateConfig) -> OperationalState {
        OperationalState {
            config,
            state: OperationalStateType::Disconnected,
            pending_write_completion : false,
            operations: HashMap::new(),
            user_operation_queue: VecDeque::new(),
            resubmit_operation_queue: VecDeque::new(),
            high_priority_operation_queue: VecDeque::new(),
            next_operation_id : 1
        }
    }

    pub(crate) fn handle_network_event(&mut self, event: &NetworkEvent) -> Mqtt5Result<()> {
        match event {
            NetworkEvent::ConnectionOpened => {
                if self.state != OperationalStateType::Disconnected {
                    return Err(Mqtt5Error::InternalStateError);
                }

                self.state = OperationalStateType::PendingConnack;
                self.pending_write_completion = false;

                // TODO: Queue up a Connect packet

                Ok(())
            }

            NetworkEvent::WriteCompletion => {
                self.pending_write_completion = false;
                Ok(())
            }

            _ => {
                Err(Mqtt5Error::Unimplemented)
            }
        }
    }

    pub(crate) fn service(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        Ok(())
    }

    pub(crate) fn handle_user_event(&mut self, event: &UserEvent) -> Mqtt5Result<()> {
        match event {
            _ => {
                Err(Mqtt5Error::Unimplemented)
            }
        }
    }

    // Internal APIs
    //fn queue_operation()
}