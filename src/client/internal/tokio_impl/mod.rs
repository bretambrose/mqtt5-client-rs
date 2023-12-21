/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate tokio;

use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep};

use crate::client::internal::*;

pub(crate) struct UserRuntimeState {
    operation_sender: tokio::sync::mpsc::Sender<OperationOptions>
}

impl UserRuntimeState {
    pub(crate) fn try_send(&self, operation_options: OperationOptions) -> Mqtt5Result<()> {
        if let Err(_) = self.operation_sender.try_send(operation_options) {
            return Err(Mqtt5Error::OperationChannelSendError);
        }

        Ok(())
    }
}

pub(crate) struct ClientRuntimeState {
    operation_receiver: tokio::sync::mpsc::Receiver<OperationOptions>,

}




impl ClientRuntimeState {

    pub(crate) async fn process_stopped(&mut self, client: &mut Mqtt5ClientImpl) -> Mqtt5Result<ClientImplState> {
        loop {
            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options)?;
                    }
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    pub(crate) async fn process_connecting(&mut self, client: &mut Mqtt5ClientImpl) -> Mqtt5Result<ClientImplState> {

        let connect = TcpStream::connect("127.0.0.1:1883");
        tokio::pin!(connect);

        let timeout = sleep(Duration::from_millis(30 * 1000));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options)?;
                    }
                }
                () = &mut timeout => {
                    return Ok(ClientImplState::PendingReconnect);
                }
                connection_result = &mut connect => {
                    return client.apply_connection_result(connection_result);
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    pub(crate) async fn process_connected(&mut self, client: &mut Mqtt5ClientImpl) -> Mqtt5Result<ClientImplState> {
        let outbound_data : Vec<u8> = Vec::with_capacity(4096);
        let inbound_data : Vec<u8> = Vec::with_capacity(4096);
        let stream = client.connection.take().unwrap();

        loop {
            let next_service_time = client.get_next_connected_service_time();

            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options)?;
                    }
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    pub(crate) async fn process_pending_reconnect(&mut self, client: &mut Mqtt5ClientImpl, wait: Duration) -> Mqtt5Result<ClientImplState> {
        let reconnect_timer = sleep(wait);
        tokio::pin!(reconnect_timer);

        loop {
            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options)?;
                    }
                }
                () = &mut reconnect_timer => {
                    return Ok(ClientImplState::Connecting);
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    /*
async fn select(&mut self, context: &mut AsyncImplSelectContext) -> Mqtt5Result<AsyncImplSelectResult> {

let sleeper: Option<tokio::time::Sleep> =
    if context.next_service_time.is_some() {
        Some(sleep(context.next_service_time.unwrap() - Instant::now()))
    } else {
        None
    };


let mut optional_sleep =
    if context.next_service_time.is_some() {
        Some(sleep(context.next_service_time.unwrap() - Instant::now()))
    } else {
        None
    };

let mut pending = std::future::pending();
let mut sleep_future: &mut dyn Future<Output = ()> = optional_sleep
    .as_mut()
    .unwrap_or(&mut pending);


        //let service = sleep(context.next_service_time - Instant::now());
        //tokio::pin!(service);
        //pin!(service);


        tokio::select! {
            operation_result = self.operation_receiver.recv() => {
                if let Some(operation_options) = operation_result {
                    return Ok(AsyncImplSelectResult::Operation(operation_options));
                }
            }
           Some(_) = conditional_sleeper(sleeper) => {
                 return Ok(AsyncImplSelectResult::Service);
            }
            Some(result) = conditional_connector(connector) => {
                return Ok(AsyncImplSelectResult::Connection(result));
            }
            stream = self.connect_future.await => {
                return Ok(AsyncImplSelectResult::Connection(stream));
            }
            bytes_written = self.write_future.await => {
                return Ok(AsyncImplSelectResult::BytesWritten(bytes_written));
            }

            () = &mut self.service_future => {
                return Ok(AsyncImplSelectResult::Service);
            }

            _ = self.read_future.await => {
                return Ok(AsyncImplSelectResult::BytesRead);
            }
        }

        Err(Mqtt5Error::InternalStateError)
    }

     */


}

/*
async fn conditional_sleeper(t: Option<tokio::time::Sleep>) -> Option<()> {
    match t {
        Some(timer) => Some(timer.await),
        None => None,
    }
}
*/


pub(crate) fn create_runtime_states() -> (UserRuntimeState, ClientRuntimeState) {
    let (sender, receiver) = tokio::sync::mpsc::channel(100);

    let user_state = UserRuntimeState {
        operation_sender: sender
    };

    let impl_state = ClientRuntimeState {
        operation_receiver: receiver
    };

    (user_state, impl_state)
}