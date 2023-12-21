/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate tokio;

use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep};

use crate::client::internal::*;

pub(crate) struct AsyncUserState {
    operation_sender: tokio::sync::mpsc::Sender<OperationOptions>
}

impl AsyncUserState {
    pub(crate) fn try_send(&self, operation_options: OperationOptions) -> Mqtt5Result<()> {
        if let Err(_) = self.operation_sender.try_send(operation_options) {
            return Err(Mqtt5Error::OperationChannelSendError);
        }

        Ok(())
    }
}

pub(crate) struct AsyncImplState {
    operation_receiver: tokio::sync::mpsc::Receiver<OperationOptions>,

}



impl AsyncImplState {

    pub async fn process_stopped(&mut self, client: &mut Mqtt5ClientImpl) -> Mqtt5Result<()> {
        loop {
            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options)?;
                    }
                }
            }

            if client.desired_state != ClientImplState::Stopped {
                return Ok(())
            }
        }
    }

    async fn process_connecting(&mut self, client: &mut Mqtt5ClientImpl) -> Mqtt5Result<std::io::Result<TcpStream>> {

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
                    return Err(Mqtt5Error::ConnectionTimeout);
                }
                result = &mut connect => {
                    return Ok(result);
                }
            }
        }
    }

    async fn process_connected(&mut self, _: &mut Mqtt5ClientImpl) -> Mqtt5Result<()> {
        Ok(())
    }

    async fn process_pending_reconnect(&mut self, _: &mut Mqtt5ClientImpl) -> Mqtt5Result<()> {
        Ok(())
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


pub(crate) fn create_async_state() -> (AsyncUserState, AsyncImplState) {
    let (sender, receiver) = tokio::sync::mpsc::channel(100);

    let user_state = AsyncUserState {
        operation_sender: sender
    };

    let impl_state = AsyncImplState {
        operation_receiver: receiver
    };

    (user_state, impl_state)
}