/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate tokio;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::WriteHalf;
use tokio::net::TcpStream;
use tokio::time::{sleep};

use crate::client::internal::*;



pub(crate) struct UserRuntimeState {
    operation_sender: tokio::sync::mpsc::Sender<OperationOptions>
}

impl UserRuntimeState {
    pub(crate) fn try_send(&self, operation_options: OperationOptions) -> Mqtt5Result<()> {
        if self.operation_sender.try_send(operation_options).is_err() {
            return Err(Mqtt5Error::OperationChannelSendError);
        }

        Ok(())
    }
}

pub(crate) struct ClientRuntimeState {
    tokio_config: TokioClientOptions,
    operation_receiver: tokio::sync::mpsc::Receiver<OperationOptions>,
    stream: Option<TcpStream>
}

impl ClientRuntimeState {
    pub(crate) async fn process_stopped(&mut self, client: &mut Mqtt5ClientImpl) -> Mqtt5Result<ClientImplState> {
        loop {
            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options);
                    }
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    pub(crate) async fn process_connecting(&mut self, client: &mut Mqtt5ClientImpl) -> Mqtt5Result<ClientImplState> {
        let mut connect = (self.tokio_config.connection_factory)();

        let timeout = sleep(Duration::from_millis(30 * 1000));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options);
                    }
                }
                () = &mut timeout => {
                    client.apply_error(Mqtt5Error::ConnectionTimeout);
                    return Ok(ClientImplState::PendingReconnect);
                }
                connection_result = &mut connect => {
                    if let Ok(stream) = connection_result {
                        self.stream = Some(stream);
                        return Ok(ClientImplState::Connected);
                    } else {
                        client.apply_error(Mqtt5Error::ConnectionEstablishmentFailure);
                        return Ok(ClientImplState::PendingReconnect);
                    }
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    pub(crate) async fn process_connected(&mut self, client: &mut Mqtt5ClientImpl) -> Mqtt5Result<ClientImplState> {
        let mut outbound_data: Vec<u8> = Vec::with_capacity(4096);
        let mut cumulative_bytes_written : usize = 0;

        let mut inbound_data: [u8; 4096] = [0; 4096];

        let mut stream = self.stream.take().unwrap();
        let (stream_reader, mut stream_writer) = stream.split();
        tokio::pin!(stream_reader);

        let mut next_state = None;
        while next_state.is_none() {
            let next_service_time_option = client.get_next_connected_service_time();
            let service_wait: Option<tokio::time::Sleep> = next_service_time_option.map(|next_service_time| sleep(next_service_time - Instant::now()));

            let outbound_slice_option: Option<&[u8]> =
                if cumulative_bytes_written < outbound_data.len() {
                    Some(&outbound_data[cumulative_bytes_written..])
                } else {
                    None
                };

            tokio::select! {
                // incoming user operations future
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options);
                    }
                }
                // incoming data on the socket future
                read_result = stream_reader.read(inbound_data.as_mut_slice()) => {
                    match read_result {
                        Ok(bytes_read) => {
                            if bytes_read == 0 {
                                client.apply_error(Mqtt5Error::ConnectionClosed);
                                next_state = Some(ClientImplState::PendingReconnect);
                            } else if client.handle_incoming_bytes(&inbound_data[..bytes_read]).is_err() {
                                next_state = Some(ClientImplState::PendingReconnect);
                            }
                        }
                        Err(_) => {
                            client.apply_error(Mqtt5Error::StreamReadFailure);
                            next_state = Some(ClientImplState::PendingReconnect);
                        }
                    }
                }
                // client service future (if relevant)
                Some(_) = conditional_wait(service_wait) => {
                    if client.handle_service(&mut outbound_data).is_err() {
                        next_state = Some(ClientImplState::PendingReconnect);
                    }
                }
                // outbound data future (if relevant)
                Some(bytes_written_result) = conditional_write(outbound_slice_option, &mut stream_writer) => {
                    match bytes_written_result {
                        Ok(bytes_written) => {
                            cumulative_bytes_written += bytes_written;
                            if cumulative_bytes_written == outbound_data.len() {
                                outbound_data.clear();
                                cumulative_bytes_written = 0;
                                if client.handle_write_completion().is_err() {
                                    next_state = Some(ClientImplState::PendingReconnect);
                                }
                            }
                        }
                        Err(_) => {
                            client.apply_error(Mqtt5Error::StreamWriteFailure);
                            next_state = Some(ClientImplState::PendingReconnect);
                        }
                    }
                }
            }

            if next_state.is_none() {
                next_state = client.compute_optional_state_transition();
            }
        }

        let _ = stream.flush().await;
        let _ = stream.shutdown().await;

        Ok(next_state.unwrap())
    }

    pub(crate) async fn process_pending_reconnect(&mut self, client: &mut Mqtt5ClientImpl, wait: Duration) -> Mqtt5Result<ClientImplState> {
        let reconnect_timer = sleep(wait);
        tokio::pin!(reconnect_timer);

        loop {
            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options);
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
}

async fn conditional_wait(wait_option: Option<tokio::time::Sleep>) -> Option<()> {
    match wait_option {
        Some(timer) => {
            timer.await;
            Some(())
        },
        None => None,
    }
}

async fn conditional_write(bytes_option: Option<&[u8]>, writer: &mut WriteHalf<'_>) -> Option<std::io::Result<usize>> {
    match bytes_option {
        Some(bytes) => Some(writer.write(bytes).await),
        None => None,
    }
}

pub(crate) fn create_runtime_states(tokio_config: TokioClientOptions) -> (UserRuntimeState, ClientRuntimeState) {
    let (sender, receiver) = tokio::sync::mpsc::channel(100);

    let user_state = UserRuntimeState {
        operation_sender: sender
    };

    let impl_state = ClientRuntimeState {
        tokio_config,
        operation_receiver: receiver,
        stream: None
    };

    (user_state, impl_state)
}