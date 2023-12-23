/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate mqtt5_client_rs;
extern crate tokio;

use mqtt5_client_rs::client;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::runtime::Handle;

use mqtt5_client_rs::*;

fn client_event_callback(event: Arc<ClientEvent>) {
    match &*event {
        ClientEvent::ConnectionAttempt(_) => {
            println!("Connection Attempt!");
        }
        ClientEvent::ConnectionFailure(_) => {
            println!("Connection Failure!");
        }
        ClientEvent::ConnectionSuccess(_) => {
            println!("Connection Success!");
        }
        ClientEvent::Disconnection(_) => {
            println!("Disconnection!");
        }
        ClientEvent::Stopped(_) => {
            println!("Stopped!");
        }
        ClientEvent::PublishReceived(_) => {
            println!("Publish Received!");
        }
    }
}

fn print_help() {
    // TODO
}

fn handle_start(client: &Mqtt5Client) {
    let _ = client.start();
}

fn handle_stop(client: &Mqtt5Client, args: &[&str]) {
    let mut stop_options = StopOptions {
        disconnect: None,
        mode: StopMode::Soft,
    };

    if args.len() > 0 {
        if let Ok(reason_code_u8) = args[0].parse::<u8>() {
            if let Ok(reason_code) = convert_u8_to_disconnect_reason_code(reason_code_u8) {
                stop_options.disconnect = Some(DisconnectPacket{
                    reason_code,
                    ..Default::default()
                })
            }
        }
    }

    let _ = client.stop(stop_options);
}

fn handle_shutdown(client: &Mqtt5Client, args: &[&str]) {

}

fn handle_publish(client: &Mqtt5Client, args: &[&str]) {

}

fn handle_subscribe(client: &Mqtt5Client, args: &[&str]) {

}

fn handle_unsubscribe(client: &Mqtt5Client, args: &[&str]) {

}

async fn handle_input(value: String, client: &Mqtt5Client) -> bool {
    let fields : Vec<&str> = value.split_whitespace().collect();

    if fields.len() == 0 {
        return false;
    }

    let command = fields[0].to_string();
    match command.to_lowercase().as_str() {
        "start" => {
            handle_start(client);
        }
        "stop" => {
            handle_stop(client, &fields[1..]);
        }
        "quit" => {
            return true;
        }
        "shutdown" => {
            handle_shutdown(client, &fields[1..]);
        }
        "publish" => {
            handle_publish(client, &fields[1..]);
        }
        "subscribe" => {
            handle_subscribe(client, &fields[1..]);
        }
        "unsubscribe" => {
            handle_unsubscribe(client, &fields[1..]);
        }
        _ => {
            print_help();
        }
    }

    false
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let function = |event|{client_event_callback(event)} ;
    let dyn_function = Box::new(function);
    let callback = ClientEventListener::Callback(dyn_function);

    let config = client::Mqtt5ClientOptions {
        connect: Some(Box::new(ConnectPacket{
            keep_alive_interval_seconds: 60,
            client_id: Some("HelloClient".to_string()),
            ..Default::default()
        })),
        offline_queue_policy: OfflineQueuePolicy::PreserveAll,
        rejoin_session_policy: RejoinSessionPolicy::PostSuccess,
        connack_timeout_millis: 60 * 1000,
        ping_timeout_millis: 60 * 1000,
        default_event_listener: Some(callback),
        ..Default::default()
    };
    let runtime_handle = Handle::current();

    let client = client::Mqtt5Client::new(config, &runtime_handle);

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Some(line) = lines.next_line().await? {
        if handle_input(line, &client).await {
            break;
        }
    }

    println!("Done");

    Ok(())
}
