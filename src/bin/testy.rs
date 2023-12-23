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

async fn handle_input(value: String, client: &Mqtt5Client) -> bool {
    let fields : Vec<&str> = value.split_whitespace().collect();

    if fields.len() == 0 {
        return false;
    }

    let command = fields[0].to_string();
    if command.to_lowercase() == "start" {
        let _ = client.start();
    } else if command.to_lowercase() == "stop" {
        let stop_options = StopOptions {
            disconnect: None,
            mode: StopMode::Hard,
        };
        let _ = client.stop(stop_options);
    } else if command.to_lowercase() == "quit" {
        return true;
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

    /*
    client.start().unwrap();

    let result = client
        .publish( PublishPacket { ..Default::default() } , client::PublishOptions { ..Default::default() })
        .await;
    match result {
        Ok(_) => {
            println!("Got a publish result!");
        }
        Err(_) => {
            println!("Got a publish error");
        }
    }

    loop {
        thread::sleep(time::Duration::from_secs(1));
    }

    client.close().expect("Hello");

    let sleep_duration = time::Duration::from_secs(2);

    thread::sleep(sleep_duration);
*/

    println!("Done");

    Ok(())
}
