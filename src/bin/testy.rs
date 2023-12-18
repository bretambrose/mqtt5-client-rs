/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate mqtt5_client_rs;
extern crate tokio;

use mqtt5_client_rs::client;
use std::sync::Arc;
use std::{thread, time};
use tokio::runtime::Handle;

use mqtt5_client_rs::*;

fn test_callback(event: Arc<ClientEvent>) {
    println!("Client event!");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let function = |event|{test_callback(event)} ;
    let dyn_function = Box::new(function);
    let callback = ClientEventListener::Callback(dyn_function);

    let config = client::Mqtt5ClientOptions {
        default_event_listener: Some(callback),
        ..Default::default()
    };
    let runtime_handle = Handle::current();

    let client = client::Mqtt5Client::new(config, &runtime_handle);

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

    client.close().expect("Hello");

    let sleep_duration = time::Duration::from_secs(2);

    thread::sleep(sleep_duration);

    println!("Done");

    Ok(())
}
