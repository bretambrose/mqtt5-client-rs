/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate mqtt5_client_rs;
extern crate tokio;

use mqtt5_client_rs::packet::PublishPacket;
use mqtt5_client_rs::client;
use tokio::runtime::Runtime;
use std::time;
use std::thread;
use tokio::runtime::Handle;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = client::Mqtt5ClientOptions {};
    let runtime_handle = Handle::current();

    let client = client::Mqtt5Client::new(config, &runtime_handle);

    let result = client.start();

    let result = client.publish(client::PublishOptions{ publish: Default::default() });
    match result {
        Ok(real_result) => {
            let res = real_result.await;
            println!("Got a publish result!");
        }
        Err(_) => {
            println!("WTF");
        }
    }

    println!("Done");

    Ok(())
}
