/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate mqtt5_client_rs;
extern crate tokio;

use mqtt5_client_rs::client;
use tokio::runtime::Handle;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = client::Mqtt5ClientOptions {};
    let runtime_handle = Handle::current();

    let client = client::Mqtt5Client::new(config, &runtime_handle);

    client.start().unwrap();

    let result = client.publish(client::PublishOptions{ publish: Default::default() }).await;
    match result {
        Ok(_) => {
            println!("Got a publish result!");
        }
        Err(_) => {
            println!("Got a publish error");
        }
    }

    println!("Done");

    Ok(())
}
