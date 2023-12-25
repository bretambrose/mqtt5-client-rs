/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate mqtt5_client_rs;
extern crate tokio;
extern crate simplelog;

use mqtt5_client_rs::client;
use simplelog::*;
use std::sync::Arc;
use std::fs::File;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::runtime::Handle;

use mqtt5_client_rs::*;

fn client_event_callback(event: Arc<ClientEvent>) {
    match &*event {
        ClientEvent::ConnectionAttempt(_) => {
            println!("Connection Attempt!");
        }
        ClientEvent::ConnectionFailure(event) => {
            println!("Connection Failure: {:?}", event);
        }
        ClientEvent::ConnectionSuccess(event) => {
            println!("Connection Success: {:?}", event);
        }
        ClientEvent::Disconnection(event) => {
            println!("Disconnection: {:?}", event);
        }
        ClientEvent::Stopped(_) => {
            println!("Stopped!");
        }
        ClientEvent::PublishReceived(event) => {
            println!("Publish Received: {:?}", &event.publish);
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
    let mut stop_options_builder = StopOptionsBuilder::new();

    if args.len() > 0 {
        if let Ok(reason_code_u8) = args[0].parse::<u8>() {
            if let Ok(reason_code) = convert_u8_to_disconnect_reason_code(reason_code_u8) {
                stop_options_builder.set_disconnect_packet(DisconnectPacket{
                    reason_code,
                    ..Default::default()
                });
            }
        }
    }

    let _ = client.stop(stop_options_builder.build());
}

fn handle_close(client: &Mqtt5Client) {
    let _ = client.close();
}

async fn handle_publish(client: &Mqtt5Client, args: &[&str]) {
    if args.len() < 2 || args.len() > 3 {
        return;
    }

    let mut publish = PublishPacket {
        qos: QualityOfService::AtMostOnce,
        topic: args[0].to_string(),
        ..Default::default()
    };

    if let Ok(qos_u8) = args[1].parse::<u8>() {
        if let Ok(qos) = convert_u8_to_quality_of_service(qos_u8) {
            publish.qos = qos;
        }
    }

    if args.len() == 3 {
        publish.payload = Some(args[2].as_bytes().to_vec());
    }

    let publish_result = client.publish(publish, PublishOptionsBuilder::new().build()).await;

    println!("Publish Result: {:?}", publish_result);
}

async fn handle_subscribe(client: &Mqtt5Client, args: &[&str]) {
    if args.len() < 1 || args.len() > 2 {
        return;
    }

    let mut subscribe_qos = QualityOfService::AtMostOnce;

    if args.len() == 2 {
        if let Ok(qos_u8) = args[1].parse::<u8>() {
            if let Ok(qos) = convert_u8_to_quality_of_service(qos_u8) {
                subscribe_qos = qos;
            }
        }
    }

    let subscribe = SubscribePacket {
        subscriptions: vec!(
            Subscription {
                qos: subscribe_qos,
                topic_filter: args[0].to_string(),
                ..Default::default()
            }
        ),
        ..Default::default()
    };

    let subscribe_result = client.subscribe(subscribe, SubscribeOptionsBuilder::new().build()).await;

    println!("Subscribe Result: {:?}", subscribe_result);
}

async fn handle_unsubscribe(client: &Mqtt5Client, args: &[&str]) {
    if args.len() != 1 {
        return;
    }

    let unsubscribe = UnsubscribePacket {
        topic_filters: vec!(
            args[0].to_string()
        ),
        ..Default::default()
    };

    let unsubscribe_result = client.unsubscribe(unsubscribe, UnsubscribeOptionsBuilder::new().build()).await;

    println!("Unsubscribe Result: {:?}", unsubscribe_result);
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
        "close" => {
            handle_close(client);
        }
        "publish" => {
            handle_publish(client, &fields[1..]).await;
        }
        "subscribe" => {
            handle_subscribe(client, &fields[1..]).await;
        }
        "unsubscribe" => {
            handle_unsubscribe(client, &fields[1..]).await;
        }
        _ => {
            print_help();
        }
    }

    false
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let log_file = File::create("/tmp/elastimqtt5.txt")?;

    let mut log_config_builder = simplelog::ConfigBuilder::new();
    let log_config = log_config_builder.build();

    WriteLogger::init(LevelFilter::Debug, log_config, log_file).unwrap();

    let function = |event|{client_event_callback(event)} ;
    let dyn_function = Box::new(function);
    let callback = ClientEventListener::Callback(dyn_function);

    let connect_options = ConnectOptionsBuilder::new()
        .with_keep_alive_interval_seconds(60)
        .with_client_id("HelloClient")
        .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
        .build();

    let config = client::Mqtt5ClientOptionsBuilder::new()
        .with_connect_options(connect_options)
        .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
        .with_connack_timeout(Duration::from_secs(60))
        .with_ping_timeout(Duration::from_secs(60))
        .with_default_event_listener(callback)
        .build();

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
