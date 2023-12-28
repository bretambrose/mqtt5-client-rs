/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate argh;
extern crate mqtt5_client_rs;
extern crate rustls;
extern crate simplelog;
extern crate tokio;
extern crate tokio_rustls;
extern crate url;

use argh::FromArgs;
use mqtt5_client_rs::client;
use simplelog::*;
use std::sync::Arc;
use std::fs::File;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader, split};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use url::Url;

use mqtt5_client_rs::*;

#[derive(FromArgs)]
/// Interactive MQTT5 client
struct CommandLineArgs {

    /// path to the root CA to use when connecting.  If the endpoint URI is a TLS-enabled
    /// scheme and this is not set, then the default system trust store will be used instead.
    #[argh(option)]
    capath: Option<String>,

    /// path to a client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    certpath: Option<String>,

    /// path to the private key associated with the client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    keypath: Option<String>,

    /// URI of endpoint to connect to.  Supported schemes include `mqtt` and `mqtts`
    #[argh(positional)]
    endpoint_uri: String,
}

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

    if !args.is_empty() {
        if let Ok(reason_code_u8) = args[0].parse::<u8>() {
            if let Ok(reason_code) = convert_u8_to_disconnect_reason_code(reason_code_u8) {
                stop_options_builder = stop_options_builder.with_disconnect_packet(DisconnectPacket{
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

    let mut publish = PublishPacket::new_empty(args[0], QualityOfService::AtLeastOnce);

    if let Ok(qos_u8) = args[1].parse::<u8>() {
        if let Ok(qos) = convert_u8_to_quality_of_service(qos_u8) {
            publish.qos = qos;
        }
    }

    if args.len() == 3 {
        publish.payload = Some(args[2].as_bytes().to_vec());
    }

    let publish_result = client.publish(publish, PublishOptionsBuilder::new().with_timeout(Duration::from_secs(5)).build()).await;

    println!("Publish Result: {:?}", publish_result);
}

async fn handle_subscribe(client: &Mqtt5Client, args: &[&str]) {
    if args.is_empty() || args.len() > 2 {
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
            Subscription::new(args[0], subscribe_qos)
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

    if fields.is_empty() {
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

use client::internal::tokio_impl::*;

struct TcpStreamWrapper {
    stream : TcpStream
}

impl AsyncTokioStream for TcpStreamWrapper {
    fn split(&mut self) -> (ReadHalf, WriteHalf) {
        self.stream.split()
    }
}

impl TcpStreamWrapper {
    async fn connect(address: SocketAddr) -> std::io::Result<Box<dyn AsyncTokioStream + Sync + Send>> {
        let stream = TcpStream::connect(address).await?;
        let boxed_stream = Box::new(TcpStreamWrapper {
            stream
        });
        Ok(boxed_stream)
    }
}

fn build_client_tokio_options(args: CommandLineArgs) -> std::io::Result<TokioClientOptions> {
    let url_parse_result = Url::parse(&args.endpoint_uri);
    if let Err(_) = url_parse_result {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid URL!"));
    }

    let uri = url_parse_result.unwrap();
    if uri.host_str().is_none() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "URL must specify host!"));
    }

    let endpoint = uri.host_str().unwrap().to_string();

    if uri.port().is_none() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "URL must specify port!"));
    }

    let port = uri.port().unwrap();

    let to_socket_addrs = (endpoint.clone(), port).to_socket_addrs();
    if let Err(_) = to_socket_addrs {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to convert URL to address!"));
    }

    let addr = to_socket_addrs.unwrap().next().unwrap();

    match uri.scheme().to_lowercase().as_str() {
        "mqtt" => {
            Ok(TokioClientOptions {
                connection_factory: Box::new(move || { Box::pin(TcpStreamWrapper::connect(addr)) })
            })
        }
        _ => {
            Err(std::io::Error::new(std::io::ErrorKind::Other, "Unsupported scheme in URL!"))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli_args: CommandLineArgs = argh::from_env();

    let log_file_result = File::create("/tmp/elastimqtt5.txt");
    if let Err(_) = log_file_result {
        println!("Could not create log file");
        return Ok(());
    }

    let mut log_config_builder = simplelog::ConfigBuilder::new();
    let log_config = log_config_builder.build();
    WriteLogger::init(LevelFilter::Debug, log_config, log_file_result.unwrap()).unwrap();

    let function = |event|{client_event_callback(event)} ;
    let dyn_function = Arc::new(function);
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
        .with_reconnect_period_jitter(ExponentialBackoffJitterType::None)
        .build();

    let runtime_handle = Handle::current();
    let tokio_options = build_client_tokio_options(cli_args);
    if let Err(err) = tokio_options {
        println!("Failed to build tokio client options: {:?}", err);
        return Ok(());
    }

    let client = client::Mqtt5Client::new(config, tokio_options.unwrap(), &runtime_handle);

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if handle_input(line, &client).await {
            break;
        }
    }

    println!("Done");

    Ok(())
}
