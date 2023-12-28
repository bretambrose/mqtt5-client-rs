/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate argh;
extern crate mqtt5_client_rs;
extern crate rustls;
extern crate rustls_pemfile;
extern crate simplelog;
extern crate tokio;
extern crate tokio_rustls;
extern crate url;

use argh::FromArgs;
use mqtt5_client_rs::client;
use rustls::pki_types::PrivateKeyDer;
use simplelog::*;
use std::sync::Arc;
use std::fs::File;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio_rustls::client::TlsStream;
use tokio_rustls::{TlsConnector};
use url::Url;

use mqtt5_client_rs::*;

#[derive(FromArgs)]
/// Interactive MQTT5 client
struct CommandLineArgs {

    /// path to the root CA to use when connecting.  If the endpoint URI is a TLS-enabled
    /// scheme and this is not set, then the default system trust store will be used instead.
    #[argh(option)]
    capath: Option<PathBuf>,

    /// path to a client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    certpath: Option<PathBuf>,

    /// path to the private key associated with the client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    keypath: Option<PathBuf>,

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

async fn make_tls_stream(addr: SocketAddr, endpoint: String, connector: TlsConnector) -> std::io::Result<TlsStream<TcpStream>> {
    let tcp_stream = TcpStream::connect(&addr).await?;

    let domain = pki_types::ServerName::try_from(endpoint.as_str())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid dnsname"))?
        .to_owned();

    connector.connect(domain, tcp_stream).await
}

fn load_private_key(keypath: &PathBuf) -> PrivateKeyDer<'static> {
    let mut reader = std::io::BufReader::new(File::open(keypath).expect("cannot open private key file"));

    loop {
        match rustls_pemfile::read_one(&mut reader).expect("cannot parse private key .pem file") {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => return key.into(),
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => return key.into(),
            Some(rustls_pemfile::Item::Sec1Key(key)) => return key.into(),
            None => break,
            _ => {}
        }
    }

    panic!(
        "no keys found in {:?} (encrypted keys not supported)",
        keypath
    );
}

fn build_client(config: Mqtt5ClientOptions, runtime: &Handle, args: &CommandLineArgs) -> std::io::Result<Mqtt5Client> {
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
            Ok(Mqtt5Client::new(config,TokioClientOptions {
                connection_factory: Box::new(move || { Box::pin(TcpStream::connect(addr)) })
            }, runtime))
        }
        "mqtts" => {
            let mut root_cert_store = rustls::RootCertStore::empty();
            if let Some(capath) = &args.capath {
                let mut pem = std::io::BufReader::new(File::open(capath)?);
                for cert in rustls_pemfile::certs(&mut pem) {
                    root_cert_store.add(cert?).unwrap();
                }
            } else {
                for cert in rustls_native_certs::load_native_certs().expect("could not load platform certs") {
                    root_cert_store.add(cert).unwrap();
                }
            }

            let rustls_config_builder = rustls::ClientConfig::builder()
                .with_root_certificates(root_cert_store);

            let rustls_config =
                if args.certpath.is_some() && args.keypath.is_some() {
                    let mut reader = std::io::BufReader::new(File::open(args.certpath.as_ref().unwrap())?);
                    let certs = rustls_pemfile::certs(&mut reader)
                        .map(|result| result.unwrap())
                        .collect();

                    let private_key = load_private_key(&args.keypath.as_ref().unwrap());

                    rustls_config_builder.with_client_auth_cert(certs, private_key).unwrap()
                } else {
                    rustls_config_builder.with_no_client_auth()
                };

            let connector = TlsConnector::from(Arc::new(rustls_config));

            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    Box::pin(make_tls_stream(addr, endpoint.clone(), connector.clone()))
                })
            };

            Ok(Mqtt5Client::new(config, tokio_options, runtime))
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

    let client = build_client(config, &Handle::current(), &cli_args).unwrap();

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
