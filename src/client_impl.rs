extern crate tokio;

use crate::client::*;
use tokio::runtime;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct PublishOptionsInternal {
    pub options: PublishOptions,

    pub response_sender: oneshot::Sender<PublishResult>
}

pub struct SubscribeOptionsInternal {
    pub options: SubscribeOptions,

    pub response_sender: oneshot::Sender<SubscribeResult>
}

pub struct UnsubscribeOptionsInternal {
    pub options: UnsubscribeOptions,

    pub response_sender: oneshot::Sender<UnsubscribeResult>
}

pub enum OperationOptions {
    Publish(PublishOptionsInternal),
    Subscribe(SubscribeOptionsInternal),
    Unsubscribe(UnsubscribeOptionsInternal),
    Start(),
    Stop(),
    Shutdown()
}

struct Mqtt5ClientImpl {
    config: Mqtt5ClientOptions,
    operation_receiver : mpsc::Receiver<OperationOptions>,
}

async fn client_event_loop(client_impl : &mut Mqtt5ClientImpl) {
    loop {
        tokio::select! {
            result = client_impl.operation_receiver.recv() => {
                match result {
                    Some(value) => {
                        match value {
                            OperationOptions::Publish(internal_options) => {
                                println!("Got a publish!");
                                let failure_result : PublishResult = Err(Mqtt5Error::<PublishOptions>::Unimplemented(internal_options.options));
                                internal_options.response_sender.send(failure_result).unwrap();
                            }
                            OperationOptions::Subscribe(internal_options) => {
                                println!("Got a subscribe!");
                                let failure_result : SubscribeResult = Err(Mqtt5Error::<SubscribeOptions>::Unimplemented(internal_options.options));
                                internal_options.response_sender.send(failure_result).unwrap();
                            }
                            OperationOptions::Unsubscribe(internal_options) => {
                                println!("Got an unsubscribe!");
                                let failure_result : UnsubscribeResult = Err(Mqtt5Error::<UnsubscribeOptions>::Unimplemented(internal_options.options));
                                internal_options.response_sender.send(failure_result).unwrap();
                            }
                            _ => {
                                println!("Got some lifecycle operation");
                            }
                        }
                    }
                    _ => {
                    }
                }
            }
        }
    }
}

pub fn spawn_client_impl(config: Mqtt5ClientOptions, operation_receiver: mpsc::Receiver<OperationOptions>, runtime_handle : &runtime::Handle) {
    let mut client_impl = Mqtt5ClientImpl { config, operation_receiver };
    runtime_handle.spawn(async move {
        client_event_loop(&mut client_impl).await;
    });
}
