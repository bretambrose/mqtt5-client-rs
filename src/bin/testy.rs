
extern crate mqtt5_client_rs;

use mqtt5_client_rs::packet as mqtt5_packet;

fn main() {
    let disconnect = mqtt5_packet::DisconnectPacket {
        reason_code : mqtt5_packet::DisconnectReasonCode::NormalDisconnection,
        ..Default::default()
    };

    if disconnect.user_properties.is_some() {
        println!("Something!");
    } else {
        println!("nothing");
    }

    println!("Derp");
}
