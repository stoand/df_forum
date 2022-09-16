extern crate df_forum_backend;
extern crate futures_channel;
extern crate futures_util;
extern crate tokio;
extern crate tokio_tungstenite;

static PORT: u32 = 5050;

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{future, pin_mut, stream::TryStreamExt, StreamExt};

use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::Message;

type Tx = UnboundedSender<Message>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, Tx>>>;

#[derive(Debug)]
enum HandlerError {
    Handshake,
    PeerMapLock,
    FailedSocketBind,
}

async fn handle_connection(
    peer_map: PeerMap,
    raw_stream: TcpStream,
    addr: SocketAddr,
) -> Result<(), HandlerError> {
    println!("tcp connection from: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .map_err(|_err| HandlerError::Handshake)?;

    let (tx, rx) = unbounded();
    peer_map
        .lock()
        .map_err(|_err| HandlerError::Handshake)?
        .insert(addr, tx);

    let (outgoing, incoming) = ws_stream.split();

    // We should not forward messages other than text or binary.
    incoming //.try_filter(|msg| future::ready(msg.is_text() || msg.is_binary()))
        .forward(outgoing)
        .await
        .expect("Failed to forward messages");

    // let broadcast_incoming = incoming.try_for_each(|msg| {
    //     println!("Received a message from {}: {}", addr, msg.to_text().unwrap());
    //     let _ = peer_map
    //         .lock()
    //         .map(|peers| {
    //             let broadcast_recipients = peers
    //                 .iter()
    //                 .filter(|(peer_addr, _)| peer_addr != &&addr)
    //                 .map(|(_, ws_sink)| ws_sink);

    //             for recp in broadcast_recipients {
    //                 let _ = recp
    //                     .unbounded_send(msg.clone())
    //                     .map_err(|_err| println!("unbounded send failed: {:?}", msg.clone()));
    //             }
    //         })
    //         .map_err(|_err| println!("incoming broadcast error"));
    //     future::ok(())
    // });

    // let recieve_from_others = rx.map(Ok).forward(outgoing);

    // pin_mut!(broadcast_incoming, recieve_from_others);
    // future::select(broadcast_incoming, recieve_from_others).await;

    // println!("{} disconnected", &addr);
    // peer_map
    //     .lock()
    //     .map_err(|_err| HandlerError::PeerMapLock)?
    //     .remove(&addr);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), HandlerError> {
    let addr = "127.0.0.1:".to_owned() + &PORT.to_string();

    let state = PeerMap::new(Mutex::new(HashMap::new()));

    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.map_err(|_err| HandlerError::FailedSocketBind)?;
    println!("listening on: {}", addr);

    loop {
        if let Ok((stream, addr)) = listener.accept().await {
            tokio::spawn(handle_connection(state.clone(), stream, addr));
        }
    }
}
