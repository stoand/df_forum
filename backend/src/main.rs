extern crate df_forum_backend;
extern crate futures_channel;
extern crate futures_util;
extern crate tokio;
extern crate tokio_tungstenite;

use std::{
    collections::HashMap,
    env,
    io::Error as IoError,
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

    let broadcast_incoming = incoming.try_for_each(|msg| {
        let _ = peer_map
            .lock()
            .map(|peers| {
                let broadcast_recipients = peers
                    .iter()
                    .filter(|(peer_addr, _)| peer_addr != &&addr)
                    .map(|(_, ws_sink)| ws_sink);

                for recp in broadcast_recipients {
                    let _ = recp
                        .unbounded_send(msg.clone())
                        .map_err(|_err| println!("unbounded send failed: {:?}", msg.clone()));
                }
            })
            .map_err(|_err| println!("incoming broadcast error"));
        future::ok(())
    });

    let recieve_from_others = rx.map(Ok).forward(outgoing);

    pin_mut!(broadcast_incoming, recieve_from_others);
    future::select(broadcast_incoming, recieve_from_others).await;

    println!("{} disconnected", &addr);
    peer_map.lock().map_err(|_err| HandlerError::PeerMapLock)?.remove(&addr);

    Result::Ok(())
}

fn main() {
    println!("Hello, world!");
}
