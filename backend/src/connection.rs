use df_forum_backend::forum_minimal::ForumMinimal;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{future, pin_mut, stream::TryStreamExt, StreamExt};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::tungstenite::protocol::Message;

use df_forum_frontend::persisted::Persisted;
use df_forum_frontend::query_result::QueryResult;

type Tx = UnboundedSender<Message>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, Tx>>>;

#[derive(Debug)]
pub enum HandlerError {
    Handshake,
    PeerMapLock,
    FailedSocketBind,
}

async fn handle_connection(
    peer_map: PeerMap,
    raw_stream: TcpStream,
    addr: SocketAddr,
    persisted_sender_locked: Arc<Mutex<broadcast::Sender<Vec<Persisted>>>>,
    query_result_receiver_locked: Arc<Mutex<broadcast::Receiver<Vec<QueryResult>>>>,
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

    let broadcast_incoming = {
        incoming.try_for_each(|msg| {
            println!(
                "Received a message from {}: {}",
                addr,
                msg.to_text().unwrap()
            );

            let parsed_msg: Vec<Persisted> =
                serde_json::from_str(&msg.to_text().unwrap_or("[]")).unwrap_or(Vec::new());

            let persisted_sender = persisted_sender_locked.lock().unwrap();
            
            persisted_sender.send(parsed_msg).unwrap();
            // sleep(Duration::from_millis(1)).await;

            if let Ok(query_results) = query_result_receiver_locked.lock().unwrap().try_recv() {
                println!("query results found");
                let output_payload = serde_json::to_string(&query_results.clone()).unwrap();

                let _ = peer_map
                    .lock()
                    .map(move |peers| {
                        let mut broadcast_recipients = peers
                            .iter()
                            .filter(|(peer_addr, _)| peer_addr == &&addr)
                            .map(|(_, ws_sink)| ws_sink);

                        if let Some(recp) = broadcast_recipients.next() {
                            recp.unbounded_send(Message::Text(output_payload)).unwrap();
                        }

                        // for recp in broadcast_recipients {
                        //     let mut forum_minimal = count.lock().unwrap();
                        //     forum_minimal.say_hi();
                        //     let m = if let Message::Text(recieved) = msg.clone() {
                        //         recieved
                        //     } else {
                        //         "__".into()
                        //     };
                        //     let _ = recp
                        //         .unbounded_send(Message::Text(m + "_asdf".into()))
                        //         .map_err(|_err| println!("unbounded send failed: {:?}", msg.clone()));
                        // }
                    })
                    .map_err(|_err| println!("incoming broadcast error"));
            } else {
                println!("none at all query results found");
            }
            
            future::ok(())
        })
    };

    let recieve_from_others = rx.map(Ok).forward(outgoing);

    pin_mut!(broadcast_incoming, recieve_from_others);
    future::select(broadcast_incoming, recieve_from_others).await;

    println!("{} disconnected", &addr);
    peer_map
        .lock()
        .map_err(|_err| HandlerError::PeerMapLock)?
        .remove(&addr);

    Ok(())
}

pub async fn establish(addr: String) -> Result<(), HandlerError> {
    let state = PeerMap::new(Mutex::new(HashMap::new()));

    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.map_err(|_err| HandlerError::FailedSocketBind)?;
    println!("listening on: {}", addr);

    let (query_result_sender, query_result_receiver) = broadcast::channel(16);
    let (persisted_sender, persisted_receiver) = broadcast::channel(16);

    let mut forum_minimal = ForumMinimal::new(persisted_receiver, query_result_sender);

    let persisted_sender_locked = Arc::new(Mutex::new(persisted_sender));
    let query_result_receiver_locked = Arc::new(Mutex::new(query_result_receiver));

    loop {
        if let Ok((stream, addr)) = listener.accept().await {
            tokio::spawn(handle_connection(
                state.clone(),
                stream,
                addr,
                persisted_sender_locked.clone(),
                query_result_receiver_locked.clone(),
            ));
        }
        // TODO run these in parallel
        forum_minimal.advance_dataflow_computation().await;
    }
}
