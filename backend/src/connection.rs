use df_forum_backend::forum_minimal::{ForumMinimal, PersistedItems};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{future, pin_mut, StreamExt};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::protocol::Message;

// use df_forum_frontend::persisted::Persisted;
use df_forum_frontend::query_result::{Query, QueryResult};

type Tx = UnboundedSender<Message>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, Tx>>>;

#[derive(Debug)]
pub enum HandlerError {
    Handshake,
    // PeerMapLock,
    // FailedSocketBind,
}

async fn handle_connection(
    _peer_map: PeerMap,
    raw_stream: TcpStream,
    _addr: SocketAddr,
    persisted_sender: broadcast::Sender<PersistedItems>,
    query_result_sender: broadcast::Sender<Vec<(Query, QueryResult)>>,
) -> Result<(), HandlerError> {
    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .map_err(|_err| HandlerError::Handshake)?;

    let (tx, rx) = unbounded();

    let (outgoing, incoming) = ws_stream.split();

    let mut incoming_strings =
        incoming.map(|msg| msg.unwrap().to_text().unwrap_or("[]").to_string());

    let mut query_result_receiver = query_result_sender.subscribe();

    let broadcast_incoming = tokio::spawn(async move {
        while let Some(msg) = incoming_strings.next().await {
            println!("got msg: {}", msg);

            let parsed_msg: PersistedItems = serde_json::from_str(&msg)
                .expect("Could not parse PersistedItems from Websocket Message");
            persisted_sender.send(parsed_msg).unwrap();
        }
    });

    tokio::spawn(async move {
        loop {
            let query_results = query_result_receiver.recv().await.unwrap();
            println!("query_results: {:?}", query_results);

            let output_payload = serde_json::to_string(&query_results.clone()).unwrap();

            tx.unbounded_send(Message::Text(output_payload)).unwrap();
        }
    });
    let recieve_from_others = rx.map(Ok).forward(outgoing);

    pin_mut!(broadcast_incoming, recieve_from_others);
    future::select(broadcast_incoming, recieve_from_others).await;

    Ok(())
}

async fn loop_check_for_connections(
    addr: String,
    persisted_sender: broadcast::Sender<PersistedItems>,
    query_result_sender: broadcast::Sender<Vec<(Query, QueryResult)>>,
) {
    let state = PeerMap::new(Mutex::new(HashMap::new()));

    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.unwrap();
    println!("listening on: {}", addr);

    loop {
        if let Ok((stream, addr)) = listener.accept().await {
            tokio::spawn(handle_connection(
                state.clone(),
                stream,
                addr,
                persisted_sender.clone(),
                query_result_sender.clone(),
            ));
        }
    }
}

pub async fn establish(addr: String) -> Result<(), HandlerError> {
    let (query_result_sender, _query_result_receiver) = broadcast::channel(64);
    let (persisted_sender, _persisted_receiver) = broadcast::channel(64);

    let mut forum_minimal =
        ForumMinimal::new(persisted_sender.clone(), query_result_sender.clone());

    tokio::join!(
        loop_check_for_connections(addr, persisted_sender, query_result_sender),
        forum_minimal.loop_advance_dataflow_computation(),
    );

    Ok(())
}
