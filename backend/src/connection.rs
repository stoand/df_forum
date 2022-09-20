use df_forum_backend::forum_minimal::ForumMinimal;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{future, pin_mut, stream::TryStreamExt, StreamExt};

use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::Message;

use df_forum_frontend::persisted::Persisted;
use df_forum_frontend::query_result::QueryResult;

use std::cell::RefCell;
use std::rc::Rc;

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
    count: Arc<Mutex<ForumMinimal>>,
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
        let fake_payload = vec![Persisted::Post {
            title: "Cool Stuff".into(),
            user_id: 20,
            likes: 0,
        }];
        let fake_msg = serde_json::to_string(&fake_payload).unwrap();

        println!("recieved fake message:\n{}", fake_msg);

        println!(
            "Received a message from {}: {}",
            addr,
            msg.to_text().unwrap()
        );

        let parsed_fake_msg: Vec<Persisted> = serde_json::from_str(&fake_msg).unwrap();

        let mut forum_minimal = count.lock().unwrap();
        forum_minimal.say_hi();
        forum_minimal.new_persisted_transaction(parsed_fake_msg);

        let _ = peer_map
            .lock()
            .map(move |peers| {
                let mut broadcast_recipients = peers
                    .iter()
                    .filter(|(peer_addr, _)| peer_addr == &&addr)
                    .map(|(_, ws_sink)| ws_sink);

                if let Some(recp) = broadcast_recipients.next() {
                    
                    let output0 = Rc::new(RefCell::new(Vec::new()));
                    let output1 = output0.clone();
                    
                    forum_minimal.compute_forum(output0);

                    let output0_vec : &Vec<QueryResult> = &*output1.borrow();
                    let output_payload = serde_json::to_string(output0_vec).unwrap();
                    
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
        future::ok(())
    });

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

    let forum_minimal = Arc::new(Mutex::new(ForumMinimal::new()));

    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.map_err(|_err| HandlerError::FailedSocketBind)?;
    println!("listening on: {}", addr);

    loop {
        if let Ok((stream, addr)) = listener.accept().await {
            tokio::spawn(handle_connection(
                state.clone(),
                stream,
                addr,
                forum_minimal.clone(),
            ));
        }
    }
}
