use std::sync::Arc;

use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use futures::executor::block_on;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use log::{debug, info};
use std::collections::HashSet;
use std::vec::Vec;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tungstenite::protocol::Message;
use uuid::Uuid;

struct ReferenceMaps {
    // key: channel_id, val: [session_ids]
    channel_map: DashMap<String, HashSet<String>>,
    // key: session_id, val: [channel_ids]
    session_map: DashMap<String, HashSet<String>>,
    // key: session_id, val: [streams]
    stream_map: StreamMap,
}

impl ReferenceMaps {
    pub fn new() -> ReferenceMaps {
        ReferenceMaps {
            channel_map: DashMap::new(),
            session_map: DashMap::new(),
            stream_map: DashMap::new(),
        }
    }
}

type StreamMap = DashMap<String, SplitSink<WebSocketStream<TcpStream>, Message>>;

fn bind_session_to_room(
    refm: Arc<ReferenceMaps>,
    session_id: &str,
    channel_id: &str,
    stream: SplitSink<WebSocketStream<TcpStream>, Message>,
) {
    let mut c_set = refm
        .session_map
        .entry(session_id.to_owned())
        .or_insert(HashSet::new());
    if !c_set.contains(channel_id) {
        c_set.insert(channel_id.to_owned());
    }
    let mut s_set = refm
        .channel_map
        .entry(channel_id.to_owned())
        .or_insert(HashSet::new());
    if !s_set.contains(session_id) {
        s_set.insert(session_id.to_owned());
    }
    if !refm.stream_map.contains_key(session_id) {
        refm.stream_map.insert(session_id.to_owned(), stream);
    }
}

fn unbind(refm: Arc<ReferenceMaps>, session_id: &str) {
    refm.stream_map.remove(session_id);
    match refm.session_map.get(session_id) {
        None => {}
        Some(channel_set) => {
            channel_set
                .iter()
                .for_each(|channel_id| match refm.channel_map.get_mut(channel_id) {
                    None => {}
                    Some(mut x) => {
                        x.remove(session_id);
                    }
                });
        }
    };
    refm.session_map.remove(session_id);
}

async fn send(refm: Arc<ReferenceMaps>, channel_id: &str, payload: &str) {
    let session_set: Ref<String, HashSet<String>>;
    match refm.channel_map.get(channel_id) {
        None => {
            debug!("no session set found for channel: {}", channel_id);
            return;
        }
        Some(x) => {
            session_set = x;
        }
    }
    let mut error_session: Vec<String> = Vec::new();
    session_set.iter().for_each(|session_id| {
        let mut w = refm.stream_map.get_mut(session_id).unwrap();
        match block_on(w.send(Message::Text(payload.to_string()))) {
            Ok(_) => debug!("sent: {}", payload),
            Err(err) => {
                error_session.push(session_id.to_owned());
                debug!("error send: {}", err)
            }
        }
    });
    drop(session_set);
    for e in error_session {
        unbind(refm.clone(), &e);
    }
}

#[tokio::main]
async fn main() {
    let mut settings = config::Config::default();
    settings
        .merge(config::File::with_name("config/config.yaml"))
        .unwrap()
        .merge(config::Environment::with_prefix("SOCKET"))
        .unwrap();
    let refm = Arc::new(ReferenceMaps::new());
    tokio::spawn(redis_sub(refm.clone()));
    let addr = "0.0.0.0:".to_string() + &settings.get_str("socket.port").unwrap();
    let _ = env_logger::try_init();
    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(&addr).await;
    let mut listener = try_socket.expect("Failed to bind");
    info!("Listening on: {}", addr);
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(accept_connection(stream, refm.clone()));
    }
}

async fn accept_connection(stream: TcpStream, refm: Arc<ReferenceMaps>) {
    let addr = stream
        .peer_addr()
        .expect("connected streams should have a peer address");
    info!("Peer address: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Error during the websocket handshake occurred");
    let session_id = Uuid::new_v4();
    info!(
        "New WebSocket connection: {}, session_id: {}",
        addr, session_id
    );
    let (write, _read) = ws_stream.split();
    bind_session_to_room(
        refm,
        &session_id.to_string(),
        "5c886c69ca50950001362635.5cb69d4d85c0790001b2fdff.message",
        write,
    )
}

async fn redis_sub(refm: Arc<ReferenceMaps>) {
    let client =
        redis::Client::open("redis://127.0.0.1:6379/").expect("Error opening redis client");
    let mut con = client
        .get_connection()
        .expect("Error getting redis connection");
    let mut pubsub = con.as_pubsub();
    pubsub.psubscribe("*").expect("Error creating pubsub");
    loop {
        let msg = pubsub.get_message().expect("Error getting pubsub message");
        let payload: String = msg.get_payload().expect("Error getting message payload");
        debug!("received: {}", payload);
        send(refm.clone(), msg.get_channel_name(), &payload).await;
    }
}
