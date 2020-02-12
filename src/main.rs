use std::{io::Error};
use std::sync::{Arc, RwLock};

use futures::stream::SplitSink;
use log::{info, debug};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tungstenite::protocol::Message;
use std::collections::{HashMap, HashSet};
use futures::{StreamExt, SinkExt};
use chashmap::CHashMap;
use uuid::Uuid;

type RoomMap = CHashMap<String, HashSet<String>>;

type SessionMap = CHashMap<String, SplitSink<WebSocketStream<TcpStream>, Message>>;

fn r_insert(map: Arc<RoomMap>, k: &str, v: &str) {
    map.upsert(k.to_owned(),
               || {
                   let mut set = HashSet::new();
                   set.insert(v.to_owned());
                   set
               },
               |set|
                   {
                       if !set.contains(v) {
                           set.insert(v.to_owned());
                       }
                   });
}

#[test]
fn test_room_map() {
    let r = Arc::new(RoomMap::new());
    let size = 50;
    for i in 0..size {
        for j in 0..size {
            let c = r.clone();
            thread::spawn(move || {
                r_insert(c, &i.to_string(), &j.to_string());
            });
        }
    }
    let ten_millis = time::Duration::from_millis(100);
    thread::sleep(ten_millis);
    for i in 0..size {
        let c = r.clone();
        let set = c.get(&i.to_string()).unwrap();
        assert_eq!(set.len(), size);
    }
}

fn s_insert(map: Arc<SessionMap>, k: &str, v: SplitSink<WebSocketStream<TcpStream>, Message>) {
    map.upsert(k.to_owned(),
               || {
                   v
               },
               |v|
                   {});
}

async fn send(room_map: Arc<RoomMap>, session_map: Arc<SessionMap>, room: &str, payload: &str) {
    debug!("room_map: {:?}", room_map);
    match room_map.get(room) {
        None => debug!("no session set found for room: {}", room),
        Some(sessions_set) => {
            debug!("session_set: {:?}", sessions_set);
            match sessions_set.iter().cloned().next() {
                None => return,
                Some(session_id) => {
                    let mut w = session_map.get_mut(&session_id).unwrap();
                    w.send(Message::Text(payload.to_string())).await;
                }
            }
        }
    }

}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut settings = config::Config::default();
    settings
        .merge(config::File::with_name("config/config.yaml")).unwrap()
        .merge(config::Environment::with_prefix("SOCKET")).unwrap();

    let room_map = Arc::new(RoomMap::new());
    let session_map = Arc::new(SessionMap::new());

    tokio::spawn(redis_sub(room_map.clone(), session_map.clone()));
    let addr = "0.0.0.0:".to_string() + &settings.get_str("socket.port").unwrap();
    let _ = env_logger::try_init();
    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(&addr).await;
    let mut listener = try_socket.expect("Failed to bind");
    info!("Listening on: {}", addr);
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(accept_connection(stream, room_map.clone(), session_map.clone()));
    }
    Ok(())
}

async fn accept_connection(stream: TcpStream, room_map: Arc<RoomMap>, session_map: Arc<SessionMap>) {
    let addr = stream
        .peer_addr()
        .expect("connected streams should have a peer address");
    info!("Peer address: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Error during the websocket handshake occurred");

    info!("New WebSocket connection: {}", addr);
    let (write, _read) = ws_stream.split();
    let session_id = Uuid::new_v4();
    s_insert(session_map, &session_id.to_string(), write);
    r_insert(room_map, "5c886c69ca50950001362635.5e098bfaeaa38f0001d9dc84.message", &session_id.to_string());
}

async fn redis_sub(room_map: Arc<RoomMap>, session_map: Arc<SessionMap>) {
    let client = redis::Client::open("redis://127.0.0.1:6379/")
        .expect("Error opening redis client");
    let mut con = client.get_connection()
        .expect("Error getting redis connection");
    let mut pubsub = con.as_pubsub();
    pubsub.psubscribe("*")
        .expect("Error creating pubsub");
    loop {
        let msg = pubsub.get_message()
            .expect("Error getting pubsub message");
        let payload: String = msg.get_payload()
            .expect("Error getting message payload");
        send(room_map.clone(), session_map.clone(), msg.get_channel_name(), &payload).await;
    }
}

