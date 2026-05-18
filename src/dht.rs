//! Kademlia DHT (BEP-5) — bootstrap, routing table, and `get_peers` lookup.
//!
//! This implementation provides the peer-discovery path only:
//!   bootstrap → ping known nodes → find_node walk → get_peers → announce_peer
//!
//! The DHT runs as a background task.  Callers add the InfoHash they want and
//! receive peers via a channel.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use rand::prelude::*;
use tokio::{
    net::UdpSocket,
    sync::{Mutex, mpsc},
};

use crate::{
    bencode::{self, Value},
    error::Result,
    types::{InfoHash, PeerAddr},
};

// Node ID
/// 160-bit DHT node ID.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct NodeId(pub [u8; 20]);

impl NodeId {
    /// Generate a cryptographically random node ID.
    pub fn random() -> Self {
        let mut id = [0u8; 20];
        rand::rng().fill(&mut id);
        NodeId(id)
    }

    /// XOR distance metric.
    pub fn distance(&self, other: &NodeId) -> [u8; 20] {
        let mut d = [0u8; 20];
        for i in 0..20 {
            d[i] = self.0[i] ^ other.0[i];
        }
        d
    }

    /// Return the raw 20 bytes of the node ID.
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }
}

// Bootstrap nodes
/// Well-known bootstrap nodes used to enter the DHT network.
pub const BOOTSTRAP_NODES: &[&str] = &[
    "router.bittorrent.com:6881",
    "router.utorrent.com:6881",
    "dht.transmissionbt.com:6881",
];

// DHT node in routing table
/// A single node record stored in the routing table.
#[derive(Clone, Debug)]
pub struct DhtNode {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub last_seen: Instant,
}

// Routing table (simplified — flat list, not full k-bucket tree)
/// Simplified flat routing table (not a full k-bucket tree).
pub struct RoutingTable {
    /// Nodes ordered by XOR distance, capped at `K * 8` entries.
    nodes: Vec<DhtNode>,
}

const K: usize = 20; // k-bucket size

impl RoutingTable {
    /// Create an empty routing table.
    pub fn new() -> Self {
        RoutingTable { nodes: Vec::new() }
    }

    /// Add or refresh a node in the routing table.
    pub fn add(&mut self, node: DhtNode) {
        // Replace stale node or append if there's room.
        if let Some(pos) = self.nodes.iter().position(|n| n.id == node.id) {
            self.nodes[pos] = node;
            return;
        }
        if self.nodes.len() < K * 8 {
            self.nodes.push(node);
        }
    }

    /// Return the K closest nodes to `target`.
    pub fn closest(&self, target: &NodeId, k: usize) -> Vec<DhtNode> {
        let mut sorted = self.nodes.clone();
        sorted.sort_by_key(|n| n.id.distance(target));
        sorted.truncate(k);
        sorted
    }

    /// Return the number of nodes currently in the routing table.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Return true if the routing table has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

// Public DHT handle
/// A running DHT instance.
///
/// Clone-able handle backed by a shared inner state; clones all refer to the
/// same background task.
#[derive(Clone)]
pub struct Dht {
    inner: Arc<DhtInner>,
}

struct DhtInner {
    socket: UdpSocket,
    own_id: NodeId,
    table: Mutex<RoutingTable>,
    pending: Mutex<HashMap<[u8; 2], PendingQuery>>,
}

struct PendingQuery {
    tx: mpsc::UnboundedSender<DhtEvent>,
}

#[derive(Debug)]
enum DhtEvent {
    Peers(Vec<PeerAddr>),
    Nodes(Vec<DhtNode>),
    Token { node_id: NodeId, token: Vec<u8> },
}

impl Dht {
    /// Start the DHT, bootstrap, and return a handle.
    pub async fn start() -> Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        let own_id = NodeId::random();
        let dht = Dht {
            inner: Arc::new(DhtInner {
                socket,
                own_id,
                table: Mutex::new(RoutingTable::new()),
                pending: Mutex::new(HashMap::new()),
            }),
        };
        // Spawn background receive loop.
        let dht2 = dht.clone();
        tokio::spawn(async move {
            dht2.recv_loop().await;
        });
        // Bootstrap.
        dht.bootstrap().await;
        Ok(dht)
    }

    /// Look up peers for `info_hash`. Returns up to `max` peers.
    pub async fn get_peers(&self, info_hash: InfoHash, max: usize) -> Vec<PeerAddr> {
        let target = NodeId(info_hash.0);
        let seed_nodes = self.inner.table.lock().await.closest(&target, K);

        let mut visited: HashSet<SocketAddr> = HashSet::new();
        let mut peers: Vec<PeerAddr> = Vec::new();
        let mut token_map: HashMap<NodeId, Vec<u8>> = HashMap::new();
        let deadline = Instant::now() + Duration::from_secs(15);

        let mut queue: Vec<DhtNode> = seed_nodes;

        while !queue.is_empty() && peers.len() < max && Instant::now() < deadline {
            let node = queue.remove(0);
            if visited.contains(&node.addr) {
                continue;
            }
            visited.insert(node.addr);

            let (tx, mut rx) = mpsc::unbounded_channel();
            let tid = self.send_get_peers(&node.addr, &info_hash, tx).await;

            // Wait up to 2s for a response.
            match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
                Ok(Some(DhtEvent::Peers(p))) => {
                    peers.extend(p);
                }
                Ok(Some(DhtEvent::Nodes(nodes))) => {
                    for n in nodes {
                        if !visited.contains(&n.addr) {
                            queue.push(n);
                        }
                    }
                }
                Ok(Some(DhtEvent::Token { node_id, token })) => {
                    token_map.insert(node_id, token);
                }
                _ => {}
            }
            // Clean up pending entry.
            self.inner.pending.lock().await.remove(&tid);
        }

        // Announce ourselves to the K closest that gave us a token.
        for (nid, token) in &token_map {
            let closest = self.inner.table.lock().await.closest(&target, K);
            if closest.iter().any(|n| &n.id == nid) {
                if let Some(node) = closest.iter().find(|n| &n.id == nid) {
                    let _ = self.send_announce_peer(&node.addr, &info_hash, token).await;
                }
            }
        }

        peers.truncate(max);
        peers
    }

    // Internals
    async fn bootstrap(&self) {
        for addr_str in BOOTSTRAP_NODES {
            if let Ok(addrs) = tokio::net::lookup_host(*addr_str).await {
                for addr in addrs {
                    self.send_find_node(&addr, &self.inner.own_id).await;
                }
            }
        }
    }

    async fn recv_loop(&self) {
        let mut buf = [0u8; 65536];
        loop {
            let (n, from) = match self.inner.socket.recv_from(&mut buf).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!("DHT recv error: {e}");
                    continue;
                }
            };
            if let Err(e) = self.handle_packet(&buf[..n], from).await {
                tracing::debug!("DHT packet error from {from}: {e}");
            }
        }
    }

    async fn handle_packet(&self, data: &[u8], from: SocketAddr) -> Result<()> {
        let msg = bencode::decode(data)?;

        let y = msg.get(b"y").and_then(|v| v.as_str()).unwrap_or("");
        match y {
            "r" => self.handle_response(&msg, from).await?,
            "q" => self.handle_query(&msg, from).await?,
            "e" => {
                tracing::debug!("DHT error from {from}: {:?}", msg.get(b"e"));
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_response(&self, msg: &Value, from: SocketAddr) -> Result<()> {
        let t = msg.get(b"t").and_then(|v| v.as_bytes()).unwrap_or(&[]);
        if t.len() < 2 {
            return Ok(());
        }
        let tid: [u8; 2] = [t[0], t[1]];

        let r = match msg.get(b"r") {
            Some(v) => v,
            None => return Ok(()),
        };

        // Update routing table if we can read the node's ID.
        if let Some(id_bytes) = r.get(b"id").and_then(|v| v.as_bytes()) {
            if id_bytes.len() == 20 {
                let mut id = [0u8; 20];
                id.copy_from_slice(id_bytes);
                self.inner.table.lock().await.add(DhtNode {
                    id: NodeId(id),
                    addr: from,
                    last_seen: Instant::now(),
                });
            }
        }

        // Dispatch to waiting caller.
        let tx = {
            let mut pending = self.inner.pending.lock().await;
            pending.remove(&tid).map(|pq| pq.tx)
        };

        if let Some(tx) = tx {
            // Peers response.
            if let Some(values) = r.get(b"values") {
                let peers = parse_peers_list(values);
                let _ = tx.send(DhtEvent::Peers(peers));
            }
            // Nodes response.
            if let Some(nodes_raw) = r.get(b"nodes").and_then(|v| v.as_bytes()) {
                let nodes = parse_compact_nodes(nodes_raw);
                // Add all to routing table.
                {
                    let mut tbl = self.inner.table.lock().await;
                    for n in &nodes {
                        tbl.add(n.clone());
                    }
                }
                let _ = tx.send(DhtEvent::Nodes(nodes));
            }
            // Token.
            if let Some(token) = r.get(b"token").and_then(|v| v.as_bytes()) {
                if let Some(id_bytes) = r.get(b"id").and_then(|v| v.as_bytes()) {
                    if id_bytes.len() == 20 {
                        let mut id = [0u8; 20];
                        id.copy_from_slice(id_bytes);
                        let _ = tx.send(DhtEvent::Token {
                            node_id: NodeId(id),
                            token: token.to_vec(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_query(&self, msg: &Value, from: SocketAddr) -> Result<()> {
        let q = msg.get(b"q").and_then(|v| v.as_str()).unwrap_or("");
        let t = msg.get(b"t").and_then(|v| v.as_bytes()).unwrap_or(&[]);
        match q {
            "ping" => self.reply_pong(from, t).await?,
            _ => {} // ignore find_node / get_peers queries for now
        }
        Ok(())
    }

    async fn reply_pong(&self, to: SocketAddr, t: &[u8]) -> Result<()> {
        let mut resp: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
        resp.insert(b"y".to_vec(), Value::Bytes(b"r".to_vec()));
        resp.insert(b"t".to_vec(), Value::Bytes(t.to_vec()));
        let mut r: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
        r.insert(b"id".to_vec(), Value::Bytes(self.inner.own_id.0.to_vec()));
        resp.insert(b"r".to_vec(), Value::Dict(r));
        let encoded = bencode_encode(&Value::Dict(resp));
        self.inner.socket.send_to(&encoded, to).await?;
        Ok(())
    }

    async fn send_find_node(&self, to: &SocketAddr, target: &NodeId) {
        let tid = fresh_tid();
        let msg = build_query(b"find_node", &tid, &self.inner.own_id, |a| {
            a.insert(b"target".to_vec(), Value::Bytes(target.0.to_vec()));
        });
        let _ = self.inner.socket.send_to(&bencode_encode(&msg), to).await;
    }

    async fn send_get_peers(
        &self,
        to: &SocketAddr,
        info_hash: &InfoHash,
        tx: mpsc::UnboundedSender<DhtEvent>,
    ) -> [u8; 2] {
        let tid = fresh_tid();
        let msg = build_query(b"get_peers", &tid, &self.inner.own_id, |a| {
            a.insert(b"info_hash".to_vec(), Value::Bytes(info_hash.0.to_vec()));
        });
        self.inner
            .pending
            .lock()
            .await
            .insert(tid, PendingQuery { tx });
        let _ = self.inner.socket.send_to(&bencode_encode(&msg), to).await;
        tid
    }

    async fn send_announce_peer(
        &self,
        to: &SocketAddr,
        info_hash: &InfoHash,
        token: &[u8],
    ) -> Result<()> {
        let tid = fresh_tid();
        let msg = build_query(b"announce_peer", &tid, &self.inner.own_id, |a| {
            a.insert(b"info_hash".to_vec(), Value::Bytes(info_hash.0.to_vec()));
            a.insert(b"port".to_vec(), Value::Int(0));
            a.insert(b"token".to_vec(), Value::Bytes(token.to_vec()));
            a.insert(b"implied_port".to_vec(), Value::Int(1));
        });
        self.inner.socket.send_to(&bencode_encode(&msg), to).await?;
        Ok(())
    }
}

// Bencode helpers
fn fresh_tid() -> [u8; 2] {
    rand::random::<u16>().to_be_bytes()
}

fn build_query(
    method: &[u8],
    tid: &[u8; 2],
    own_id: &NodeId,
    args_fn: impl FnOnce(&mut BTreeMap<Vec<u8>, Value>),
) -> Value {
    let mut a: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
    a.insert(b"id".to_vec(), Value::Bytes(own_id.0.to_vec()));
    args_fn(&mut a);

    let mut msg: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
    msg.insert(b"t".to_vec(), Value::Bytes(tid.to_vec()));
    msg.insert(b"y".to_vec(), Value::Bytes(b"q".to_vec()));
    msg.insert(b"q".to_vec(), Value::Bytes(method.to_vec()));
    msg.insert(b"a".to_vec(), Value::Dict(a));
    Value::Dict(msg)
}

fn bencode_encode(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value(v, &mut out);
    out
}

fn encode_value(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Bytes(b) => {
            out.extend_from_slice(b.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(b);
        }
        Value::Int(i) => {
            out.push(b'i');
            out.extend_from_slice(i.to_string().as_bytes());
            out.push(b'e');
        }
        Value::List(l) => {
            out.push(b'l');
            for item in l {
                encode_value(item, out);
            }
            out.push(b'e');
        }
        Value::Dict(d) => {
            out.push(b'd');
            for (k, val) in d {
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(k);
                encode_value(val, out);
            }
            out.push(b'e');
        }
    }
}

/// Parse compact node info: 26-byte entries (20 id + 4 IPv4 + 2 port).
fn parse_compact_nodes(data: &[u8]) -> Vec<DhtNode> {
    let mut nodes = Vec::new();
    for chunk in data.chunks_exact(26) {
        let mut id = [0u8; 20];
        id.copy_from_slice(&chunk[..20]);
        let ip = Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
        let port = u16::from_be_bytes([chunk[24], chunk[25]]);
        nodes.push(DhtNode {
            id: NodeId(id),
            addr: SocketAddr::new(IpAddr::V4(ip), port),
            last_seen: Instant::now(),
        });
    }
    nodes
}

/// Parse a BEP-5 peers list (a bencoded list of 6-byte compact peer strings).
fn parse_peers_list(v: &Value) -> Vec<PeerAddr> {
    use std::net::{IpAddr, Ipv4Addr};
    let mut out = Vec::new();
    if let Some(list) = v.as_list() {
        for entry in list {
            if let Some(b) = entry.as_bytes() {
                if b.len() == 6 {
                    let ip = Ipv4Addr::new(b[0], b[1], b[2], b[3]);
                    let port = u16::from_be_bytes([b[4], b[5]]);
                    out.push(PeerAddr {
                        ip: IpAddr::V4(ip),
                        port,
                    });
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Instant;

    fn make_node(id: [u8; 20], ip: u8) -> DhtNode {
        DhtNode {
            id: NodeId(id),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)), 6881),
            last_seen: Instant::now(),
        }
    }

    #[test]
    fn node_id_random_has_correct_length() {
        let id = NodeId::random();
        assert_eq!(id.as_bytes().len(), 20);
    }

    #[test]
    fn node_id_random_produces_unique_ids() {
        let a = NodeId::random();
        let b = NodeId::random();
        assert_ne!(a, b);
    }

    #[test]
    fn node_id_distance_self_is_zero() {
        let id = NodeId([5u8; 20]);
        assert_eq!(id.distance(&id), [0u8; 20]);
    }

    #[test]
    fn node_id_distance_known_xor() {
        let a = NodeId([0b10101010u8; 20]);
        let b = NodeId([0b01010101u8; 20]);
        let dist = a.distance(&b);
        assert_eq!(dist, [0b11111111u8; 20]);
    }

    #[test]
    fn node_id_distance_is_symmetric() {
        let a = NodeId([0x12u8; 20]);
        let b = NodeId([0x34u8; 20]);
        assert_eq!(a.distance(&b), b.distance(&a));
    }

    #[test]
    fn routing_table_starts_empty() {
        let rt = RoutingTable::new();
        assert_eq!(rt.len(), 0);
    }

    #[test]
    fn routing_table_add_increases_len() {
        let mut rt = RoutingTable::new();
        rt.add(make_node([1u8; 20], 1));
        rt.add(make_node([2u8; 20], 2));
        assert_eq!(rt.len(), 2);
    }

    #[test]
    fn routing_table_add_refreshes_existing_node() {
        let mut rt = RoutingTable::new();
        rt.add(make_node([1u8; 20], 1));
        rt.add(make_node([1u8; 20], 2)); // same ID, different addr
        assert_eq!(rt.len(), 1);
        assert_eq!(rt.nodes[0].addr.ip().to_string(), "10.0.0.2");
    }

    #[test]
    fn routing_table_closest_returns_k_nearest() {
        let target = NodeId([0u8; 20]);
        let mut rt = RoutingTable::new();
        // Add nodes at increasing distances from target.
        for i in 1u8..=10 {
            let mut id = [0u8; 20];
            id[19] = i; // distance = i from target
            rt.add(make_node(id, i));
        }
        let closest = rt.closest(&target, 3);
        assert_eq!(closest.len(), 3);
        // The closest should be distance 1 (id[19] = 1).
        assert_eq!(closest[0].id.0[19], 1);
    }

    #[test]
    fn routing_table_closest_returns_all_when_fewer_than_k() {
        let mut rt = RoutingTable::new();
        rt.add(make_node([1u8; 20], 1));
        rt.add(make_node([2u8; 20], 2));
        let closest = rt.closest(&NodeId::random(), 10);
        assert_eq!(closest.len(), 2);
    }

    #[test]
    fn bootstrap_nodes_are_non_empty() {
        assert!(!BOOTSTRAP_NODES.is_empty());
        for node in BOOTSTRAP_NODES {
            // Each bootstrap node should contain a port separator.
            assert!(node.contains(':'), "Bootstrap node '{node}' has no port");
        }
    }
}
