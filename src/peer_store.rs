//! Bounded, network-bound persistence for peer addresses learned over P2P.

use std::{collections::HashMap, net::IpAddr, path::Path, str::FromStr, sync::Mutex};

use bitcoin::{Network, hashes::Hash, p2p::ServiceFlags};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::p2p::PeerAddress;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("peer_metadata");
const PEERS: TableDefinition<&str, &[u8]> = TableDefinition::new("peer_addresses");
const GENESIS_KEY: &str = "genesis";
const MAX_STORED_PEERS: usize = 4_096;
const MAX_PEERS_PER_SOURCE_GROUP: usize = 64;
const ADDRESS_HORIZON_SECS: u32 = 30 * 24 * 60 * 60;
const ADDRESS_TIME_PENALTY_SECS: u32 = 2 * 60 * 60;
const BAD_TIMESTAMP_REPLACEMENT_AGE_SECS: u32 = 5 * 24 * 60 * 60;
const MAX_FUTURE_SECS: u32 = 10 * 60;
const MIN_REASONABLE_TIMESTAMP: u32 = 100_000_000;

/// Peer-address persistence failures.
#[derive(Debug, Error)]
pub enum PeerStoreError {
    /// Database open/create failed.
    #[error("peer database: {0}")]
    Database(#[from] redb::DatabaseError),
    /// Transaction creation failed.
    #[error("peer transaction: {0}")]
    Transaction(#[from] redb::TransactionError),
    /// Table access failed.
    #[error("peer table: {0}")]
    Table(#[from] redb::TableError),
    /// Key/value access failed.
    #[error("peer storage: {0}")]
    Storage(#[from] redb::StorageError),
    /// Transaction commit failed.
    #[error("peer commit: {0}")]
    Commit(#[from] redb::CommitError),
    /// Stored JSON could not be decoded.
    #[error("peer encoding: {0}")]
    Encoding(#[from] serde_json::Error),
    /// The database was created for another Bitcoin network.
    #[error("peer database belongs to another Bitcoin network")]
    NetworkMismatch,
    /// Persisted data violates the store format.
    #[error("malformed peer store: {0}")]
    Malformed(&'static str),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredPeer {
    services: u64,
    last_seen: u32,
    source_group: String,
}

/// Result counters for one learned-address batch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PeerInsertStats {
    /// New or newer records accepted before capacity eviction.
    pub accepted: usize,
    /// Records rejected by service, address, freshness, or source limits.
    pub rejected: usize,
}

/// Redb-backed bounded peer-address pool.
pub struct RedbPeerStore {
    db: Database,
    network: Network,
    write_guard: Mutex<()>,
}

impl RedbPeerStore {
    /// Opens or creates a peer database bound to `network`.
    pub fn open(path: impl AsRef<Path>, network: Network) -> Result<Self, PeerStoreError> {
        let genesis = bitcoin::blockdata::constants::genesis_block(network)
            .block_hash()
            .to_byte_array();
        let db = Database::create(path)?;
        let transaction = db.begin_write()?;
        {
            let mut meta = transaction.open_table(META)?;
            if let Some(stored) = meta.get(GENESIS_KEY)? {
                if stored.value() != genesis {
                    return Err(PeerStoreError::NetworkMismatch);
                }
            } else {
                meta.insert(GENESIS_KEY, genesis.as_slice())?;
            }
            let _peers = transaction.open_table(PEERS)?;
        }
        transaction.commit()?;
        Ok(Self {
            db,
            network,
            write_guard: Mutex::new(()),
        })
    }

    /// Atomically filters and stores one peer-sourced address batch.
    pub fn insert_discovered(
        &self,
        source: std::net::SocketAddr,
        addresses: &[PeerAddress],
        now: u32,
    ) -> Result<PeerInsertStats, PeerStoreError> {
        let _guard = self.write_guard.lock().expect("peer lock not poisoned");
        let mut records = self.load_records()?;
        let group = source_group(source.ip());
        let mut group_count = records
            .values()
            .filter(|record| record.source_group == group)
            .count();
        let required = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let mut incoming = addresses.to_vec();
        incoming.sort_unstable_by_key(|address| std::cmp::Reverse(address.last_seen));
        let mut stats = PeerInsertStats::default();

        for address in incoming {
            let key = address.socket.to_string();
            let existing = records.get(&key);
            if !address.services.has(required)
                || !acceptable_ip(address.socket.ip(), self.network)
                || address.socket.port() == 0
            {
                stats.rejected += 1;
                continue;
            }
            let last_seen = normalize_last_seen(address.last_seen, now);
            if now.saturating_sub(last_seen) > ADDRESS_HORIZON_SECS {
                stats.rejected += 1;
                continue;
            }
            if existing.is_none() && group_count >= MAX_PEERS_PER_SOURCE_GROUP {
                stats.rejected += 1;
                continue;
            }
            if existing.is_some_and(|record| record.last_seen >= last_seen) {
                stats.rejected += 1;
                continue;
            }
            if existing.is_none() {
                group_count += 1;
            }
            records.insert(
                key,
                StoredPeer {
                    services: address.services.to_u64(),
                    last_seen,
                    source_group: group.clone(),
                },
            );
            stats.accepted += 1;
        }

        let mut ordered = records.into_iter().collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|(_, record)| std::cmp::Reverse(record.last_seen));
        ordered.truncate(MAX_STORED_PEERS);
        self.replace_all(&ordered)?;
        Ok(stats)
    }

    /// Returns fresh full-history+witness candidates, newest first.
    pub fn candidates(
        &self,
        now: u32,
        limit: usize,
    ) -> Result<Vec<std::net::SocketAddr>, PeerStoreError> {
        let required = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let mut candidates = self
            .load_records()?
            .into_iter()
            .filter_map(|(address, record)| {
                let socket = std::net::SocketAddr::from_str(&address).ok()?;
                let services = ServiceFlags::from(record.services);
                (services.has(required)
                    && acceptable_ip(socket.ip(), self.network)
                    && now.saturating_sub(record.last_seen) <= ADDRESS_HORIZON_SECS)
                    .then_some((record.last_seen, socket))
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable_by_key(|(last_seen, _)| std::cmp::Reverse(*last_seen));
        candidates.truncate(limit.min(MAX_STORED_PEERS));
        Ok(candidates.into_iter().map(|(_, socket)| socket).collect())
    }

    /// Returns the number of persisted records.
    pub fn len(&self) -> Result<usize, PeerStoreError> {
        Ok(self.load_records()?.len())
    }

    /// Returns whether the store contains no peer records.
    pub fn is_empty(&self) -> Result<bool, PeerStoreError> {
        Ok(self.len()? == 0)
    }

    fn load_records(&self) -> Result<HashMap<String, StoredPeer>, PeerStoreError> {
        let transaction = self.db.begin_read()?;
        let table = transaction.open_table(PEERS)?;
        let mut records = HashMap::new();
        for row in table.iter()? {
            let (key, value) = row?;
            let key = key.value().to_owned();
            if std::net::SocketAddr::from_str(&key).is_err() {
                return Err(PeerStoreError::Malformed("peer address key"));
            }
            records.insert(key, serde_json::from_slice(value.value())?);
        }
        Ok(records)
    }

    fn replace_all(&self, records: &[(String, StoredPeer)]) -> Result<(), PeerStoreError> {
        let transaction = self.db.begin_write()?;
        {
            let mut table = transaction.open_table(PEERS)?;
            let keys = table
                .iter()?
                .map(|row| row.map(|(key, _)| key.value().to_owned()))
                .collect::<Result<Vec<_>, _>>()?;
            for key in keys {
                table.remove(key.as_str())?;
            }
            for (key, record) in records {
                let encoded = serde_json::to_vec(record)?;
                table.insert(key.as_str(), encoded.as_slice())?;
            }
        }
        transaction.commit()?;
        Ok(())
    }
}

fn normalize_last_seen(last_seen: u32, now: u32) -> u32 {
    let plausible =
        last_seen > MIN_REASONABLE_TIMESTAMP && last_seen <= now.saturating_add(MAX_FUTURE_SECS);
    let normalized = if plausible {
        last_seen
    } else {
        now.saturating_sub(BAD_TIMESTAMP_REPLACEMENT_AGE_SECS)
    };
    normalized.saturating_sub(ADDRESS_TIME_PENALTY_SECS)
}

fn source_group(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            format!("v4:{}:{}", octets[0], octets[1])
        }
        IpAddr::V6(ip) => {
            let segments = ip.segments();
            format!("v6:{:x}:{:x}", segments[0], segments[1])
        }
    }
}

fn acceptable_ip(ip: IpAddr, network: Network) -> bool {
    if network == Network::Regtest {
        return !ip.is_unspecified() && !ip.is_multicast();
    }
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && b == 0)
                || (a == 192 && b == 168)
                || (a == 198 && (b == 18 || b == 19))
                || (a == 198 && b == 51 && c == 100)
                || (a == 203 && b == 0 && c == 113)
                || a >= 224)
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return acceptable_ip(IpAddr::V4(mapped), network);
            }
            let segments = ip.segments();
            !(ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_multicast()
                || segments[0] & 0xfe00 == 0xfc00
                || segments[0] & 0xffc0 == 0xfe80
                || (segments[0] == 0x2001 && segments[1] == 0x0db8)
                || (segments[0] == 0x2001 && matches!(segments[1] & 0xfff0, 0x0010 | 0x0020)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn learned(socket: &str, services: ServiceFlags, last_seen: u32) -> PeerAddress {
        PeerAddress {
            socket: socket.parse().unwrap(),
            services,
            last_seen,
        }
    }

    #[test]
    fn persists_network_bound_fresh_full_service_candidates() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("peers.redb");
        let now = 1_800_000_000;
        let services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let store = RedbPeerStore::open(&path, Network::Signet).unwrap();
        let stats = store
            .insert_discovered(
                "8.8.4.4:38333".parse().unwrap(),
                &[
                    learned("1.1.1.1:38333", services, now),
                    learned("10.0.0.1:38333", services, now),
                    learned("9.9.9.9:38333", ServiceFlags::NETWORK, now),
                ],
                now,
            )
            .unwrap();
        assert_eq!(
            stats,
            PeerInsertStats {
                accepted: 1,
                rejected: 2
            }
        );
        drop(store);

        let reopened = RedbPeerStore::open(&path, Network::Signet).unwrap();
        assert_eq!(
            reopened.candidates(now, 16).unwrap(),
            vec!["1.1.1.1:38333".parse().unwrap()]
        );
        drop(reopened);
        assert!(matches!(
            RedbPeerStore::open(path, Network::Bitcoin),
            Err(PeerStoreError::NetworkMismatch)
        ));
    }

    #[test]
    fn normalizes_bad_times_rejects_stale_and_caps_each_source_group() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        let now = 1_800_000_000;
        let services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let mut addresses = (1_u16..=70)
            .map(|port| learned(&format!("127.0.0.1:{}", 20_000 + port), services, now))
            .collect::<Vec<_>>();
        addresses.push(learned(
            "127.0.0.1:30000",
            services,
            now - ADDRESS_HORIZON_SECS - 1,
        ));
        addresses.push(learned("127.0.0.1:30001", services, 0));
        let stats = store
            .insert_discovered("127.0.0.2:18444".parse().unwrap(), &addresses, now)
            .unwrap();
        assert_eq!(stats.accepted, MAX_PEERS_PER_SOURCE_GROUP);
        assert_eq!(store.len().unwrap(), MAX_PEERS_PER_SOURCE_GROUP);
        assert_eq!(
            store.candidates(now, 100).unwrap().len(),
            MAX_PEERS_PER_SOURCE_GROUP
        );
    }

    #[test]
    fn rejects_reserved_ipv6_and_private_ipv4_mapped_addresses() {
        for address in [
            "2001:10::1".parse().unwrap(),
            "2001:20::1".parse().unwrap(),
            "::ffff:10.0.0.1".parse().unwrap(),
        ] {
            assert!(!acceptable_ip(IpAddr::V6(address), Network::Bitcoin));
        }
        assert!(acceptable_ip(
            IpAddr::V6("::ffff:1.1.1.1".parse().unwrap()),
            Network::Bitcoin
        ));
    }
}
