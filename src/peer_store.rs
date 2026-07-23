//! Bounded, network-bound persistence for peer addresses learned over P2P.

use std::{collections::HashMap, net::IpAddr, path::Path, str::FromStr, sync::Mutex};

use bitcoin::{Network, hashes::Hash, p2p::ServiceFlags};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::p2p::PeerAddress;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("peer_metadata");
const PEERS: TableDefinition<&str, &[u8]> = TableDefinition::new("peer_addresses");
const PENALTIES: TableDefinition<&str, &[u8]> = TableDefinition::new("peer_penalties");
const GENESIS_KEY: &str = "genesis";
const MAX_STORED_PEERS: usize = 4_096;
const MAX_PEERS_PER_SOURCE_GROUP: usize = 64;
const ADDRESS_HORIZON_SECS: u32 = 30 * 24 * 60 * 60;
const ADDRESS_TIME_PENALTY_SECS: u32 = 2 * 60 * 60;
const BAD_TIMESTAMP_REPLACEMENT_AGE_SECS: u32 = 5 * 24 * 60 * 60;
const MAX_FUTURE_SECS: u32 = 10 * 60;
const MIN_REASONABLE_TIMESTAMP: u32 = 100_000_000;
const INITIAL_RETRY_BACKOFF_SECS: u32 = 60;
const MAX_RETRY_BACKOFF_SECS: u32 = 6 * 60 * 60;
const INITIAL_PROTOCOL_COOLDOWN_SECS: u32 = 60 * 60;
const MAX_PROTOCOL_COOLDOWN_SECS: u32 = 24 * 60 * 60;
const PROTOCOL_VIOLATION_DECAY_SECS: u32 = 7 * 24 * 60 * 60;
const MAX_STORED_PENALTIES: usize = 1_024;
const MAX_STORED_PEER_BYTES: usize = 1_024;
const MAX_STORED_PENALTY_BYTES: usize = 256;

type PeerPriority = (
    u8,
    u8,
    std::cmp::Reverse<u32>,
    std::cmp::Reverse<u32>,
    std::cmp::Reverse<u32>,
    std::cmp::Reverse<u32>,
);
type PrioritizedPeer = (PeerPriority, std::net::SocketAddr);

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
#[serde(deny_unknown_fields)]
struct StoredPeer {
    services: u64,
    last_seen: u32,
    source_group: String,
    #[serde(default)]
    last_attempt: u32,
    #[serde(default)]
    last_success: u32,
    #[serde(default)]
    consecutive_failures: u8,
    #[serde(default)]
    last_session_success: u32,
    #[serde(default)]
    successful_sessions: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredPenalty {
    violations: u8,
    last_violation: u32,
    discouraged_until: u32,
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
            let _penalties = transaction.open_table(PENALTIES)?;
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
        let required = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        records.retain(|address, record| {
            std::net::SocketAddr::from_str(address).is_ok_and(|socket| {
                ServiceFlags::from(record.services).has(required)
                    && is_acceptable_peer_address(socket, self.network)
                    && now.saturating_sub(record.last_seen) <= ADDRESS_HORIZON_SECS
            })
        });
        let group = source_group(source.ip());
        let mut group_count = records
            .values()
            .filter(|record| record.source_group == group)
            .count();
        let mut incoming = addresses.to_vec();
        incoming.sort_unstable_by_key(|address| std::cmp::Reverse(address.last_seen));
        let mut stats = PeerInsertStats::default();

        for address in incoming {
            let key = address.socket.to_string();
            let existing = records.get(&key);
            if !address.services.has(required)
                || !is_acceptable_peer_address(address.socket, self.network)
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
            let (
                source_group,
                last_attempt,
                last_success,
                consecutive_failures,
                last_session_success,
                successful_sessions,
            ) = existing.map_or_else(
                || (group.clone(), 0, 0, 0, 0, 0),
                |record| {
                    (
                        record.source_group.clone(),
                        record.last_attempt,
                        record.last_success,
                        record.consecutive_failures,
                        record.last_session_success,
                        record.successful_sessions,
                    )
                },
            );
            records.insert(
                key,
                StoredPeer {
                    services: address.services.to_u64(),
                    last_seen,
                    source_group,
                    last_attempt,
                    last_success,
                    consecutive_failures,
                    last_session_success,
                    successful_sessions,
                },
            );
            stats.accepted += 1;
        }

        let ordered = retain_best(records, MAX_STORED_PEERS);
        self.replace_all(&ordered)?;
        Ok(stats)
    }

    /// Persists a peer whose advertised services were verified by a successful handshake.
    ///
    /// Unlike learned addresses, a verified peer is immediately marked successful so a
    /// restart can use it without inheriting a stale retry delay.
    pub fn insert_verified(
        &self,
        address: std::net::SocketAddr,
        services: ServiceFlags,
        now: u32,
    ) -> Result<bool, PeerStoreError> {
        let stats = self.insert_discovered(
            address,
            &[PeerAddress {
                socket: address,
                services,
                last_seen: now,
            }],
            now,
        )?;
        let existed = self.record_success(address, now)?;
        Ok(stats.accepted > 0 || existed)
    }

    /// Returns fresh full-history+witness candidates, newest first.
    pub fn candidates(
        &self,
        now: u32,
        limit: usize,
    ) -> Result<Vec<std::net::SocketAddr>, PeerStoreError> {
        let required = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let penalties = self.load_penalties()?;
        let mut candidates = self
            .load_records()?
            .into_iter()
            .filter_map(|(address, record)| {
                let socket = std::net::SocketAddr::from_str(&address).ok()?;
                let services = ServiceFlags::from(record.services);
                (services.has(required)
                    && is_acceptable_peer_address(socket, self.network)
                    && now.saturating_sub(record.last_seen) <= ADDRESS_HORIZON_SECS
                    && retry_ready(&record, now))
                .then_some((record, socket))
                .filter(|_| {
                    penalties
                        .get(&address)
                        .is_none_or(|penalty| penalty.discouraged_until <= now)
                })
                .map(|(record, socket)| (peer_priority(&record), socket))
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable_by_key(|candidate| *candidate);
        Ok(diversify_candidates(
            candidates,
            limit.min(MAX_STORED_PEERS),
        ))
    }

    /// Durably records a connection attempt before network I/O starts.
    ///
    /// The failure count is incremented up front so a process crash cannot
    /// cause the same learned address to be retried immediately on restart.
    pub fn record_attempt(
        &self,
        address: std::net::SocketAddr,
        now: u32,
    ) -> Result<bool, PeerStoreError> {
        self.update_existing(address, |record| {
            record.last_attempt = now;
            record.consecutive_failures = record.consecutive_failures.saturating_add(1);
        })
    }

    /// Records a successful full-history+witness handshake and clears backoff.
    pub fn record_success(
        &self,
        address: std::net::SocketAddr,
        now: u32,
    ) -> Result<bool, PeerStoreError> {
        self.update_existing(address, |record| {
            record.last_success = now;
            record.consecutive_failures = 0;
        })
    }

    /// Records a completed synchronization session and forgives prior protocol violations.
    pub fn record_session_success(
        &self,
        address: std::net::SocketAddr,
        now: u32,
    ) -> Result<bool, PeerStoreError> {
        let updated = self.update_existing(address, |record| {
            record.last_success = now;
            record.consecutive_failures = 0;
            record.last_session_success = now;
            record.successful_sessions = record.successful_sessions.saturating_add(1);
        })?;
        self.clear_penalty(address)?;
        Ok(updated)
    }

    /// Durably discourages a non-manual peer after an objective wire-protocol violation.
    ///
    /// Repeated violations double the cooldown from one hour up to one day. The violation
    /// count decays after seven quiet days, and the table is bounded independently from addrman.
    pub fn record_protocol_violation(
        &self,
        address: std::net::SocketAddr,
        now: u32,
    ) -> Result<u32, PeerStoreError> {
        let _guard = self.write_guard.lock().expect("peer lock not poisoned");
        let mut penalties = self.load_penalties()?;
        penalties.retain(|_, penalty| {
            now.saturating_sub(penalty.last_violation) <= PROTOCOL_VIOLATION_DECAY_SECS
        });
        let key = address.to_string();
        let violations = penalties
            .get(&key)
            .map_or(1, |penalty| penalty.violations.saturating_add(1));
        let exponent = u32::from(violations.saturating_sub(1)).min(5);
        let cooldown = INITIAL_PROTOCOL_COOLDOWN_SECS
            .saturating_mul(1_u32 << exponent)
            .min(MAX_PROTOCOL_COOLDOWN_SECS);
        let discouraged_until = now.saturating_add(cooldown);
        penalties.insert(
            key,
            StoredPenalty {
                violations,
                last_violation: now,
                discouraged_until,
            },
        );
        let mut penalties = penalties.into_iter().collect::<Vec<_>>();
        penalties.sort_unstable_by_key(|(_, penalty)| {
            (
                std::cmp::Reverse(penalty.discouraged_until),
                std::cmp::Reverse(penalty.last_violation),
            )
        });
        penalties.truncate(MAX_STORED_PENALTIES);
        self.replace_penalties(&penalties)?;
        Ok(discouraged_until)
    }

    /// Returns all addresses whose objective protocol cooldown is still active.
    pub fn discouraged_addresses(
        &self,
        now: u32,
    ) -> Result<Vec<std::net::SocketAddr>, PeerStoreError> {
        Ok(self
            .load_penalties()?
            .into_iter()
            .filter_map(|(address, penalty)| {
                (penalty.discouraged_until > now)
                    .then(|| std::net::SocketAddr::from_str(&address).ok())
                    .flatten()
            })
            .collect())
    }

    /// Returns whether the address is under an active protocol-violation cooldown.
    pub fn is_discouraged(
        &self,
        address: std::net::SocketAddr,
        now: u32,
    ) -> Result<bool, PeerStoreError> {
        Ok(self
            .load_penalties()?
            .get(&address.to_string())
            .is_some_and(|penalty| penalty.discouraged_until > now))
    }

    /// Returns the number of persisted records.
    pub fn len(&self) -> Result<usize, PeerStoreError> {
        Ok(self.load_records()?.len())
    }

    /// Returns whether the store contains no peer records.
    pub fn is_empty(&self) -> Result<bool, PeerStoreError> {
        Ok(self.len()? == 0)
    }

    fn update_existing(
        &self,
        address: std::net::SocketAddr,
        update: impl FnOnce(&mut StoredPeer),
    ) -> Result<bool, PeerStoreError> {
        let _guard = self.write_guard.lock().expect("peer lock not poisoned");
        let mut records = self.load_records()?;
        let Some(record) = records.get_mut(&address.to_string()) else {
            return Ok(false);
        };
        update(record);
        let records = records.into_iter().collect::<Vec<_>>();
        self.replace_all(&records)?;
        Ok(true)
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
            records.insert(key, decode_stored_peer(value.value())?);
        }
        Ok(records)
    }

    fn load_penalties(&self) -> Result<HashMap<String, StoredPenalty>, PeerStoreError> {
        let transaction = self.db.begin_read()?;
        let table = transaction.open_table(PENALTIES)?;
        let mut penalties = HashMap::new();
        for row in table.iter()? {
            let (key, value) = row?;
            let key = key.value().to_owned();
            if std::net::SocketAddr::from_str(&key).is_err() {
                return Err(PeerStoreError::Malformed("peer penalty address key"));
            }
            penalties.insert(key, decode_stored_penalty(value.value())?);
        }
        Ok(penalties)
    }

    fn clear_penalty(&self, address: std::net::SocketAddr) -> Result<(), PeerStoreError> {
        let _guard = self.write_guard.lock().expect("peer lock not poisoned");
        let transaction = self.db.begin_write()?;
        {
            let mut table = transaction.open_table(PENALTIES)?;
            table.remove(address.to_string().as_str())?;
        }
        transaction.commit()?;
        Ok(())
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

    fn replace_penalties(
        &self,
        penalties: &[(String, StoredPenalty)],
    ) -> Result<(), PeerStoreError> {
        let transaction = self.db.begin_write()?;
        {
            let mut table = transaction.open_table(PENALTIES)?;
            let keys = table
                .iter()?
                .map(|row| row.map(|(key, _)| key.value().to_owned()))
                .collect::<Result<Vec<_>, _>>()?;
            for key in keys {
                table.remove(key.as_str())?;
            }
            for (key, penalty) in penalties {
                let encoded = serde_json::to_vec(penalty)?;
                table.insert(key.as_str(), encoded.as_slice())?;
            }
        }
        transaction.commit()?;
        Ok(())
    }
}

fn decode_stored_peer(input: &[u8]) -> Result<StoredPeer, PeerStoreError> {
    if input.len() > MAX_STORED_PEER_BYTES {
        return Err(PeerStoreError::Malformed("peer record exceeds size limit"));
    }
    let record: StoredPeer = serde_json::from_slice(input)?;
    if !valid_source_group(&record.source_group) {
        return Err(PeerStoreError::Malformed("peer source group"));
    }
    if (record.last_session_success == 0) != (record.successful_sessions == 0)
        || record.last_session_success > record.last_success
    {
        return Err(PeerStoreError::Malformed("peer session success history"));
    }
    Ok(record)
}

fn decode_stored_penalty(input: &[u8]) -> Result<StoredPenalty, PeerStoreError> {
    if input.len() > MAX_STORED_PENALTY_BYTES {
        return Err(PeerStoreError::Malformed(
            "peer penalty record exceeds size limit",
        ));
    }
    let penalty: StoredPenalty = serde_json::from_slice(input)?;
    if penalty.violations == 0 || penalty.discouraged_until < penalty.last_violation {
        return Err(PeerStoreError::Malformed("peer penalty values"));
    }
    Ok(penalty)
}

/// Validates one untrusted persisted peer-address JSON value.
pub fn validate_stored_peer_record(input: &[u8]) -> Result<(), PeerStoreError> {
    decode_stored_peer(input).map(drop)
}

/// Validates one untrusted persisted protocol-penalty JSON value.
pub fn validate_stored_peer_penalty(input: &[u8]) -> Result<(), PeerStoreError> {
    decode_stored_penalty(input).map(drop)
}

fn valid_source_group(group: &str) -> bool {
    let mut parts = group.split(':');
    let Some(family) = parts.next() else {
        return false;
    };
    let Some(first) = parts.next() else {
        return false;
    };
    let Some(second) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    match family {
        "v4" => {
            first
                .parse::<u8>()
                .is_ok_and(|value| value.to_string() == first)
                && second
                    .parse::<u8>()
                    .is_ok_and(|value| value.to_string() == second)
        }
        "v6" => {
            u16::from_str_radix(first, 16).is_ok_and(|value| format!("{value:x}") == first)
                && u16::from_str_radix(second, 16).is_ok_and(|value| format!("{value:x}") == second)
        }
        _ => false,
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

fn retry_ready(record: &StoredPeer, now: u32) -> bool {
    if record.consecutive_failures == 0 {
        return true;
    }
    let exponent = u32::from(record.consecutive_failures.saturating_sub(1)).min(9);
    let delay = INITIAL_RETRY_BACKOFF_SECS
        .saturating_mul(1_u32 << exponent)
        .min(MAX_RETRY_BACKOFF_SECS);
    now.saturating_sub(record.last_attempt) >= delay
}

fn peer_priority(record: &StoredPeer) -> PeerPriority {
    (
        record.consecutive_failures,
        u8::from(record.last_session_success == 0),
        std::cmp::Reverse(record.last_session_success),
        std::cmp::Reverse(record.successful_sessions),
        std::cmp::Reverse(record.last_success),
        std::cmp::Reverse(record.last_seen),
    )
}

fn retain_best(records: HashMap<String, StoredPeer>, limit: usize) -> Vec<(String, StoredPeer)> {
    let mut ordered = records.into_iter().collect::<Vec<_>>();
    ordered.sort_unstable_by(|(left_address, left), (right_address, right)| {
        peer_priority(left)
            .cmp(&peer_priority(right))
            .then_with(|| left_address.cmp(right_address))
    });
    ordered.truncate(limit);
    ordered
}

fn diversify_candidates(ordered: Vec<PrioritizedPeer>, limit: usize) -> Vec<std::net::SocketAddr> {
    let mut group_indexes = HashMap::new();
    let mut groups: Vec<Vec<std::net::SocketAddr>> = Vec::new();
    for (_, address) in ordered {
        let group = source_group(address.ip());
        let index = *group_indexes.entry(group).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[index].push(address);
    }

    let mut selected = Vec::with_capacity(limit.min(groups.len()));
    let mut offset = 0;
    while selected.len() < limit {
        let before = selected.len();
        for group in &groups {
            if let Some(address) = group.get(offset) {
                selected.push(*address);
                if selected.len() == limit {
                    break;
                }
            }
        }
        if selected.len() == before {
            break;
        }
        offset += 1;
    }
    selected
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

/// Returns whether a resolved socket is eligible for outbound use on `network`.
///
/// Public networks exclude local, private, documentation, transition, multicast, and other
/// reserved ranges. Regtest deliberately permits local addresses for isolated test networks.
pub fn is_acceptable_peer_address(address: std::net::SocketAddr, network: Network) -> bool {
    address.port() != 0 && acceptable_ip(address.ip(), network)
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
        let now: u32 = 1_800_000_000;
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
    fn verified_peers_are_immediately_persisted_and_service_filtered() {
        let directory = TempDir::new().unwrap();
        let now = 1_800_000_000;
        let address = "127.0.0.1:18444".parse().unwrap();
        let store =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        assert!(
            !store
                .insert_verified(address, ServiceFlags::NETWORK, now)
                .unwrap()
        );
        assert!(store.is_empty().unwrap());
        assert!(
            store
                .insert_verified(address, ServiceFlags::NETWORK | ServiceFlags::WITNESS, now,)
                .unwrap()
        );
        assert_eq!(store.candidates(now, 16).unwrap(), vec![address]);

        let public =
            RedbPeerStore::open(directory.path().join("public.redb"), Network::Bitcoin).unwrap();
        assert!(
            !public
                .insert_verified(
                    "10.0.0.1:8333".parse().unwrap(),
                    ServiceFlags::NETWORK | ServiceFlags::WITNESS,
                    now,
                )
                .unwrap()
        );
        assert!(public.is_empty().unwrap());
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
        store
            .insert_discovered(
                "127.0.0.2:18444".parse().unwrap(),
                &[],
                now + ADDRESS_HORIZON_SECS,
            )
            .unwrap();
        assert!(store.is_empty().unwrap());
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

    #[test]
    fn persists_exponential_retry_backoff_and_resets_it_after_success() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("peers.redb");
        let now = 1_800_000_000;
        let address = "127.0.0.1:18444".parse().unwrap();
        let services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let store = RedbPeerStore::open(&path, Network::Regtest).unwrap();
        store
            .insert_discovered(
                "127.0.0.2:18444".parse().unwrap(),
                &[learned("127.0.0.1:18444", services, now)],
                now,
            )
            .unwrap();

        assert!(store.record_attempt(address, now).unwrap());
        assert!(store.candidates(now + 59, 16).unwrap().is_empty());
        assert_eq!(store.candidates(now + 60, 16).unwrap(), vec![address]);
        assert!(store.record_attempt(address, now + 60).unwrap());
        store
            .insert_discovered(
                "127.1.0.2:18444".parse().unwrap(),
                &[learned("127.0.0.1:18444", services, now + 61)],
                now + 61,
            )
            .unwrap();
        assert_eq!(
            store.load_records().unwrap()[&address.to_string()].source_group,
            "v4:127:0"
        );
        assert!(store.candidates(now + 179, 16).unwrap().is_empty());
        drop(store);

        let reopened = RedbPeerStore::open(path, Network::Regtest).unwrap();
        assert_eq!(reopened.candidates(now + 180, 16).unwrap(), vec![address]);
        assert!(reopened.record_success(address, now + 61).unwrap());
        assert_eq!(reopened.candidates(now + 61, 16).unwrap(), vec![address]);
        let unproven = "127.0.0.3:18444".parse().unwrap();
        reopened
            .insert_discovered(
                "127.0.0.2:18444".parse().unwrap(),
                &[learned("127.0.0.3:18444", services, now + 62)],
                now + 62,
            )
            .unwrap();
        assert_eq!(
            reopened.candidates(now + 62, 16).unwrap(),
            vec![address, unproven]
        );
        assert!(
            !reopened
                .record_attempt("127.0.0.9:18444".parse().unwrap(), now)
                .unwrap()
        );

        let maximally_failed = StoredPeer {
            services: services.to_u64(),
            last_seen: now,
            source_group: "v4:127:0".to_owned(),
            last_attempt: now,
            last_success: 0,
            consecutive_failures: u8::MAX,
            last_session_success: 0,
            successful_sessions: 0,
        };
        assert!(!retry_ready(
            &maximally_failed,
            now + MAX_RETRY_BACKOFF_SECS - 1
        ));
        assert!(retry_ready(&maximally_failed, now + MAX_RETRY_BACKOFF_SECS));
    }

    #[test]
    fn protocol_discouragement_persists_expires_escalates_and_clears_on_success() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("peers.redb");
        let now: u32 = 1_800_000_000;
        let address = "127.0.0.1:18444".parse().unwrap();
        let services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let store = RedbPeerStore::open(&path, Network::Regtest).unwrap();
        store
            .insert_verified(address, services, now.saturating_sub(1))
            .unwrap();

        let first_until = store.record_protocol_violation(address, now).unwrap();
        assert_eq!(first_until, now + INITIAL_PROTOCOL_COOLDOWN_SECS);
        assert!(store.is_discouraged(address, now).unwrap());
        assert!(store.candidates(now, 16).unwrap().is_empty());
        drop(store);

        let reopened = RedbPeerStore::open(&path, Network::Regtest).unwrap();
        assert!(reopened.is_discouraged(address, now + 1).unwrap());
        assert_eq!(
            reopened
                .candidates(now + INITIAL_PROTOCOL_COOLDOWN_SECS, 16)
                .unwrap(),
            vec![address]
        );
        let second_at = now + INITIAL_PROTOCOL_COOLDOWN_SECS;
        let second_until = reopened
            .record_protocol_violation(address, second_at)
            .unwrap();
        assert_eq!(second_until, second_at + 2 * INITIAL_PROTOCOL_COOLDOWN_SECS);
        assert!(
            reopened
                .discouraged_addresses(second_at)
                .unwrap()
                .contains(&address)
        );

        let after_decay = second_at + PROTOCOL_VIOLATION_DECAY_SECS + 1;
        assert_eq!(
            reopened
                .record_protocol_violation(address, after_decay)
                .unwrap(),
            after_decay + INITIAL_PROTOCOL_COOLDOWN_SECS
        );
        assert_eq!(
            reopened.load_penalties().unwrap()[&address.to_string()].violations,
            1
        );
        reopened
            .insert_verified(address, services, after_decay + 1)
            .unwrap();
        assert!(reopened.is_discouraged(address, after_decay + 1).unwrap());
        reopened
            .record_session_success(address, after_decay + 1)
            .unwrap();
        assert!(!reopened.is_discouraged(address, after_decay + 1).unwrap());
        assert!(reopened.load_penalties().unwrap().is_empty());
        let records = reopened.load_records().unwrap();
        let record = &records[&address.to_string()];
        assert_eq!(record.last_session_success, after_decay + 1);
        assert_eq!(record.successful_sessions, 1);
    }

    #[test]
    fn candidate_selection_round_robins_target_network_groups() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Signet).unwrap();
        let now = 1_800_000_000;
        let services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        store
            .insert_discovered(
                "8.8.8.8:38333".parse().unwrap(),
                &[
                    learned("1.1.1.1:38333", services, now),
                    learned("1.1.2.1:38333", services, now - 1),
                    learned("1.1.3.1:38333", services, now - 2),
                    learned("2.2.2.2:38333", services, now - 3),
                    learned("3.3.3.3:38333", services, now - 4),
                ],
                now,
            )
            .unwrap();

        let selected = store.candidates(now, 3).unwrap();
        assert_eq!(selected[0], "1.1.1.1:38333".parse().unwrap());
        assert!(selected.contains(&"2.2.2.2:38333".parse().unwrap()));
        assert!(selected.contains(&"3.3.3.3:38333".parse().unwrap()));
        assert_eq!(
            selected
                .iter()
                .map(|address| source_group(address.ip()))
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
    }

    #[test]
    fn completed_session_reputation_persists_and_outranks_newer_handshakes() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("peers.redb");
        let store = RedbPeerStore::open(&path, Network::Signet).unwrap();
        let now: u32 = 1_800_000_000;
        let services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let proven = "1.1.1.1:38333".parse().unwrap();
        let handshake_only = "2.2.2.2:38333".parse().unwrap();
        store
            .insert_verified(proven, services, now.saturating_sub(100))
            .unwrap();
        store
            .record_session_success(proven, now.saturating_sub(90))
            .unwrap();
        store
            .insert_verified(handshake_only, services, now)
            .unwrap();
        assert_eq!(
            store.candidates(now, 2).unwrap(),
            vec![proven, handshake_only]
        );
        store.record_session_success(proven, now + 1).unwrap();
        drop(store);

        let reopened = RedbPeerStore::open(path, Network::Signet).unwrap();
        assert_eq!(
            reopened.candidates(now + 1, 2).unwrap(),
            vec![proven, handshake_only]
        );
        let records = reopened.load_records().unwrap();
        let record = &records[&proven.to_string()];
        assert_eq!(record.last_session_success, now + 1);
        assert_eq!(record.successful_sessions, 2);
    }

    #[test]
    fn loads_records_written_before_attempt_history_fields_existed() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        let now = 1_800_000_000;
        let address = "127.0.0.1:18444";
        let services = (ServiceFlags::NETWORK | ServiceFlags::WITNESS).to_u64();
        let legacy = serde_json::to_vec(&serde_json::json!({
            "services": services,
            "last_seen": now,
            "source_group": "v4:127:0"
        }))
        .unwrap();
        let transaction = store.db.begin_write().unwrap();
        {
            let mut table = transaction.open_table(PEERS).unwrap();
            table.insert(address, legacy.as_slice()).unwrap();
        }
        transaction.commit().unwrap();

        let socket = address.parse().unwrap();
        assert_eq!(store.candidates(now, 16).unwrap(), vec![socket]);
        assert!(store.record_attempt(socket, now).unwrap());
        assert!(store.candidates(now, 16).unwrap().is_empty());
    }

    #[test]
    fn persisted_record_parsers_are_strict_bounded_and_semantic() {
        let services = (ServiceFlags::NETWORK | ServiceFlags::WITNESS).to_u64();
        let valid_peer = serde_json::to_vec(&serde_json::json!({
            "services": services,
            "last_seen": 1_800_000_000_u32,
            "source_group": "v4:127:0",
            "last_attempt": 0,
            "last_success": 0,
            "consecutive_failures": 0
        }))
        .unwrap();
        validate_stored_peer_record(&valid_peer).unwrap();
        validate_stored_peer_penalty(
            br#"{"violations":1,"last_violation":1800000000,"discouraged_until":1800003600}"#,
        )
        .unwrap();

        for invalid in [
            br#"{"services":9,"last_seen":1,"source_group":"v4:01:2"}"#.as_slice(),
            br#"{"services":9,"last_seen":1,"source_group":"v4:1:2","extra":true}"#,
            br#"{"services":9,"last_seen":1,"source_group":"other:1:2"}"#,
            br#"{"services":9,"last_seen":1,"source_group":"v4:1:2","last_success":1,"last_session_success":1,"successful_sessions":0}"#,
            br#"{"services":9,"last_seen":1,"source_group":"v4:1:2","last_success":1,"last_session_success":2,"successful_sessions":1}"#,
        ] {
            assert!(validate_stored_peer_record(invalid).is_err());
        }
        for invalid in [
            br#"{"violations":0,"last_violation":1,"discouraged_until":2}"#.as_slice(),
            br#"{"violations":1,"last_violation":2,"discouraged_until":1}"#,
            br#"{"violations":1,"last_violation":1,"discouraged_until":2,"extra":true}"#,
        ] {
            assert!(validate_stored_peer_penalty(invalid).is_err());
        }
        assert!(validate_stored_peer_record(&vec![b' '; MAX_STORED_PEER_BYTES + 1]).is_err());
        assert!(validate_stored_peer_penalty(&vec![b' '; MAX_STORED_PENALTY_BYTES + 1]).is_err());
    }

    #[test]
    fn malformed_persisted_records_fail_without_rewriting_the_store() {
        let directory = TempDir::new().unwrap();
        let store =
            RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap();
        let address = "127.0.0.1:18444";
        let malformed =
            br#"{"services":9,"last_seen":1800000000,"source_group":"v4:127:0","unknown":1}"#;
        let transaction = store.db.begin_write().unwrap();
        {
            let mut table = transaction.open_table(PEERS).unwrap();
            table.insert(address, malformed.as_slice()).unwrap();
        }
        transaction.commit().unwrap();

        assert!(matches!(
            store.candidates(1_800_000_000, 16),
            Err(PeerStoreError::Encoding(_))
        ));
        let transaction = store.db.begin_read().unwrap();
        let table = transaction.open_table(PEERS).unwrap();
        assert_eq!(table.get(address).unwrap().unwrap().value(), malformed);
    }

    #[test]
    fn capacity_retains_successful_and_unfailed_peers_before_failed_ones() {
        let record = |last_seen, last_success, consecutive_failures| StoredPeer {
            services: (ServiceFlags::NETWORK | ServiceFlags::WITNESS).to_u64(),
            last_seen,
            source_group: "v4:1:1".to_owned(),
            last_attempt: 0,
            last_success,
            consecutive_failures,
            last_session_success: 0,
            successful_sessions: 0,
        };
        let records = HashMap::from([
            ("1.1.1.1:8333".to_owned(), record(100, 90, 0)),
            ("2.2.2.2:8333".to_owned(), record(110, 0, 0)),
            ("3.3.3.3:8333".to_owned(), record(120, 100, 1)),
        ]);

        let retained = retain_best(records, 2)
            .into_iter()
            .map(|(address, _)| address)
            .collect::<Vec<_>>();
        assert_eq!(retained, vec!["1.1.1.1:8333", "2.2.2.2:8333"]);
    }
}
