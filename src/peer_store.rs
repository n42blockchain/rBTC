//! Bounded, network-bound persistence for peer addresses learned over P2P.

use std::{collections::HashMap, net::IpAddr, path::Path, str::FromStr, sync::Mutex};

use bitcoin::{
    Network,
    hashes::{Hash, HashEngine, sha256},
    p2p::ServiceFlags,
};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::p2p::PeerAddress;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("peer_metadata");
const PEERS: TableDefinition<&str, &[u8]> = TableDefinition::new("peer_addresses");
const PENALTIES: TableDefinition<&str, &[u8]> = TableDefinition::new("peer_penalties");
const GENESIS_KEY: &str = "genesis";
const BUCKET_KEY: &str = "bucket_key";
const BUCKET_KEY_LEN: usize = 32;
const MAX_STORED_PEERS: usize = 4_096;
const MAX_PEERS_PER_SOURCE_GROUP: usize = 64;
const NEW_BUCKET_COUNT: u16 = 1_024;
const TRIED_BUCKET_COUNT: u16 = 256;
const BUCKET_CAPACITY: usize = 64;
const NEW_BUCKETS_PER_SOURCE_GROUP: u64 = 64;
const TRIED_BUCKETS_PER_ADDRESS_GROUP: u64 = 8;
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
const MAX_RECORDED_HANDSHAKE_MILLIS: u32 = 60_000;
const MAX_RECORDED_BLOCK_THROUGHPUT_BPS: u32 = 1_000_000_000;

type PeerPriority = (
    u8,
    u8,
    std::cmp::Reverse<u32>,
    std::cmp::Reverse<u32>,
    u8,
    u32,
    std::cmp::Reverse<u32>,
    std::cmp::Reverse<u32>,
    std::cmp::Reverse<u32>,
);
type PeerBucket = (bool, u16);
type PrioritizedPeer = (PeerPriority, PeerBucket, std::net::SocketAddr);

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
    #[serde(default)]
    handshake_millis: u32,
    #[serde(default)]
    block_throughput_bps: u32,
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
    bucket_key: [u8; BUCKET_KEY_LEN],
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
        let bucket_key = {
            let mut meta = transaction.open_table(META)?;
            if let Some(stored) = meta.get(GENESIS_KEY)? {
                if stored.value() != genesis {
                    return Err(PeerStoreError::NetworkMismatch);
                }
            } else {
                meta.insert(GENESIS_KEY, genesis.as_slice())?;
            }
            let bucket_key = if let Some(stored) = meta.get(BUCKET_KEY)? {
                stored
                    .value()
                    .try_into()
                    .map_err(|_| PeerStoreError::Malformed("peer bucket key"))?
            } else {
                let bucket_key = rand::random::<[u8; BUCKET_KEY_LEN]>();
                meta.insert(BUCKET_KEY, bucket_key.as_slice())?;
                bucket_key
            };
            let _peers = transaction.open_table(PEERS)?;
            let _penalties = transaction.open_table(PENALTIES)?;
            bucket_key
        };
        transaction.commit()?;
        Ok(Self {
            db,
            network,
            bucket_key,
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
            .filter(|record| record.last_success == 0 && record.source_group == group)
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
                handshake_millis,
                block_throughput_bps,
            ) = existing.map_or_else(
                || (group.clone(), 0, 0, 0, 0, 0, 0, 0),
                |record| {
                    (
                        record.source_group.clone(),
                        record.last_attempt,
                        record.last_success,
                        record.consecutive_failures,
                        record.last_session_success,
                        record.successful_sessions,
                        record.handshake_millis,
                        record.block_throughput_bps,
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
                    handshake_millis,
                    block_throughput_bps,
                },
            );
            stats.accepted += 1;
        }

        let ordered = retain_bucketed(records, MAX_STORED_PEERS, &self.bucket_key);
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
        let required = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        if !services.has(required) || !is_acceptable_peer_address(address, self.network) {
            return Ok(false);
        }
        let _guard = self.write_guard.lock().expect("peer lock not poisoned");
        let mut records = self.load_records()?;
        records.retain(|stored_address, record| {
            std::net::SocketAddr::from_str(stored_address).is_ok_and(|socket| {
                ServiceFlags::from(record.services).has(required)
                    && is_acceptable_peer_address(socket, self.network)
                    && now.saturating_sub(record.last_seen) <= ADDRESS_HORIZON_SECS
            })
        });
        let key = address.to_string();
        let existing = records.remove(&key);
        let mut record = existing.unwrap_or_else(|| StoredPeer {
            services: services.to_u64(),
            last_seen: normalize_last_seen(now, now),
            source_group: source_group(address.ip()),
            last_attempt: 0,
            last_success: 0,
            consecutive_failures: 0,
            last_session_success: 0,
            successful_sessions: 0,
            handshake_millis: 0,
            block_throughput_bps: 0,
        });
        record.services = services.to_u64();
        record.last_seen = normalize_last_seen(now, now).max(record.last_seen);
        record.last_success = now;
        record.consecutive_failures = 0;
        records.insert(key.clone(), record);
        let ordered = retain_bucketed(records, MAX_STORED_PEERS, &self.bucket_key);
        let retained = ordered.iter().any(|(stored, _)| stored == &key);
        self.replace_all(&ordered)?;
        Ok(retained)
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
                .map(|(record, socket)| {
                    (
                        peer_priority(&record),
                        peer_bucket(&self.bucket_key, socket, &record),
                        socket,
                    )
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| left.2.cmp(&right.2))
        });
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

    /// Records the most recent successful outbound handshake latency.
    ///
    /// Zero remains reserved for legacy/unknown records, and extreme values
    /// are capped so a clock anomaly cannot dominate candidate scoring.
    pub fn record_handshake_latency(
        &self,
        address: std::net::SocketAddr,
        millis: u32,
    ) -> Result<bool, PeerStoreError> {
        let millis = millis.clamp(1, MAX_RECORDED_HANDSHAKE_MILLIS);
        self.update_existing(address, |record| {
            record.handshake_millis = millis;
        })
    }

    /// Records completed requested-block payload throughput for candidate scoring.
    pub fn record_block_throughput(
        &self,
        address: std::net::SocketAddr,
        bytes_per_second: u32,
    ) -> Result<bool, PeerStoreError> {
        let bytes_per_second = bytes_per_second.clamp(1, MAX_RECORDED_BLOCK_THROUGHPUT_BPS);
        self.update_existing(address, |record| {
            record.block_throughput_bps = bytes_per_second;
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
        let key = address.to_string();
        let Some(record) = records.get_mut(&key) else {
            return Ok(false);
        };
        update(record);
        let records = retain_bucketed(records, MAX_STORED_PEERS, &self.bucket_key);
        self.replace_all(&records)?;
        Ok(records.iter().any(|(stored, _)| stored == &key))
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
    if record.handshake_millis > MAX_RECORDED_HANDSHAKE_MILLIS {
        return Err(PeerStoreError::Malformed("peer handshake latency"));
    }
    if record.block_throughput_bps > MAX_RECORDED_BLOCK_THROUGHPUT_BPS {
        return Err(PeerStoreError::Malformed("peer block throughput"));
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
        u8::from(record.handshake_millis == 0),
        record.handshake_millis,
        std::cmp::Reverse(record.block_throughput_bps),
        std::cmp::Reverse(record.last_success),
        std::cmp::Reverse(record.last_seen),
    )
}

fn bucket_hash(key: &[u8; BUCKET_KEY_LEN], domain: &[u8], components: &[&[u8]]) -> u64 {
    let mut engine = sha256::Hash::engine();
    engine.input(key);
    engine.input(
        &u64::try_from(domain.len())
            .expect("bucket domain length fits u64")
            .to_le_bytes(),
    );
    engine.input(domain);
    for component in components {
        engine.input(
            &u64::try_from(component.len())
                .expect("bucket component length fits u64")
                .to_le_bytes(),
        );
        engine.input(component);
    }
    let digest = sha256::Hash::from_engine(engine).to_byte_array();
    u64::from_le_bytes(digest[..8].try_into().expect("SHA-256 prefix is fixed"))
}

fn address_bucket_key(address: std::net::SocketAddr) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(19);
    match address.ip() {
        IpAddr::V4(ip) => {
            encoded.push(4);
            encoded.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            encoded.push(6);
            encoded.extend_from_slice(&ip.octets());
        }
    }
    encoded.extend_from_slice(&address.port().to_be_bytes());
    encoded
}

fn new_bucket(
    key: &[u8; BUCKET_KEY_LEN],
    address: std::net::SocketAddr,
    learned_from: &str,
) -> u16 {
    let address_group = source_group(address.ip());
    let selector = bucket_hash(
        key,
        b"new-source",
        &[learned_from.as_bytes(), address_group.as_bytes()],
    ) % NEW_BUCKETS_PER_SOURCE_GROUP;
    let selector = selector.to_le_bytes();
    u16::try_from(
        bucket_hash(key, b"new-bucket", &[learned_from.as_bytes(), &selector])
            % u64::from(NEW_BUCKET_COUNT),
    )
    .expect("new bucket index fits u16")
}

fn tried_bucket(key: &[u8; BUCKET_KEY_LEN], address: std::net::SocketAddr) -> u16 {
    let address_key = address_bucket_key(address);
    let address_group = source_group(address.ip());
    let selector =
        bucket_hash(key, b"tried-address", &[&address_key]) % TRIED_BUCKETS_PER_ADDRESS_GROUP;
    let selector = selector.to_le_bytes();
    u16::try_from(
        bucket_hash(key, b"tried-bucket", &[address_group.as_bytes(), &selector])
            % u64::from(TRIED_BUCKET_COUNT),
    )
    .expect("tried bucket index fits u16")
}

fn peer_bucket(
    key: &[u8; BUCKET_KEY_LEN],
    address: std::net::SocketAddr,
    record: &StoredPeer,
) -> PeerBucket {
    if record.last_success == 0 {
        (false, new_bucket(key, address, &record.source_group))
    } else {
        (true, tried_bucket(key, address))
    }
}

fn retain_bucketed(
    records: HashMap<String, StoredPeer>,
    limit: usize,
    key: &[u8; BUCKET_KEY_LEN],
) -> Vec<(String, StoredPeer)> {
    let mut ordered = records.into_iter().collect::<Vec<_>>();
    ordered.sort_unstable_by(|(left_address, left), (right_address, right)| {
        peer_priority(left)
            .cmp(&peer_priority(right))
            .then_with(|| left_address.cmp(right_address))
    });
    let mut bucket_counts = HashMap::new();
    let mut retained = Vec::with_capacity(limit.min(ordered.len()));
    for (address, record) in ordered {
        let socket = std::net::SocketAddr::from_str(&address)
            .expect("stored peer address was validated before retention");
        let bucket = peer_bucket(key, socket, &record);
        let count = bucket_counts.entry(bucket).or_insert(0_usize);
        if *count == BUCKET_CAPACITY {
            continue;
        }
        *count += 1;
        retained.push((address, record));
        if retained.len() == limit {
            break;
        }
    }
    retained
}

fn diversify_candidates(ordered: Vec<PrioritizedPeer>, limit: usize) -> Vec<std::net::SocketAddr> {
    let mut group_indexes = HashMap::new();
    let mut groups: Vec<Vec<(PeerBucket, std::net::SocketAddr)>> = Vec::new();
    for (_, bucket, address) in ordered {
        let group = source_group(address.ip());
        let index = *group_indexes.entry(group).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[index].push((bucket, address));
    }
    let groups = groups
        .into_iter()
        .map(diversify_peer_buckets)
        .collect::<Vec<_>>();

    let mut selected = Vec::with_capacity(limit);
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

fn diversify_peer_buckets(
    ordered: Vec<(PeerBucket, std::net::SocketAddr)>,
) -> Vec<std::net::SocketAddr> {
    let mut bucket_indexes = HashMap::new();
    let mut buckets: Vec<Vec<std::net::SocketAddr>> = Vec::new();
    for (bucket, address) in ordered {
        let index = *bucket_indexes.entry(bucket).or_insert_with(|| {
            buckets.push(Vec::new());
            buckets.len() - 1
        });
        buckets[index].push(address);
    }
    let mut selected = Vec::with_capacity(buckets.iter().map(Vec::len).sum());
    let mut offset = 0;
    loop {
        let before = selected.len();
        for bucket in &buckets {
            if let Some(address) = bucket.get(offset) {
                selected.push(*address);
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
    fn bucket_key_migrates_legacy_metadata_persists_and_rejects_corruption() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("legacy.redb");
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Signet)
            .block_hash()
            .to_byte_array();
        let database = Database::create(&path).unwrap();
        let transaction = database.begin_write().unwrap();
        {
            let mut meta = transaction.open_table(META).unwrap();
            meta.insert(GENESIS_KEY, genesis.as_slice()).unwrap();
            let _peers = transaction.open_table(PEERS).unwrap();
            let _penalties = transaction.open_table(PENALTIES).unwrap();
        }
        transaction.commit().unwrap();
        drop(database);

        let store = RedbPeerStore::open(&path, Network::Signet).unwrap();
        let key = store.bucket_key;
        let address = "1.2.3.4:38333".parse().unwrap();
        let new = new_bucket(&key, address, "v4:8:8");
        let tried = tried_bucket(&key, address);
        drop(store);

        let reopened = RedbPeerStore::open(&path, Network::Signet).unwrap();
        assert_eq!(reopened.bucket_key, key);
        assert_eq!(new_bucket(&reopened.bucket_key, address, "v4:8:8"), new);
        assert_eq!(tried_bucket(&reopened.bucket_key, address), tried);
        let transaction = reopened.db.begin_write().unwrap();
        {
            let mut meta = transaction.open_table(META).unwrap();
            meta.insert(BUCKET_KEY, &[1_u8, 2, 3][..]).unwrap();
        }
        transaction.commit().unwrap();
        drop(reopened);
        assert!(matches!(
            RedbPeerStore::open(path, Network::Signet),
            Err(PeerStoreError::Malformed("peer bucket key"))
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
        let promoted = store.candidates(now, 1).unwrap()[0];
        assert!(
            store
                .record_success(promoted, now.saturating_add(1))
                .unwrap()
        );
        let stats = store
            .insert_discovered(
                "127.0.0.2:18444".parse().unwrap(),
                &[learned("127.0.0.1:41000", services, now.saturating_add(2))],
                now.saturating_add(2),
            )
            .unwrap();
        assert_eq!(stats.accepted, 1);
        assert_eq!(store.len().unwrap(), MAX_PEERS_PER_SOURCE_GROUP + 1);
        assert!(
            store
                .insert_verified(
                    "127.0.0.1:40000".parse().unwrap(),
                    services,
                    now.saturating_add(1),
                )
                .unwrap()
        );
        assert_eq!(store.len().unwrap(), MAX_PEERS_PER_SOURCE_GROUP + 2);
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
            handshake_millis: 0,
            block_throughput_bps: 0,
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
    fn measured_handshake_latency_is_bounded_persisted_and_ranks_unknown_last() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("peers.redb");
        let store = RedbPeerStore::open(&path, Network::Signet).unwrap();
        let now = 1_800_000_000;
        let services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let fast = "1.1.1.1:38333".parse().unwrap();
        let slow = "2.2.2.2:38333".parse().unwrap();
        let unknown = "3.3.3.3:38333".parse().unwrap();
        for address in [fast, slow, unknown] {
            store.insert_verified(address, services, now).unwrap();
        }
        store.record_handshake_latency(fast, 0).unwrap();
        store.record_handshake_latency(slow, u32::MAX).unwrap();
        assert_eq!(store.candidates(now, 3).unwrap(), vec![fast, slow, unknown]);
        drop(store);

        let reopened = RedbPeerStore::open(path, Network::Signet).unwrap();
        let records = reopened.load_records().unwrap();
        assert_eq!(records[&fast.to_string()].handshake_millis, 1);
        assert_eq!(
            records[&slow.to_string()].handshake_millis,
            MAX_RECORDED_HANDSHAKE_MILLIS
        );
        assert_eq!(records[&unknown.to_string()].handshake_millis, 0);
        assert_eq!(
            reopened.candidates(now, 3).unwrap(),
            vec![fast, slow, unknown]
        );
    }

    #[test]
    fn measured_block_throughput_is_bounded_persisted_and_ranks_higher_first() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("peers.redb");
        let store = RedbPeerStore::open(&path, Network::Signet).unwrap();
        let now = 1_800_000_000;
        let services = ServiceFlags::NETWORK | ServiceFlags::WITNESS;
        let fast = "4.4.4.4:38333".parse().unwrap();
        let slow = "5.5.5.5:38333".parse().unwrap();
        let unknown = "6.6.6.6:38333".parse().unwrap();
        for address in [fast, slow, unknown] {
            store.insert_verified(address, services, now).unwrap();
            store.record_handshake_latency(address, 10).unwrap();
        }
        store.record_block_throughput(fast, u32::MAX).unwrap();
        store.record_block_throughput(slow, 0).unwrap();
        assert_eq!(store.candidates(now, 3).unwrap(), vec![fast, slow, unknown]);
        drop(store);

        let reopened = RedbPeerStore::open(path, Network::Signet).unwrap();
        let records = reopened.load_records().unwrap();
        assert_eq!(
            records[&fast.to_string()].block_throughput_bps,
            MAX_RECORDED_BLOCK_THROUGHPUT_BPS
        );
        assert_eq!(records[&slow.to_string()].block_throughput_bps, 1);
        assert_eq!(records[&unknown.to_string()].block_throughput_bps, 0);
        assert_eq!(
            reopened.candidates(now, 3).unwrap(),
            vec![fast, slow, unknown]
        );
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
            br#"{"services":9,"last_seen":1,"source_group":"v4:1:2","handshake_millis":60001}"#,
            br#"{"services":9,"last_seen":1,"source_group":"v4:1:2","block_throughput_bps":1000000001}"#,
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
            handshake_millis: 0,
            block_throughput_bps: 0,
        };
        let records = HashMap::from([
            ("1.1.1.1:8333".to_owned(), record(100, 90, 0)),
            ("2.2.2.2:8333".to_owned(), record(110, 0, 0)),
            ("3.3.3.3:8333".to_owned(), record(120, 100, 1)),
        ]);

        let retained = retain_bucketed(records, 2, &[7; BUCKET_KEY_LEN])
            .into_iter()
            .map(|(address, _)| address)
            .collect::<Vec<_>>();
        assert_eq!(retained, vec!["1.1.1.1:8333", "2.2.2.2:8333"]);
    }

    #[test]
    fn keyed_new_and_tried_buckets_enforce_independent_capacity() {
        let key = [11; BUCKET_KEY_LEN];
        let services = (ServiceFlags::NETWORK | ServiceFlags::WITNESS).to_u64();
        let make_record = |last_success| StoredPeer {
            services,
            last_seen: 1_800_000_000,
            source_group: "v4:8:8".to_owned(),
            last_attempt: 0,
            last_success,
            consecutive_failures: 0,
            last_session_success: 0,
            successful_sessions: 0,
            handshake_millis: 0,
            block_throughput_bps: 0,
        };

        let mut tried_by_bucket: HashMap<u16, Vec<std::net::SocketAddr>> = HashMap::new();
        for third in 0_u8..=u8::MAX {
            for fourth in 1_u8..=u8::MAX {
                let address = format!("1.1.{third}.{fourth}:8333").parse().unwrap();
                let bucket = tried_bucket(&key, address);
                let addresses = tried_by_bucket.entry(bucket).or_default();
                addresses.push(address);
                if addresses.len() > BUCKET_CAPACITY {
                    break;
                }
            }
            if tried_by_bucket
                .values()
                .any(|addresses| addresses.len() > BUCKET_CAPACITY)
            {
                break;
            }
        }
        let tried = tried_by_bucket
            .into_values()
            .find(|addresses| addresses.len() > BUCKET_CAPACITY)
            .unwrap();
        let tried_records = tried
            .into_iter()
            .map(|address| (address.to_string(), make_record(1_799_999_999)))
            .collect();
        assert_eq!(
            retain_bucketed(tried_records, MAX_STORED_PEERS, &key).len(),
            BUCKET_CAPACITY
        );

        let mut new_by_bucket: HashMap<u16, Vec<std::net::SocketAddr>> = HashMap::new();
        for third in 0_u8..=u8::MAX {
            for fourth in 1_u8..=u8::MAX {
                let address = format!("2.2.{third}.{fourth}:8333").parse().unwrap();
                let bucket = new_bucket(&key, address, "v4:8:8");
                let addresses = new_by_bucket.entry(bucket).or_default();
                addresses.push(address);
                if addresses.len() > BUCKET_CAPACITY {
                    break;
                }
            }
            if new_by_bucket
                .values()
                .any(|addresses| addresses.len() > BUCKET_CAPACITY)
            {
                break;
            }
        }
        let new = new_by_bucket
            .into_values()
            .find(|addresses| addresses.len() > BUCKET_CAPACITY)
            .unwrap();
        let new_records = new
            .into_iter()
            .map(|address| (address.to_string(), make_record(0)))
            .collect();
        assert_eq!(
            retain_bucketed(new_records, MAX_STORED_PEERS, &key).len(),
            BUCKET_CAPACITY
        );
    }

    #[test]
    fn candidate_bucket_diversification_round_robins_equal_target_groups() {
        let first = "1.1.1.1:8333".parse().unwrap();
        let second = "1.1.2.2:8333".parse().unwrap();
        let other_bucket = "1.1.3.3:8333".parse().unwrap();
        assert_eq!(
            diversify_peer_buckets(vec![
                ((false, 1), first),
                ((false, 1), second),
                ((false, 2), other_bucket),
            ]),
            vec![first, other_bucket, second]
        );
    }
}
