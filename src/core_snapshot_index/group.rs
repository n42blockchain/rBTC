//! Bounded canonical-vout sorting of first-pass decoded snapshot coins.
use crate::{
    core_snapshot::{CoreSnapshotError, MAX_COINS_PER_TXID, update_core_utxo_hash},
    node_memory::{MemoryBudget, MemoryLease, SpoolLease},
    utxo::{OutPointKey, Utxo},
};
use bitcoin::hashes::sha256;
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

const WINDOW: usize = 64 * 1024;
const MAX_SCRIPT: usize = 10_000;
const HEADER: usize = 17;

pub(super) struct Group {
    offsets: Vec<(u32, u64)>,
    bytes: Vec<u8>,
    file: Option<File>,
    length: u64,
    count: usize,
    // File and all payloads must drop before their allowances.
    disk: Option<SpoolLease>,
    _memory: Option<MemoryLease>,
}

impl Group {
    pub(super) fn new(count: u64, memory: Option<&MemoryBudget>) -> io::Result<Self> {
        if count == 0 || count > MAX_COINS_PER_TXID {
            return Err(io::Error::other("invalid spool group count"));
        }
        let count = usize::try_from(count).map_err(io::Error::other)?;
        // Includes the bounded buffer, one decoded script and codec scratch.
        let allowance = (count * std::mem::size_of::<(u32, u64)>() + 128 * 1024) as u64;
        let reservation = memory.map(|budget| budget.reserve(allowance)).transpose()?;
        Ok(Self {
            offsets: Vec::with_capacity(count),
            bytes: Vec::with_capacity((count * 64).min(WINDOW)),
            file: None,
            length: 0,
            count,
            disk: None,
            _memory: reservation,
        })
    }

    pub(super) fn push(
        &mut self,
        vout: u32,
        coin: &Utxo,
        directory: &Path,
        memory: Option<&MemoryBudget>,
    ) -> io::Result<()> {
        if self.offsets.len() == self.count || coin.script_pubkey.len() > MAX_SCRIPT {
            return Err(io::Error::other("spool coin exceeds admitted bounds"));
        }
        let length = HEADER + coin.script_pubkey.len();
        if self.file.is_none() && self.bytes.len() + length > WINDOW {
            // Charge the group's bounded maximum before creating or growing its
            // file; independent builders share this with execution spools.
            let disk = memory
                .map(|budget| budget.reserve_spool((self.count * (HEADER + MAX_SCRIPT)) as u64))
                .transpose()?;
            let mut file = tempfile::tempfile_in(directory)?;
            file.write_all(&self.bytes)?;
            self.file = Some(file);
            self.disk = disk;
            self.bytes.clear();
        }
        let mut header = [0; HEADER];
        header[..4].copy_from_slice(&coin.height.to_le_bytes());
        header[4] = u8::from(coin.is_coinbase);
        header[5..13].copy_from_slice(&coin.value_sats.to_le_bytes());
        header[13..].copy_from_slice(
            &u32::try_from(coin.script_pubkey.len())
                .expect("bounded script")
                .to_le_bytes(),
        );
        if let Some(file) = &mut self.file {
            file.write_all(&header)?;
            file.write_all(&coin.script_pubkey)?;
        } else {
            if self.bytes.capacity() < self.bytes.len() + length {
                self.bytes.reserve_exact(WINDOW - self.bytes.len());
            }
            self.bytes.extend_from_slice(&header);
            self.bytes.extend_from_slice(&coin.script_pubkey);
        }
        self.offsets.push((vout, self.length));
        self.length += length as u64;
        Ok(())
    }

    pub(super) fn hash(
        mut self,
        txid: [u8; 32],
        engine: &mut sha256::HashEngine,
    ) -> Result<(), CoreSnapshotError> {
        if self.offsets.len() != self.count {
            return Err(CoreSnapshotError::Invalid("incomplete spool group"));
        }
        self.offsets.sort_unstable_by_key(|(vout, _)| *vout);
        if self.offsets.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(CoreSnapshotError::Invalid("duplicate output index"));
        }
        for &(vout, offset) in &self.offsets {
            let coin = if let Some(file) = &mut self.file {
                file.seek(SeekFrom::Start(offset))?;
                read_coin(file)?
            } else {
                read_coin(
                    &mut &self.bytes[usize::try_from(offset).expect("in-memory group offset")..],
                )?
            };
            let mut key = [0; 36];
            key[..32].copy_from_slice(&txid);
            key[32..].copy_from_slice(&vout.to_le_bytes());
            update_core_utxo_hash(
                engine,
                OutPointKey::from_bytes(&key).expect("fixed key"),
                &coin,
            );
        }
        Ok(())
    }
}

fn read_coin(reader: &mut impl Read) -> io::Result<Utxo> {
    let mut header = [0; HEADER];
    reader.read_exact(&mut header)?;
    let length = u32::from_le_bytes(header[13..].try_into().expect("fixed")) as usize;
    if length > MAX_SCRIPT || header[4] > 1 {
        return Err(io::Error::other("invalid private spool record"));
    }
    let mut script_pubkey = vec![0; length];
    reader.read_exact(&mut script_pubkey)?;
    Ok(Utxo {
        height: u32::from_le_bytes(header[..4].try_into().expect("fixed")),
        is_coinbase: header[4] == 1,
        value_sats: u64::from_le_bytes(header[5..13].try_into().expect("fixed")),
        script_pubkey,
        last_touched: 0,
        creation_mtp: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::{Hash as _, sha256d};
    fn coin(vout: u32) -> Utxo {
        Utxo {
            value_sats: u64::from(vout) + 1,
            height: vout,
            is_coinbase: vout % 2 == 0,
            script_pubkey: vec![vout.to_le_bytes()[0]; MAX_SCRIPT],
            last_touched: 0,
            creation_mtp: 0,
        }
    }
    fn expected(count: u32) -> sha256::HashEngine {
        let mut engine = sha256d::Hash::engine();
        for vout in 0..count {
            let mut key = [9; 36];
            key[32..].copy_from_slice(&vout.to_le_bytes());
            update_core_utxo_hash(
                &mut engine,
                OutPointKey::from_bytes(&key).unwrap(),
                &coin(vout),
            );
        }
        engine
    }
    #[test]
    fn large_unsorted_groups_spill_and_hash_first_pass_bytes_with_bounded_memory() {
        let directory = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(256 * 1024);
        for count in [3, 1000] {
            let mut group = Group::new(count, Some(&budget)).unwrap();
            for vout in (0..u32::try_from(count).unwrap()).rev() {
                group
                    .push(vout, &coin(vout), directory.path(), Some(&budget))
                    .unwrap();
            }
            assert_eq!(group.file.is_some(), count > 3);
            assert!(group.bytes.capacity() <= WINDOW);
            assert!(budget.snapshot().used < 256 * 1024);
            assert_eq!(budget.spool_snapshot().used > 0, count > 3);
            let mut engine = sha256d::Hash::engine();
            group.hash([9; 32], &mut engine).unwrap();
            assert_eq!(
                sha256d::Hash::from_engine(engine),
                sha256d::Hash::from_engine(expected(u32::try_from(count).unwrap()))
            );
            assert_eq!(budget.snapshot().used, 0);
            assert_eq!(budget.spool_snapshot().used, 0);
            assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        }
    }
    #[test]
    fn spill_denial_and_late_duplicate_or_truncation_refund_resources() {
        let directory = tempfile::tempdir().unwrap();
        let denied = MemoryBudget::new(0);
        assert!(Group::new(8, Some(&denied)).is_err());
        let budget = MemoryBudget::new(256 * 1024);
        let pressure = budget
            .reserve_spool(crate::node_memory::DEFAULT_EXECUTION_SPOOL_BYTES)
            .unwrap();
        let mut group = Group::new(8, Some(&budget)).unwrap();
        for vout in 0..6 {
            group
                .push(vout, &coin(vout), directory.path(), Some(&budget))
                .unwrap();
        }
        assert!(
            group
                .push(6, &coin(6), directory.path(), Some(&budget))
                .is_err()
        );
        assert!(group.file.is_none());
        assert_eq!(group.length, 6 * (HEADER + MAX_SCRIPT) as u64);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        drop(pressure);
        for vout in 6..8 {
            group
                .push(vout, &coin(vout), directory.path(), Some(&budget))
                .unwrap();
        }
        group.file.as_mut().unwrap().set_len(1).unwrap();
        assert!(group.hash([9; 32], &mut sha256d::Hash::engine()).is_err());
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(budget.spool_snapshot().used, 0);
        let mut group = Group::new(8, Some(&budget)).unwrap();
        for _ in 0..8 {
            group
                .push(0, &coin(0), directory.path(), Some(&budget))
                .unwrap();
        }
        assert!(matches!(
            group.hash([9; 32], &mut sha256d::Hash::engine()),
            Err(CoreSnapshotError::Invalid("duplicate output index"))
        ));
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(budget.spool_snapshot().used, 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}
