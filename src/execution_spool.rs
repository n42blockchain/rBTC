//! Ephemeral execution results; durable raw blocks remain the recovery source.
use crate::{
    chain_store::{ConnectTransition, LeasedConnectTransition},
    execution_store::ExecutionTip,
    node_memory::{MemoryBudget, MemoryLease},
    utxo::{OutPointKey, Utxo, UtxoUndo},
};
use bitcoin::{BlockHash, hashes::Hash};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

/// Shared node allowance and directory for temporary execution results.
#[derive(Clone, Debug)]
pub struct ExecutionSpoolContext {
    directory: PathBuf,
    memory: MemoryBudget,
}
impl ExecutionSpoolContext {
    pub(crate) fn for_path(path: &Path) -> io::Result<Option<Self>> {
        Ok(crate::node_memory::for_path(path)?.map(|memory| Self {
            directory: path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf(),
            memory,
        }))
    }
    pub(crate) fn open(&self) -> io::Result<ExecutionSpool> {
        Ok(ExecutionSpool {
            // Anonymous/delete-on-close file: a crash discards preparation and
            // replay starts from the durable raw-block checkpoint.
            file: Mutex::new(tempfile::tempfile_in(&self.directory)?),
            memory: self.memory.clone(),
            charges: Mutex::new(Vec::new()),
        })
    }
}

pub(crate) struct ExecutionSpool {
    // Close/delete before returning disk charges.
    file: Mutex<File>,
    memory: MemoryBudget,
    charges: Mutex<Vec<crate::node_memory::SpoolLease>>,
}

pub(crate) struct Record {
    offset: u64,
    len: usize,
    memory: u64,
    digest: [u8; 32],
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid execution spool record")
}
fn add(total: &mut usize, value: usize) -> io::Result<()> {
    *total = total.checked_add(value).ok_or_else(invalid)?;
    Ok(())
}
fn coin_size(coin: &Utxo) -> io::Result<usize> {
    29usize
        .checked_add(coin.script_pubkey.len())
        .ok_or_else(invalid)
}
fn sizes(transition: &ConnectTransition) -> io::Result<(usize, u64)> {
    let mut wire = 68 + 24; // parent, next tip, three vector lengths
    let mut heap = size_of::<ConnectTransition>();
    add(
        &mut wire,
        transition.spent.len().checked_mul(36).ok_or_else(invalid)?,
    )?;
    add(
        &mut heap,
        transition
            .spent
            .len()
            .checked_mul(size_of::<OutPointKey>())
            .ok_or_else(invalid)?,
    )?;
    for (_, coin) in &transition.created {
        add(&mut wire, 36 + 8)?;
        add(&mut wire, coin_size(coin)?)?;
        add(&mut heap, size_of::<(OutPointKey, Utxo)>())?;
        add(&mut heap, coin.script_pubkey.len())?;
    }
    for undo in &transition.transaction_undos {
        add(&mut wire, 8 + 8)?; // encoded length, two u32 counts
        add(&mut heap, size_of::<UtxoUndo>())?;
        for (_, coin) in undo.spent() {
            add(&mut wire, 36 + 4)?;
            add(&mut wire, coin_size(coin)?)?;
            add(&mut heap, size_of::<(OutPointKey, Utxo)>())?;
            add(&mut heap, coin.script_pubkey.len())?;
        }
        add(
            &mut wire,
            undo.created().len().checked_mul(36).ok_or_else(invalid)?,
        )?;
        add(
            &mut heap,
            undo.created()
                .len()
                .checked_mul(size_of::<OutPointKey>())
                .ok_or_else(invalid)?,
        )?;
    }
    // Encoded input plus per-record codec scratch; decoded Vec capacities and
    // script buffers. No claim about pre-existing preparation allocations.
    let memory = wire
        .checked_add(heap)
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| n.checked_add(65536))
        .ok_or_else(invalid)?;
    Ok((wire, u64::try_from(memory).map_err(|_| invalid())?))
}
fn put_count(bytes: &mut Vec<u8>, count: usize) -> io::Result<()> {
    bytes.extend_from_slice(&u64::try_from(count).map_err(|_| invalid())?.to_le_bytes());
    Ok(())
}
fn put_blob(bytes: &mut Vec<u8>, blob: &[u8]) -> io::Result<()> {
    put_count(bytes, blob.len())?;
    bytes.extend_from_slice(blob);
    Ok(())
}
fn encode(transition: &ConnectTransition, len: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(len);
    bytes.extend_from_slice(transition.expected_parent.as_byte_array());
    bytes.extend_from_slice(&transition.next.height.to_le_bytes());
    bytes.extend_from_slice(transition.next.hash.as_byte_array());
    put_count(&mut bytes, transition.spent.len())?;
    for key in &transition.spent {
        bytes.extend_from_slice(key.as_bytes());
    }
    put_count(&mut bytes, transition.created.len())?;
    for (key, coin) in &transition.created {
        bytes.extend_from_slice(key.as_bytes());
        put_blob(&mut bytes, &coin.encode().map_err(io::Error::other)?)?;
    }
    put_count(&mut bytes, transition.transaction_undos.len())?;
    for undo in &transition.transaction_undos {
        put_blob(&mut bytes, &undo.encode().map_err(io::Error::other)?)?;
    }
    if bytes.len() != len {
        return Err(invalid());
    }
    Ok(bytes)
}
fn take<'a>(bytes: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
    if n > bytes.len() {
        return Err(invalid());
    }
    let (head, tail) = bytes.split_at(n);
    *bytes = tail;
    Ok(head)
}
fn count(bytes: &mut &[u8]) -> io::Result<usize> {
    let raw = u64::from_le_bytes(take(bytes, 8)?.try_into().map_err(|_| invalid())?);
    usize::try_from(raw).map_err(|_| invalid())
}
fn blob<'a>(bytes: &mut &'a [u8]) -> io::Result<&'a [u8]> {
    let n = count(bytes)?;
    take(bytes, n)
}
fn decode(mut bytes: &[u8]) -> io::Result<ConnectTransition> {
    let expected_parent = BlockHash::from_slice(take(&mut bytes, 32)?).map_err(io::Error::other)?;
    let height = u32::from_le_bytes(take(&mut bytes, 4)?.try_into().map_err(|_| invalid())?);
    let hash = BlockHash::from_slice(take(&mut bytes, 32)?).map_err(io::Error::other)?;
    let n = count(&mut bytes)?;
    if n > bytes.len() / 36 {
        return Err(invalid());
    }
    let mut spent = Vec::with_capacity(n);
    for _ in 0..n {
        spent.push(OutPointKey::from_bytes(take(&mut bytes, 36)?).map_err(io::Error::other)?);
    }
    let n = count(&mut bytes)?;
    if n > bytes.len() / 73 {
        return Err(invalid());
    }
    let mut created = Vec::with_capacity(n);
    for _ in 0..n {
        let key = OutPointKey::from_bytes(take(&mut bytes, 36)?).map_err(io::Error::other)?;
        let coin = Utxo::decode(blob(&mut bytes)?).map_err(io::Error::other)?;
        created.push((key, coin));
    }
    let n = count(&mut bytes)?;
    if n > bytes.len() / 16 {
        return Err(invalid());
    }
    let mut transaction_undos = Vec::with_capacity(n);
    for _ in 0..n {
        transaction_undos.push(UtxoUndo::decode(blob(&mut bytes)?).map_err(io::Error::other)?);
    }
    if !bytes.is_empty() {
        return Err(invalid());
    }
    Ok(ConnectTransition {
        expected_parent,
        next: ExecutionTip { height, hash },
        spent,
        created,
        transaction_undos,
    })
}
impl ExecutionSpool {
    pub(crate) fn write(&self, transition: &ConnectTransition) -> io::Result<Record> {
        let (len, memory) = sizes(transition)?;
        let disk = self.memory.reserve_spool(len as u64)?;
        let _encoding: MemoryLease = self.memory.reserve(memory)?;
        let bytes = encode(transition, len)?;
        let digest = Sha256::digest(&bytes).into();
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let offset = file.seek(SeekFrom::End(0))?;
        // Keep the charge even after a partial write error. The whole spool is
        // discarded by the failing checkpoint; no uncharged residual growth.
        self.charges
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(disk);
        file.write_all(&bytes)?;
        Ok(Record {
            offset,
            len,
            memory,
            digest,
        })
    }
    pub(crate) fn read(&self, record: &Record) -> io::Result<LeasedConnectTransition> {
        let lease = self.memory.reserve(record.memory)?;
        let mut bytes = vec![0; record.len];
        {
            let mut file = self
                .file
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            file.seek(SeekFrom::Start(record.offset))?;
            file.read_exact(&mut bytes)?;
        }
        // Authenticate before trusting any stored counts/lengths. Descriptors
        // are in-process values and cannot be reconstructed from untrusted disk.
        if <[u8; 32]>::from(Sha256::digest(&bytes)) != record.digest {
            return Err(invalid());
        }
        let transition = decode(&bytes)?;
        Ok(LeasedConnectTransition::with_reservation(transition, lease))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utxo::Utxo;
    fn fixture() -> ConnectTransition {
        let key = OutPointKey::from_bytes(&[7; 36]).unwrap();
        let coin = Utxo {
            value_sats: 42,
            height: 9,
            is_coinbase: true,
            last_touched: 123,
            creation_mtp: 456,
            script_pubkey: vec![0x51; 1000],
        };
        ConnectTransition {
            expected_parent: BlockHash::from_byte_array([1; 32]),
            next: ExecutionTip {
                height: 1,
                hash: BlockHash::from_byte_array([2; 32]),
            },
            spent: vec![key],
            created: vec![(key, coin.clone())],
            transaction_undos: vec![UtxoUndo::from_parts(vec![(key, coin)], vec![key])],
        }
    }
    #[test]
    fn execution_spool_roundtrip_corruption_and_owner_lifetime() {
        let dir = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(8 * 1024 * 1024);
        let context = ExecutionSpoolContext {
            directory: dir.path().into(),
            memory: budget.clone(),
        };
        let spool = context.open().unwrap();
        let original = fixture();
        let record = spool.write(&original).unwrap();
        assert_eq!(budget.snapshot().used, 0);
        assert_eq!(budget.spool_snapshot().used, record.len as u64);
        let decoded = spool.read(&record).unwrap();
        assert_eq!(decoded.expected_parent, original.expected_parent);
        assert_eq!(decoded.next, original.next);
        assert_eq!(decoded.spent, original.spent);
        assert_eq!(decoded.created, original.created);
        assert_eq!(decoded.transaction_undos, original.transaction_undos);
        assert_eq!(budget.snapshot().used, record.memory);
        {
            let mut file = spool.file.lock().unwrap();
            file.seek(SeekFrom::Start(record.offset + 68)).unwrap();
            // Corrupt a vector count to a huge value. Hash verification must
            // reject before allocating from that count.
            file.write_all(&u64::MAX.to_le_bytes()).unwrap();
        }
        assert!(spool.read(&record).is_err());
        assert_eq!(budget.snapshot().used, record.memory);
        drop(spool);
        assert_eq!(budget.spool_snapshot().used, 0);
        assert_eq!(budget.snapshot().used, record.memory);
        drop(decoded);
        assert_eq!(budget.snapshot().used, 0);
    }
    #[test]
    fn execution_spool_shares_disk_budget_and_rejects_before_growth() {
        let dir = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(8 * 1024 * 1024);
        let context = ExecutionSpoolContext {
            directory: dir.path().into(),
            memory: budget.clone(),
        };
        let first = context.open().unwrap();
        let second = context.open().unwrap();
        let record = first.write(&fixture()).unwrap();
        let occupied = budget
            .reserve_spool(crate::node_memory::DEFAULT_EXECUTION_SPOOL_BYTES - record.len as u64)
            .unwrap();
        assert!(second.write(&fixture()).is_err());
        assert_eq!(second.file.lock().unwrap().metadata().unwrap().len(), 0);
        drop(occupied);
        second.write(&fixture()).unwrap();
        let occupied = budget.reserve(budget.snapshot().limit).unwrap();
        assert!(first.read(&record).is_err());
        let before = first.file.lock().unwrap().metadata().unwrap().len();
        assert!(first.write(&fixture()).is_err());
        assert_eq!(first.file.lock().unwrap().metadata().unwrap().len(), before);
        drop(occupied);
        drop(first);
        drop(second);
        assert_eq!(budget.spool_snapshot().used, 0);
        assert_eq!(budget.snapshot().used, 0);
    }
    #[test]
    fn execution_spool_consumes_results_larger_than_memory_allowance() {
        let directory = tempfile::tempdir().unwrap();
        let budget = MemoryBudget::new(2 * 1024 * 1024);
        let context = ExecutionSpoolContext {
            directory: directory.path().into(),
            memory: budget.clone(),
        };
        let spool = context.open().unwrap();
        let mut transition = fixture();
        transition.created[0].1.script_pubkey = vec![0x51; 64 * 1024];
        transition.transaction_undos = vec![UtxoUndo::from_parts(
            transition.created.clone(),
            transition.spent.clone(),
        )];
        let records = (0..64)
            .map(|_| spool.write(&transition).unwrap())
            .collect::<Vec<_>>();
        assert!(budget.spool_snapshot().used > budget.snapshot().limit);
        assert_eq!(budget.snapshot().used, 0);
        let held = spool.read(&records[0]).unwrap();
        assert!(
            spool.read(&records[1]).is_err(),
            "returned payload must remain charged"
        );
        drop(held);
        for record in &records {
            let decoded = spool.read(record).unwrap();
            assert_eq!(decoded.created, transition.created);
            assert_eq!(decoded.transaction_undos, transition.transaction_undos);
        }
        assert_eq!(budget.snapshot().used, 0);
        assert!(budget.snapshot().peak <= budget.snapshot().limit);
        drop(spool);
        assert_eq!(budget.spool_snapshot().used, 0);
    }

    #[test]
    fn execution_spool_process_death_removes_temporary_results() {
        const CHILD_DIR: &str = "RBTC_EXECUTION_SPOOL_CRASH_TEST";
        if let Some(path) = std::env::var_os(CHILD_DIR) {
            let context = ExecutionSpoolContext {
                directory: PathBuf::from(&path),
                memory: MemoryBudget::new(8 * 1024 * 1024),
            };
            let spool = context.open().unwrap();
            let _record = spool.write(&fixture()).unwrap();
            std::fs::write(Path::new(&path).join("ready"), b"written").unwrap();
            loop {
                std::thread::park();
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "execution_spool::tests::execution_spool_process_death_removes_temporary_results",
                "--nocapture",
            ])
            .env(CHILD_DIR, directory.path())
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !directory.path().join("ready").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "spool child exited before ready"
            );
            if std::time::Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("spool child did not become ready");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let files = std::fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(files, vec![std::ffi::OsString::from("ready")]);
    }
}
