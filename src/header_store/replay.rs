//! Fixed-size raw replay slices pinned to one authoritative database version.
use super::{HEADERS, HeaderStoreError, INSERTION_ORDER, RedbHeaderStore};
use crate::headers::HeaderWorkBudget;
use bitcoin::{block::Header, consensus::deserialize, hashes::Hash};
use redb::{ReadTransaction, ReadableTableMetadata};

const BATCH: usize = 2_000;

/// A bounded raw-header replay cursor. The read transaction pins one database
/// version so concurrent writes cannot change the measured rows during replay.
/// Raw records are not validated consensus until the caller accepts the batch.
pub struct HeaderReplayReader {
    transaction: ReadTransaction,
    next_sequence: u64,
    remaining: u64,
}

impl RedbHeaderStore {
    /// Opens a parent-first replay cursor without constructing a retained DAG.
    pub fn replay_reader(&self) -> Result<HeaderReplayReader, HeaderStoreError> {
        let transaction = self.db.begin_read()?;
        let remaining = transaction.open_table(HEADERS)?.len()?;
        if transaction.open_table(INSERTION_ORDER)?.len()? != remaining {
            return Err(HeaderStoreError::Malformed(
                "header and insertion counts differ",
            ));
        }
        Ok(HeaderReplayReader {
            transaction,
            next_sequence: 0,
            remaining,
        })
    }
}

impl HeaderReplayReader {
    /// Rows not yet delivered from the pinned read version.
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Reads at most 2,000 fixed-size raw headers. Exhaustion or malformed data
    /// leaves the cursor unchanged; earlier work charges are not refunded.
    pub fn next_batch(
        &mut self,
        work: &mut HeaderWorkBudget,
    ) -> Result<Option<Vec<Header>>, HeaderStoreError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let count = usize::try_from(self.remaining.min(BATCH as u64)).expect("bounded count");
        work.consume(4 * count as u64)?;
        let headers = self.transaction.open_table(HEADERS)?;
        let order = self.transaction.open_table(INSERTION_ORDER)?;
        let mut batch = Vec::with_capacity(count);
        let mut next = self.next_sequence;
        for row in order.range(self.next_sequence..)?.take(count) {
            let (sequence, hash) = row?;
            let value = headers
                .get(hash.value())?
                .ok_or(HeaderStoreError::Malformed("ordered header missing"))?;
            if value.value().len() != 80 {
                return Err(HeaderStoreError::Malformed("header encoding length"));
            }
            let header: Header = deserialize(value.value())?;
            if header.block_hash().as_byte_array() != hash.value() {
                return Err(HeaderStoreError::Malformed("header hash mismatch"));
            }
            next = sequence
                .value()
                .checked_add(1)
                .ok_or(HeaderStoreError::Malformed("header sequence overflow"))?;
            batch.push(header);
        }
        if batch.len() != count {
            return Err(HeaderStoreError::Malformed("incomplete header order"));
        }
        #[cfg(test)]
        super::REPLAYED_HEADERS.with(|counter| counter.set(counter.get() + batch.len()));
        self.next_sequence = next;
        self.remaining -= count as u64;
        Ok(Some(batch))
    }
}
