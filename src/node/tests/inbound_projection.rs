use super::*;

#[test]
#[allow(clippy::too_many_lines)]
fn inbound_projection_hides_unexecuted_headers_and_rejects_mismatched_ledger_records() {
    let directory = TempDir::new().unwrap();
    let (mut source, funding, coin) = dry_run_test_source(directory.path());
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let first = regtest_block_at_height(genesis.block_hash(), genesis.header.time + 600, 1);
    let next = regtest_block_at_height(first.block_hash(), first.header.time + 600, 2);
    source.ledger.append(1, &[serialize(&first)]).unwrap();
    assert!(source.basic_filter(1).unwrap().is_none());
    let filter = Arc::new(
        RedbAuxiliaryIndex::open(
            directory.path().join("filter.redb"),
            Network::Regtest,
            AuxiliaryIndexKind::BasicFilter,
        )
        .unwrap(),
    );
    filter.connect_block(1, &first, &[]).unwrap();
    source.basic_filter = Some(filter);
    let transaction = Transaction {
        version: TransactionVersion::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: funding,
            script_sig: ScriptBuf::from_bytes(vec![1, 0x51]),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(coin.value_sats - 10_000),
            script_pubkey: ScriptBuf::from_bytes([vec![0, 32], vec![17; 32]].concat()),
        }],
    };
    let context = transaction_admission_context(
        &source.chainstate,
        &source.headers.read().unwrap(),
        &source.deployments,
        true,
    )
    .unwrap();
    source
        .transaction_pool
        .lock()
        .unwrap()
        .admit_package(
            source.chainstate.as_ref(),
            vec![transaction.clone()],
            context,
        )
        .unwrap();
    source.headers.write().unwrap().insert(next.header).unwrap();
    let source = Arc::new(source);
    let advertised = "127.0.0.1:18444".parse().unwrap();
    let shared = Arc::new(SharedInboundSource::new(
        1 << 20,
        Some(advertised),
        ServiceFlags::WITNESS,
    ));
    assert!(shared.start_height().is_err());
    assert_eq!(
        shared.advertised_address(),
        Some((advertised, ServiceFlags::WITNESS))
    );
    assert!(shared.addresses().unwrap().is_empty());
    let peers = Arc::new(
        RedbPeerStore::open(directory.path().join("peers.redb"), Network::Regtest).unwrap(),
    );
    shared.install_peer_store(Some(peers));
    assert!(shared.addresses().unwrap().is_empty());
    assert!(Arc::ptr_eq(&shared.stats(), &shared.stats));
    shared.install_advertised_onion(None);
    shared.install_advertised_i2p(None);
    assert!(shared.advertised_onion().is_none());
    assert!(shared.advertised_i2p().is_none());
    let lease = shared.install(source.clone());
    assert_eq!(shared.start_height().unwrap(), 1);
    assert_eq!(shared.active_header(1).unwrap(), Some(first.header));
    assert!(shared.active_header(2).unwrap().is_none());
    assert_eq!(shared.active_height(first.block_hash()).unwrap(), Some(1));
    assert!(shared.active_height(next.block_hash()).unwrap().is_none());
    assert_eq!(
        shared.block(first.block_hash()).unwrap(),
        Some(serialize(&first))
    );
    assert!(shared.block(next.block_hash()).unwrap().is_none());
    assert_eq!(shared.mempool().unwrap(), vec![transaction.clone()]);
    for inventory in [
        Inventory::Transaction(transaction.compute_txid()),
        Inventory::WitnessTransaction(transaction.compute_txid()),
        Inventory::WTx(transaction.compute_wtxid()),
    ] {
        assert_eq!(
            shared.transaction(inventory).unwrap(),
            Some(transaction.clone())
        );
    }
    assert!(
        shared
            .transaction(Inventory::Block(first.block_hash()))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        shared.utxo(OutPointKey::from(funding)).unwrap(),
        Some(coin.clone())
    );
    assert_eq!(
        shared.chainstate_page(None, 1).unwrap(),
        vec![(OutPointKey::from(funding), coin)]
    );
    assert_eq!(
        shared.fee_filter_sat_kvb().unwrap(),
        source.fee_filter_sat_kvb().unwrap()
    );
    assert_eq!(
        shared.basic_filter(1).unwrap().unwrap().block_hash,
        first.block_hash()
    );
    assert!(shared.basic_filter(2).unwrap().is_none());
    assert!(shared.submit_transaction(transaction.clone()).unwrap());
    assert!(!shared.submit_transaction(transaction.clone()).unwrap());
    assert_eq!(
        source.pending_transactions.lock().unwrap().drain(),
        vec![transaction]
    );
    assert!(
        shared
            .test_accept(Vec::new())
            .unwrap_err()
            .contains("active header tip")
    );
    assert!(matches!(
        shared.submit_block(next).unwrap(),
        BlockSubmission::Queued(_)
    ));
    assert_eq!(source.pending_blocks.lock().unwrap().drain().len(), 1);
    source.ledger.truncate_from(1).unwrap();
    let wrong = regtest_block_at_height(genesis.block_hash(), genesis.header.time + 601, 1);
    source.ledger.append(1, &[serialize(&wrong)]).unwrap();
    assert!(
        shared
            .block(first.block_hash())
            .unwrap_err()
            .contains("expected")
    );
    source.ledger.truncate_from(1).unwrap();
    assert!(shared.block(first.block_hash()).unwrap().is_none());
    // Dropping an old session's lease cannot clear a replacement session's projection.
    let other = TempDir::new().unwrap();
    let (replacement, _, _) = dry_run_test_source(other.path());
    let replacement_lease = shared.install(Arc::new(replacement));
    drop(lease);
    assert_eq!(shared.start_height().unwrap(), 1);
    drop(replacement_lease);
    assert!(shared.start_height().is_err());
    assert!(shared.mempool().is_err());
}
