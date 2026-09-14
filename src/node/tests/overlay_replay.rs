//! Small, deterministic replay through the production overlay driver.

use super::*;
use crate::snapshot_overlay::tests::write_base_snapshot;

async fn replay<C: OverlayCatchupStore>(
    store: C,
    options: &Options,
    directory: &std::path::Path,
    headers: &HeaderDag,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = listener.local_addr().unwrap();
    let server = tokio::spawn(accept_peer(listener, peer_version(9_001)));
    let mut session = connect_outbound(
        remote,
        Network::Regtest.magic(),
        9_002,
        "/rbtc:replay-test/".to_owned(),
        0,
    )
    .await
    .unwrap();
    let (peer, _) = server.await.unwrap();
    timeout(
        Duration::from_secs(30),
        run_overlay_catchup(&mut session, options, directory, headers, store),
    )
    .await
    .expect("bounded replay finishes")
    .unwrap();
    drop(peer);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlay_replay_recovers_staging_flushes_and_stops_at_corpus_end() {
    exercise_replay(false).await;
    exercise_replay(true).await;
}

#[allow(clippy::too_many_lines)]
async fn exercise_replay(maintenance: bool) {
    let directory = TempDir::new().unwrap();
    let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
    let coin = Utxo {
        value_sats: 5_000,
        height: 0,
        is_coinbase: false,
        last_touched: 0,
        creation_mtp: 0,
        script_pubkey: vec![0x51],
    };
    let (snapshot, index, identity) =
        write_base_snapshot(directory.path(), 0, genesis.block_hash(), &[(10, 0, coin)]);
    let mut headers = HeaderDag::new(Network::Regtest);
    let mut blocks = Vec::new();
    let mut previous_output = OutPoint::new(Txid::from_byte_array([10; 32]), 0);
    let mut final_key = OutPointKey::from(previous_output);
    for height in 1..=7 {
        let transaction = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::TxIn {
                previous_output,
                script_sig: ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(5_000 - u64::from(height)),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        previous_output = OutPoint::new(transaction.compute_txid(), 0);
        let mut template = crate::block_assembly::BlockTemplate::regtest(
            headers.active_tip().hash,
            height,
            genesis.header.time + height,
        );
        template.transactions.push(transaction);
        template.fee_sats = 1;
        let block = crate::block_assembly::assemble_block(&template).unwrap();
        headers.insert(block.header).unwrap();
        if height <= 6 {
            final_key = OutPointKey::from(previous_output);
            blocks.push(serialize(&block));
        }
    }
    let corpus_path = directory.path().join("corpus");
    let corpus = PrunedBlockLedger::open(&corpus_path, LedgerRetention::default()).unwrap();
    corpus.append(1, &blocks).unwrap();
    let corpus_before = corpus.verify_block_hashes(1, 6, 1 << 20).unwrap().hashes;
    let mut digests = Vec::new();
    for engine in [SnapshotOverlayEngine::Mdbx, SnapshotOverlayEngine::Redb] {
        for buffered in [false, true] {
            let data = directory.path().join(format!("{engine:?}-{buffered}"));
            fs::create_dir(&data).unwrap();
            let retained =
                PrunedBlockLedger::open(data.join("blocks"), LedgerRetention::default()).unwrap();
            // Simulate a crash with both published and staged blocks ahead of the durable tip.
            retained.append(1, &blocks[..2]).unwrap();
            retained.stage(3, &blocks[2..4]).unwrap();
            let mut options = peer_retry_test_options(true, Arc::new(RuntimeControl::default()));
            options.validation_limits.max_blocks_per_batch = 2;
            options.snapshot_overlay = Some(NodeSnapshotOverlayConfig {
                // Derived rebase outputs belong to this lane, while the initial base is shared.
                snapshot: data.join("utxo-0.dat"),
                index: index.clone(),
                capacity_bytes: 64 << 20,
                // Zero thresholds deterministically exercise maintenance on this tiny fixture.
                compact_percent: if maintenance { 0 } else { 100 },
                rebase_percent: if maintenance { 0 } else { 100 },
                engine,
                replay_blocks: Some(corpus_path.clone()),
                flush_batches: if buffered { 3 } else { 1 },
                flush_coins: 100,
            });
            let database = data.join("overlay");
            let config = || SnapshotOverlayConfig {
                database_dir: database.clone(),
                snapshot_path: snapshot.clone(),
                index_path: index.clone(),
                capacity_bytes: 64 << 20,
                import_time: 0,
                mtp_by_height: vec![0],
            };
            let reopened_config = || {
                let stored = match engine {
                    SnapshotOverlayEngine::Mdbx => {
                        SnapshotOverlayChainstate::stored_identity(&database, 64 << 20).unwrap()
                    }
                    SnapshotOverlayEngine::Redb => {
                        SnapshotOverlayRedbChainstate::stored_identity(&database).unwrap()
                    }
                }
                .unwrap();
                if maintenance {
                    assert!(stored.height > 0 && stored.height < 6);
                } else {
                    assert_eq!(stored, identity);
                }
                let mut reopened = config();
                if stored.height > 0 {
                    (reopened.snapshot_path, reopened.index_path) =
                        overlay_base_paths_for_height(&data.join("utxo-0.dat"), stored.height);
                    reopened.mtp_by_height =
                        creation_mtp_range(&headers, 0, stored.height).unwrap();
                }
                (reopened, stored)
            };
            let limits = WriteBackLimits {
                max_batches: 3,
                max_created: 100,
            };
            match engine {
                SnapshotOverlayEngine::Mdbx => {
                    let store = SnapshotOverlayChainstate::open(config(), Some(&identity)).unwrap();
                    if buffered {
                        replay(
                            WriteBackChainstate::new(store, limits),
                            &options,
                            &data,
                            &headers,
                        )
                        .await;
                    } else {
                        replay(store, &options, &data, &headers).await;
                    }
                    let (reopened, stored) = reopened_config();
                    let store = SnapshotOverlayChainstate::open(reopened, Some(&stored)).unwrap();
                    assert_eq!(store.execution_tip().unwrap().height, 6);
                    assert_eq!(store.get(final_key).unwrap().unwrap().value_sats, 4_994);
                    replay(store, &options, &data, &headers).await;
                    digests.push(
                        SnapshotOverlayChainstate::audit_content(&database, 64 << 20)
                            .unwrap()
                            .content_sha256,
                    );
                }
                SnapshotOverlayEngine::Redb => {
                    let store =
                        SnapshotOverlayRedbChainstate::open(config(), Some(&identity)).unwrap();
                    if buffered {
                        replay(
                            WriteBackChainstate::new(store, limits),
                            &options,
                            &data,
                            &headers,
                        )
                        .await;
                    } else {
                        replay(store, &options, &data, &headers).await;
                    }
                    let (reopened, stored) = reopened_config();
                    let store =
                        SnapshotOverlayRedbChainstate::open(reopened, Some(&stored)).unwrap();
                    assert_eq!(store.execution_tip().unwrap().height, 6);
                    assert_eq!(store.get(final_key).unwrap().unwrap().value_sats, 4_994);
                    replay(store, &options, &data, &headers).await;
                    digests.push(
                        SnapshotOverlayRedbChainstate::audit_content(&database)
                            .unwrap()
                            .content_sha256,
                    );
                }
            }
            assert!(retained.staged().unwrap().is_none());
            assert_eq!(
                retained.verify_block_hashes(1, 6, 1 << 20).unwrap().hashes,
                corpus_before
            );
        }
    }
    assert!(digests.windows(2).all(|pair| pair[0] == pair[1]));
    assert_eq!(
        corpus.verify_block_hashes(1, 6, 1 << 20).unwrap().hashes,
        corpus_before
    );
}
