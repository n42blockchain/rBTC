use super::*;

fn execute(
    store: &RedbChainStore,
    headers: &HeaderDag,
    block: &Block,
    height: u32,
) -> AppliedBlock {
    let deployments = block_deployment_context_for_headers(
        &DeploymentConfig::for_network(Network::Regtest),
        headers,
        height,
        block.block_hash(),
    )
    .unwrap();
    crate::block_execution::connect_active_block(
        store,
        headers,
        block,
        u64::from(block.header.time),
        DEFAULT_HOT_WINDOW_SECS,
        &deployments,
    )
    .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn indexes_rewind_stale_forks_and_backfill_from_local_or_peer_blocks() {
    for local in [true, false] {
        let directory = TempDir::new().unwrap();
        let genesis = bitcoin::blockdata::constants::genesis_block(Network::Regtest);
        let mut headers = HeaderDag::new(Network::Regtest);
        let chainstate =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        let explorer =
            RedbExplorerIndex::open(directory.path().join("explorer.redb"), Network::Regtest)
                .unwrap();
        let indexes = AuxiliaryIndexes::open(
            directory.path(),
            Network::Regtest,
            NodeIndexConfig {
                transaction: true,
                spent_output: true,
                basic_filter: true,
            },
        )
        .unwrap();
        let first = regtest_block_at_height(genesis.block_hash(), genesis.header.time + 1, 1);
        let stale = regtest_block_at_height(first.block_hash(), genesis.header.time + 2, 2);
        for (height, block) in [(1, &first), (2, &stale)] {
            headers.insert(block.header).unwrap();
            let applied = execute(&chainstate, &headers, block, height);
            explorer.connect(height, block, &applied).unwrap();
            for (_, index) in indexes.enabled() {
                index
                    .connect_block(height, block, &applied.transaction_undos)
                    .unwrap();
            }
        }
        disconnect_execution_tip(
            &chainstate,
            &headers,
            u64::from(genesis.header.time + 3),
            DEFAULT_HOT_WINDOW_SECS,
        )
        .unwrap();
        let replacement = regtest_block_at_height(first.block_hash(), genesis.header.time + 10, 2);
        let third = regtest_block_at_height(replacement.block_hash(), genesis.header.time + 11, 3);
        headers.insert(replacement.header).unwrap();
        headers.insert(third.header).unwrap();
        execute(&chainstate, &headers, &replacement, 2);
        execute(&chainstate, &headers, &third, 3);
        let ledger =
            PrunedBlockLedger::open(directory.path().join("blocks"), LedgerRetention::default())
                .unwrap();
        if local {
            ledger
                .append(
                    1,
                    &[
                        serialize(&first),
                        serialize(&replacement),
                        serialize(&third),
                    ],
                )
                .unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote = listener.local_addr().unwrap();
        let backfill = vec![replacement.clone(), third.clone()];
        let (done, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut peer, _) = accept_peer(listener, peer_version(9_011)).await;
            if !local {
                // Explorer and each of the three independent optional indexes must fetch history.
                for _ in 0..4 {
                    let NetworkMessage::GetData(request) =
                        peer.read_message().await.unwrap().into_payload()
                    else {
                        panic!("expected backfill request");
                    };
                    assert_eq!(request.len(), 2);
                    for block in &backfill {
                        peer.write_message(NetworkMessage::Block(block.clone()))
                            .await
                            .unwrap();
                    }
                }
            }
            finished.await.unwrap();
        });
        let mut session = connect_outbound(
            remote,
            Network::Regtest.magic(),
            9_012,
            "/rbtc:index-test/".to_owned(),
            0,
        )
        .await
        .unwrap();
        let deployments = DeploymentConfig::for_network(Network::Regtest);
        timeout(Duration::from_secs(20), async {
            reconcile_explorer(
                &mut session,
                &deployments,
                &headers,
                &chainstate,
                &ledger,
                &explorer,
                None,
                &[],
            )
            .await
            .unwrap();
            reconcile_auxiliary_indexes(
                &mut session,
                &deployments,
                &headers,
                &chainstate,
                &ledger,
                &indexes,
                &[],
            )
            .await
            .unwrap();
        })
        .await
        .unwrap();
        let tip = chainstate.execution_tip().unwrap();
        assert_eq!(explorer.tip().unwrap(), tip);
        for (_, index) in indexes.enabled() {
            assert_eq!(index.tip().unwrap(), tip);
        }
        let transaction_index = indexes.transaction.as_ref().unwrap();
        // Coinbase txids are height-based, so assert the containing block changed with the fork.
        assert_eq!(
            transaction_index
                .transaction(replacement.txdata[0].compute_txid())
                .unwrap()
                .unwrap()
                .block_hash,
            replacement.block_hash()
        );
        let filter = indexes.basic_filter.as_ref().unwrap();
        assert!(
            filter
                .basic_filter_by_hash(stale.block_hash())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            filter.basic_filter(3).unwrap(),
            filter.basic_filter_by_hash(third.block_hash()).unwrap()
        );
        assert!(filter.basic_filter(3).unwrap().is_some());
        // Reconciliation is idempotent and makes no further peer requests.
        reconcile_explorer(
            &mut session,
            &deployments,
            &headers,
            &chainstate,
            &ledger,
            &explorer,
            None,
            &[],
        )
        .await
        .unwrap();
        reconcile_auxiliary_indexes(
            &mut session,
            &deployments,
            &headers,
            &chainstate,
            &ledger,
            &indexes,
            &[],
        )
        .await
        .unwrap();
        done.send(()).unwrap();
        server.await.unwrap();
        drop(explorer);
        drop(indexes);
        drop(chainstate);
        let mut options = peer_retry_test_options(true, Arc::new(RuntimeControl::default()));
        options.data_dir = Some(directory.path().to_owned());
        report_utxo_activity(&options).unwrap();
        retier_utxos(&options, 0).unwrap();
        let reopened =
            RedbChainStore::open(directory.path().join("chainstate.redb"), Network::Regtest)
                .unwrap();
        assert_eq!(reopened.execution_tip().unwrap(), tip);
        let population = scan_utxo_population(&reopened, 3).unwrap();
        assert_eq!(population.total_count, 3);
        assert!(scan_utxo_population(&reopened, 2).is_err());
    }
}
