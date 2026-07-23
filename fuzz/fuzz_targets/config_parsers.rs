#![no_main]

use bitcoin::Network;
use libfuzzer_sys::fuzz_target;
use rbtc::{deployments::DeploymentConfig, ibd::IbdPolicy};

fn exercise(network: Network, value: &str) {
    let mut deployments = DeploymentConfig::for_network(network);
    let before = deployments.clone();
    if deployments.apply_vbparams(value).is_err() {
        assert_eq!(deployments, before);
    }

    let mut deployments = DeploymentConfig::for_network(network);
    let before = deployments.clone();
    if deployments.apply_test_activation_height(value).is_err() {
        assert_eq!(deployments, before);
    }

    let mut policy = IbdPolicy::for_network(network);
    let before = policy;
    if policy.set_minimum_chainwork(value).is_err() {
        assert_eq!(policy, before);
    }

    let mut policy = IbdPolicy::for_network(network);
    let before = policy;
    if policy.set_assume_valid(value).is_err() {
        assert_eq!(policy, before);
    }
}

fuzz_target!(|input: &[u8]| {
    if input.len() > 4096 {
        return;
    }
    let Some((&selector, bytes)) = input.split_first() else {
        return;
    };
    let Ok(value) = std::str::from_utf8(bytes) else {
        return;
    };
    let network = match selector % 5 {
        0 => Network::Bitcoin,
        1 => Network::Testnet,
        2 => Network::Signet,
        3 => Network::Regtest,
        _ => Network::Testnet4,
    };
    exercise(network, value);
    if let Some(value) = value.strip_suffix('\n') {
        exercise(network, value);
    }
});
