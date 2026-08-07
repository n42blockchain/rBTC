//! Strict, bounded, network-scoped operator configuration adapter.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
};

use bitcoin::Network;

const MAX_CONFIG_FILE_BYTES: u64 = 64 * 1024;
const LIST_KEYS: [&str; 7] = [
    "connect",
    "dns_seed",
    "signetseednode",
    "testactivationheight",
    "vbparams",
    "whitelist",
    "onlynet",
];
const BOOL_KEYS: [&str; 10] = [
    "block_filter_index",
    "cleanup_validation_dir",
    "dns_seeds",
    "mempool_full_rbf",
    "listen",
    "once",
    "spent_output_index",
    "txindex",
    "v2_transport",
    "validation_deferred_repair",
];
const VALUE_KEYS: [&str; 33] = [
    "automatic_hot_standbys",
    "assumevalid",
    "background_assumeutxo",
    "background_chainstate_cache_bytes",
    "bulk_validation_cache_bytes",
    "chainstate_cache_bytes",
    "complete_assumeutxo",
    "data_dir",
    "explorer_listen",
    "external_address",
    "inbound_listen",
    "inbound_requests_per_minute",
    "log_dir",
    "log_level",
    "log_max_bytes",
    "log_max_files",
    "minimum_chainwork",
    "mempool_max_bytes",
    "mempool_max_transactions",
    "max_inbound_peers",
    "max_inbound_peers_per_ip",
    "max_upload_bytes_per_day",
    "minimum_free_bytes",
    "network",
    "prune_blocks",
    "prune_max_bytes",
    "proxy",
    "rpc_auth_token_file",
    "signetchallenge",
    "validation_batch_size",
    "validation_pause_ms",
    "wallet_auth_token_file",
    "wallet_descriptors",
];

#[derive(Clone, Debug)]
struct Entry {
    section: Option<Network>,
    key: String,
    value: String,
    line: usize,
}

#[derive(Clone, Debug)]
struct ConfigArgument {
    flag: &'static str,
    value: Option<String>,
}

pub(super) fn merge_config_arguments(arguments: Vec<String>) -> Result<Vec<String>, String> {
    let (config_path, cli_arguments) = extract_config_path(arguments)?;
    let Some(config_path) = config_path else {
        return Ok(cli_arguments);
    };
    let entries = parse_config_file(&config_path)?;
    let cli_network = option_value(&cli_arguments, "--network")
        .map(|value| parse_network(value, "command line"))
        .transpose()?;
    let file_network = global_network(&entries)?;
    let selected_network = cli_network.or(file_network).unwrap_or(Network::Bitcoin);
    let config_arguments = selected_arguments(&entries, selected_network)?;
    let cli_groups = cli_arguments
        .iter()
        .filter_map(|argument| known_flag_group(argument))
        .collect::<BTreeSet<_>>();
    let mut merged = Vec::new();
    for argument in config_arguments {
        if cli_groups.contains(option_group(argument.flag)) {
            continue;
        }
        merged.push(argument.flag.to_owned());
        if let Some(value) = argument.value {
            merged.push(value);
        }
    }
    merged.extend(cli_arguments);
    Ok(merged)
}

fn extract_config_path(arguments: Vec<String>) -> Result<(Option<PathBuf>, Vec<String>), String> {
    let mut config_path = None;
    let mut retained = Vec::with_capacity(arguments.len());
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        if argument == "--config" {
            if config_path.is_some() {
                return Err("--config cannot be supplied more than once".to_owned());
            }
            let value = arguments
                .next()
                .ok_or_else(|| "missing value for --config".to_owned())?;
            if value.starts_with("--") {
                return Err("missing value for --config".to_owned());
            }
            config_path = Some(PathBuf::from(value));
        } else {
            retained.push(argument);
        }
    }
    Ok((config_path, retained))
}

fn parse_config_file(path: &Path) -> Result<Vec<Entry>, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("read config metadata {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "config file {} must be a regular file, not a symlink",
            path.display()
        ));
    }
    if metadata.len() > MAX_CONFIG_FILE_BYTES {
        return Err(format!(
            "config file {} exceeds {MAX_CONFIG_FILE_BYTES} bytes",
            path.display()
        ));
    }
    let mut contents = String::new();
    fs::File::open(path)
        .map_err(|error| format!("open config file {}: {error}", path.display()))?
        .take(MAX_CONFIG_FILE_BYTES.saturating_add(1))
        .read_to_string(&mut contents)
        .map_err(|error| format!("read config file {}: {error}", path.display()))?;
    if u64::try_from(contents.len()).unwrap_or(u64::MAX) > MAX_CONFIG_FILE_BYTES {
        return Err(format!(
            "config file {} exceeds {MAX_CONFIG_FILE_BYTES} bytes",
            path.display()
        ));
    }
    parse_config(&contents)
}

fn parse_config(contents: &str) -> Result<Vec<Entry>, String> {
    let mut section = None;
    let mut entries = Vec::new();
    for (index, raw_line) in contents.lines().enumerate() {
        let line_number = index + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') || line.ends_with(']') {
            if !(line.starts_with('[') && line.ends_with(']')) {
                return Err(format!("invalid config section at line {line_number}"));
            }
            let name = &line[1..line.len() - 1];
            section = Some(parse_network(name, &format!("config line {line_number}"))?);
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("config line {line_number} must use key=value"))?;
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() || value.is_empty() {
            return Err(format!(
                "config line {line_number} requires a non-empty key and value"
            ));
        }
        if !known_key(key) {
            return Err(format!("unknown config key '{key}' at line {line_number}"));
        }
        if key == "network" && section.is_some() {
            return Err(format!(
                "network may only be selected in the global config scope (line {line_number})"
            ));
        }
        if BOOL_KEYS.contains(&key) && !matches!(value, "true" | "false" | "1" | "0") {
            return Err(format!(
                "config key '{key}' at line {line_number} must be true or false"
            ));
        }
        entries.push(Entry {
            section,
            key: key.to_owned(),
            value: value.to_owned(),
            line: line_number,
        });
    }
    Ok(entries)
}

fn global_network(entries: &[Entry]) -> Result<Option<Network>, String> {
    let matches = entries
        .iter()
        .filter(|entry| entry.section.is_none() && entry.key == "network")
        .collect::<Vec<_>>();
    if matches.len() > 1 {
        return Err(format!(
            "duplicate scalar config key 'network' at line {}",
            matches[1].line
        ));
    }
    matches
        .first()
        .map(|entry| parse_network(&entry.value, &format!("config line {}", entry.line)))
        .transpose()
}

fn selected_arguments(
    entries: &[Entry],
    selected_network: Network,
) -> Result<Vec<ConfigArgument>, String> {
    let mut scalars = BTreeMap::<String, &Entry>::new();
    let mut global_lists = BTreeMap::<String, Vec<&Entry>>::new();
    let mut network_lists = BTreeMap::<String, Vec<&Entry>>::new();
    for entry in entries {
        let selected_scope = entry.section.is_none() || entry.section == Some(selected_network);
        if !selected_scope || entry.key == "network" {
            continue;
        }
        if LIST_KEYS.contains(&entry.key.as_str()) {
            let lists = if entry.section.is_some() {
                &mut network_lists
            } else {
                &mut global_lists
            };
            lists.entry(entry.key.clone()).or_default().push(entry);
            continue;
        }
        if let Some(previous) = scalars.get(&entry.key) {
            if previous.section == entry.section {
                return Err(format!(
                    "duplicate scalar config key '{}' at line {}",
                    entry.key, entry.line
                ));
            }
            if entry.section.is_none() {
                continue;
            }
        }
        scalars.insert(entry.key.clone(), entry);
    }
    if global_network(entries)?.is_some() {
        scalars.insert(
            "network".to_owned(),
            entries
                .iter()
                .find(|entry| entry.section.is_none() && entry.key == "network")
                .expect("parsed global network has an entry"),
        );
    }
    if scalars
        .get("dns_seeds")
        .is_some_and(|entry| parse_bool(entry).is_ok_and(|enabled| !enabled))
        && (global_lists.contains_key("dns_seed") || network_lists.contains_key("dns_seed"))
    {
        return Err("dns_seeds=false conflicts with configured dns_seed values".to_owned());
    }

    let mut arguments = Vec::new();
    for entry in scalars.values() {
        arguments.push(argument_for_scalar(entry)?);
    }
    for key in LIST_KEYS {
        let selected = network_lists.get(key).or_else(|| global_lists.get(key));
        for entry in selected.into_iter().flatten() {
            arguments.push(ConfigArgument {
                flag: flag_for_key(key),
                value: Some(entry.value.clone()),
            });
        }
    }
    Ok(arguments)
}

fn argument_for_scalar(entry: &Entry) -> Result<ConfigArgument, String> {
    if BOOL_KEYS.contains(&entry.key.as_str()) {
        let enabled = parse_bool(entry)?;
        if entry.key == "listen" && enabled {
            return Err(format!(
                "config key 'listen=true' at line {} requires an explicit address; use inbound_listen=IP:PORT",
                entry.line
            ));
        }
        let flag = match (entry.key.as_str(), enabled) {
            ("block_filter_index", true) => "--block-filter-index",
            ("block_filter_index", false) => "--no-block-filter-index",
            ("cleanup_validation_dir", true) => "--cleanup-validation-dir",
            ("cleanup_validation_dir", false) => "--no-cleanup-validation-dir",
            ("dns_seeds", true) => "--dns-seeds",
            ("dns_seeds", false) => "--no-dns-seeds",
            ("mempool_full_rbf", true) => "--mempool-full-rbf",
            ("mempool_full_rbf", false) => "--no-mempool-full-rbf",
            ("listen", false) => "--no-listen",
            ("once", true) => "--once",
            ("once", false) => "--no-once",
            ("spent_output_index", true) => "--spent-output-index",
            ("spent_output_index", false) => "--no-spent-output-index",
            ("txindex", true) => "--txindex",
            ("txindex", false) => "--no-txindex",
            ("v2_transport", true) => "--v2-transport",
            ("v2_transport", false) => "--no-v2-transport",
            ("validation_deferred_repair", true) => "--validation-deferred-repair",
            ("validation_deferred_repair", false) => "--validation-quick-repair",
            _ => unreachable!("known boolean config key"),
        };
        Ok(ConfigArgument { flag, value: None })
    } else {
        Ok(ConfigArgument {
            flag: flag_for_key(&entry.key),
            value: Some(entry.value.clone()),
        })
    }
}

fn parse_bool(entry: &Entry) -> Result<bool, String> {
    match entry.value.as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(format!(
            "config key '{}' at line {} must be true or false",
            entry.key, entry.line
        )),
    }
}

fn known_key(key: &str) -> bool {
    LIST_KEYS.contains(&key) || BOOL_KEYS.contains(&key) || VALUE_KEYS.contains(&key)
}

fn flag_for_key(key: &str) -> &'static str {
    match key {
        "assumevalid" => "--assumevalid",
        "automatic_hot_standbys" => "--automatic-hot-standbys",
        "background_assumeutxo" => "--background-assumeutxo",
        "background_chainstate_cache_bytes" => "--background-chainstate-cache-bytes",
        "bulk_validation_cache_bytes" => "--bulk-validation-cache-bytes",
        "chainstate_cache_bytes" => "--chainstate-cache-bytes",
        "complete_assumeutxo" => "--complete-assumeutxo",
        "connect" => "--connect",
        "data_dir" => "--data-dir",
        "dns_seed" => "--dns-seed",
        "explorer_listen" => "--explorer-listen",
        "external_address" => "--external-address",
        "inbound_listen" => "--listen",
        "inbound_requests_per_minute" => "--inbound-requests-per-minute",
        "log_dir" => "--log-dir",
        "log_level" => "--log-level",
        "log_max_bytes" => "--log-max-bytes",
        "log_max_files" => "--log-max-files",
        "minimum_chainwork" => "--minimum-chainwork",
        "mempool_max_bytes" => "--mempool-max-bytes",
        "mempool_max_transactions" => "--mempool-max-transactions",
        "max_inbound_peers" => "--max-inbound-peers",
        "max_inbound_peers_per_ip" => "--max-inbound-peers-per-ip",
        "max_upload_bytes_per_day" => "--max-upload-bytes-per-day",
        "minimum_free_bytes" => "--minimum-free-bytes",
        "network" => "--network",
        "onlynet" => "--onlynet",
        "prune_blocks" => "--prune-blocks",
        "prune_max_bytes" => "--prune-max-bytes",
        "proxy" => "--proxy",
        "rpc_auth_token_file" => "--rpc-auth-token-file",
        "signetchallenge" => "--signetchallenge",
        "signetseednode" => "--signetseednode",
        "testactivationheight" => "--testactivationheight",
        "validation_batch_size" => "--validation-batch-size",
        "validation_pause_ms" => "--validation-pause-ms",
        "vbparams" => "--vbparams",
        "wallet_auth_token_file" => "--wallet-auth-token-file",
        "wallet_descriptors" => "--wallet-descriptors",
        "whitelist" => "--whitelist",
        _ => unreachable!("known value or list config key"),
    }
}

fn option_value<'a>(arguments: &'a [String], option: &str) -> Option<&'a str> {
    arguments
        .windows(2)
        .rev()
        .find_map(|pair| (pair[0] == option).then_some(pair[1].as_str()))
}

fn known_flag_group(argument: &str) -> Option<&'static str> {
    let group = option_group(argument);
    matches!(
        argument,
        "--assumevalid"
            | "--assume-valid"
            | "--automatic-hot-standbys"
            | "--background-assumeutxo"
            | "--background-chainstate-cache-bytes"
            | "--block-filter-index"
            | "--bulk-validation-cache-bytes"
            | "--chainstate-cache-bytes"
            | "--cleanup-validation-dir"
            | "--complete-assumeutxo"
            | "--connect"
            | "--data-dir"
            | "--dns-seed"
            | "--dns-seeds"
            | "--explorer-listen"
            | "--external-address"
            | "--inbound-requests-per-minute"
            | "--listen"
            | "--log-dir"
            | "--log-level"
            | "--log-max-bytes"
            | "--log-max-files"
            | "--mempool-full-rbf"
            | "--mempool-max-bytes"
            | "--mempool-max-transactions"
            | "--max-inbound-peers"
            | "--max-inbound-peers-per-ip"
            | "--max-upload-bytes-per-day"
            | "--minimum-free-bytes"
            | "--minimum-chainwork"
            | "--network"
            | "--no-cleanup-validation-dir"
            | "--no-block-filter-index"
            | "--no-dns-seeds"
            | "--no-mempool-full-rbf"
            | "--no-listen"
            | "--no-once"
            | "--no-spent-output-index"
            | "--no-txindex"
            | "--no-v2-transport"
            | "--once"
            | "--prune-blocks"
            | "--prune-max-bytes"
            | "--proxy"
            | "--rpc-auth-token-file"
            | "--spent-output-index"
            | "--signetchallenge"
            | "--signetseednode"
            | "--testactivationheight"
            | "--txindex"
            | "--v2-transport"
            | "--validation-batch-size"
            | "--validation-deferred-repair"
            | "--validation-pause-ms"
            | "--validation-quick-repair"
            | "--vbparams"
            | "--onlynet"
            | "--wallet-auth-token-file"
            | "--wallet-descriptors"
            | "--whitelist"
    )
    .then_some(group)
}

fn option_group(flag: &str) -> &'static str {
    match flag {
        "--assume-valid" | "--assumevalid" => "assumevalid",
        "--automatic-hot-standbys" => "automatic-hot-standbys",
        "--cleanup-validation-dir" | "--no-cleanup-validation-dir" => "cleanup-validation-dir",
        "--block-filter-index" | "--no-block-filter-index" => "block-filter-index",
        "--dns-seed" | "--dns-seeds" | "--no-dns-seeds" | "--signetseednode" => "dns-seeds",
        "--mempool-full-rbf" | "--no-mempool-full-rbf" => "mempool-full-rbf",
        "--no-once" | "--once" => "once",
        "--spent-output-index" | "--no-spent-output-index" => "spent-output-index",
        "--txindex" | "--no-txindex" => "txindex",
        "--v2-transport" | "--no-v2-transport" => "v2-transport",
        "--validation-deferred-repair" | "--validation-quick-repair" => "validation-repair",
        "--background-assumeutxo" => "background-assumeutxo",
        "--background-chainstate-cache-bytes" => "background-chainstate-cache-bytes",
        "--bulk-validation-cache-bytes" => "bulk-validation-cache-bytes",
        "--chainstate-cache-bytes" => "chainstate-cache-bytes",
        "--complete-assumeutxo" => "complete-assumeutxo",
        "--connect" => "connect",
        "--data-dir" => "data-dir",
        "--explorer-listen" => "explorer-listen",
        "--external-address" => "external-address",
        "--inbound-requests-per-minute" => "inbound-requests-per-minute",
        "--listen" | "--no-listen" => "listen",
        "--log-dir" => "log-dir",
        "--log-level" => "log-level",
        "--log-max-bytes" => "log-max-bytes",
        "--log-max-files" => "log-max-files",
        "--max-inbound-peers" => "max-inbound-peers",
        "--max-inbound-peers-per-ip" => "max-inbound-peers-per-ip",
        "--max-upload-bytes-per-day" => "max-upload-bytes-per-day",
        "--minimum-chainwork" => "minimum-chainwork",
        "--mempool-max-bytes" => "mempool-max-bytes",
        "--mempool-max-transactions" => "mempool-max-transactions",
        "--minimum-free-bytes" => "minimum-free-bytes",
        "--network" => "network",
        "--prune-blocks" => "prune-blocks",
        "--prune-max-bytes" => "prune-max-bytes",
        "--proxy" => "proxy",
        "--rpc-auth-token-file" => "rpc-auth-token-file",
        "--signetchallenge" => "signetchallenge",
        "--testactivationheight" => "testactivationheight",
        "--validation-batch-size" => "validation-batch-size",
        "--validation-pause-ms" => "validation-pause-ms",
        "--vbparams" => "vbparams",
        "--onlynet" => "onlynet",
        "--wallet-auth-token-file" => "wallet-auth-token-file",
        "--wallet-descriptors" => "wallet-descriptors",
        "--whitelist" => "whitelist",
        _ => "unknown",
    }
}

fn parse_network(value: &str, source: &str) -> Result<Network, String> {
    Network::from_str(value).map_err(|_| format!("unsupported network '{value}' in {source}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn strict_network_scope_and_cli_precedence_are_deterministic() {
        let entries = parse_config(
            "network=bitcoin\n\
             data_dir=/global\n\
             connect=127.0.0.1:8333\n\
             prune_blocks=600\n\
             once=true\n\
             txindex=true\n\
             spent_output_index=true\n\
             block_filter_index=true\n\
             [bitcoin]\n\
             data_dir=/bitcoin\n\
             connect=127.0.0.2:8333\n\
             [testnet4]\n\
             data_dir=/testnet4\n",
        )
        .unwrap();
        let selected = selected_arguments(&entries, Network::Bitcoin).unwrap();
        assert!(selected.iter().any(|entry| {
            entry.flag == "--data-dir" && entry.value.as_deref() == Some("/bitcoin")
        }));
        assert!(selected.iter().any(|entry| {
            entry.flag == "--connect" && entry.value.as_deref() == Some("127.0.0.2:8333")
        }));
        assert!(!selected.iter().any(|entry| {
            entry.flag == "--connect" && entry.value.as_deref() == Some("127.0.0.1:8333")
        }));
        assert!(selected.iter().any(|entry| entry.flag == "--once"));
        assert!(selected.iter().any(|entry| entry.flag == "--txindex"));
        assert!(
            selected
                .iter()
                .any(|entry| entry.flag == "--spent-output-index")
        );
        assert!(
            selected
                .iter()
                .any(|entry| entry.flag == "--block-filter-index")
        );
    }

    #[test]
    fn rejects_unknown_duplicate_and_cross_field_values() {
        assert!(parse_config("mystery=true").is_err());
        let duplicate = parse_config("prune_blocks=500\nprune_blocks=600").unwrap();
        assert!(selected_arguments(&duplicate, Network::Bitcoin).is_err());
        let dns_conflict = parse_config("dns_seeds=false\ndns_seed=seed.example:8333").unwrap();
        assert!(selected_arguments(&dns_conflict, Network::Bitcoin).is_err());
        assert!(parse_config("once=yes").is_err());
    }

    #[test]
    fn file_reader_rejects_oversize_and_symlink_inputs() {
        let directory = TempDir::new().unwrap();
        let oversized = directory.path().join("oversized.conf");
        fs::write(
            &oversized,
            vec![b'x'; usize::try_from(MAX_CONFIG_FILE_BYTES + 1).unwrap()],
        )
        .unwrap();
        assert!(parse_config_file(&oversized).is_err());

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let target = directory.path().join("target.conf");
            let link = directory.path().join("link.conf");
            fs::write(&target, "network=regtest\n").unwrap();
            symlink(&target, &link).unwrap();
            assert!(parse_config_file(&link).is_err());
        }
    }

    #[test]
    fn cli_inbound_override_keeps_unrelated_config_limits() {
        let directory = TempDir::new().unwrap();
        let config = directory.path().join("rbtc.conf");
        fs::write(
            &config,
            "inbound_listen=127.0.0.1:18444\n\
             max_inbound_peers=12\n\
             max_inbound_peers_per_ip=3\n\
             max_upload_bytes_per_day=1000000\n\
             inbound_requests_per_minute=120\n",
        )
        .unwrap();
        let merged = merge_config_arguments(vec![
            "--config".to_owned(),
            config.display().to_string(),
            "--inbound-requests-per-minute".to_owned(),
            "240".to_owned(),
        ])
        .unwrap();
        for retained in [
            "--listen",
            "--max-inbound-peers",
            "--max-inbound-peers-per-ip",
            "--max-upload-bytes-per-day",
        ] {
            assert!(
                merged.iter().any(|argument| argument == retained),
                "{retained} was incorrectly suppressed by another inbound override"
            );
        }
        assert_eq!(
            merged
                .windows(2)
                .filter(|pair| pair[0] == "--inbound-requests-per-minute")
                .count(),
            1
        );
        assert!(merged.ends_with(&["--inbound-requests-per-minute".to_owned(), "240".to_owned()]));
    }
}
