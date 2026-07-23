//! Strict parsing for the durable AssumeUTXO validation-directory owner marker.

use std::str::FromStr;

use bitcoin::{BlockHash, Network};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Maximum encoded validation owner-marker size.
pub const MAX_VALIDATION_OWNER_BYTES: usize = 4_096;

/// Identity proving that an rBTC validation directory belongs to one snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationDirectoryOwner {
    version: u32,
    network: String,
    target_height: u32,
    target_block_hash: String,
}

impl ValidationDirectoryOwner {
    /// Builds the current owner-marker format.
    pub fn new(network: Network, target_height: u32, target_block_hash: BlockHash) -> Self {
        Self {
            version: 1,
            network: network.to_string(),
            target_height,
            target_block_hash: target_block_hash.to_string(),
        }
    }
}

/// Owner-marker decoding or validation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ValidationOwnerError {
    /// The marker exceeds its persistent format ceiling.
    #[error("validation owner marker exceeds its size limit")]
    TooLarge,
    /// The marker is not strict JSON in the expected shape.
    #[error("validation owner marker is malformed")]
    Malformed,
    /// The marker uses an unsupported version or non-canonical identity.
    #[error("validation owner marker has an invalid identity")]
    InvalidIdentity,
}

/// Parses and semantically validates an untrusted validation owner marker.
pub fn parse_validation_directory_owner(
    input: &[u8],
) -> Result<ValidationDirectoryOwner, ValidationOwnerError> {
    if input.len() > MAX_VALIDATION_OWNER_BYTES {
        return Err(ValidationOwnerError::TooLarge);
    }
    let owner: ValidationDirectoryOwner =
        serde_json::from_slice(input).map_err(|_| ValidationOwnerError::Malformed)?;
    let network =
        Network::from_str(&owner.network).map_err(|_| ValidationOwnerError::InvalidIdentity)?;
    let block_hash = BlockHash::from_str(&owner.target_block_hash)
        .map_err(|_| ValidationOwnerError::InvalidIdentity)?;
    if owner.version != 1
        || owner.network != network.to_string()
        || owner.target_block_hash != block_hash.to_string()
    {
        return Err(ValidationOwnerError::InvalidIdentity);
    }
    Ok(owner)
}

#[cfg(test)]
mod tests {
    use bitcoin::hashes::Hash;

    use super::*;

    #[test]
    fn owner_marker_is_strict_bounded_and_canonical() {
        let expected = ValidationDirectoryOwner::new(
            Network::Regtest,
            42,
            BlockHash::from_byte_array([7; 32]),
        );
        let encoded = serde_json::to_vec(&expected).unwrap();
        assert_eq!(
            parse_validation_directory_owner(&encoded).unwrap(),
            expected
        );

        for invalid in [
            br#"{"version":2,"network":"regtest","target_height":42,"target_block_hash":"0707070707070707070707070707070707070707070707070707070707070707"}"#
                .as_slice(),
            br#"{"version":1,"network":"unknown","target_height":42,"target_block_hash":"0707070707070707070707070707070707070707070707070707070707070707"}"#,
            br#"{"version":1,"network":"regtest","target_height":42,"target_block_hash":"0707070707070707070707070707070707070707070707070707070707070707","extra":true}"#,
        ] {
            assert!(parse_validation_directory_owner(invalid).is_err());
        }
        assert_eq!(
            parse_validation_directory_owner(&vec![b' '; MAX_VALIDATION_OWNER_BYTES + 1]),
            Err(ValidationOwnerError::TooLarge)
        );
    }
}
