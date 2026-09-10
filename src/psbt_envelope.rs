//! Bounded wire checks around the wallet's BIP174 semantic decoder.
//!
//! Unknown keys and arbitrary map order remain permitted. This layer only
//! enforces canonical lengths, exact map consumption and witness framing.

/// Decodes canonical, padded RFC4648 base64 without allocating above `limit`.
pub(crate) fn decode_base64(encoded: &str, limit: usize) -> Result<Vec<u8>, &'static str> {
    let encoded = encoded.as_bytes();
    if encoded.len() % 4 != 0 {
        return Err("invalid PSBT base64 length");
    }
    let padding = encoded
        .iter()
        .rev()
        .take_while(|byte| **byte == b'=')
        .count();
    if padding > 2 {
        return Err("invalid PSBT base64 padding");
    }
    let length = (encoded.len() / 4)
        .checked_mul(3)
        .and_then(|length| length.checked_sub(padding))
        .ok_or("invalid PSBT base64 length")?;
    if length > limit {
        return Err("decoded PSBT exceeds request bound");
    }
    let mut decoded = Vec::with_capacity(length);
    for (index, group) in encoded.chunks_exact(4).enumerate() {
        let last = index + 1 == encoded.len() / 4;
        let a = base64_digit(group[0])?;
        let b = base64_digit(group[1])?;
        decoded.push((a << 2) | (b >> 4));
        if group[2] == b'=' {
            if !last || group[3] != b'=' || b & 15 != 0 {
                return Err("invalid PSBT base64 padding");
            }
            continue;
        }
        let c = base64_digit(group[2])?;
        decoded.push((b << 4) | (c >> 2));
        if group[3] == b'=' {
            if !last || c & 3 != 0 {
                return Err("invalid PSBT base64 padding");
            }
            continue;
        }
        decoded.push((c << 6) | base64_digit(group[3])?);
    }
    Ok(decoded)
}

fn base64_digit(byte: u8) -> Result<u8, &'static str> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err("invalid PSBT base64 character"),
    }
}

/// Checks one global map and the exact number of input/output maps.
///
/// Counts come from the semantic PSBT decoder, whose input/output budgets must
/// already have been checked. No allocation or signature interpretation occurs.
pub(crate) fn validate_envelope(
    raw: &[u8],
    inputs: usize,
    outputs: usize,
) -> Result<(), &'static str> {
    let mut remaining = raw.strip_prefix(b"psbt\xff").ok_or("invalid PSBT magic")?;
    scan_map(&mut remaining, false)?;
    for _ in 0..inputs {
        scan_map(&mut remaining, true)?;
    }
    for _ in 0..outputs {
        scan_map(&mut remaining, false)?;
    }
    if !remaining.is_empty() {
        return Err("trailing PSBT data");
    }
    Ok(())
}

fn scan_map(remaining: &mut &[u8], input_map: bool) -> Result<(), &'static str> {
    loop {
        let key_length = compact_size(remaining)?;
        if key_length == 0 {
            return Ok(());
        }
        let key = take(remaining, key_length)?;
        let value_length = compact_size(remaining)?;
        let value = take(remaining, value_length)?;
        if input_map && key == [0x08] {
            validate_witness(value)?;
        }
    }
}

fn validate_witness(mut remaining: &[u8]) -> Result<(), &'static str> {
    let count = compact_size(&mut remaining)?;
    // Each witness element needs at least a one-byte length prefix.
    if count > remaining.len() {
        return Err("truncated PSBT witness");
    }
    for _ in 0..count {
        let length = compact_size(&mut remaining)?;
        let _ = take(&mut remaining, length)?;
    }
    if !remaining.is_empty() {
        return Err("trailing PSBT witness data");
    }
    Ok(())
}

fn take<'a>(remaining: &mut &'a [u8], length: usize) -> Result<&'a [u8], &'static str> {
    if length > remaining.len() {
        return Err("truncated PSBT field");
    }
    let (value, rest) = remaining.split_at(length);
    *remaining = rest;
    Ok(value)
}

fn compact_size(remaining: &mut &[u8]) -> Result<usize, &'static str> {
    let prefix = take(remaining, 1)?[0];
    let (width, minimum) = match prefix {
        0..=252 => return Ok(usize::from(prefix)),
        253 => (2, 253),
        254 => (4, 0x1_0000),
        255 => (8, 0x1_0000_0000),
    };
    let mut bytes = [0_u8; 8];
    bytes[..width].copy_from_slice(take(remaining, width)?);
    let value = u64::from_le_bytes(bytes);
    if value < minimum {
        return Err("noncanonical PSBT CompactSize");
    }
    usize::try_from(value).map_err(|_| "PSBT field length exceeds address space")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc4648_vectors_padding_and_allocation_bounds() {
        for (encoded, raw) in [
            ("", ""),
            ("Zg==", "f"),
            ("Zm8=", "fo"),
            ("Zm9v", "foo"),
            ("Zm9vYg==", "foob"),
            ("Zm9vYmE=", "fooba"),
            ("Zm9vYmFy", "foobar"),
        ] {
            assert_eq!(decode_base64(encoded, raw.len()).unwrap(), raw.as_bytes());
            if !raw.is_empty() {
                assert!(decode_base64(encoded, raw.len() - 1).is_err());
            }
        }
        for invalid in [
            "Zg", "Zh==", "Zm9=", "Z===", "=g==", "Zg==Zg==", "Zm?=", "Zm 8",
        ] {
            assert!(decode_base64(invalid, 128).is_err(), "{invalid}");
        }
    }

    #[test]
    fn consumes_exact_map_count_and_preserves_unknown_keys() {
        let raw = decode_base64("cHNidP8AAAA=", 8).unwrap();
        assert_eq!(validate_envelope(&raw, 1, 1), Ok(()));
        let mut trailing = raw.clone();
        trailing.push(0);
        assert_eq!(
            validate_envelope(&trailing, 1, 1),
            Err("trailing PSBT data")
        );
        assert!(validate_envelope(&raw[..7], 1, 1).is_err());
        // Unknown global key 0xfc with a two-byte value, followed by maps.
        let unknown = b"psbt\xff\x01\xfc\x02\x12\x34\x00\x00\x00";
        assert_eq!(validate_envelope(unknown, 1, 1), Ok(()));
    }

    #[test]
    fn final_witness_must_consume_its_entire_value() {
        let valid = b"psbt\xff\x00\x01\x08\x03\x01\x01\x42\x00\x00";
        assert_eq!(validate_envelope(valid, 1, 1), Ok(()));
        let trailing = b"psbt\xff\x00\x01\x08\x02\x00\x42\x00\x00";
        assert_eq!(
            validate_envelope(trailing, 1, 1),
            Err("trailing PSBT witness data")
        );
        assert!(validate_witness(&[2, 0]).is_err());
        assert!(validate_witness(&[1, 2, 0]).is_err());
    }

    #[test]
    fn compact_lengths_are_canonical_and_cannot_overrun_the_message() {
        for bad in [
            vec![0xfd, 0, 0],
            vec![0xfe, 0xff, 0, 0, 0],
            vec![0xff, 0, 0, 0, 0, 0, 0, 0, 0],
            vec![0xfd, 1],
        ] {
            assert!(compact_size(&mut bad.as_slice()).is_err());
        }
        let enormous = b"psbt\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";
        assert!(validate_envelope(enormous, 0, 0).is_err());
    }
}
