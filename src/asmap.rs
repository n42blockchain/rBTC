//! Bitcoin Core-compatible asmap interpretation for ASN-based peer bucketing.
//!
//! An asmap file is a compressed binary trie mapping every IP address to the
//! autonomous system number (ASN) announcing it. Bitcoin Core has shipped the
//! format since 0.20 and embeds a default map since 31.0, so address-manager
//! bucketing diversifies across network operators rather than across raw IP
//! prefixes. This module interprets exactly that format: the instruction
//! encoding, the IPv4-in-IPv6 lookup form, and the structural sanity check
//! are ports of Core's `util/asmap.cpp`, so one data file answers identically
//! in both implementations.
//!
//! Every map accepted here passed the full structural validation first. The
//! interpreter itself still refuses to trust its input: malformed data can
//! only terminate a lookup with "unknown" (ASN 0), never panic and never
//! loop, because each iteration consumes at least one instruction bit and
//! jumps only move forward.

use std::{
    net::IpAddr,
    path::Path,
    sync::{Arc, OnceLock},
};

use thiserror::Error;

/// Ceiling for one asmap file, embedded or operator-supplied.
///
/// The 2026-08 published map is ~1.5 MiB; the bound leaves an order of
/// magnitude of growth while keeping a hostile file from staging an
/// unbounded allocation.
pub const MAX_ASMAP_BYTES: usize = 16 * 1024 * 1024;

/// The asmap data compiled into this binary.
///
/// Source: `bitcoin-core/asmap-data` file `2026/1786032000_asmap.dat`
/// (map epoch 2026-08-06T16:00:00Z, published in upstream commit
/// `508156eb35a0c12e6fd14edd33c35db53bc1eea3`), SHA-256
/// `5691b328295cd4266f86e85f84316eea049896049246646b918edec67348eb99`.
/// Provenance and the update procedure are documented in `docs/ASMAP.md`.
const EMBEDDED_ASMAP: &[u8] = include_bytes!("../vendor/asmap/1786032000_asmap.dat");

/// Sentinel for a decode that ran out of bits or overran a field.
const INVALID: u32 = u32::MAX;

/// Bit-size ladder for instruction opcodes.
const TYPE_BIT_SIZES: &[u8] = &[0, 0, 1];
/// Bit-size ladder for ASN operands; ASN 0 is deliberately unencodable
/// because RFC 7607 reserves it, which is what makes 0 safe as "unknown".
const ASN_BIT_SIZES: &[u8] = &[15, 16, 17, 18, 19, 20, 21, 22, 23, 24];
/// Bit-size ladder for match operands (1 to 8 prefix bits per instruction).
const MATCH_BIT_SIZES: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8];
/// Bit-size ladder for jump offsets; the minimum of 17 is the size of the
/// smallest possible subtree (one RETURN instruction).
const JUMP_BIT_SIZES: &[u8] = &[
    5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    30,
];

/// Failures loading or validating an asmap file.
#[derive(Debug, Error)]
pub enum AsmapError {
    /// The file or byte buffer holds no data.
    #[error("asmap data is empty")]
    Empty,
    /// The file exceeds [`MAX_ASMAP_BYTES`].
    #[error("asmap data exceeds the {MAX_ASMAP_BYTES}-byte ceiling")]
    TooLarge,
    /// The data is not a structurally valid asmap program.
    #[error("asmap data failed structural validation")]
    Invalid,
    /// The map compiled into this binary failed validation, which indicates
    /// a corrupted build rather than an operator mistake.
    #[error("embedded asmap data failed structural validation")]
    EmbeddedInvalid,
    /// Reading an operator-supplied file failed.
    #[error("asmap file: {0}")]
    Io(#[from] std::io::Error),
}

/// One decoded asmap instruction opcode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Instruction {
    Return,
    Jump,
    Match,
    Default,
}

/// A validated IP-to-ASN map.
pub struct Asmap {
    data: Vec<u8>,
}

impl Asmap {
    /// Validates `data` as a complete asmap program and takes ownership.
    pub fn from_bytes(data: Vec<u8>) -> Result<Self, AsmapError> {
        if data.is_empty() {
            return Err(AsmapError::Empty);
        }
        if data.len() > MAX_ASMAP_BYTES {
            return Err(AsmapError::TooLarge);
        }
        let map = Self { data };
        if !map.sanity_check() {
            return Err(AsmapError::Invalid);
        }
        Ok(map)
    }

    /// Reads and validates one operator-supplied asmap file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, AsmapError> {
        let metadata = std::fs::metadata(path.as_ref())?;
        if metadata.len() > MAX_ASMAP_BYTES as u64 {
            return Err(AsmapError::TooLarge);
        }
        Self::from_bytes(std::fs::read(path.as_ref())?)
    }

    /// Returns the map compiled into this binary, validating it once.
    pub fn embedded() -> Result<Arc<Self>, AsmapError> {
        static EMBEDDED: OnceLock<Result<Arc<Asmap>, AsmapError>> = OnceLock::new();
        EMBEDDED
            .get_or_init(|| Asmap::from_bytes(EMBEDDED_ASMAP.to_vec()).map(Arc::new))
            .as_ref()
            .map(Arc::clone)
            .map_err(|_| AsmapError::EmbeddedInvalid)
    }

    /// Interprets `data` as an asmap program with no structural validation.
    ///
    /// Exists so fuzzing can drive the interpreter with bytes that never
    /// passed [`Asmap::from_bytes`]; the interpreter's own guarantee — no
    /// panic, bounded termination, "unknown" on malformed programs — is
    /// exactly what the fuzz target asserts. Production callers construct
    /// through the validating paths instead.
    #[must_use]
    pub fn interpret_unvalidated(data: &[u8], ip: IpAddr) -> u32 {
        let map = Self {
            data: data.to_vec(),
        };
        map.map_asn(ip)
    }

    /// Maps one IP address to the ASN announcing it; 0 means unknown.
    ///
    /// IPv4 addresses and the IPv6 transition encodings carrying one
    /// (v4-mapped, NAT64 RFC 6052, translated RFC 6145, 6to4 RFC 3964,
    /// Teredo RFC 4380) look up as `::ffff:a.b.c.d`, exactly as Core's
    /// `GetMappedAS` does, so both clients bucket such a peer identically.
    #[must_use]
    pub fn map_asn(&self, ip: IpAddr) -> u32 {
        self.interpret(&lookup_bits(ip))
    }

    fn bit_len(&self) -> usize {
        self.data.len() * 8
    }

    fn bit(&self, position: usize) -> bool {
        (self.data[position / 8] >> (position % 8)) & 1 == 1
    }

    /// Executes the asmap program over 128 address bits.
    ///
    /// Mirrors Core's `Interpret`, except that where Core asserts that a
    /// sanity-checked map cannot misbehave, this returns "unknown".
    fn interpret(&self, ip: &[bool; 128]) -> u32 {
        let mut reader = BitReader {
            map: self,
            position: 0,
        };
        let mut bits: u32 = 128;
        let mut default_asn = 0;
        while reader.remaining() > 0 {
            let Some(opcode) = decode_type(&mut reader) else {
                break;
            };
            match opcode {
                Instruction::Return => {
                    let asn = decode_bits(&mut reader, 1, ASN_BIT_SIZES);
                    if asn == INVALID {
                        break;
                    }
                    return asn;
                }
                Instruction::Jump => {
                    let jump = decode_bits(&mut reader, 17, JUMP_BIT_SIZES);
                    if jump == INVALID || bits == 0 {
                        break;
                    }
                    if jump as usize >= reader.remaining() {
                        break;
                    }
                    if ip[128 - bits as usize] {
                        reader.position += jump as usize;
                    }
                    bits -= 1;
                }
                Instruction::Match => {
                    let matched = decode_bits(&mut reader, 2, MATCH_BIT_SIZES);
                    if matched == INVALID {
                        break;
                    }
                    let match_len = 31 - matched.leading_zeros();
                    if bits < match_len {
                        break;
                    }
                    let mut diverged = false;
                    for offset in 0..match_len {
                        let expected = (matched >> (match_len - 1 - offset)) & 1 == 1;
                        if ip[128 - bits as usize] != expected {
                            diverged = true;
                            break;
                        }
                        bits -= 1;
                    }
                    if diverged {
                        return default_asn;
                    }
                }
                Instruction::Default => {
                    let asn = decode_bits(&mut reader, 1, ASN_BIT_SIZES);
                    if asn == INVALID {
                        break;
                    }
                    default_asn = asn;
                }
            }
        }
        0
    }

    /// Verifies that every reachable execution path decodes cleanly, jumps
    /// stay in range and consume address bits, no code is unreachable, and
    /// the trailing padding is shorter than one byte and zero.
    ///
    /// Port of Core's `SanityCheckASMap` over 128 address bits.
    #[allow(clippy::too_many_lines)]
    fn sanity_check(&self) -> bool {
        let end = self.bit_len();
        let mut reader = BitReader {
            map: self,
            position: 0,
        };
        let mut bits: u32 = 128;
        // Pending jump targets: bit offset paired with the address bits that
        // remain to be consumed on that branch. Targets are strictly
        // decreasing from back to front, so the vector never exceeds one
        // entry per address bit.
        let mut jumps: Vec<(usize, u32)> = Vec::with_capacity(128);
        let mut previous = Instruction::Jump;
        let mut had_incomplete_match = false;
        while reader.remaining() > 0 {
            if let Some(&(target, _)) = jumps.last() {
                if reader.position >= target {
                    // A jump landed inside the previous instruction.
                    return false;
                }
            }
            let Some(opcode) = decode_type(&mut reader) else {
                return false;
            };
            match opcode {
                Instruction::Return => {
                    if previous == Instruction::Default {
                        // A RETURN directly after a DEFAULT could have been
                        // one RETURN.
                        return false;
                    }
                    if decode_bits(&mut reader, 1, ASN_BIT_SIZES) == INVALID {
                        return false;
                    }
                    match jumps.pop() {
                        None => {
                            if end - reader.position > 7 {
                                // More than one byte of padding.
                                return false;
                            }
                            while reader.remaining() > 0 {
                                if reader.read() != Some(true) {
                                    continue;
                                }
                                // Nonzero padding bit.
                                return false;
                            }
                            return true;
                        }
                        Some((target, remaining_bits)) => {
                            if reader.position != target {
                                // Unreachable code before the jump target.
                                return false;
                            }
                            bits = remaining_bits;
                            previous = Instruction::Jump;
                        }
                    }
                }
                Instruction::Jump => {
                    let jump = decode_bits(&mut reader, 17, JUMP_BIT_SIZES);
                    if jump == INVALID {
                        return false;
                    }
                    if jump as usize > reader.remaining() {
                        return false;
                    }
                    if bits == 0 {
                        // Consuming address bits past the end of the input.
                        return false;
                    }
                    bits -= 1;
                    let target = reader.position + jump as usize;
                    if let Some(&(pending, _)) = jumps.last() {
                        if target >= pending {
                            // Intersecting jump ranges.
                            return false;
                        }
                    }
                    jumps.push((target, bits));
                    previous = Instruction::Jump;
                }
                Instruction::Match => {
                    let matched = decode_bits(&mut reader, 2, MATCH_BIT_SIZES);
                    if matched == INVALID {
                        return false;
                    }
                    let match_len = 31 - matched.leading_zeros();
                    if previous != Instruction::Match {
                        had_incomplete_match = false;
                    }
                    if match_len < 8 && had_incomplete_match {
                        // Only the last match in a chain may be short.
                        return false;
                    }
                    had_incomplete_match = match_len < 8;
                    if bits < match_len {
                        return false;
                    }
                    bits -= match_len;
                    previous = Instruction::Match;
                }
                Instruction::Default => {
                    if previous == Instruction::Default {
                        // Two successive DEFAULTs could have been one.
                        return false;
                    }
                    if decode_bits(&mut reader, 1, ASN_BIT_SIZES) == INVALID {
                        return false;
                    }
                    previous = Instruction::Default;
                }
            }
        }
        // Reached the end without a terminating RETURN.
        false
    }
}

struct BitReader<'a> {
    map: &'a Asmap,
    position: usize,
}

impl BitReader<'_> {
    fn remaining(&self) -> usize {
        self.map.bit_len().saturating_sub(self.position)
    }

    fn read(&mut self) -> Option<bool> {
        if self.position >= self.map.bit_len() {
            return None;
        }
        let bit = self.map.bit(self.position);
        self.position += 1;
        Some(bit)
    }
}

/// Decodes one variable-length value; [`INVALID`] when the field straddles
/// the end of the data.
///
/// The encoding walks `bit_sizes`: each step reads one selector bit (absent
/// on the final step), a set selector adds `1 << size` to the value, and a
/// clear selector reads `size` literal bits most-significant first and
/// terminates. The accumulated value never overflows `u32` for any ladder
/// used by the format.
fn decode_bits(reader: &mut BitReader<'_>, minval: u32, bit_sizes: &[u8]) -> u32 {
    let mut value = minval;
    for (index, &size) in bit_sizes.iter().enumerate() {
        let selector = if index + 1 == bit_sizes.len() {
            false
        } else {
            match reader.read() {
                Some(bit) => bit,
                None => break,
            }
        };
        if selector {
            value += 1 << size;
        } else {
            for offset in 0..size {
                let Some(bit) = reader.read() else {
                    return INVALID;
                };
                if bit {
                    value += 1 << (size - 1 - offset);
                }
            }
            return value;
        }
    }
    INVALID
}

fn decode_type(reader: &mut BitReader<'_>) -> Option<Instruction> {
    match decode_bits(reader, 0, TYPE_BIT_SIZES) {
        0 => Some(Instruction::Return),
        1 => Some(Instruction::Jump),
        2 => Some(Instruction::Match),
        3 => Some(Instruction::Default),
        _ => None,
    }
}

/// The `::ffff:0:0/96` prefix carrying an IPv4 address inside IPv6.
const IPV4_IN_IPV6_PREFIX: [u8; 12] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff];

/// Extracts the IPv4 address an IPv6 transition encoding carries, if any.
///
/// Covers the same encodings as Core's `HasLinkedIPv4`/`GetLinkedIPv4`:
/// v4-mapped, NAT64 well-known prefix (RFC 6052), IPv4-translated
/// (RFC 6145), 6to4 (RFC 3964), and Teredo (RFC 4380).
fn linked_ipv4(octets: &[u8; 16]) -> Option<[u8; 4]> {
    if octets[..12] == IPV4_IN_IPV6_PREFIX {
        // v4-mapped ::ffff:a.b.c.d
        return Some([octets[12], octets[13], octets[14], octets[15]]);
    }
    if octets[..12] == [0, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0] {
        // RFC 6052 64:ff9b::a.b.c.d
        return Some([octets[12], octets[13], octets[14], octets[15]]);
    }
    if octets[..12] == [0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0, 0] {
        // RFC 6145 ::ffff:0:a.b.c.d
        return Some([octets[12], octets[13], octets[14], octets[15]]);
    }
    if octets[..2] == [0x20, 0x02] {
        // RFC 3964 2002:aabb:ccdd::/48 embeds a.b.c.d in bytes 2..6.
        return Some([octets[2], octets[3], octets[4], octets[5]]);
    }
    if octets[..4] == [0x20, 0x01, 0, 0] {
        // RFC 4380 Teredo stores the client address inverted in the tail.
        return Some([!octets[12], !octets[13], !octets[14], !octets[15]]);
    }
    None
}

/// Expands one address into the 128 lookup bits, most significant first.
fn lookup_bits(ip: IpAddr) -> [bool; 128] {
    let octets = match ip {
        IpAddr::V4(v4) => v4.to_ipv6_mapped().octets(),
        IpAddr::V6(v6) => {
            let octets = v6.octets();
            match linked_ipv4(&octets) {
                Some(v4) => {
                    let mut mapped = [0_u8; 16];
                    mapped[..12].copy_from_slice(&IPV4_IN_IPV6_PREFIX);
                    mapped[12..].copy_from_slice(&v4);
                    mapped
                }
                None => octets,
            }
        }
    };
    let mut bits = [false; 128];
    for (byte_index, byte) in octets.iter().enumerate() {
        for bit_index in 0..8 {
            bits[byte_index * 8 + bit_index] = (byte >> (7 - bit_index)) & 1 == 1;
        }
    }
    bits
}

/// Test-only asmap construction: a minimal encoder for prefix-to-ASN tables.
///
/// Kept beside the interpreter so round-trip tests and the peer-store
/// eclipse tests can build small, structurally valid maps without shipping
/// fixture files. It emits the same instruction encoding Core's Python
/// encoder does, minus the DEFAULT-hoisting optimization, which the
/// interpreter does not require.
#[cfg(test)]
pub(crate) mod test_encoder {
    use super::{ASN_BIT_SIZES, JUMP_BIT_SIZES, MATCH_BIT_SIZES, TYPE_BIT_SIZES};

    /// One mapping entry: the first `prefix_len` of `prefix_bits` map to
    /// `asn`.
    pub(crate) struct Entry {
        pub prefix_bits: [bool; 128],
        pub prefix_len: u32,
        pub asn: u32,
    }

    /// Maps an IPv4 `a.b.0.0/16` prefix (in its v4-mapped lookup form).
    pub(crate) fn v4_prefix16(a: u8, b: u8, asn: u32) -> Entry {
        let mut bits = [false; 128];
        for (index, bit) in bits.iter_mut().enumerate().take(96).skip(80) {
            *bit = (0xffff_u32 >> (95 - index)) & 1 == 1;
        }
        for index in 0..8 {
            bits[96 + index] = (a >> (7 - index)) & 1 == 1;
            bits[104 + index] = (b >> (7 - index)) & 1 == 1;
        }
        Entry {
            prefix_bits: bits,
            prefix_len: 112,
            asn,
        }
    }

    /// Maps an IPv6 `first:second::/32` prefix.
    pub(crate) fn v6_prefix32(first: u16, second: u16, asn: u32) -> Entry {
        let mut bits = [false; 128];
        for index in 0..16 {
            bits[index] = (first >> (15 - index)) & 1 == 1;
            bits[16 + index] = (second >> (15 - index)) & 1 == 1;
        }
        Entry {
            prefix_bits: bits,
            prefix_len: 32,
            asn,
        }
    }

    enum Node {
        Leaf(u32),
        Branch(Box<Node>, Box<Node>),
    }

    impl Node {
        fn unknown(&self) -> bool {
            matches!(self, Self::Leaf(0))
        }
    }

    fn insert(node: &mut Node, bits: &[bool; 128], depth: u32, len: u32, asn: u32) {
        if depth == len {
            *node = Node::Leaf(asn);
            return;
        }
        if let Node::Leaf(existing) = node {
            let value = *existing;
            *node = Node::Branch(Box::new(Node::Leaf(value)), Box::new(Node::Leaf(value)));
        }
        let Node::Branch(zero, one) = node else {
            unreachable!("leaf was split above");
        };
        let child = if bits[depth as usize] { one } else { zero };
        insert(child, bits, depth + 1, len, asn);
    }

    fn simplify(node: &mut Node) {
        if let Node::Branch(zero, one) = node {
            simplify(zero);
            simplify(one);
            if let (Node::Leaf(left), Node::Leaf(right)) = (zero.as_ref(), one.as_ref()) {
                if left == right {
                    *node = Node::Leaf(*left);
                }
            }
        }
    }

    fn encode_bits(out: &mut Vec<bool>, value: u32, minval: u32, bit_sizes: &[u8]) {
        let mut remaining = value - minval;
        for (index, &size) in bit_sizes.iter().enumerate() {
            let last = index + 1 == bit_sizes.len();
            if last || (remaining >> size) == 0 {
                if !last {
                    out.push(false);
                }
                for offset in (0..size).rev() {
                    out.push((remaining >> offset) & 1 == 1);
                }
                return;
            }
            out.push(true);
            remaining -= 1 << size;
        }
    }

    fn emit(node: &Node, out: &mut Vec<bool>) {
        let mut pattern = Vec::new();
        let mut current = node;
        while let Node::Branch(zero, one) = current {
            if zero.unknown() && !one.unknown() {
                pattern.push(true);
                current = one;
            } else if one.unknown() && !zero.unknown() {
                pattern.push(false);
                current = zero;
            } else {
                break;
            }
        }
        for chunk in pattern.chunks(8) {
            let mut matched = 1_u32;
            for &bit in chunk {
                matched = (matched << 1) | u32::from(bit);
            }
            encode_bits(out, 2, 0, TYPE_BIT_SIZES);
            encode_bits(out, matched, 2, MATCH_BIT_SIZES);
        }
        match current {
            Node::Leaf(asn) => {
                assert!(*asn != 0, "an all-unknown subtree has no encoding");
                encode_bits(out, 0, 0, TYPE_BIT_SIZES);
                encode_bits(out, *asn, 1, ASN_BIT_SIZES);
            }
            Node::Branch(zero, one) => {
                let mut left = Vec::new();
                emit(zero, &mut left);
                encode_bits(out, 1, 0, TYPE_BIT_SIZES);
                encode_bits(
                    out,
                    u32::try_from(left.len()).expect("test subtree fits u32"),
                    17,
                    JUMP_BIT_SIZES,
                );
                out.extend_from_slice(&left);
                emit(one, out);
            }
        }
    }

    /// Encodes `entries` into asmap bytes; unlisted space maps to unknown.
    pub(crate) fn encode(entries: &[Entry]) -> Vec<u8> {
        let mut root = Node::Leaf(0);
        for entry in entries {
            insert(
                &mut root,
                &entry.prefix_bits,
                0,
                entry.prefix_len,
                entry.asn,
            );
        }
        simplify(&mut root);
        let mut bits = Vec::new();
        emit(&root, &mut bits);
        let mut bytes = vec![0_u8; bits.len().div_ceil(8)];
        for (index, &bit) in bits.iter().enumerate() {
            if bit {
                bytes[index / 8] |= 1 << (index % 8);
            }
        }
        bytes
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::{Asmap, AsmapError, test_encoder};

    fn map_of(entries: &[test_encoder::Entry]) -> Asmap {
        Asmap::from_bytes(test_encoder::encode(entries)).expect("encoded test map validates")
    }

    #[test]
    fn a_single_return_maps_every_address_to_one_asn() {
        let map = map_of(&[test_encoder::Entry {
            prefix_bits: [false; 128],
            prefix_len: 0,
            asn: 64512,
        }]);
        assert_eq!(map.map_asn(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4))), 64512);
        assert_eq!(map.map_asn(IpAddr::V6(Ipv6Addr::LOCALHOST)), 64512);
    }

    #[test]
    fn distinct_prefixes_resolve_to_their_own_asns_and_unknown_elsewhere() {
        let map = map_of(&[
            test_encoder::v4_prefix16(8, 8, 15169),
            test_encoder::v4_prefix16(1, 1, 13335),
        ]);
        assert_eq!(map.map_asn(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))), 15169);
        assert_eq!(map.map_asn(IpAddr::V4(Ipv4Addr::new(8, 8, 200, 1))), 15169);
        assert_eq!(map.map_asn(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))), 13335);
        assert_eq!(map.map_asn(IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9))), 0);
        assert_eq!(
            map.map_asn(IpAddr::V6("2001:4860:4860::8888".parse().unwrap())),
            0,
            "a v6 address outside every entry stays unknown"
        );
    }

    #[test]
    fn transition_encodings_look_up_as_their_linked_ipv4() {
        let map = map_of(&[test_encoder::v4_prefix16(8, 8, 15169)]);
        let expectations: &[(&str, u32)] = &[
            ("::ffff:8.8.8.8", 15169),
            ("64:ff9b::8.8.8.8", 15169),
            ("::ffff:0:808:808", 15169),
            ("2002:808:808::", 15169),
            // Teredo stores the client inverted: !8 = 247.
            ("2001::f7f7:f7f7", 15169),
            ("2002:909:909::", 0),
        ];
        for (address, expected) in expectations {
            let ip: Ipv6Addr = address.parse().unwrap();
            assert_eq!(map.map_asn(IpAddr::V6(ip)), *expected, "{address}");
        }
    }

    #[test]
    fn a_default_instruction_answers_for_diverging_matches() {
        // Hand-built program: DEFAULT 64500, then MATCH on the first
        // lookup bit being zero, then RETURN 64501. The lookup form of an
        // IPv4 address starts with 80 zero bits, so every plain IPv4
        // address matches, while any address starting with a one bit
        // diverges to the default.
        let map = {
            let mut bits = Vec::new();
            let push = |bits: &mut Vec<bool>, pattern: &[u8]| {
                bits.extend(pattern.iter().map(|bit| *bit == 1));
            };
            // DEFAULT (type 3 = "111"), ASN 64500 (separator "10", 16 bits).
            push(&mut bits, &[1, 1, 1]);
            push(&mut bits, &[1, 0]);
            let default_operand = 64500_u32 - 1 - (1 << 15);
            for offset in (0..16).rev() {
                push(&mut bits, &[u8::from((default_operand >> offset) & 1 == 1)]);
            }
            // MATCH (type 2 = "110"), value 2 = first bit must be zero.
            push(&mut bits, &[1, 1, 0]);
            push(&mut bits, &[0, 0]);
            // RETURN (type 0 = "0"), ASN 64501.
            push(&mut bits, &[0]);
            push(&mut bits, &[1, 0]);
            let return_operand = 64501_u32 - 1 - (1 << 15);
            for offset in (0..16).rev() {
                push(&mut bits, &[u8::from((return_operand >> offset) & 1 == 1)]);
            }
            let mut bytes = vec![0_u8; bits.len().div_ceil(8)];
            for (index, &bit) in bits.iter().enumerate() {
                if bit {
                    bytes[index / 8] |= 1 << (index % 8);
                }
            }
            Asmap::from_bytes(bytes).expect("hand-built program validates")
        };
        assert_eq!(map.map_asn(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))), 64501);
        assert_eq!(
            map.map_asn(IpAddr::V6("8000::1".parse().unwrap())),
            64500,
            "a diverging match falls back to the preceding DEFAULT"
        );
    }

    #[test]
    fn malformed_data_is_refused_at_load() {
        assert!(matches!(
            Asmap::from_bytes(Vec::new()),
            Err(AsmapError::Empty)
        ));
        // Truncated: a JUMP opcode with no operand.
        assert!(matches!(
            Asmap::from_bytes(vec![0b0000_0010]),
            Err(AsmapError::Invalid)
        ));
        // All-ones padding after a valid program is nonzero padding.
        let mut padded = test_encoder::encode(&[test_encoder::Entry {
            prefix_bits: [false; 128],
            prefix_len: 0,
            asn: 64512,
        }]);
        padded.push(0xff);
        assert!(matches!(
            Asmap::from_bytes(padded),
            Err(AsmapError::Invalid)
        ));
    }

    #[test]
    fn interpretation_never_panics_on_bytes_that_skip_validation() {
        // The interpreter is exercised directly so a future caller holding
        // unvalidated bytes still cannot be panicked by them.
        for seed in 0_u32..256 {
            let data = (0..64)
                .map(|index| {
                    let value = seed
                        .wrapping_mul(2_654_435_761)
                        .wrapping_add(index)
                        .wrapping_mul(2_246_822_519);
                    u8::try_from((value >> 16) & 0xff).expect("masked to one byte")
                })
                .collect::<Vec<_>>();
            let map = Asmap { data };
            let _ = map.map_asn(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)));
            let _ = map.map_asn(IpAddr::V6("2001:4860:4860::8888".parse().unwrap()));
        }
    }

    #[test]
    fn the_embedded_map_validates_and_diversifies_a_real_address_set() {
        let map = Asmap::embedded().expect("embedded asmap data validates");
        // Well-known anycast services in distinct autonomous systems. The
        // exact ASNs are pinned so a silently regenerated or corrupted
        // data file cannot pass as equivalent.
        let google = map.map_asn(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)));
        let cloudflare = map.map_asn(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));
        let quad9 = map.map_asn(IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9)));
        assert_eq!(google, 15169);
        assert_eq!(cloudflare, 13335);
        assert_eq!(quad9, 19281);
        let google_v6 = map.map_asn(IpAddr::V6("2001:4860:4860::8888".parse().unwrap()));
        assert_eq!(
            google_v6, 15169,
            "the same operator's v4 and v6 space resolves to the same ASN"
        );
    }
}
