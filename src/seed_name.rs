//! Strict DNS names accepted for proxied seed discovery.
//!
//! A name-proxy bootstrap submits a DNS seed authority's hostname to a SOCKS5
//! proxy so the proxy resolves it and the local host never does. That name
//! crosses a trust boundary in a length-prefixed wire field, so it is validated
//! here before any connection is opened rather than by the proxy afterwards:
//! the proxy's rejection would arrive as an opaque failure, and a name that
//! encoded ambiguously would be worse than one that failed outright.
//!
//! Validation is deliberately narrower than DNS permits. Only what a Bitcoin
//! seed authority actually needs is accepted, because every additional accepted
//! shape is one more thing the proxy might parse differently than this node.
//!
//! See [`docs/NAME_PROXY_DISCOVERY.md`](../docs/NAME_PROXY_DISCOVERY.md) for
//! the surrounding design; this module implements only the name rule.

use std::fmt;
use std::net::IpAddr;

/// Longest name SOCKS5 can carry: the domain field is length-prefixed by one byte.
pub const MAX_SEED_NAME_LEN: usize = 255;

/// Longest single DNS label.
const MAX_LABEL_LEN: usize = 63;

/// Why one candidate name was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeedNameError {
    /// The name was empty.
    Empty,
    /// The name exceeded what a SOCKS5 domain field can carry.
    TooLong,
    /// The name contained a byte outside the permitted ASCII set.
    ForbiddenCharacter,
    /// The name contained an empty label, such as a leading or doubled dot.
    EmptyLabel,
    /// One label exceeded the DNS label ceiling.
    LabelTooLong,
    /// A label started or ended with a hyphen.
    HyphenAtLabelEdge,
    /// The name parsed as an IP address, which must use the literal form.
    IpLiteral,
    /// The name had no dot, so it is not a fully qualified seed authority.
    NotQualified,
}

impl fmt::Display for SeedNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Empty => "seed name is empty",
            Self::TooLong => "seed name exceeds the SOCKS5 domain field",
            Self::ForbiddenCharacter => "seed name contains a forbidden character",
            Self::EmptyLabel => "seed name contains an empty label",
            Self::LabelTooLong => "seed name contains an over-long label",
            Self::HyphenAtLabelEdge => "seed name has a hyphen at a label edge",
            Self::IpLiteral => "seed name is an IP literal, which must not use the domain form",
            Self::NotQualified => "seed name is not a qualified domain name",
        };
        formatter.write_str(message)
    }
}

/// One DNS seed authority accepted for submission to a name proxy.
///
/// Holding a validated type rather than a `String` means the encoding path
/// cannot be reached with a name that was never checked.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SeedName(String);

impl SeedName {
    /// Validates one candidate seed authority name.
    ///
    /// Accepts only lowercase-normalised ASCII letters, digits, hyphens, and
    /// dots, in at least two non-empty labels, with no hyphen at a label edge.
    /// A trailing root dot is accepted and removed, because a seed list may
    /// carry either form and the two must not reach the proxy as different
    /// names.
    ///
    /// # Errors
    ///
    /// Returns the specific rule the candidate broke, so a configuration error
    /// names what to fix rather than reporting a generic refusal.
    pub fn parse(candidate: &str) -> Result<Self, SeedNameError> {
        if candidate.is_empty() {
            return Err(SeedNameError::Empty);
        }
        // Checked before anything else: a non-ASCII name has no unambiguous
        // byte form here, and comparing or lowercasing it would already be a
        // decision about encoding this node must not make on the proxy's
        // behalf.
        if !candidate.is_ascii() {
            return Err(SeedNameError::ForbiddenCharacter);
        }
        let trimmed = candidate.strip_suffix('.').unwrap_or(candidate);
        if trimmed.is_empty() {
            return Err(SeedNameError::Empty);
        }
        if trimmed.len() > MAX_SEED_NAME_LEN {
            return Err(SeedNameError::TooLong);
        }
        // An IP literal reaching the domain form would ask the proxy to resolve
        // something that needs no resolution, and a proxy that accepted it
        // would silently bypass the IP-literal path's address checks.
        if trimmed.parse::<IpAddr>().is_ok() {
            return Err(SeedNameError::IpLiteral);
        }
        let mut labels = 0_usize;
        for label in trimmed.split('.') {
            labels += 1;
            if label.is_empty() {
                return Err(SeedNameError::EmptyLabel);
            }
            if label.len() > MAX_LABEL_LEN {
                return Err(SeedNameError::LabelTooLong);
            }
            if label.starts_with('-') || label.ends_with('-') {
                return Err(SeedNameError::HyphenAtLabelEdge);
            }
            if !label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            {
                return Err(SeedNameError::ForbiddenCharacter);
            }
        }
        if labels < 2 {
            return Err(SeedNameError::NotQualified);
        }
        Ok(Self(trimmed.to_ascii_lowercase()))
    }

    /// Returns the validated name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the SOCKS5 domain-field bytes, length prefix excluded.
    ///
    /// Always fits the single-byte prefix, because [`Self::parse`] bounds it.
    #[must_use]
    pub fn to_socks5_domain(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }
}

impl fmt::Display for SeedName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_seed_authorities_are_accepted_and_normalised() {
        for (candidate, expected) in [
            ("seed.bitcoin.sipa.be", "seed.bitcoin.sipa.be"),
            // A trailing root dot and mixed case must not reach the proxy as
            // names distinct from their canonical form.
            ("dnsseed.bluematt.me.", "dnsseed.bluematt.me"),
            ("SEED.BITCOINSTATS.COM", "seed.bitcoinstats.com"),
            ("seed-1.example.org", "seed-1.example.org"),
        ] {
            let name = SeedName::parse(candidate).expect(candidate);
            assert_eq!(name.as_str(), expected);
            assert_eq!(name.to_socks5_domain(), expected.as_bytes());
        }
    }

    #[test]
    fn an_ip_literal_never_takes_the_domain_form() {
        // Accepting these would ask the proxy to resolve something that needs
        // no resolution, and would route an address around the checks the IP
        // literal path applies.
        for candidate in ["127.0.0.1", "::1", "8.8.8.8", "2001:db8::1"] {
            assert_eq!(SeedName::parse(candidate), Err(SeedNameError::IpLiteral));
        }
    }

    #[test]
    fn injection_shapes_are_refused_before_any_connection() {
        for (candidate, expected) in [
            ("seed.example.com\0", SeedNameError::ForbiddenCharacter),
            ("seed.example.com/path", SeedNameError::ForbiddenCharacter),
            ("seed.example.com ", SeedNameError::ForbiddenCharacter),
            (" seed.example.com", SeedNameError::ForbiddenCharacter),
            ("seed.example.com\r\n", SeedNameError::ForbiddenCharacter),
            ("seed.example.com:8333", SeedNameError::ForbiddenCharacter),
            ("seed.exämple.com", SeedNameError::ForbiddenCharacter),
            ("seed@example.com", SeedNameError::ForbiddenCharacter),
        ] {
            assert_eq!(SeedName::parse(candidate), Err(expected), "{candidate}");
        }
    }

    #[test]
    fn malformed_label_structure_is_refused() {
        assert_eq!(SeedName::parse(""), Err(SeedNameError::Empty));
        assert_eq!(SeedName::parse("."), Err(SeedNameError::Empty));
        assert_eq!(
            SeedName::parse(".seed.example.com"),
            Err(SeedNameError::EmptyLabel)
        );
        assert_eq!(
            SeedName::parse("seed..example.com"),
            Err(SeedNameError::EmptyLabel)
        );
        assert_eq!(
            SeedName::parse("localhost"),
            Err(SeedNameError::NotQualified)
        );
        assert_eq!(
            SeedName::parse("-seed.example.com"),
            Err(SeedNameError::HyphenAtLabelEdge)
        );
        assert_eq!(
            SeedName::parse("seed-.example.com"),
            Err(SeedNameError::HyphenAtLabelEdge)
        );
        assert_eq!(
            SeedName::parse(&format!("{}.example.com", "a".repeat(MAX_LABEL_LEN + 1))),
            Err(SeedNameError::LabelTooLong)
        );
    }

    #[test]
    fn the_socks5_domain_field_bound_is_enforced() {
        // One byte prefixes the domain field, so a longer name could not be
        // encoded and must be refused rather than truncated.
        let longest = format!("{}.example.com", "a".repeat(MAX_LABEL_LEN));
        let fill = MAX_SEED_NAME_LEN - longest.len();
        let at_limit = format!("{}{longest}", "b.".repeat(fill / 2));
        assert!(at_limit.len() <= MAX_SEED_NAME_LEN);
        let accepted = SeedName::parse(&at_limit).expect("a name at the limit is usable");
        assert!(accepted.to_socks5_domain().len() <= MAX_SEED_NAME_LEN);

        let over = format!("c.{at_limit}");
        assert!(over.len() > MAX_SEED_NAME_LEN);
        assert_eq!(SeedName::parse(&over), Err(SeedNameError::TooLong));
    }

    #[test]
    fn every_accepted_name_encodes_within_one_length_prefix() {
        // The property the encoder depends on: parse is the only constructor,
        // so nothing can reach the wire that the prefix cannot describe.
        for candidate in [
            "a.bc",
            "seed.bitcoin.sipa.be",
            &format!("{}.example.com", "a".repeat(MAX_LABEL_LEN)),
        ] {
            let name = SeedName::parse(candidate).expect(candidate);
            assert!(u8::try_from(name.to_socks5_domain().len()).is_ok());
        }
    }
}
