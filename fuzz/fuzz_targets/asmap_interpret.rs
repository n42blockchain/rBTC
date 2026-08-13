#![no_main]

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use libfuzzer_sys::fuzz_target;
use rbtc::asmap::Asmap;

fuzz_target!(|input: &[u8]| {
    if input.len() < 16 {
        return;
    }
    let (map_bytes, ip_bytes) = input.split_at(input.len() - 16);
    let ip_octets: [u8; 16] = ip_bytes.try_into().expect("fixed address suffix");
    let v6 = IpAddr::V6(Ipv6Addr::from(ip_octets));
    let v4 = IpAddr::V4(Ipv4Addr::new(
        ip_octets[0],
        ip_octets[1],
        ip_octets[2],
        ip_octets[3],
    ));
    // The interpreter must terminate without panicking on arbitrary bytes
    // that never passed validation, for both address families.
    let _ = Asmap::interpret_unvalidated(map_bytes, v6);
    let _ = Asmap::interpret_unvalidated(map_bytes, v4);
    // Validation must itself be total, and a map it accepts must answer
    // the same as the unvalidated walk over identical bytes.
    if let Ok(map) = Asmap::from_bytes(map_bytes.to_vec()) {
        assert_eq!(map.map_asn(v6), Asmap::interpret_unvalidated(map_bytes, v6));
        assert_eq!(map.map_asn(v4), Asmap::interpret_unvalidated(map_bytes, v4));
    }
});
