extern crate cc;

use std::env;

fn main() {
    let sixty_four_bit_target = env::var("CARGO_CFG_TARGET_POINTER_WIDTH").unwrap() == "64";
    let native_int128 = sixty_four_bit_target
        && cc::Build::new()
            .file("depend/check_uint128_t.c")
            .cargo_metadata(false)
            .try_compile("check_uint128_t")
            .is_ok();
    let target = env::var("TARGET").expect("TARGET was not set");
    let is_big_endian = env::var("CARGO_CFG_TARGET_ENDIAN").expect("No endian is set") == "big";
    let mut base_config = cc::Build::new();
    base_config
        .include("depend/bitcoin/src/secp256k1/include")
        .define("__STDC_FORMAT_MACROS", None)
        .flag_if_supported("-Wno-implicit-fallthrough");

    if target.contains("windows") {
        base_config.define("WIN32", "1");
        // libsecp256k1 is linked statically here. Without this define its
        // public headers declare every entry point `__declspec(dllimport)`,
        // so MSVC emits unresolvable `__imp_secp256k1_*` references from the
        // C++ consensus sources.
        base_config.define("SECP256K1_STATIC", "1");
    }

    let mut secp_config = base_config.clone();
    let mut consensus_config = base_config;

    // **Secp256k1**
    if !cfg!(feature = "external-secp") {
        secp_config
            .include("depend/bitcoin/src/secp256k1/include")
            .include("depend/bitcoin/src/secp256k1/src")
            .flag_if_supported("-Wno-unused-function") // some ecmult stuff is defined but not used upstream
            .define("ECMULT_WINDOW_SIZE", "15")
            .define("ECMULT_GEN_PREC_BITS", "4")
            .define("ENABLE_MODULE_SCHNORRSIG", "1")
            .define("ENABLE_MODULE_EXTRAKEYS", "1")
            // `pubkey.cpp` compiles `EllSwiftPubKey::Decode`, which references
            // `secp256k1_ellswift_decode`. Without this module the symbol is
            // never emitted and the final link fails.
            .define("ENABLE_MODULE_ELLSWIFT", "1")
            // Technically libconsensus doesn't require the recovery feautre, but `pubkey.cpp` does.
            .define("ENABLE_MODULE_RECOVERY", "1")
            .file("depend/bitcoin/src/secp256k1/src/precomputed_ecmult_gen.c")
            .file("depend/bitcoin/src/secp256k1/src/precomputed_ecmult.c")
            .file("depend/bitcoin/src/secp256k1/src/secp256k1.c");

        if is_big_endian {
            secp_config.define("WORDS_BIGENDIAN", "1");
        }

        // This libsecp256k1 no longer reads USE_FIELD_*/USE_SCALAR_* at all; the
        // field (5x52 vs 10x26) and scalar (4x64 vs 8x32) implementations are
        // both selected by SECP256K1_WIDEMUL_INT128, which `util.h` derives on
        // its own. It picks the 64-bit limbs from a native __int128 where one
        // exists and otherwise from the `int128_struct` intrinsic path on
        // 64-bit MSVC, so the only thing worth doing here is asserting that a
        // 64-bit target really did get them.
        if native_int128 {
            secp_config.define("HAVE___INT128", "1");
        }
        if sixty_four_bit_target
            && secp_config
                .clone()
                .file("depend/check_widemul_int128.c")
                .cargo_metadata(false)
                .try_compile("check_widemul_int128")
                .is_err()
        {
            println!(
                "cargo:warning=libsecp256k1 fell back to 32-bit limb arithmetic on a 64-bit target; signature verification will be substantially slower."
            );
        }

        secp_config.compile("libsecp256k1.a");
    }

    let tool = consensus_config.get_compiler();
    if tool.is_like_msvc() {
        consensus_config.flag("/std:c++17").flag("/wd4100");
    } else if tool.is_like_clang() || tool.is_like_gnu() {
        consensus_config.flag("-std=c++17").flag("-Wno-unused-parameter");
    }

    consensus_config
        .cpp(true)
        .include("depend/bitcoin/src")
        .include("depend/bitcoin/src/secp256k1/include")
        .file("depend/bitcoin/src/util/strencodings.cpp")
        .file("depend/bitcoin/src/uint256.cpp")
        .file("depend/bitcoin/src/pubkey.cpp")
        .file("depend/bitcoin/src/hash.cpp")
        .file("depend/bitcoin/src/primitives/transaction.cpp")
        .file("depend/bitcoin/src/crypto/ripemd160.cpp")
        .file("depend/bitcoin/src/crypto/sha1.cpp")
        .file("depend/bitcoin/src/crypto/sha256.cpp")
        .file("depend/bitcoin/src/crypto/sha512.cpp")
        .file("depend/bitcoin/src/crypto/hmac_sha512.cpp")
        .file("depend/bitcoin/src/script/bitcoinconsensus.cpp")
        .file("depend/bitcoin/src/script/interpreter.cpp")
        .file("depend/bitcoin/src/script/script.cpp")
        .file("depend/bitcoin/src/script/script_error.cpp")
        .compile("libbitcoinconsensus.a");
}
