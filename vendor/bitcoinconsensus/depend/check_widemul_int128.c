/* Asserts that libsecp256k1 selects its 64-bit-limb arithmetic.
 *
 * `util.h` derives SECP256K1_WIDEMUL_INT128 itself: from a native __int128
 * where one exists, and otherwise from the `int128_struct` intrinsic path on
 * 64-bit MSVC targets. That macro alone drives the field (5x52 vs 10x26) and
 * scalar (4x64 vs 8x32) implementations, so compiling this file proves the
 * fast path was chosen without depending on the toolchain's __int128 support.
 */

#include "util.h"

#if !defined(SECP256K1_WIDEMUL_INT128)
#error "libsecp256k1 fell back to 32-bit limb arithmetic"
#endif

int main(void) { return 0; }
