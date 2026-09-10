#!/usr/bin/env bash
# Reproduce the upstream follow-up gates without treating partial runs as acceptance.
set -euo pipefail

mode="${1:-local}"
case "$mode" in
    standalone|local|core) ;;
    *) echo "Usage: bash scripts/check-upstream-followup.sh [standalone|local|core]" >&2; exit 2 ;;
esac
if (( $# > 1 )); then
    echo "Expected at most one mode argument" >&2
    exit 2
fi
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
mkdir -p target/upstream-followup
report_dir="$(mktemp -d "$repo_root/target/upstream-followup/run.XXXXXXXX")"
summary="$report_dir/summary.tsv"
printf 'gate\tresult\texit_code\n' > "$summary"
printf 'Reports: %s\n' "$report_dir"
failed=0
cargo_ready=0

run_gate() {
    local name="$1"
    shift
    local result=0
    printf 'Running %s\n' "$name"
    if "$@" > "$report_dir/$name.log" 2>&1; then
        printf '%s\tPASS\t0\n' "$name" >> "$summary"
    else
        result=$?
        printf '%s\tFAIL\t%s\n' "$name" "$result" >> "$summary"
        tail -n 12 "$report_dir/$name.log" >&2
        failed=1
    fi
    return "$result"
}

{
    date -u '+%Y-%m-%dT%H:%M:%SZ'
    git rev-parse HEAD
    git status --short
    rustc --version
    cargo --version
    printf 'mode=%s\nCARGO_HOME=%s\n' "$mode" "${CARGO_HOME:-default}"
} > "$report_dir/environment.log"

for module in feerate_diagram script_queue psbt_envelope; do
    if run_gate "$module-build" rustc --edition=2024 --test "src/$module.rs" -o "$report_dir/$module"; then
        run_gate "$module-test" "$report_dir/$module" || :
    fi
done
run_gate format cargo fmt --all -- --check || :

if [[ "$mode" != standalone ]]; then
    # Compilation is a separate gate: never report a dependency failure as a test failure.
    if run_gate cargo-build cargo test --locked --lib --tests --no-run; then
        cargo_ready=1
        run_gate cargo-tests cargo test --locked --lib --tests --no-fail-fast || :
    else
        printf 'cargo-tests\tNOT_RUN\t-\n' >> "$summary"
    fi
else
    printf 'cargo-tests\tNOT_RUN\t-\n' >> "$summary"
fi

if [[ "$mode" == core ]]; then
    if (( cargo_ready == 0 )); then
        printf 'core-differentials\tNOT_RUN\t-\n' >> "$summary"
        failed=1
    elif [[ -n "${RBTC_BITCOIND:-}" && -x "$RBTC_BITCOIND" && -x "$(dirname -- "$RBTC_BITCOIND")/bitcoin-cli" ]]; then
        mkdir -p "$report_dir/core-version-data"
        run_gate core-version "$RBTC_BITCOIND" "-datadir=$report_dir/core-version-data" -version || :
        run_gate core-block cargo test --locked --release --test core_block_differential -- --ignored --nocapture || :
        run_gate core-replacement cargo test --locked --release --test core_replacement_differential -- --ignored --nocapture || :
    else
        printf 'core-differentials\tNOT_RUN\t-\n' >> "$summary"
        echo "Core mode requires RBTC_BITCOIND and an executable bitcoin-cli beside it." >&2
        failed=1
    fi
else
    printf 'core-differentials\tNOT_RUN\t-\n' >> "$summary"
fi
cat "$summary"
echo "Scope: $mode. Ignored real Tor/I2P tests, MDBX, fuzz, public-network soak and resource-bound acceptance remain separate gates."
exit "$failed"
