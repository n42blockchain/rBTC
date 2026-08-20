#!/bin/sh
set -eu

# This is a storage-only, deterministic 900,000-transition target. It is not a
# mainnet block replay; see README.md for the separate corpus gate.
module_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_dir=$(CDPATH= cd -- "$module_dir/../.." && pwd)
run_stamp=$(date -u '+%Y%m%dT%H%M%SZ')
report_root=${RBTC_MATRIX_REPORT_DIR:-"$module_dir/../../target/btcd-storage-matrix-$run_stamp"}
seconds_per_lane=${RBTC_MATRIX_SECONDS_PER_LANE:-450}
engines=${RBTC_MATRIX_ENGINES:-"leveldb pebble badger bbolt"}
scenarios=${RBTC_MATRIX_SCENARIOS:-"serving ibd-256"}

case "$seconds_per_lane" in
	''|*[!0-9]*) echo "RBTC_MATRIX_SECONDS_PER_LANE must be an integer" >&2; exit 2 ;;
esac
if [ "$seconds_per_lane" -lt 1 ]; then
	echo "RBTC_MATRIX_SECONDS_PER_LANE must be positive" >&2
	exit 2
fi

for command_name in go git jq ps; do
	if ! command -v "$command_name" >/dev/null 2>&1; then
		echo "required command is missing: $command_name" >&2
		exit 2
	fi
done
if [ -e "$report_root" ]; then
	echo "report directory already exists: $report_root" >&2
	exit 2
fi
mkdir -p "$report_root"
binary="$report_root/btcd-storage-bench"
(cd "$module_dir" && go build -trimpath -o "$binary" .)
revision=$(git -C "$repository_dir" rev-parse HEAD)
if [ -n "$(git -C "$repository_dir" status --porcelain)" ]; then
	source_dirty=true
else
	source_dirty=false
fi
if command -v shasum >/dev/null 2>&1; then
	binary_sha256=$(shasum -a 256 "$binary" | awk '{print $1}')
else
	binary_sha256=$(sha256sum "$binary" | awk '{print $1}')
fi

lane_pid=
stop_lane() {
	if [ -n "$lane_pid" ] && kill -0 "$lane_pid" 2>/dev/null; then
		kill "$lane_pid" 2>/dev/null || true
		wait "$lane_pid" 2>/dev/null || true
	fi
}
trap stop_lane EXIT HUP INT TERM

started_epoch=$(date +%s)
for engine in $engines; do
	for scenario in $scenarios; do
		case "$engine" in leveldb|pebble|badger|bbolt) ;; *) echo "unsupported engine: $engine" >&2; exit 2 ;; esac
		case "$scenario" in serving|ibd-256) ;; *) echo "unsupported scenario: $scenario" >&2; exit 2 ;; esac
		name="$engine-$scenario"
		echo "running $name (mutation budget ${seconds_per_lane}s)" >&2
		(
			RBTC_ENGINE_BENCH_UTXOS=2000000 \
			RBTC_ENGINE_BENCH_BLOCKS=900000 \
			RBTC_ENGINE_BENCH_UPDATES=5000 \
			RBTC_ENGINE_BENCH_LOOKUPS=500000 \
			RBTC_ENGINE_BENCH_MAX_SECONDS="$seconds_per_lane" \
			RBTC_BTCD_ENGINES="$engine" \
			RBTC_BTCD_SCENARIOS="$scenario" \
			RBTC_ENGINE_BENCH_REPORT="$report_root/$name.json" \
			"$binary"
		) >"$report_root/$name.stdout" 2>"$report_root/$name.stderr" &
		lane_pid=$!
		peak_rss_kib=0
		while kill -0 "$lane_pid" 2>/dev/null; do
			rss_kib=$(ps -p "$lane_pid" -o rss= 2>/dev/null | tr -d ' ')
			case "$rss_kib" in ''|*[!0-9]*) ;; *)
				if [ "$rss_kib" -gt "$peak_rss_kib" ]; then
					peak_rss_kib=$rss_kib
				fi
			esac
			sleep 1
		done
		if ! wait "$lane_pid"; then
			echo "$name failed before producing a report" >&2
			exit 1
		fi
		lane_pid=
		jq --argjson peak_rss_kib "$peak_rss_kib" \
			'. + {observed_peak_rss_kib: $peak_rss_kib}' \
			"$report_root/$name.json" >"$report_root/$name.json.next"
		mv "$report_root/$name.json.next" "$report_root/$name.json"
	done
done
finished_epoch=$(date +%s)

jq -s \
	--argjson started "$started_epoch" \
	--argjson finished "$finished_epoch" \
	--argjson seconds_per_lane "$seconds_per_lane" \
	--arg revision "$revision" \
	--argjson source_dirty "$source_dirty" \
	--arg binary_sha256 "$binary_sha256" \
	'{
	  schema_version: 1,
	  boundary: "storage-only synthetic chainstate; not mainnet blocks, validation, btcd cache, or complete IBD",
	  target_transitions: 900000,
	  mutation_seconds_per_lane: $seconds_per_lane,
	  source_revision: $revision,
	  source_dirty: $source_dirty,
	  binary_sha256: $binary_sha256,
	  started_epoch: $started,
	  finished_epoch: $finished,
	  wall_seconds: ($finished - $started),
	  reports: .
	}' "$report_root"/*.json \
	>"$report_root/matrix.json"

trap - EXIT HUP INT TERM
echo "$report_root/matrix.json"
