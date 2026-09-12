#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
report=${1:-"$repo_root/scripts/public-network-soak-report.sh"}
fixture=$(mktemp -d "${TMPDIR:-/tmp}/rbtc-soak-report.XXXXXX")
trap 'rm -rf "$fixture"' EXIT

python3 - "$fixture" <<'PY'
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
root = Path(sys.argv[1])
(root / 'state').mkdir()
(root / 'metrics').mkdir()
binary = root / 'state/rbtcd-soak-start'
binary.touch()
binary.chmod(0o755)
start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=1)
def stamp(minute):
    return (start + timedelta(minutes=minute)).strftime('%Y-%m-%dT%H:%M:%SZ')
(root / 'state/baseline.txt').write_text(
    'started_utc=' + stamp(0) + '\n'
    'commit=0123456789abcdef0123456789abcdef01234567\n'
    'binary_sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n')
headers = {
 'process.tsv': 'timestamp_utc\tnetwork\tpid\telapsed\trss_kib\tcpu_percent',
 'disk.tsv': 'timestamp_utc\tnetwork\tdata_kib\tfilesystem_free_kib',
 'peers.tsv': 'timestamp_utc\tnetwork\tpid\tremote_endpoint',
 'tips.tsv': 'timestamp_utc\tnetwork\theader_height\theader_hash\texecution_height',
 'freezer.tsv': 'timestamp_utc\tnetwork\tnext_slot\tsegments\tfirst_height\tlast_height\tbytes',
 'persistent.tsv': 'timestamp_utc\tnetwork\tmempool_bytes\tpeer_store_bytes',
}
rows = {name:[header] for name, header in headers.items()}
events = []
for network in ('bitcoin', 'testnet4'):
 mode = 'graceful' if network == 'bitcoin' else 'abrupt'
 for minute in range(61):
  prefix = f'{stamp(minute)}\t{network}\t'
  pid = 101 if minute < 30 else 202
  height = 100 + minute
  rows['process.tsv'].append(prefix + f'{pid}\t1:00\t1000\t1.0')
  rows['tips.tsv'].append(prefix + f'{height}\t{height:064x}\t{height}')
  rows['freezer.tsv'].append(prefix + f'{10+minute}\t3\t1\t{height}\t{1000+minute}')
  rows['persistent.tsv'].append(prefix + '4096\t8192')
  rows['disk.tsv'].append(prefix + f'{1000+minute}\t9000')
  for group in range(1,5):
   rows['peers.tsv'].append(prefix + f'{pid}\t10.{group}.0.1:8333')
 events.extend([
  f'{stamp(29)}\t{network}\tscenario=controlled-restart mode={mode} status=started old_pid=101',
  f'{stamp(30)}\t{network}\tscenario=controlled-restart mode={mode} status=completed old_pid=101 new_pid=202 duration_seconds=60',
 ])
 if network == 'testnet4':
  events.append(f'{stamp(30)}\t{network}\tscenario=fault-abrupt-kill status=completed old_pid=101 new_pid=202 duration_seconds=60')
for name, values in rows.items():
 (root / 'metrics' / name).write_text('\n'.join(values) + '\n')
(root / 'metrics/events.log').write_text('\n'.join(events) + '\n')
PY

"$report" "$fixture" 1 >"$fixture/report.md"
grep -q -- '- Duration status: `PASS`' "$fixture/report.md"
grep -q -- '- Fault scenarios completed: `1`' "$fixture/report.md"
cp -R "$fixture/metrics" "$fixture/pristine"
cp "$fixture/state/baseline.txt" "$fixture/baseline-original.txt"

reject() {
  if "$report" "$fixture" 1 >"$fixture/rejected.md" 2>&1; then
    echo "soak report accepted $1" >&2
    exit 1
  fi
}

sed 's/old_pid=101/old_pid=999/g' "$fixture/pristine/events.log" >"$fixture/metrics/events.log"
reject 'an unobserved restart PID'
sed 's/duration_seconds=60/duration_seconds=999/g' "$fixture/pristine/events.log" >"$fixture/metrics/events.log"
reject 'a restart duration inconsistent with its timestamps'
sed '/scenario=fault-/s/new_pid=202/new_pid=999/' "$fixture/pristine/events.log" >"$fixture/metrics/events.log"
reject 'a fault unrelated to the completed restart'
sed 's/status=completed/status=failed status=completed/' "$fixture/pristine/events.log" >"$fixture/metrics/events.log"
reject 'duplicate event status fields'
cp "$fixture/pristine/events.log" "$fixture/metrics/events.log"

awk -F '\t' 'BEGIN {OFS="\t"} NR > 1 {$4=0} {print}' \
  "$fixture/pristine/persistent.tsv" >"$fixture/metrics/persistent.tsv"
reject 'an empty peer store'
cp "$fixture/pristine/persistent.tsv" "$fixture/metrics/persistent.tsv"

awk -F '\t' 'BEGIN {OFS="\t"} NR == 2 {$3=""; $5=""} {print}' \
  "$fixture/pristine/tips.tsv" >"$fixture/metrics/tips.tsv"
reject 'empty matching baseline tip fields'
cp "$fixture/pristine/tips.tsv" "$fixture/metrics/tips.tsv"

# A calendar-old baseline plus a few samples used to be accepted.
awk -F '\t' 'NR == 1 || !seen[$2]++ || $4 == "1:00" && seen[$2] == 61' \
  "$fixture/pristine/process.tsv" >"$fixture/metrics/process.tsv"
reject 'sparse live-process coverage'
RBTC_SOAK_ALLOW_INCOMPLETE=1 "$report" "$fixture" 1 >"$fixture/progress.md"
grep -q -- '- Acceptance status: `INCOMPLETE`' "$fixture/progress.md"
cp "$fixture/pristine/process.tsv" "$fixture/metrics/process.tsv"

for scenario in future regressing; do
  python3 - "$fixture" "$scenario" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[1])
rows = (root/'pristine/process.tsv').read_text().splitlines()
if sys.argv[2] == 'future':
    rows[1] = '2099-01-01T00:00:00Z\t' + rows[1].split('\t', 1)[1]
else:
    rows[10], rows[11] = rows[11], rows[10]
(root/'metrics/process.tsv').write_text('\n'.join(rows)+'\n')
PY
  reject "$scenario process timestamps"
done
cp "$fixture/pristine/process.tsv" "$fixture/metrics/process.tsv"

printf 'tampered\n' >"$fixture/state/rbtcd-soak-start"
reject 'a modified baseline binary'
: >"$fixture/state/rbtcd-soak-start"

awk -F '\t' 'BEGIN {OFS="\t"} NR > 1 {$5=99} {print}' \
  "$fixture/pristine/tips.tsv" >"$fixture/metrics/tips.tsv"
reject 'an inconsistent execution tip'
cp "$fixture/pristine/tips.tsv" "$fixture/metrics/tips.tsv"

sed 's/^started_utc=.*/started_utc=2000-01-01T00:00:00Z/' \
  "$fixture/baseline-original.txt" >"$fixture/state/baseline.txt"
reject 'a baseline predating the actual samples'
cp "$fixture/baseline-original.txt" "$fixture/state/baseline.txt"

head -n 2 "$fixture/pristine/persistent.tsv" >"$fixture/metrics/persistent.tsv"
reject 'stale or absent persistent-state evidence'
cp "$fixture/pristine/persistent.tsv" "$fixture/metrics/persistent.tsv"

sed 's/status=completed/status=failed/g' "$fixture/pristine/events.log" >"$fixture/metrics/events.log"
reject 'a failed recovery event'
cp "$fixture/pristine/events.log" "$fixture/metrics/events.log"

# A bounded, paired controlled restart can explain a process sampling gap.
python3 - "$fixture" <<'PY'
from pathlib import Path
import sys
root = Path(sys.argv[1])
rows = (root/'pristine/process.tsv').read_text().splitlines()
counts = {}
kept = [rows[0]]
for row in rows[1:]:
 network = row.split('\t')[1]
 minute = counts.get(network, 0)
 counts[network] = minute + 1
 if not 28 <= minute <= 31:
  kept.append(row)
(root/'metrics/process.tsv').write_text('\n'.join(kept)+'\n')
PY
"$report" "$fixture" 1 >"$fixture/planned-restart.md"
# Removing just the start event must prevent a claimed completion excusing a gap.
sed '/status=started/d' "$fixture/pristine/events.log" >"$fixture/metrics/events.log"
reject 'restart completion without its start'
cp "$fixture/pristine/events.log" "$fixture/metrics/events.log"
cp "$fixture/pristine/process.tsv" "$fixture/metrics/process.tsv"

if "$report" "$fixture" 604800 >"$fixture/short-run.md" 2>&1; then
  echo 'soak report accepted one hour as seven days' >&2
  exit 1
fi

# Exercise the formal seven-day branch with generated evidence, not elapsed
# wall time. This remains a fixture test, never public-network acceptance.
python3 - "$fixture" <<'PY'
from datetime import datetime, timedelta, timezone
from pathlib import Path
from contextlib import ExitStack
import sys
root = Path(sys.argv[1])
start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=7)
def stamp(minute):
    return (start + timedelta(minutes=minute)).strftime('%Y-%m-%dT%H:%M:%SZ')
baseline = (root/'state/baseline.txt').read_text().splitlines()
baseline[0] = 'started_utc=' + stamp(0)
(root/'state/baseline.txt').write_text('\n'.join(baseline)+'\n')
with ExitStack() as stack:
    streams = {}
    for path in (root/'pristine').glob('*.tsv'):
        stream = stack.enter_context((root/'metrics'/path.name).open('w'))
        stream.write(path.read_text().splitlines()[0]+'\n')
        streams[path.name] = stream
    for minute in range(10081):
        for network in ('bitcoin', 'testnet4'):
            prefix = f'{stamp(minute)}\t{network}\t'
            pid = 101 if minute < 1442 else 202
            streams['process.tsv'].write(prefix+f'{pid}\t1:00\t1000\t1.0\n')
            if minute % 5 == 0:
                height = 100 + minute
                streams['tips.tsv'].write(prefix+f'{height}\t{height:064x}\t{height}\n')
                streams['freezer.tsv'].write(prefix+f'{10+minute}\t3\t1\t{height}\t{1000+minute}\n')
                streams['persistent.tsv'].write(prefix+'4096\t8192\n')
                for group in range(1,5):
                    streams['peers.tsv'].write(prefix+f'{pid}\t10.{group}.0.1:8333\n')
            if minute % 60 == 0:
                streams['disk.tsv'].write(prefix+f'{1000+minute}\t9000\n')
events = []
for network, mode in (('bitcoin','graceful'), ('testnet4','abrupt')):
    events.append(f'{stamp(1441)}\t{network}\tscenario=controlled-restart mode={mode} status=started old_pid=101')
    events.append(f'{stamp(1442)}\t{network}\tscenario=controlled-restart mode={mode} status=completed old_pid=101 new_pid=202 duration_seconds=60')
events.append(f'{stamp(1442)}\ttestnet4\tscenario=fault-abrupt-kill status=completed old_pid=101 new_pid=202 duration_seconds=60')
(root/'metrics/events.log').write_text('\n'.join(events)+'\n')
PY
"$report" "$fixture" 604800 >"$fixture/generated-week.md"
grep -q -- '- Acceptance status: `PASS`' "$fixture/generated-week.md"

echo 'public-network soak report tests passed'
