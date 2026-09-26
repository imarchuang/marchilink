#!/bin/sh
# Graduation demo: real marchiq, windowed count, kill -9, restart.
#
# Expects `docker compose up -d marchiq` already healthy on localhost:9092.
# The processor binary is built and killed directly (go run's child would
# survive kill -9 of the parent). Checkpoints live in $DATA_DIR.
#
# PASS: a checkpoint exists before the kill, and after restart every window
# result in the output topic still appears exactly once. Results flushed to
# the sink in the gap after the last checkpoint can repeat; the script counts
# them.
set -eu

cd "$(dirname "$0")/.."

broker="${BROKER:-http://localhost:9092}"
data="${DATA_DIR:-./demo-data}"
count="${COUNT:-40}"
bin="${BIN:-/tmp/marchilink-demo-bin}"

rm -rf "$data"
mkdir -p "$data"

echo "== build =="
go build -o "$bin" ./cmd/marchilink

echo "== produce $count out-of-order events =="
./scripts/produce-events.sh "$broker" events "$count"

start_job() {
  "$bin" \
    -job windowed-count \
    -broker "$broker" \
    -topic events \
    -out-topic counts \
    -group flink \
    -window 10s \
    -watermark-bound 3s \
    -checkpoint 2s \
    -data-dir "$data" \
    -http :9081 \
    -parallelism 1 >"$data/run.log" 2>&1 &
  echo $!
}

echo "== start processor =="
pid=$(start_job)
trap 'kill -9 "$pid" 2>/dev/null || true' EXIT

echo "== wait for a completed checkpoint =="
latest="$data/checkpoints/windowed-count/LATEST"
i=0
while [ "$i" -lt 30 ]; do
  if [ -f "$latest" ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
if [ ! -f "$latest" ]; then
  echo "FAIL: no checkpoint written" >&2
  exit 1
fi
echo "LATEST=$(cat "$latest")"
# Let the commit that follows the snapshot land.
sleep 1
echo "lag before kill:"
curl -sf "$broker/debug/lag?group=flink" || true
echo
before=$(curl -sf "$broker/fetch?topic=counts&partition=0&offset=0&max_records=500")

echo "== kill -9 pid $pid =="
kill -9 "$pid" 2>/dev/null || true
wait "$pid" 2>/dev/null || true
trap - EXIT

echo "== restart =="
pid=$(start_job)
trap 'kill -9 "$pid" 2>/dev/null || true' EXIT

echo "== wait until group lag is 0 =="
i=0
while [ "$i" -lt 30 ]; do
  lag=$(curl -sf "$broker/debug/lag?group=flink" || true)
  echo "$lag"
  echo "$lag" | grep -q '"lag":0' && break
  i=$((i + 1))
  sleep 1
done

echo "== output topic =="
after=$(curl -sf "$broker/fetch?topic=counts&partition=0&offset=0&max_records=500")
echo "$after"
echo

python3 - "$before" "$after" <<'PY'
import json, sys, base64, collections
def values(raw):
    doc = json.loads(raw)
    out = []
    for rec in doc.get("records", []):
        key = base64.b64decode(rec["key"]).decode()
        val = base64.b64decode(rec["value"]).decode()
        out.append(f"{key} {val}")
    return out
before, after = values(sys.argv[1]), values(sys.argv[2])
print(f"results before kill: {len(before)}")
print(f"results after restart: {len(after)}")
counts = collections.Counter(after)
dups = {k: v for k, v in counts.items() if v > 1}
if dups:
    print("DUPLICATES:")
    for k, v in sorted(dups.items()):
        print(f"  {v}x {k}")
    sys.exit(1)
print("PASS: every window result appears once")
PY

kill -9 "$pid" 2>/dev/null || true
wait "$pid" 2>/dev/null || true
trap - EXIT
echo "== done =="
