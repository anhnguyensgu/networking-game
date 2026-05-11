#!/opt/homebrew/bin/bash
set -euo pipefail

BENCH=./bin/bench
CLIENTS=500
WARMUP_MS=3000
DURATION_MS=10000
TIMEOUT_MS=2000
FD_LIMIT=4096

NAMES=(odin go zig rust)
BINS=(./bin/server-odin ./bin/server-go ./bin/server-zig ./bin/server-rust)

run_bench() {
  local name=$1 binary=$2
  echo "=== $name ==="
  ulimit -n $FD_LIMIT
  "$binary" &
  local pid=$!
  sleep 1.0

  echo "Warmup..."
  ulimit -n $FD_LIMIT
  "$BENCH" --clients "$CLIENTS" --duration-ms "$WARMUP_MS" --timeout-ms "$TIMEOUT_MS" > /dev/null

  echo "Benchmarking..."
  ulimit -n $FD_LIMIT
  "$BENCH" --clients "$CLIENTS" --duration-ms "$DURATION_MS" --timeout-ms "$TIMEOUT_MS"

  kill "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null || true
  sleep 0.5
  echo
}

for i in "${!NAMES[@]}"; do
  run_bench "${NAMES[$i]}" "${BINS[$i]}"
done
