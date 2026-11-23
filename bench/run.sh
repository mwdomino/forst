#!/usr/bin/env bash
# Run each benchmark scenario against a fresh server.
# Usage: bench/run.sh [path-to-server-binary]
set -euo pipefail
cd "$(dirname "$0")"
server=${1:-../target/release/server}
go build -o ../target/gobench .

scenarios=(
  "legacy 1conn/20 sets|-conns 1 -c 20"
  "16conn/256 sets|-conns 16 -c 256"
  "16conn/256 sets ttl=1s (reaper under load)|-conns 16 -c 256 -ttl 1"
  "16conn/256 80% get|-conns 16 -c 256 -get 80 -prefill -keys 100000"
  "16conn/256 get+query|-conns 16 -c 256 -get 70 -query 10 -prefill -keys 100000 -n 100000"
)

for s in "${scenarios[@]}"; do
  "$server" >/dev/null 2>&1 &
  pid=$!
  sleep 0.5
  echo "== ${s%%|*}"
  # shellcheck disable=SC2086
  ../target/gobench ${s#*|} 2>/dev/null | tail -2
  echo "server cpu: $(ps -o time= -p $pid)"
  kill -INT $pid; wait $pid || true
done
