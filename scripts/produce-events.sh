#!/bin/sh
# Produce N events to a marchiq topic. Event time is carried in the value as
# "<unix-nano>|1" because the broker stamps records with its own clock.
# Every 5th event is 2s earlier than its neighbors (out of order).
#
# Usage: produce-events.sh [broker] [topic] [count]
set -eu

broker="${1:-http://localhost:9092}"
topic="${2:-events}"
count="${3:-40}"

curl -sf -X POST "$broker/topics" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$topic\",\"partitions\":1}" >/dev/null || true
curl -sf -X POST "$broker/topics" \
  -H 'Content-Type: application/json' \
  -d '{"name":"counts","partitions":1}' >/dev/null || true

base=$(date +%s)
i=0
while [ "$i" -lt "$count" ]; do
  ts=$((base + i))
  if [ $((i % 5)) -eq 4 ]; then
    ts=$((ts - 2))
  fi
  key="key-$((i % 4))"
  curl -sf -X POST "$broker/produce?topic=$topic&partition=-1&key=$key" \
    --data "${ts}000000000|1" >/dev/null
  i=$((i + 1))
done

echo "produced $count events to $broker/topics/$topic"
