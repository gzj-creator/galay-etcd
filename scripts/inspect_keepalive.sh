#!/usr/bin/env bash
set -euo pipefail

ETCD_ENDPOINT="${1:-http://140.143.142.251:2379}"

LRESP="$(curl -sS -m 5 -X POST "$ETCD_ENDPOINT/v3/lease/grant" \
  -H "Content-Type: application/json" \
  -d '{"TTL":5}')"

LID="$(echo "$LRESP" | awk -F'"ID":"' '{print $2}' | awk -F'"' '{print $1}')"
echo "LEASE_ID=$LID"
echo

echo "--- keepalive with Connection: keep-alive ---"
curl -sS -m 5 -D - -X POST "$ETCD_ENDPOINT/v3/lease/keepalive" \
  -H "Content-Type: application/json" \
  -H "Connection: keep-alive" \
  -d "{\"ID\":\"$LID\"}"
echo
echo

echo "--- keepalive with Connection: close ---"
curl -sS -m 5 -D - -X POST "$ETCD_ENDPOINT/v3/lease/keepalive" \
  -H "Content-Type: application/json" \
  -H "Connection: close" \
  -d "{\"ID\":\"$LID\"}"
echo
