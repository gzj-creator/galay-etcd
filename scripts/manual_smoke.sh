#!/usr/bin/env bash
set -euo pipefail

ETCD_ENDPOINT="${1:-http://140.143.142.251:2379}"
KEY="/galay-etcd/test/$(date +%s)"
VAL="value-$(date +%s)"

K64="$(printf "%s" "$KEY" | base64)"
V64="$(printf "%s" "$VAL" | base64)"

echo "ETCD_ENDPOINT=$ETCD_ENDPOINT"
echo "KEY=$KEY"
echo

echo "[1/8] PUT"
curl -sS -m 5 -X POST "$ETCD_ENDPOINT/v3/kv/put" \
  -H "Content-Type: application/json" \
  -d "{\"key\":\"$K64\",\"value\":\"$V64\"}"
echo
echo

echo "[2/8] GET"
curl -sS -m 5 -X POST "$ETCD_ENDPOINT/v3/kv/range" \
  -H "Content-Type: application/json" \
  -d "{\"key\":\"$K64\"}"
echo
echo

echo "[3/8] DELETE"
curl -sS -m 5 -X POST "$ETCD_ENDPOINT/v3/kv/deleterange" \
  -H "Content-Type: application/json" \
  -d "{\"key\":\"$K64\"}"
echo
echo

echo "[4/8] LEASE GRANT"
LRESP="$(curl -sS -m 5 -X POST "$ETCD_ENDPOINT/v3/lease/grant" \
  -H "Content-Type: application/json" \
  -d '{"TTL":4}')"
echo "$LRESP"
echo

LID="$(echo "$LRESP" | awk -F'"ID":"' '{print $2}' | awk -F'"' '{print $1}')"
if [[ -z "$LID" ]]; then
  echo "Failed to parse lease ID from response"
  exit 1
fi
echo "LEASE_ID=$LID"
echo

echo "[5/8] PUT WITH LEASE"
curl -sS -m 5 -X POST "$ETCD_ENDPOINT/v3/kv/put" \
  -H "Content-Type: application/json" \
  -d "{\"key\":\"$K64\",\"value\":\"$V64\",\"lease\":\"$LID\"}"
echo
echo

echo "[6/8] KEEPALIVE ONCE"
curl -sS -m 5 -X POST "$ETCD_ENDPOINT/v3/lease/keepalive" \
  -H "Content-Type: application/json" \
  -d "{\"ID\":\"$LID\"}"
echo
echo

echo "[7/8] GET AFTER LEASE PUT"
curl -sS -m 5 -X POST "$ETCD_ENDPOINT/v3/kv/range" \
  -H "Content-Type: application/json" \
  -d "{\"key\":\"$K64\"}"
echo
echo

echo "[8/8] SLEEP 6s AND GET AGAIN"
sleep 6
curl -sS -m 5 -X POST "$ETCD_ENDPOINT/v3/kv/range" \
  -H "Content-Type: application/json" \
  -d "{\"key\":\"$K64\"}"
echo
