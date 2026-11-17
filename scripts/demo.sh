#!/usr/bin/env bash
# End-to-end demo of the PITR recovery flow:
#   collect -> restore point -> accidental delete -> keep collecting -> recover
# Run against a live stack (default http://localhost:8000; override with BASE).
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
POINT="${POINT:-before_delete}"

say()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null || fail "missing dependency: $1"; }
need curl; need jq

say "0. wait for the API and the startup physical base backup"
status=""
for _ in $(seq 1 90); do
  status=$(curl -fsS "$BASE/backup" 2>/dev/null | jq -r '.status // empty' || true)
  [ "$status" = "ready" ] && break
  sleep 2
done
[ "$status" = "ready" ] || fail "base backup never became ready (last: ${status:-unknown})"
curl -fsS "$BASE/backup" | jq .

say "1. simulate collection: 3 intersections x 20 minutes on the primary"
curl -fsS -X POST "$BASE/demo/generate?intersections=3&minutes=20&prefix=IX" | jq .

say "2. create named restore point '$POINT'"
curl -fsS -X POST "$BASE/restore-points?name=$POINT" | jq .

say "3. wait until its WAL is fully archived (point becomes selectable)"
rp=""
for _ in $(seq 1 60); do
  rp=$(curl -fsS "$BASE/restore-points" | jq -r --arg p "$POINT" \
       '.items[] | select(.name==$p) | .status')
  [ "$rp" = "ready" ] && break
  sleep 2
done
[ "$rp" = "ready" ] || fail "restore point never became ready"

say "4. operator accident: delete the latest 10 minutes on the primary"
curl -fsS -X POST "$BASE/demo/delete?minutes=10" | jq .

say "5. primary keeps collecting (new IXB intersections) - writes are not blocked"
curl -fsS -X POST "$BASE/demo/generate?intersections=2&minutes=3&prefix=IXB" | jq .

say "6. start PITR recovery to '$POINT'"
rec=$(curl -fsS -X POST "$BASE/recoveries" -H 'Content-Type: application/json' \
      -d "{\"restore_point\":\"$POINT\"}")
echo "$rec" | jq .
rid=$(echo "$rec" | jq -r .id)

st=""
for _ in $(seq 1 90); do
  st=$(curl -fsS "$BASE/recoveries/$rid" | jq -r .status)
  [ "$st" = "success" ] && break
  if [ "$st" = "failed" ]; then
    curl -fsS "$BASE/recoveries/$rid" | jq .
    fail "recovery failed"
  fi
  sleep 2
done
[ "$st" = "success" ] || fail "recovery did not succeed (last status: $st)"
curl -fsS "$BASE/recoveries/$rid" | jq .

say "7. query the recovered read-only replica"
replica=$(curl -fsS "$BASE/replica/traffic?limit=1000")
echo "$replica" | jq '{recovery: .recovery, total_rows: .total_rows, sample: .rows[0:3]}'

say "8. compare with the primary (which kept collecting)"
primary=$(curl -fsS "$BASE/primary/traffic?limit=1000")
echo "$primary" | jq '{total_rows: .total_rows, sample: .rows[0:3]}'

replica_total=$(echo "$replica" | jq .total_rows)
replica_ixb=$(echo "$replica" | jq '[.rows[] | select(.intersection_id | startswith("IXB"))] | length')
primary_total=$(echo "$primary" | jq .total_rows)
primary_ixb=$(echo "$primary" | jq '[.rows[] | select(.intersection_id | startswith("IXB"))] | length')

say "9. assertions"
[ "$replica_total" = "60" ] || fail "replica should hold 60 rows (pre-delete state), got $replica_total"
[ "$replica_ixb"  = "0"  ] || fail "replica must not contain post-restore-point commits (IXB), got $replica_ixb"
[ "$primary_total" = "36" ] || fail "primary should hold 36 rows (30 left + 6 new), got $primary_total"
[ "$primary_ixb"  = "6"  ] || fail "primary should hold 6 IXB rows, got $primary_ixb"

say "10. backfill order: snapshot the deleted rows for one intersection from the replica"
# wide UTC half-open window covering the whole demo dataset (only IX-1 rows match)
order=$(curl -fsS -X POST "$BASE/backfill/orders" -H 'Content-Type: application/json' -d "{
  \"recovery_id\": $rid,
  \"intersection_id\": \"IX-1\",
  \"minute_start\": \"$(date -u -d '2 hours ago' +%Y-%m-%dT%H:%M:%SZ)\",
  \"minute_end\": \"$(date -u -d '2 hours'     +%Y-%m-%dT%H:%M:%SZ)\"
}")
echo "$order" | jq '{id, source_task, intersection_id, status, row_count}'
oid=$(echo "$order" | jq -r .id)
[ "$(echo "$order" | jq -r .row_count)" = "20" ] || fail "order should snapshot 20 replica rows"
# the full snapshot is stored on the primary for later verification
curl -fsS "$BASE/backfill/orders/$oid/rows" | jq '{row_count, sample: .rows[0:2]}'

say "11. confirm the backfill: missing rows inserted, equal rows skipped"
receipt=$(curl -fsS -X POST "$BASE/backfill/orders/$oid/confirm")
echo "$receipt" | jq '{order_id, source_task, status, backfilled_rows, skipped_rows, already_confirmed}'
[ "$(echo "$receipt" | jq -r .backfilled_rows)" = "10" ] || fail "10 deleted rows should be backfilled"
[ "$(echo "$receipt" | jq -r .skipped_rows)" = "10" ] || fail "10 existing equal rows should be skipped"
[ "$(echo "$receipt" | jq -r .source_task)" = "$POINT" ] || fail "receipt must carry the source task name"

say "12. exactly-once: re-confirm returns the original receipt, no second apply"
again=$(curl -fsS -X POST "$BASE/backfill/orders/$oid/confirm")
echo "$again" | jq '{status, backfilled_rows, already_confirmed}'
[ "$(echo "$again" | jq -r .already_confirmed)" = "true" ] || fail "retry must be idempotent"

say "13. the primary now holds the restored rows (and kept collecting all along)"
curl -fsS "$BASE/primary/traffic?intersection_id=IX-1&limit=100" \
  | jq '{ix1_rows: .total_rows}'
final_total=$(curl -fsS "$BASE/primary/traffic?limit=1000" | jq .total_rows)
[ "$final_total" = "46" ] || fail "primary should now hold 46 rows (36 + 10 backfilled), got $final_total"

printf '\n\033[32mDEMO OK: deleted rows recovered on the isolated replica and back-filled to the primary exactly once.\033[0m\n'
