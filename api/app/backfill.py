"""Backfill (补数) API: restore accidentally deleted counts to the primary.

A backfill *order* is made against one **successful recovery task** (which
owns an isolated read-only replica), for one intersection and one UTC
minute range, left-closed / right-open. At order time the complete set of
rows visible on that replica is snapshotted and stored verbatim in the
*primary* database's dedicated ``backfill`` management schema. The source
business table and the physical-recovery machinery are untouched.

Order confirmation runs in a single primary transaction:

* missing rows are inserted;
* rows already present with the same value are skipped;
* any key present with a *different* value rejects the whole order
  (nothing is written) and the conflicts are listed;
* the management-table receipt is committed together with the inserts,
  so a failure leaves no partial backfill.

The stored snapshot makes confirmation independent of the replica: once
the order exists, retiring / superseding its source replica has no effect.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

from . import config, db, recovery, state

log = logging.getLogger("backfill")

# One order may hold at most this many snapshot rows.
MAX_ROWS = 1000

# Order lifecycle.
STATUS_CREATED = "created"      # snapshot stored, not confirmed yet
STATUS_SUCCEEDED = "succeeded"  # applied exactly once, receipt stored
STATUS_FAILED = "failed"        # permanently rejected (value conflicts)

_create_lock = threading.Lock()


class Conflict(Exception):
    """Maps to HTTP 409."""


class NotFound(Exception):
    """Maps to HTTP 404."""


class Rejected(Exception):
    """Order-level rejection (e.g. over the row cap). Maps to HTTP 422."""


# --------------------------------------------------------------------------
# management schema on the primary (independent tables; business table kept)
# --------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS backfill;

CREATE TABLE IF NOT EXISTS backfill.orders (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recovery_id      INTEGER      NOT NULL,
    source_task      TEXT         NOT NULL,
    intersection_id  TEXT         NOT NULL,
    minute_start     TIMESTAMPTZ  NOT NULL,
    minute_end       TIMESTAMPTZ  NOT NULL,
    status           TEXT         NOT NULL,
    row_count        INTEGER      NOT NULL,
    inserted_rows    INTEGER      NOT NULL DEFAULT 0,
    skipped_rows     INTEGER      NOT NULL DEFAULT 0,
    conflict_detail  JSONB,
    replica_replay_lsn TEXT,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    confirmed_at     TIMESTAMPTZ,
    CHECK (status IN ('created', 'succeeded', 'failed')),
    CHECK (minute_end > minute_start)
);

CREATE TABLE IF NOT EXISTS backfill.order_rows (
    order_id        BIGINT       NOT NULL
                    REFERENCES backfill.orders(id) ON DELETE CASCADE,
    intersection_id TEXT         NOT NULL,
    minute_utc      TIMESTAMPTZ  NOT NULL,
    vehicle_count   INTEGER      NOT NULL,
    PRIMARY KEY (order_id, intersection_id, minute_utc)
);
"""


def ensure_schema():
    """Create the backfill management schema/tables on the primary (idempotent).

    Waits briefly for the primary at startup; a failure is logged but does not
    block the API (the endpoints will report the database error themselves).
    """
    deadline = time.time() + 60
    while True:
        try:
            with db.primary() as conn:
                conn.execute(_SCHEMA_SQL)
            return
        except Exception:  # noqa: BLE001 - retried, then degraded
            if time.time() >= deadline:
                log.exception("could not create backfill schema; endpoints will fail "
                              "until the primary is reachable")
                return
            time.sleep(2)


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _load_order(conn, order_id: int, for_update: bool = False):
    sql = "SELECT * FROM backfill.orders WHERE id = %s"
    if for_update:
        sql += " FOR UPDATE"
    return conn.execute(sql, (order_id,)).fetchone()


def _order_to_json(row):
    return {
        "id": row["id"],
        "recovery_id": row["recovery_id"],
        "source_task": row["source_task"],
        "intersection_id": row["intersection_id"],
        "minute_start": row["minute_start"],
        "minute_end": row["minute_end"],
        "status": row["status"],
        "row_count": row["row_count"],
        "inserted_rows": row["inserted_rows"],
        "skipped_rows": row["skipped_rows"],
        "conflict_detail": row["conflict_detail"],
        "replica_replay_lsn": row["replica_replay_lsn"],
        "created_at": row["created_at"],
        "confirmed_at": row["confirmed_at"],
    }


# --------------------------------------------------------------------------
# order creation: snapshot the exact replica, or reject on a replica switch
# --------------------------------------------------------------------------

def create_order(recovery_id: int, intersection_id: str,
                 minute_start: datetime, minute_end: datetime) -> dict:
    start = _as_utc(minute_start)
    end = _as_utc(minute_end)
    if end <= start:
        raise Rejected("minute_end must be after minute_start (UTC, left-closed/right-open)")
    if not intersection_id or len(intersection_id) > 200:
        raise Rejected("intersection_id is required and must be <= 200 chars")

    with _create_lock:
        rec = state.one(
            "SELECT * FROM recoveries WHERE id = ?", (recovery_id,)
        )
        if not rec:
            raise NotFound(f"recovery task {recovery_id} not found")
        if rec["status"] != "success":
            raise Conflict(
                f"recovery task {recovery_id} is not successful "
                f"(status={rec['status']}); only a successful recovery can source a backfill"
            )
        # The order must be sourced from the *currently served* replica.
        current = recovery.current()
        if not current or current["id"] != rec["id"]:
            raise Conflict(
                f"recovery task {recovery_id} is not the currently served replica; "
                "a replica switch happened and its point-in-time data is no longer "
                "obtainable - order rejected"
            )
        if not recovery.replica_alive(rec["work_dir"]):
            raise Conflict(
                f"the replica of recovery {recovery_id} is not running; "
                "cannot obtain the original point-in-time records"
            )

        # Snapshot read. Re-check identity/aliveness immediately before AND
        # after the read so that a concurrent replica switch can never
        # produce a mixed/partial snapshot.
        replay_lsn, rows = _snapshot_replica(
            rec, intersection_id, start, end
        )
        current2 = recovery.current()
        if not current2 or current2["id"] != rec["id"]:
            raise Conflict(
                "the source replica was switched while reading; no complete "
                "original records could be obtained - order rejected"
            )
        if not recovery.replica_alive(rec["work_dir"]):
            raise Conflict("the source replica exited while reading; order rejected")

        if len(rows) > MAX_ROWS:
            raise Rejected(
                f"{len(rows)} rows in range exceeds the {MAX_ROWS}-row limit; "
                "narrow the range and make separate orders"
            )

        return _persist_order(rec, intersection_id, start, end, rows, replay_lsn)


def _snapshot_replica(rec, intersection_id, start, end):
    """Read the complete row set from the named recovery's read-only replica."""
    try:
        with db.replica() as conn:
            meta = conn.execute(
                "SELECT pg_is_in_recovery() AS in_recovery, "
                "pg_last_wal_replay_lsn()::text AS replay_lsn"
            ).fetchone()
            rows = conn.execute(
                f"SELECT intersection_id, minute_utc, vehicle_count "
                f"FROM {config.TRAFFIC_TABLE} "
                f"WHERE intersection_id = %(ix)s "
                f"  AND minute_utc >= %(start)s AND minute_utc < %(end)s "
                f"ORDER BY minute_utc",
                {"ix": intersection_id, "start": start, "end": end},
            ).fetchall()
            # fetch one row beyond the cap to detect overflow deterministically
            overflow = conn.execute(
                f"SELECT 1 FROM {config.TRAFFIC_TABLE} "
                f"WHERE intersection_id = %(ix)s "
                f"  AND minute_utc >= %(start)s AND minute_utc < %(end)s "
                f"OFFSET %(cap)s LIMIT 1",
                {"ix": intersection_id, "start": start, "end": end,
                 "cap": MAX_ROWS},
            ).fetchall()
    except Exception as exc:  # noqa: BLE001 - surfaced as 409
        raise Conflict(f"failed to read the source replica: {exc}") from exc
    if overflow:
        raise Rejected(
            f"more than {MAX_ROWS} rows in range; narrow the range and make separate orders"
        )
    return meta["replay_lsn"], [dict(r) for r in rows]


def _persist_order(rec, intersection_id, start, end, rows, replay_lsn):
    """Store the snapshot verbatim; header and rows commit atomically."""
    conn = db.primary(row_factory=dict_row)
    conn.autocommit = False
    conn.execute("SET LOCAL lock_timeout = '10s'")
    conn.execute("SET LOCAL statement_timeout = '60s'")
    conn.execute("SET LOCAL idle_in_transaction_session_timeout = '30s'")
    try:
        row = conn.execute(
            "INSERT INTO backfill.orders "
            "(recovery_id, source_task, intersection_id, minute_start, minute_end, "
            " status, row_count, replica_replay_lsn) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING *",
            (rec["id"], rec["restore_point"], intersection_id, start, end,
             STATUS_CREATED, len(rows), replay_lsn),
        ).fetchone()
        order_id = row["id"]
        if rows:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO backfill.order_rows "
                    "(order_id, intersection_id, minute_utc, vehicle_count) "
                    "VALUES (%s, %s, %s, %s)",
                    [(order_id, r["intersection_id"], r["minute_utc"],
                      r["vehicle_count"]) for r in rows],
                )
        conn.commit()
        result = _order_to_json(row)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    log.info(
        "backfill order %s created from recovery %s (%s): %d snapshot rows",
        result["id"], rec["id"], rec["restore_point"], len(rows),
    )
    return result


# --------------------------------------------------------------------------
# confirmation: insert missing, skip equal, reject whole order on conflict
# --------------------------------------------------------------------------

def confirm_order(order_id: int) -> dict:
    # Pool connections are autocommit; manage one explicit transaction so the
    # order-row lock, business-table inserts and receipt commit atomically.
    conn = db.primary(row_factory=dict_row)
    conn.autocommit = False
    # Never wait forever on a contended key, and never leave an open
    # transaction behind if the worker stalls (e.g. client disconnect):
    # fail fast so the caller can retry, nothing is written until a
    # statement succeeds.
    conn.execute("SET LOCAL lock_timeout = '10s'")
    conn.execute("SET LOCAL statement_timeout = '60s'")
    conn.execute("SET LOCAL idle_in_transaction_session_timeout = '30s'")
    try:
        # Lock the order row first: concurrent confirmations of the same order
        # queue here, guaranteeing exactly-once application.
        order = _load_order(conn, order_id, for_update=True)
        if not order:
            conn.rollback()
            raise NotFound(f"backfill order {order_id} not found")

        if order["status"] == STATUS_SUCCEEDED:
            # Idempotent retry (duplicate concurrent call, client disconnect,
            # process restart): the original receipt is returned, the business
            # table is never touched again - even if rows were deleted meanwhile.
            conn.rollback()  # release the order lock
            return _receipt(order, already_confirmed=True)
        if order["status"] == STATUS_FAILED:
            conn.rollback()
            raise Conflict(
                f"backfill order {order_id} already failed and cannot be confirmed; "
                "create a new order"
            )

        snapshot = conn.execute(
            "SELECT intersection_id, minute_utc, vehicle_count "
            "FROM backfill.order_rows WHERE order_id = %s "
            "ORDER BY minute_utc",
            (order_id,),
        ).fetchall()

        # 1. Lock *only* this intersection's existing rows in-range, so that
        #    concurrent collection / direct-SQL corrections cannot change them
        #    under us, while unrelated intersections stay fully writable.
        existing = {
            (r["intersection_id"], r["minute_utc"]): r["vehicle_count"]
            for r in conn.execute(
                f"SELECT intersection_id, minute_utc, vehicle_count "
                f"FROM {config.TRAFFIC_TABLE} "
                f"WHERE intersection_id = %s "
                f"  AND minute_utc >= %s AND minute_utc < %s "
                f"FOR UPDATE",
                (order["intersection_id"],
                 order["minute_start"], order["minute_end"]),
            ).fetchall()
        }

        # 2. Same key, different value -> reject the entire order, list
        #    conflicts. The terminal 'failed' status is committed, but no
        #    business-table write has happened in this transaction.
        conflicts = []
        for r in snapshot:
            key = (r["intersection_id"], r["minute_utc"])
            if key in existing and existing[key] != r["vehicle_count"]:
                conflicts.append({
                    "intersection_id": r["intersection_id"],
                    "minute_utc": r["minute_utc"].isoformat(),
                    "snapshot_vehicle_count": r["vehicle_count"],
                    "primary_vehicle_count": existing[key],
                })
        if conflicts:
            conn.execute(
                "UPDATE backfill.orders SET status = %s, conflict_detail = %s, "
                "confirmed_at = now() WHERE id = %s",
                (STATUS_FAILED, psycopg.types.json.Json(conflicts), order_id),
            )
            conn.commit()
            raise Conflict(
                f"{len(conflicts)} conflicting row(s): same key exists on the primary "
                "with a different value; the whole order was rejected and nothing was "
                f"written. Conflicts: {conflicts}"
            )

        # 3. Insert only missing rows. ON CONFLICT DO NOTHING additionally
        #    absorbs races with rows inserted after our lock; equal values are
        #    simply skipped and newer values are never overwritten.
        to_insert = [
            r for r in snapshot
            if (r["intersection_id"], r["minute_utc"]) not in existing
        ]
        if to_insert:
            with conn.cursor() as cur:
                cur.executemany(
                    f"INSERT INTO {config.TRAFFIC_TABLE} "
                    f"(intersection_id, minute_utc, vehicle_count) "
                    f"VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    [(r["intersection_id"], r["minute_utc"], r["vehicle_count"])
                     for r in to_insert],
                )

        # 4. Re-read the touched keys to compute the receipt from committed
        #    intent: equal -> skipped, absent-then-inserted -> backfilled.
        inserted = 0
        skipped = 0
        for r in snapshot:
            got = conn.execute(
                f"SELECT vehicle_count FROM {config.TRAFFIC_TABLE} "
                f"WHERE intersection_id = %s AND minute_utc = %s",
                (r["intersection_id"], r["minute_utc"]),
            ).fetchone()
            if got is None:
                # extremely defensive: should be impossible after the insert
                raise RuntimeError("backfill row missing after insert; rolling back")
            if got["vehicle_count"] != r["vehicle_count"]:
                # A concurrent writer won the race with a different value:
                # never overwrite. Roll the whole order back instead.
                raise Conflict(
                    "concurrent write produced a different value at "
                    f"{r['intersection_id']} {r['minute_utc']}; order rolled back"
                )
            if (r["intersection_id"], r["minute_utc"]) in existing:
                skipped += 1
            else:
                inserted += 1

        # 5. Receipt committed in the SAME transaction as the inserts.
        conn.execute(
            "UPDATE backfill.orders SET status = %s, inserted_rows = %s, "
            "skipped_rows = %s, confirmed_at = now() WHERE id = %s",
            (STATUS_SUCCEEDED, inserted, skipped, order_id),
        )
        conn.commit()
        order = _load_order(conn, order_id)
        return _receipt(order, already_confirmed=False)
    except Conflict:
        if not conn.autocommit:
            conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _receipt(order, already_confirmed: bool) -> dict:
    return {
        "order_id": order["id"],
        "source_task": order["source_task"],
        "recovery_id": order["recovery_id"],
        "intersection_id": order["intersection_id"],
        "minute_start": order["minute_start"],
        "minute_end": order["minute_end"],
        "status": order["status"],
        "backfilled_rows": order["inserted_rows"],
        "skipped_rows": order["skipped_rows"],
        "already_confirmed": already_confirmed,
        "confirmed_at": order["confirmed_at"],
        "conflict_detail": order["conflict_detail"],
    }


def get_order(order_id: int) -> dict:
    with db.primary(row_factory=dict_row) as conn:
        order = _load_order(conn, order_id)
        if not order:
            raise NotFound(f"backfill order {order_id} not found")
        result = _order_to_json(order)
    return result


def list_orders(limit: int = 100) -> list[dict]:
    with db.primary(row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT * FROM backfill.orders ORDER BY id DESC LIMIT %s", (limit,)
        ).fetchall()
        return [_order_to_json(r) for r in rows]
