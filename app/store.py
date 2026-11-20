"""SQLite-backed trace collection and tail-sampling decisions.

Semantics implemented here:

* A trace becomes *active* when its first span is accepted.  Its collection
  deadline is ``first_seen_at + window_seconds`` and never moves.
* At the deadline the trace is decided in one transaction: all spans are kept
  if any accepted span in the window had ERROR status, or if a root span took
  longer than ``slow_root_ms``; otherwise the trace is dropped.
* Leaving the ``collecting`` state is what releases a capacity slot, so the
  decision and the slot release are the same atomic write.
* Dropping a trace does **not** delete its spans in the decision transaction.
  The decision only adds the trace's span count to a single-row
  ``cleanup_state`` ledger; a background loop then deletes the raw spans in
  small commits (at most ``cleanup_batch_size`` rows each) so ingestion can
  slip in between batches instead of waiting on one long write lock.
* The moment a trace is dropped it reads as zero-retention: list and detail
  views report no retained spans and the detail view stops serving raw span
  bodies, even while the rows still wait for the cleanup loop.
* Admission control reads only committed, O(1) state: the collecting-trace
  count and the ``cleanup_state`` ledger.  When the ledger reaches
  ``max_pending_delete_spans`` new traces are rejected (collecting traces
  still report normally) until the backlog drains below the threshold.
  The ledger is updated in the same transactions that decide traces and
  purge spans, and is reconciled with the actual rows at startup, so a
  crash or a failed cleanup batch never desynchronizes it.
* Spans arriving after the deadline are rejected and can never change a
  decision.  Duplicate ``(trace_id, span_id)`` pairs are stored once.
* All state lives in SQLite, so a restart keeps the original deadlines.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from .config import Settings
from .otlp import ParsedSpan

STATE_COLLECTING = "collecting"
STATE_KEPT = "kept"
STATE_DROPPED = "dropped"
ALL_STATES = (STATE_COLLECTING, STATE_KEPT, STATE_DROPPED)

SCHEMA = """
CREATE TABLE IF NOT EXISTS traces (
    trace_id TEXT PRIMARY KEY,
    first_seen_at REAL NOT NULL,
    deadline_at REAL NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('collecting', 'kept', 'dropped')),
    decided_at REAL,
    decision_reason TEXT,
    has_error INTEGER NOT NULL DEFAULT 0,
    root_duration_ns INTEGER,
    root_span_id TEXT,
    span_count INTEGER NOT NULL DEFAULT 0,
    service TEXT
);

CREATE INDEX IF NOT EXISTS idx_traces_state_deadline
    ON traces (state, deadline_at);
CREATE INDEX IF NOT EXISTS idx_traces_first_seen
    ON traces (first_seen_at DESC);

CREATE TABLE IF NOT EXISTS spans (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    name TEXT NOT NULL,
    service TEXT,
    scope TEXT,
    start_ns INTEGER,
    end_ns INTEGER,
    duration_ns INTEGER,
    is_root INTEGER NOT NULL DEFAULT 0,
    is_error INTEGER NOT NULL DEFAULT 0,
    body TEXT NOT NULL,
    received_at REAL NOT NULL,
    PRIMARY KEY (trace_id, span_id),
    FOREIGN KEY (trace_id) REFERENCES traces (trace_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans (trace_id);

-- Single-row ledger: spans of dropped traces that still await deletion.
-- Maintained inside the decision/purge transactions and reconciled with the
-- actual rows at startup, so admission control is an O(1) read of committed
-- state no matter how large the backlog grows.
CREATE TABLE IF NOT EXISTS cleanup_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    pending_spans INTEGER NOT NULL DEFAULT 0
);
"""

_TRACE_COLUMNS = (
    "trace_id, state, deadline_at, root_duration_ns, root_span_id, service"
)


@dataclass
class IngestResult:
    accepted: int = 0
    duplicates: int = 0
    rejected_capacity: int = 0
    rejected_cleanup: int = 0
    rejected_late: int = 0
    invalid: int = 0
    admitted_traces: list[str] = field(default_factory=list)
    decided: list[str] = field(default_factory=list)

    @property
    def rejected(self) -> int:
        return (
            self.rejected_capacity
            + self.rejected_cleanup
            + self.rejected_late
            + self.invalid
        )

    def rejection_message(self) -> str:
        parts = []
        if self.rejected_capacity:
            parts.append(f"{self.rejected_capacity} span(s) rejected: trace capacity full")
        if self.rejected_cleanup:
            parts.append(
                f"{self.rejected_cleanup} span(s) rejected: cleanup backlog full"
            )
        if self.rejected_late:
            parts.append(
                f"{self.rejected_late} span(s) rejected: collection window closed"
            )
        if self.invalid:
            parts.append(f"{self.invalid} span(s) rejected: invalid trace/span id")
        return "; ".join(parts)


class SqliteStore:
    """All trace state, serialized through one in-process lock.

    The app runs as a single process (one uvicorn worker); the lock together
    with ``BEGIN IMMEDIATE`` makes capacity checks and decisions race-free.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = threading.RLock()
        self._path = Path(settings.db_path)
        if str(self._path) != ":memory:":
            self._path.parent.mkdir(parents=True, exist_ok=True)

    # -- connection helpers -------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._path), timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def init_schema(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(SCHEMA)
                conn.execute(
                    "INSERT OR IGNORE INTO cleanup_state (id, pending_spans) "
                    "VALUES (1, 0)"
                )
                # Reconcile the ledger with the rows actually present, so a
                # restart (after a crash or a failed cleanup batch) resumes
                # cleanup and admission control from what is really stored.
                conn.execute(
                    "UPDATE cleanup_state SET pending_spans = ("
                    "  SELECT COUNT(*) FROM spans s"
                    "  JOIN traces t ON t.trace_id = s.trace_id"
                    "  WHERE t.state = ?"
                    ") WHERE id = 1",
                    (STATE_DROPPED,),
                )
            finally:
                conn.close()

    @contextmanager
    def _tx(self, conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    # -- decision logic -----------------------------------------------------

    def _decide(self, row: sqlite3.Row) -> tuple[str, str]:
        """Return ``(state, reason)`` for an expiring trace."""
        reasons = []
        if row["has_error"]:
            reasons.append("error")
        root_duration = row["root_duration_ns"]
        if root_duration is not None and root_duration > self.settings.slow_root_ns:
            reasons.append("slow_root")
        if reasons:
            return STATE_KEPT, "+".join(reasons)
        return STATE_DROPPED, "clean"

    def _settle_conn(self, conn: sqlite3.Connection, now: float) -> list[str]:
        """Decide every expired trace inside the caller's transaction."""
        rows = conn.execute(
            "SELECT trace_id, deadline_at, has_error, root_duration_ns, span_count "
            "FROM traces WHERE state = ? AND deadline_at <= ? "
            "ORDER BY deadline_at, trace_id",
            (STATE_COLLECTING, now),
        ).fetchall()
        decided: list[str] = []
        pending_add = 0
        for row in rows:
            state, reason = self._decide(row)
            updated = conn.execute(
                "UPDATE traces SET state = ?, decision_reason = ?, decided_at = ? "
                "WHERE trace_id = ? AND state = ?",
                (state, reason, row["deadline_at"], row["trace_id"], STATE_COLLECTING),
            ).rowcount
            if not updated:
                continue
            if state == STATE_DROPPED:
                # Raw spans are deleted later by the background cleanup loop;
                # here we only account for them in the ledger.
                pending_add += row["span_count"]
            decided.append(row["trace_id"])
        if pending_add:
            conn.execute(
                "UPDATE cleanup_state SET pending_spans = pending_spans + ? "
                "WHERE id = 1",
                (pending_add,),
            )
        return decided

    def settle_expired(self, now: float) -> list[str]:
        """Apply every due decision; used by the background sweeper."""
        with self._lock:
            conn = self._connect()
            try:
                with self._tx(conn):
                    return self._settle_conn(conn, now)
            finally:
                conn.close()

    def _pending_delete(self, conn: sqlite3.Connection) -> int:
        row = conn.execute(
            "SELECT pending_spans FROM cleanup_state WHERE id = 1"
        ).fetchone()
        return int(row[0]) if row else 0

    def purge_dropped_batch(self, batch_size: int | None = None) -> int:
        """Delete up to ``batch_size`` spans of dropped traces in one commit.

        Each call is a single short transaction so ingestion can proceed
        between batches.  Returns the number of spans deleted.  The ledger
        is decremented in the same transaction, so an interrupted purge
        (failure or crash) leaves committed state consistent and the next
        batch — or the next process — simply continues from what remains.
        """
        size = batch_size if batch_size is not None else self.settings.cleanup_batch_size
        size = max(1, size)
        with self._lock:
            conn = self._connect()
            try:
                # Skip the write transaction entirely when nothing is pending.
                if self._pending_delete(conn) <= 0:
                    return 0
                with self._tx(conn):
                    cursor = conn.execute(
                        "DELETE FROM spans WHERE rowid IN ("
                        "  SELECT s.rowid FROM spans s"
                        "  JOIN traces t ON t.trace_id = s.trace_id"
                        "  WHERE t.state = ? LIMIT ?"
                        ")",
                        (STATE_DROPPED, size),
                    )
                    deleted = cursor.rowcount
                    if deleted:
                        conn.execute(
                            "UPDATE cleanup_state "
                            "SET pending_spans = MAX(pending_spans - ?, 0) "
                            "WHERE id = 1",
                            (deleted,),
                        )
                return deleted
            finally:
                conn.close()

    # -- ingestion ----------------------------------------------------------

    def ingest(
        self, spans: Sequence[ParsedSpan], now: float, invalid: int = 0
    ) -> IngestResult:
        result = IngestResult(invalid=invalid)
        with self._lock:
            conn = self._connect()
            try:
                with self._tx(conn):
                    # Settling first frees slots due at this instant and makes
                    # the decision visible before admitting new work.
                    result.decided = self._settle_conn(conn, now)
                    active = self._count_collecting(conn)
                    # O(1) read of committed state: admission cost does not
                    # grow with the size of the cleanup backlog.
                    pending = self._pending_delete(conn)
                    for span in spans:
                        admitted = self._ingest_one(
                            conn, span, now, active, pending, result
                        )
                        if admitted:
                            active += 1
            finally:
                conn.close()
        return result

    def _count_collecting(self, conn: sqlite3.Connection) -> int:
        return conn.execute(
            "SELECT COUNT(*) FROM traces WHERE state = ?", (STATE_COLLECTING,)
        ).fetchone()[0]

    def _ingest_one(
        self,
        conn: sqlite3.Connection,
        span: ParsedSpan,
        now: float,
        active: int,
        pending: int,
        result: IngestResult,
    ) -> bool:
        """Store one span.  Returns True when a new trace was admitted."""
        trace = conn.execute(
            f"SELECT {_TRACE_COLUMNS} FROM traces WHERE trace_id = ?",
            (span.trace_id,),
        ).fetchone()

        admitted = False
        if trace is None:
            if active >= self.settings.max_active_traces:
                result.rejected_capacity += 1
                return False
            if pending >= self.settings.max_pending_delete_spans:
                # Dropped-but-not-yet-deleted spans piled up: pause new
                # traces until the cleanup loop drains the backlog.  Traces
                # that are already collecting are unaffected (see below).
                result.rejected_cleanup += 1
                return False
            conn.execute(
                "INSERT INTO traces (trace_id, first_seen_at, deadline_at, state) "
                "VALUES (?, ?, ?, ?)",
                (
                    span.trace_id,
                    now,
                    now + self.settings.window_seconds,
                    STATE_COLLECTING,
                ),
            )
            trace = conn.execute(
                f"SELECT {_TRACE_COLUMNS} FROM traces WHERE trace_id = ?",
                (span.trace_id,),
            ).fetchone()
            result.admitted_traces.append(span.trace_id)
            admitted = True

        if trace["state"] != STATE_COLLECTING:
            # Trace is kept or dropped: its window is closed, the span is late.
            result.rejected_late += 1
            return admitted

        cursor = conn.execute(
            "INSERT OR IGNORE INTO spans ("
            "trace_id, span_id, parent_span_id, name, service, scope, "
            "start_ns, end_ns, duration_ns, is_root, is_error, body, received_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                span.trace_id,
                span.span_id,
                span.parent_span_id,
                span.name,
                span.service,
                span.scope,
                span.start_ns,
                span.end_ns,
                span.duration_ns,
                int(span.is_root),
                int(span.is_error),
                span.body,
                now,
            ),
        )
        if cursor.rowcount == 0:
            result.duplicates += 1
            return admitted

        sql = "UPDATE traces SET span_count = span_count + 1"
        params: list[Any] = []
        if span.is_error:
            # Any accepted error span wins for the lifetime of the trace.
            sql += ", has_error = 1"
        if trace["service"] is None and span.service:
            sql += ", service = ?"
            params.append(span.service)
        if span.is_root and span.duration_ns is not None:
            current = trace["root_duration_ns"]
            if current is None or span.duration_ns > current:
                sql += ", root_duration_ns = ?, root_span_id = ?"
                params.extend([span.duration_ns, span.span_id])
        sql += " WHERE trace_id = ?"
        params.append(span.trace_id)
        conn.execute(sql, params)
        result.accepted += 1
        return admitted

    # -- reads --------------------------------------------------------------

    def settle_and_get(self, trace_id: str, now: float) -> dict[str, Any] | None:
        with self._lock:
            conn = self._connect()
            try:
                with self._tx(conn):
                    decided = self._settle_conn(conn, now)
                row = conn.execute(
                    "SELECT * FROM traces WHERE trace_id = ?", (trace_id,)
                ).fetchone()
                if row is None:
                    return None
                if row["state"] == STATE_DROPPED:
                    # Dropped traces stop serving raw spans immediately, even
                    # while rows still await the background cleanup loop.
                    spans: list[sqlite3.Row] = []
                else:
                    spans = conn.execute(
                        "SELECT span_id, parent_span_id, name, service, scope, "
                        "start_ns, end_ns, duration_ns, is_root, is_error, body, received_at "
                        "FROM spans WHERE trace_id = ? "
                        "ORDER BY (start_ns IS NULL), start_ns, span_id",
                        (trace_id,),
                    ).fetchall()
                payload = _trace_dict(row, spans, now, self.settings)
                payload["just_decided"] = trace_id in decided
                return payload
            finally:
                conn.close()

    def list_traces(
        self, now: float, limit: int = 50, state: str | None = None
    ) -> dict[str, Any]:
        limit = max(1, min(limit, 500))
        if state is not None and state not in ALL_STATES:
            raise ValueError("unknown state filter")
        with self._lock:
            conn = self._connect()
            try:
                with self._tx(conn):
                    self._settle_conn(conn, now)
                sql = (
                    "SELECT t.*, "
                    "CASE WHEN t.state = 'dropped' THEN 0 "
                    "ELSE (SELECT COUNT(*) FROM spans s WHERE s.trace_id = t.trace_id) "
                    "END AS retained_spans FROM traces t"
                )
                params: list[Any] = []
                if state is not None:
                    sql += " WHERE t.state = ?"
                    params.append(state)
                sql += " ORDER BY t.first_seen_at DESC, t.trace_id LIMIT ?"
                params.append(limit)
                rows = conn.execute(sql, params).fetchall()
                counts = {
                    row["state"]: row["count"]
                    for row in conn.execute(
                        "SELECT state, COUNT(*) AS count FROM traces GROUP BY state"
                    ).fetchall()
                }
                return {
                    "traces": [
                        _trace_summary_dict(row, now, self.settings) for row in rows
                    ],
                    "counts": {name: counts.get(name, 0) for name in ALL_STATES},
                    "capacity": self.settings.max_active_traces,
                    "pending_delete_spans": self._pending_delete(conn),
                    "max_pending_delete_spans": self.settings.max_pending_delete_spans,
                    "now": now,
                }
            finally:
                conn.close()


def _trace_summary_dict(
    row: sqlite3.Row, now: float, settings: Settings
) -> dict[str, Any]:
    remaining = 0.0
    if row["state"] == STATE_COLLECTING:
        remaining = max(0.0, row["deadline_at"] - now)
    root_duration_ns = row["root_duration_ns"]
    keys = row.keys()
    return {
        "trace_id": row["trace_id"],
        "state": row["state"],
        "first_seen_at": row["first_seen_at"],
        "deadline_at": row["deadline_at"],
        "remaining_seconds": remaining,
        "decided_at": row["decided_at"],
        "decision_reason": row["decision_reason"],
        "has_error": bool(row["has_error"]),
        "root_duration_ns": root_duration_ns,
        "root_duration_ms": (
            root_duration_ns / 1_000_000 if root_duration_ns is not None else None
        ),
        "span_count": row["span_count"],
        "retained_spans": (
            row["retained_spans"] if "retained_spans" in keys else None
        ),
        "service": row["service"],
        "window_seconds": settings.window_seconds,
        "slow_root_ms": settings.slow_root_ms,
    }


def _trace_dict(
    row: sqlite3.Row,
    spans: Iterable[sqlite3.Row],
    now: float,
    settings: Settings,
) -> dict[str, Any]:
    span_list = list(spans)
    payload = _trace_summary_dict(row, now, settings)
    payload["retained_spans"] = len(span_list)
    payload["spans"] = [
        {
            "span_id": span["span_id"],
            "parent_span_id": span["parent_span_id"],
            "name": span["name"],
            "service": span["service"],
            "scope": span["scope"],
            "start_ns": span["start_ns"],
            "end_ns": span["end_ns"],
            "duration_ns": span["duration_ns"],
            "duration_ms": (
                span["duration_ns"] / 1_000_000
                if span["duration_ns"] is not None
                else None
            ),
            "is_root": bool(span["is_root"]),
            "is_error": bool(span["is_error"]),
            "received_at": span["received_at"],
            "body": span["body"],
        }
        for span in span_list
    ]
    return payload
