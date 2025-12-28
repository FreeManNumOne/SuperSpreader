from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Iterable


class SqliteStore:
    """
    Simple SQLite persistence layer.
    - Thread-safe via a single connection + lock (fine for this asyncio app).
    - Stores: markets, orders, fills, tape events, position snapshots, pnl snapshots.
    """

    def __init__(self, path: str):
        if not path or not str(path).strip():
            raise ValueError("SQLITE_PATH is empty; set SQLITE_PATH in your .env")

        raw = str(path).strip()
        # Special cases:
        # - :memory: is valid and should not touch the filesystem.
        # - file: URIs may be used by advanced users; we won't try to mkdir those.
        self._is_memory = raw == ":memory:"
        self._is_uri = raw.startswith("file:")

        if self._is_memory or self._is_uri:
            self.path = raw
        else:
            p = Path(raw).expanduser()
            # Make relative paths resolve against current working directory.
            if not p.is_absolute():
                p = Path(os.getcwd()) / p
            p.parent.mkdir(parents=True, exist_ok=True)
            self.path = str(p)

        self._lock = threading.Lock()
        try:
            self._conn = sqlite3.connect(self.path, check_same_thread=False, uri=self._is_uri)
        except sqlite3.OperationalError as e:
            # Provide a more actionable error message (especially on Windows).
            raise sqlite3.OperationalError(
                "unable to open SQLite database file. "
                f"SQLITE_PATH={self.path!r} cwd={os.getcwd()!r}. "
                "Fix by setting SQLITE_PATH to a writable location (e.g. ./data/polymarket_trader.sqlite) "
                "or ensure the parent directory exists."
            ) from e
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")

    def init_db(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS markets (
                  market_id TEXT PRIMARY KEY,
                  question TEXT,
                  event_id TEXT,
                  active INTEGER,
                  end_ts REAL,
                  volume_24h_usd REAL,
                  liquidity_usd REAL,
                  updated_ts REAL
                );

                CREATE TABLE IF NOT EXISTS orders (
                  order_id TEXT PRIMARY KEY,
                  market_id TEXT,
                  side TEXT,
                  price REAL,
                  size REAL,
                  created_ts REAL,
                  status TEXT,
                  filled_size REAL,
                  meta_json TEXT
                );

                CREATE TABLE IF NOT EXISTS fills (
                  fill_id TEXT PRIMARY KEY,
                  order_id TEXT,
                  market_id TEXT,
                  side TEXT,
                  price REAL,
                  size REAL,
                  ts REAL,
                  meta_json TEXT
                );

                CREATE TABLE IF NOT EXISTS tape (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts REAL,
                  market_id TEXT,
                  kind TEXT,
                  payload_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_tape_ts ON tape(ts);
                CREATE INDEX IF NOT EXISTS idx_tape_market ON tape(market_id, ts);

                CREATE TABLE IF NOT EXISTS position_snapshots (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts REAL,
                  market_id TEXT,
                  event_id TEXT,
                  position REAL,
                  avg_price REAL,
                  mark_price REAL,
                  unrealized_pnl REAL,
                  realized_pnl REAL
                );

                CREATE TABLE IF NOT EXISTS pnl_snapshots (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts REAL,
                  total_unrealized REAL,
                  total_realized REAL,
                  total_pnl REAL
                );

                -- Scanner/watchlist for monitoring UI
                CREATE TABLE IF NOT EXISTS scanner_snapshots (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts REAL,
                  eligible_count INTEGER,
                  top_count INTEGER
                );

                CREATE INDEX IF NOT EXISTS idx_scanner_ts ON scanner_snapshots(ts);

                -- Current ranked watchlist (top N), rewritten on every scan.
                CREATE TABLE IF NOT EXISTS watchlist (
                  rank INTEGER PRIMARY KEY,
                  market_id TEXT,
                  ts REAL
                );
                """
            )
            self._conn.commit()

    def upsert_markets(self, markets: Iterable[dict[str, Any]]) -> None:
        now = time.time()
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO markets(market_id, question, event_id, active, end_ts, volume_24h_usd, liquidity_usd, updated_ts)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(market_id) DO UPDATE SET
                  question=excluded.question,
                  event_id=excluded.event_id,
                  active=excluded.active,
                  end_ts=excluded.end_ts,
                  volume_24h_usd=excluded.volume_24h_usd,
                  liquidity_usd=excluded.liquidity_usd,
                  updated_ts=excluded.updated_ts
                """,
                [
                    (
                        m["market_id"],
                        m.get("question"),
                        m.get("event_id"),
                        1 if m.get("active", True) else 0,
                        m.get("end_ts"),
                        float(m.get("volume_24h_usd", 0.0)),
                        float(m.get("liquidity_usd", 0.0)),
                        now,
                    )
                    for m in markets
                ],
            )
            self._conn.commit()

    def insert_order(self, order: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO orders(order_id, market_id, side, price, size, created_ts, status, filled_size, meta_json)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    order["order_id"],
                    order["market_id"],
                    order["side"],
                    order["price"],
                    order["size"],
                    order["created_ts"],
                    order["status"],
                    order.get("filled_size", 0.0),
                    json.dumps(order.get("meta", {}), separators=(",", ":")),
                ),
            )
            self._conn.commit()

    def update_order_status(self, order_id: str, status: str, filled_size: float | None = None) -> None:
        with self._lock:
            if filled_size is None:
                self._conn.execute("UPDATE orders SET status=? WHERE order_id=?", (status, order_id))
            else:
                self._conn.execute(
                    "UPDATE orders SET status=?, filled_size=? WHERE order_id=?",
                    (status, float(filled_size), order_id),
                )
            self._conn.commit()

    def insert_fill(self, fill: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO fills(fill_id, order_id, market_id, side, price, size, ts, meta_json)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    fill["fill_id"],
                    fill["order_id"],
                    fill["market_id"],
                    fill["side"],
                    fill["price"],
                    fill["size"],
                    fill["ts"],
                    json.dumps(fill.get("meta", {}), separators=(",", ":")),
                ),
            )
            self._conn.commit()

    def insert_tape(self, ts: float, market_id: str, kind: str, payload: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO tape(ts, market_id, kind, payload_json) VALUES(?,?,?,?)",
                (float(ts), market_id, kind, json.dumps(payload, separators=(",", ":"))),
            )
            self._conn.commit()

    def insert_position_snapshot(self, snap: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO position_snapshots(ts, market_id, event_id, position, avg_price, mark_price, unrealized_pnl, realized_pnl)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    snap["ts"],
                    snap["market_id"],
                    snap["event_id"],
                    snap["position"],
                    snap["avg_price"],
                    snap["mark_price"],
                    snap["unrealized_pnl"],
                    snap["realized_pnl"],
                ),
            )
            self._conn.commit()

    def insert_pnl_snapshot(self, snap: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO pnl_snapshots(ts, total_unrealized, total_realized, total_pnl) VALUES(?,?,?,?)",
                (snap["ts"], snap["total_unrealized"], snap["total_realized"], snap["total_pnl"]),
            )
            self._conn.commit()

    def fetch_latest_positions(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            # IMPORTANT:
            # Avoid `GROUP BY market_id` with non-aggregated columns.
            # SQLite will return arbitrary values for non-aggregated columns, which can
            # make telemetry lie (e.g. showing a stale non-zero position even if the
            # latest snapshot is flat).
            #
            # We select the latest row per market by max autoincrement `id`.
            cur = self._conn.execute(
                """
                WITH latest AS (
                  SELECT market_id, MAX(id) AS id_max
                  FROM position_snapshots
                  GROUP BY market_id
                )
                SELECT ps.market_id,
                       ps.event_id,
                       ps.position,
                       ps.avg_price,
                       ps.mark_price,
                       ps.unrealized_pnl,
                       ps.realized_pnl,
                       ps.ts
                FROM position_snapshots ps
                JOIN latest ON latest.id_max = ps.id
                ORDER BY ps.ts DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def fetch_latest_pnl(self) -> dict[str, Any] | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT ts, total_unrealized, total_realized, total_pnl FROM pnl_snapshots ORDER BY ts DESC LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return None
            return {"ts": row[0], "total_unrealized": row[1], "total_realized": row[2], "total_pnl": row[3]}

    def insert_scanner_snapshot(self, ts: float, eligible_count: int, top_count: int) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO scanner_snapshots(ts, eligible_count, top_count) VALUES(?,?,?)",
                (float(ts), int(eligible_count), int(top_count)),
            )
            self._conn.commit()

    def fetch_latest_scanner_snapshot(self) -> dict[str, Any] | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT ts, eligible_count, top_count FROM scanner_snapshots ORDER BY ts DESC LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return None
            return {"ts": float(row[0]), "eligible_count": int(row[1]), "top_count": int(row[2])}

    def update_watchlist(self, market_ids: list[str], ts: float | None = None) -> None:
        ts0 = time.time() if ts is None else float(ts)
        with self._lock:
            self._conn.execute("DELETE FROM watchlist")
            self._conn.executemany(
                "INSERT INTO watchlist(rank, market_id, ts) VALUES(?,?,?)",
                [(i + 1, str(mid), ts0) for i, mid in enumerate(market_ids)],
            )
            self._conn.commit()

    def fetch_watchlist(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT w.rank, w.market_id, w.ts as watch_ts,
                       m.question, m.event_id, m.active, m.end_ts, m.volume_24h_usd, m.liquidity_usd, m.updated_ts
                FROM watchlist w
                LEFT JOIN markets m ON m.market_id = w.market_id
                ORDER BY w.rank ASC
                LIMIT ?
                """,
                (int(limit),),
            )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def fetch_markets(self, limit: int = 200, active_only: bool = True) -> list[dict[str, Any]]:
        q = """
            SELECT market_id, question, event_id, active, end_ts, volume_24h_usd, liquidity_usd, updated_ts
            FROM markets
        """
        params: list[Any] = []
        if active_only:
            q += " WHERE active = 1"
        q += " ORDER BY volume_24h_usd DESC, liquidity_usd DESC LIMIT ?"
        params.append(int(limit))
        with self._lock:
            cur = self._conn.execute(q, tuple(params))
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[dict[str, Any]]:
        ids = [str(x) for x in market_ids if str(x).strip()]
        if not ids:
            return []
        placeholders = ",".join(["?"] * len(ids))
        q = f"""
            SELECT market_id, question, event_id, active, end_ts, volume_24h_usd, liquidity_usd, updated_ts
            FROM markets
            WHERE market_id IN ({placeholders})
        """
        with self._lock:
            cur = self._conn.execute(q, tuple(ids))
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        by_id = {r["market_id"]: r for r in rows}
        return [by_id.get(mid, {"market_id": mid}) for mid in ids]

    def fetch_pnl_series(self, since_ts: float, limit: int = 4000) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT ts, total_unrealized, total_realized, total_pnl
                FROM pnl_snapshots
                WHERE ts >= ?
                ORDER BY ts ASC
                LIMIT ?
                """,
                (float(since_ts), int(limit)),
            )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def fetch_recent_orders(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT order_id, market_id, side, price, size, created_ts, status, filled_size, meta_json
                FROM orders
                ORDER BY created_ts DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            cols = [c[0] for c in cur.description]
            out: list[dict[str, Any]] = []
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                try:
                    d["meta"] = json.loads(d.get("meta_json") or "{}")
                except Exception:
                    d["meta"] = {}
                out.append(d)
            return out

    def fetch_recent_fills(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT fill_id, order_id, market_id, side, price, size, ts, meta_json
                FROM fills
                ORDER BY ts DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            cols = [c[0] for c in cur.description]
            out: list[dict[str, Any]] = []
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                try:
                    d["meta"] = json.loads(d.get("meta_json") or "{}")
                except Exception:
                    d["meta"] = {}
                out.append(d)
            return out

    def fetch_latest_tape_ts(self) -> float | None:
        with self._lock:
            cur = self._conn.execute("SELECT MAX(ts) FROM tape")
            row = cur.fetchone()
            if not row or row[0] is None:
                return None
            return float(row[0])

    def fetch_latest_market_update_ts(self) -> float | None:
        with self._lock:
            cur = self._conn.execute("SELECT MAX(updated_ts) FROM markets")
            row = cur.fetchone()
            if not row or row[0] is None:
                return None
            return float(row[0])

    def iter_tape(self, start_ts: float | None, end_ts: float | None):
        q = "SELECT ts, market_id, kind, payload_json FROM tape WHERE 1=1"
        params: list[Any] = []
        if start_ts is not None:
            q += " AND ts >= ?"
            params.append(float(start_ts))
        if end_ts is not None:
            q += " AND ts <= ?"
            params.append(float(end_ts))
        q += " ORDER BY ts ASC, id ASC"
        with self._lock:
            cur = self._conn.execute(q, tuple(params))
            for ts, market_id, kind, payload_json in cur:
                yield float(ts), str(market_id), str(kind), json.loads(payload_json)

