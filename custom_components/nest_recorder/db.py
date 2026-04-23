"""SQLite-backed store for the Nest Recorder index."""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from homeassistant.core import HomeAssistant

from .const import GlacierStatus

_MIGRATIONS: list[str] = [
    # v1 -- initial schema
    """
    CREATE TABLE IF NOT EXISTS cameras (
        id TEXT PRIMARY KEY,
        device_id TEXT NOT NULL,
        name TEXT NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('rtsp','webrtc')),
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS segments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        camera_id TEXT NOT NULL,
        start_ts INTEGER NOT NULL,
        end_ts INTEGER NOT NULL,
        duration_s INTEGER NOT NULL,
        local_path TEXT,
        size_bytes INTEGER NOT NULL DEFAULT 0,
        s3_key TEXT,
        glacier_status TEXT NOT NULL DEFAULT 'PENDING'
            CHECK(glacier_status IN ('PENDING','ARCHIVED','RESTORING','RESTORED','FAILED')),
        restore_requested_at INTEGER,
        restore_ready_at INTEGER,
        restore_expires_at INTEGER,
        protected_until INTEGER
    );
    CREATE INDEX IF NOT EXISTS ix_segments_camera_start
        ON segments(camera_id, start_ts);
    CREATE INDEX IF NOT EXISTS ix_segments_glacier
        ON segments(glacier_status);
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        camera_id TEXT NOT NULL,
        type TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        event_session_id TEXT,
        segment_id INTEGER
    );
    CREATE INDEX IF NOT EXISTS ix_events_camera_ts
        ON events(camera_id, timestamp);
    CREATE INDEX IF NOT EXISTS ix_events_segment
        ON events(segment_id);
    """,
    # v2 -- event media columns (snapshot + clip)
    """
    ALTER TABLE events ADD COLUMN snapshot_local_path TEXT;
    ALTER TABLE events ADD COLUMN snapshot_s3_key TEXT;
    ALTER TABLE events ADD COLUMN clip_local_path TEXT;
    ALTER TABLE events ADD COLUMN clip_s3_key TEXT;
    ALTER TABLE events ADD COLUMN media_status TEXT DEFAULT 'PENDING';
    """,
]


@dataclass
class Segment:
    id: int
    camera_id: str
    start_ts: int
    end_ts: int
    duration_s: int
    local_path: str | None
    size_bytes: int
    s3_key: str | None
    glacier_status: str
    restore_requested_at: int | None
    restore_ready_at: int | None
    restore_expires_at: int | None
    protected_until: int | None


@dataclass
class EventRow:
    id: int
    camera_id: str
    type: str
    timestamp: int
    event_session_id: str | None
    segment_id: int | None
    snapshot_local_path: str | None = None
    snapshot_s3_key: str | None = None
    clip_local_path: str | None = None
    clip_s3_key: str | None = None
    media_status: str | None = None


class SegmentStore:
    """Thin async wrapper over sqlite3. All DB work runs in the executor."""

    def __init__(self, hass: HomeAssistant, db_path: Path) -> None:
        self._hass = hass
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    async def async_open(self) -> None:
        await self._hass.async_add_executor_job(self._open_sync)

    def _open_sync(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
        )
        row = conn.execute("SELECT version FROM schema_version").fetchone()
        current = row[0] if row else 0
        for i, sql in enumerate(_MIGRATIONS, start=1):
            if i > current:
                conn.executescript(sql)
                if row is None and i == 1:
                    conn.execute("INSERT INTO schema_version(version) VALUES(?)", (i,))
                    row = (i,)
                else:
                    conn.execute("UPDATE schema_version SET version=?", (i,))
        self._conn = conn

    async def async_close(self) -> None:
        if self._conn is not None:
            await self._hass.async_add_executor_job(self._conn.close)
            self._conn = None

    def _exec(self, fn, *args, **kwargs):
        return self._hass.async_add_executor_job(lambda: fn(*args, **kwargs))

    # ----- cameras -----

    async def upsert_camera(
        self, cam_id: str, device_id: str, name: str, kind: str, enabled: bool
    ) -> None:
        def _work() -> None:
            assert self._conn is not None
            self._conn.execute(
                """
                INSERT INTO cameras(id, device_id, name, kind, enabled, created_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    device_id=excluded.device_id,
                    name=excluded.name,
                    kind=excluded.kind,
                    enabled=excluded.enabled
                """,
                (cam_id, device_id, name, kind, int(enabled), int(time.time())),
            )

        await self._exec(_work)

    async def list_cameras(self) -> list[dict[str, Any]]:
        def _work() -> list[dict[str, Any]]:
            assert self._conn is not None
            rows = self._conn.execute("SELECT * FROM cameras ORDER BY name").fetchall()
            return [dict(r) for r in rows]

        return await self._exec(_work)

    # ----- segments -----

    async def insert_segment(
        self,
        camera_id: str,
        start_ts: int,
        end_ts: int,
        duration_s: int,
        local_path: str,
        size_bytes: int,
    ) -> int:
        def _work() -> int:
            assert self._conn is not None
            cur = self._conn.execute(
                """
                INSERT INTO segments(
                    camera_id, start_ts, end_ts, duration_s, local_path,
                    size_bytes, glacier_status
                ) VALUES(?,?,?,?,?,?, 'PENDING')
                """,
                (camera_id, start_ts, end_ts, duration_s, local_path, size_bytes),
            )
            return int(cur.lastrowid)

        return await self._exec(_work)

    async def mark_archived(self, segment_id: int, s3_key: str) -> None:
        def _work() -> None:
            assert self._conn is not None
            self._conn.execute(
                "UPDATE segments SET s3_key=?, glacier_status='ARCHIVED' WHERE id=?",
                (s3_key, segment_id),
            )

        await self._exec(_work)

    async def mark_failed(self, segment_id: int) -> None:
        def _work() -> None:
            assert self._conn is not None
            self._conn.execute(
                "UPDATE segments SET glacier_status='FAILED' WHERE id=?",
                (segment_id,),
            )

        await self._exec(_work)

    async def mark_restore_requested(self, segment_id: int, now: int) -> None:
        def _work() -> None:
            assert self._conn is not None
            self._conn.execute(
                """
                UPDATE segments
                SET glacier_status='RESTORING', restore_requested_at=?
                WHERE id=?
                """,
                (now, segment_id),
            )

        await self._exec(_work)

    async def mark_restored(self, segment_id: int, ready_at: int, expires_at: int | None) -> None:
        def _work() -> None:
            assert self._conn is not None
            self._conn.execute(
                """
                UPDATE segments
                SET glacier_status='RESTORED', restore_ready_at=?, restore_expires_at=?
                WHERE id=?
                """,
                (ready_at, expires_at, segment_id),
            )

        await self._exec(_work)

    async def clear_local_path(self, segment_id: int) -> None:
        def _work() -> None:
            assert self._conn is not None
            self._conn.execute(
                "UPDATE segments SET local_path=NULL WHERE id=?", (segment_id,)
            )

        await self._exec(_work)

    async def get_segment(self, segment_id: int) -> Segment | None:
        def _work() -> Segment | None:
            assert self._conn is not None
            row = self._conn.execute(
                "SELECT * FROM segments WHERE id=?", (segment_id,)
            ).fetchone()
            return Segment(**dict(row)) if row else None

        return await self._exec(_work)

    async def segments_for_day(
        self, camera_id: str, day_start_ts: int, day_end_ts: int
    ) -> list[Segment]:
        def _work() -> list[Segment]:
            assert self._conn is not None
            rows = self._conn.execute(
                """
                SELECT * FROM segments
                WHERE camera_id=? AND start_ts>=? AND start_ts<?
                ORDER BY start_ts
                """,
                (camera_id, day_start_ts, day_end_ts),
            ).fetchall()
            return [Segment(**dict(r)) for r in rows]

        return await self._exec(_work)

    async def segments_expiring_local(
        self, cutoff_ts: int
    ) -> list[Segment]:
        def _work() -> list[Segment]:
            assert self._conn is not None
            rows = self._conn.execute(
                """
                SELECT * FROM segments
                WHERE local_path IS NOT NULL
                  AND start_ts < ?
                  AND glacier_status IN ('ARCHIVED','RESTORED')
                  AND (protected_until IS NULL OR protected_until < ?)
                """,
                (cutoff_ts, int(time.time())),
            ).fetchall()
            return [Segment(**dict(r)) for r in rows]

        return await self._exec(_work)

    async def distinct_days(self, camera_id: str) -> list[int]:
        def _work() -> list[int]:
            assert self._conn is not None
            rows = self._conn.execute(
                """
                SELECT DISTINCT (start_ts - (start_ts % 86400)) AS day
                FROM segments WHERE camera_id=? ORDER BY day DESC
                """,
                (camera_id,),
            ).fetchall()
            return [int(r["day"]) for r in rows]

        return await self._exec(_work)

    async def archived_only(self, camera_id: str) -> list[Segment]:
        def _work() -> list[Segment]:
            assert self._conn is not None
            rows = self._conn.execute(
                """
                SELECT * FROM segments
                WHERE camera_id=? AND local_path IS NULL
                ORDER BY start_ts DESC
                """,
                (camera_id,),
            ).fetchall()
            return [Segment(**dict(r)) for r in rows]

        return await self._exec(_work)

    async def open_segment_for(self, camera_id: str, ts: int) -> Segment | None:
        def _work() -> Segment | None:
            assert self._conn is not None
            row = self._conn.execute(
                """
                SELECT * FROM segments
                WHERE camera_id=? AND start_ts <= ? AND end_ts >= ?
                ORDER BY start_ts DESC LIMIT 1
                """,
                (camera_id, ts, ts),
            ).fetchone()
            return Segment(**dict(row)) if row else None

        return await self._exec(_work)

    async def extend_retention(self, segment_id: int, until_ts: int) -> None:
        def _work() -> None:
            assert self._conn is not None
            self._conn.execute(
                "UPDATE segments SET protected_until=? WHERE id=?",
                (until_ts, segment_id),
            )

        await self._exec(_work)

    # ----- events -----

    async def insert_event(
        self,
        camera_id: str,
        type_: str,
        timestamp: int,
        session_id: str | None,
        segment_id: int | None,
    ) -> int:
        def _work() -> int:
            assert self._conn is not None
            cur = self._conn.execute(
                """
                INSERT INTO events(camera_id, type, timestamp, event_session_id, segment_id)
                VALUES(?,?,?,?,?)
                """,
                (camera_id, type_, timestamp, session_id, segment_id),
            )
            return int(cur.lastrowid)

        return await self._exec(_work)

    async def backfill_segment_ids(
        self, camera_id: str, start_ts: int, end_ts: int, segment_id: int
    ) -> int:
        def _work() -> int:
            assert self._conn is not None
            cur = self._conn.execute(
                """
                UPDATE events
                SET segment_id=?
                WHERE camera_id=? AND segment_id IS NULL
                  AND timestamp BETWEEN ? AND ?
                """,
                (segment_id, camera_id, start_ts, end_ts),
            )
            return cur.rowcount

        return await self._exec(_work)

    async def events_for_day(
        self, camera_id: str, day_start_ts: int, day_end_ts: int
    ) -> list[EventRow]:
        def _work() -> list[EventRow]:
            assert self._conn is not None
            rows = self._conn.execute(
                """
                SELECT * FROM events
                WHERE camera_id=? AND timestamp>=? AND timestamp<?
                ORDER BY timestamp
                """,
                (camera_id, day_start_ts, day_end_ts),
            ).fetchall()
            return [EventRow(**dict(r)) for r in rows]

        return await self._exec(_work)

    async def all_events(self, camera_id: str, limit: int = 500) -> list[EventRow]:
        def _work() -> list[EventRow]:
            assert self._conn is not None
            rows = self._conn.execute(
                """
                SELECT * FROM events WHERE camera_id=?
                ORDER BY timestamp DESC LIMIT ?
                """,
                (camera_id, limit),
            ).fetchall()
            return [EventRow(**dict(r)) for r in rows]

        return await self._exec(_work)

    async def get_event(self, event_id: int) -> EventRow | None:
        def _work() -> EventRow | None:
            assert self._conn is not None
            row = self._conn.execute(
                "SELECT * FROM events WHERE id=?", (event_id,)
            ).fetchone()
            return EventRow(**dict(row)) if row else None

        return await self._exec(_work)

    async def update_event_media(
        self,
        event_id: int,
        *,
        snapshot_local_path: str | None = None,
        snapshot_s3_key: str | None = None,
        clip_local_path: str | None = None,
        clip_s3_key: str | None = None,
        media_status: str | None = None,
    ) -> None:
        fields: list[str] = []
        values: list[Any] = []
        for col, val in (
            ("snapshot_local_path", snapshot_local_path),
            ("snapshot_s3_key", snapshot_s3_key),
            ("clip_local_path", clip_local_path),
            ("clip_s3_key", clip_s3_key),
            ("media_status", media_status),
        ):
            if val is not None:
                fields.append(f"{col}=?")
                values.append(val)
        if not fields:
            return
        values.append(event_id)

        def _work() -> None:
            assert self._conn is not None
            self._conn.execute(
                f"UPDATE events SET {', '.join(fields)} WHERE id=?",
                tuple(values),
            )

        await self._exec(_work)
