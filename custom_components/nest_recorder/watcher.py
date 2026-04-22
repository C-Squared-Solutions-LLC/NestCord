"""Filesystem watcher that fires when FFmpeg finalizes a segment."""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Awaitable, Callable

_LOGGER = logging.getLogger(__name__)

FinalizeCallback = Callable[[str, Path], Awaitable[None]]


class SegmentWatcher:
    """Watches the storage root for new .mp4 segments across all cameras.

    Uses watchdog where available; falls back to a polling loop for environments
    where watchdog's native observer isn't supported.
    """

    def __init__(
        self,
        hass,
        storage_root: Path,
        finalize_cb: FinalizeCallback,
        debounce_seconds: float = 3.0,
    ) -> None:
        self._hass = hass
        self._storage_root = storage_root
        self._cb = finalize_cb
        self._debounce = debounce_seconds
        self._observer = None
        self._poll_task: asyncio.Task | None = None
        self._seen: dict[Path, float] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending: dict[Path, asyncio.TimerHandle] = {}

    async def async_start(self) -> None:
        self._loop = asyncio.get_running_loop()
        await self._hass.async_add_executor_job(
            lambda: self._storage_root.mkdir(parents=True, exist_ok=True)
        )
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler

            outer = self

            class _Handler(FileSystemEventHandler):
                def on_closed(self, event):
                    if not event.is_directory and str(event.src_path).endswith(".mp4"):
                        outer._schedule_finalize(Path(event.src_path), delay=0.5)

                def on_modified(self, event):
                    if not event.is_directory and str(event.src_path).endswith(".mp4"):
                        outer._schedule_finalize(Path(event.src_path), delay=outer._debounce)

                def on_moved(self, event):
                    dest = getattr(event, "dest_path", None)
                    if dest and str(dest).endswith(".mp4"):
                        outer._schedule_finalize(Path(dest), delay=0.5)

            def _build_and_start():
                observer = Observer()
                observer.schedule(_Handler(), str(self._storage_root), recursive=True)
                observer.start()
                return observer

            self._observer = await self._hass.async_add_executor_job(_build_and_start)
            _LOGGER.debug("watchdog observer started on %s", self._storage_root)
        except Exception as err:  # noqa: BLE001 - watchdog optional
            _LOGGER.warning(
                "watchdog unavailable (%s); falling back to polling watcher", err
            )
            self._poll_task = self._hass.async_create_background_task(
                self._poll_loop(), name="nest_recorder_watcher_poll"
            )

    async def async_stop(self) -> None:
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None
        if self._poll_task is not None:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        for h in self._pending.values():
            h.cancel()
        self._pending.clear()

    def _schedule_finalize(self, path: Path, delay: float) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        self._loop.call_soon_threadsafe(self._arm_debounce, path, delay)

    def _arm_debounce(self, path: Path, delay: float) -> None:
        existing = self._pending.get(path)
        if existing is not None:
            existing.cancel()
        handle = self._loop.call_later(delay, self._fire, path)
        self._pending[path] = handle

    def _fire(self, path: Path) -> None:
        self._pending.pop(path, None)
        if not path.exists():
            return
        # Only finalize if the file size is stable (not actively being written)
        try:
            size = path.stat().st_size
        except OSError:
            return
        if size == 0:
            return
        seen_at = self._seen.get(path)
        if seen_at is not None and time.time() - seen_at < 1.0:
            return
        self._seen[path] = time.time()
        camera_id = self._extract_camera_id(path)
        if camera_id is None:
            return
        self._hass.async_create_task(self._cb(camera_id, path))

    def _extract_camera_id(self, path: Path) -> str | None:
        try:
            rel = path.relative_to(self._storage_root)
        except ValueError:
            return None
        parts = rel.parts
        if len(parts) < 2:
            return None
        return parts[0]

    async def _poll_loop(self) -> None:
        known: dict[Path, tuple[int, float]] = {}
        try:
            while True:
                for p in self._storage_root.rglob("*.mp4"):
                    try:
                        st = p.stat()
                    except OSError:
                        continue
                    prev = known.get(p)
                    key = (st.st_size, st.st_mtime)
                    if prev is None:
                        known[p] = key
                        continue
                    if prev == key and p not in self._seen:
                        # Stable for one cycle -> finalize.
                        self._seen[p] = time.time()
                        camera_id = self._extract_camera_id(p)
                        if camera_id:
                            await self._cb(camera_id, p)
                    known[p] = key
                await asyncio.sleep(self._debounce)
        except asyncio.CancelledError:
            pass
