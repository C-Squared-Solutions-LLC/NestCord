"""Coordinator — owns DB, S3 client, camera registry, and runtime tasks."""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import Event, HomeAssistant, callback

from .const import (
    BATTERY_IDLE_TEARDOWN_SECONDS,
    CONF_AWS_ACCESS_KEY_ID,
    CONF_AWS_REGION,
    CONF_AWS_SECRET_ACCESS_KEY,
    CONF_BUCKET,
    CONF_CAMERAS,
    CONF_ENDPOINT_URL,
    CONF_RESTORE_DAYS,
    CONF_RESTORE_TIER,
    CONF_RETENTION_DAYS,
    CONF_SEGMENT_SECONDS,
    CONF_STORAGE_CLASS,
    CONF_STORAGE_ROOT,
    CONF_UPLOAD_CONCURRENCY,
    DEFAULT_RESTORE_DAYS,
    DEFAULT_RESTORE_TIER,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_SEGMENT_SECONDS,
    DEFAULT_STORAGE_CLASS,
    DEFAULT_UPLOAD_CONCURRENCY,
    DOMAIN,
    EVENT_MANUAL,
    KNOWN_EVENT_TYPES,
    NEST_BUS_EVENT,
    RESTORE_POLL_SECONDS,
    CameraKind,
    GlacierStatus,
)
from .db import SegmentStore, Segment
from .glacier import GlacierClient
from .recorder import CameraConfig, RecorderTask
from .storage import (
    prune_empty_dirs,
    s3_key_for,
    safe_unlink,
)
from .stream_source import Go2RtcStreamProvider, RtspStreamProvider, StreamProvider
from .watcher import SegmentWatcher

_LOGGER = logging.getLogger(__name__)


class NestRecorderCoordinator:
    """Wires the DB, S3, camera registry, watcher, and recorders together."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self.hass = hass
        self.entry = entry
        data = entry.data
        options = entry.options

        self.storage_root = Path(data[CONF_STORAGE_ROOT])
        self.glacier = GlacierClient(
            aws_access_key_id=data[CONF_AWS_ACCESS_KEY_ID],
            aws_secret_access_key=data[CONF_AWS_SECRET_ACCESS_KEY],
            region=data[CONF_AWS_REGION],
            bucket=data[CONF_BUCKET],
            endpoint_url=data.get(CONF_ENDPOINT_URL),
        )
        self.store = SegmentStore(hass, self.storage_root / "recorder.db")

        self._recorders: dict[str, RecorderTask] = {}
        self._providers: dict[str, StreamProvider] = {}
        self._watcher: SegmentWatcher | None = None
        self._upload_sem = asyncio.Semaphore(
            int(options.get(CONF_UPLOAD_CONCURRENCY, DEFAULT_UPLOAD_CONCURRENCY))
        )
        self._restore_pollers: dict[int, asyncio.Task] = {}
        self._event_listener_unsub = None
        self._pending_uploads: set[int] = set()

    # -------- lifecycle --------

    async def async_start(self) -> None:
        self.storage_root.mkdir(parents=True, exist_ok=True)
        await self.store.async_open()

        cams = self._discover_cameras()
        for cam in cams:
            await self.store.upsert_camera(
                cam_id=cam["id"],
                device_id=cam["device_id"],
                name=cam["name"],
                kind=cam["kind"].value,
                enabled=cam["enabled"],
            )

        self._watcher = SegmentWatcher(
            self.hass, self.storage_root, self._on_segment_finalized
        )
        await self._watcher.async_start()

        for cam in cams:
            await self._start_recorder(cam)

        self._event_listener_unsub = self.hass.bus.async_listen(
            NEST_BUS_EVENT, self._on_nest_event
        )

    async def async_stop(self) -> None:
        if self._event_listener_unsub is not None:
            self._event_listener_unsub()
            self._event_listener_unsub = None
        for t in list(self._restore_pollers.values()):
            t.cancel()
        self._restore_pollers.clear()
        for rec in list(self._recorders.values()):
            await rec.async_stop()
        self._recorders.clear()
        self._providers.clear()
        if self._watcher is not None:
            await self._watcher.async_stop()
            self._watcher = None
        await self.store.async_close()

    async def async_apply_options(self, options: dict[str, Any]) -> None:
        """Re-apply per-camera toggles and tunables without a full reload."""
        new_conc = int(options.get(CONF_UPLOAD_CONCURRENCY, DEFAULT_UPLOAD_CONCURRENCY))
        self._upload_sem = asyncio.Semaphore(new_conc)
        # Enable/disable per camera
        per_cam: dict[str, dict[str, Any]] = options.get(CONF_CAMERAS, {}) or {}
        for cam_id, rec in list(self._recorders.items()):
            cfg = per_cam.get(cam_id, {})
            if cfg.get("enabled", True) is False:
                await rec.async_stop()
                self._recorders.pop(cam_id, None)
        # Start any camera that became newly enabled.
        for cam in self._discover_cameras():
            if cam["id"] not in self._recorders and cam["enabled"]:
                await self._start_recorder(cam)

    # -------- camera discovery --------

    def _discover_cameras(self) -> list[dict[str, Any]]:
        """Pull Nest cameras out of the entity registry.

        Classification: if the entity reports `frontend_stream_type=hls` (old
        wired cams return HLS backed by RTSP) we treat it as RTSP; anything
        else (WebRTC) falls through to go2rtc.
        """
        from homeassistant.helpers import entity_registry as er

        registry = er.async_get(self.hass)
        cams_opts: dict[str, dict[str, Any]] = self.entry.options.get(CONF_CAMERAS, {}) or {}
        out: list[dict[str, Any]] = []
        for entry in registry.entities.values():
            if entry.platform != "nest" or entry.domain != "camera":
                continue
            entity_id = entry.entity_id
            state = self.hass.states.get(entity_id)
            attrs = state.attributes if state else {}
            stream_type = attrs.get("frontend_stream_type", "")
            kind = CameraKind.RTSP if stream_type in ("hls", "rtsp") else CameraKind.WEBRTC
            cam_id = self._slug(entity_id)
            opts = cams_opts.get(cam_id, {})
            configured_kind = opts.get("kind")
            if configured_kind in (CameraKind.RTSP.value, CameraKind.WEBRTC.value):
                kind = CameraKind(configured_kind)
            out.append(
                {
                    "id": cam_id,
                    "entity_id": entity_id,
                    "device_id": entry.device_id or "",
                    "name": entry.name or entry.original_name or entity_id,
                    "kind": kind,
                    "enabled": opts.get("enabled", True),
                }
            )
        return out

    @staticmethod
    def _slug(entity_id: str) -> str:
        return entity_id.replace(".", "_")

    async def _start_recorder(self, cam: dict[str, Any]) -> None:
        if cam["kind"] == CameraKind.RTSP:
            provider: StreamProvider = RtspStreamProvider(
                self.hass,
                cam["id"],
                cam["entity_id"],
                cam["device_id"],
            )
        else:
            provider = Go2RtcStreamProvider(self.hass, cam["id"], cam["entity_id"])
        self._providers[cam["id"]] = provider

        cfg = CameraConfig(
            camera_id=cam["id"],
            entity_id=cam["entity_id"],
            kind=cam["kind"],
            segment_seconds=int(
                self.entry.options.get(CONF_SEGMENT_SECONDS, DEFAULT_SEGMENT_SECONDS)
            ),
            enabled=cam["enabled"],
        )
        rec = RecorderTask(self.hass, cfg, provider, self.storage_root)
        self._recorders[cam["id"]] = rec
        await rec.async_start()

    # -------- segment finalize path --------

    async def _on_segment_finalized(self, camera_id: str, path: Path) -> None:
        try:
            st = path.stat()
        except FileNotFoundError:
            return
        if st.st_size == 0:
            return
        from .storage import parse_segment_ts

        start_ts = parse_segment_ts(path)
        if start_ts is None:
            _LOGGER.debug("unparseable segment filename: %s", path)
            return
        duration = int(
            self.entry.options.get(CONF_SEGMENT_SECONDS, DEFAULT_SEGMENT_SECONDS)
        )
        end_ts = start_ts + duration

        segment_id = await self.store.insert_segment(
            camera_id=camera_id,
            start_ts=start_ts,
            end_ts=end_ts,
            duration_s=duration,
            local_path=str(path),
            size_bytes=st.st_size,
        )
        self._pending_uploads.add(segment_id)
        await self.store.backfill_segment_ids(camera_id, start_ts, end_ts, segment_id)
        self.hass.async_create_background_task(
            self._upload_segment(segment_id, camera_id, start_ts, path),
            name=f"nest_recorder_upload_{segment_id}",
        )

    async def _upload_segment(
        self, segment_id: int, camera_id: str, start_ts: int, path: Path
    ) -> None:
        try:
            async with self._upload_sem:
                key = s3_key_for(camera_id, start_ts, path.name)
                storage_class = self.entry.options.get(
                    CONF_STORAGE_CLASS, DEFAULT_STORAGE_CLASS
                )
                await self.glacier.async_upload_segment(path, key, storage_class)
                await self.store.mark_archived(segment_id, key)
                _LOGGER.info(
                    "%s: archived segment %s (%s)",
                    camera_id,
                    path.name,
                    storage_class,
                )
        except Exception as err:  # noqa: BLE001
            _LOGGER.error(
                "upload failed for segment %s (%s): %s", segment_id, path, err
            )
            await self.store.mark_failed(segment_id)
        finally:
            self._pending_uploads.discard(segment_id)

    # -------- nest event handling --------

    @callback
    def _on_nest_event(self, event: Event) -> None:
        """HA bus event from the nest integration — data carries SDM details."""
        self.hass.async_create_task(self._handle_nest_event(event.data))

    async def _handle_nest_event(self, data: dict[str, Any]) -> None:
        device_id = data.get("device_id") or data.get("nest_event_id")
        raw_type = data.get("type") or ""
        if not device_id or not raw_type:
            return
        event_type = self._normalize_event_type(raw_type)
        if event_type not in KNOWN_EVENT_TYPES:
            return
        cams = await self.store.list_cameras()
        camera_id: str | None = None
        for c in cams:
            if c["device_id"] == device_id:
                camera_id = c["id"]
                break
        if camera_id is None:
            return
        ts = int(time.time())
        session_id = data.get("nest_event_id") or data.get("event_session_id")
        open_seg = await self.store.open_segment_for(camera_id, ts)
        await self.store.insert_event(
            camera_id=camera_id,
            type_=event_type,
            timestamp=ts,
            session_id=session_id,
            segment_id=open_seg.id if open_seg else None,
        )

    @staticmethod
    def _normalize_event_type(raw: str) -> str:
        """Map nest integration event types to our KNOWN_EVENT_TYPES constants."""
        key = raw.lower()
        if "motion" in key:
            return "camera_motion"
        if "person" in key:
            return "camera_person"
        if "sound" in key:
            return "camera_sound"
        if "doorbell" in key or "chime" in key:
            return "doorbell_chime"
        return key

    # -------- services --------

    async def async_record_clip(
        self, entity_id: str, duration: int, tag: str | None
    ) -> None:
        """Manual clip: insert a manual event tied to the current segment."""
        camera_id = self._slug(entity_id)
        ts = int(time.time())
        open_seg = await self.store.open_segment_for(camera_id, ts)
        await self.store.insert_event(
            camera_id=camera_id,
            type_=EVENT_MANUAL,
            timestamp=ts,
            session_id=tag,
            segment_id=open_seg.id if open_seg else None,
        )

    async def async_restore(self, data: dict[str, Any]) -> None:
        tier = data.get("tier", DEFAULT_RESTORE_TIER)
        days = int(data.get("days", DEFAULT_RESTORE_DAYS))
        targets: list[int] = []
        if "segment_id" in data:
            targets.append(int(data["segment_id"]))
        elif "range" in data:
            rng = data["range"]
            camera_id = self._slug(rng["entity_id"])
            start = int(datetime.fromisoformat(rng["start"]).timestamp())
            end = int(datetime.fromisoformat(rng["end"]).timestamp())
            segs = await self.store.segments_for_day(camera_id, start, end)
            targets.extend(s.id for s in segs if s.local_path is None)
        if not targets:
            _LOGGER.warning("restore: no matching archived segments")
            return
        for sid in targets:
            seg = await self.store.get_segment(sid)
            if seg is None or not seg.s3_key:
                continue
            if seg.glacier_status == GlacierStatus.RESTORING.value:
                continue
            await self.glacier.async_initiate_restore(seg.s3_key, tier, days)
            await self.store.mark_restore_requested(seg.id, int(time.time()))
            self._restore_pollers[seg.id] = self.hass.async_create_background_task(
                self._poll_restore(seg.id, seg.s3_key),
                name=f"nest_recorder_restore_{seg.id}",
            )

    async def _poll_restore(self, segment_id: int, key: str) -> None:
        try:
            while True:
                await asyncio.sleep(RESTORE_POLL_SECONDS)
                state = await self.glacier.async_head_object(key)
                if state is None:
                    continue
                if not state.ongoing:
                    expires_ts: int | None = None
                    if state.expiry_date:
                        try:
                            expires_ts = int(
                                datetime.strptime(
                                    state.expiry_date, "%a, %d %b %Y %H:%M:%S %Z"
                                ).replace(tzinfo=timezone.utc).timestamp()
                            )
                        except ValueError:
                            expires_ts = None
                    await self.store.mark_restored(
                        segment_id, int(time.time()), expires_ts
                    )
                    self.hass.async_create_task(
                        self._fire_restore_notice(segment_id, key)
                    )
                    return
        except asyncio.CancelledError:
            pass
        finally:
            self._restore_pollers.pop(segment_id, None)

    async def _fire_restore_notice(self, segment_id: int, key: str) -> None:
        await self.hass.services.async_call(
            "persistent_notification",
            "create",
            {
                "title": "Nest Recorder: restore ready",
                "message": (
                    f"Segment #{segment_id} ({key}) has been restored from Glacier "
                    f"and is available in the Media browser."
                ),
                "notification_id": f"nest_recorder_restore_{segment_id}",
            },
        )

    async def async_rebuild_index(self) -> None:
        from .storage import parse_segment_ts

        count = 0
        for path in self.storage_root.rglob("*.mp4"):
            try:
                rel = path.relative_to(self.storage_root)
            except ValueError:
                continue
            parts = rel.parts
            if len(parts) < 2:
                continue
            camera_id = parts[0]
            start_ts = parse_segment_ts(path)
            if start_ts is None:
                continue
            seg = await self.store.open_segment_for(camera_id, start_ts)
            if seg is not None and seg.local_path == str(path):
                continue
            st = path.stat()
            duration = int(
                self.entry.options.get(CONF_SEGMENT_SECONDS, DEFAULT_SEGMENT_SECONDS)
            )
            await self.store.insert_segment(
                camera_id=camera_id,
                start_ts=start_ts,
                end_ts=start_ts + duration,
                duration_s=duration,
                local_path=str(path),
                size_bytes=st.st_size,
            )
            count += 1
        _LOGGER.info("rebuild_index: added %s segments from disk", count)

    async def async_purge_local(
        self, entity_id: str | None, older_than_days: int
    ) -> None:
        cutoff = int(time.time()) - older_than_days * 86400
        segs = await self.store.segments_expiring_local(cutoff)
        if entity_id is not None:
            wanted = self._slug(entity_id)
            segs = [s for s in segs if s.camera_id == wanted]
        deleted = 0
        for seg in segs:
            if seg.local_path:
                safe_unlink(Path(seg.local_path))
            await self.store.clear_local_path(seg.id)
            deleted += 1
        prune_empty_dirs(self.storage_root)
        _LOGGER.info("purge_local: cleared %s local copies", deleted)

    async def async_extend_retention(self, segment_id: int, until: str) -> None:
        until_ts = int(datetime.fromisoformat(until).timestamp())
        await self.store.extend_retention(segment_id, until_ts)

    # -------- GC --------

    async def async_run_gc(self, _now: Any = None) -> None:
        retention_days = int(
            self.entry.options.get(CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS)
        )
        cutoff = int(time.time()) - retention_days * 86400
        segs = await self.store.segments_expiring_local(cutoff)
        if not segs:
            return
        deleted = 0
        for seg in segs:
            if seg.id in self._pending_uploads:
                continue  # safety interlock
            if seg.local_path:
                safe_unlink(Path(seg.local_path))
            await self.store.clear_local_path(seg.id)
            deleted += 1
        prune_empty_dirs(self.storage_root)
        if deleted:
            _LOGGER.info("gc: evicted %s local segments past retention", deleted)
