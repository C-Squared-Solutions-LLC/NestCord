"""Authenticated HTTP views that serve local segment + event files to the Media browser."""
from __future__ import annotations

from pathlib import Path

from aiohttp import web

from homeassistant.components.http import HomeAssistantView

from .coordinator import NestRecorderCoordinator


class SegmentMediaView(HomeAssistantView):
    """GET /api/nest_recorder/segment/<segment_id> → video/mp4 stream."""

    url = "/api/nest_recorder/segment/{segment_id}"
    name = "api:nest_recorder:segment"
    requires_auth = True

    def __init__(self, coordinator: NestRecorderCoordinator) -> None:
        self._coordinator = coordinator

    async def get(self, request: web.Request, segment_id: str) -> web.Response:
        try:
            sid = int(segment_id)
        except ValueError:
            return web.Response(status=400, text="bad segment id")
        seg = await self._coordinator.store.get_segment(sid)
        if seg is None:
            return web.Response(status=404, text="not found")
        if not seg.local_path:
            return web.Response(
                status=409,
                text="segment archived to Glacier; call nest_recorder.restore_from_glacier",
            )
        path = Path(seg.local_path)
        if not path.exists():
            return web.Response(status=410, text="local file missing")
        return web.FileResponse(path, headers={"Content-Type": "video/mp4"})


class EventMediaView(HomeAssistantView):
    """GET /api/nest_recorder/event/<event_id>/<kind> → jpg or mp4.

    kind is "snapshot" or "clip".
    """

    url = "/api/nest_recorder/event/{event_id}/{kind}"
    name = "api:nest_recorder:event"
    requires_auth = True

    def __init__(self, coordinator: NestRecorderCoordinator) -> None:
        self._coordinator = coordinator

    async def get(
        self, request: web.Request, event_id: str, kind: str
    ) -> web.Response:
        try:
            eid = int(event_id)
        except ValueError:
            return web.Response(status=400, text="bad event id")
        if kind not in ("snapshot", "clip"):
            return web.Response(status=400, text="kind must be snapshot or clip")
        ev = await self._coordinator.store.get_event(eid)
        if ev is None:
            return web.Response(status=404, text="event not found")
        if kind == "snapshot":
            path_str = ev.snapshot_local_path
            mime = "image/jpeg"
        else:
            path_str = ev.clip_local_path
            mime = "video/mp4"
        if not path_str:
            return web.Response(status=404, text=f"event has no {kind}")
        path = Path(path_str)
        if not path.exists():
            return web.Response(status=410, text="local file missing")
        return web.FileResponse(path, headers={"Content-Type": mime})
