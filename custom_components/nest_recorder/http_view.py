"""Authenticated HTTP view that serves local segment files to the Media browser."""
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
