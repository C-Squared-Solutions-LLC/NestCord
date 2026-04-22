"""Media source: exposes local + archived recordings in the HA Media browser."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from homeassistant.components.media_player import MediaClass
from homeassistant.components.media_source import (
    BrowseMediaSource,
    MediaSource,
    MediaSourceItem,
    PlayMedia,
)
from homeassistant.core import HomeAssistant

from .const import DOMAIN, HTTP_VIEW_URL, GlacierStatus
from .coordinator import NestRecorderCoordinator


async def async_get_media_source(hass: HomeAssistant) -> "NestRecorderMediaSource":
    return NestRecorderMediaSource(hass)


class NestRecorderMediaSource(MediaSource):
    name = "Nest Recorder"

    def __init__(self, hass: HomeAssistant) -> None:
        super().__init__(DOMAIN)
        self.hass = hass

    def _coordinators(self) -> list[NestRecorderCoordinator]:
        bucket = self.hass.data.get(DOMAIN, {}) or {}
        return [c for c in bucket.values() if isinstance(c, NestRecorderCoordinator)]

    async def async_resolve_media(self, item: MediaSourceItem) -> PlayMedia:
        ident = item.identifier or ""
        parts = ident.split("/")
        if len(parts) < 2 or parts[0] != "segment":
            raise ValueError(f"cannot resolve {ident}")
        segment_id = int(parts[1])
        for coord in self._coordinators():
            seg = await coord.store.get_segment(segment_id)
            if seg is None:
                continue
            if seg.local_path is None:
                # Return a placeholder URL; HA will surface the URL in the player
                # but the user should call restore_from_glacier first.
                return PlayMedia(
                    url=f"data:text/plain,archived%20segment%20{segment_id}",
                    mime_type="text/plain",
                )
            return PlayMedia(
                url=HTTP_VIEW_URL.format(segment_id=segment_id),
                mime_type="video/mp4",
            )
        raise ValueError(f"segment {segment_id} not found")

    async def async_browse_media(self, item: MediaSourceItem) -> BrowseMediaSource:
        ident = item.identifier or ""
        if ident == "":
            return await self._browse_root()
        parts = ident.split("/")
        if parts[0] == "camera" and len(parts) == 2:
            return await self._browse_camera(parts[1])
        if parts[0] == "camera" and len(parts) == 3 and parts[2] == "events":
            return await self._browse_events(parts[1])
        if parts[0] == "camera" and len(parts) == 3 and parts[2] == "timeline":
            return await self._browse_timeline_root(parts[1])
        if parts[0] == "camera" and len(parts) == 6 and parts[2] == "timeline":
            return await self._browse_timeline_day(parts[1], parts[3], parts[4], parts[5])
        if parts[0] == "camera" and len(parts) == 3 and parts[2] == "archived":
            return await self._browse_archived(parts[1])
        raise ValueError(f"unknown identifier {ident}")

    # ----- tree nodes -----

    async def _browse_root(self) -> BrowseMediaSource:
        children: list[BrowseMediaSource] = []
        for coord in self._coordinators():
            for cam in await coord.store.list_cameras():
                children.append(self._dir(f"camera/{cam['id']}", cam["name"]))
        return self._dir("", "Nest Recorder", children=children)

    async def _browse_camera(self, cam_id: str) -> BrowseMediaSource:
        children = [
            self._dir(f"camera/{cam_id}/events", "Events"),
            self._dir(f"camera/{cam_id}/timeline", "Timeline"),
            self._dir(f"camera/{cam_id}/archived", "Archived (Glacier)"),
        ]
        return self._dir(f"camera/{cam_id}", cam_id, children=children)

    async def _browse_events(self, cam_id: str) -> BrowseMediaSource:
        children: list[BrowseMediaSource] = []
        for coord in self._coordinators():
            events = await coord.store.all_events(cam_id)
            for ev in events:
                ts = datetime.fromtimestamp(ev.timestamp, tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                title = f"{ts} — {ev.type}"
                if ev.segment_id is None:
                    children.append(self._leaf_unavailable(f"event/{ev.id}", title))
                else:
                    seg = await coord.store.get_segment(ev.segment_id)
                    if seg is None:
                        continue
                    children.append(self._segment_leaf(seg, title_override=title))
            break
        return self._dir(f"camera/{cam_id}/events", "Events", children=children)

    async def _browse_timeline_root(self, cam_id: str) -> BrowseMediaSource:
        children: list[BrowseMediaSource] = []
        for coord in self._coordinators():
            for day_ts in await coord.store.distinct_days(cam_id):
                d = datetime.fromtimestamp(day_ts, tz=timezone.utc)
                ident = (
                    f"camera/{cam_id}/timeline/{d.year:04d}/{d.month:02d}/{d.day:02d}"
                )
                children.append(self._dir(ident, d.strftime("%Y-%m-%d")))
            break
        return self._dir(f"camera/{cam_id}/timeline", "Timeline", children=children)

    async def _browse_timeline_day(
        self, cam_id: str, year: str, month: str, day: str
    ) -> BrowseMediaSource:
        children: list[BrowseMediaSource] = []
        start = int(
            datetime(int(year), int(month), int(day), tzinfo=timezone.utc).timestamp()
        )
        end = start + 86400
        for coord in self._coordinators():
            for seg in await coord.store.segments_for_day(cam_id, start, end):
                children.append(self._segment_leaf(seg))
            break
        return self._dir(
            f"camera/{cam_id}/timeline/{year}/{month}/{day}",
            f"{year}-{month}-{day}",
            children=children,
        )

    async def _browse_archived(self, cam_id: str) -> BrowseMediaSource:
        children: list[BrowseMediaSource] = []
        for coord in self._coordinators():
            for seg in await coord.store.archived_only(cam_id):
                children.append(self._segment_leaf(seg))
            break
        return self._dir(f"camera/{cam_id}/archived", "Archived", children=children)

    # ----- helpers -----

    def _dir(
        self,
        identifier: str,
        title: str,
        *,
        children: list[BrowseMediaSource] | None = None,
    ) -> BrowseMediaSource:
        return BrowseMediaSource(
            domain=DOMAIN,
            identifier=identifier,
            media_class=MediaClass.DIRECTORY,
            media_content_type="",
            title=title,
            can_play=False,
            can_expand=True,
            children_media_class=MediaClass.VIDEO,
            children=children or [],
        )

    def _segment_leaf(
        self, seg, *, title_override: str | None = None
    ) -> BrowseMediaSource:
        ts_s = datetime.fromtimestamp(seg.start_ts, tz=timezone.utc).strftime(
            "%H:%M:%S"
        )
        title = title_override or ts_s
        archived = seg.local_path is None
        if archived:
            if seg.glacier_status == GlacierStatus.RESTORING.value:
                title = f"{title}  (restoring…)"
            elif seg.glacier_status == GlacierStatus.RESTORED.value:
                title = f"{title}  (restored — download manually)"
            else:
                title = f"{title}  (archived — call restore_from_glacier)"
        return BrowseMediaSource(
            domain=DOMAIN,
            identifier=f"segment/{seg.id}",
            media_class=MediaClass.VIDEO,
            media_content_type="video/mp4",
            title=title,
            can_play=not archived,
            can_expand=False,
        )

    def _leaf_unavailable(self, identifier: str, title: str) -> BrowseMediaSource:
        return BrowseMediaSource(
            domain=DOMAIN,
            identifier=identifier,
            media_class=MediaClass.VIDEO,
            media_content_type="",
            title=f"{title}  (no segment)",
            can_play=False,
            can_expand=False,
        )
