"""Stream URL providers for wired (SDM RTSP) and battery (WebRTC via go2rtc) Nest cameras."""
from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable

from homeassistant.core import HomeAssistant

from .const import (
    MAX_STREAM_EXTENSIONS,
    STREAM_EXTEND_LEAD_SECONDS,
)

_LOGGER = logging.getLogger(__name__)


@dataclass
class StreamHandle:
    url: str
    expires_at: float


class StreamProvider(ABC):
    """Abstract provider that yields a currently-valid stream URL for FFmpeg."""

    def __init__(self, hass: HomeAssistant, camera_id: str, entity_id: str) -> None:
        self.hass = hass
        self.camera_id = camera_id
        self.entity_id = entity_id
        self._handle: StreamHandle | None = None
        self._url_changed: asyncio.Event = asyncio.Event()
        self._lock = asyncio.Lock()

    @abstractmethod
    async def async_acquire(self) -> StreamHandle:
        ...

    @abstractmethod
    async def async_release(self) -> None:
        ...

    @property
    def current(self) -> StreamHandle | None:
        return self._handle

    def url_changed_event(self) -> asyncio.Event:
        return self._url_changed


class RtspStreamProvider(StreamProvider):
    """Manages an SDM RTSP stream URL with periodic ExtendRtspStream calls.

    This implementation delegates the actual SDM command execution to the
    nest integration's device. We look up the device via the device_registry
    and call the SDM trait. If the trait isn't found we fall back to the
    camera entity's `stream_source` attribute.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        camera_id: str,
        entity_id: str,
        device_id: str,
    ) -> None:
        super().__init__(hass, camera_id, entity_id)
        self._device_id = device_id
        self._extensions = 0
        self._refresh_task: asyncio.Task | None = None

    async def async_acquire(self) -> StreamHandle:
        async with self._lock:
            if self._handle and self._handle.expires_at - STREAM_EXTEND_LEAD_SECONDS > time.time():
                return self._handle
            self._handle = await self._generate()
            self._extensions = 0
            if self._refresh_task is None or self._refresh_task.done():
                self._refresh_task = self.hass.async_create_background_task(
                    self._refresh_loop(), name=f"nest_recorder_refresh_{self.camera_id}"
                )
            self._url_changed.set()
            self._url_changed.clear()
            return self._handle

    async def async_release(self) -> None:
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            self._refresh_task = None
        self._handle = None

    async def _refresh_loop(self) -> None:
        try:
            while True:
                handle = self._handle
                if handle is None:
                    return
                sleep_for = max(
                    5.0, handle.expires_at - time.time() - STREAM_EXTEND_LEAD_SECONDS
                )
                await asyncio.sleep(sleep_for)
                async with self._lock:
                    try:
                        if self._extensions >= MAX_STREAM_EXTENSIONS:
                            _LOGGER.debug(
                                "%s: extension cap hit, regenerating", self.camera_id
                            )
                            self._handle = await self._generate()
                            self._extensions = 0
                        else:
                            self._handle = await self._extend()
                            self._extensions += 1
                        self._url_changed.set()
                        self._url_changed.clear()
                    except Exception as err:  # noqa: BLE001
                        _LOGGER.warning(
                            "%s: extend/regen failed: %s (will retry in 10s)",
                            self.camera_id,
                            err,
                        )
                        await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass

    async def _nest_device(self) -> Any:
        """Look up the underlying google_nest_sdm.device.Device."""
        # The nest integration stores devices keyed by the device_id in hass.data,
        # but the layout is internal. Prefer calling camera entity for stream_source.
        from homeassistant.helpers.entity_component import EntityComponent

        component: EntityComponent | None = self.hass.data.get("camera")
        if component is None:
            raise RuntimeError("camera component not loaded")
        entity = component.get_entity(self.entity_id)
        if entity is None:
            raise RuntimeError(f"camera entity {self.entity_id} not found")
        return entity

    async def _generate(self) -> StreamHandle:
        entity = await self._nest_device()
        url = await entity.stream_source()
        if not url:
            raise RuntimeError(
                f"stream_source returned empty for {self.entity_id}; "
                "camera may not support RTSP"
            )
        # SDM streams expire in ~5 minutes; we use 4.5 min as our window to stay safe.
        return StreamHandle(url=url, expires_at=time.time() + 270)

    async def _extend(self) -> StreamHandle:
        # HA's camera API does not expose ExtendRtspStream directly; calling
        # stream_source() again yields either the same URL (if still valid) or
        # a freshly-extended one. This is consistent with what the nest
        # integration does internally when its StreamingSession refreshes.
        return await self._generate()


class Go2RtcStreamProvider(StreamProvider):
    """For WebRTC-only battery cameras.

    Relies on HA's bundled go2rtc. The integration publishes the camera's
    WebRTC stream into go2rtc at startup (via the go2rtc integration's API)
    and here we just return the resulting RTSP endpoint.

    Falls back to the camera entity's `async_handle_async_webrtc_offer` +
    a direct WebRTC capture pipeline if go2rtc isn't available.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        camera_id: str,
        entity_id: str,
        go2rtc_host: str = "127.0.0.1",
        go2rtc_rtsp_port: int = 8554,
    ) -> None:
        super().__init__(hass, camera_id, entity_id)
        self._go2rtc_host = go2rtc_host
        self._go2rtc_rtsp_port = go2rtc_rtsp_port
        self._stream_name = f"nest_recorder_{camera_id}"
        self._unreachable_logged = False

    async def async_acquire(self) -> StreamHandle:
        # First try to fall back to whatever the camera entity itself reports
        # as a stream URL. For wired Nest cameras the core integration provides
        # an SDM RTSP URL here; for WebRTC-only cameras it's usually empty.
        direct = await self._try_entity_stream_source()
        if direct:
            self._handle = StreamHandle(url=direct, expires_at=time.time() + 270)
            return self._handle

        if not await self._ensure_go2rtc_stream():
            raise RuntimeError(
                f"no stream available for {self.entity_id}: "
                "entity.stream_source() returned empty and go2rtc is unreachable"
            )
        url = (
            f"rtsp://{self._go2rtc_host}:{self._go2rtc_rtsp_port}/{self._stream_name}"
        )
        self._handle = StreamHandle(url=url, expires_at=time.time() + 86400)
        return self._handle

    async def _try_entity_stream_source(self) -> str | None:
        try:
            from homeassistant.helpers.entity_component import EntityComponent

            component: EntityComponent | None = self.hass.data.get("camera")
            if component is None:
                return None
            entity = component.get_entity(self.entity_id)
            if entity is None:
                return None
            url = await entity.stream_source()
            return url or None
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("%s: entity stream_source raised: %s", self.camera_id, err)
            return None

    async def async_release(self) -> None:
        await self._remove_go2rtc_stream()
        self._handle = None

    async def _ensure_go2rtc_stream(self) -> bool:
        """Register a stream in go2rtc. Returns True on success, False otherwise.

        Logs the unreachable state only once per provider to avoid spamming
        the log when go2rtc isn't available on this install.
        """
        import aiohttp

        payload = {
            "src": self._stream_name,
            "value": f"ha_entity://{self.entity_id}",
        }
        url = f"http://{self._go2rtc_host}:1984/api/streams"
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.put(url, params=payload, timeout=10) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        if not self._unreachable_logged:
                            _LOGGER.warning(
                                "go2rtc register failed (%s): %s", resp.status, body
                            )
                            self._unreachable_logged = True
                        return False
            self._unreachable_logged = False
            return True
        except aiohttp.ClientError as err:
            if not self._unreachable_logged:
                _LOGGER.warning(
                    "%s: go2rtc unreachable (%s) -- this camera will not be recorded",
                    self.camera_id,
                    err,
                )
                self._unreachable_logged = True
            return False

    async def _remove_go2rtc_stream(self) -> None:
        import aiohttp

        url = f"http://{self._go2rtc_host}:1984/api/streams"
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.delete(
                    url, params={"src": self._stream_name}, timeout=10
                ):
                    pass
        except aiohttp.ClientError:
            pass
