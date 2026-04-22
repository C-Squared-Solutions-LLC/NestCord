"""Per-camera recording task and segment-finalized watcher."""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from homeassistant.components.ffmpeg import get_ffmpeg_manager
from homeassistant.core import HomeAssistant, State, callback
from homeassistant.helpers.event import async_track_state_change_event

from .const import (
    BATTERY_IDLE_TEARDOWN_SECONDS,
    CameraKind,
    FFMPEG_RELAUNCH_BACKOFF_MAX,
    FFMPEG_RELAUNCH_BACKOFF_MIN,
)
from .storage import ensure_dirs, segment_pattern
from .stream_source import StreamProvider

_LOGGER = logging.getLogger(__name__)


@dataclass
class CameraConfig:
    camera_id: str
    entity_id: str
    kind: CameraKind
    segment_seconds: int
    enabled: bool


class RecorderTask:
    """Runs one FFmpeg subprocess per camera with supervised restart."""

    def __init__(
        self,
        hass: HomeAssistant,
        cfg: CameraConfig,
        provider: StreamProvider,
        storage_root: Path,
    ) -> None:
        self._hass = hass
        self._cfg = cfg
        self._provider = provider
        self._storage_root = storage_root
        self._task: asyncio.Task | None = None
        self._proc: asyncio.subprocess.Process | None = None
        self._stop_event = asyncio.Event()
        self._battery_active = True  # RTSP cams are always "active"
        self._idle_since: float | None = None
        self._state_unsub: Callable[[], None] | None = None

    @property
    def camera_id(self) -> str:
        return self._cfg.camera_id

    async def async_start(self) -> None:
        if not self._cfg.enabled:
            _LOGGER.info("%s disabled, skipping", self._cfg.camera_id)
            return
        self._stop_event.clear()
        if self._cfg.kind == CameraKind.WEBRTC:
            self._state_unsub = async_track_state_change_event(
                self._hass, [self._cfg.entity_id], self._on_entity_state
            )
            state = self._hass.states.get(self._cfg.entity_id)
            self._battery_active = self._is_active_state(state)
        ensure_dirs(self._storage_root, self._cfg.camera_id)
        self._task = self._hass.async_create_background_task(
            self._supervisor(), name=f"nest_recorder_ffmpeg_{self._cfg.camera_id}"
        )

    async def async_stop(self) -> None:
        self._stop_event.set()
        if self._state_unsub is not None:
            self._state_unsub()
            self._state_unsub = None
        await self._kill_proc()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        await self._provider.async_release()

    async def _supervisor(self) -> None:
        backoff = FFMPEG_RELAUNCH_BACKOFF_MIN
        while not self._stop_event.is_set():
            if self._cfg.kind == CameraKind.WEBRTC and not self._battery_active:
                await asyncio.sleep(2)
                continue
            try:
                handle = await self._provider.async_acquire()
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning(
                    "%s: stream acquire failed: %s", self._cfg.camera_id, err
                )
                await asyncio.sleep(backoff)
                backoff = min(FFMPEG_RELAUNCH_BACKOFF_MAX, backoff * 2)
                continue

            rc = await self._run_ffmpeg(handle.url)
            if self._stop_event.is_set():
                break
            if rc == 0:
                _LOGGER.info(
                    "%s: ffmpeg exited cleanly, relaunching", self._cfg.camera_id
                )
                backoff = FFMPEG_RELAUNCH_BACKOFF_MIN
            else:
                _LOGGER.warning(
                    "%s: ffmpeg exited rc=%s, backoff %.1fs",
                    self._cfg.camera_id,
                    rc,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(FFMPEG_RELAUNCH_BACKOFF_MAX, backoff * 2)

    async def _run_ffmpeg(self, url: str) -> int:
        binary = get_ffmpeg_manager(self._hass).binary
        pattern = segment_pattern(self._storage_root, self._cfg.camera_id)
        # Ensure at least today's dir exists so FFmpeg can write.
        ensure_dirs(self._storage_root, self._cfg.camera_id)

        common_tail = [
            "-map", "0:v", "-map", "0:a?",
            "-c", "copy",
            "-f", "segment",
            "-segment_time", str(self._cfg.segment_seconds),
            "-segment_format", "mp4",
            "-reset_timestamps", "1",
            "-strftime", "1",
            "-movflags", "+faststart+frag_keyframe+empty_moov",
            "-segment_atclocktime", "1",
            pattern,
        ]

        if self._cfg.kind == CameraKind.RTSP:
            args = [
                binary,
                "-hide_banner", "-loglevel", "warning",
                "-rtsp_transport", "tcp",
                "-use_wallclock_as_timestamps", "1",
                "-i", url,
                *common_tail,
            ]
        else:
            args = [
                binary,
                "-hide_banner", "-loglevel", "warning",
                "-fflags", "+genpts",
                "-rtsp_transport", "tcp",
                "-reconnect", "1",
                "-reconnect_streamed", "1",
                "-reconnect_delay_max", "30",
                "-i", url,
                *common_tail,
            ]

        _LOGGER.debug("%s: exec %s", self._cfg.camera_id, " ".join(args))
        self._proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        stderr_task = self._hass.async_create_background_task(
            self._drain_stderr(self._proc),
            name=f"nest_recorder_stderr_{self._cfg.camera_id}",
        )

        stop_waiter = self._hass.async_create_background_task(
            self._watch_stop(), name=f"nest_recorder_stopw_{self._cfg.camera_id}"
        )

        try:
            rc = await self._proc.wait()
        finally:
            stop_waiter.cancel()
            stderr_task.cancel()
        return rc

    async def _watch_stop(self) -> None:
        await self._stop_event.wait()
        await self._kill_proc()

    async def _kill_proc(self) -> None:
        if self._proc is None or self._proc.returncode is not None:
            return
        try:
            self._proc.terminate()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self._proc.kill()
                await self._proc.wait()
        except ProcessLookupError:
            pass

    async def _drain_stderr(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stderr is not None
        try:
            async for line in proc.stderr:
                _LOGGER.debug(
                    "%s ffmpeg: %s", self._cfg.camera_id, line.decode("utf-8", "replace").rstrip()
                )
        except asyncio.CancelledError:
            pass

    @callback
    def _on_entity_state(self, event: Any) -> None:
        new: State | None = event.data.get("new_state")
        active = self._is_active_state(new)
        if active == self._battery_active:
            return
        self._battery_active = active
        if active:
            _LOGGER.info("%s: source entity active, starting recording", self._cfg.camera_id)
            self._idle_since = None
        else:
            self._idle_since = time.time()
            self._hass.async_create_background_task(
                self._teardown_after_idle(),
                name=f"nest_recorder_teardown_{self._cfg.camera_id}",
            )

    async def _teardown_after_idle(self) -> None:
        started_idle = self._idle_since
        await asyncio.sleep(BATTERY_IDLE_TEARDOWN_SECONDS)
        if self._idle_since != started_idle:
            return  # became active again
        if self._battery_active:
            return
        _LOGGER.info("%s: idle, stopping recording", self._cfg.camera_id)
        await self._kill_proc()

    @staticmethod
    def _is_active_state(state: State | None) -> bool:
        if state is None:
            return False
        return state.state in ("streaming", "recording", "on", "idle") and state.state != "unavailable"
