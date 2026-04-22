"""Nest Recorder integration."""
from __future__ import annotations

import logging
from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.typing import ConfigType
import voluptuous as vol

from .const import (
    DOMAIN,
    SERVICE_RECORD_CLIP,
    SERVICE_RESTORE_FROM_GLACIER,
    SERVICE_REBUILD_INDEX,
    SERVICE_PURGE_LOCAL,
    SERVICE_EXTEND_RETENTION,
)
from .coordinator import NestRecorderCoordinator
from .http_view import SegmentMediaView

_LOGGER = logging.getLogger(__name__)

CONFIG_SCHEMA = vol.Schema({DOMAIN: vol.Schema({})}, extra=vol.ALLOW_EXTRA)

RECORD_CLIP_SCHEMA = vol.Schema(
    {
        vol.Required("entity_id"): str,
        vol.Optional("duration", default=30): vol.All(int, vol.Range(min=5, max=600)),
        vol.Optional("tag"): str,
    }
)

RESTORE_SCHEMA = vol.Schema(
    {
        vol.Exclusive("segment_id", "target"): int,
        vol.Exclusive("range", "target"): vol.Schema(
            {
                vol.Required("entity_id"): str,
                vol.Required("start"): str,
                vol.Required("end"): str,
            }
        ),
        vol.Optional("tier", default="Standard"): vol.In(["Standard", "Expedited", "Bulk"]),
        vol.Optional("days", default=7): vol.All(int, vol.Range(min=1, max=365)),
    }
)

PURGE_SCHEMA = vol.Schema(
    {
        vol.Optional("entity_id"): str,
        vol.Required("older_than_days"): vol.All(int, vol.Range(min=1)),
    }
)

EXTEND_RETENTION_SCHEMA = vol.Schema(
    {
        vol.Required("segment_id"): int,
        vol.Required("until"): str,
    }
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    hass.data.setdefault(DOMAIN, {})
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    coordinator = NestRecorderCoordinator(hass, entry)
    await coordinator.async_start()
    hass.data[DOMAIN][entry.entry_id] = coordinator

    hass.http.register_view(SegmentMediaView(coordinator))

    async def _handle_record_clip(call: ServiceCall) -> None:
        await coordinator.async_record_clip(
            call.data["entity_id"],
            call.data["duration"],
            call.data.get("tag"),
        )

    async def _handle_restore(call: ServiceCall) -> None:
        await coordinator.async_restore(call.data)

    async def _handle_rebuild_index(call: ServiceCall) -> None:
        await coordinator.async_rebuild_index()

    async def _handle_purge_local(call: ServiceCall) -> None:
        await coordinator.async_purge_local(
            call.data.get("entity_id"),
            call.data["older_than_days"],
        )

    async def _handle_extend_retention(call: ServiceCall) -> None:
        await coordinator.async_extend_retention(
            call.data["segment_id"], call.data["until"]
        )

    hass.services.async_register(DOMAIN, SERVICE_RECORD_CLIP, _handle_record_clip, schema=RECORD_CLIP_SCHEMA)
    hass.services.async_register(DOMAIN, SERVICE_RESTORE_FROM_GLACIER, _handle_restore, schema=RESTORE_SCHEMA)
    hass.services.async_register(DOMAIN, SERVICE_REBUILD_INDEX, _handle_rebuild_index)
    hass.services.async_register(DOMAIN, SERVICE_PURGE_LOCAL, _handle_purge_local, schema=PURGE_SCHEMA)
    hass.services.async_register(DOMAIN, SERVICE_EXTEND_RETENTION, _handle_extend_retention, schema=EXTEND_RETENTION_SCHEMA)

    entry.async_on_unload(
        async_track_time_interval(hass, coordinator.async_run_gc, timedelta(hours=1))
    )
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    # MediaSource is auto-discovered by HA from this integration's
    # media_source.py (async_get_media_source). No explicit registration.

    return True


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry) -> None:
    coordinator: NestRecorderCoordinator = hass.data[DOMAIN][entry.entry_id]
    await coordinator.async_apply_options(entry.options)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    coordinator: NestRecorderCoordinator = hass.data[DOMAIN].pop(entry.entry_id)
    await coordinator.async_stop()
    for svc in (
        SERVICE_RECORD_CLIP,
        SERVICE_RESTORE_FROM_GLACIER,
        SERVICE_REBUILD_INDEX,
        SERVICE_PURGE_LOCAL,
        SERVICE_EXTEND_RETENTION,
    ):
        if hass.services.has_service(DOMAIN, svc):
            hass.services.async_remove(DOMAIN, svc)
    return True
