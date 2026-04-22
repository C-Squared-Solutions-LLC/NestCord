"""Config + options flow for Nest Recorder."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult

from .const import (
    CONF_AWS_ACCESS_KEY_ID,
    CONF_AWS_REGION,
    CONF_AWS_SECRET_ACCESS_KEY,
    CONF_BUCKET,
    CONF_CAMERAS,
    CONF_ENDPOINT_URL,
    CONF_RETENTION_DAYS,
    CONF_SEGMENT_SECONDS,
    CONF_STORAGE_CLASS,
    CONF_STORAGE_ROOT,
    CONF_UPLOAD_CONCURRENCY,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_SEGMENT_SECONDS,
    DEFAULT_STORAGE_CLASS,
    DEFAULT_UPLOAD_CONCURRENCY,
    DOMAIN,
    EARLY_DELETE_DAYS,
    VALID_STORAGE_CLASSES,
)
from .glacier import GlacierClient

_LOGGER = logging.getLogger(__name__)


USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_AWS_ACCESS_KEY_ID): str,
        vol.Required(CONF_AWS_SECRET_ACCESS_KEY): str,
        vol.Required(CONF_AWS_REGION, default="us-east-1"): str,
        vol.Required(CONF_BUCKET): str,
        vol.Required(CONF_STORAGE_ROOT, default="/config/nest_recorder"): str,
        vol.Optional(CONF_ENDPOINT_URL): str,
    }
)


class NestRecorderConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            await self.async_set_unique_id(f"{user_input[CONF_BUCKET]}@{user_input[CONF_AWS_REGION]}")
            self._abort_if_unique_id_configured()

            client = GlacierClient(
                self.hass,
                aws_access_key_id=user_input[CONF_AWS_ACCESS_KEY_ID],
                aws_secret_access_key=user_input[CONF_AWS_SECRET_ACCESS_KEY],
                region=user_input[CONF_AWS_REGION],
                bucket=user_input[CONF_BUCKET],
                endpoint_url=user_input.get(CONF_ENDPOINT_URL),
            )
            try:
                await client.async_head_bucket()
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("bucket validation failed: %s", err)
                errors["base"] = "auth"

            root = Path(user_input[CONF_STORAGE_ROOT])

            def _probe_write() -> None:
                root.mkdir(parents=True, exist_ok=True)
                probe = root / ".nest_recorder_probe"
                probe.write_text("ok", encoding="utf-8")
                probe.unlink()

            try:
                await self.hass.async_add_executor_job(_probe_write)
            except OSError as err:
                _LOGGER.warning("storage path not writable: %s", err)
                errors["base"] = "storage_path"

            if not errors:
                return self.async_create_entry(
                    title=f"Nest Recorder ({user_input[CONF_BUCKET]})",
                    data=user_input,
                    options={
                        CONF_STORAGE_CLASS: DEFAULT_STORAGE_CLASS,
                        CONF_RETENTION_DAYS: DEFAULT_RETENTION_DAYS,
                        CONF_SEGMENT_SECONDS: DEFAULT_SEGMENT_SECONDS,
                        CONF_UPLOAD_CONCURRENCY: DEFAULT_UPLOAD_CONCURRENCY,
                        CONF_CAMERAS: {},
                    },
                )

        return self.async_show_form(
            step_id="user", data_schema=USER_SCHEMA, errors=errors
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> "NestRecorderOptionsFlow":
        return NestRecorderOptionsFlow(config_entry)


class NestRecorderOptionsFlow(config_entries.OptionsFlow):
    def __init__(self, entry: config_entries.ConfigEntry) -> None:
        self.entry = entry

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            storage_class = user_input[CONF_STORAGE_CLASS]
            retention = user_input[CONF_RETENTION_DAYS]
            early = EARLY_DELETE_DAYS.get(storage_class, 0)
            if early and retention < early:
                # Warn but don't block — user may want this intentionally.
                _LOGGER.warning(
                    "retention %sd < %s early-delete minimum %sd -- you will pay "
                    "storage fees for data already deleted locally",
                    retention,
                    storage_class,
                    early,
                )
            cams_opts = self.entry.options.get(CONF_CAMERAS, {}) or {}
            merged = {**self.entry.options, **user_input, CONF_CAMERAS: cams_opts}
            return self.async_create_entry(title="", data=merged)

        current = self.entry.options
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_STORAGE_CLASS,
                    default=current.get(CONF_STORAGE_CLASS, DEFAULT_STORAGE_CLASS),
                ): vol.In(VALID_STORAGE_CLASSES),
                vol.Required(
                    CONF_RETENTION_DAYS,
                    default=current.get(CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS),
                ): vol.All(int, vol.Range(min=1, max=365)),
                vol.Required(
                    CONF_SEGMENT_SECONDS,
                    default=current.get(CONF_SEGMENT_SECONDS, DEFAULT_SEGMENT_SECONDS),
                ): vol.All(int, vol.Range(min=60, max=3600)),
                vol.Required(
                    CONF_UPLOAD_CONCURRENCY,
                    default=current.get(
                        CONF_UPLOAD_CONCURRENCY, DEFAULT_UPLOAD_CONCURRENCY
                    ),
                ): vol.All(int, vol.Range(min=1, max=16)),
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema, errors=errors)
