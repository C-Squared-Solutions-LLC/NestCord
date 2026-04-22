"""AWS S3 / Glacier client using boto3 via the HA executor.

We deliberately use synchronous boto3 offloaded to hass.async_add_executor_job
rather than aiobotocore. Reason: HA bundles boto3 for other integrations
(already present, no pin fight), while aiobotocore version tracking against
HA's botocore is a constant source of breakage.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

_ONGOING_RE = re.compile(r'ongoing-request="(true|false)"')
_EXPIRY_RE = re.compile(r'expiry-date="([^"]+)"')


@dataclass
class RestoreState:
    ongoing: bool
    expiry_date: str | None


class GlacierClient:
    def __init__(
        self,
        hass: HomeAssistant,
        *,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region: str,
        bucket: str,
        endpoint_url: str | None = None,
    ) -> None:
        self._hass = hass
        self._bucket = bucket
        self._kwargs: dict[str, Any] = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "region_name": region,
        }
        if endpoint_url:
            self._kwargs["endpoint_url"] = endpoint_url
        self._client: Any = None

    @property
    def bucket(self) -> str:
        return self._bucket

    def _get_client(self) -> Any:
        if self._client is None:
            import boto3

            self._client = boto3.client("s3", **self._kwargs)
        return self._client

    async def _run(self, fn, *args, **kwargs):
        return await self._hass.async_add_executor_job(partial(fn, *args, **kwargs))

    async def async_head_bucket(self) -> None:
        def _work() -> None:
            self._get_client().head_bucket(Bucket=self._bucket)

        await self._run(_work)

    async def async_upload_segment(
        self, path: Path, key: str, storage_class: str
    ) -> None:
        def _work() -> None:
            self._get_client().upload_file(
                str(path),
                self._bucket,
                key,
                ExtraArgs={
                    "StorageClass": storage_class,
                    "ContentType": "video/mp4",
                },
            )

        await self._run(_work)

    async def async_initiate_restore(
        self, key: str, tier: str, days: int
    ) -> None:
        def _work() -> None:
            try:
                self._get_client().restore_object(
                    Bucket=self._bucket,
                    Key=key,
                    RestoreRequest={
                        "Days": days,
                        "GlacierJobParameters": {"Tier": tier},
                    },
                )
            except Exception as err:  # noqa: BLE001
                if "RestoreAlreadyInProgress" in str(err):
                    _LOGGER.debug("Restore already in progress for %s", key)
                    return
                raise

        await self._run(_work)

    async def async_head_object(self, key: str) -> RestoreState | None:
        def _work() -> dict[str, Any] | None:
            try:
                return self._get_client().head_object(Bucket=self._bucket, Key=key)
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("head_object failed for %s: %s", key, err)
                return None

        resp = await self._run(_work)
        if resp is None:
            return None
        restore_hdr = resp.get("Restore") or resp.get("ResponseMetadata", {}).get(
            "HTTPHeaders", {}
        ).get("x-amz-restore")
        if not restore_hdr:
            return None
        ongoing_match = _ONGOING_RE.search(restore_hdr)
        expiry_match = _EXPIRY_RE.search(restore_hdr)
        return RestoreState(
            ongoing=bool(ongoing_match and ongoing_match.group(1) == "true"),
            expiry_date=expiry_match.group(1) if expiry_match else None,
        )

    async def async_list_prefix(self, prefix: str) -> list[dict[str, Any]]:
        def _work() -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            paginator = self._get_client().get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                out.extend(page.get("Contents", []) or [])
            return out

        return await self._run(_work)
