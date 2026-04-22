"""AWS S3 / Glacier client wrapper using aiobotocore."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_LOGGER = logging.getLogger(__name__)

_ONGOING_RE = re.compile(r'ongoing-request="(true|false)"')
_EXPIRY_RE = re.compile(r'expiry-date="([^"]+)"')


@dataclass
class RestoreState:
    ongoing: bool
    expiry_date: str | None


class GlacierClient:
    """Wraps aiobotocore for uploads, restores, and head checks.

    The client is constructed once per coordinator and owns a session that
    creates fresh clients on each call (aiobotocore's recommended pattern).
    """

    def __init__(
        self,
        *,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        region: str,
        bucket: str,
        endpoint_url: str | None = None,
    ) -> None:
        self._bucket = bucket
        self._region = region
        self._endpoint_url = endpoint_url
        self._kwargs: dict[str, Any] = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "region_name": region,
        }
        if endpoint_url:
            self._kwargs["endpoint_url"] = endpoint_url

    @property
    def bucket(self) -> str:
        return self._bucket

    def _session(self):
        # Imported lazily so HA can install the requirement before use.
        from aiobotocore.session import get_session

        return get_session()

    async def async_head_bucket(self) -> None:
        """Used by config flow to validate credentials."""
        async with self._session().create_client("s3", **self._kwargs) as s3:
            await s3.head_bucket(Bucket=self._bucket)

    async def async_upload_segment(
        self, path: Path, key: str, storage_class: str
    ) -> None:
        async with self._session().create_client("s3", **self._kwargs) as s3:
            with path.open("rb") as fp:
                await s3.put_object(
                    Bucket=self._bucket,
                    Key=key,
                    Body=fp,
                    StorageClass=storage_class,
                    ContentType="video/mp4",
                )

    async def async_initiate_restore(
        self, key: str, tier: str, days: int
    ) -> None:
        async with self._session().create_client("s3", **self._kwargs) as s3:
            try:
                await s3.restore_object(
                    Bucket=self._bucket,
                    Key=key,
                    RestoreRequest={
                        "Days": days,
                        "GlacierJobParameters": {"Tier": tier},
                    },
                )
            except Exception as err:  # noqa: BLE001 - boto wraps many kinds
                if "RestoreAlreadyInProgress" in str(err):
                    _LOGGER.debug("Restore already in progress for %s", key)
                    return
                raise

    async def async_head_object(self, key: str) -> RestoreState | None:
        async with self._session().create_client("s3", **self._kwargs) as s3:
            try:
                resp = await s3.head_object(Bucket=self._bucket, Key=key)
            except Exception as err:  # noqa: BLE001
                _LOGGER.warning("head_object failed for %s: %s", key, err)
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
        out: list[dict[str, Any]] = []
        async with self._session().create_client("s3", **self._kwargs) as s3:
            paginator = s3.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                out.extend(page.get("Contents", []) or [])
        return out
