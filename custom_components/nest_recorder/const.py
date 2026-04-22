"""Constants for the Nest Recorder integration."""
from __future__ import annotations

from enum import StrEnum
from typing import Final

DOMAIN: Final = "nest_recorder"

# Config entry keys (set-once data)
CONF_AWS_ACCESS_KEY_ID: Final = "aws_access_key_id"
CONF_AWS_SECRET_ACCESS_KEY: Final = "aws_secret_access_key"
CONF_AWS_REGION: Final = "aws_region"
CONF_BUCKET: Final = "bucket"
CONF_ENDPOINT_URL: Final = "endpoint_url"
CONF_STORAGE_ROOT: Final = "storage_root"

# Options keys (hot-reloadable)
CONF_STORAGE_CLASS: Final = "storage_class"
CONF_RETENTION_DAYS: Final = "retention_days"
CONF_SEGMENT_SECONDS: Final = "segment_seconds"
CONF_UPLOAD_CONCURRENCY: Final = "upload_concurrency"
CONF_CAMERAS: Final = "cameras"
CONF_RESTORE_TIER: Final = "restore_tier"
CONF_RESTORE_DAYS: Final = "restore_days"

DEFAULT_STORAGE_CLASS: Final = "GLACIER"
DEFAULT_RETENTION_DAYS: Final = 14
DEFAULT_SEGMENT_SECONDS: Final = 600
DEFAULT_UPLOAD_CONCURRENCY: Final = 4
DEFAULT_RESTORE_TIER: Final = "Standard"
DEFAULT_RESTORE_DAYS: Final = 7

VALID_STORAGE_CLASSES: Final = ("GLACIER", "DEEP_ARCHIVE", "GLACIER_IR")
VALID_RESTORE_TIERS: Final = ("Standard", "Expedited", "Bulk")

EARLY_DELETE_DAYS: Final = {
    "GLACIER": 90,
    "GLACIER_IR": 90,
    "DEEP_ARCHIVE": 180,
}

# The nest integration fires this bus event for every SDM camera event.
NEST_BUS_EVENT: Final = "nest_event"
EVENT_MOTION: Final = "camera_motion"
EVENT_PERSON: Final = "camera_person"
EVENT_SOUND: Final = "camera_sound"
EVENT_DOORBELL: Final = "doorbell_chime"
EVENT_MANUAL: Final = "manual"
KNOWN_EVENT_TYPES: Final = (
    EVENT_MOTION,
    EVENT_PERSON,
    EVENT_SOUND,
    EVENT_DOORBELL,
    EVENT_MANUAL,
)


class CameraKind(StrEnum):
    RTSP = "rtsp"
    WEBRTC = "webrtc"


class GlacierStatus(StrEnum):
    PENDING = "PENDING"
    ARCHIVED = "ARCHIVED"
    RESTORING = "RESTORING"
    RESTORED = "RESTORED"
    FAILED = "FAILED"


# Services
SERVICE_RECORD_CLIP: Final = "record_clip"
SERVICE_RESTORE_FROM_GLACIER: Final = "restore_from_glacier"
SERVICE_REBUILD_INDEX: Final = "rebuild_index"
SERVICE_PURGE_LOCAL: Final = "purge_local"
SERVICE_EXTEND_RETENTION: Final = "extend_retention"

# Media source URIs
MEDIA_SOURCE_DOMAIN: Final = DOMAIN
HTTP_VIEW_URL: Final = "/api/nest_recorder/segment/{segment_id}"

# Upload / FFmpeg tunables
FFMPEG_RELAUNCH_BACKOFF_MIN: Final = 1.0
FFMPEG_RELAUNCH_BACKOFF_MAX: Final = 30.0
STREAM_EXTEND_LEAD_SECONDS: Final = 60
MAX_STREAM_EXTENSIONS: Final = 4  # SDM allows ~5 total, regen before hitting cap
RESTORE_POLL_SECONDS: Final = 300
BATTERY_IDLE_TEARDOWN_SECONDS: Final = 30
