"""Filesystem layout and helpers."""
from __future__ import annotations

import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path


def camera_dir(root: Path, camera_id: str) -> Path:
    return root / camera_id


def segment_pattern(root: Path, camera_id: str) -> str:
    """Return the strftime pattern FFmpeg uses for a camera's output files.

    Files are written flat within the camera directory so FFmpeg's segment
    muxer never needs to create intermediate parents across midnight.
    """
    return str(camera_dir(root, camera_id) / "%Y-%m-%dT%H-%M-%S.mp4")


def parse_segment_ts(path: Path) -> int | None:
    """Parse segment start timestamp (epoch seconds UTC) from filename."""
    try:
        dt = datetime.strptime(path.stem, "%Y-%m-%dT%H-%M-%S")
    except ValueError:
        return None
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def ensure_dirs(root: Path, camera_id: str, ts: int | None = None) -> Path:
    d = camera_dir(root, camera_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def prune_empty_dirs(root: Path) -> None:
    """Remove any empty camera directories."""
    if not root.exists():
        return
    for camera in root.iterdir():
        if camera.is_dir() and not any(camera.iterdir()):
            camera.rmdir()


def free_bytes(path: Path) -> int:
    usage = shutil.disk_usage(path if path.exists() else path.parent)
    return usage.free


def s3_key_for(camera_id: str, ts: int, filename: str) -> str:
    day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    return f"nest_recorder/{camera_id}/{day}/{filename}"


def safe_unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass
