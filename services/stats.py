import asyncio
import shutil
import time
from pathlib import Path
from typing import Optional

from config import BASE_DOWNLOAD_DIR, MIN_FREE_DISK_SPACE_BYTES
from services.database import db

START_TIME = time.time()
_MAINTENANCE_MODE: bool = False
_INITIALIZED: bool = False

_STATS = {
    "total_requests": 0,
    "total_downloads": 0,
    "successful_downloads": 0,
    "failed_downloads": 0,
    "cache_hits": 0,
    "audio_extractions": 0,
    "active_downloads": 0,
}


async def init_stats() -> None:
    """Initializes persistent stats and loads maintenance setting."""
    global _MAINTENANCE_MODE, _INITIALIZED
    if _INITIALIZED:
        return
    await db.initialize()
    val = await db.get_setting("maintenance_mode", "0")
    _MAINTENANCE_MODE = val == "1"
    _INITIALIZED = True


def is_maintenance_mode() -> bool:
    """Returns True if the bot is currently in maintenance mode."""
    return _MAINTENANCE_MODE


async def set_maintenance_mode(enabled: bool) -> None:
    """Toggles maintenance mode persistently."""
    global _MAINTENANCE_MODE
    _MAINTENANCE_MODE = enabled
    await db.set_setting("maintenance_mode", "1" if enabled else "0")


def record_request() -> None:
    _STATS["total_requests"] += 1


def record_cache_hit() -> None:
    _STATS["cache_hits"] += 1


def record_download_start() -> None:
    _STATS["total_downloads"] += 1
    _STATS["active_downloads"] += 1


def record_download_finish(success: bool) -> None:
    _STATS["active_downloads"] = max(0, _STATS["active_downloads"] - 1)
    if success:
        _STATS["successful_downloads"] += 1
    else:
        _STATS["failed_downloads"] += 1


def record_audio_extraction() -> None:
    _STATS["audio_extractions"] += 1

async def record_delivery_metric(
    req_id: str,
    platform: str,
    quality: str,
    is_cache_hit: bool,
    cache_lookup_s: float,
    extraction_s: float,
    download_s: float,
    ffmpeg_s: float,
    upload_s: float,
    total_s: float,
    bottleneck: str,
    file_size: int,
) -> None:
    """Stores delivery performance telemetry in SQLite."""
    try:
        await db.record_metric(
            req_id=req_id,
            platform=platform,
            quality=quality,
            is_cache_hit=is_cache_hit,
            cache_lookup_s=cache_lookup_s,
            extraction_s=extraction_s,
            download_s=download_s,
            ffmpeg_s=ffmpeg_s,
            upload_s=upload_s,
            total_s=total_s,
            bottleneck=bottleneck,
            file_size=file_size,
        )
    except Exception:
        pass


async def record_error(platform: str, url: str, error_code: str, error_message: str) -> None:
    """Stores operational error log in SQLite."""
    try:
        await db.log_error(platform, url, error_code, error_message)
    except Exception:
        pass


async def get_stats_summary() -> dict:
    uptime_seconds = int(time.time() - START_TIME)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime_str = f"{hours}h {minutes}m {seconds}s"

    total = _STATS["total_requests"]
    hits = _STATS["cache_hits"]
    hit_ratio = f"{(hits / total * 100):.1f}%" if total > 0 else "0%"

    has_space, free_gb, total_gb = check_disk_space()
    perf = await db.get_perf_summary()

    return {
        **_STATS,
        "uptime": uptime_str,
        "hit_ratio": hit_ratio,
        "maintenance_mode": _MAINTENANCE_MODE,
        "disk_free_gb": f"{free_gb:.1f} GB",
        "disk_total_gb": f"{total_gb:.1f} GB",
        "perf_summary": perf,
    }


def check_disk_space(path: Optional[Path] = None) -> tuple[bool, float, float]:
    """
    Checks if there is enough free disk space.
    Returns: (has_enough_space, free_gb, total_gb)
    """
    target = path or BASE_DOWNLOAD_DIR
    target.mkdir(parents=True, exist_ok=True)
    try:
        usage = shutil.disk_usage(target)
        free_bytes = usage.free
        total_bytes = usage.total
        has_space = free_bytes >= MIN_FREE_DISK_SPACE_BYTES
        return has_space, free_bytes / (1024**3), total_bytes / (1024**3)
    except Exception:
        return True, 0.0, 0.0
