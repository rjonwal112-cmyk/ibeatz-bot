from services.cache import (
    IN_FLIGHT_DOWNLOADS,
    IN_FLIGHT_LOCK,
    CacheEntry,
    media_cache,
    normalize_media_url,
)
from services.database import CachedMediaRecord, DatabaseManager, db
from services.platforms import (
    PlatformInfo,
    detect_platform_from_url,
    extract_canonical_id,
)
from services.audio_extractor import (
    download_audio_from_url,
    extract_audio_from_video,
)
from services.queue_manager import Job, JobQueueManager, JobStatus, job_queue
from services.stats import (
    check_disk_space,
    get_stats_summary,
    init_stats,
    is_maintenance_mode,
    record_audio_extraction,
    record_cache_hit,
    record_delivery_metric,
    record_download_finish,
    record_download_start,
    record_error,
    record_request,
    set_maintenance_mode,
)
from services.system_check import (
    get_ffmpeg_version,
    is_ffmpeg_installed,
    log_startup_system_report,
    resolve_ffmpeg_path,
)

__all__ = [
    "resolve_ffmpeg_path",
    "is_ffmpeg_installed",
    "get_ffmpeg_version",
    "log_startup_system_report",
    "PlatformInfo",
    "detect_platform_from_url",
    "extract_canonical_id",
    "db",
    "DatabaseManager",
    "CachedMediaRecord",
    "CacheEntry",
    "media_cache",
    "normalize_media_url",
    "IN_FLIGHT_DOWNLOADS",
    "IN_FLIGHT_LOCK",
    "job_queue",
    "JobStatus",
    "init_stats",
    "is_maintenance_mode",
    "set_maintenance_mode",
    "record_request",
    "record_cache_hit",
    "record_download_start",
    "record_download_finish",
    "record_audio_extraction",
    "record_delivery_metric",
    "record_error",
    "extract_audio_from_video",
    "download_audio_from_url",
    "get_stats_summary",
    "check_disk_space",
]
