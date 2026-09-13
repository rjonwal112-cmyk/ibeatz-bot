import asyncio
import logging
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from config import CACHE_DIR
from services.database import CachedMediaRecord, db
from services.platforms import detect_platform_from_url, extract_canonical_id

logger = logging.getLogger(__name__)

# Global registry for in-flight downloads to coalesce simultaneous identical requests
IN_FLIGHT_DOWNLOADS: dict[str, asyncio.Future] = {}
IN_FLIGHT_LOCK = asyncio.Lock()


@dataclass
class CacheEntry:
    """Represents a cached media item with local and Telegram references."""
    media_id: str
    normalized_url: str
    platform: str
    title: str
    requested_quality: str
    actual_quality: str
    file_path: str
    file_size: int
    duration: Optional[int]
    created_at: float
    last_used_at: float
    telegram_file_id: Optional[str] = None
    telegram_audio_id: Optional[str] = None
    uploader: str = ""
    hits: int = 1

    @classmethod
    def from_record(cls, rec: CachedMediaRecord, normalized_url: str = "") -> "CacheEntry":
        return cls(
            media_id=rec.media_id,
            normalized_url=normalized_url,
            platform=rec.platform,
            title=rec.title,
            requested_quality=rec.quality,
            actual_quality=rec.actual_quality,
            file_path=rec.file_path,
            file_size=rec.file_size,
            duration=rec.duration,
            created_at=rec.created_at,
            last_used_at=rec.last_used_at,
            telegram_file_id=rec.telegram_file_id,
            telegram_audio_id=rec.telegram_audio_id,
            uploader=rec.uploader,
            hits=rec.hits,
        )


def normalize_media_url(url: str) -> tuple[str, str, str]:
    """
    Normalizes a media URL across platforms:
    - Strips all marketing, tracking, and referral query parameters.
    - Canonicalizes YouTube short URLs and Shorts to standard URLs.
    - Extracts platform key and canonical media ID.
    Returns: (normalized_url, platform_key, media_id)
    """
    if not url:
        return "", "generic", ""

    url = url.strip()
    platform_info = detect_platform_from_url(url)
    plat_key = platform_info.key

    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)

    # Tracking & referral parameters to strip
    drop_params = {
        "si", "fbclid", "igsh", "stkn", "utm_source", "utm_medium", "utm_campaign",
        "utm_term", "utm_content", "ref", "feature", "app", "context",
        "share_id", "rdt", "mibextid", "fs", "s", "wtsid", "ref_src",
    }

    cleaned_query: dict[str, list[str]] = {}

    if plat_key == "youtube":
        # Handle youtu.be/<id>
        if "youtu.be" in parsed.netloc:
            video_id = parsed.path.strip("/").split("/")[0]
            canonical_url = f"https://www.youtube.com/watch?v={video_id}"
            return canonical_url, plat_key, video_id

        # Preserve only 'v' for standard watch URLs
        if "v" in query_params:
            cleaned_query["v"] = query_params["v"]
    elif plat_key == "instagram":
        ig_match = re.search(r"\/(p|reel|reels|tv|share\/reel)\/([A-Za-z0-9_-]+)", parsed.path)
        if ig_match:
            kind = ig_match.group(1)
            media_id = ig_match.group(2)
            if kind in ("reels", "share/reel"):
                kind = "reel"
            canonical_url = f"https://www.instagram.com/{kind}/{media_id}/"
            return canonical_url, plat_key, media_id
        media_id = extract_canonical_id(plat_key, url) or "unknown"
        if media_id != "unknown":
            canonical_url = f"https://www.instagram.com/p/{media_id}/"
            return canonical_url, plat_key, media_id
    elif plat_key == "facebook":
        if "v" in query_params:
            cleaned_query["v"] = query_params["v"]
        else:
            for k, v in query_params.items():
                if k.lower() not in drop_params:
                    cleaned_query[k] = v
    else:
        for k, v in query_params.items():
            if k.lower() not in drop_params:
                cleaned_query[k] = v

    new_query = urlencode(cleaned_query, doseq=True)
    clean_path = parsed.path.rstrip("/")
    if not clean_path and not new_query:
        clean_path = "/"

    normalized = urlunparse((
        parsed.scheme or "https",
        parsed.netloc.lower(),
        clean_path,
        "",
        new_query,
        "",
    ))

    media_id = extract_canonical_id(plat_key, normalized) or "unknown"
    return normalized, plat_key, media_id


class SmartMediaCache:
    """
    Production persistent smart cache backed by SQLite WAL database.
    Prevents duplicate downloads, coordinates in-flight requests,
    supports instant Telegram file_id re-sending, and maintains disk capacity limits.
    """

    def __init__(self, cache_dir: Path = CACHE_DIR, max_bytes: int = 2 * 1024 * 1024 * 1024):
        self.cache_dir = cache_dir
        self.max_bytes = max_bytes  # 2 GB default cache capacity
        self._active_files: set[str] = set()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    async def get(
        self,
        platform: str,
        media_id: str,
        requested_quality: str,
    ) -> Optional[CacheEntry]:
        """
        Retrieves a cached media entry from SQLite if it exists and has either:
        1. A valid Telegram file_id (can be re-sent instantly in 0.1s), OR
        2. A valid physical file on disk.
        """
        record = await db.get_media(platform, media_id, requested_quality)
        if not record:
            return None

        file_exists = False
        if record.file_path:
            p = Path(record.file_path)
            if p.exists() and p.stat().st_size > 0:
                file_exists = True

        # Valid if either telegram_file_id is known or physical file is on disk
        if record.telegram_file_id or file_exists:
            return CacheEntry.from_record(record)

        return None

    async def put(
        self,
        platform: str,
        media_id: str,
        normalized_url: str,
        title: str,
        requested_quality: str,
        actual_quality: str,
        source_file_path: Path,
        duration: Optional[int],
        uploader: str = "",
        telegram_file_id: Optional[str] = None,
        telegram_audio_id: Optional[str] = None,
    ) -> CacheEntry:
        """
        Atomically saves media into persistent cache directory and registers in SQLite.
        """
        q_norm = requested_quality.lower().strip()
        if platform in ("instagram", "facebook") and q_norm in ("highest", "best"):
            q_norm = "highest"

        ext = source_file_path.suffix or ".mp4"
        dest_filename = f"{platform}_{media_id}_{q_norm}{ext}"
        dest_path = self.cache_dir / dest_filename

        if dest_path.resolve() != source_file_path.resolve():
            try:
                shutil.move(str(source_file_path), str(dest_path))
            except Exception as move_err:
                logger.debug(f"shutil.move fallback to copy2: {move_err}")
                shutil.copy2(str(source_file_path), str(dest_path))

        file_size = dest_path.stat().st_size if dest_path.exists() else 0

        record = await db.put_media(
            platform=platform,
            media_id=media_id,
            quality=q_norm,
            actual_quality=actual_quality,
            title=title,
            uploader=uploader,
            duration=duration,
            file_path=str(dest_path),
            file_size=file_size,
            telegram_file_id=telegram_file_id,
            telegram_audio_id=telegram_audio_id,
        )

        logger.info(
            f"⚡ Cached media saved in SQLite: {platform}:{media_id}:{q_norm} "
            f"({file_size / (1024*1024):.2f} MB)"
        )
        return CacheEntry.from_record(record, normalized_url)

    async def update_telegram_file_id(
        self,
        platform: str,
        media_id: str,
        quality: str,
        file_id: str,
        is_audio: bool = False,
    ) -> None:
        """Stores Telegram file_id or audio_id for 0.1s instant repeat delivery."""
        q_norm = quality.lower().strip()
        if platform in ("instagram", "facebook") and q_norm in ("highest", "best"):
            q_norm = "highest"
        cache_key = f"{platform}:{media_id}:{q_norm}"
        await db.update_telegram_file_id(cache_key, file_id, is_audio=is_audio)

    def acquire_file_use(self, file_path: str) -> None:
        """Marks a file as active so background cleanup will not touch it."""
        self._active_files.add(file_path)

    def release_file_use(self, file_path: str) -> None:
        """Releases active hold on a file."""
        self._active_files.discard(file_path)

    async def cleanup(self, retention_seconds: int = 86400 * 3) -> int:
        """
        Evicts expired media and enforces LRU cache capacity.
        Never deletes files in _active_files.
        """
        removed = await db.cleanup(
            max_bytes=self.max_bytes,
            active_files=self._active_files,
            retention_seconds=retention_seconds,
        )
        if removed > 0:
            logger.info(f"SmartMediaCache cleanup evicted {removed} files.")
        return removed


# Global Cache Instance
media_cache = SmartMediaCache()
