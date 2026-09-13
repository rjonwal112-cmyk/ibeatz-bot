import asyncio
import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

from config import BASE_DOWNLOAD_DIR

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = BASE_DOWNLOAD_DIR / "ibeatz.db"


@dataclass
class CachedMediaRecord:
    cache_key: str
    platform: str
    media_id: str
    quality: str
    actual_quality: str
    title: str
    uploader: str
    duration: Optional[int]
    file_path: str
    file_size: int
    telegram_file_id: Optional[str]
    telegram_audio_id: Optional[str]
    created_at: float
    last_used_at: float
    hits: int = 0


class DatabaseManager:
    """
    Thread-safe, asynchronous SQLite manager with Write-Ahead Logging (WAL).
    Provides persistent caching, metadata caching, performance metrics, and error logging.
    """

    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self._initialized = False

    @contextmanager
    def _get_connection(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._get_connection() as conn:
            # 1. Media Cache Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS media_cache (
                    cache_key TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    media_id TEXT NOT NULL,
                    quality TEXT NOT NULL,
                    actual_quality TEXT NOT NULL,
                    title TEXT,
                    uploader TEXT,
                    duration INTEGER,
                    file_path TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    telegram_file_id TEXT,
                    telegram_audio_id TEXT,
                    created_at REAL NOT NULL,
                    last_used_at REAL NOT NULL,
                    hits INTEGER DEFAULT 0
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_media_platform_id ON media_cache(platform, media_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_media_last_used ON media_cache(last_used_at);")

            # 2. Metadata Cache Table (Short TTL to prevent duplicate info extraction)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata_cache (
                    url_hash TEXT PRIMARY KEY,
                    canonical_id TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    title TEXT,
                    raw_json TEXT NOT NULL,
                    available_qualities TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_meta_expires ON metadata_cache(expires_at);")

            # 3. Delivery Performance Metrics Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS delivery_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    req_id TEXT,
                    platform TEXT,
                    quality TEXT,
                    is_cache_hit INTEGER,
                    cache_lookup_s REAL,
                    extraction_s REAL,
                    download_s REAL,
                    ffmpeg_s REAL,
                    upload_s REAL,
                    total_s REAL,
                    bottleneck TEXT,
                    file_size INTEGER,
                    created_at REAL NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_created ON delivery_metrics(created_at);")

            # 4. Error Logs Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS error_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    platform TEXT,
                    url TEXT,
                    error_code TEXT,
                    error_message TEXT
                );
            """)

            # 5. Bot Settings Table (Persistent Maintenance Mode & Config)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)

            # 6. User History Table (Per-user download tracking, isolated by user_id)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    platform TEXT NOT NULL,
                    media_id TEXT NOT NULL,
                    title TEXT,
                    quality TEXT,
                    created_at REAL NOT NULL
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_user_history_user ON user_history(user_id, created_at DESC);")

            # 7. User Settings Table (Per-user preferences, key-value)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_settings (
                    user_id INTEGER NOT NULL,
                    setting_key TEXT NOT NULL,
                    setting_value TEXT NOT NULL,
                    PRIMARY KEY (user_id, setting_key)
                );
            """)

    async def initialize(self) -> None:
        """Initializes database schema asynchronously."""
        if self._initialized:
            return
        async with self._lock:
            if not self._initialized:
                await asyncio.to_thread(self._init_schema)
                self._initialized = True
                logger.info(f"Persistent SQLite storage initialized at {self.db_path} (WAL mode active)")

    # -------------------------------------------------------------
    # Media Cache Operations
    # -------------------------------------------------------------
    def _get_media_sync(self, platform: str, media_id: str, quality: str) -> Optional[CachedMediaRecord]:
        cache_key = f"{platform}:{media_id}:{quality.lower().strip()}"
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM media_cache WHERE cache_key = ?",
                (cache_key,),
            )
            row = cursor.fetchone()
            if not row:
                # Fallback: check if 'highest' or 'best' exists for this media
                alt_keys = (
                    f"{platform}:{media_id}:highest",
                    f"{platform}:{media_id}:best",
                )
                cursor = conn.execute(
                    "SELECT * FROM media_cache WHERE cache_key IN (?, ?) ORDER BY last_used_at DESC LIMIT 1",
                    alt_keys,
                )
                row = cursor.fetchone()

            if row:
                now = time.time()
                conn.execute(
                    "UPDATE media_cache SET last_used_at = ?, hits = hits + 1 WHERE cache_key = ?",
                    (now, row["cache_key"]),
                )
                return CachedMediaRecord(
                    cache_key=row["cache_key"],
                    platform=row["platform"],
                    media_id=row["media_id"],
                    quality=row["quality"],
                    actual_quality=row["actual_quality"],
                    title=row["title"] or "",
                    uploader=row["uploader"] or "",
                    duration=row["duration"],
                    file_path=row["file_path"],
                    file_size=row["file_size"],
                    telegram_file_id=row["telegram_file_id"],
                    telegram_audio_id=row["telegram_audio_id"],
                    created_at=row["created_at"],
                    last_used_at=now,
                    hits=row["hits"] + 1,
                )
            return None

    async def get_media(self, platform: str, media_id: str, quality: str) -> Optional[CachedMediaRecord]:
        return await asyncio.to_thread(self._get_media_sync, platform, media_id, quality)

    def _put_media_sync(
        self,
        platform: str,
        media_id: str,
        quality: str,
        actual_quality: str,
        title: str,
        uploader: str,
        duration: Optional[int],
        file_path: str,
        file_size: int,
        telegram_file_id: Optional[str] = None,
        telegram_audio_id: Optional[str] = None,
    ) -> CachedMediaRecord:
        q_norm = quality.lower().strip()
        cache_key = f"{platform}:{media_id}:{q_norm}"
        now = time.time()

        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO media_cache (
                    cache_key, platform, media_id, quality, actual_quality,
                    title, uploader, duration, file_path, file_size,
                    telegram_file_id, telegram_audio_id, created_at, last_used_at, hits
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(cache_key) DO UPDATE SET
                    actual_quality = excluded.actual_quality,
                    title = excluded.title,
                    uploader = excluded.uploader,
                    duration = excluded.duration,
                    file_path = excluded.file_path,
                    file_size = excluded.file_size,
                    telegram_file_id = COALESCE(excluded.telegram_file_id, media_cache.telegram_file_id),
                    telegram_audio_id = COALESCE(excluded.telegram_audio_id, media_cache.telegram_audio_id),
                    last_used_at = excluded.last_used_at,
                    hits = media_cache.hits + 1;
                """,
                (
                    cache_key, platform, media_id, q_norm, actual_quality,
                    title, uploader, duration, file_path, file_size,
                    telegram_file_id, telegram_audio_id, now, now,
                ),
            )
        return CachedMediaRecord(
            cache_key=cache_key,
            platform=platform,
            media_id=media_id,
            quality=q_norm,
            actual_quality=actual_quality,
            title=title,
            uploader=uploader,
            duration=duration,
            file_path=file_path,
            file_size=file_size,
            telegram_file_id=telegram_file_id,
            telegram_audio_id=telegram_audio_id,
            created_at=now,
            last_used_at=now,
            hits=1,
        )

    async def put_media(
        self,
        platform: str,
        media_id: str,
        quality: str,
        actual_quality: str,
        title: str,
        uploader: str,
        duration: Optional[int],
        file_path: str,
        file_size: int,
        telegram_file_id: Optional[str] = None,
        telegram_audio_id: Optional[str] = None,
    ) -> CachedMediaRecord:
        return await asyncio.to_thread(
            self._put_media_sync,
            platform, media_id, quality, actual_quality,
            title, uploader, duration, file_path, file_size,
            telegram_file_id, telegram_audio_id,
        )

    def _update_telegram_file_id_sync(self, cache_key: str, file_id: str, is_audio: bool = False) -> None:
        col = "telegram_audio_id" if is_audio else "telegram_file_id"
        with self._get_connection() as conn:
            conn.execute(f"UPDATE media_cache SET {col} = ? WHERE cache_key = ?", (file_id, cache_key))

    async def update_telegram_file_id(self, cache_key: str, file_id: str, is_audio: bool = False) -> None:
        await asyncio.to_thread(self._update_telegram_file_id_sync, cache_key, file_id, is_audio)

    # -------------------------------------------------------------
    # Metadata Cache Operations
    # -------------------------------------------------------------
    def _get_metadata_sync(self, url_hash: str) -> Optional[dict]:
        now = time.time()
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM metadata_cache WHERE url_hash = ? AND expires_at > ?",
                (url_hash, now),
            )
            row = cursor.fetchone()
            if row:
                try:
                    return {
                        "canonical_id": row["canonical_id"],
                        "platform": row["platform"],
                        "title": row["title"],
                        "raw_info": json.loads(row["raw_json"]),
                        "available_qualities": json.loads(row["available_qualities"]),
                    }
                except Exception as e:
                    logger.warning(f"Error decoding cached metadata: {e}")
            return None

    async def get_metadata(self, url_hash: str) -> Optional[dict]:
        return await asyncio.to_thread(self._get_metadata_sync, url_hash)

    def _put_metadata_sync(
        self,
        url_hash: str,
        canonical_id: str,
        platform: str,
        title: str,
        raw_info: dict,
        available_qualities: list[str],
        ttl_seconds: int = 3600,
    ) -> None:
        now = time.time()
        expires_at = now + ttl_seconds
        raw_json = json.dumps(raw_info)
        qualities_json = json.dumps(available_qualities)

        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO metadata_cache (
                    url_hash, canonical_id, platform, title, raw_json,
                    available_qualities, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (url_hash, canonical_id, platform, title, raw_json, qualities_json, now, expires_at),
            )

    async def put_metadata(
        self,
        url_hash: str,
        canonical_id: str,
        platform: str,
        title: str,
        raw_info: dict,
        available_qualities: list[str],
        ttl_seconds: int = 3600,
    ) -> None:
        await asyncio.to_thread(
            self._put_metadata_sync,
            url_hash, canonical_id, platform, title, raw_info, available_qualities, ttl_seconds,
        )

    # -------------------------------------------------------------
    # Performance Telemetry & Error Logging
    # -------------------------------------------------------------
    def _record_metric_sync(
        self,
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
        now = time.time()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO delivery_metrics (
                    req_id, platform, quality, is_cache_hit, cache_lookup_s,
                    extraction_s, download_s, ffmpeg_s, upload_s, total_s,
                    bottleneck, file_size, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    req_id, platform, quality, 1 if is_cache_hit else 0,
                    cache_lookup_s, extraction_s, download_s, ffmpeg_s,
                    upload_s, total_s, bottleneck, file_size, now,
                ),
            )

    async def record_metric(
        self,
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
        await asyncio.to_thread(
            self._record_metric_sync,
            req_id, platform, quality, is_cache_hit, cache_lookup_s,
            extraction_s, download_s, ffmpeg_s, upload_s, total_s,
            bottleneck, file_size,
        )

    def _log_error_sync(self, platform: str, url: str, error_code: str, error_message: str) -> None:
        now = time.time()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO error_logs (timestamp, platform, url, error_code, error_message)
                VALUES (?, ?, ?, ?, ?)
                """,
                (now, platform, url, error_code, error_message),
            )

    async def log_error(self, platform: str, url: str, error_code: str, error_message: str) -> None:
        await asyncio.to_thread(self._log_error_sync, platform, url, error_code, error_message)

    def _get_recent_errors_sync(self, limit: int = 10) -> list[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT timestamp, platform, url, error_code, error_message FROM error_logs ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            return [dict(row) for row in cursor.fetchall()]

    async def get_recent_errors(self, limit: int = 10) -> list[dict]:
        return await asyncio.to_thread(self._get_recent_errors_sync, limit)

    def _get_perf_summary_sync(self) -> dict:
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT
                    COUNT(*) as total_deliveries,
                    AVG(cache_lookup_s) as avg_cache_lookup,
                    AVG(extraction_s) as avg_extraction,
                    AVG(download_s) as avg_download,
                    AVG(ffmpeg_s) as avg_ffmpeg,
                    AVG(upload_s) as avg_upload,
                    AVG(total_s) as avg_total,
                    AVG(file_size) as avg_file_size
                FROM delivery_metrics
                WHERE is_cache_hit = 0;
            """)
            non_cached = dict(cursor.fetchone() or {})

            cursor = conn.execute("""
                SELECT
                    COUNT(*) as cache_hits,
                    AVG(total_s) as avg_cached_total
                FROM delivery_metrics
                WHERE is_cache_hit = 1;
            """)
            cached = dict(cursor.fetchone() or {})

            cursor = conn.execute("""
                SELECT bottleneck, COUNT(*) as count
                FROM delivery_metrics
                GROUP BY bottleneck
                ORDER BY count DESC
                LIMIT 1;
            """)
            top_b_row = cursor.fetchone()
            top_bottleneck = top_b_row["bottleneck"] if top_b_row else "None"

            return {
                "total_deliveries": (non_cached.get("total_deliveries") or 0) + (cached.get("cache_hits") or 0),
                "cache_hits": cached.get("cache_hits") or 0,
                "avg_cached_total": cached.get("avg_cached_total") or 0.0,
                "avg_extraction": non_cached.get("avg_extraction") or 0.0,
                "avg_download": non_cached.get("avg_download") or 0.0,
                "avg_ffmpeg": non_cached.get("avg_ffmpeg") or 0.0,
                "avg_upload": non_cached.get("avg_upload") or 0.0,
                "avg_total": non_cached.get("avg_total") or 0.0,
                "top_bottleneck": top_bottleneck,
            }

    async def get_perf_summary(self) -> dict:
        return await asyncio.to_thread(self._get_perf_summary_sync)

    def _get_recent_metrics_sync(self, limit: int = 5) -> list[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT req_id, platform, quality, is_cache_hit, cache_lookup_s,
                       extraction_s, download_s, ffmpeg_s, upload_s, total_s,
                       bottleneck, file_size, created_at
                FROM delivery_metrics
                ORDER BY id DESC LIMIT ?
                """,
                (limit,),
            )
            return [dict(row) for row in cursor.fetchall()]

    async def get_recent_metrics(self, limit: int = 5) -> list[dict]:
        return await asyncio.to_thread(self._get_recent_metrics_sync, limit)

    # -------------------------------------------------------------
    # Bot Settings & Maintenance Mode
    # -------------------------------------------------------------
    def _get_setting_sync(self, key: str, default: Optional[str] = None) -> Optional[str]:
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT value FROM bot_settings WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row["value"] if row else default

    async def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return await asyncio.to_thread(self._get_setting_sync, key, default)

    def _set_setting_sync(self, key: str, value: str) -> None:
        with self._get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)",
                (key, value),
            )

    async def set_setting(self, key: str, value: str) -> None:
        await asyncio.to_thread(self._set_setting_sync, key, value)

    # -------------------------------------------------------------
    # User History & User Settings Operations
    # -------------------------------------------------------------
    def _record_user_download_sync(
        self, user_id: int, platform: str, media_id: str, title: str, quality: str
    ) -> None:
        now = time.time()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO user_history (user_id, platform, media_id, title, quality, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (user_id, platform, media_id, title, quality, now),
            )

    async def record_user_download(
        self, user_id: int, platform: str, media_id: str, title: str, quality: str
    ) -> None:
        await asyncio.to_thread(
            self._record_user_download_sync, user_id, platform, media_id, title, quality
        )

    def _get_user_history_sync(self, user_id: int, limit: int = 5) -> list[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT platform, media_id, title, quality, created_at
                FROM user_history
                WHERE user_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (user_id, limit),
            )
            return [dict(r) for r in cursor.fetchall()]

    async def get_user_history(self, user_id: int, limit: int = 5) -> list[dict]:
        return await asyncio.to_thread(self._get_user_history_sync, user_id, limit)

    def _clear_user_history_sync(self, user_id: int) -> int:
        with self._get_connection() as conn:
            cursor = conn.execute(
                "DELETE FROM user_history WHERE user_id = ?",
                (user_id,),
            )
            return cursor.rowcount

    async def clear_user_history(self, user_id: int) -> int:
        return await asyncio.to_thread(self._clear_user_history_sync, user_id)

    def _get_user_setting_sync(self, user_id: int, key: str, default: Any = None) -> Any:
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT setting_value FROM user_settings WHERE user_id = ? AND setting_key = ?",
                (user_id, key),
            )
            row = cursor.fetchone()
            return row["setting_value"] if row else default

    async def get_user_setting(self, user_id: int, key: str, default: Any = None) -> Any:
        return await asyncio.to_thread(self._get_user_setting_sync, user_id, key, default)

    def _set_user_setting_sync(self, user_id: int, key: str, value: Any) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO user_settings (user_id, setting_key, setting_value)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, setting_key) DO UPDATE SET setting_value = excluded.setting_value
                """,
                (user_id, key, str(value)),
            )

    async def set_user_setting(self, user_id: int, key: str, value: Any) -> None:
        await asyncio.to_thread(self._set_user_setting_sync, user_id, key, value)

    # -------------------------------------------------------------
    # Cache Eviction & Cleanup
    # -------------------------------------------------------------
    def _cleanup_sync(self, max_bytes: int, active_files: set[str], retention_seconds: int = 86400 * 3) -> int:
        now = time.time()
        removed_count = 0
        with self._get_connection() as conn:
            # 1. Purge expired metadata
            conn.execute("DELETE FROM metadata_cache WHERE expires_at < ?", (now,))

            # 2. Age-based media eviction
            cutoff = now - retention_seconds
            cursor = conn.execute("SELECT cache_key, file_path FROM media_cache WHERE last_used_at < ?", (cutoff,))
            expired_rows = cursor.fetchall()
            for r in expired_rows:
                f_path = r["file_path"]
                if f_path not in active_files:
                    try:
                        p = Path(f_path)
                        if p.exists():
                            p.unlink(missing_ok=True)
                        conn.execute("DELETE FROM media_cache WHERE cache_key = ?", (r["cache_key"],))
                        removed_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to delete expired file {f_path}: {e}")

            # 3. Capacity-based LRU eviction
            cursor = conn.execute("SELECT SUM(file_size) as total_size FROM media_cache;")
            row = cursor.fetchone()
            total_size = row["total_size"] or 0

            if total_size > max_bytes:
                cursor = conn.execute(
                    "SELECT cache_key, file_path, file_size FROM media_cache ORDER BY last_used_at ASC;"
                )
                for r in cursor.fetchall():
                    if total_size <= max_bytes:
                        break
                    f_path = r["file_path"]
                    if f_path in active_files:
                        continue
                    try:
                        p = Path(f_path)
                        if p.exists():
                            p.unlink(missing_ok=True)
                        conn.execute("DELETE FROM media_cache WHERE cache_key = ?", (r["cache_key"],))
                        total_size -= r["file_size"]
                        removed_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to delete LRU cache file {f_path}: {e}")

        return removed_count

    async def cleanup(self, max_bytes: int, active_files: set[str], retention_seconds: int = 86400 * 3) -> int:
        return await asyncio.to_thread(self._cleanup_sync, max_bytes, active_files, retention_seconds)


# Global Database Instance
db = DatabaseManager()
