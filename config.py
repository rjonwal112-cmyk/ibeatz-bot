import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "").strip()

# Support multiple admin IDs (comma-separated or single)
_ADMIN_RAW = os.getenv("ADMIN_ID", "0").replace(";", ",").split(",")
ADMIN_IDS: set[int] = {int(x.strip()) for x in _ADMIN_RAW if x.strip().isdigit()}
ADMIN_ID: int = next(iter(ADMIN_IDS), 0)


def get_bot_token() -> str:
    """Validate and return the Telegram bot token."""
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token or token in ("YOUR_NEW_TOKEN", "your_bot_token_here", "PASTE_NEW_TOKEN_HERE"):
        logging.error(
            "\n"
            "============================================================\n"
            "❌ CRITICAL CONFIGURATION ERROR: BOT_TOKEN is missing or not configured!\n"
            "Please configure your valid BOT_TOKEN in the environment or .env file.\n"
            "============================================================"
        )
        sys.exit(1)
    return token


# Telegram Local Bot API Server configuration (optional for 2000 MB / 2 GB transfers)
BOT_LOCAL_SERVER_URL: Optional[str] = (
    os.getenv("BOT_LOCAL_SERVER_URL", "").strip()
    or os.getenv("TELEGRAM_API_SERVER", "").strip()
    or None
)

# Telegram Bot API upload limit: 2000 MB for local server, 50 MB for standard cloud API
IS_LOCAL_SERVER: bool = bool(BOT_LOCAL_SERVER_URL)
MAX_FILE_SIZE_BYTES: int = (2000 if IS_LOCAL_SERVER else 50) * 1024 * 1024

# Minimum required free disk space before initiating download (500 MB)
MIN_FREE_DISK_SPACE_BYTES: int = 500 * 1024 * 1024

# Temporary downloads and cache base directories
BASE_DOWNLOAD_DIR: Path = Path("downloads")
CACHE_DIR: Path = BASE_DOWNLOAD_DIR / "cache"

# Concurrency and queue settings
MAX_CONCURRENT_DOWNLOADS: int = 3
USER_RATE_LIMIT_SECONDS: int = 3
DOWNLOAD_TIMEOUT_SECONDS: int = 300
PROGRESS_UPDATE_INTERVAL_SECONDS: float = 2.0

# Quality presets
QUALITY_HIGHEST = "highest"
QUALITY_BEST = "best"
QUALITY_2160P = "2160p"
QUALITY_1440P = "1440p"
QUALITY_1080P = "1080p"
QUALITY_720P = "720p"
QUALITY_480P = "480p"
QUALITY_360P = "360p"
QUALITY_240P = "240p"
QUALITY_AUDIO = "audio"

QUALITY_LABELS = {
    QUALITY_HIGHEST: "Highest Quality",
    QUALITY_BEST: "Best",
    QUALITY_2160P: "4K (2160p)",
    QUALITY_1440P: "2K (1440p)",
    QUALITY_1080P: "1080p",
    QUALITY_720P: "720p",
    QUALITY_480P: "480p",
    QUALITY_360P: "360p",
    QUALITY_240P: "240p",
    QUALITY_AUDIO: "Audio Only (MP3)",
}


def is_ffmpeg_installed() -> bool:
    """Dynamically checks if ffmpeg binary is available."""
    try:
        from services.system_check import is_ffmpeg_installed as _check
        return _check()
    except Exception:
        return bool(shutil.which("ffmpeg"))
