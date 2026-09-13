import asyncio
import copy
import hashlib
import html
import logging
import mimetypes
import re
import shutil
import time
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import yt_dlp

from config import (
    BASE_DOWNLOAD_DIR,
    MAX_CONCURRENT_DOWNLOADS,
    MAX_FILE_SIZE_BYTES,
    PROGRESS_UPDATE_INTERVAL_SECONDS,
    QUALITY_2160P,
    QUALITY_1440P,
    QUALITY_1080P,
    QUALITY_720P,
    QUALITY_480P,
    QUALITY_360P,
    QUALITY_240P,
    QUALITY_BEST,
    QUALITY_HIGHEST,
    is_ffmpeg_installed,
)
from services.database import db
from services.platforms import PlatformInfo, detect_platform_from_url
from services.stats import (
    check_disk_space,
    record_download_finish,
    record_download_start,
    record_error,
)
from services.system_check import get_ffmpeg_version, resolve_ffmpeg_path

logger = logging.getLogger(__name__)

# Pre-resolve FFmpeg and Node.js at module initialization to eliminate per-download subprocess overhead
FFMPEG_BIN: Optional[str] = resolve_ffmpeg_path()
FFMPEG_VER: Optional[str] = get_ffmpeg_version()
HAS_FFMPEG: bool = bool(FFMPEG_BIN)

_node_cand = shutil.which("node") or (
    r"C:\Program Files\nodejs\node.exe"
    if Path(r"C:\Program Files\nodejs\node.exe").exists()
    else None
)
NODE_PATH: Optional[str] = str(_node_cand) if _node_cand else None

# Standard browser headers for Instagram, Facebook, and secondary sites
DEFAULT_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Mode": "navigate",
}


def _patch_instagram_extractor() -> None:
    """
    Patches yt-dlp's InstagramIE so it does not drop photo/carousel posts with
    'There is no video in this post', but instead returns the full product
    metadata (title, uploader, thumbnails, etc.) for photo/carousel delivery.
    """
    try:
        from yt_dlp.extractor.instagram import InstagramIE
        from yt_dlp.utils import traverse_obj
        import json

        orig_rnf = InstagramIE.raise_no_formats

        def patched_raise_no_formats(self, msg, expected=False, video_id=None):
            err_str = str(msg)
            if "There is no video in this post" in err_str or "No video formats found" in err_str:
                self.report_warning(msg, video_id)
                return
            return orig_rnf(self, msg, expected=expected, video_id=video_id)

        InstagramIE.raise_no_formats = patched_raise_no_formats

        orig_real_extract = InstagramIE._real_extract

        def patched_real_extract(self, url):
            try:
                return orig_real_extract(self, url)
            except yt_dlp.utils.ExtractorError as e:
                err_text = str(e)
                if "There is no video in this post" in err_text or "No video formats found" in err_text:
                    video_id, _ = self._match_valid_url(url).group('id', 'url')
                    webpage, _ = self._download_webpage_handle(
                        f'https://www.instagram.com/p/{video_id}', video_id, impersonate=self._can_impersonate)
                    media = traverse_obj(webpage, (
                        {self._SJS_RE.findall}, ..., {json.loads},
                        'require', ..., ..., ..., '__bbox', 'require',
                        lambda _, v: v[0] == 'RelayPrefetchedStreamCache', ...,
                        lambda _, v: v['__bbox']['result']['data']['xig_polaris_media'],
                        '__bbox', 'result', 'data', 'xig_polaris_media', {dict}, any))
                    product_info = traverse_obj(media, ('if_not_gated_logged_out', {dict}))
                    if product_info:
                        return self._extract_product(product_info, video_id=video_id, get_comments=False)
                raise

        InstagramIE._real_extract = patched_real_extract
    except Exception as patch_err:
        logger.warning(f"Could not patch InstagramIE: {patch_err}")


_patch_instagram_extractor()

# Hook YoutubeDL.process_ie_result to retain in-memory extracted info_dict across errors
_orig_process_ie_result = yt_dlp.YoutubeDL.process_ie_result

def _hooked_process_ie_result(self, ie_result, download=True, extra_info=None):
    self._last_ie_result = ie_result
    return _orig_process_ie_result(self, ie_result, download=download, extra_info=extra_info)

yt_dlp.YoutubeDL.process_ie_result = _hooked_process_ie_result

# Global concurrency semaphore to protect CPU and bandwidth
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

# Active cancellation and shutdown tracking
CANCELLED_REQUESTS: set[str] = set()
SHUTDOWN_REQUESTED: bool = False
ACTIVE_TEMP_DIRS: set[Path] = set()

# Regex to detect URLs in user text
URL_REGEX = re.compile(r"https?://[^\s<>\"'{}|\\^`\[\]]+", re.IGNORECASE)
DOMAIN_URL_REGEX = re.compile(
    r"(?:(?:www\.)|(?:(?:m\.)?(?:youtube\.com|youtu\.be|facebook\.com|fb\.watch|fb\.com|instagram\.com|instagr\.am|tiktok\.com|twitter\.com|x\.com|reddit\.com|pin\.it|pinterest\.com|vimeo\.com|threads\.net|dailymotion\.com|dai\.ly|soundcloud\.com|bandcamp\.com)/))[^\s<>\"'{}|\\^`\[\]]+",
    re.IGNORECASE,
)


@dataclass
class DownloadTimings:
    """Accurate timing metrics for identifying delivery bottlenecks."""
    format_extraction_s: float = 0.0
    ttfb_s: float = 0.0
    download_s: float = 0.0
    ffmpeg_processing_s: float = 0.0
    upload_s: float = 0.0
    total_s: float = 0.0
    file_size: int = 0

    def dominant_stage(self) -> str:
        stages = [
            ("Format Extraction", self.format_extraction_s),
            ("Source Download", self.download_s),
            ("FFmpeg Remuxing", self.ffmpeg_processing_s),
            ("Telegram Upload", self.upload_s),
        ]
        return max(stages, key=lambda x: x[1])[0]


@dataclass
class MediaInfo:
    """Extracted metadata for a media item."""
    title: str
    uploader: str
    duration: Optional[int]
    thumbnail: Optional[str]
    filesize_approx: Optional[int]
    width: Optional[int]
    height: Optional[int]
    webpage_url: str
    platform: PlatformInfo
    available_qualities: list[str] = field(default_factory=list)
    max_quality: str = "best"
    has_audio: bool = True
    raw_info: Optional[dict] = None
    extraction_time_s: float = 0.0
    media_type: str = "video"  # "video", "photo", "carousel"
    is_carousel: bool = False
    carousel_items: list[dict] = field(default_factory=list)


@dataclass
class DownloadResult:
    """Outcome of a media download task."""
    success: bool
    file_path: Optional[Path] = None
    media_info: Optional[MediaInfo] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    temp_dir: Optional[Path] = None
    is_audio: bool = False
    timings: DownloadTimings = field(default_factory=DownloadTimings)
    is_carousel: bool = False
    carousel_files: list[tuple[str, Path]] = field(default_factory=list)


def extract_url(text: str) -> Optional[str]:
    """Extracts first valid HTTP/HTTPS or domain media URL from string."""
    if not text:
        return None
    match = URL_REGEX.search(text)
    if match:
        return match.group(0)
    domain_match = DOMAIN_URL_REGEX.search(text)
    if domain_match:
        return "https://" + domain_match.group(0)
    return None


def format_duration(seconds: Optional[int]) -> str:
    """Formats duration in seconds to MM:SS or HH:MM:SS."""
    if not seconds or seconds <= 0:
        return "N/A"
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def cleanup_temp_dir(temp_dir: Optional[Path]) -> None:
    """Removes a temporary user download directory."""
    if temp_dir:
        ACTIVE_TEMP_DIRS.discard(temp_dir)
        if temp_dir.exists():
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp dir {temp_dir}: {e}")


def cancel_download(req_id: str) -> None:
    """Registers cancellation for an active download request."""
    CANCELLED_REQUESTS.add(req_id)


def cancel_all_active_downloads() -> int:
    """Signals cancellation to all running download threads during process shutdown."""
    global SHUTDOWN_REQUESTED
    SHUTDOWN_REQUESTED = True
    count = len(ACTIVE_TEMP_DIRS)
    logger.info(f"🛑 [DOWNLOADER] Global cancellation triggered for {count} active temp dirs.")
    return count


def cleanup_orphaned_temp_dirs(retention_seconds: int = 0) -> int:
    """
    Scans BASE_DOWNLOAD_DIR and safely purges stale 'user_*' download directories.
    Preserves cache/ and database files.
    Invoked on startup to scrub leftovers from previous abrupt restarts/SIGTERMs, and on shutdown.
    """
    if not BASE_DOWNLOAD_DIR.exists():
        return 0

    removed_count = 0
    now = time.time()
    try:
        for item in BASE_DOWNLOAD_DIR.iterdir():
            if item.is_dir() and item.name.startswith("user_"):
                if retention_seconds > 0:
                    try:
                        mtime = item.stat().st_mtime
                        if now - mtime < retention_seconds:
                            continue
                    except Exception:
                        pass
                try:
                    ACTIVE_TEMP_DIRS.discard(item)
                    shutil.rmtree(item, ignore_errors=True)
                    removed_count += 1
                except Exception as rm_err:
                    logger.warning(f"Failed to remove orphaned temp dir {item}: {rm_err}")
    except Exception as e:
        logger.warning(f"Error scanning for orphaned temp dirs: {e}")

    if removed_count > 0:
        logger.info(f"🧹 [DOWNLOADER] Cleaned up {removed_count} orphaned temporary directories.")
    return removed_count


def _is_video_format(f: dict) -> bool:
    """Checks if a format dictionary represents a video stream."""
    vcodec = f.get("vcodec")
    if vcodec == "none":
        return False
    if vcodec:
        return True
    ext = (f.get("ext") or "").lower()
    if ext in ("mp4", "webm", "mkv", "mov", "flv", "avi"):
        return True
    if f.get("format_id") in ("sd", "hd") or f.get("height") or f.get("width"):
        return True
    return False


def _has_audio_in_format(f: dict) -> bool:
    """Checks if a format dictionary contains audio."""
    acodec = f.get("acodec")
    if acodec == "none":
        return False
    if acodec:
        return True
    ext = (f.get("ext") or "").lower()
    if ext in ("m4a", "mp3", "opus", "aac", "ogg", "wav", "flac"):
        return True
    if f.get("format_id") in ("sd", "hd") or ext in ("mp4", "webm", "mkv"):
        return True
    return False


def analyze_media_formats(
    formats: list[dict],
    platform_info: PlatformInfo,
    has_ffmpeg: bool,
    max_filesize: int = MAX_FILE_SIZE_BYTES,
    duration: Optional[int] = None,
) -> tuple[list[str], str, bool]:
    """
    Intelligently analyzes extracted formats.
    Returns: (available_qualities, max_quality_label, has_audio)

    Policy:
    - Instagram & Facebook:
      Automatically selects highest available quality directly.
      Returns available_qualities = ["highest"] (no selection picker needed).
    - YouTube & Other Platforms:
      Inspects formats fitting within max_filesize.
      If max available quality >= 1080p:
        Returns genuine available quality options (e.g. ['1080p', '720p', '480p', '360p']).
      If max available quality < 1080p (e.g. 720p or Shorts):
        Returns single quality tier (e.g. ['720p']), triggering direct automatic download.
    """
    has_audio = any(_has_audio_in_format(f) for f in formats)

    video_formats = [
        f for f in formats
        if _is_video_format(f)
    ]

    def _get_est_size(f: dict) -> int:
        sz = f.get("filesize") or f.get("filesize_approx") or 0
        if not sz and duration:
            bitrate = f.get("tbr") or f.get("vbr") or 0
            if bitrate:
                sz = int((bitrate * 1000 / 8) * duration)
        return sz

    plat_key = getattr(platform_info, "key", "").lower()

    # 1. Instagram & Facebook: Always highest available quality directly
    if plat_key in ("instagram", "facebook"):
        max_h = 0
        has_hd = False
        for f in video_formats:
            sz = _get_est_size(f)
            if sz > max_filesize:
                continue
            is_prog = _has_audio_in_format(f)
            if not is_prog and not (has_ffmpeg and has_audio):
                continue

            h = f.get("height") or 0
            w = f.get("width") or 0
            res = min(h, w) if (h and w) else h
            if res and res > max_h:
                max_h = res
            note = str(f.get("format_note", "")).lower() + " " + str(f.get("format_id", "")).lower()
            if "hd" in note:
                has_hd = True

        if max_h >= 1080:
            quality_label = "1080p"
        elif max_h >= 720:
            quality_label = "720p"
        elif max_h >= 480:
            quality_label = "480p"
        elif max_h > 0:
            quality_label = f"{max_h}p"
        elif has_hd:
            quality_label = "HD"
        else:
            quality_label = "Highest"

        return [QUALITY_HIGHEST], quality_label, has_audio

    # 2. YouTube & Secondary Platforms
    tier_ranges = [
        (QUALITY_2160P, lambda r: r >= 2160),
        (QUALITY_1440P, lambda r: 1440 <= r < 2160),
        (QUALITY_1080P, lambda r: 1080 <= r < 1440),
        (QUALITY_720P, lambda r: 720 <= r < 1080),
        (QUALITY_480P, lambda r: 480 <= r < 720),
        (QUALITY_360P, lambda r: 360 <= r < 480),
        (QUALITY_240P, lambda r: r < 360),
    ]

    valid_tiers: list[str] = []
    for tier_name, matcher in tier_ranges:
        matched = False
        for f in video_formats:
            sz = _get_est_size(f)
            if sz > max_filesize:
                continue
            is_prog = _has_audio_in_format(f)
            if not is_prog and not (has_ffmpeg and has_audio):
                continue
            h = f.get("height") or 0
            w = f.get("width") or 0
            res = min(h, w) if (h and w) else h
            if res and matcher(res):
                matched = True
                break
        if matched:
            valid_tiers.append(tier_name)

    if not valid_tiers:
        return [QUALITY_BEST], "Best", has_audio

    highest_tier = valid_tiers[0]
    h_match = re.search(r"\d+", highest_tier)
    highest_h = int(h_match.group(0)) if h_match else 0

    # If highest available is >= 1080p, present genuine quality tiers; otherwise auto-download
    if highest_h >= 1080:
        return valid_tiers[:4], highest_tier, has_audio
    else:
        return [highest_tier], highest_tier, has_audio


def _build_format_options(
    quality: str,
    platform_info: PlatformInfo,
    has_ffmpeg: bool,
) -> tuple[str, list, Optional[str]]:
    """
    Constructs the optimal format specifier and postprocessors.
    Prioritizes direct progressive MP4 streams to avoid merging overhead.
    When merging is required, stream-copying (-c copy) is used with ZERO re-encoding.
    """
    postprocessors = []
    merge_output_format = None

    plat_key = getattr(platform_info, "key", "").lower()

    # Instagram & Facebook: Highest available quality
    # Prioritize progressive streams with audio, or merge with AAC audio
    if plat_key in ("instagram", "facebook"):
        if has_ffmpeg:
            merge_output_format = "mp4"
            format_spec = (
                "best[ext=mp4][acodec!=none]/"
                "best[acodec!=none]/"
                "bestvideo[ext=mp4]+bestaudio[ext=m4a]/"
                "bestvideo+bestaudio[ext=m4a]/"
                "bestvideo+bestaudio/"
                "best"
            )
        else:
            format_spec = (
                "best[ext=mp4][acodec!=none]/"
                "best[acodec!=none]/"
                "best"
            )
        return format_spec, postprocessors, merge_output_format

    # YouTube and generic platforms for 'highest' / 'best':
    # Cap at 1080p and guarantee audio stream is always included and merged
    if quality in (QUALITY_HIGHEST, QUALITY_BEST):
        if has_ffmpeg:
            merge_output_format = "mp4"
            format_spec = (
                "22/18/"
                "best[height<=1080][ext=mp4][acodec!=none]/"
                "best[height<=1080][acodec!=none]/"
                "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/"
                "bestvideo[height<=1080]+bestaudio[ext=m4a]/"
                "bestvideo[height<=1080]+bestaudio/"
                "bestvideo+bestaudio/"
                "best[height<=720][ext=mp4][acodec!=none]/"
                "best[height<=720][acodec!=none]/"
                "best[acodec!=none]/"
                "best"
            )
        else:
            format_spec = (
                "22/18/"
                "best[height<=1080][ext=mp4][acodec!=none]/"
                "best[height<=1080][acodec!=none]/"
                "best[height<=720][ext=mp4][acodec!=none]/"
                "best[height<=720][acodec!=none]/"
                "best[acodec!=none]/"
                "best"
            )
        return format_spec, postprocessors, merge_output_format

    # YouTube and specific resolution tiers (e.g. 1080p, 720p, 480p)
    h_match = re.search(r"\d+", quality)
    h_val = int(h_match.group(0)) if h_match else 720
    h_min = max(1, h_val - 120)

    if h_val <= 720:
        if has_ffmpeg:
            merge_output_format = "mp4"
            format_spec = (
                "22/18/"
                f"best[height<={h_val}][height>{h_min}][ext=mp4][acodec!=none]/"
                f"best[width<={h_val}][width>{h_min}][ext=mp4][acodec!=none]/"
                f"best[height<={h_val}][ext=mp4][acodec!=none]/"
                f"best[height<={h_val}][acodec!=none]/"
                f"bestvideo[height<={h_val}][height>{h_min}][ext=mp4]+bestaudio[ext=m4a]/"
                f"bestvideo[width<={h_val}][width>{h_min}][ext=mp4]+bestaudio[ext=m4a]/"
                f"bestvideo[height<={h_val}]+bestaudio[ext=m4a]/"
                f"bestvideo[height<={h_val}]+bestaudio/"
                f"best[acodec!=none]/"
                f"best"
            )
        else:
            format_spec = (
                "22/18/"
                f"best[height<={h_val}][height>{h_min}][ext=mp4][acodec!=none]/"
                f"best[width<={h_val}][width>{h_min}][ext=mp4][acodec!=none]/"
                f"best[height<={h_val}][acodec!=none]/"
                f"best[width<={h_val}][acodec!=none]/"
                f"best[acodec!=none]/"
                f"best"
            )
    else:
        # For 1080p+ separate streams are necessary to achieve 1080p/2K/4K resolution
        if has_ffmpeg:
            merge_output_format = "mp4"
            format_spec = (
                f"bestvideo[height<={h_val}][height>{h_min}][ext=mp4]+bestaudio[ext=m4a]/"
                f"bestvideo[width<={h_val}][width>{h_min}][ext=mp4]+bestaudio[ext=m4a]/"
                f"bestvideo[height<={h_val}][height>{h_min}]+bestaudio[ext=m4a]/"
                f"bestvideo[height<={h_val}][height>{h_min}]+bestaudio/"
                f"bestvideo[height<={h_val}]+bestaudio[ext=m4a]/"
                f"bestvideo[height<={h_val}]+bestaudio/"
                f"bestvideo+bestaudio/"
                f"best[height<={h_val}][height>{h_min}][ext=mp4][acodec!=none]/"
                f"best[width<={h_val}][width>{h_min}][ext=mp4][acodec!=none]/"
                f"best[height<={h_val}][ext=mp4][acodec!=none]/"
                f"best[height<={h_val}][acodec!=none]/"
                f"best[acodec!=none]/"
                f"best"
            )
        else:
            format_spec = (
                f"best[height<={h_val}][height>{h_min}][ext=mp4][acodec!=none]/"
                f"best[width<={h_val}][width>{h_min}][ext=mp4][acodec!=none]/"
                f"best[height<={h_val}][ext=mp4][acodec!=none]/"
                f"best[height<={h_val}][acodec!=none]/"
                f"best[acodec!=none]/"
                f"best"
            )

    return format_spec, postprocessors, merge_output_format


def _extract_instagram_embed_fallback(url: str) -> Optional[dict]:
    """
    Extracts high-resolution media (video or photo) and caption from Instagram embed HTML
    when yt-dlp refuses single photo posts or encounters transient API format mismatches.
    Respects private and login-required restrictions without bypassing them.
    """
    shortcode_match = re.search(r"/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)", url)
    if not shortcode_match:
        return None
    shortcode = shortcode_match.group(1)
    embed_url = f"https://www.instagram.com/p/{shortcode}/embed/captioned/"
    headers = {
        "User-Agent": DEFAULT_HTTP_HEADERS["User-Agent"],
        "Accept-Language": "en-US,en;q=0.9",
    }
    req = urllib.request.Request(embed_url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            body_lower = body.lower()

            # Respect Instagram platform restrictions without bypassing them
            if any(k in body_lower for k in ("this account is private", "private account", "only approved followers")):
                logger.info(f"Instagram embed confirms private account for {shortcode}")
                raise yt_dlp.utils.ExtractorError(
                    "This Instagram account is private. Content is only visible to approved followers.",
                    expected=True,
                )
            if any(k in body_lower for k in ("please log in", "login required", "checkpoint_required", "redirected to login")):
                logger.info(f"Instagram embed confirms login required for {shortcode}")
                raise yt_dlp.utils.ExtractorError(
                    "Instagram requires authentication to access this content (Login Required).",
                    expected=True,
                )
            if any(k in body_lower for k in ("age-restricted", "sensitive content", "viewer discretion", "must be 18")):
                logger.info(f"Instagram embed confirms sensitive/age barrier for {shortcode}")
                raise yt_dlp.utils.ExtractorError(
                    "Instagram restricted this post behind an age/sensitivity verification barrier.",
                    expected=True,
                )
            if any(k in body_lower for k in ("content isn't available", "link you followed may be broken", "page not found")):
                logger.info(f"Instagram embed confirms post unavailable/deleted for {shortcode}")
                raise yt_dlp.utils.ExtractorError(
                    "This post does not exist or has been deleted by the owner.",
                    expected=True,
                )

            # 1. Check for Video
            video_url = None
            m_vid = re.search(r'<video[^>]*class="EmbeddedMediaVideo"[^>]*src="([^"]+)"', body)
            if not m_vid:
                m_vid = re.search(r'<video[^>]*src="([^"]+)"', body)
            if m_vid:
                video_url = html.unescape(m_vid.group(1)).replace("&amp;", "&")
            else:
                for match in re.findall(r'"video_url"\s*:\s*"([^"]+)"', body):
                    try:
                        cand = html.unescape(match.encode().decode('unicode-escape', errors='ignore')).replace("&amp;", "&")
                        if "cdninstagram" in cand or "fbcdn" in cand:
                            video_url = cand
                            break
                    except Exception:
                        pass

            # 2. Check for Photo / Thumbnail
            photo_url = None
            m_img = re.search(r'<img[^>]*class="EmbeddedMediaImage"[^>]*src="([^"]+)"', body)
            if not m_img:
                m_img = re.search(r'<img[^>]*src="([^"]+)"[^>]*class="EmbeddedMediaImage"', body)

            if m_img:
                photo_url = html.unescape(m_img.group(1)).replace("&amp;", "&")
            else:
                for match in re.findall(r'https://[^"\'<>\s]+', body):
                    cand = html.unescape(match).replace("&amp;", "&")
                    if ("scontent" in cand or "fbcdn" in cand) and "static.cdninstagram.com" not in cand:
                        if any(ext in cand for ext in (".jpg", ".webp", ".png", "stp=dst-jpg")):
                            photo_url = cand
                            break

            if not video_url and not photo_url:
                return None

            author = "Instagram Creator"
            auth_m = re.search(r'<a[^>]*class="UsernameText"[^>]*>([^<]+)</a>', body)
            if auth_m:
                author = auth_m.group(1).strip()

            caption = ""
            cap_m = re.search(r'<div class="Caption"[^>]*>(.*?)</div>', body, re.DOTALL)
            if cap_m:
                raw_c = re.sub(r"<[^>]+>", " ", cap_m.group(1))
                caption = html.unescape(raw_c).strip()

            title = caption[:80] if caption else f"Instagram Media by {author}"

            if video_url:
                return {
                    "id": shortcode,
                    "title": title,
                    "uploader": author,
                    "url": video_url,
                    "formats": [{
                        "url": video_url,
                        "ext": "mp4",
                        "format_id": "embed_video",
                        "vcodec": "h264",
                        "acodec": "aac",
                    }],
                    "thumbnail": photo_url,
                    "thumbnails": [{"url": photo_url}] if photo_url else [],
                    "ext": "mp4",
                    "media_type": "video",
                    "is_carousel": False,
                    "carousel_items": [],
                    "extractor": "generic",
                    "extractor_key": "Generic",
                }
            else:
                return {
                    "id": shortcode,
                    "title": title,
                    "uploader": author,
                    "thumbnails": [{"url": photo_url}],
                    "thumbnail": photo_url,
                    "url": photo_url,
                    "ext": "jpg",
                    "media_type": "photo",
                    "is_carousel": False,
                    "carousel_items": [],
                    "extractor": "generic",
                    "extractor_key": "Generic",
                }
    except urllib.error.HTTPError as http_err:
        if http_err.code == 404:
            raise yt_dlp.utils.ExtractorError(
                "This post does not exist or has been deleted by the owner.",
                expected=True,
            )
        elif http_err.code == 429:
            raise yt_dlp.utils.ExtractorError(
                "Instagram is temporarily limiting requests (429 Too Many Requests).",
                expected=True,
            )
        elif http_err.code in (401, 403):
            raise yt_dlp.utils.ExtractorError(
                "Instagram requires authentication to access this content.",
                expected=True,
            )
        logger.warning(f"Instagram embed HTTP error for {url}: {http_err}")
        return None
    except yt_dlp.utils.ExtractorError:
        raise
    except Exception as e:
        logger.warning(f"Instagram embed fallback error for {url}: {e}")
        return None



def format_extraction_error(error: Any, url: str = "") -> str:
    """
    Translates extraction/download exceptions into precise, transparent technical reasons
    (Private account, Login requirement, Age/Sensitive content barrier, Deleted/Unavailable,
    Geographic restriction, Rate-limiting, or exact underlying platform error)
    instead of opaque or generic rejection messages.
    """
    err_str = str(error).strip()
    err_lower = err_str.lower()

    # 1. Private Account (Do not bypass)
    if any(k in err_lower for k in ("this account is private", "private account", "only approved followers")):
        return "🔒 Private Account: This Instagram account is private. Content is only visible to approved followers."

    # 2. Login Requirement (Do not bypass)
    if any(k in err_lower for k in ("login required", "please log in", "checkpoint_required", "redirected to login", "sign in to view")):
        return "🔒 Login Required: Instagram requires authentication to access this content."

    # 3. Age / Sensitive Content Access Controls (Do not bypass)
    if any(k in err_lower for k in ("age-restricted", "age restricted", "sensitive content", "viewer discretion", "must be 18")):
        return "🔞 Age / Sensitive Content: Instagram has restricted this post behind an age/sensitivity verification barrier."

    # 4. Deleted or Unavailable Content
    if any(k in err_lower for k in ("not found", "deleted", "unavailable", "404", "this content isn't available")):
        return "🚫 Post Unavailable: This post does not exist or has been deleted by the owner."

    # 5. Geographic / Region Restriction
    if any(k in err_lower for k in ("geo-restricted", "geoblocked", "not available in your region", "not available in your country")):
        return "🌍 Region Restricted: This Instagram media is restricted in the server's geographic location."

    # 6. Rate Limited / Temporary Block
    if any(k in err_lower for k in ("429", "too many requests", "rate limit", "temporarily blocked")):
        return "⏳ Rate Limited: Instagram is temporarily limiting requests. Please wait a minute and retry."

    # 7. Empty Media Response
    if "empty media response" in err_lower:
        return "⚠️ Empty Media: Instagram returned an empty response (post may be restricted, expired, or unavailable)."

    # 8. File Size Limit
    if "too large" in err_lower or "max_filesize" in err_lower:
        return "⚠️ File Size Limit: Media exceeds Telegram's file size limit (50 MB)."

    # 9. Unsupported URL
    if "unsupported url" in err_lower:
        return "❌ Unsupported Link: This URL format is not supported."

    # 10. Clean and expose underlying technical error
    clean_msg = re.sub(r"^ERROR:\s*\[[^\]]+\]\s*[^:]*:\s*", "", err_str, flags=re.IGNORECASE).strip()
    if clean_msg:
        first_line = clean_msg.split("\n")[0].strip()
        if len(first_line) > 140:
            first_line = first_line[:137] + "…"
        return f"❌ Technical Reason: {first_line}"

    return "❌ Media extraction failed. Please check if the link is accessible."



def _sync_extract_preview(url: str) -> tuple[bool, Optional[MediaInfo], Optional[str]]:
    """Synchronous fast info extraction without downloading media."""
    t_start = time.time()
    platform_info = detect_platform_from_url(url)
    has_ffmpeg = HAS_FFMPEG

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "extract_flat": False,
        "socket_timeout": 8,
        "http_headers": DEFAULT_HTTP_HEADERS,
        "extractor_args": {
            "youtube": {
                "player_client": ["android", "ios"],
                "player_skip": ["configs", "webpage"],
            }
        },
    }
    if NODE_PATH:
        ydl_opts["js_runtimes"] = {"node": {"path": NODE_PATH}}

    info = None
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                info = ydl.extract_info(url, download=False)
            except yt_dlp.utils.DownloadError as dl_err:
                err_lower = str(dl_err).lower()
                if "no video formats found" in err_lower or "there is no video in this post" in err_lower or "no video in this post" in err_lower:
                    logger.info(f"Fallback to process=False for potential photo/carousel: {url}")
                    try:
                        with yt_dlp.YoutubeDL({
                            "quiet": True,
                            "no_warnings": True,
                            "extract_flat": False,
                            "http_headers": DEFAULT_HTTP_HEADERS,
                        }) as ydl_raw:
                            info = ydl_raw.extract_info(url, download=False, process=False)
                    except yt_dlp.utils.DownloadError as raw_err:
                        raw_err_lower = str(raw_err).lower()
                        if platform_info.key == "instagram" and ("there is no video in this post" in raw_err_lower or "no video in this post" in raw_err_lower):
                            info = _extract_instagram_embed_fallback(url)
                            if not info:
                                raise raw_err
                        else:
                            raise raw_err
                    if not info and platform_info.key == "instagram":
                        info = _extract_instagram_embed_fallback(url)
                elif platform_info.key == "instagram" and any(k in err_lower for k in ("empty media response", "unable to extract", "no video formats", "format is not available")):
                    info = _extract_instagram_embed_fallback(url)
                    if not info:
                        raise dl_err
                else:
                    raise dl_err

        if not info and platform_info.key == "instagram":
            info = _extract_instagram_embed_fallback(url)

        if not info:
            if platform_info.key == "instagram":
                return False, None, "⚠️ Empty Media: Instagram returned an empty response (post may be restricted, expired, or unavailable)."
            return False, None, "❌ Media extraction failed. Platform returned no data."

        # Detect Carousel vs Single Photo vs Video
        is_carousel = False
        carousel_items = []
        media_type = "video"

        entries = info.get("entries")
        if entries and len(entries) > 1:
            is_carousel = True
            media_type = "carousel"
            for idx, entry in enumerate(entries):
                if not entry:
                    continue
                formats = entry.get("formats") or []
                item_is_video = bool(formats and any(_is_video_format(f) for f in formats))
                item_type = "video" if item_is_video else "photo"
                item_url = None
                if item_is_video:
                    if formats:
                        item_url = formats[-1].get("url")
                    if not item_url:
                        item_url = entry.get("webpage_url") or entry.get("url") or url
                else:
                    if entry.get("thumbnails"):
                        item_url = entry["thumbnails"][-1].get("url")
                    elif entry.get("url"):
                        item_url = entry.get("url")

                carousel_items.append({
                    "index": idx,
                    "id": entry.get("id", f"item_{idx}"),
                    "type": item_type,
                    "url": item_url,
                    "raw_entry": entry,
                })
                logger.info(
                    f"carousel item index: {idx} | media type: {item_type} | "
                    f"source URL: {item_url[:80] if item_url else 'None'}"
                )
            available_qualities = ["highest"]
            max_quality = "highest"
            has_audio = any(it.get("type") == "video" for it in carousel_items)
            primary_entry = entries[0]
        else:
            if entries and len(entries) == 1:
                info = entries[0]
            primary_entry = info

            formats = info.get("formats") or []
            has_video = any(_is_video_format(f) for f in formats)

            if not has_video:
                media_type = "photo"
                photo_url = None
                if info.get("thumbnails"):
                    photo_url = info["thumbnails"][-1].get("url")
                elif info.get("url"):
                    photo_url = info.get("url")
                logger.info("media_type: photo")
                logger.info(f"image_url_available: {bool(photo_url)}")
                available_qualities = ["photo"]
                max_quality = "photo"
                has_audio = False
            else:
                media_type = "video"
                available_qualities, max_quality, has_audio = analyze_media_formats(
                    formats=formats,
                    platform_info=platform_info,
                    has_ffmpeg=has_ffmpeg,
                    duration=info.get("duration"),
                )
                logger.info(f"extractor name: {info.get('extractor_key') or info.get('extractor')}")
                logger.info(f"title: {info.get('title')}")
                logger.info(f"duration: {info.get('duration')}")
                logger.info(f"available formats: {len(formats)}")
                logger.info(f"selected format: {max_quality}")
                logger.info(f"extension: {info.get('ext') or 'mp4'}")

        extraction_time_s = time.time() - t_start

        # Store detected fields inside raw_info for cache restoration
        info["media_type"] = media_type
        info["is_carousel"] = is_carousel
        info["carousel_items"] = carousel_items

        media_info = MediaInfo(
            title=info.get("title", "Media"),
            uploader=info.get("uploader") or info.get("channel") or "Unknown Creator",
            duration=info.get("duration"),
            thumbnail=info.get("thumbnail") or (carousel_items[0]["url"] if (carousel_items and carousel_items[0]["url"]) else None),
            filesize_approx=info.get("filesize") or info.get("filesize_approx"),
            width=primary_entry.get("width"),
            height=primary_entry.get("height"),
            webpage_url=info.get("webpage_url", url),
            platform=platform_info,
            available_qualities=available_qualities,
            max_quality=max_quality,
            has_audio=has_audio,
            raw_info=info,
            extraction_time_s=extraction_time_s,
            media_type=media_type,
            is_carousel=is_carousel,
            carousel_items=carousel_items,
        )
        return True, media_info, None

    except yt_dlp.utils.DownloadError as e:
        logger.error(f"Preview extraction DownloadError for {url}: {e}", exc_info=True)
        return False, None, format_extraction_error(e, url)
    except Exception as e:
        logger.error(f"Unexpected preview error for {url}: {e}", exc_info=True)
        return False, None, format_extraction_error(e, url)


async def get_media_preview(url: str) -> tuple[bool, Optional[MediaInfo], Optional[str]]:
    """
    Asynchronous wrapper for metadata extraction with SQLite metadata cache.
    Eliminates redundant network calls if the same link was recently inspected.
    """
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:24]

    # Check SQLite metadata cache
    cached_meta = await db.get_metadata(url_hash)
    if cached_meta:
        platform_info = detect_platform_from_url(url)
        raw_info = cached_meta["raw_info"] or {}
        media_type = raw_info.get("media_type", "video")
        is_carousel = raw_info.get("is_carousel", False)
        carousel_items = raw_info.get("carousel_items", [])
        media_info = MediaInfo(
            title=cached_meta["title"],
            uploader=raw_info.get("uploader") or raw_info.get("channel") or "Unknown Creator",
            duration=raw_info.get("duration"),
            thumbnail=raw_info.get("thumbnail"),
            filesize_approx=raw_info.get("filesize") or raw_info.get("filesize_approx"),
            width=raw_info.get("width"),
            height=raw_info.get("height"),
            webpage_url=raw_info.get("webpage_url", url),
            platform=platform_info,
            available_qualities=cached_meta["available_qualities"],
            max_quality=cached_meta["available_qualities"][0] if cached_meta["available_qualities"] else "best",
            has_audio=True,
            raw_info=raw_info,
            extraction_time_s=0.01,
            media_type=media_type,
            is_carousel=is_carousel,
            carousel_items=carousel_items,
        )
        return True, media_info, None

    success, media_info, err = await asyncio.to_thread(_sync_extract_preview, url)
    if success and media_info and media_info.raw_info:
        await db.put_metadata(
            url_hash=url_hash,
            canonical_id=media_info.platform.key,
            platform=media_info.platform.key,
            title=media_info.title,
            raw_info=media_info.raw_info,
            available_qualities=media_info.available_qualities,
            ttl_seconds=3600,
        )
    return success, media_info, err


def _sync_download(
    url: str,
    user_id: int,
    quality: str = QUALITY_BEST,
    req_id: Optional[str] = None,
    progress_callback: Optional[Callable[[str, str, str], None]] = None,
    cached_info: Optional[dict] = None,
    extraction_time_s: float = 0.0,
) -> DownloadResult:
    """
    Synchronous worker executing download with concurrent fragment acceleration,
    FFmpeg stream-copy merging (-c copy), and stage timing breakdown.
    """
    t_start = time.time()

    # Verify free disk space
    has_space, free_gb, _ = check_disk_space()
    if not has_space:
        return DownloadResult(
            success=False,
            error_code="DISK_SPACE_LOW",
            error_message="⚠️ Server storage is temporarily low.",
        )

    # Check FFmpeg (Requirement 5)
    ffmpeg_bin = resolve_ffmpeg_path()
    ffmpeg_ver = get_ffmpeg_version()
    has_ffmpeg = bool(ffmpeg_bin)
    logger.info(f"ffmpeg exists: {has_ffmpeg}")
    logger.info(f"ffmpeg executable path: {ffmpeg_bin}")
    logger.info(f"ffmpeg version: {ffmpeg_ver}")

    platform_info = detect_platform_from_url(url)
    timestamp = int(time.time())
    unique_id = uuid.uuid4().hex[:8]
    temp_dir = BASE_DOWNLOAD_DIR / f"user_{user_id}_{timestamp}_{unique_id}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    ACTIVE_TEMP_DIRS.add(temp_dir)

    record_download_start()

    is_carousel = bool(cached_info and (cached_info.get("is_carousel") or len(cached_info.get("carousel_items", [])) > 1))
    media_type = cached_info.get("media_type", "video") if cached_info else ("photo" if quality == "photo" else "video")

    # -------------------------------------------------------------
    # 1. CAROUSEL PIPELINE (Requirement 7)
    # -------------------------------------------------------------
    if is_carousel:
        logger.info(f"Entering carousel download pipeline: {len(cached_info.get('carousel_items', []))} items")
        carousel_items = cached_info.get("carousel_items", [])
        carousel_files: list[tuple[str, Path]] = []
        for it in carousel_items:
            idx = it["index"]
            itype = it["type"]
            iurl = it["url"]
            iid = it.get("id", f"item_{idx}")
            logger.info(f"carousel item index: {idx}")
            logger.info(f"carousel item media type: {itype}")
            logger.info(f"carousel item source URL: {iurl}")

            if not iurl:
                logger.warning(f"Carousel item {idx} missing source URL, skipping")
                continue

            if itype == "photo":
                try:
                    req = urllib.request.Request(iurl, headers=DEFAULT_HTTP_HEADERS)
                    with urllib.request.urlopen(req, timeout=20) as resp:
                        ctype = resp.headers.get("Content-Type", "")
                        ext = ".webp" if "webp" in ctype else (".png" if "png" in ctype else ".jpg")
                        item_file = temp_dir / f"carousel_{idx}_{iid}{ext}"
                        with open(item_file, "wb") as f:
                            f.write(resp.read())
                        item_size = item_file.stat().st_size
                        if item_file.exists() and item_size > 0:
                            logger.info(f"carousel item download success: True")
                            logger.info(f"carousel item local path: {item_file}")
                            carousel_files.append(("photo", item_file))
                        else:
                            logger.error(f"Carousel item {idx} downloaded 0 bytes")
                except Exception as dl_e:
                    logger.error(f"Failed to download carousel photo {idx}: {dl_e}", exc_info=True)
            else:
                # Video item download
                try:
                    item_file = temp_dir / f"carousel_{idx}_{iid}.mp4"
                    downloaded_ok = False
                    if iurl and ("fbcdn.net" in iurl or "cdninstagram.com" in iurl or ".mp4" in iurl):
                        try:
                            req = urllib.request.Request(iurl, headers=DEFAULT_HTTP_HEADERS)
                            with urllib.request.urlopen(req, timeout=30) as resp:
                                with open(item_file, "wb") as f:
                                    while chunk := resp.read(1024 * 512):
                                        f.write(chunk)
                            if item_file.exists() and item_file.stat().st_size > 0:
                                downloaded_ok = True
                        except Exception as dl_direct_e:
                            logger.debug(f"Direct stream download failed for carousel video {idx}, falling back to yt-dlp: {dl_direct_e}")

                    if not downloaded_ok:
                        v_ydl_opts = {
                            "format": "best[ext=mp4][acodec!=none]/best[acodec!=none]/bestvideo+bestaudio/best",
                            "outtmpl": str(temp_dir / f"carousel_{idx}_%(id)s.%(ext)s"),
                            "merge_output_format": "mp4",
                            "postprocessor_args": {"merger": ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"]},
                            "quiet": True,
                            "no_warnings": True,
                            "socket_timeout": 20,
                            "http_headers": DEFAULT_HTTP_HEADERS,
                        }
                        if ffmpeg_bin:
                            v_ydl_opts["ffmpeg_location"] = str(Path(ffmpeg_bin).parent)
                        with yt_dlp.YoutubeDL(v_ydl_opts) as ydl_item:
                            ydl_item.download([iurl])
                        found_v = [f for f in temp_dir.glob(f"carousel_{idx}_*") if f.is_file() and not f.name.endswith((".part", ".ytdl"))]
                        if found_v and found_v[0].stat().st_size > 0:
                            item_file = found_v[0]
                            downloaded_ok = True

                    if downloaded_ok and item_file.exists() and item_file.stat().st_size > 0:
                        logger.info(f"carousel item download success: True")
                        logger.info(f"carousel item local path: {item_file}")
                        carousel_files.append(("video", item_file))
                    else:
                        logger.error(f"Carousel item {idx} video download failed")
                except Exception as vid_e:
                    logger.error(f"Failed to download carousel video {idx}: {vid_e}", exc_info=True)

        if not carousel_files:
            record_download_finish(success=False)
            return DownloadResult(
                success=False,
                error_code="CAROUSEL_DOWNLOAD_FAILED",
                error_message="❌ Failed to download carousel items.",
                temp_dir=temp_dir,
            )

        record_download_finish(success=True)
        primary_file = carousel_files[0][1]
        media_info = MediaInfo(
            title=cached_info.get("title", "Carousel"),
            uploader=cached_info.get("uploader", "Creator"),
            duration=None,
            thumbnail=None,
            filesize_approx=sum(p.stat().st_size for _, p in carousel_files),
            width=None,
            height=None,
            webpage_url=url,
            platform=platform_info,
            available_qualities=["highest"],
            max_quality="highest",
            is_carousel=True,
            media_type="carousel",
        )
        return DownloadResult(
            success=True,
            file_path=primary_file,
            media_info=media_info,
            temp_dir=temp_dir,
            is_carousel=True,
            carousel_files=carousel_files,
            timings=DownloadTimings(format_extraction_s=extraction_time_s, download_s=time.time() - t_start),
        )

    # -------------------------------------------------------------
    # 2. PHOTO PIPELINE (Requirement 6)
    # -------------------------------------------------------------
    if quality == "photo" or media_type == "photo":
        logger.info("Entering dedicated photo download pipeline (no video formats)")
        photo_url = None
        if cached_info:
            if cached_info.get("thumbnails"):
                photo_url = cached_info["thumbnails"][-1].get("url")
            elif cached_info.get("url"):
                photo_url = cached_info.get("url")

        if not photo_url:
            # Fallback extraction of image URL
            try:
                with yt_dlp.YoutubeDL({
                    "quiet": True,
                    "no_warnings": True,
                    "extract_flat": False,
                    "http_headers": DEFAULT_HTTP_HEADERS,
                }) as ydl_p:
                    raw_p = ydl_p.extract_info(url, download=False, process=False)
                    if raw_p:
                        entries_p = raw_p.get("entries") or [raw_p]
                        e0 = entries_p[0]
                        if e0.get("thumbnails"):
                            photo_url = e0["thumbnails"][-1].get("url")
                        elif e0.get("url"):
                            photo_url = e0.get("url")
            except Exception as e:
                logger.warning(f"Photo URL fallback error: {e}")

        if not photo_url and platform_info.key == "instagram":
            fb_info = _extract_instagram_embed_fallback(url)
            if fb_info and fb_info.get("url"):
                photo_url = fb_info["url"]

        logger.info("media_type: photo")
        logger.info(f"image_url_available: {bool(photo_url)}")

        if not photo_url:
            record_download_finish(success=False)
            return DownloadResult(
                success=False,
                error_code="PHOTO_URL_NOT_FOUND",
                error_message="❌ Couldn't locate photo URL.",
                temp_dir=temp_dir,
            )

        try:
            req = urllib.request.Request(photo_url, headers=DEFAULT_HTTP_HEADERS)
            with urllib.request.urlopen(req, timeout=20) as resp:
                status = resp.getcode()
                ctype = resp.headers.get("Content-Type", "image/jpeg")
                data = resp.read()
                ext = ".webp" if "webp" in ctype else (".png" if "png" in ctype else ".jpg")
                photo_file = temp_dir / f"photo_{unique_id}{ext}"
                with open(photo_file, "wb") as f:
                    f.write(data)

                d_size = len(data)
                logger.info(f"image content-type: {ctype}")
                logger.info(f"HTTP status: {status}")
                logger.info(f"downloaded size: {d_size} bytes")
                logger.info(f"final image path: {photo_file}")

                if not photo_file.exists() or d_size == 0:
                    record_download_finish(success=False)
                    return DownloadResult(
                        success=False,
                        error_code="PHOTO_DOWNLOAD_FAILED",
                        error_message="❌ Photo download failed (0 bytes received).",
                        temp_dir=temp_dir,
                    )

                record_download_finish(success=True)
                media_info = MediaInfo(
                    title=(cached_info.get("title") if cached_info else None) or "Photo",
                    uploader=(cached_info.get("uploader") if cached_info else None) or "Unknown Creator",
                    duration=None,
                    thumbnail=photo_url,
                    filesize_approx=d_size,
                    width=None,
                    height=None,
                    webpage_url=url,
                    platform=platform_info,
                    available_qualities=["photo"],
                    max_quality="photo",
                    has_audio=False,
                    media_type="photo",
                )
                return DownloadResult(
                    success=True,
                    file_path=photo_file,
                    media_info=media_info,
                    temp_dir=temp_dir,
                    is_audio=False,
                    timings=DownloadTimings(format_extraction_s=extraction_time_s, download_s=time.time() - t_start),
                )
        except Exception as photo_dl_err:
            record_download_finish(success=False)
            logger.error(f"Failed direct photo download from {photo_url}: {photo_dl_err}", exc_info=True)
            return DownloadResult(
                success=False,
                error_code="PHOTO_DOWNLOAD_ERROR",
                error_message="❌ Photo download failed.",
                temp_dir=temp_dir,
            )

    # -------------------------------------------------------------
    # 3. VIDEO PIPELINE (Requirement 4 & 5)
    # -------------------------------------------------------------
    format_spec, postprocessors, merge_format = _build_format_options(quality, platform_info, has_ffmpeg)
    outtmpl_pattern = str(temp_dir / "%(title).60s [%(id)s].%(ext)s")
    logger.info(f"selected format: {format_spec}")
    logger.info(f"final output path: {temp_dir}")

    last_update_time = [0.0]

    def _progress_hook(d: dict) -> None:
        if SHUTDOWN_REQUESTED or (req_id and req_id in CANCELLED_REQUESTS):
            raise yt_dlp.utils.DownloadCancelled("Download cancelled by user or shutdown")

        if d.get("status") == "downloading" and progress_callback:
            now = time.time()
            if now - last_update_time[0] >= PROGRESS_UPDATE_INTERVAL_SECONDS:
                last_update_time[0] = now
                percent = d.get("_percent_str", "0%").strip()
                speed = d.get("_speed_str", "N/A").strip()
                eta = d.get("_eta_str", "N/A").strip()
                try:
                    progress_callback(percent, speed, eta)
                except Exception:
                    pass

    ffmpeg_processing_time = [0.0]
    pp_start_time = [0.0]

    def _pp_hook(d: dict) -> None:
        if SHUTDOWN_REQUESTED or (req_id and req_id in CANCELLED_REQUESTS):
            raise yt_dlp.utils.DownloadCancelled("Download cancelled by user or shutdown")

        if d.get("status") == "started":
            pp_start_time[0] = time.time()
            if progress_callback:
                try:
                    progress_callback("100%", "merging", "0s")
                except Exception:
                    pass
        elif d.get("status") == "finished":
            if pp_start_time[0] > 0:
                ffmpeg_processing_time[0] += time.time() - pp_start_time[0]

    ydl_opts = {
        "format": format_spec,
        "outtmpl": outtmpl_pattern,
        "max_filesize": MAX_FILE_SIZE_BYTES,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": False,
        "restrictfilenames": True,
        "socket_timeout": 12,
        "retries": 3,
        "fragment_retries": 3,
        "extract_flat": False,
        "concurrent_fragment_downloads": 8,
        "buffersize": 1024 * 1024,
        "http_chunk_size": 10485760,
        "http_headers": DEFAULT_HTTP_HEADERS,
        "extractor_args": {
            "youtube": {
                "player_client": ["android", "ios"],
                "player_skip": ["configs", "webpage"],
            }
        },
        "progress_hooks": [_progress_hook],
        "postprocessor_hooks": [_pp_hook],
    }

    if NODE_PATH:
        ydl_opts["js_runtimes"] = {"node": {"path": NODE_PATH}}

    if merge_format:
        ydl_opts["merge_output_format"] = merge_format
        # Lossless video copy (-c:v copy) + standard AAC audio (-c:a aac) for universal Telegram sound compatibility
        ydl_opts["postprocessor_args"] = {"merger": ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"]}

    if postprocessors:
        ydl_opts["postprocessors"] = postprocessors

    if ffmpeg_bin:
        ydl_opts["ffmpeg_location"] = str(Path(ffmpeg_bin).parent)

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = None
            if cached_info:
                try:
                    cleaned_cached = copy.deepcopy(cached_info)
                    for k in (
                        "requested_formats",
                        "requested_subtitles",
                        "format",
                        "format_id",
                        "format_note",
                        "is_carousel",
                        "carousel_items",
                        "media_type",
                    ):
                        cleaned_cached.pop(k, None)
                    info_dict = ydl.process_ie_result(cleaned_cached, download=True)
                except Exception as proc_err:
                    logger.warning(f"process_ie_result fallback to extract_info: {proc_err}")
                    info_dict = None

            if not info_dict:
                info_dict = ydl.extract_info(url, download=True)

            if not info_dict and platform_info.key == "instagram":
                info_dict = _extract_instagram_embed_fallback(url)

            if not info_dict:
                record_download_finish(success=False)
                return DownloadResult(
                    success=False,
                    error_code="DOWNLOAD_FAILED",
                    error_message="⚠️ Empty Media: Platform returned no downloadable media for this link.",
                    temp_dir=temp_dir,
                )

            if "entries" in info_dict and len(info_dict["entries"]) > 1:
                logger.info(f"Multi-item carousel detected ({len(info_dict['entries'])} items), routing to carousel pipeline...")
                carousel_items = []
                for idx, entry in enumerate(info_dict["entries"]):
                    if not entry:
                        continue
                    formats = entry.get("formats") or []
                    item_is_video = bool(formats and any(_is_video_format(f) for f in formats))
                    item_type = "video" if item_is_video else "photo"
                    item_url = None
                    if item_is_video:
                        if formats:
                            item_url = formats[-1].get("url")
                        if not item_url:
                            item_url = entry.get("webpage_url") or entry.get("url") or url
                    else:
                        if entry.get("thumbnails"):
                            item_url = entry["thumbnails"][-1].get("url")
                        elif entry.get("url"):
                            item_url = entry.get("url")
                    carousel_items.append({
                        "index": idx,
                        "id": entry.get("id", f"item_{idx}"),
                        "type": item_type,
                        "url": item_url,
                        "raw_entry": entry,
                    })
                info_dict["is_carousel"] = True
                info_dict["carousel_items"] = carousel_items
                info_dict["media_type"] = "carousel"
                return _sync_download(
                    url=url,
                    user_id=user_id,
                    quality=quality,
                    req_id=req_id,
                    progress_callback=progress_callback,
                    cached_info=info_dict,
                )

            if "entries" in info_dict and info_dict["entries"]:
                video_entry = next(
                    (e for e in info_dict["entries"] if e and (e.get("vcodec") != "none" or e.get("formats"))),
                    info_dict["entries"][0],
                )
                info_dict = video_entry

            media_info = MediaInfo(
                title=info_dict.get("title", "Media"),
                uploader=info_dict.get("uploader") or info_dict.get("channel") or "Unknown Creator",
                duration=info_dict.get("duration"),
                thumbnail=info_dict.get("thumbnail"),
                filesize_approx=info_dict.get("filesize") or info_dict.get("filesize_approx"),
                width=info_dict.get("width"),
                height=info_dict.get("height"),
                webpage_url=info_dict.get("webpage_url", url),
                platform=platform_info,
                media_type="video",
            )

            downloaded_files = [
                f for f in temp_dir.iterdir()
                if f.is_file() and not f.name.endswith((".part", ".ytdl"))
            ]

            if not downloaded_files:
                logger.info(f"No video files generated for {url}, routing to photo/carousel fallback...")
                p_success, p_info, _ = _sync_extract_preview(url)
                if p_success and p_info:
                    if p_info.is_carousel and len(p_info.carousel_items) > 1:
                        return _sync_download(
                            url=url,
                            user_id=user_id,
                            quality=quality,
                            req_id=req_id,
                            progress_callback=progress_callback,
                            cached_info=p_info.raw_info,
                            extraction_time_s=p_info.extraction_time_s,
                        )
                    else:
                        return _sync_download(
                            url=url,
                            user_id=user_id,
                            quality="photo",
                            req_id=req_id,
                            progress_callback=progress_callback,
                            cached_info=p_info.raw_info,
                            extraction_time_s=p_info.extraction_time_s,
                        )
                record_download_finish(success=False)
                logger.error(f"Download finished but no output file in {temp_dir}")
                return DownloadResult(
                    success=False,
                    media_info=media_info,
                    error_code="DOWNLOAD_FAILED",
                    error_message="❌ Download finished but no file was generated.",
                    temp_dir=temp_dir,
                )

            downloaded_file = downloaded_files[0]
            file_size = downloaded_file.stat().st_size
            exists = downloaded_file.exists()
            ext = downloaded_file.suffix.lower()

            logger.info(f"file exists: {exists}")
            logger.info(f"file size: {file_size}")
            logger.info(f"correct media extension: {ext in ('.mp4', '.mkv', '.webm', '.mov', '.m4a', '.mp3')}")

            # Enforce Telegram file-size limit
            if file_size > MAX_FILE_SIZE_BYTES:
                record_download_finish(success=False)
                limit_mb = MAX_FILE_SIZE_BYTES // (1024 * 1024)
                actual_mb = file_size / (1024 * 1024)
                return DownloadResult(
                    success=False,
                    media_info=media_info,
                    error_code="FILE_TOO_LARGE",
                    error_message=(
                        f"⚠️ Media exceeds Telegram's upload limit "
                        f"({actual_mb:.1f} MB > {limit_mb} MB).\n"
                        f"Try selecting a lower resolution (e.g. 720p/480p)."
                    ),
                    temp_dir=temp_dir,
                )

            is_audio_file = ext in (
                ".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".flac"
            )

            record_download_finish(success=True)

            t_download_elapsed = max(0.0, (time.time() - t_start) - ffmpeg_processing_time[0])
            timings = DownloadTimings(
                format_extraction_s=extraction_time_s,
                download_s=t_download_elapsed,
                ffmpeg_processing_s=ffmpeg_processing_time[0],
            )

            return DownloadResult(
                success=True,
                file_path=downloaded_file,
                media_info=media_info,
                temp_dir=temp_dir,
                is_audio=is_audio_file,
                timings=timings,
            )

    except yt_dlp.utils.DownloadCancelled:
        record_download_finish(success=False)
        return DownloadResult(
            success=False,
            error_code="CANCELLED",
            error_message="❌ Download cancelled.",
            temp_dir=temp_dir,
        )

    except yt_dlp.utils.DownloadError as e:
        record_download_finish(success=False)
        logger.error(f"DownloadError during media download for {url}: {e}", exc_info=True)
        err_str = str(e).lower()
        if "there is no video in this post" in err_str or "no video formats found" in err_str or "no video in this post" in err_str:
            logger.info(f"No video format found for {url}, routing to photo/carousel fallback pipeline...")
            cached_ie = getattr(ydl, "_last_ie_result", None)
            if cached_ie:
                if "entries" in cached_ie and len(cached_ie["entries"]) > 1:
                    carousel_items = []
                    for idx, entry in enumerate(cached_ie["entries"]):
                        if not entry:
                            continue
                        formats = entry.get("formats") or []
                        item_is_video = bool(formats and any(_is_video_format(f) for f in formats))
                        item_type = "video" if item_is_video else "photo"
                        item_url = None
                        if item_is_video:
                            if formats:
                                item_url = formats[-1].get("url")
                            if not item_url:
                                item_url = entry.get("webpage_url") or entry.get("url") or url
                        else:
                            if entry.get("thumbnails"):
                                item_url = entry["thumbnails"][-1].get("url")
                            elif entry.get("url"):
                                item_url = entry.get("url")
                        carousel_items.append({
                            "index": idx,
                            "id": entry.get("id", f"item_{idx}"),
                            "type": item_type,
                            "url": item_url,
                            "raw_entry": entry,
                        })
                    cached_ie["is_carousel"] = True
                    cached_ie["carousel_items"] = carousel_items
                    cached_ie["media_type"] = "carousel"
                    return _sync_download(
                        url=url,
                        user_id=user_id,
                        quality=quality,
                        req_id=req_id,
                        progress_callback=progress_callback,
                        cached_info=cached_ie,
                    )
                else:
                    cached_ie["media_type"] = "photo"
                    cached_ie["is_carousel"] = False
                    return _sync_download(
                        url=url,
                        user_id=user_id,
                        quality="photo",
                        req_id=req_id,
                        progress_callback=progress_callback,
                        cached_info=cached_ie,
                    )

            p_success, p_info, _ = _sync_extract_preview(url)
            if p_success and p_info:
                if p_info.is_carousel and len(p_info.carousel_items) > 1:
                    return _sync_download(
                        url=url,
                        user_id=user_id,
                        quality=quality,
                        req_id=req_id,
                        progress_callback=progress_callback,
                        cached_info=p_info.raw_info,
                        extraction_time_s=p_info.extraction_time_s,
                    )
                else:
                    return _sync_download(
                        url=url,
                        user_id=user_id,
                        quality="photo",
                        req_id=req_id,
                        progress_callback=progress_callback,
                        cached_info=p_info.raw_info,
                        extraction_time_s=p_info.extraction_time_s,
                    )

        # Instagram embed fallback if download failed with empty media or unable to extract
        if platform_info.key == "instagram" and any(k in err_str for k in ("empty media response", "unable to extract", "no video formats")):
            try:
                fb_info = _extract_instagram_embed_fallback(url)
                if fb_info:
                    return _sync_download(
                        url=url,
                        user_id=user_id,
                        quality="photo" if fb_info.get("media_type") == "photo" else quality,
                        req_id=req_id,
                        progress_callback=progress_callback,
                        cached_info=fb_info,
                    )
            except Exception as fb_err:
                msg = format_extraction_error(fb_err, url)
                return DownloadResult(
                    success=False,
                    error_code="DOWNLOAD_ERROR",
                    error_message=msg,
                    temp_dir=temp_dir,
                )

        msg = format_extraction_error(e, url)
        return DownloadResult(
            success=False,
            error_code="DOWNLOAD_ERROR",
            error_message=msg,
            temp_dir=temp_dir,
        )

    except Exception as e:
        record_download_finish(success=False)
        logger.error(f"Unexpected download error for {url}: {e}", exc_info=True)
        return DownloadResult(
            success=False,
            error_code="UNKNOWN_ERROR",
            error_message=format_extraction_error(e, url),
            temp_dir=temp_dir,
        )
    finally:
        if req_id:
            CANCELLED_REQUESTS.discard(req_id)


async def download_media(
    url: str,
    user_id: int,
    quality: str = QUALITY_BEST,
    req_id: Optional[str] = None,
    progress_callback: Optional[Callable[[str, str, str], None]] = None,
    cached_info: Optional[dict] = None,
    extraction_time_s: float = 0.0,
) -> DownloadResult:
    """Asynchronous wrapper with concurrency semaphore."""
    async with DOWNLOAD_SEMAPHORE:
        res = await asyncio.to_thread(
            _sync_download,
            url=url,
            user_id=user_id,
            quality=quality,
            req_id=req_id,
            progress_callback=progress_callback,
            cached_info=cached_info,
            extraction_time_s=extraction_time_s,
        )
        if not res.success and res.error_code:
            await record_error(
                platform=detect_platform_from_url(url).key,
                url=url,
                error_code=res.error_code,
                error_message=res.error_message or "",
            )
        return res
