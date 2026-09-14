import asyncio
import copy
import hashlib
import html
import json
import logging
import mimetypes
import os
import re
import shutil
import subprocess
import time
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

import yt_dlp

try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    Image = None
    HAS_PIL = False

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
                        res = self._extract_product(product_info, video_id=video_id, get_comments=False)
                        if isinstance(res, dict):
                            res["original_width"] = product_info.get("original_width")
                            res["original_height"] = product_info.get("original_height")
                            res["image_versions2"] = product_info.get("image_versions2")
                            res["display_uri"] = product_info.get("display_uri")
                            cm = product_info.get("carousel_media") or []
                            entries = res.get("entries") or []
                            if cm and entries:
                                for idx, entry in enumerate(entries):
                                    if idx < len(cm) and isinstance(entry, dict) and isinstance(cm[idx], dict):
                                        entry["original_width"] = cm[idx].get("original_width")
                                        entry["original_height"] = cm[idx].get("original_height")
                                        entry["image_versions2"] = cm[idx].get("image_versions2")
                                        entry["display_uri"] = cm[idx].get("display_uri")
                        return res
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
    if not isinstance(f, dict):
        return False
    acodec = f.get("acodec")
    if acodec == "none":
        return False
    if acodec:
        return True
    audio_ext = (f.get("audio_ext") or "").lower()
    if audio_ext == "none" and f.get("vcodec") and f.get("vcodec") != "none":
        return False
    if audio_ext and audio_ext != "none":
        return True
    ext = (f.get("ext") or "").lower()
    if ext in ("m4a", "mp3", "opus", "aac", "ogg", "wav", "flac"):
        return True
    if f.get("format_id") in ("sd", "hd"):
        return True
    # Progressive mp4/webm/mkv streams without explicit codecs (e.g. IG formats 1, 2, 3)
    if ext in ("mp4", "webm", "mkv") and f.get("vcodec") not in ("none",) and f.get("acodec") not in ("none",):
        return True
    return False


def inspect_image_dimensions(image_path: Path) -> tuple[Optional[int], Optional[int]]:
    """Inspects downloaded image width and height using PIL (with ffprobe fallback)."""
    if not image_path or not image_path.exists():
        return None, None
    if HAS_PIL and Image is not None:
        try:
            with Image.open(image_path) as img:
                return img.width, img.height
        except Exception as e:
            logger.debug(f"PIL inspection failed for {image_path.name}: {e}")
    probe = probe_media_detailed(image_path)
    return probe.get("width"), probe.get("height")


def select_best_photo_candidate(info_dict: dict, fallback_url: Optional[str] = None) -> dict:
    """
    Ranks all available photo candidates by actual pixel area (width * height).
    Preserves original aspect ratio and prefers uncropped master originals over
    cropped or downscaled variants.

    Returns:
    {
        "selected_url": str,
        "width": Optional[int],
        "height": Optional[int],
        "aspect_ratio": Optional[float],
        "source_candidates": int,
    }
    """
    raw_candidates: list[Any] = []

    # 1. Gather all candidates from various metadata keys
    if isinstance(info_dict, dict):
        # image_versions2 (Instagram direct Polaris candidates)
        iv2 = info_dict.get("image_versions2")
        if isinstance(iv2, dict):
            c_list = iv2.get("candidates") or []
            if isinstance(c_list, list):
                raw_candidates.extend(c_list)

        # thumbnails list
        thumbs = info_dict.get("thumbnails")
        if isinstance(thumbs, list):
            raw_candidates.extend(thumbs)

        # single field candidates
        for k in ("display_uri", "display_url", "url", "thumbnail"):
            val = info_dict.get(k)
            if val and isinstance(val, str) and val.startswith(("http://", "https://")):
                raw_candidates.append({"url": val})

    if fallback_url and isinstance(fallback_url, str) and fallback_url.startswith(("http://", "https://")):
        raw_candidates.append({"url": fallback_url})

    # Reference original dimensions if known
    orig_w = None
    orig_h = None
    if isinstance(info_dict, dict):
        orig_w = info_dict.get("original_width") or info_dict.get("width")
        orig_h = info_dict.get("original_height") or info_dict.get("height")
    try:
        orig_w = int(orig_w) if orig_w else None
        orig_h = int(orig_h) if orig_h else None
    except (ValueError, TypeError):
        orig_w, orig_h = None, None

    # Pre-scan candidate URLs to infer original dimensions and aspect ratio if missing
    max_scale_bound = 0
    if not (orig_w and orig_h):
        for c in raw_candidates:
            c_u = c.get("url") if isinstance(c, dict) else (c if isinstance(c, str) else "")
            if not c_u:
                continue
            parsed_u = urllib.parse.urlparse(c_u)
            stp_u = urllib.parse.parse_qs(parsed_u.query).get("stp", [""])[0]
            c_match = re.search(r"c(\d+)\.(\d+)\.(\d+)\.(\d+)", stp_u)
            if c_match:
                cx, cy, cw, ch = int(c_match.group(1)), int(c_match.group(2)), int(c_match.group(3)), int(c_match.group(4))
                if cx == 0 and cy > 0:
                    orig_w = cw
                    orig_h = cy * 2 + ch
                    break
                elif cy == 0 and cx > 0:
                    orig_w = cx * 2 + cw
                    orig_h = ch
                    break
                elif cx == 0 and cy == 0 and cw > 0 and ch > 0:
                    orig_w = cw
                    orig_h = ch
                    break
            s_match = re.search(r"[sp](\d+)x(\d+)", stp_u)
            if s_match:
                max_scale_bound = max(max_scale_bound, int(s_match.group(1)), int(s_match.group(2)))

    orig_aspect = (orig_w / orig_h) if (orig_w and orig_h and orig_h > 0) else None

    evaluated: list[dict] = []
    seen_urls: set[str] = set()

    for c in raw_candidates:
        if isinstance(c, str):
            c_url = c
            meta_w, meta_h = None, None
        elif isinstance(c, dict):
            c_url = c.get("url")
            meta_w = c.get("width")
            meta_h = c.get("height")
        else:
            continue

        if not c_url or not isinstance(c_url, str) or not c_url.startswith(("http://", "https://")):
            continue

        # Ignore video streams
        if any(c_url.lower().endswith(vext) for vext in (".mp4", ".mkv", ".webm", ".mov", ".m4a", ".mp3")):
            continue

        # Strip fragment
        c_url_clean = c_url.split("#")[0]
        if c_url_clean in seen_urls:
            continue
        seen_urls.add(c_url_clean)

        parsed = urllib.parse.urlparse(c_url_clean)
        qs = urllib.parse.parse_qs(parsed.query)
        stp = qs.get("stp", [""])[0]

        # Detect crop (e.g. c0.240.1440.1440a)
        is_cropped = bool(re.search(r"c\d+\.\d+\.\d+\.\d+", stp))

        # Detect scale (e.g. s640x640, p1080x1080)
        scale_m = re.search(r"[sp](\d+)x(\d+)", stp)

        # Master original check: uncropped, no scale modifier
        is_master = ("dst-jpg" in stp or "stp=" not in c_url_clean) and not is_cropped and not scale_m

        # Determine dimensions
        w, h = None, None
        if meta_w and meta_h:
            try:
                mw, mh = int(meta_w), int(meta_h)
                if mw > 0 and mh > 0:
                    w, h = mw, mh
            except (ValueError, TypeError):
                pass

        if w is None or h is None:
            if is_master:
                if orig_w and orig_h:
                    w, h = orig_w, orig_h
                elif max_scale_bound > 0:
                    w, h = max(max_scale_bound, 1080), max(max_scale_bound, 1080)
                else:
                    w, h = 1080, 1080
            elif scale_m:
                bound_w = int(scale_m.group(1))
                bound_h = int(scale_m.group(2))
                if is_cropped:
                    w, h = bound_w, bound_h
                elif orig_aspect:
                    if orig_aspect >= 1.0:
                        w = bound_w
                        h = max(1, int(bound_w / orig_aspect))
                    else:
                        h = bound_h
                        w = max(1, int(bound_h * orig_aspect))
                else:
                    w, h = bound_w, bound_h
            elif is_cropped:
                c_m = re.search(r"c\d+\.\d+\.(\d+)\.(\d+)", stp)
                if c_m:
                    w, h = int(c_m.group(1)), int(c_m.group(2))
                else:
                    w, h = 150, 150
            elif orig_w and orig_h:
                w, h = orig_w, orig_h
            else:
                w, h = 0, 0

        area = (w or 0) * (h or 0)
        is_direct_cdn = any(cdn in c_url_clean for cdn in ("fbcdn.net", "cdninstagram.com", "instagram.f"))
        ar = round(w / h, 4) if (w and h and h > 0) else None

        # Score tuple:
        # 1. Uncropped preferred over cropped (preserves original aspect ratio)
        # 2. Highest actual pixel area (width * height)
        # 3. Master original preferred over scaled derivatives
        # 4. Direct CDN URL preferred over proxy
        score = (
            1 if not is_cropped else 0,
            area,
            1 if is_master else 0,
            1 if is_direct_cdn else 0,
        )

        evaluated.append({
            "url": c_url_clean,
            "width": w if (w and w > 0) else None,
            "height": h if (h and h > 0) else None,
            "aspect_ratio": ar,
            "is_cropped": is_cropped,
            "is_master": is_master,
            "area": area,
            "score": score,
        })

    if not evaluated:
        return {
            "selected_url": fallback_url or "",
            "width": None,
            "height": None,
            "aspect_ratio": None,
            "source_candidates": 0,
        }

    evaluated.sort(key=lambda x: x["score"], reverse=True)
    best = evaluated[0]

    return {
        "selected_url": best["url"],
        "width": best["width"],
        "height": best["height"],
        "aspect_ratio": best["aspect_ratio"],
        "source_candidates": len(evaluated),
    }


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
    # Guarantee bestvideo+bestaudio is selected whenever separate streams exist,
    # and require progressive streams with audio before falling back to video-only
    if plat_key in ("instagram", "facebook"):
        if has_ffmpeg:
            merge_output_format = "mp4"
            format_spec = "bestvideo+bestaudio/best[vcodec!=none][acodec!=none]/best[acodec!=none]/best"
        else:
            format_spec = "best[vcodec!=none][acodec!=none]/best[acodec!=none]/best"
        return format_spec, postprocessors, merge_output_format

    # YouTube and generic platforms for 'highest' / 'best':
    # Cap at 1080p and guarantee audio stream is always included and merged
    if quality in (QUALITY_HIGHEST, QUALITY_BEST):
        if has_ffmpeg:
            merge_output_format = "mp4"
            format_spec = (
                "bestvideo[height<=1080]+bestaudio/"
                "bestvideo+bestaudio/"
                "22/18/"
                "best[height<=1080][vcodec!=none][acodec!=none]/"
                "best[vcodec!=none][acodec!=none]/"
                "best[acodec!=none]/"
                "best"
            )
        else:
            format_spec = (
                "22/18/"
                "best[height<=1080][vcodec!=none][acodec!=none]/"
                "best[vcodec!=none][acodec!=none]/"
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
                f"bestvideo[height<={h_val}][height>{h_min}]+bestaudio/"
                f"bestvideo[width<={h_val}][width>{h_min}]+bestaudio/"
                f"bestvideo[height<={h_val}]+bestaudio/"
                f"bestvideo+bestaudio/"
                f"best[height<={h_val}][height>{h_min}][vcodec!=none][acodec!=none]/"
                f"best[width<={h_val}][width>{h_min}][vcodec!=none][acodec!=none]/"
                f"best[height<={h_val}][vcodec!=none][acodec!=none]/"
                f"best[vcodec!=none][acodec!=none]/"
                "best"
            )
        else:
            format_spec = (
                "22/18/"
                f"best[height<={h_val}][height>{h_min}][vcodec!=none][acodec!=none]/"
                f"best[width<={h_val}][width>{h_min}][vcodec!=none][acodec!=none]/"
                f"best[height<={h_val}][vcodec!=none][acodec!=none]/"
                f"best[vcodec!=none][acodec!=none]/"
                "best"
            )
    else:
        # For 1080p+ separate streams are necessary to achieve 1080p/2K/4K resolution
        if has_ffmpeg:
            merge_output_format = "mp4"
            format_spec = (
                f"bestvideo[height<={h_val}][height>{h_min}]+bestaudio/"
                f"bestvideo[width<={h_val}][width>{h_min}]+bestaudio/"
                f"bestvideo[height<={h_val}]+bestaudio/"
                f"bestvideo+bestaudio/"
                f"best[height<={h_val}][height>{h_min}][vcodec!=none][acodec!=none]/"
                f"best[width<={h_val}][width>{h_min}][vcodec!=none][acodec!=none]/"
                f"best[height<={h_val}][vcodec!=none][acodec!=none]/"
                f"best[vcodec!=none][acodec!=none]/"
                "best"
            )
        else:
            format_spec = (
                f"best[height<={h_val}][height>{h_min}][vcodec!=none][acodec!=none]/"
                f"best[width<={h_val}][width>{h_min}][vcodec!=none][acodec!=none]/"
                f"best[height<={h_val}][vcodec!=none][acodec!=none]/"
                f"best[vcodec!=none][acodec!=none]/"
                "best"
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
    without adding subjective visual-content/category judgment or generic placeholders.
    """
    err_str = str(error).strip()
    err_lower = err_str.lower()

    # 1. Access / Audience Restrictions from source (Do not bypass, do not add visual category labels)
    if any(k in err_lower for k in ("can't be seen by certain audiences", "not available to everyone", "certain audiences")):
        return "🔒 Instagram: This content isn't available to everyone: It can't be seen by certain audiences."

    # 2. Private Account (Do not bypass)
    if any(k in err_lower for k in ("this account is private", "private account", "only approved followers")):
        return "🔒 Private Account: This Instagram account is private. Content is only visible to approved followers."

    # 3. Login Requirement (Do not bypass)
    if any(k in err_lower for k in ("login required", "please log in", "checkpoint_required", "redirected to login", "sign in to view")):
        return "🔒 Login Required: Instagram requires authentication to access this content."

    # 4. Age Restriction (Only when explicitly reported by source)
    if any(k in err_lower for k in ("age-restricted", "age restricted", "viewer discretion", "must be 18")):
        return "🔞 Age Restricted: Instagram reports this post has an age verification restriction."

    # 5. Deleted / Not Found Content (Only when explicitly reported as deleted/404)
    if any(k in err_lower for k in ("does not exist", "post not found", "has been deleted", "deleted by the owner", "404 not found", "http error 404")):
        return "🚫 Post Unavailable: This post does not exist or has been deleted by the owner."

    # 6. Geographic / Region Restriction
    if any(k in err_lower for k in ("geo-restricted", "geoblocked", "not available in your region", "not available in your country")):
        return "🌍 Region Restricted: This Instagram media is restricted in the server's geographic location."

    # 7. Rate Limited / Temporary Block
    if any(k in err_lower for k in ("429", "too many requests", "rate limit", "temporarily blocked")):
        return "⏳ Rate Limited: Instagram is temporarily limiting requests. Please wait a minute and retry."

    # 8. Empty Media Response
    if "empty media response" in err_lower:
        return "⚠️ Empty Media: Instagram returned an empty response (post may be restricted or unavailable)."

    # 9. File Size Limit
    if "too large" in err_lower or "max_filesize" in err_lower:
        return "⚠️ File Size Limit: Media exceeds Telegram's file size limit (50 MB)."

    # 10. Clean and expose underlying technical error directly from platform
    clean_msg = re.sub(r"^ERROR:\s*\[[^\]]+\]\s*[^:]*:\s*", "", err_str, flags=re.IGNORECASE).strip()
    if clean_msg:
        first_line = clean_msg.split("\n")[0].strip()
        if len(first_line) > 140:
            first_line = first_line[:137] + "…"
        return f"❌ Source Error: {first_line}"

    return "❌ Media extraction failed. Please check if the link is accessible."


def categorize_error(error: Any) -> tuple[str, str]:
    """
    Distinguishes error causes into distinct technical categories:
    - extractor failure
    - no usable formats
    - source unavailable/restricted
    - rate limiting
    - network error
    - Telegram upload error
    Returns: (category_name, error_code)
    """
    err_str = str(error).strip()
    err_lower = err_str.lower()

    if any(k in err_lower for k in ("telegram", "send_media_group", "reply_video", "reply_photo", "answer_media_group", "upload to telegram")):
        return "Telegram upload error", "TELEGRAM_UPLOAD_ERROR"

    if any(k in err_lower for k in ("429", "too many requests", "rate limit", "temporarily blocked", "exceeded the rate-limit")):
        return "rate limiting", "RATE_LIMITING"

    if any(k in err_lower for k in (
        "private account", "this account is private", "only approved followers",
        "login required", "please log in", "checkpoint_required", "redirected to login",
        "age-restricted", "sensitive content", "viewer discretion", "must be 18",
        "not found", "deleted", "unavailable", "404", "content isn't available",
        "can't be seen by certain audiences", "not available to everyone", "certain audiences",
        "geo-restricted", "geoblocked", "not available in your region"
    )):
        return "source unavailable/restricted", "SOURCE_RESTRICTED"

    if any(k in err_lower for k in (
        "connection", "timed out", "timeout", "socket", "ssl", "dns",
        "name resolution", "temporary failure in name", "network is unreachable", "remote host"
    )):
        return "network error", "NETWORK_ERROR"

    if any(k in err_lower for k in (
        "no video formats found", "requested format is not available",
        "no usable format", "there is no video in this post", "no video in this post", "there is no video"
    )):
        return "no usable formats", "NO_USABLE_FORMATS"

    return "extractor failure", "EXTRACTOR_FAILURE"


def probe_media_streams(file_path: Optional[Path]) -> tuple[bool, bool]:
    """
    Probes file using ffprobe or ffmpeg to verify whether video and audio streams exist.
    Returns: (has_video_stream, has_audio_stream)
    """
    info = probe_media_detailed(file_path)
    return info["has_video"], info["has_audio"]


def probe_media_detailed(file_path: Optional[Path]) -> dict:
    """
    Probes file using ffprobe to extract codec names, container format, and stream presence.
    Returns:
    {
        "has_video": bool,
        "has_audio": bool,
        "video_codec": Optional[str],  # normalized lowercase e.g. "h264", "vp9", "av1"
        "audio_codec": Optional[str],  # normalized lowercase e.g. "aac", "opus", "mp3"
        "container": Optional[str],    # e.g. "mov,mp4,m4a,3gp,3g2,mj2" or "mp4"
        "width": Optional[int],
        "height": Optional[int],
        "is_ios_compatible": bool,
    }
    """
    info = {
        "has_video": False,
        "has_audio": False,
        "video_codec": None,
        "audio_codec": None,
        "container": None,
        "width": None,
        "height": None,
        "is_ios_compatible": False,
    }
    if not file_path or not file_path.exists() or file_path.stat().st_size == 0:
        return info

    ffmpeg_bin = resolve_ffmpeg_path()
    ffprobe_bin = None
    if ffmpeg_bin:
        ffprobe_cand = Path(ffmpeg_bin).parent / ("ffprobe.exe" if os.name == "nt" else "ffprobe")
        if ffprobe_cand.exists():
            ffprobe_bin = str(ffprobe_cand)
        else:
            ffprobe_which = shutil.which("ffprobe")
            if ffprobe_which:
                ffprobe_bin = ffprobe_which

    if ffprobe_bin:
        try:
            cmd = [
                ffprobe_bin,
                "-v", "error",
                "-show_entries", "stream=index,codec_type,codec_name,width,height:format=format_name",
                "-of", "json",
                str(file_path),
            ]
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if res.returncode == 0 and res.stdout.strip():
                data = json.loads(res.stdout)
                streams = data.get("streams", [])
                fmt = data.get("format", {})
                info["container"] = fmt.get("format_name") or file_path.suffix.lstrip(".").lower()

                for s in streams:
                    ctype = s.get("codec_type")
                    cname = (s.get("codec_name") or "").lower()
                    if ctype == "video" and not info["has_video"]:
                        info["has_video"] = True
                        info["video_codec"] = cname
                        info["width"] = s.get("width")
                        info["height"] = s.get("height")
                        info["pix_fmt"] = s.get("pix_fmt")
                    elif ctype == "audio" and not info["has_audio"]:
                        info["has_audio"] = True
                        info["audio_codec"] = cname
                        info["channels"] = s.get("channels")
        except Exception as probe_err:
            logger.debug(f"ffprobe detailed check failed for {file_path}: {probe_err}")

    # Fallback to basic ffmpeg probing if ffprobe was unavailable or failed
    if not info["has_video"] and not info["has_audio"] and ffmpeg_bin and Path(ffmpeg_bin).exists():
        try:
            cmd = [ffmpeg_bin, "-i", str(file_path)]
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            output = res.stderr.lower()
            info["has_video"] = "video:" in output or ("stream #0:" in output and "video" in output)
            info["has_audio"] = "audio:" in output or ("stream #0:" in output and "audio" in output)
            if "h264" in output:
                info["video_codec"] = "h264"
            elif "vp9" in output:
                info["video_codec"] = "vp9"
            elif "av1" in output:
                info["video_codec"] = "av1"
            if "aac" in output:
                info["audio_codec"] = "aac"
            elif "opus" in output:
                info["audio_codec"] = "opus"
        except Exception:
            pass

    if not info["container"]:
        info["container"] = file_path.suffix.lstrip(".").lower()

    info["is_ios_compatible"] = is_ios_compatible(info)
    return info


def is_ios_compatible(probe_info: dict) -> bool:
    """
    Evaluates iOS compatibility:
    1. Container must be MP4 (or mov,mp4 container format)
    2. Video codec must be h264 / avc1 (NEVER vp9 or av1) and pixel format yuv420p (never 10-bit or yuv444p)
    3. Audio codec (if present) must be aac and stereo/mono (<= 2 channels)
    """
    if not probe_info:
        return False
    v_ok = bool(probe_info.get("has_video") and probe_info.get("video_codec") in ("h264", "avc1") and probe_info.get("pix_fmt") in ("yuv420p", None))
    a_ok = bool(not probe_info.get("has_audio") or (probe_info.get("audio_codec") == "aac" and (probe_info.get("channels") is None or probe_info.get("channels") <= 2)))
    cont = str(probe_info.get("container") or "")
    c_ok = bool("mp4" in cont.lower())
    return bool(v_ok and a_ok and c_ok)


def normalize_video_for_ios(
    file_path: Path,
    source_has_audio: bool = True,
    temp_dir: Optional[Path] = None,
    audio_path: Optional[Path] = None,
) -> tuple[Path, dict]:
    """
    Normalizes any video file to an iOS-compatible MP4 container (H.264 + AAC) with faststart.
    - If already iOS-compatible (H.264 + AAC MP4) and no separate audio needed: stream-copy / remux with faststart.
    - If video is VP9/AV1/etc. or audio is not AAC: transcode via FFmpeg (libx264 + aac).
    - When separate streams: -map 0:v:0 -map 1:a:0.
    - When single input has both streams: -map 0:v:0 -map 0:a:0.
    - When source has no audio: -map 0:v:0.
    Returns: (final_file_path, probe_info)
    """
    if not file_path or not file_path.exists():
        return file_path, {}

    ffmpeg_bin = resolve_ffmpeg_path()
    if not ffmpeg_bin:
        logger.warning(f"FFmpeg not available, cannot normalize {file_path.name} for iOS")
        return file_path, probe_media_detailed(file_path)

    probe = probe_media_detailed(file_path)
    has_input_audio = bool(probe.get("has_audio"))
    has_separate_audio = bool(audio_path and audio_path.exists() and audio_path.stat().st_size > 0)
    needs_audio = bool(source_has_audio and (has_input_audio or has_separate_audio))

    logger.info(
        f"[iOS COMPATIBILITY CHECK] input={file_path.name}, vcodec={probe.get('video_codec')}, "
        f"acodec={probe.get('audio_codec')}, container={probe.get('container')}, "
        f"is_ios_compatible={probe.get('is_ios_compatible')}, separate_audio={has_separate_audio}"
    )

    # Determine destination directory and target output path
    target_dir = temp_dir or file_path.parent
    norm_file = target_dir / f"{file_path.stem}_ios.mp4"

    # Prevent duplicate normalization if file is already an iOS-normalized file
    if not has_separate_audio and file_path.name.endswith("_ios.mp4") and probe.get("is_ios_compatible"):
        logger.info(f"File {file_path.name} is already normalized for iOS, skipping duplicate normalization.")
        return file_path, probe

    # Case 1: Already H.264 + AAC MP4 and no separate audio merge required -> Stream copy / remux with faststart
    if not has_separate_audio and probe.get("is_ios_compatible") and file_path.suffix.lower() == ".mp4":
        logger.info(f"File {file_path.name} is already iOS-compatible (H.264 + AAC MP4). Remuxing with faststart...")
        cmd = [
            ffmpeg_bin, "-y",
            "-i", str(file_path),
            "-map", "0:v:0",
        ]
        if has_input_audio:
            cmd.extend(["-map", "0:a:0"])
        cmd.extend(["-c", "copy", "-movflags", "+faststart", str(norm_file)])

        remux_res = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if remux_res.returncode == 0 and norm_file.exists() and norm_file.stat().st_size > 0:
            final_probe = probe_media_detailed(norm_file)
            if not needs_audio or final_probe.get("has_audio"):
                return norm_file, final_probe
            logger.warning(f"Fast remux lost audio ({remux_res.stderr}), falling back to transcode")
        else:
            logger.warning(f"Fast remux failed ({remux_res.stderr}), proceeding to transcode")

    # Case 2: Transcode / Merge incompatible video or separate audio streams
    logger.info(f"Normalizing video {file_path.name} (codec={probe.get('video_codec')}) to H.264 + AAC MP4 for iOS...")
    cmd = [
        ffmpeg_bin, "-y",
        "-i", str(file_path),
    ]

    if has_separate_audio:
        cmd.extend(["-i", str(audio_path)])
        cmd.extend(["-map", "0:v:0", "-map", "1:a:0"])
    else:
        cmd.extend(["-map", "0:v:0"])
        if has_input_audio:
            cmd.extend(["-map", "0:a:0"])

    # Video: stream copy ONLY if already h264/avc1 with standard 8-bit yuv420p in mp4, else libx264 with yuv420p
    if not has_separate_audio and probe.get("video_codec") in ("h264", "avc1") and probe.get("pix_fmt") == "yuv420p" and file_path.suffix.lower() == ".mp4":
        cmd.extend(["-c:v", "copy"])
    else:
        cmd.extend([
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "23",
            "-pix_fmt", "yuv420p",
        ])

    # Audio: preserve/copy if already stereo/mono AAC, otherwise encode to AAC stereo at 192k
    if has_separate_audio or has_input_audio:
        a_probe = probe if not has_separate_audio else probe_media_detailed(audio_path)
        a_codec = a_probe.get("audio_codec")
        channels = a_probe.get("channels") or 2
        if a_codec == "aac" and channels <= 2:
            cmd.extend(["-c:a", "copy"])
        else:
            cmd.extend(["-c:a", "aac", "-b:a", "192k", "-ac", "2"])

    cmd.extend(["-movflags", "+faststart", str(norm_file)])

    tc_res = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if tc_res.returncode == 0 and norm_file.exists() and norm_file.stat().st_size > 0:
        final_probe = probe_media_detailed(norm_file)
        if (has_input_audio or has_separate_audio) and not final_probe.get("has_audio"):
            logger.error(f"iOS normalization dropped audio stream for {file_path.name}! Reverting to original file.")
            return file_path, probe
        logger.info(f"Successfully normalized {file_path.name} to iOS MP4 ({norm_file.name}, size={norm_file.stat().st_size})")
        return norm_file, final_probe
    else:
        logger.error(f"FFmpeg iOS normalization failed: {tc_res.stderr}")
        return file_path, probe



def _sync_extract_preview(url: str) -> tuple[bool, Optional[MediaInfo], Optional[str]]:
    """Synchronous fast info extraction without downloading media."""
    t_start = time.time()
    platform_info = detect_platform_from_url(url)
    has_ffmpeg = HAS_FFMPEG

    is_instagram = bool(platform_info.key == "instagram")
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": not is_instagram,  # Allow Instagram multi-item carousel extraction
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

        # Safely convert entries generator/iterable to concrete list to avoid len() crashes
        entries = list(info.get("entries") or [])
        if len(entries) > 1:
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
                    valid_prog = [f for f in formats if f.get("url") and f.get("vcodec") != "none" and f.get("acodec") not in (None, "none")]
                    if valid_prog:
                        item_url = valid_prog[-1].get("url")
                    elif formats:
                        item_url = formats[-1].get("url")
                    if not item_url:
                        item_url = entry.get("webpage_url") or entry.get("url") or url
                else:
                    best_cand = select_best_photo_candidate(entry)
                    item_url = best_cand.get("selected_url") or entry.get("url")

                carousel_items.append({
                    "index": idx,
                    "id": entry.get("id", f"item_{idx}"),
                    "type": item_type,
                    "url": item_url,
                    "width": best_cand.get("width") if item_type == "photo" else entry.get("width"),
                    "height": best_cand.get("height") if item_type == "photo" else entry.get("height"),
                    "aspect_ratio": best_cand.get("aspect_ratio") if item_type == "photo" else None,
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
            if len(entries) == 1:
                info = entries[0]
            primary_entry = info

            formats = info.get("formats") or []
            has_video = any(_is_video_format(f) for f in formats)

            if not has_video:
                media_type = "photo"
                best_cand = select_best_photo_candidate(info)
                photo_url = best_cand.get("selected_url") or info.get("url")
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

        # Structured diagnostic logging for all extracted media
        logger.info(f"canonical URL: {url}")
        logger.info(f"extractor result type: {'playlist' if is_carousel else media_type}")
        logger.info(f"format count: {len(carousel_items) if is_carousel else len(info.get('formats') or [])}")
        logger.info(f"selected format: {max_quality}")
        logger.info(f"whether video stream exists: {'YES' if (has_audio or (not is_carousel and has_video)) else 'NO'}")
        logger.info(f"whether audio stream exists: {'YES' if has_audio else 'NO'}")

        if platform_info.key == "instagram":
            ig_fmt_count = len(carousel_items) if is_carousel else len(info.get('formats') or [])
            logger.info(f"[IG DEBUG] URL={url}")
            logger.info("[IG DEBUG] extractor=instagram")
            logger.info(f"[IG DEBUG] formats={ig_fmt_count}")
            logger.info(f"[IG DEBUG] selected={max_quality}")
            logger.info(f"[IG DEBUG] video_stream={'YES' if (has_audio or (not is_carousel and has_video)) else 'NO'}")
            logger.info(f"[IG DEBUG] audio_stream={'YES' if has_audio else 'NO'}")
            logger.info("[IG DEBUG] download_error=None")
            logger.info(f"[IG DEBUG] ffprobe_video={'YES' if (not is_carousel and has_video) else 'NO'}")
            logger.info(f"[IG DEBUG] ffprobe_audio={'YES' if has_audio else 'NO'}")

        extraction_time_s = time.time() - t_start

        preview_w = None
        preview_h = None
        if is_carousel and carousel_items:
            preview_w = carousel_items[0].get("width") or primary_entry.get("width")
            preview_h = carousel_items[0].get("height") or primary_entry.get("height")
        elif not has_video:
            preview_w = best_cand.get("width") or primary_entry.get("width")
            preview_h = best_cand.get("height") or primary_entry.get("height")
        else:
            preview_w = primary_entry.get("width")
            preview_h = primary_entry.get("height")

        # Store detected fields inside raw_info for cache restoration
        info["media_type"] = media_type
        info["is_carousel"] = is_carousel
        info["carousel_items"] = carousel_items
        info["width"] = preview_w
        info["height"] = preview_h

        media_info = MediaInfo(
            title=info.get("title", "Media"),
            uploader=info.get("uploader") or info.get("channel") or "Unknown Creator",
            duration=info.get("duration"),
            thumbnail=info.get("thumbnail") or (carousel_items[0]["url"] if (carousel_items and carousel_items[0]["url"]) else None),
            filesize_approx=info.get("filesize") or info.get("filesize_approx"),
            width=preview_w,
            height=preview_h,
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
        err_cat, err_code = categorize_error(e)
        logger.error(f"Preview extraction DownloadError for {url}: {e}", exc_info=True)
        logger.error(f"canonical URL: {url}")
        logger.error(f"extractor result type: {platform_info.key}")
        logger.error("format count: 0")
        logger.error("selected format: N/A")
        logger.error("whether video stream exists: NO")
        logger.error("whether audio stream exists: NO")
        logger.error(f"final exception/error: {type(e).__name__}: {e}")
        logger.error(f"error category: {err_cat}")

        if platform_info.key == "instagram":
            logger.info(f"[IG DEBUG] URL={url}")
            logger.info("[IG DEBUG] extractor=instagram")
            logger.info("[IG DEBUG] formats=0")
            logger.info("[IG DEBUG] selected=None")
            logger.info("[IG DEBUG] video_stream=NO")
            logger.info("[IG DEBUG] audio_stream=NO")
            logger.info(f"[IG DEBUG] download_error={e}")
            logger.info("[IG DEBUG] ffprobe_video=NO")
            logger.info("[IG DEBUG] ffprobe_audio=NO")

        return False, None, format_extraction_error(e, url)
    except Exception as e:
        err_cat, err_code = categorize_error(e)
        logger.error(f"Unexpected preview error for {url}: {e}", exc_info=True)
        logger.error(f"canonical URL: {url}")
        logger.error(f"extractor result type: {platform_info.key}")
        logger.error("format count: 0")
        logger.error("selected format: N/A")
        logger.error("whether video stream exists: NO")
        logger.error("whether audio stream exists: NO")
        logger.error(f"final exception/error: {type(e).__name__}: {e}")
        logger.error(f"error category: {err_cat}")

        if platform_info.key == "instagram":
            logger.info(f"[IG DEBUG] URL={url}")
            logger.info("[IG DEBUG] extractor=instagram")
            logger.info("[IG DEBUG] formats=0")
            logger.info("[IG DEBUG] selected=None")
            logger.info("[IG DEBUG] video_stream=NO")
            logger.info("[IG DEBUG] audio_stream=NO")
            logger.info(f"[IG DEBUG] download_error={e}")
            logger.info("[IG DEBUG] ffprobe_video=NO")
            logger.info("[IG DEBUG] ffprobe_audio=NO")

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
        carousel_items = cached_info.get("carousel_items", []) if cached_info else []
        if not carousel_items and cached_info and "entries" in cached_info:
            c_entries = list(cached_info.get("entries") or [])
            if len(c_entries) > 1:
                carousel_items = []
                for idx, entry in enumerate(c_entries):
                    if not entry:
                        continue
                    formats = entry.get("formats") or []
                    item_is_video = bool(formats and any(_is_video_format(f) for f in formats))
                    item_type = "video" if item_is_video else "photo"
                    item_url = None
                    if item_is_video:
                        valid_prog = [f for f in formats if f.get("url") and f.get("vcodec") != "none" and f.get("acodec") not in (None, "none")]
                        if valid_prog:
                            item_url = valid_prog[-1].get("url")
                        elif formats:
                            item_url = formats[-1].get("url")
                        if not item_url:
                            item_url = entry.get("webpage_url") or entry.get("url") or url
                    else:
                        photo_cand = select_best_photo_candidate(entry, fallback_url=entry.get("url"))
                        item_url = photo_cand.get("selected_url") or entry.get("url")
                    carousel_items.append({
                        "index": idx,
                        "id": entry.get("id", f"item_{idx}"),
                        "type": item_type,
                        "url": item_url,
                        "raw_entry": entry,
                    })

        logger.info(f"canonical URL: {url}")
        logger.info("extractor result type: playlist")
        logger.info(f"format count: {len(carousel_items)}")
        logger.info("selected format: highest")
        logger.info(f"Entering carousel download pipeline: {len(carousel_items)} items")

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
                    raw_e = it.get("raw_entry") or {}
                    photo_cand = select_best_photo_candidate(raw_e, fallback_url=iurl)
                    target_url = photo_cand.get("selected_url") or iurl

                    req = urllib.request.Request(target_url, headers=DEFAULT_HTTP_HEADERS)
                    with urllib.request.urlopen(req, timeout=20) as resp:
                        ctype = resp.headers.get("Content-Type", "")
                        ext = ".webp" if "webp" in ctype else (".png" if "png" in ctype else ".jpg")
                        item_file = temp_dir / f"carousel_{idx}_{iid}{ext}"
                        with open(item_file, "wb") as f:
                            f.write(resp.read())
                        item_size = item_file.stat().st_size
                        if item_file.exists() and item_size > 0:
                            pil_w, pil_h = inspect_image_dimensions(item_file)
                            src_w = photo_cand.get("width") or pil_w
                            src_h = photo_cand.get("height") or pil_h
                            dl_w = pil_w or src_w
                            dl_h = pil_h or src_h
                            actual_ar = round(dl_w / dl_h, 4) if (dl_w and dl_h and dl_h > 0) else None
                            c_count = photo_cand.get("source_candidates", 1)

                            # 7 MANDATORY PHOTO DEBUG LOGS FOR CAROUSEL ITEM
                            logger.info(f"[PHOTO DEBUG] selected_url={target_url}")
                            logger.info(f"[PHOTO DEBUG] source_width={src_w}")
                            logger.info(f"[PHOTO DEBUG] source_height={src_h}")
                            logger.info(f"[PHOTO DEBUG] downloaded_width={dl_w}")
                            logger.info(f"[PHOTO DEBUG] downloaded_height={dl_h}")
                            logger.info(f"[PHOTO DEBUG] aspect_ratio={actual_ar}")
                            logger.info(f"[PHOTO DEBUG] candidate_count={c_count}")
                            logger.info(f"[PHOTO DEBUG] width={dl_w}")
                            logger.info(f"[PHOTO DEBUG] height={dl_h}")
                            logger.info(f"[PHOTO DEBUG] source_candidates={c_count}")

                            logger.info("carousel item download success: True")
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
                    raw_entry = it.get("raw_entry")
                    entry_fmts = (raw_entry.get("formats") if raw_entry else None) or []
                    entry_has_audio = any(_has_audio_in_format(f) for f in entry_fmts)
                    if not entry_has_audio and raw_entry and raw_entry.get("acodec") not in (None, "none"):
                        entry_has_audio = True

                    # If the entry has formats with separate or merged audio, use yt-dlp to process and merge
                    if raw_entry and entry_has_audio and ffmpeg_bin:
                        try:
                            v_ydl_opts = {
                                "format": "bestvideo+bestaudio/best[vcodec!=none][acodec!=none]",
                                "outtmpl": str(temp_dir / f"carousel_{idx}_%(id)s.%(ext)s"),
                                "merge_output_format": "mp4",
                                "postprocessor_args": {"merger": ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"]},
                                "ffmpeg_location": str(Path(ffmpeg_bin).parent),
                                "quiet": True,
                                "no_warnings": True,
                                "socket_timeout": 20,
                                "http_headers": DEFAULT_HTTP_HEADERS,
                            }
                            cleaned_entry = copy.deepcopy(raw_entry)
                            for k in ("requested_formats", "requested_subtitles", "format", "format_id", "format_note"):
                                cleaned_entry.pop(k, None)
                            with yt_dlp.YoutubeDL(v_ydl_opts) as ydl_item:
                                ydl_item.process_ie_result(cleaned_entry, download=True)
                            found_v = [f for f in temp_dir.glob(f"carousel_{idx}_*") if f.is_file() and not f.name.endswith((".part", ".ytdl"))]
                            if found_v and found_v[0].stat().st_size > 0:
                                item_file = found_v[0]
                                downloaded_ok = True
                        except Exception as proc_e:
                            logger.debug(f"process_ie_result failed for carousel video {idx}: {proc_e}")

                    if not downloaded_ok and iurl and ("fbcdn.net" in iurl or "cdninstagram.com" in iurl or ".mp4" in iurl):
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
                            "format": "bestvideo+bestaudio/best[vcodec!=none][acodec!=none]/best",
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
                        has_v, has_a = probe_media_streams(item_file)

                        # Emergency audio merge for carousel video if audio is missing
                        if not has_a and entry_has_audio and ffmpeg_bin:
                            logger.warning(f"Audio missing in carousel video {idx}, attempting emergency audio merge...")
                            try:
                                audio_fmts = [f for f in entry_fmts if _has_audio_in_format(f) and f.get("vcodec") == "none"]
                                if not audio_fmts:
                                    audio_fmts = [f for f in entry_fmts if _has_audio_in_format(f)]
                                if audio_fmts:
                                    best_a = max(audio_fmts, key=lambda f: f.get("abr") or f.get("tbr") or 0)
                                    a_url = best_a.get("url")
                                    if a_url:
                                        audio_file = temp_dir / f"carousel_{idx}_audio_{uuid.uuid4().hex[:6]}.m4a"
                                        req = urllib.request.Request(a_url, headers=DEFAULT_HTTP_HEADERS)
                                        with urllib.request.urlopen(req, timeout=30) as resp:
                                            with open(audio_file, "wb") as af:
                                                while chunk := resp.read(1024 * 512):
                                                    af.write(chunk)
                                        if audio_file.exists() and audio_file.stat().st_size > 0:
                                            merged_file = temp_dir / f"carousel_{idx}_merged_{uuid.uuid4().hex[:6]}.mp4"
                                            cmd = [
                                                ffmpeg_bin, "-y",
                                                "-i", str(item_file),
                                                "-i", str(audio_file),
                                                "-map", "0:v:0",
                                                "-map", "1:a:0",
                                                "-c:v", "copy",
                                                "-c:a", "aac",
                                                "-b:a", "192k",
                                                "-movflags", "+faststart",
                                                str(merged_file),
                                            ]
                                            merge_res = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                                            if merge_res.returncode == 0 and merged_file.exists() and merged_file.stat().st_size > 0:
                                                item_file = merged_file
                                                has_v, has_a = probe_media_streams(item_file)
                            except Exception as m_err:
                                logger.error(f"Carousel emergency audio merge failed: {m_err}", exc_info=True)

                        # Normalize to iOS-compatible H.264 + AAC MP4 with faststart
                        item_file, item_probe = normalize_video_for_ios(
                            item_file, source_has_audio=entry_has_audio, temp_dir=temp_dir
                        )
                        has_v = item_probe.get("has_video", False)
                        has_a = item_probe.get("has_audio", False)

                        logger.info("carousel item download success: True")
                        logger.info(f"carousel item video stream: {'YES' if has_v else 'NO'}")
                        logger.info(f"carousel item audio stream: {'YES' if has_a else 'NO'}")
                        logger.info(f"VIDEO CHECK: codec={item_probe.get('video_codec')}")
                        if has_a:
                            logger.info(f"AUDIO CHECK: codec={item_probe.get('audio_codec')}")
                            logger.info("CONTAINER CHECK: mp4")
                            logger.info("STREAMS: video=True audio=True")
                        else:
                            logger.info("AUDIO SOURCE: unavailable")
                            logger.info(f"AUDIO CHECK: codec={item_probe.get('audio_codec') or 'none'}")
                            logger.info("CONTAINER CHECK: mp4")
                            logger.info("STREAMS: video=True audio=False")

                        # Validate carousel video item criteria
                        if not has_v:
                            logger.error(f"Carousel item {idx} validation failed: video stream missing")
                        if entry_has_audio and not has_a:
                            logger.error(f"Carousel item {idx} validation failed: audio stream missing")
                        if item_probe.get("video_codec") in ("vp9", "vp8", "av1", "av01"):
                            logger.error(f"Carousel item {idx} validation failed: incompatible codec {item_probe.get('video_codec')}")
                        if not item_probe.get("container") or "mp4" not in str(item_probe.get("container", "")):
                            logger.error(f"Carousel item {idx} validation failed: container {item_probe.get('container')}")

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
            title=(cached_info.get("title") if cached_info else None) or "Carousel",
            uploader=(cached_info.get("uploader") if cached_info else None) or "Creator",
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

        logger.info(f"whether video stream exists: {'YES' if any(t == 'video' for t, _ in carousel_files) else 'NO'}")
        logger.info(f"whether audio stream exists: {'YES' if any(t == 'video' for t, _ in carousel_files) else 'NO'}")

        if platform_info.key == "instagram":
            has_v = any(t == 'video' for t, _ in carousel_files)
            logger.info(f"[IG DEBUG] URL={url}")
            logger.info("[IG DEBUG] extractor=instagram")
            logger.info(f"[IG DEBUG] formats={len(carousel_files)}")
            logger.info("[IG DEBUG] selected=carousel")
            logger.info(f"[IG DEBUG] video_stream={'YES' if has_v else 'NO'}")
            logger.info(f"[IG DEBUG] audio_stream={'YES' if has_v else 'NO'}")
            logger.info("[IG DEBUG] download_error=None")
            logger.info(f"[IG DEBUG] ffprobe_video={'YES' if has_v else 'NO'}")
            logger.info(f"[IG DEBUG] ffprobe_audio={'YES' if has_v else 'NO'}")

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
        photo_cand = {}
        if cached_info:
            photo_cand = select_best_photo_candidate(cached_info, fallback_url=cached_info.get("url"))
            photo_url = photo_cand.get("selected_url")

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
                        entries_p = list(raw_p.get("entries") or [raw_p])
                        if len(entries_p) > 1:
                            logger.info(f"Detected multi-item carousel ({len(entries_p)} items) in photo fallback, routing to carousel pipeline...")
                            raw_p["is_carousel"] = True
                            return _sync_download(
                                url=url,
                                user_id=user_id,
                                quality=quality,
                                req_id=req_id,
                                progress_callback=progress_callback,
                                cached_info=raw_p,
                            )
                        e0 = entries_p[0]
                        photo_cand = select_best_photo_candidate(e0, fallback_url=e0.get("url"))
                        photo_url = photo_cand.get("selected_url")
            except Exception as e:
                logger.warning(f"Photo URL fallback error: {e}")

        if not photo_url and platform_info.key == "instagram":
            fb_info = _extract_instagram_embed_fallback(url)
            if fb_info:
                photo_cand = select_best_photo_candidate(fb_info, fallback_url=fb_info.get("url"))
                photo_url = photo_cand.get("selected_url")

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
                if not photo_file.exists() or d_size == 0:
                    record_download_finish(success=False)
                    return DownloadResult(
                        success=False,
                        error_code="PHOTO_DOWNLOAD_FAILED",
                        error_message="❌ Photo download failed (0 bytes received).",
                        temp_dir=temp_dir,
                    )

                pil_w, pil_h = inspect_image_dimensions(photo_file)
                src_w = photo_cand.get("width") or pil_w
                src_h = photo_cand.get("height") or pil_h
                dl_w = pil_w or src_w
                dl_h = pil_h or src_h
                actual_ar = round(dl_w / dl_h, 4) if (dl_w and dl_h and dl_h > 0) else None
                c_count = photo_cand.get("source_candidates", 1)

                # 7 MANDATORY PHOTO DEBUG LOGS FOR SINGLE PHOTO
                logger.info(f"[PHOTO DEBUG] selected_url={photo_url}")
                logger.info(f"[PHOTO DEBUG] source_width={src_w}")
                logger.info(f"[PHOTO DEBUG] source_height={src_h}")
                logger.info(f"[PHOTO DEBUG] downloaded_width={dl_w}")
                logger.info(f"[PHOTO DEBUG] downloaded_height={dl_h}")
                logger.info(f"[PHOTO DEBUG] aspect_ratio={actual_ar}")
                logger.info(f"[PHOTO DEBUG] candidate_count={c_count}")
                logger.info(f"[PHOTO DEBUG] width={dl_w}")
                logger.info(f"[PHOTO DEBUG] height={dl_h}")
                logger.info(f"[PHOTO DEBUG] source_candidates={c_count}")

                logger.info(f"canonical URL: {url}")
                logger.info("extractor result type: photo")
                logger.info("format count: 1")
                logger.info("selected format: photo")
                logger.info("whether video stream exists: NO")
                logger.info("whether audio stream exists: NO")
                logger.info(f"image content-type: {ctype}")
                logger.info(f"HTTP status: {status}")
                logger.info(f"downloaded size: {d_size} bytes")
                logger.info(f"final image path: {photo_file}")

                record_download_finish(success=True)
                media_info = MediaInfo(
                    title=(cached_info.get("title") if cached_info else None) or "Photo",
                    uploader=(cached_info.get("uploader") if cached_info else None) or "Unknown Creator",
                    duration=None,
                    thumbnail=photo_url,
                    filesize_approx=d_size,
                    width=dl_w,
                    height=dl_h,
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

    is_instagram = bool(platform_info and platform_info.key == "instagram")
    ydl_opts = {
        "format": format_spec,
        "outtmpl": outtmpl_pattern,
        "max_filesize": MAX_FILE_SIZE_BYTES,
        "noplaylist": not is_instagram,
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

            raw_entries = info_dict.get("entries")
            if raw_entries is not None:
                entries_list = [e for e in raw_entries if e]
                info_dict["entries"] = entries_list
                if len(entries_list) > 1:
                    logger.info(f"Multi-item carousel detected ({len(entries_list)} items), routing to carousel pipeline...")
                    carousel_items = []
                    for idx, entry in enumerate(entries_list):
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
                            photo_cand = select_best_photo_candidate(entry, fallback_url=entry.get("url"))
                            item_url = photo_cand.get("selected_url") or entry.get("url")
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
                elif len(entries_list) == 1:
                    video_entry = next(
                        (e for e in entries_list if e and (e.get("vcodec") != "none" or e.get("formats"))),
                        entries_list[0],
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

            # Group candidate files in temp_dir by type
            video_files = [
                f for f in downloaded_files
                if f.suffix.lower() in (".mp4", ".mkv", ".webm", ".mov")
            ]
            audio_files = [
                f for f in downloaded_files
                if f.suffix.lower() in (".m4a", ".mp3", ".aac", ".opus", ".ogg", ".wav", ".flac")
            ]

            ffmpeg_merge_status = "direct"
            downloaded_audio_detected = False

            if video_files and audio_files and ffmpeg_bin:
                v_cand = video_files[0]
                a_cand = audio_files[0]
                has_v, has_a = probe_media_streams(v_cand)
                downloaded_audio_detected = True
                if not has_a:
                    logger.info(
                        f"Separate video ({v_cand.name}) and audio ({a_cand.name}) detected in temp_dir! "
                        "Merging into single MP4..."
                    )
                    merged_out = temp_dir / f"merged_{uuid.uuid4().hex[:6]}.mp4"
                    cmd = [
                        ffmpeg_bin, "-y",
                        "-i", str(v_cand),
                        "-i", str(a_cand),
                        "-map", "0:v:0",
                        "-map", "1:a:0",
                        "-c:v", "copy",
                        "-c:a", "aac",
                        "-b:a", "192k",
                        "-movflags", "+faststart",
                        str(merged_out),
                    ]
                    m_proc = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
                    if m_proc.returncode == 0 and merged_out.exists() and merged_out.stat().st_size > 0:
                        downloaded_file = merged_out
                        ffmpeg_merge_status = "success"
                    else:
                        logger.error(f"FFmpeg merge of separate files failed: {m_proc.stderr}")
                        downloaded_file = v_cand
                else:
                    downloaded_file = v_cand
            elif video_files:
                downloaded_file = video_files[0]
            else:
                downloaded_file = downloaded_files[0]

            file_size = downloaded_file.stat().st_size
            exists = downloaded_file.exists()
            ext = downloaded_file.suffix.lower()

            logger.info(f"file exists: {exists}")
            logger.info(f"file size: {file_size}")
            logger.info(f"correct media extension: {ext in ('.mp4', '.mkv', '.webm', '.mov', '.m4a', '.mp3')}")

            source_fmts = info_dict.get("formats") or []
            source_has_audio = any(_has_audio_in_format(f) for f in source_fmts)
            if not source_has_audio and info_dict.get("acodec") not in (None, "none"):
                source_has_audio = True
            media_info.has_audio = source_has_audio

            # Probe media streams to verify sound/video presence
            has_v_stream, has_a_stream = probe_media_streams(downloaded_file)
            if has_a_stream:
                downloaded_audio_detected = True

            # Emergency fallback merge if audio was available at source but missing in downloaded file
            if not has_a_stream and source_has_audio and ffmpeg_bin:
                logger.warning(
                    f"Audio missing in downloaded file {downloaded_file.name} despite source having audio! "
                    "Attempting emergency audio extraction and merge..."
                )
                try:
                    audio_fmts = [f for f in source_fmts if _has_audio_in_format(f) and f.get("vcodec") == "none"]
                    if not audio_fmts:
                        audio_fmts = [f for f in source_fmts if _has_audio_in_format(f)]
                    if audio_fmts:
                        best_a = max(audio_fmts, key=lambda f: f.get("abr") or f.get("tbr") or 0)
                        a_target = temp_dir / f"emergency_audio_{uuid.uuid4().hex[:6]}.m4a"
                        dl_audio_ok = False

                        # 1. Try yt-dlp single stream audio extraction first
                        try:
                            a_fmt_id = best_a.get("format_id")
                            if a_fmt_id:
                                a_ydl_opts = {
                                    "format": a_fmt_id,
                                    "outtmpl": str(a_target),
                                    "quiet": True,
                                    "no_warnings": True,
                                    "socket_timeout": 15,
                                    "http_headers": DEFAULT_HTTP_HEADERS,
                                }
                                with yt_dlp.YoutubeDL(a_ydl_opts) as a_ydl:
                                    a_ydl.download([url])
                                if a_target.exists() and a_target.stat().st_size > 0:
                                    dl_audio_ok = True
                        except Exception as a_dl_err:
                            logger.debug(f"yt-dlp audio download attempt failed: {a_dl_err}")

                        # 2. Fallback to direct URL download if yt-dlp did not generate file
                        if not dl_audio_ok:
                            a_url = best_a.get("url")
                            if a_url:
                                req = urllib.request.Request(a_url, headers=DEFAULT_HTTP_HEADERS)
                                with urllib.request.urlopen(req, timeout=30) as resp:
                                    with open(a_target, "wb") as af:
                                        while chunk := resp.read(1024 * 512):
                                            af.write(chunk)
                                if a_target.exists() and a_target.stat().st_size > 0:
                                    dl_audio_ok = True

                        if dl_audio_ok:
                            downloaded_audio_detected = True
                            merged_tmp = temp_dir / f"emergency_merged_{uuid.uuid4().hex[:6]}.mp4"
                            cmd = [
                                ffmpeg_bin, "-y",
                                "-i", str(downloaded_file),
                                "-i", str(a_target),
                                "-map", "0:v:0",
                                "-map", "1:a:0",
                                "-c:v", "copy",
                                "-c:a", "aac",
                                "-b:a", "192k",
                                "-movflags", "+faststart",
                                str(merged_tmp),
                            ]
                            m_proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                            if m_proc.returncode == 0 and merged_tmp.exists() and merged_tmp.stat().st_size > 0:
                                downloaded_file = merged_tmp
                                file_size = downloaded_file.stat().st_size
                                ext = downloaded_file.suffix.lower()
                                has_v_stream, has_a_stream = probe_media_streams(downloaded_file)
                                ffmpeg_merge_status = "success"
                except Exception as emerg_err:
                    logger.error(f"Emergency audio merge failed: {emerg_err}", exc_info=True)

            is_audio_file = ext in (
                ".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".flac"
            ) or quality == "audio"

            # Normalize video for iOS compatibility (H.264/AVC + AAC + MP4 + faststart)
            if not is_audio_file and downloaded_file.exists():
                downloaded_file, norm_probe = normalize_video_for_ios(
                    downloaded_file,
                    source_has_audio=source_has_audio,
                    temp_dir=temp_dir,
                )
                file_size = downloaded_file.stat().st_size
                ext = downloaded_file.suffix.lower()
                has_v_stream = norm_probe.get("has_video", False)
                has_a_stream = norm_probe.get("has_audio", False)
                video_codec = norm_probe.get("video_codec") or "unknown"
                audio_codec = norm_probe.get("audio_codec") or ("none" if not has_a_stream else "unknown")
                container = norm_probe.get("container") or "unknown"

                if ffmpeg_merge_status == "direct":
                    ffmpeg_merge_status = "remux_faststart" if norm_probe.get("is_ios_compatible") else "transcode"

                if norm_probe.get("width"):
                    media_info.width = norm_probe.get("width")
                if norm_probe.get("height"):
                    media_info.height = norm_probe.get("height")

                # Mandatory iOS checks logged cleanly
                logger.info(f"VIDEO CHECK: codec={video_codec}")
                logger.info(f"AUDIO CHECK: codec={audio_codec}")
                logger.info(f"CONTAINER CHECK: {container}")
                logger.info(f"STREAMS: video={has_v_stream} audio={has_a_stream}")

                if not source_has_audio and not has_a_stream:
                    logger.info("AUDIO SOURCE: unavailable")

                # Format identification for AUDIO DEBUG
                selected_v_fmt = "unknown"
                selected_a_fmt = "none"
                source_vcodec = info_dict.get("vcodec") or "unknown"
                source_acodec = info_dict.get("acodec") or ("none" if not source_has_audio else "unknown")

                req_fmts = info_dict.get("requested_formats")
                if req_fmts:
                    v_f = next((f for f in req_fmts if f.get("vcodec") != "none"), None)
                    a_f = next((f for f in req_fmts if f.get("acodec") != "none" or (f.get("audio_ext") and f.get("audio_ext") != "none")), None)
                    if v_f:
                        selected_v_fmt = v_f.get("format_id") or "unknown"
                        source_vcodec = v_f.get("vcodec") or source_vcodec
                    if a_f:
                        selected_a_fmt = a_f.get("format_id") or "embedded"
                        source_acodec = a_f.get("acodec") or source_acodec
                    if not v_f:
                        selected_v_fmt = info_dict.get("format_id") or "unknown"
                else:
                    selected_v_fmt = info_dict.get("format_id") or "unknown"
                    if _has_audio_in_format(info_dict) or source_has_audio:
                        selected_a_fmt = "embedded" if has_a_stream else "none"

                ffmpeg_ran = bool(ffmpeg_merge_status not in ("none", "direct")) or bool(norm_probe.get("is_ios_compatible") is False)

                # 13 MANDATORY [AUDIO DEBUG] DIAGNOSTIC LOGS
                logger.info(f"[AUDIO DEBUG] source={url}")
                logger.info(f"[AUDIO DEBUG] selected_format={info_dict.get('format_id') or format_spec}")
                logger.info(f"[AUDIO DEBUG] video_format={selected_v_fmt}")
                logger.info(f"[AUDIO DEBUG] audio_format={selected_a_fmt}")
                logger.info(f"[AUDIO DEBUG] source_vcodec={source_vcodec}")
                logger.info(f"[AUDIO DEBUG] source_acodec={source_acodec}")
                logger.info(f"[AUDIO DEBUG] audio_downloaded={downloaded_audio_detected}")
                logger.info(f"[AUDIO DEBUG] ffmpeg_started={ffmpeg_ran}")
                logger.info(f"[AUDIO DEBUG] ffmpeg_result={ffmpeg_merge_status}")
                logger.info(f"[AUDIO DEBUG] final_vcodec={video_codec}")
                logger.info(f"[AUDIO DEBUG] final_acodec={audio_codec}")
                logger.info(f"[AUDIO DEBUG] final_video_stream={has_v_stream}")
                logger.info(f"[AUDIO DEBUG] final_audio_stream={has_a_stream}")

                # Also retain backwards-compatible keys
                logger.info(f"[AUDIO DEBUG] source_has_audio={source_has_audio}")
                logger.info(f"[AUDIO DEBUG] selected_video_format={selected_v_fmt}")
                logger.info(f"[AUDIO DEBUG] selected_audio_format={selected_a_fmt}")
                logger.info(f"[AUDIO DEBUG] downloaded_audio={downloaded_audio_detected}")
                logger.info(f"[AUDIO DEBUG] ffmpeg_merge={ffmpeg_merge_status}")

                # MANDATORY FINAL VALIDATION LOGS
                v_dur = norm_probe.get("duration") or info_dict.get("duration")
                logger.info(f"VIDEO STREAM = {'TRUE' if has_v_stream else 'FALSE'}")
                logger.info(f"AUDIO STREAM = {'TRUE' if has_a_stream else 'FALSE'}")
                logger.info(f"final_video_codec={video_codec}")
                logger.info(f"final_audio_codec={audio_codec}")
                logger.info(f"final_duration={v_dur}")

                # Validation per iOS & Audio requirements:
                if not has_v_stream:
                    record_download_finish(success=False)
                    return DownloadResult(
                        success=False,
                        media_info=media_info,
                        error_code="VIDEO_STREAM_MISSING",
                        error_message="❌ Validation failed: Downloaded video does not contain a valid video stream.",
                        temp_dir=temp_dir,
                    )
                if source_has_audio and not has_a_stream:
                    record_download_finish(success=False)
                    return DownloadResult(
                        success=False,
                        media_info=media_info,
                        error_code="AUDIO_STREAM_MISSING",
                        error_message="❌ Validation failed: Audio stream was available at source but missing in the final delivered video. Refusing to send silent video.",
                        temp_dir=temp_dir,
                    )
                if video_codec.lower() in ("vp9", "av1", "vp09", "av01"):
                    record_download_finish(success=False)
                    return DownloadResult(
                        success=False,
                        media_info=media_info,
                        error_code="INVALID_VIDEO_CODEC",
                        error_message=f"❌ Validation failed: Delivered video codec is {video_codec}, which is not playable on iOS.",
                        temp_dir=temp_dir,
                    )
                if "mp4" not in container.lower() and ext != ".mp4":
                    record_download_finish(success=False)
                    return DownloadResult(
                        success=False,
                        media_info=media_info,
                        error_code="INVALID_CONTAINER",
                        error_message=f"❌ Validation failed: Delivered container is {container}, expected mp4.",
                        temp_dir=temp_dir,
                    )

            logger.info(
                f"[VIDEO VERIFICATION] file={downloaded_file.name}, size={file_size}, "
                f"has_video={has_v_stream}, has_audio={has_a_stream}, ext={ext}"
            )
            logger.info(
                f"[DIAGNOSTICS] URL: {url} | ResultType: {info_dict.get('_type', 'video')} | "
                f"Formats: {len(info_dict.get('formats') or [])} | SelectedFormat: {info_dict.get('format_id')} | "
                f"VideoStream: {has_v_stream} | AudioStream: {has_a_stream} | Error: None"
            )

            # Strict logging per Requirement 11 & 12
            if not source_has_audio and not has_a_stream:
                logger.info("AUDIO SOURCE: unavailable")
                logger.info(f"AUDIO CHECK: video={has_v_stream} audio=False")
            else:
                logger.info(f"AUDIO CHECK: video={has_v_stream} audio={has_a_stream}")

            if platform_info.key == "instagram":
                logger.info(f"[IG DEBUG] URL={url}")
                logger.info("[IG DEBUG] extractor=instagram")
                logger.info(f"[IG DEBUG] formats={len(info_dict.get('formats') or [])}")
                logger.info(f"[IG DEBUG] selected={info_dict.get('format_id')}")
                logger.info(f"[IG DEBUG] video_stream={'YES' if has_v_stream else 'NO'}")
                logger.info(f"[IG DEBUG] audio_stream={'YES' if has_a_stream else 'NO'}")
                logger.info("[IG DEBUG] download_error=None")
                logger.info(f"[IG DEBUG] ffprobe_video={'YES' if has_v_stream else 'NO'}")
                logger.info(f"[IG DEBUG] ffprobe_audio={'YES' if has_a_stream else 'NO'}")

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
            if cached_ie and cached_ie.get("entries"):
                raw_c_entries = cached_ie.get("entries")
                c_entries = [e for e in raw_c_entries if e]
                cached_ie["entries"] = c_entries
                if len(c_entries) > 1:
                    carousel_items = []
                    for idx, entry in enumerate(c_entries):
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
                            photo_cand = select_best_photo_candidate(entry, fallback_url=entry.get("url"))
                            item_url = photo_cand.get("selected_url") or entry.get("url")
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

            # Check preview extraction to accurately detect multi-item carousels vs single photos
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
                err_cat, err_code = categorize_error(fb_err)
                logger.warning(
                    f"[DIAGNOSTICS] URL: {url} | ResultType: fallback_error | "
                    f"Formats: 0 | SelectedFormat: None | "
                    f"VideoStream: False | AudioStream: False | "
                    f"Category: {err_cat} | ErrorCode: {err_code} | Error: {fb_err}"
                )
                msg = format_extraction_error(fb_err, url)
                return DownloadResult(
                    success=False,
                    error_code=err_code,
                    error_message=msg,
                    temp_dir=temp_dir,
                )

        err_cat, err_code = categorize_error(e)
        logger.warning(
            f"[DIAGNOSTICS] URL: {url} | ResultType: download_error | "
            f"Formats: 0 | SelectedFormat: None | "
            f"VideoStream: False | AudioStream: False | "
            f"Category: {err_cat} | ErrorCode: {err_code} | Error: {e}"
        )
        if platform_info.key == "instagram":
            logger.info(f"[IG DEBUG] URL={url}")
            logger.info("[IG DEBUG] extractor=instagram")
            logger.info("[IG DEBUG] formats=0")
            logger.info("[IG DEBUG] selected=None")
            logger.info("[IG DEBUG] video_stream=NO")
            logger.info("[IG DEBUG] audio_stream=NO")
            logger.info(f"[IG DEBUG] download_error={e}")
            logger.info("[IG DEBUG] ffprobe_video=NO")
            logger.info("[IG DEBUG] ffprobe_audio=NO")

        msg = format_extraction_error(e, url)
        return DownloadResult(
            success=False,
            error_code=err_code,
            error_message=msg,
            temp_dir=temp_dir,
        )

    except Exception as e:
        record_download_finish(success=False)
        err_cat, err_code = categorize_error(e)
        logger.warning(
            f"[DIAGNOSTICS] URL: {url} | ResultType: unexpected_error | "
            f"Formats: 0 | SelectedFormat: None | "
            f"VideoStream: False | AudioStream: False | "
            f"Category: {err_cat} | ErrorCode: {err_code} | Error: {e}"
        )
        if platform_info.key == "instagram":
            logger.info(f"[IG DEBUG] URL={url}")
            logger.info("[IG DEBUG] extractor=instagram")
            logger.info("[IG DEBUG] formats=0")
            logger.info("[IG DEBUG] selected=None")
            logger.info("[IG DEBUG] video_stream=NO")
            logger.info("[IG DEBUG] audio_stream=NO")
            logger.info(f"[IG DEBUG] download_error={e}")
            logger.info("[IG DEBUG] ffprobe_video=NO")
            logger.info("[IG DEBUG] ffprobe_audio=NO")

        logger.error(f"Unexpected download error for {url}: {e}", exc_info=True)
        return DownloadResult(
            success=False,
            error_code=err_code,
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
