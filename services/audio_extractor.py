import asyncio
import logging
from pathlib import Path
from typing import Optional

import yt_dlp
from services.system_check import resolve_ffmpeg_path

logger = logging.getLogger(__name__)


async def extract_audio_from_video(
    video_path: Path,
    output_dir: Optional[Path] = None,
    title: Optional[str] = None,
    artist: Optional[str] = None,
) -> tuple[bool, Optional[Path], Optional[str]]:
    """
    Extracts high-quality MP3 audio from a local video file using FFmpeg.
    """
    if not video_path.exists():
        return False, None, "Video file not found on disk."

    ffmpeg_bin = resolve_ffmpeg_path()
    if not ffmpeg_bin:
        return False, None, "FFmpeg is not available on this server."

    target_dir = output_dir or video_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)
    audio_path = target_dir / f"{video_path.stem}.mp3"

    if audio_path.exists() and audio_path.stat().st_size > 0:
        return True, audio_path, None

    cmd = [
        ffmpeg_bin,
        "-y",
        "-i", str(video_path),
        "-vn",
        "-c:a", "libmp3lame",
        "-q:a", "2",  # VBR ~190 kbps high quality
    ]
    if title:
        cmd.extend(["-metadata", f"title={title}"])
    if artist:
        cmd.extend(["-metadata", f"artist={artist}"])
    cmd.append(str(audio_path))

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            err_text = stderr.decode(errors="replace")[-300:] if stderr else "Unknown error"
            logger.error(f"FFmpeg audio extraction failed (code {proc.returncode}): {err_text}")
            return False, None, "Audio extraction failed."

        if not audio_path.exists() or audio_path.stat().st_size == 0:
            return False, None, "Extracted audio file is empty."

        return True, audio_path, None
    except Exception as e:
        logger.exception(f"Unexpected error during audio extraction: {e}")
        return False, None, "Audio extraction error."


def download_audio_from_url_sync(
    url: str,
    output_dir: Path,
) -> tuple[bool, Optional[Path], Optional[str]]:
    """
    Fallback: Downloads audio directly from source URL and converts to MP3
    when local video is no longer cached on disk.
    """
    ffmpeg_bin = resolve_ffmpeg_path()
    output_dir.mkdir(parents=True, exist_ok=True)
    outtmpl = str(output_dir / "%(title).50s.%(ext)s")

    ydl_opts = {
        "format": "bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best",
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }],
    }
    if ffmpeg_bin:
        ydl_opts["ffmpeg_location"] = str(Path(ffmpeg_bin).parent)

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.extract_info(url, download=True)

        # Look for the resulting .mp3 file
        for f in output_dir.glob("*.mp3"):
            if f.is_file() and f.stat().st_size > 0:
                return True, f, None

        for f in output_dir.iterdir():
            if f.is_file() and f.stat().st_size > 0:
                return True, f, None

        return False, None, "No audio file produced."
    except Exception as e:
        logger.warning(f"Audio download fallback error for {url}: {e}")
        return False, None, str(e)


async def download_audio_from_url(
    url: str,
    output_dir: Path,
) -> tuple[bool, Optional[Path], Optional[str]]:
    """Asynchronous wrapper for download_audio_from_url_sync."""
    return await asyncio.to_thread(download_audio_from_url_sync, url, output_dir)
