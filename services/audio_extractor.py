import asyncio
import logging
from pathlib import Path
from typing import Optional

from services.system_check import resolve_ffmpeg_path

logger = logging.getLogger(__name__)


async def extract_audio_from_video(
    video_path: Path,
    output_dir: Optional[Path] = None,
    title: Optional[str] = None,
    artist: Optional[str] = None,
) -> tuple[bool, Optional[Path], Optional[str]]:
    """
    Extracts high-quality MP3 audio directly from a local cached video using FFmpeg.
    Avoids re-downloading from the remote source.
    """
    if not video_path.exists():
        return False, None, "Video file not found."

    ffmpeg_bin = resolve_ffmpeg_path()
    if not ffmpeg_bin:
        return False, None, "FFmpeg is not available on this server."

    target_dir = output_dir or video_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)
    audio_path = target_dir / f"{video_path.stem}.mp3"

    # If audio already extracted and valid, return it immediately
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

        logger.info(f"Audio successfully extracted to {audio_path} ({audio_path.stat().st_size / 1024:.1f} KB)")
        return True, audio_path, None

    except Exception as e:
        logger.exception(f"Unexpected error during audio extraction: {e}")
        return False, None, "Audio extraction error."
