import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_CACHED_FFMPEG_PATH: Optional[str] = None


def resolve_ffmpeg_path() -> Optional[str]:
    """
    Finds ffmpeg executable on PATH or in common Windows installation directories.
    If found outside PATH, automatically prepends its directory to os.environ["PATH"]
    so subsequent shutil.which() calls and child processes (yt-dlp) immediately find it.
    """
    global _CACHED_FFMPEG_PATH
    if _CACHED_FFMPEG_PATH and os.path.isfile(_CACHED_FFMPEG_PATH):
        return _CACHED_FFMPEG_PATH

    bin_path = shutil.which("ffmpeg")
    if bin_path:
        _CACHED_FFMPEG_PATH = bin_path
        return bin_path

    # Search common Windows installation paths
    candidate_patterns = [
        Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "WinGet" / "Packages",
        Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "FFmpeg" / "bin",
        Path("C:\\ffmpeg\\bin"),
        Path(os.environ.get("USERPROFILE", "")) / "scoop" / "apps" / "ffmpeg" / "current" / "bin",
        Path("C:\\ProgramData\\chocolatey\\bin"),
    ]

    for base in candidate_patterns:
        if not base.exists():
            continue
        if base.is_dir() and "WinGet" in str(base):
            for exe in base.glob("**/ffmpeg.exe"):
                if exe.is_file():
                    bin_dir = str(exe.parent)
                    if bin_dir not in os.environ.get("PATH", ""):
                        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
                    _CACHED_FFMPEG_PATH = str(exe)
                    return _CACHED_FFMPEG_PATH
        else:
            exe = base / "ffmpeg.exe"
            if exe.is_file():
                bin_dir = str(exe.parent)
                if bin_dir not in os.environ.get("PATH", ""):
                    os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
                _CACHED_FFMPEG_PATH = str(exe)
                return _CACHED_FFMPEG_PATH

    return shutil.which("ffmpeg")


def is_ffmpeg_installed() -> bool:
    """Returns True if ffmpeg is found and executable."""
    return bool(resolve_ffmpeg_path())


def get_ffmpeg_version() -> Optional[str]:
    """Runs `ffmpeg -version` and returns the first line of output."""
    ffmpeg_bin = resolve_ffmpeg_path()
    if not ffmpeg_bin:
        return None
    try:
        proc = subprocess.run(
            [ffmpeg_bin, "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
            check=False,
        )
        if proc.returncode == 0 and proc.stdout:
            return proc.stdout.splitlines()[0].strip()
    except Exception as e:
        logger.debug(f"Error checking ffmpeg version: {e}")
    return None


def log_startup_system_report() -> None:
    """Logs a clean diagnostics banner during bot startup."""
    ffmpeg_bin = resolve_ffmpeg_path()
    if ffmpeg_bin:
        version = get_ffmpeg_version() or "Unknown Version"
        logger.info(
            f"\n==============================================================\n"
            f"✅ [SYSTEM CHECK] FFmpeg is INSTALLED & ACTIVE\n"
            f"• Binary: {ffmpeg_bin}\n"
            f"• Version: {version}\n"
            f"• Capabilities: Full stream merging (1080p+), MP4 remuxing enabled\n"
            f"=============================================================="
        )
    else:
        logger.warning(
            f"\n==============================================================\n"
            f"⚠️ [SYSTEM CHECK] FFmpeg is NOT detected!\n"
            f"• Quality limit: Progressive formats only (max 720p/360p)\n"
            f"• Install via: winget install Gyan.FFmpeg\n"
            f"=============================================================="
        )
