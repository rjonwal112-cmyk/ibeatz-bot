import re
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PlatformInfo:
    """Metadata representing a detected public media platform."""
    name: str
    key: str
    is_primary: bool = False
    icon: str = "🌐"


SUPPORTED_PLATFORMS: dict[str, PlatformInfo] = {
    "youtube": PlatformInfo(name="YouTube", key="youtube", is_primary=True, icon="🔴"),
    "instagram": PlatformInfo(name="Instagram", key="instagram", is_primary=True, icon="📸"),
    "facebook": PlatformInfo(name="Facebook", key="facebook", is_primary=True, icon="🔵"),
    "tiktok": PlatformInfo(name="TikTok", key="tiktok", is_primary=False, icon="🎵"),
    "twitter": PlatformInfo(name="X / Twitter", key="twitter", is_primary=False, icon="🐦"),
    "reddit": PlatformInfo(name="Reddit", key="reddit", is_primary=False, icon="🤖"),
    "pinterest": PlatformInfo(name="Pinterest", key="pinterest", is_primary=False, icon="📌"),
    "vimeo": PlatformInfo(name="Vimeo", key="vimeo", is_primary=False, icon="🎬"),
    "twitch": PlatformInfo(name="Twitch", key="twitch", is_primary=False, icon="🟣"),
    "dailymotion": PlatformInfo(name="Dailymotion", key="dailymotion", is_primary=False, icon="📺"),
    "soundcloud": PlatformInfo(name="SoundCloud", key="soundcloud", is_primary=False, icon="☁️"),
    "bandcamp": PlatformInfo(name="Bandcamp", key="bandcamp", is_primary=False, icon="⛺"),
    "threads": PlatformInfo(name="Threads", key="threads", is_primary=False, icon="🧵"),
    "tumblr": PlatformInfo(name="Tumblr", key="tumblr", is_primary=False, icon="📝"),
    "streamable": PlatformInfo(name="Streamable", key="streamable", is_primary=False, icon="📹"),
    "9gag": PlatformInfo(name="9GAG", key="9gag", is_primary=False, icon="😆"),
    "bilibili": PlatformInfo(name="Bilibili", key="bilibili", is_primary=False, icon="⚡"),
    "vk": PlatformInfo(name="VK", key="vk", is_primary=False, icon="🔷"),
    "linkedin": PlatformInfo(name="LinkedIn", key="linkedin", is_primary=False, icon="💼"),
    "rumble": PlatformInfo(name="Rumble", key="rumble", is_primary=False, icon="🟢"),
}

DEFAULT_PLATFORM = PlatformInfo(name="Media Source", key="generic", is_primary=False, icon="🌐")

_DOMAIN_MAP: list[tuple[str, str]] = [
    (r"(?:youtube\.com|youtu\.be)", "youtube"),
    (r"(?:instagram\.com|instagr\.am)", "instagram"),
    (r"(?:facebook\.com|fb\.watch|fb\.com)", "facebook"),
    (r"tiktok\.com", "tiktok"),
    (r"(?:twitter\.com|x\.com)", "twitter"),
    (r"(?:reddit\.com|redd\.it)", "reddit"),
    (r"(?:pinterest\.com|pin\.it)", "pinterest"),
    (r"vimeo\.com", "vimeo"),
    (r"twitch\.tv", "twitch"),
    (r"(?:dailymotion\.com|dai\.ly)", "dailymotion"),
    (r"soundcloud\.com", "soundcloud"),
    (r"bandcamp\.com", "bandcamp"),
    (r"threads\.net", "threads"),
    (r"tumblr\.com", "tumblr"),
    (r"streamable\.com", "streamable"),
    (r"9gag\.com", "9gag"),
    (r"bilibili\.com", "bilibili"),
    (r"vk\.com", "vk"),
    (r"linkedin\.com", "linkedin"),
    (r"rumble\.com", "rumble"),
]


def detect_platform_from_url(url: str) -> PlatformInfo:
    """Detects the platform from a media URL."""
    if not url:
        return DEFAULT_PLATFORM

    url_lower = url.lower()
    for pattern, key in _DOMAIN_MAP:
        if re.search(pattern, url_lower):
            return SUPPORTED_PLATFORMS.get(key, DEFAULT_PLATFORM)

    return DEFAULT_PLATFORM


def extract_canonical_id(platform_key: str, url: str) -> Optional[str]:
    """
    Extracts a stable unique identifier for the media on that platform.
    Used for caching and request deduplication.
    """
    if platform_key == "youtube":
        match = re.search(r"(?:v=|youtu\.be\/|embed\/|shorts\/|live\/)([0-9A-Za-z_-]{11})", url)
        if match:
            return match.group(1)
    elif platform_key == "instagram":
        match = re.search(r"(?:reel|reels|p|tv|share\/reel)\/([A-Za-z0-9_-]+)", url)
        if match:
            return match.group(1)
    elif platform_key == "facebook":
        # Handle v=12345 query param
        v_match = re.search(r"[?&]v=(\d+)", url)
        if v_match:
            return v_match.group(1)
        # Handle /videos/123, /reel/123, /watch/123, /share/r/123, /share/v/123
        match = re.search(r"(?:videos|reel|watch|share\/[rv])\/?(?:[^\/]*\/)?(\d+)", url)
        if match:
            return match.group(1)
        # Handle fb.watch shortcodes
        fb_watch = re.search(r"fb\.watch\/([A-Za-z0-9_-]+)", url)
        if fb_watch:
            return fb_watch.group(1)
    elif platform_key == "tiktok":
        match = re.search(r"(?:video|v)\/(\d+)", url)
        if match:
            return match.group(1)
    elif platform_key == "twitter":
        match = re.search(r"status\/(\d+)", url)
        if match:
            return match.group(1)
    elif platform_key == "reddit":
        match = re.search(r"comments\/([a-zA-Z0-9]+)", url)
        if match:
            return match.group(1)

    # Fallback to normalized alphanumeric hash
    import hashlib
    clean_url = url.split("?")[0].rstrip("/")
    return hashlib.md5(clean_url.encode()).hexdigest()[:16]
