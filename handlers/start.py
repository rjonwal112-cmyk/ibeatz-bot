import html
import logging
import time
from typing import Any, Optional

from aiogram import F, Router
from aiogram.enums import ChatType
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from downloader import cancel_download
from handlers.media import (
    _process_single_media_url,
    extract_media_url_from_message,
    media_url_handler,
)
from services.database import db
from services.queue_manager import job_queue

logger = logging.getLogger(__name__)

start_router = Router(name="start_router")
print("START ROUTER LOADED")


def _format_time_ago(ts: float) -> str:
    diff = time.time() - ts
    if diff < 60:
        return "just now"
    elif diff < 3600:
        return f"{int(diff // 60)}m ago"
    elif diff < 86400:
        return f"{int(diff // 3600)}h ago"
    else:
        return f"{int(diff // 86400)}d ago"


def _build_start_keyboard() -> Any:
    builder = InlineKeyboardBuilder()
    builder.button(text="📖 How to Use", callback_data="menu:how_to_use")
    builder.button(text="🌐 Supported Platforms", callback_data="menu:platforms")
    builder.button(text="📥 My Downloads", callback_data="menu:history")
    builder.button(text="⚙️ Settings", callback_data="menu:settings")
    builder.button(text="ℹ️ Help", callback_data="menu:help")
    builder.adjust(2, 2, 1)
    return builder.as_markup()


def _build_back_keyboard() -> Any:
    builder = InlineKeyboardBuilder()
    builder.button(text="« Back to Menu", callback_data="menu:start")
    return builder.as_markup()


async def _render_settings_view(user_id: int) -> tuple[str, Any]:
    pref_quality = await db.get_user_setting(user_id, "preferred_quality", "highest")
    auto_audio = await db.get_user_setting(user_id, "auto_audio", "off")

    text = (
        "⚙️ <b>Personal Preferences</b>\n\n"
        f"• <b>Preferred YouTube Quality:</b> {html.escape(pref_quality.capitalize())}\n"
        f"• <b>Auto-Extract Audio:</b> {'Enabled' if auto_audio == 'on' else 'Disabled'}\n\n"
        "<i>Select an option below to update your preference:</i>"
    )

    builder = InlineKeyboardBuilder()
    qualities = ["highest", "1080p", "720p", "480p"]
    for q in qualities:
        mark = "✓ " if pref_quality == q else ""
        builder.button(text=f"{mark}{q.capitalize()}", callback_data=f"menu:set_q:{q}")
    audio_label = "🔊 Auto-Audio: ON" if auto_audio == "on" else "🔈 Auto-Audio: OFF"
    new_audio_state = "off" if auto_audio == "on" else "on"
    builder.button(text=audio_label, callback_data=f"menu:set_aud:{new_audio_state}")
    builder.button(text="« Back to Menu", callback_data="menu:start")
    builder.adjust(2, 2, 1, 1)
    return text, builder.as_markup()



# =====================================================================
# 1. /start
# =====================================================================
@start_router.message(CommandStart(ignore_case=True))
@start_router.message(Command("start", ignore_case=True))
async def command_start_handler(message: Message) -> None:
    """Sends a clean, welcoming start message with interactive buttons."""
    user = message.from_user
    raw_name = user.first_name if user else "there"
    user_name = html.escape(raw_name)
    logger.info(f"/start from user {user.id if user else 0} ({raw_name})")

    welcome_text = (
        f"👋 Hi <b>{user_name}</b>!\n\n"
        "⚡ <b>ɪʙᴇᴀᴛᴢ bot</b> (@ibeatzfetch_bot)\n"
        "High-performance media downloader for Telegram.\n\n"
        "Send any supported public video URL directly to begin downloading.\n\n"
        "• <b>Primary:</b> YouTube, Shorts, Instagram Reels, Facebook Videos\n"
        "• <b>Secondary:</b> TikTok, X, Reddit, Pinterest, Twitch, Vimeo\n\n"
        "<i>Works in private chats, groups, and supergroups.</i>"
    )
    await message.answer(welcome_text, reply_markup=_build_start_keyboard())


# =====================================================================
# 2. /help
# =====================================================================
@start_router.message(Command("help", ignore_case=True))
async def command_help_handler(message: Message) -> None:
    """Sends comprehensive help and platform usage guide."""
    logger.info(f"/help requested by user {message.from_user.id if message.from_user else 0}")
    help_text = (
        "ℹ️ <b>ɪʙᴇᴀᴛᴢ Bot Guide & Help</b>\n\n"
        "<b>How to Download:</b>\n"
        "1. Send any public video link directly in chat, or use <code>/download &lt;url&gt;</code>.\n"
        "2. The bot fetches media details and starts high-speed downloading.\n"
        "3. In groups, progress updates are cleaned up automatically upon completion.\n\n"
        "<b>Platform Quality Policies:</b>\n"
        "• <b>Instagram & Facebook:</b> Automatically downloads highest available quality (zero friction).\n"
        "• <b>YouTube & Others:</b> Shows genuine resolution options (1080p, 720p, 480p, 360p) if available.\n"
        "• <b>Audio Extraction:</b> Tap <code>[🎵 Audio]</code> on any delivered video for instant MP3.\n\n"
        "<b>Cache & Performance:</b>\n"
        "• Previously downloaded videos are delivered instantly (~0.1s) without re-downloading.\n"
        "• Simultaneous requests for the same media share a single download task.\n\n"
        "<b>Commands:</b>\n"
        "• /start - Start the bot & main menu\n"
        "• /help - How to use & feature guide\n"
        "• /download - Send link or download via command\n"
        "• /settings - Personal preferences\n"
        "• /history - View your recent downloads\n"
        "• /clear - Clear your download history\n"
        "• /cancel - Cancel active download\n"
        "• /status - Check active download progress\n"
        "• /about - Bot architecture & info\n"
        "• /privacy - Data and privacy policy\n\n"
        "<b>Limitations:</b>\n"
        "• Standard Telegram Cloud API limit: 50 MB per file.\n"
        "• Only public media is supported."
    )
    await message.answer(help_text)


# =====================================================================
# 3. /download
# =====================================================================
@start_router.message(Command("download", ignore_case=True))
async def command_download_handler(message: Message) -> None:
    """Handles /download command. If URL is provided, delegates to media pipeline."""
    logger.info(f"/download from user {message.from_user.id if message.from_user else 0}")
    url = extract_media_url_from_message(message)
    if url:
        is_group = message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP)
        await _process_single_media_url(message=message, url=url, is_group=is_group)
    else:
        guide_text = (
            "📥 <b>How to Download:</b>\n\n"
            "Send a public video link directly into the chat, or use:\n"
            "<code>/download &lt;url&gt;</code>\n\n"
            "<b>Supported Platforms:</b>\n"
            "• 🔴 YouTube (Videos & Shorts)\n"
            "• 📸 Instagram (Reels & Videos)\n"
            "• 🔵 Facebook (Videos & Reels)\n"
            "• 🌐 TikTok, X, Reddit, Pinterest, Vimeo, Twitch"
        )
        await message.reply(guide_text)


# =====================================================================
# 4. /settings
# =====================================================================
@start_router.message(Command("settings", ignore_case=True))
async def command_settings_handler(message: Message) -> None:
    """Displays and allows editing personal preferences."""
    user_id = message.from_user.id if message.from_user else 0
    logger.info(f"/settings from user {user_id}")
    text, kb = await _render_settings_view(user_id)
    await message.answer(text, reply_markup=kb)


# =====================================================================
# 5. /history
# =====================================================================
@start_router.message(Command("history", ignore_case=True))
async def command_history_handler(message: Message) -> None:
    """Displays the user's personal download history."""
    user_id = message.from_user.id if message.from_user else 0
    logger.info(f"/history from user {user_id}")
    history = await db.get_user_history(user_id, limit=5)
    if not history:
        await message.answer(
            "📥 <b>Your Download History</b>\n\n"
            "You have no recorded downloads yet. Send any public video link to get started!"
        )
        return

    lines = ["📥 <b>Your Recent Downloads:</b>\n"]
    for item in history:
        time_str = _format_time_ago(item["created_at"])
        raw_title = item.get("title") or "Video"
        if len(raw_title) > 40:
            raw_title = raw_title[:37] + "..."
        title = html.escape(raw_title)
        plat = html.escape((item.get("platform") or "media").capitalize())
        qual = html.escape(item.get("quality") or "best")
        lines.append(f"• <b>{title}</b>\n  └ <i>{plat} · {qual} · {time_str}</i>")

    builder = InlineKeyboardBuilder()
    builder.button(text="🗑 Clear History", callback_data="menu:clear_prompt")
    await message.answer("\n".join(lines), reply_markup=builder.as_markup())


# =====================================================================
# 6. /clear
# =====================================================================
@start_router.message(Command("clear", ignore_case=True))
async def command_clear_handler(message: Message) -> None:
    """Prompts for confirmation before clearing user's history."""
    logger.info(f"/clear from user {message.from_user.id if message.from_user else 0}")
    confirm_text = (
        "⚠️ <b>Clear Download History</b>\n\n"
        "Are you sure you want to clear your personal download history?\n\n"
        "<i>Note: This only deletes your personal history records. "
        "Global cache and shared media files will remain intact.</i>"
    )
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Yes, Clear History", callback_data="menu:clear_confirm")
    builder.button(text="❌ Cancel", callback_data="menu:clear_cancel")
    builder.adjust(1, 1)
    await message.answer(confirm_text, reply_markup=builder.as_markup())


# =====================================================================
# 7. /cancel
# =====================================================================
@start_router.message(Command("cancel", ignore_case=True))
async def command_cancel_handler(message: Message) -> None:
    """Cancels the requesting user's active download job."""
    user_id = message.from_user.id if message.from_user else 0
    logger.info(f"/cancel from user {user_id}")
    active_job = job_queue.get_user_active_job(user_id)
    if active_job:
        cancel_download(active_job.job_id)
        job_queue.cancel_job(active_job.job_id)
        plat_name = html.escape(active_job.platform.capitalize())
        await message.reply(
            f"🛑 Your active download for <b>{plat_name}</b> has been cancelled."
        )
    else:
        await message.reply("ℹ️ You do not have any active download in progress.")


# =====================================================================
# 8. /status
# =====================================================================
@start_router.message(Command("status", ignore_case=True))
async def command_status_handler(message: Message) -> None:
    """Shows the requesting user's active download status."""
    user_id = message.from_user.id if message.from_user else 0
    logger.info(f"/status from user {user_id}")
    active_job = job_queue.get_user_active_job(user_id)
    if active_job:
        status_text = (
            "⏳ <b>Active Download Status</b>\n\n"
            f"• <b>Platform:</b> {html.escape(active_job.platform.capitalize())}\n"
            f"• <b>Stage:</b> {html.escape(active_job.status.value.capitalize())}\n"
            f"• <b>Progress:</b> {html.escape(active_job.progress_percent)}\n"
            f"• <b>Speed:</b> {html.escape(active_job.speed)}\n"
            f"• <b>ETA:</b> {html.escape(active_job.eta)}\n"
            f"• <b>Elapsed:</b> {int(active_job.duration_s())}s\n\n"
            "Use /cancel to abort this download."
        )
        await message.reply(status_text)
    else:
        await message.reply("ℹ️ No active download in progress. Send any public video link to start!")


# =====================================================================
# 9. /about
# =====================================================================
@start_router.message(Command("about", ignore_case=True))
async def command_about_handler(message: Message) -> None:
    """Sends concise bot description and architecture info."""
    logger.info(f"/about from user {message.from_user.id if message.from_user else 0}")
    about_text = (
        "⚡ <b>ɪʙᴇᴀᴛᴢ bot</b> (@ibeatzfetch_bot)\n\n"
        "A high-performance Telegram media downloader built with Python, aiogram 3, yt-dlp, and FFmpeg.\n\n"
        "<b>Core Architecture:</b>\n"
        "• YouTube, Instagram & Facebook prioritized pipelines\n"
        "• Multi-layer SQLite caching (instant ~0.1s telegram_file_id re-delivery)\n"
        "• Singleflight request coalescing (prevents duplicate downloads)\n"
        "• Lossless stream-copy remuxing (zero re-encoding)\n"
        "• On-demand high-fidelity MP3 extraction\n"
        "• Group and supergroup friendly"
    )
    await message.answer(about_text)


# =====================================================================
# 10. /privacy
# =====================================================================
@start_router.message(Command("privacy", ignore_case=True))
async def command_privacy_handler(message: Message) -> None:
    """Displays transparent privacy policy."""
    logger.info(f"/privacy from user {message.from_user.id if message.from_user else 0}")
    privacy_text = (
        "🔒 <b>Privacy & Data Notice</b>\n\n"
        "• <b>Media URLs:</b> Links sent to the bot are processed exclusively to retrieve and deliver requested media files.\n"
        "• <b>Caching:</b> Telegram <code>file_id</code> references and metadata are stored in a local SQLite database to accelerate repeat deliveries and avoid re-downloading identical media.\n"
        "• <b>User Data:</b> Only your Telegram user ID and download history are retained for personal history tracking. You can clear your records at any time using /clear.\n"
        "• <b>Disk Storage:</b> Temporary media files on server disk are isolated per job and deleted automatically.\n"
        "• <b>Security:</b> No user credentials, cookies, or chat messages are recorded or shared with third parties."
    )
    await message.answer(privacy_text)


# =====================================================================
# Interactive Start Menu Callback Handlers
# =====================================================================
@start_router.callback_query(F.data.startswith("menu:"))
async def menu_callback_handler(callback: CallbackQuery) -> None:
    """Handles all interactive start menu navigation buttons."""
    user = callback.from_user
    user_id = user.id if user else 0
    raw_name = user.first_name if user else "there"
    user_name = html.escape(raw_name)
    action = callback.data.split(":")[1]

    if action == "start":
        await callback.answer()
        welcome_text = (
            f"👋 Hi <b>{user_name}</b>!\n\n"
            "⚡ <b>ɪʙᴇᴀᴛᴢ bot</b> (@ibeatzfetch_bot)\n"
            "High-performance media downloader for Telegram.\n\n"
            "Send any supported public video URL directly to begin downloading.\n\n"
            "• <b>Primary:</b> YouTube, Shorts, Instagram Reels, Facebook Videos\n"
            "• <b>Secondary:</b> TikTok, X, Reddit, Pinterest, Twitch, Vimeo\n\n"
            "<i>Works in private chats, groups, and supergroups.</i>"
        )
        if callback.message:
            await callback.message.edit_text(welcome_text, reply_markup=_build_start_keyboard())

    elif action == "how_to_use":
        await callback.answer()
        text = (
            "📖 <b>How to Use ɪʙᴇᴀᴛᴢ:</b>\n\n"
            "1. <b>Send a Link:</b> Paste any public video link into the chat, or send <code>/download &lt;url&gt;</code>.\n"
            "2. <b>Automatic Quality:</b> Instagram and Facebook videos download in the highest available quality automatically.\n"
            "3. <b>Quality Choice:</b> YouTube videos with 1080p+ present resolution options (1080p, 720p, 480p, 360p).\n"
            "4. <b>Audio Extraction:</b> Tap <code>[🎵 Audio]</code> on any delivered video to get a high-quality MP3.\n"
            "5. <b>Cancel Anytime:</b> Use <code>/cancel</code> during download to stop an in-flight job."
        )
        if callback.message:
            await callback.message.edit_text(text, reply_markup=_build_back_keyboard())

    elif action == "platforms":
        await callback.answer()
        text = (
            "🌐 <b>Supported Platforms:</b>\n\n"
            "<b>Primary Platforms:</b>\n"
            "• 🔴 <b>YouTube:</b> Videos, Shorts, Music\n"
            "• 📸 <b>Instagram:</b> Reels, Videos, Posts, Carousels\n"
            "• 🔵 <b>Facebook:</b> Watch, Reels, Public Videos\n\n"
            "<b>Secondary Platforms:</b>\n"
            "• 🎵 TikTok (Watermark-free)\n"
            "• 🐦 Twitter / X Videos\n"
            "• 🤖 Reddit Videos\n"
            "• 📌 Pinterest Videos\n"
            "• 🎮 Twitch Clips\n"
            "• 🎬 Vimeo & SoundCloud\n\n"
            "<i>Note: Media must be public. Private or restricted posts cannot be downloaded.</i>"
        )
        if callback.message:
            await callback.message.edit_text(text, reply_markup=_build_back_keyboard())

    elif action == "help":
        await callback.answer()
        help_text = (
            "ℹ️ <b>ɪʙᴇᴀᴛᴢ Quick Help</b>\n\n"
            "• Send any public video link to download.\n"
            "• YouTube $\\ge$ 1080p offers quality selection.\n"
            "• Instagram & Facebook auto-select highest quality.\n"
            "• /download - Download with URL\n"
            "• /status - Check active download\n"
            "• /cancel - Cancel active download\n"
            "• /history - View download history\n"
            "• /settings - Personal preferences"
        )
        if callback.message:
            await callback.message.edit_text(help_text, reply_markup=_build_back_keyboard())

    elif action == "history":
        await callback.answer()
        history = await db.get_user_history(user_id, limit=5)
        if not history:
            text = (
                "📥 <b>Your Download History</b>\n\n"
                "You have no recorded downloads yet. Send any public video link to get started!"
            )
            if callback.message:
                await callback.message.edit_text(text, reply_markup=_build_back_keyboard())
            return

        lines = ["📥 <b>Your Recent Downloads:</b>\n"]
        for item in history:
            time_str = _format_time_ago(item["created_at"])
            raw_title = item.get("title") or "Video"
            if len(raw_title) > 35:
                raw_title = raw_title[:32] + "..."
            title = html.escape(raw_title)
            plat = html.escape((item.get("platform") or "media").capitalize())
            qual = html.escape(item.get("quality") or "best")
            lines.append(f"• <b>{title}</b>\n  └ <i>{plat} · {qual} · {time_str}</i>")

        builder = InlineKeyboardBuilder()
        builder.button(text="🗑 Clear History", callback_data="menu:clear_prompt")
        builder.button(text="« Back to Menu", callback_data="menu:start")
        builder.adjust(1, 1)
        if callback.message:
            await callback.message.edit_text("\n".join(lines), reply_markup=builder.as_markup())

    elif action == "settings":
        await callback.answer()
        text, kb = await _render_settings_view(user_id)
        if callback.message:
            await callback.message.edit_text(text, reply_markup=kb)

    elif action == "set_q":
        new_q = callback.data.split(":")[2]
        await db.set_user_setting(user_id, "preferred_quality", new_q)
        await callback.answer(f"Quality preference updated to {new_q.capitalize()}")
        text, kb = await _render_settings_view(user_id)
        if callback.message:
            await callback.message.edit_text(text, reply_markup=kb)

    elif action == "set_aud":
        new_aud = callback.data.split(":")[2]
        await db.set_user_setting(user_id, "auto_audio", new_aud)
        await callback.answer(f"Auto-Audio set to {new_aud.upper()}")
        text, kb = await _render_settings_view(user_id)
        if callback.message:
            await callback.message.edit_text(text, reply_markup=kb)

    elif action == "clear_prompt":
        await callback.answer()
        confirm_text = (
            "⚠️ <b>Clear Download History</b>\n\n"
            "Are you sure you want to clear your personal download history?\n\n"
            "<i>Note: This only deletes your personal history records. "
            "Global cache and shared media files will remain intact.</i>"
        )
        builder = InlineKeyboardBuilder()
        builder.button(text="✅ Yes, Clear History", callback_data="menu:clear_confirm")
        builder.button(text="❌ Cancel", callback_data="menu:history")
        builder.adjust(1, 1)
        if callback.message:
            await callback.message.edit_text(confirm_text, reply_markup=builder.as_markup())

    elif action == "clear_confirm":
        await db.clear_user_history(user_id)
        await callback.answer("History cleared!")
        text = "✅ Your download history has been cleared."
        if callback.message:
            await callback.message.edit_text(text, reply_markup=_build_back_keyboard())

    elif action == "clear_cancel":
        await callback.answer("Cancelled.")
        text, kb = await _render_settings_view(user_id)
        if callback.message:
            await callback.message.edit_text(text, reply_markup=kb)
