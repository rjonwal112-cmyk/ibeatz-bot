import datetime
import logging
from typing import Any
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import config
from config import ADMIN_IDS
from services.cache import media_cache
from services.database import db
from services.queue_manager import job_queue
from services.stats import (
    get_stats_summary,
    is_maintenance_mode,
    set_maintenance_mode,
)

logger = logging.getLogger(__name__)

admin_router = Router(name="admin_router")
print("ADMIN ROUTER LOADED")


def is_admin(user_id: int) -> bool:
    """Checks if the given user is authorized as an administrator."""
    return user_id in config.ADMIN_IDS and user_id != 0


def _build_admin_keyboard() -> Any:
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Stats", callback_data="admin:stats")
    builder.button(text="⏱ Perf", callback_data="admin:perf")
    builder.button(text="💾 Cache", callback_data="admin:cache")
    builder.button(text="🛠 Maint", callback_data="admin:maint")
    builder.button(text="⚠️ Errors", callback_data="admin:errors")
    builder.adjust(2, 2, 1)
    return builder.as_markup()


@admin_router.message(Command("admin", ignore_case=True))
async def admin_dashboard_handler(message: Message) -> None:
    """Displays the administrative control dashboard."""
    user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id):
        return

    stats = await get_stats_summary()
    mode_str = "🔴 Maintenance Active" if stats["maintenance_mode"] else "🟢 Normal Operational"
    queue_stats = job_queue.get_stats()

    dashboard = (
        "🛠 <b>ɪʙᴇᴀᴛᴢ Administrator Dashboard</b>\n\n"
        f"• <b>Status:</b> {mode_str}\n"
        f"• <b>Active Queue Jobs:</b> {queue_stats['active_jobs']}\n"
        f"• <b>Total Downloads:</b> {stats['total_downloads']}\n"
        f"• <b>Cache Hits:</b> {stats['cache_hits']} ({stats['hit_ratio']})\n"
        f"• <b>Disk Free:</b> {stats['disk_free_gb']} / {stats['disk_total_gb']}\n\n"
        "<b>Admin Commands:</b>\n"
        "• /stats — Operational metrics\n"
        "• /perf — Latency breakdown\n"
        "• /cache — Cache inspection & cleanup\n"
        "• /maintenance — Toggle maintenance mode\n"
        "• /errors — System error logs"
    )
    await message.answer(dashboard, reply_markup=_build_admin_keyboard())


@admin_router.callback_query(F.data.startswith("admin:"))
async def admin_callback_handler(callback: CallbackQuery) -> None:
    """Handles admin dashboard button clicks."""
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("⚠️ Unauthorized", show_alert=True)
        return

    action = callback.data.split(":")[1]
    await callback.answer()

    if not callback.message:
        return

    if action == "stats":
        await admin_stats_handler(callback.message)
    elif action == "perf":
        await admin_perf_handler(callback.message)
    elif action == "cache":
        await admin_cache_handler(callback.message)
    elif action == "maint":
        await callback.message.answer(
            _format_maintenance_text(),
            reply_markup=_build_maintenance_keyboard(),
        )
    elif action == "errors":
        await admin_errors_handler(callback.message)


@admin_router.message(Command("stats", ignore_case=True))
async def admin_stats_handler(message: Message) -> None:
    """Outputs operational metrics for admin."""
    user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id):
        return

    stats = await get_stats_summary()
    mode_str = "🔴 Active" if stats["maintenance_mode"] else "🟢 Normal"
    perf = stats.get("perf_summary", {})
    queue_stats = job_queue.get_stats()

    report = (
        "📊 <b>ɪʙᴇᴀᴛᴢ bot Operational Statistics</b>\n\n"
        f"• <b>Status:</b> {mode_str}\n"
        f"• <b>Uptime:</b> {stats['uptime']}\n"
        f"• <b>Total Requests:</b> {stats['total_requests']}\n"
        f"• <b>Total Downloads:</b> {stats['total_downloads']}\n"
        f"• <b>Successful:</b> {stats['successful_downloads']}\n"
        f"• <b>Failed:</b> {stats['failed_downloads']}\n"
        f"• <b>Cache Hits:</b> {stats['cache_hits']} ({stats['hit_ratio']})\n"
        f"• <b>Audio Extractions:</b> {stats['audio_extractions']}\n"
        f"• <b>Active Jobs:</b> {queue_stats['active_jobs']}\n"
        f"• <b>Free Disk:</b> {stats['disk_free_gb']} / {stats['disk_total_gb']}\n"
        f"• <b>Recorded Deliveries:</b> {perf.get('total_deliveries', 0)}"
    )
    await message.answer(report)


@admin_router.message(Command("perf", ignore_case=True))
async def admin_perf_handler(message: Message) -> None:
    """Outputs latency breakdown across delivery stages and identifies bottlenecks."""
    user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id):
        return

    perf = await db.get_perf_summary()
    recent = await db.get_recent_metrics(limit=1)
    total = perf.get("total_deliveries", 0)

    if total == 0:
        await message.answer("⏱ <b>Performance Telemetry:</b> No media deliveries recorded yet.")
        return

    latest_str = ""
    if recent:
        r = recent[0]
        sz_mb = (r.get("file_size") or 0) / (1024 * 1024)
        dl_s = r.get("download_s") or 0.01
        up_s = r.get("upload_s") or 0.01
        dl_mbps = (sz_mb / dl_s) if dl_s > 0 else 0.0
        up_mbps = (sz_mb / up_s) if up_s > 0 else 0.0
        cache_status = "HIT" if r.get("is_cache_hit") else "MISS"
        latest_str = (
            "\n\n<b>Latest Delivery:</b>\n"
            f"• Platform: <code>{r.get('platform')}</code> | Quality: <code>{r.get('quality')}</code>\n"
            f"• Cache: <code>{cache_status}</code>\n"
            f"• File Size: {sz_mb:.2f} MB\n"
            f"• 🔍 Format / Extraction: {r.get('extraction_s', 0.0):.2f}s\n"
            f"• ⬇️ Download: {r.get('download_s', 0.0):.2f}s ({dl_mbps:.1f} MB/s)\n"
            f"• ⚙️ FFmpeg Remux: {r.get('ffmpeg_s', 0.0):.2f}s\n"
            f"• 📤 Telegram Upload: {r.get('upload_s', 0.0):.2f}s ({up_mbps:.1f} MB/s)\n"
            f"• 🏁 <b>TOTAL Delivery: {r.get('total_s', 0.0):.2f}s</b>\n"
            f"• Bottleneck: {r.get('bottleneck', 'None')}"
        )

    report = (
        "⏱ <b>ɪʙᴇᴀᴛᴢ Delivery Performance Breakdown</b>\n\n"
        f"• <b>Deliveries Analyzed:</b> {total}\n"
        f"• <b>Cache Hits:</b> {perf.get('cache_hits', 0)}\n"
        f"• <b>Avg Cached Delivery:</b> {perf.get('avg_cached_total', 0.0):.2f}s\n\n"
        "<b>Non-Cached Pipeline Stages (Averages):</b>\n"
        f"• 🔍 <b>Format Extraction:</b> {perf.get('avg_extraction', 0.0):.2f}s\n"
        f"• ⬇️ <b>Source Download:</b>    {perf.get('avg_download', 0.0):.2f}s\n"
        f"• ⚙️ <b>FFmpeg Remuxing:</b>    {perf.get('avg_ffmpeg', 0.0):.2f}s\n"
        f"• 📤 <b>Telegram Upload:</b>    {perf.get('avg_upload', 0.0):.2f}s\n"
        f"• 🏁 <b>Total Delivery:</b>     {perf.get('avg_total', 0.0):.2f}s\n\n"
        f"🎯 <b>Primary Bottleneck:</b> {perf.get('top_bottleneck', 'None')}"
        f"{latest_str}"
    )
    await message.answer(report)


@admin_router.message(Command("cache", ignore_case=True))
async def admin_cache_handler(message: Message) -> None:
    """Displays cache usage and triggers cleanup."""
    user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id):
        return

    status_msg = await message.answer("🧹 Inspecting and cleaning cache…")
    evicted = await media_cache.cleanup(retention_seconds=86400 * 3)

    with db._get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) as count, SUM(file_size) as total_size FROM media_cache;")
        row = cursor.fetchone()
        count = row["count"] if row else 0
        total_mb = (row["total_size"] or 0) / (1024 * 1024)

    report = (
        "💾 <b>Smart Media Cache Status</b>\n\n"
        f"• <b>Cached Items in DB:</b> {count}\n"
        f"• <b>Total Cache Disk Size:</b> {total_mb:.2f} MB\n"
        f"• <b>Files Evicted in Run:</b> {evicted}\n"
        f"• <b>Capacity Limit:</b> {media_cache.max_bytes // (1024*1024)} MB"
    )
    await status_msg.edit_text(report)


def _build_maintenance_keyboard() -> Any:
    """Builds inline keyboard for maintenance management."""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Enable Maintenance", callback_data="maint:enable")
    builder.button(text="❌ Disable Maintenance", callback_data="maint:disable")
    builder.button(text="ℹ️ Status", callback_data="maint:status")
    builder.adjust(2, 1)
    return builder.as_markup()


def _format_maintenance_text() -> str:
    """Formats current maintenance state message."""
    active = is_maintenance_mode()
    state_str = "🔴 <b>ENABLED</b>" if active else "🟢 <b>DISABLED</b>"
    desc_str = (
        "• <b>Status:</b> Regular user downloads are <b>BLOCKED</b>.\n"
        "• <b>Admin Access:</b> Admins can still test and download media.\n"
        "• <b>Queue:</b> New normal-user jobs are rejected before queueing."
        if active
        else "• <b>Status:</b> All normal downloads and features are <b>ACTIVE</b>."
    )
    return (
        "🛠 <b>Bot Maintenance Control Panel</b>\n\n"
        f"• <b>Current State:</b> {state_str}\n"
        f"{desc_str}\n\n"
        "<i>Tap a button below to update or refresh maintenance state:</i>"
    )


@admin_router.message(Command("maintenance", ignore_case=True))
async def admin_maintenance_handler(message: Message) -> None:
    """Shows current maintenance status and controls (Admin only)."""
    user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id):
        chat_id = message.chat.id if getattr(message, "chat", None) else None
        logger.warning(f"Unauthorized /maintenance attempt user_id={user_id} chat_id={chat_id}")
        await message.reply("⛔ This command is restricted to bot administrators.")
        return

    text = (message.text or "").strip().lower()
    parts = text.split()
    if len(parts) > 1:
        arg = parts[1]
        if arg in ("on", "enable", "start", "1", "true"):
            if not is_maintenance_mode():
                await set_maintenance_mode(True)
                logger.info(f"MAINTENANCE ENABLED by {user_id}")
            else:
                logger.info(f"MAINTENANCE ENABLED by {user_id} (already active)")
        elif arg in ("off", "disable", "stop", "0", "false"):
            if is_maintenance_mode():
                await set_maintenance_mode(False)
                logger.info(f"MAINTENANCE DISABLED by {user_id}")
            else:
                logger.info(f"MAINTENANCE DISABLED by {user_id} (already inactive)")

    await message.answer(
        _format_maintenance_text(),
        reply_markup=_build_maintenance_keyboard(),
    )


@admin_router.callback_query(F.data.startswith("maint:"))
async def maintenance_callback_handler(callback: CallbackQuery) -> None:
    """Handles maintenance inline keyboard actions (Admin only)."""
    user_id = callback.from_user.id
    if not is_admin(user_id):
        chat_id = callback.message.chat.id if callback.message and getattr(callback.message, "chat", None) else None
        logger.warning(f"Unauthorized /maintenance attempt user_id={user_id} chat_id={chat_id}")
        await callback.answer("⛔ Unauthorized: Admin access required.", show_alert=True)
        return

    parts = callback.data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "enable":
        if not is_maintenance_mode():
            await set_maintenance_mode(True)
            logger.info(f"MAINTENANCE ENABLED by {user_id}")
            toast = "✅ Maintenance mode ENABLED."
        else:
            toast = "ℹ️ Maintenance mode is already ENABLED."
    elif action == "disable":
        if is_maintenance_mode():
            await set_maintenance_mode(False)
            logger.info(f"MAINTENANCE DISABLED by {user_id}")
            toast = "❌ Maintenance mode DISABLED."
        else:
            toast = "ℹ️ Maintenance mode is already DISABLED."
    elif action == "status":
        curr = "ENABLED" if is_maintenance_mode() else "DISABLED"
        toast = f"ℹ️ Current State: {curr}"
    else:
        toast = "Unknown action."

    await callback.answer(toast, show_alert=False)
    if callback.message:
        try:
            await callback.message.edit_text(
                _format_maintenance_text(),
                reply_markup=_build_maintenance_keyboard(),
            )
        except Exception:
            pass


@admin_router.message(Command("errors", ignore_case=True))
async def admin_errors_handler(message: Message) -> None:
    """Displays the most recent recorded operational errors."""
    user_id = message.from_user.id if message.from_user else 0
    if not is_admin(user_id):
        return

    errors = await db.get_recent_errors(limit=8)
    if not errors:
        await message.answer("✅ <b>Error Monitor:</b> No recent errors recorded in SQLite.")
        return

    lines = ["⚠️ <b>Recent Operational Errors:</b>\n"]
    for err in errors:
        dt = datetime.datetime.fromtimestamp(err["timestamp"]).strftime("%m-%d %H:%M:%S")
        lines.append(
            f"• <code>[{dt}]</code> <b>{err.get('platform', 'unknown').upper()}</b>: "
            f"<code>{err.get('error_code', 'ERR')}</code>\n"
            f"  <i>{err.get('error_message', '')[:100]}</i>"
        )

    await message.answer("\n".join(lines))


# ── Runtime diagnostics stored in module-level vars ──
_last_group_update_time: float = 0.0
_last_group_chat_id: int = 0
_last_group_url: str = "(none yet)"


def record_group_update(chat_id: int) -> None:
    """Called by media handler to record last group update reception."""
    global _last_group_update_time, _last_group_chat_id
    import time
    _last_group_update_time = time.time()
    _last_group_chat_id = chat_id


def record_group_url(url: str) -> None:
    """Called by media handler to record last detected group URL."""
    global _last_group_url
    _last_group_url = url


