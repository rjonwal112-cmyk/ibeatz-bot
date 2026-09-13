import asyncio
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Optional


if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.enums import ParseMode
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    MenuButtonCommands,
    MenuButtonDefault,
)

from config import (
    ADMIN_IDS,
    BASE_DOWNLOAD_DIR,
    BOT_LOCAL_SERVER_URL,
    CACHE_DIR,
    IS_LOCAL_SERVER,
    MAX_FILE_SIZE_BYTES,
    get_bot_token,
)
from handlers import (
    admin_router,
    echo_router,
    media_router,
    start_router,
)
from services import (
    db,
    init_stats,
    job_queue,
    log_startup_system_report,
    media_cache,
)
from downloader import (
    cancel_all_active_downloads,
    cleanup_orphaned_temp_dirs,
)
from flask import Flask
from werkzeug.serving import make_server

logger = logging.getLogger(__name__)

# ==============================================================================
# RENDER HEALTH & LIFECYCLE FLASK SERVER (PORT 10000)
# ==============================================================================
flask_app = Flask(__name__)
BOT_START_TIME: float = time.time()
RECEIVED_SIGNAL: Optional[str] = None
_shutdown_done: bool = False


@flask_app.route("/")
def index():
    uptime_s = int(time.time() - BOT_START_TIME)
    stats = job_queue.get_stats()
    return (
        "<h1>⚡ ɪʙᴇᴀᴛᴢ Media Bot is Online</h1>"
        f"<p>Status: Healthy | Uptime: {uptime_s}s | Active jobs: {stats['active_jobs']} | Queued: {stats['queue_length']}</p>",
        200,
    )


@flask_app.route("/health")
def health():
    stats = job_queue.get_stats()
    uptime_s = int(time.time() - BOT_START_TIME)
    return {
        "status": "healthy",
        "bot": "@ibeatzfetch_bot",
        "uptime_seconds": uptime_s,
        "active_jobs": stats["active_jobs"],
        "queue_length": stats["queue_length"],
        "max_concurrent": stats["max_concurrent"],
    }, 200


class FlaskServerThread(threading.Thread):
    """Dedicated thread for Werkzeug HTTP server supporting clean programmatic shutdown."""

    def __init__(self, app: Flask, host: str = "0.0.0.0", port: int = 10000):
        super().__init__(daemon=True)
        import logging as py_logging

        py_logging.getLogger("werkzeug").setLevel(py_logging.WARNING)
        self.server = make_server(host, port, app, threaded=True)
        self.port = port

    def run(self):
        logger.info(f"🌐 Flask health server listening on port {self.port}...")
        try:
            self.server.serve_forever()
        except Exception as e:
            logger.debug(f"Flask server stopped: {e}")

    def shutdown(self):
        if self.server:
            try:
                logger.info("🌐 Shutting down Flask health server...")
                self.server.shutdown()
                logger.info("🌐 Flask health server stopped cleanly.")
            except Exception as e:
                logger.debug(f"Flask server shutdown notice: {e}")


def start_flask_server() -> FlaskServerThread:
    """Starts the Flask health server in background with clean shutdown capability."""
    port = int(os.getenv("PORT", 10000))
    server_thread = FlaskServerThread(flask_app, host="0.0.0.0", port=port)
    server_thread.start()
    return server_thread


async def perform_graceful_shutdown(
    reason: str,
    bot: Optional[Bot],
    cleanup_task: Optional[asyncio.Task],
    server_thread: Optional[FlaskServerThread],
) -> None:
    """
    Executes orderly shutdown sequence:
    1. Cancels background cache cleanup task
    2. Drains & cancels active/queued jobs, releasing concurrency slots
    3. Signals downloader cancellation to abort in-flight transfers
    4. Cleans up temporary download directories safely
    5. Checkpoints & flushes SQLite WAL database
    6. Closes Telegram bot session
    7. Shuts down Flask HTTP server
    """
    global _shutdown_done
    if _shutdown_done:
        return
    _shutdown_done = True

    logger.info(
        f"\n============================================================\n"
        f"🛑 [SHUTDOWN: {reason}] Initiating complete graceful shutdown...\n"
        f"============================================================"
    )

    # 1. Cancel background cache cleanup task
    if cleanup_task and not cleanup_task.done():
        cleanup_task.cancel()
        logger.info("  [1/7] Background cache cleanup task cancelled.")

    # 2. Cancel and drain all queue jobs & release concurrency slots
    try:
        await job_queue.shutdown()
        logger.info("  [2/7] Queue manager drained: active=0, queued=0, slots released.")
    except Exception as q_err:
        logger.warning(f"  [2/7] Queue shutdown warning: {q_err}")

    # 3. Signal active downloads to cancel
    try:
        cancelled_active = cancel_all_active_downloads()
        logger.info(f"  [3/7] Downloader cancellation signalled ({cancelled_active} active).")
    except Exception as dl_err:
        logger.warning(f"  [3/7] Downloader cancel warning: {dl_err}")

    # 4. Clean up any leftover temporary download directories
    try:
        cleaned_count = cleanup_orphaned_temp_dirs()
        logger.info(f"  [4/7] Cleaned up {cleaned_count} temporary directories.")
    except Exception as c_err:
        logger.warning(f"  [4/7] Temp dir cleanup warning: {c_err}")

    # 5. Flush and close SQLite WAL
    try:
        await db.shutdown()
        logger.info("  [5/7] SQLite WAL database checkpointed and closed cleanly.")
    except Exception as db_err:
        logger.warning(f"  [5/7] Database shutdown warning: {db_err}")

    # 6. Close Telegram session
    if bot and bot.session:
        try:
            await bot.session.close()
            logger.info("  [6/7] Telegram Bot session closed.")
        except Exception as s_err:
            logger.warning(f"  [6/7] Bot session close warning: {s_err}")

    # 7. Stop Flask server
    if server_thread:
        server_thread.shutdown()
        logger.info("  [7/7] Flask HTTP server stopped.")

    logger.info(
        f"\n============================================================\n"
        f"✅ [SHUTDOWN COMPLETE: {reason}]\n"
        f"All resources released cleanly. No phantom active jobs or file locks remain.\n"
        f"============================================================"
    )


async def _periodic_cache_cleanup() -> None:
    """Background task to clean expired cache entries every hour."""
    while True:
        try:
            await asyncio.sleep(3600)
            await media_cache.cleanup(retention_seconds=86400 * 3)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Periodic cache cleanup error: {e}")


async def main() -> None:
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    logger.info(
        f"\n============================================================\n"
        f"🚀 [STARTUP: BOOT/RESTART] ɪʙᴇᴀᴛᴢ Media Bot initializing...\n"
        f"  • PID: {os.getpid()}\n"
        f"  • Max Concurrent Downloads: {job_queue.max_concurrent}\n"
        f"============================================================"
    )

    # Validate and retrieve BOT_TOKEN from environment
    BOT_TOKEN = get_bot_token()

    # Ensure storage directories exist
    BASE_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Reset in-memory queue to guarantee zero phantom active jobs
    job_queue.reset_on_startup()

    # 2. Cleanup orphaned temp directories left by any previous abrupt termination
    cleaned_dirs = cleanup_orphaned_temp_dirs()
    if cleaned_dirs:
        logger.info(f"🧹 [STARTUP] Removed {cleaned_dirs} orphaned temp directories from previous session.")

    # 3. Initialize SQLite database and telemetry
    await db.initialize()
    await init_stats()

    # 4. Run FFmpeg environment diagnostics
    log_startup_system_report()

    # Configure Telegram Bot API Session (Cloud or Local Server)
    if BOT_LOCAL_SERVER_URL:
        api_server = TelegramAPIServer.from_base(BOT_LOCAL_SERVER_URL, is_local=True)
        session = AiohttpSession(api=api_server, timeout=600)
        logger.info(
            f"🚀 Connected to Local Bot API Server: {BOT_LOCAL_SERVER_URL} "
            f"(2,000 MB / 2 GB upload limit active)"
        )
    else:
        session = AiohttpSession(timeout=300)
        logger.info(f"🌐 Standard Telegram Cloud API active ({MAX_FILE_SIZE_BYTES // (1024*1024)} MB upload limit)")

    bot = Bot(
        token=BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    # Register routers in priority order
    dp.include_router(start_router)
    dp.include_router(admin_router)
    dp.include_router(media_router)
    dp.include_router(echo_router)
    print("ALL ROUTERS REGISTERED")

    # Diagnostic Update Outer Middleware
    @dp.update.outer_middleware()
    async def log_update_middleware(handler, event, data):
        msg = event.message or event.edited_message
        if msg:
            chat_type = msg.chat.type
            user_info = f"{msg.from_user.id} (@{msg.from_user.username})" if msg.from_user else "unknown"
            text_preview = (msg.text or msg.caption or f"<{event.event_type}>")[:60]
            logger.info(f"📥 [UPDATE #{event.update_id}] Chat={msg.chat.id} ({chat_type}) User={user_info} Content={text_preview!r}")
        else:
            logger.info(f"📥 [UPDATE #{event.update_id}] EventType={event.event_type}")
        return await handler(event, data)

    # Verify Telegram authentication
    try:
        bot_user = await bot.get_me()
        logger.info(
            f"Telegram authentication: SUCCESS\n"
            f"Bot username: @{bot_user.username}\n"
            f"Bot ID: {bot_user.id}"
        )
        logger.info(
            f"GROUP ACCESS CHECK: can_read_all_group_messages="
            f"{getattr(bot_user, 'can_read_all_group_messages', None)}"
        )
    except Exception as conn_err:
        logger.error(f"❌ Telegram authentication FAILED: Invalid or expired bot token. Error: {conn_err}")
        await bot.session.close()
        sys.exit(1)

    # Register bot menu commands in Telegram UI with proper scopes
    user_commands = [
        BotCommand(command="start", description="Start the bot & main menu"),
        BotCommand(command="help", description="How to use & feature guide"),
        BotCommand(command="download", description="Download media from URL"),
        BotCommand(command="settings", description="Personal preferences"),
        BotCommand(command="history", description="View your recent downloads"),
        BotCommand(command="clear", description="Clear your download history"),
        BotCommand(command="cancel", description="Cancel your active download"),
        BotCommand(command="status", description="Check active download status"),
        BotCommand(command="about", description="About ɪʙᴇᴀᴛᴢ bot"),
        BotCommand(command="privacy", description="Privacy and data notice"),
    ]

    admin_commands = user_commands + [
        BotCommand(command="admin", description="Admin control dashboard"),
        BotCommand(command="stats", description="Operational metrics"),
        BotCommand(command="perf", description="Delivery latency breakdown"),
        BotCommand(command="cache", description="Cache management & cleanup"),
        BotCommand(command="maintenance", description="Toggle maintenance mode"),
        BotCommand(command="errors", description="Recent operational errors"),
    ]

    # Print comprehensive process and runtime diagnostics
    registered_router_names = [r.name for r in dp.sub_routers]
    registered_command_names = [f"/{c.command}" for c in user_commands]
    print("=" * 60)
    print("RUNTIME PROCESS & ENVIRONMENT DIAGNOSTICS:")
    print(f"  • Python Executable:  {sys.executable}")
    print(f"  • Working Directory:  {os.getcwd()}")
    print(f"  • Project Root Path:  {Path(__file__).resolve().parent}")
    print(f"  • Process ID (PID):   {os.getpid()}")
    print(f"  • Bot Username:       @{bot_user.username}")
    print(f"  • Registered Routers: {registered_router_names}")
    print(f"  • Registered Commands:{registered_command_names}")
    print("=" * 60)
    print("⚠️  TELEGRAM GROUP PRIVACY MODE NOTICE:")
    print("  Telegram bots have Privacy Mode enabled by default by Telegram.")
    print(f"  To allow @{bot_user.username} to auto-detect media URLs in groups without")
    print("  requiring a bot mention or slash command:")
    print("    1. Open @BotFather in Telegram")
    print("    2. Send /setprivacy")
    print(f"    3. Select @{bot_user.username}")
    print("    4. Select 'Disable'")
    print(f"  Alternatively, grant @{bot_user.username} Administrator status in the group.")
    print("=" * 60)

    try:
        # 1. Reset & Register Default User Commands (Clear stale commands first)
        await bot.delete_my_commands(scope=BotCommandScopeDefault())
        await bot.set_my_commands(commands=user_commands, scope=BotCommandScopeDefault())
        verified_default = await bot.get_my_commands(scope=BotCommandScopeDefault())
        logger.info(
            f"✅ Registered & verified {len(verified_default)} user commands in Default scope: "
            f"{[f'/{c.command}' for c in verified_default]}"
        )

        # 2. Reset & Register Admin Command Scope (Visible ONLY to configured ADMIN_IDS)
        for admin_id in ADMIN_IDS:
            if admin_id != 0:
                try:
                    await bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=admin_id))
                    await bot.set_my_commands(
                        commands=admin_commands,
                        scope=BotCommandScopeChat(chat_id=admin_id),
                    )
                    verified_admin = await bot.get_my_commands(
                        scope=BotCommandScopeChat(chat_id=admin_id)
                    )
                    logger.info(
                        f"✅ Registered & verified {len(verified_admin)} commands in Admin scope for chat {admin_id}: "
                        f"{[f'/{c.command}' for c in verified_admin]}"
                    )
                except Exception as admin_cmd_err:
                    logger.error(
                        f"❌ Failed to register/verify admin command scope for {admin_id}: {admin_cmd_err}",
                        exc_info=True,
                    )

        # 3. Configure Telegram Bot Left-side Menu Button to open commands menu
        await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
        verified_menu_btn = await bot.get_chat_menu_button()
        logger.info(
            f"✅ Telegram Bot Menu Button configured: type={verified_menu_btn.type}"
        )

    except Exception as cmd_err:
        logger.error(f"❌ Failed to register bot commands or menu button: {cmd_err}", exc_info=True)

    # Launch background cache cleanup
    cleanup_task = asyncio.create_task(_periodic_cache_cleanup())

    # Start Flask Web Server for Render Health Checks
    server_thread = start_flask_server()

    # Install cross-platform signal listener for SIGTERM / SIGINT
    loop = asyncio.get_running_loop()

    def _handle_platform_signal(sig_name: str):
        global RECEIVED_SIGNAL
        RECEIVED_SIGNAL = sig_name
        logger.info(
            f"\n============================================================\n"
            f"🛑 [SIGNAL: {sig_name}] Platform signal received (e.g. Render spin-down / maintenance).\n"
            f"Stopping polling and triggering graceful shutdown...\n"
            f"============================================================"
        )

    for s, name in [(signal.SIGTERM, "SIGTERM"), (signal.SIGINT, "SIGINT")]:
        try:
            loop.add_signal_handler(s, lambda sn=name: _handle_platform_signal(sn))
        except (NotImplementedError, AttributeError, RuntimeError):
            try:
                signal.signal(s, lambda signum, frame, sn=name: _handle_platform_signal(sn))
            except Exception:
                pass

    logger.info(f"Starting @{bot_user.username} media downloader engine...")
    _allowed_updates = ["message", "edited_message", "callback_query", "my_chat_member"]
    logger.info(f"Polling with allowed_updates: {_allowed_updates}")

    shutdown_reason = "NORMAL"
    try:
        # Drop pending updates so stale messages are not processed
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Polling started")
        await dp.start_polling(bot, allowed_updates=_allowed_updates)
        shutdown_reason = RECEIVED_SIGNAL or "NORMAL"
    except asyncio.CancelledError:
        shutdown_reason = RECEIVED_SIGNAL or "SIGTERM"
    except Exception as e:
        shutdown_reason = f"CRASH: {type(e).__name__} ({e})"
        logger.error(f"❌ [CRASH: EXCEPTION] Unhandled exception in polling: {e}", exc_info=True)
    finally:
        await perform_graceful_shutdown(
            reason=shutdown_reason,
            bot=bot,
            cleanup_task=cleanup_task,
            server_thread=server_thread,
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 [SHUTDOWN: SIGINT] Process terminated by user.")
    except SystemExit as se:
        if se.code != 0:
            logger.info(f"🛑 [SHUTDOWN: SYSTEM_EXIT] Process exited with code {se.code}.")
    except Exception as exc:
        logger.critical(f"❌ [CRASH: FATAL] Process crashed with unhandled exception: {exc}", exc_info=True)
        sys.exit(1)
