import asyncio
import logging
import sys

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
    log_startup_system_report,
    media_cache,
)

import os
import threading
from flask import Flask

logger = logging.getLogger(__name__)

# ==============================================================================
# 24/7 KEEP-ALIVE FLASK SERVER FOR RENDER (PORT 10000)
# ==============================================================================
flask_app = Flask(__name__)


@flask_app.route("/")
def index():
    return (
        "<h1>⚡ ɪʙᴇᴀᴛᴢ Bot is running 24/7</h1>"
        "<p>High-performance Telegram media downloader active.</p>",
        200,
    )


@flask_app.route("/health")
def health():
    return {"status": "ok", "bot": "@ibeatzfetch_bot"}, 200


def _run_flask():
    port = int(os.getenv("PORT", 10000))
    import logging as py_logging

    py_logging.getLogger("werkzeug").setLevel(py_logging.WARNING)
    logger.info(f"🌐 24/7 Keep-Alive Flask server listening on port {port}...")
    flask_app.run(host="0.0.0.0", port=port, threaded=True)


def keep_alive():
    """Starts the 24/7 Keep-Alive Flask server in a daemon background thread."""
    thread = threading.Thread(target=_run_flask, daemon=True)
    thread.start()


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

    # Validate and retrieve BOT_TOKEN from environment
    BOT_TOKEN = get_bot_token()

    # Ensure storage directories exist
    BASE_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize SQLite database and telemetry
    await db.initialize()
    await init_stats()

    # Run FFmpeg environment diagnostics
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
    import os
    from pathlib import Path
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

    # Start 24/7 Keep-Alive Flask Web Server on Port 10000 for Render
    keep_alive()

    logger.info(f"Starting @{bot_user.username} media downloader engine...")
    # Explicitly request all update types we need: message (all chats) + callback_query
    _allowed_updates = ["message", "edited_message", "callback_query", "my_chat_member"]
    logger.info(f"Polling with allowed_updates: {_allowed_updates}")
    try:
        # Drop pending updates so stale messages are not processed
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Polling started")
        await dp.start_polling(bot, allowed_updates=_allowed_updates)
    finally:
        cleanup_task.cancel()
        await bot.session.close()
        logger.info("Bot session closed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("ɪʙᴇᴀᴛᴢ bot stopped by user.")
    except SystemExit:
        sys.exit(1)
