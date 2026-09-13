import logging
from typing import Optional

from aiogram import Bot, F, Router
from aiogram.enums import ChatType
from aiogram.types import Message

logger = logging.getLogger(__name__)

echo_router = Router(name="echo_router")
print("ECHO ROUTER LOADED")


@echo_router.message(F.text.startswith("/"))
async def deprecated_command_handler(message: Message, bot: Optional[Bot] = None) -> None:
    """
    Catches any deprecated, retired, or unrecognized commands.
    Informs the user cleanly without leaving old handlers active.
    In groups, only responds if the command was directed at this bot.
    """
    text = message.text or ""
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        if bot is not None:
            try:
                bot_user = await bot.me()
                bot_username = (bot_user.username or "").lower()
                cmd_token = text.split()[0].lower()
                if "@" in cmd_token:
                    _, mention = cmd_token.partition("@")[::2]
                    if mention != bot_username:
                        return
                else:
                    return
            except Exception:
                return

    await message.reply(
        "This command is no longer available. Please send a video link directly or use /help."
    )


@echo_router.message()
async def fallback_echo_handler(message: Message) -> None:
    """
    Fallback message handler for non-URL messages.
    Remains silent in groups/supergroups to prevent disturbance.
    In private chats, provides guidance on sending public media links.
    This handler is also the diagnostic catch-all: if it fires for a URL
    message in a group, it means the media_url_handler filter rejected it.
    """
    text_preview = (message.text or message.caption or "<media>")[:80]
    logger.info(
        f"UPDATE RECEIVED (fallback): chat_type={message.chat.type} "
        f"chat_id={message.chat.id} message_id={message.message_id} "
        f"text={text_preview!r}"
    )
    if message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
        return

    guide_text = (
        "💡 <b>Send any public video or music link to download!</b>\n\n"
        "• 🔴 <b>YouTube:</b> Videos, Shorts\n"
        "• 📸 <b>Instagram:</b> Reels, Videos\n"
        "• 🔵 <b>Facebook:</b> Videos, Reels\n"
        "• 🌐 TikTok, X, Reddit, Vimeo, Twitch, and more\n\n"
        "Use /help for commands or add me to a group!"
    )
    await message.reply(guide_text)
