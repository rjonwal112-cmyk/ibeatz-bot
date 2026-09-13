from handlers.admin import admin_router
from handlers.echo import echo_router
from handlers.media import media_router
from handlers.start import start_router

__all__ = [
    "start_router",
    "admin_router",
    "media_router",
    "echo_router",
]
