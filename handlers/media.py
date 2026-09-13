import asyncio
import logging
import mimetypes
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from aiogram import F, Router
from aiogram.enums import ChatType
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import (
    QUALITY_AUDIO,
    QUALITY_HIGHEST,
    QUALITY_LABELS,
    USER_RATE_LIMIT_SECONDS,
)
from downloader import (
    DOMAIN_URL_REGEX,
    URL_REGEX,
    cancel_download,
    cleanup_temp_dir,
    download_media,
    extract_url,
    get_media_preview,
)
from services import (
    IN_FLIGHT_DOWNLOADS,
    IN_FLIGHT_LOCK,
    CacheEntry,
    JobStatus,
    extract_audio_from_video,
    is_maintenance_mode,
    job_queue,
    media_cache,
    normalize_media_url,
    record_audio_extraction,
    record_cache_hit,
    record_request,
)
from services.platforms import SUPPORTED_PLATFORMS, detect_platform_from_url
from services.stats import record_delivery_metric
from handlers.admin import record_group_update, record_group_url

logger = logging.getLogger(__name__)

media_router = Router(name="media_router")

# User rate limiting tracking
USER_LAST_REQUEST_TIME: dict[int, float] = {}

# Pending requests metadata storage: req_id -> dict
PENDING_REQUESTS: dict[str, dict] = {}


def clean_and_validate_media_url(raw_url: str) -> Optional[str]:
    """
    Cleans candidate URL: trims whitespace, strips surrounding punctuation,
    verifies scheme, and validates that it belongs to a supported media platform.
    """
    if not raw_url:
        return None
    url = raw_url.strip().strip(" \t\r\n()[]{}<>,.;:!?'\"`")
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        domain_match = DOMAIN_URL_REGEX.search(url)
        if domain_match:
            url = "https://" + domain_match.group(0).strip(" \t\r\n()[]{}<>,.;:!?'\"`")
        else:
            return None

    url = url.strip(" \t\r\n()[]{}<>,.;:!?'\"`")

    # Verify supported platform (ignore unknown URLs like google.com)
    plat_info = detect_platform_from_url(url)
    if plat_info.key == "generic" or plat_info.key not in SUPPORTED_PLATFORMS:
        return None

    return url


def extract_all_media_urls_from_message(message: Message) -> list[str]:
    """
    Extracts all supported media URLs from message text or caption,
    preferring Telegram entities (URL and text_link) with regex fallback.
    Trims surrounding punctuation and filters for supported media platforms.
    """
    found_urls: list[str] = []
    seen: set[str] = set()

    def add_candidate(raw: Optional[str]) -> None:
        if not raw:
            return
        cleaned = clean_and_validate_media_url(raw)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            found_urls.append(cleaned)

    # 1. Inspect text entities (using ent.extract_from to avoid UTF-16 slicing bugs)
    if message.text and message.entities:
        for ent in message.entities:
            if ent.type == "text_link" and ent.url:
                add_candidate(ent.url)
            elif ent.type == "url":
                try:
                    url_text = ent.extract_from(message.text)
                    add_candidate(url_text)
                except Exception:
                    url_text = message.text[ent.offset : ent.offset + ent.length]
                    add_candidate(url_text)

    # 2. Inspect caption entities (for media attachments with URL captions)
    if message.caption and message.caption_entities:
        for ent in message.caption_entities:
            if ent.type == "text_link" and ent.url:
                add_candidate(ent.url)
            elif ent.type == "url":
                try:
                    url_text = ent.extract_from(message.caption)
                    add_candidate(url_text)
                except Exception:
                    url_text = message.caption[ent.offset : ent.offset + ent.length]
                    add_candidate(url_text)

    # 3. Regex fallback across entire text and caption
    for text_source in (message.text, message.caption):
        if text_source:
            for match in URL_REGEX.finditer(text_source):
                add_candidate(match.group(0))
            for d_match in DOMAIN_URL_REGEX.finditer(text_source):
                add_candidate("https://" + d_match.group(0))

    return found_urls


def extract_media_url_from_message(message: Message) -> Optional[str]:
    """Backward-compatible helper returning the first detected supported URL."""
    urls = extract_all_media_urls_from_message(message)
    return urls[0] if urls else None


def _build_cancel_keyboard(req_id: str, user_id: int) -> Any:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Cancel", callback_data=f"cancel:{req_id[:16]}:{user_id}")
    return builder.as_markup()


def _build_retry_keyboard(req_id: str, user_id: int) -> Any:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Retry", callback_data=f"retry:{req_id[:16]}:{user_id}")
    return builder.as_markup()


def _build_quality_keyboard(
    qualities: list[str],
    req_id: str,
    user_id: int,
) -> Any:
    builder = InlineKeyboardBuilder()
    for q in qualities:
        label = QUALITY_LABELS.get(q, q)
        builder.button(
            text=f"⬇️ {label}",
            callback_data=f"q:{q[:8]}:{req_id[:16]}:{user_id}",
        )
    builder.button(
        text="❌ Cancel",
        callback_data=f"cancel:{req_id[:16]}:{user_id}",
    )
    builder.adjust(1)
    return builder.as_markup()


async def _deliver_cached_media(
    message: Message,
    status_msg: Optional[Message],
    entry: CacheEntry,
    user_id: int,
    user_mention: str,
    is_group: bool,
    t_start: float = 0.0,
) -> bool:
    """
    Delivers media directly from persistent cache.
    Uses Telegram file_id for instantaneous 0.1s re-delivery when available,
    or local file path if file_id is not yet registered.
    """
    t_lookup = time.time() - t_start if t_start > 0 else 0.01
    file_path = Path(entry.file_path) if entry.file_path else None
    has_local_file = file_path is not None and file_path.exists() and file_path.stat().st_size > 0

    if not entry.telegram_file_id and not has_local_file:
        return False

    short_title = (entry.title[:50] + "...") if len(entry.title) > 55 else entry.title
    quality_label = QUALITY_LABELS.get(entry.requested_quality, entry.requested_quality)

    if is_group:
        caption = f"👤 {user_mention}\n{short_title}\nQuality: {quality_label}"
    else:
        caption = f"{short_title}\nQuality: {quality_label}"

    # Post-delivery Audio button
    audio_kb = InlineKeyboardBuilder()
    audio_kb.button(
        text="🎵 Audio",
        callback_data=f"aud:{entry.platform[:8]}:{entry.media_id[:24]}:{entry.requested_quality[:6]}:{user_id}",
    )

    t_send_start = time.time()
    sent_msg = None

    is_photo = file_path is not None and file_path.suffix.lower() in (
        ".jpg", ".jpeg", ".png", ".webp"
    )

    # Fast Path 1: Telegram file_id (instant 0.1s delivery)
    if entry.telegram_file_id:
        try:
            if is_photo:
                sent_msg = await message.reply_photo(
                    photo=entry.telegram_file_id,
                    caption=caption,
                    request_timeout=60,
                )
            else:
                sent_msg = await message.reply_video(
                    video=entry.telegram_file_id,
                    caption=caption,
                    duration=int(entry.duration) if (entry.duration and int(entry.duration) > 0) else None,
                    supports_streaming=True,
                    reply_markup=audio_kb.as_markup(),
                    request_timeout=60,
                )
        except Exception as id_err:
            logger.debug(f"telegram_file_id re-send failed ({id_err}), falling back to disk")
            sent_msg = None

    # Fast Path 2: Upload local file from disk
    if not sent_msg and has_local_file:
        media_cache.acquire_file_use(str(file_path))
        try:
            media_file = FSInputFile(file_path)
            if is_photo:
                try:
                    sent_msg = await message.reply_photo(
                        photo=media_file,
                        caption=caption,
                        request_timeout=120,
                    )
                except Exception:
                    sent_msg = await message.answer_photo(
                        photo=media_file,
                        caption=caption,
                        request_timeout=120,
                    )
            else:
                try:
                    sent_msg = await message.reply_video(
                        video=media_file,
                        caption=caption,
                        duration=int(entry.duration) if (entry.duration and int(entry.duration) > 0) else None,
                        supports_streaming=True,
                        reply_markup=audio_kb.as_markup(),
                        request_timeout=300,
                    )
                except Exception as vid_err:
                    logger.warning(f"reply_video failed ({vid_err}), attempting answer_video")
                    try:
                        sent_msg = await message.answer_video(
                            video=media_file,
                            caption=caption,
                            duration=int(entry.duration) if (entry.duration and int(entry.duration) > 0) else None,
                            supports_streaming=True,
                            reply_markup=audio_kb.as_markup(),
                            request_timeout=300,
                        )
                    except Exception as ans_err:
                        logger.warning(f"answer_video failed ({ans_err}), falling back to document")
                        try:
                            sent_msg = await message.reply_document(
                                document=media_file,
                                caption=caption,
                                reply_markup=audio_kb.as_markup(),
                                request_timeout=300,
                            )
                        except Exception:
                            sent_msg = await message.answer_document(
                                document=media_file,
                                caption=caption,
                                reply_markup=audio_kb.as_markup(),
                                request_timeout=300,
                            )
                except Exception as ans_err:
                    logger.warning(f"answer_video failed ({ans_err}), falling back to document")
                    try:
                        sent_msg = await message.reply_document(
                            document=media_file,
                            caption=caption,
                            reply_markup=audio_kb.as_markup(),
                            request_timeout=300,
                        )
                    except Exception:
                        sent_msg = await message.answer_document(
                            document=media_file,
                            caption=caption,
                            reply_markup=audio_kb.as_markup(),
                            request_timeout=300,
                        )

            # Store resulting telegram_file_id for future 0.1s instant deliveries
            if sent_msg and sent_msg.video:
                await media_cache.update_telegram_file_id(
                    platform=entry.platform,
                    media_id=entry.media_id,
                    quality=entry.requested_quality,
                    file_id=sent_msg.video.file_id,
                )
        finally:
            media_cache.release_file_use(str(file_path))

    if not sent_msg:
        return False

    t_send_elapsed = time.time() - t_send_start

    # Clean up status message to keep chat history tidy
    if status_msg:
        try:
            await status_msg.delete()
        except Exception:
            pass

    # Record delivery telemetry
    await record_delivery_metric(
        req_id=uuid.uuid4().hex[:8],
        platform=entry.platform,
        quality=entry.requested_quality,
        is_cache_hit=True,
        cache_lookup_s=t_lookup,
        extraction_s=0.0,
        download_s=0.0,
        ffmpeg_s=0.0,
        upload_s=t_send_elapsed,
        total_s=t_lookup + t_send_elapsed,
        bottleneck="Cache Send",
        file_size=entry.file_size,
    )

    # Record user history
    if user_id:
        try:
            await db.record_user_download(
                user_id=user_id,
                platform=entry.platform,
                media_id=entry.media_id,
                title=entry.title or "Video",
                quality=entry.requested_quality,
            )
        except Exception as e:
            logger.debug(f"Failed to record user download: {e}")

    logger.info(
        f"⚡ Delivered cached media: {entry.platform}:{entry.media_id} "
        f"in {t_lookup + t_send_elapsed:.2f}s (file_id={bool(entry.telegram_file_id)})"
    )
    return True


async def _download_and_deliver(
    message: Message,
    status_msg: Message,
    url: str,
    platform_key: str,
    media_id: str,
    quality: str,
    req_id: str,
    user_id: int,
    user_mention: str,
    is_group: bool,
    media_info: Optional[Any],
    cancel_kb: Any,
    t_start: float,
) -> None:
    """Downloads media, caches it in SQLite, and uploads it to Telegram."""
    loop = asyncio.get_running_loop()
    flight_key = f"{platform_key}:{media_id}"
    flight_future = None

    # Register with centralized Job Queue Manager
    await job_queue.register_job(
        job_id=req_id,
        user_id=user_id,
        chat_id=message.chat.id,
        platform=platform_key,
        url=url,
        quality=quality,
    )
    job_queue.update_status(req_id, JobStatus.DOWNLOADING)

    async with IN_FLIGHT_LOCK:
        if flight_key not in IN_FLIGHT_DOWNLOADS:
            flight_future = loop.create_future()
            IN_FLIGHT_DOWNLOADS[flight_key] = flight_future

    last_update_time = [0.0]

    def _on_progress(percent: str, speed: str, eta: str) -> None:
        job_queue.update_progress(req_id, percent, speed, eta)
        now = time.time()
        if now - last_update_time[0] >= 2.0:
            last_update_time[0] = now
            if speed == "merging":
                job_queue.update_status(req_id, JobStatus.MERGING)
                text = "⚙️ Remuxing video & audio…"
            elif speed and speed != "N/A" and eta and eta != "N/A":
                text = f"⬇️ Downloading {percent} ({speed}, ETA {eta})"
            else:
                text = f"⬇️ Downloading {percent}"
            asyncio.run_coroutine_threadsafe(
                status_msg.edit_text(text, reply_markup=cancel_kb),
                loop,
            )

    cached_raw_info = media_info.raw_info if media_info else None
    extraction_time_s = media_info.extraction_time_s if media_info else 0.0
    temp_dir = None
    upload_path_str: Optional[str] = None

    try:
        logger.info("download started")
        result = await download_media(
            url=url,
            user_id=user_id,
            quality=quality,
            req_id=req_id,
            progress_callback=_on_progress,
            cached_info=cached_raw_info,
            extraction_time_s=extraction_time_s,
        )
        temp_dir = result.temp_dir

        if not result.success:
            logger.error(f"Download failed for {url}: {result.error_message}")
            job_queue.finish_job(req_id, success=False, error=result.error_message)
            async with IN_FLIGHT_LOCK:
                if flight_future and not flight_future.done():
                    flight_future.set_result(None)
                IN_FLIGHT_DOWNLOADS.pop(flight_key, None)

            retry_kb = _build_retry_keyboard(req_id, user_id)
            await status_msg.edit_text(
                result.error_message or "❌ Couldn't download this link.",
                reply_markup=retry_kb,
            )
            return

        logger.info("download completed")
        logger.info(f"DOWNLOAD COMPLETE: {req_id}")
        file_path = result.file_path
        file_exists = file_path.exists() if file_path else False
        file_sz = file_path.stat().st_size if (file_path and file_exists) else 0

        logger.info(f"file path: {file_path}")
        logger.info(f"file exists: {file_exists}")
        logger.info(f"file size: {file_sz} bytes")

        info = result.media_info
        raw_title = (info.title if info else None) or (result.file_path.stem if result.file_path else "Media")
        short_title = (raw_title[:50] + "...") if len(raw_title) > 55 else raw_title
        quality_label = QUALITY_LABELS.get(quality, quality)

        if is_group:
            caption = f"👤 {user_mention}\n{short_title}\nQuality: {quality_label}"
        else:
            caption = f"{short_title}\nQuality: {quality_label}"

        audio_kb = InlineKeyboardBuilder()
        audio_kb.button(
            text="🎵 Audio",
            callback_data=f"aud:{platform_key[:8]}:{media_id[:24]}:{quality[:6]}:{user_id}",
        )

        job_queue.update_status(req_id, JobStatus.UPLOADING)
        if status_msg:
            try:
                asyncio.create_task(status_msg.edit_text("⚡ Uploading…", reply_markup=None))
            except Exception:
                pass

        t_upload_start = time.time()
        sent_msg = None

        # -----------------------------------------------------------------
        # CASE 1: Instagram / Multi-item Carousel (Requirement 7)
        # -----------------------------------------------------------------
        if result.is_carousel and len(result.carousel_files) > 1:
            logger.info(f"Preparing Telegram media group for carousel: {len(result.carousel_files)} items")
            media_group = []
            for idx, (itype, ipath) in enumerate(result.carousel_files):
                item_cap = caption if idx == 0 else None
                if itype == "photo":
                    media_group.append(InputMediaPhoto(media=FSInputFile(ipath, chunk_size=524288), caption=item_cap))
                else:
                    media_group.append(InputMediaVideo(media=FSInputFile(ipath, chunk_size=524288), caption=item_cap))

            logger.info(f"path: {result.carousel_files[0][1]}")
            logger.info(f"exists: {result.carousel_files[0][1].exists()}")
            logger.info(f"size: {sum(p.stat().st_size for _, p in result.carousel_files)} bytes total")
            logger.info(f"mime/type: multipart/media-group")
            logger.info("Telegram send started")

            try:
                sent_msgs = await message.reply_media_group(media=media_group, request_timeout=300)
            except Exception as mg_err:
                logger.warning(f"reply_media_group failed ({mg_err}), attempting answer_media_group")
                try:
                    sent_msgs = await message.answer_media_group(media=media_group, request_timeout=300)
                except Exception as final_mg_err:
                    logger.error(f"Telegram media group send failed: {final_mg_err}", exc_info=True)
                    sent_msgs = None

            if not sent_msgs:
                logger.error("Failed to send carousel media group into Telegram.")
                job_queue.finish_job(req_id, success=False, error="Telegram send failed")
                if status_msg:
                    await status_msg.edit_text("❌ Couldn't upload media to Telegram. Please try again.")
                return

            for idx in range(len(result.carousel_files)):
                logger.info(f"carousel item {idx} Telegram delivery success: True")

            logger.info("Telegram send completed")
            logger.info(f"TELEGRAM DELIVERY COMPLETE: {req_id}")

        # -----------------------------------------------------------------
        # CASE 2: Single Media (Video / Photo / Audio) with SQLite Caching
        # -----------------------------------------------------------------
        else:
            upload_path = result.file_path
            upload_path_str = str(upload_path)
            media_cache.acquire_file_use(upload_path_str)
            media_file = FSInputFile(upload_path, chunk_size=524288)

            mime_type = mimetypes.guess_type(str(upload_path))[0] or "application/octet-stream"
            logger.info(f"path: {upload_path}")
            logger.info(f"exists: {upload_path.exists()}")
            logger.info(f"size: {upload_path.stat().st_size} bytes")
            logger.info(f"mime/type: {mime_type}")
            logger.info("Telegram send started")

            is_photo = upload_path.suffix.lower() in (".jpg", ".jpeg", ".png", ".webp") or (info and getattr(info, "media_type", "") == "photo")

            if result.is_audio:
                try:
                    sent_msg = await message.reply_audio(
                        audio=media_file,
                        caption=caption,
                        duration=int(info.duration) if (info and info.duration and int(info.duration) > 0) else None,
                        performer=info.uploader if info else "",
                        title=raw_title,
                        request_timeout=300,
                    )
                except Exception as aud_err:
                    logger.warning(f"reply_audio failed ({aud_err}), attempting answer_audio")
                    sent_msg = await message.answer_audio(
                        audio=media_file,
                        caption=caption,
                        duration=int(info.duration) if (info and info.duration and int(info.duration) > 0) else None,
                        performer=info.uploader if info else "",
                        title=raw_title,
                        request_timeout=300,
                    )
            elif is_photo:
                try:
                    sent_msg = await message.reply_photo(
                        photo=media_file,
                        caption=caption,
                        request_timeout=120,
                    )
                except Exception as photo_err:
                    logger.warning(f"reply_photo failed ({photo_err}), attempting answer_photo")
                    try:
                        sent_msg = await message.answer_photo(
                            photo=media_file,
                            caption=caption,
                            request_timeout=120,
                        )
                    except Exception:
                        sent_msg = await message.reply_document(
                            document=media_file,
                            caption=caption,
                            request_timeout=120,
                        )
            else:
                v_duration = int(info.duration) if (info and info.duration and int(info.duration) > 0) else None
                v_width = int(info.width) if (info and info.width and int(info.width) > 0) else None
                v_height = int(info.height) if (info and info.height and int(info.height) > 0) else None
                try:
                    sent_msg = await message.reply_video(
                        video=media_file,
                        caption=caption,
                        duration=v_duration,
                        width=v_width,
                        height=v_height,
                        supports_streaming=True,
                        reply_markup=audio_kb.as_markup(),
                        request_timeout=300,
                    )
                except Exception as vid_err:
                    logger.warning(f"reply_video failed ({vid_err}), attempting answer_video")
                    try:
                        sent_msg = await message.answer_video(
                            video=media_file,
                            caption=caption,
                            duration=v_duration,
                            width=v_width,
                            height=v_height,
                            supports_streaming=True,
                            reply_markup=audio_kb.as_markup(),
                            request_timeout=300,
                        )
                    except Exception as ans_err:
                        logger.warning(f"answer_video failed ({ans_err}), sending as document")
                        try:
                            sent_msg = await message.reply_document(
                                document=media_file,
                                caption=caption,
                                reply_markup=audio_kb.as_markup(),
                                request_timeout=300,
                            )
                        except Exception:
                            try:
                                sent_msg = await message.answer_document(
                                    document=media_file,
                                    caption=caption,
                                    reply_markup=audio_kb.as_markup(),
                                    request_timeout=300,
                                )
                            except Exception as doc_err:
                                logger.error(f"answer_document failed: {doc_err}", exc_info=True)
                                sent_msg = None

            if not sent_msg:
                logger.error(f"Telegram upload failed: all send attempts failed for {upload_path}")
                job_queue.finish_job(req_id, success=False, error="Telegram send failed")
                if status_msg:
                    await status_msg.edit_text("❌ Couldn't upload media to Telegram. Please try again.")
                return

            logger.info("Telegram send completed")
            logger.info(f"TELEGRAM DELIVERY COMPLETE: {req_id}")

            # Register with media cache and store telegram_file_id in SQLite for instant repeat delivery
            tg_file_id = None
            tg_audio_id = None
            if sent_msg and sent_msg.video:
                tg_file_id = sent_msg.video.file_id
            elif sent_msg and sent_msg.photo:
                tg_file_id = sent_msg.photo[-1].file_id
            elif sent_msg and sent_msg.audio:
                tg_audio_id = sent_msg.audio.file_id

            try:
                entry = await media_cache.put(
                    platform=platform_key,
                    media_id=media_id,
                    normalized_url=url,
                    title=raw_title,
                    requested_quality=quality,
                    actual_quality=info.max_quality if info else quality,
                    source_file_path=result.file_path,
                    duration=info.duration if info else None,
                    uploader=info.uploader if info else "",
                    telegram_file_id=tg_file_id,
                    telegram_audio_id=tg_audio_id,
                )
                async with IN_FLIGHT_LOCK:
                    if flight_future and not flight_future.done():
                        flight_future.set_result(entry)
                    IN_FLIGHT_DOWNLOADS.pop(flight_key, None)
            except Exception as cache_err:
                logger.debug(f"Post-delivery cache put error: {cache_err}")

        t_upload_elapsed = time.time() - t_upload_start
        timings = result.timings
        timings.upload_s = t_upload_elapsed
        timings.total_s = (
            timings.format_extraction_s
            + timings.download_s
            + timings.ffmpeg_processing_s
            + timings.upload_s
        )

        job_queue.finish_job(req_id, success=True)

        # Record delivery telemetry to SQLite
        await record_delivery_metric(
            req_id=req_id,
            platform=platform_key,
            quality=quality,
            is_cache_hit=False,
            cache_lookup_s=0.01,
            extraction_s=timings.format_extraction_s,
            download_s=timings.download_s,
            ffmpeg_s=timings.ffmpeg_processing_s,
            upload_s=timings.upload_s,
            total_s=timings.total_s,
            bottleneck=timings.dominant_stage(),
            file_size=file_sz,
        )

        # Record user history
        if user_id:
            try:
                await db.record_user_download(
                    user_id=user_id,
                    platform=platform_key,
                    media_id=media_id,
                    title=raw_title,
                    quality=quality,
                )
            except Exception as e:
                logger.debug(f"Failed to record user download: {e}")

        logger.info(
            f"⏱ [PERF] Req {req_id} ({platform_key} - {quality}): "
            f"Ext={timings.format_extraction_s:.2f}s, DL={timings.download_s:.2f}s, "
            f"FFmpeg={timings.ffmpeg_processing_s:.2f}s, Up={timings.upload_s:.2f}s | "
            f"Total={timings.total_s:.2f}s | Bottleneck={timings.dominant_stage()}"
        )

        # Clean up progress status message
        try:
            await status_msg.delete()
        except Exception:
            pass

    finally:
        if upload_path_str:
            media_cache.release_file_use(upload_path_str)
        cleanup_temp_dir(temp_dir)
        logger.info("cleanup completed")
        async with IN_FLIGHT_LOCK:
            if flight_future and not flight_future.done():
                flight_future.set_result(None)
            IN_FLIGHT_DOWNLOADS.pop(flight_key, None)


async def _process_single_media_url(message: Message, url: str, is_group: bool) -> None:
    """
    Processes a single supported media URL through cache, inspection, and download/delivery.
    """
    logger.info("request received")
    logger.info(f"URL detected: {url}")
    logger.info(f"URL DETECTED: {url}")
    logger.info(f"original URL: {url}")

    user = message.from_user
    user_id = user.id if user else 0
    user_mention = f"@{user.username}" if (user and user.username) else (user.first_name if user else "User")

    record_request()

    # Maintenance mode check
    if is_maintenance_mode():
        await message.reply("🛠 Bot is temporarily undergoing maintenance. Please try again shortly.")
        return

    # Per-user rate limiting (relaxed in groups to avoid dropping different user messages)
    now = time.time()
    last_req = USER_LAST_REQUEST_TIME.get(user_id, 0.0)
    if now - last_req < USER_RATE_LIMIT_SECONDS:
        wait_secs = int(USER_RATE_LIMIT_SECONDS - (now - last_req)) + 1
        if not is_group:
            await message.reply(f"⏳ Please wait {wait_secs}s before sending another link.")
        return
    USER_LAST_REQUEST_TIME[user_id] = now

    # Concurrency limit per user via centralized JobQueueManager
    if job_queue.is_user_busy(user_id):
        if not is_group:
            await message.reply("⚠️ You already have a download in progress. Please wait for it to complete.")
        return

    t_start = time.time()
    norm_url, platform_key, media_id = normalize_media_url(url)
    logger.info(f"canonical URL: {norm_url}")
    logger.info(f"CANONICAL URL: {norm_url}")
    logger.info(f"canonicalized URL: {norm_url}")
    logger.info(f"platform detected: {platform_key}")
    logger.info(f"PLATFORM: {platform_key}")
    req_id = uuid.uuid4().hex[:12]

    # Register job in JobQueueManager (correct method name: register_job, not create_job)
    await job_queue.register_job(
        job_id=req_id,
        user_id=user_id,
        chat_id=message.chat.id,
        url=norm_url,
        platform=platform_key,
        quality=QUALITY_HIGHEST,
    )
    logger.info(f"JOB CREATED: {req_id}")

    # 1. SQLite Smart Cache Check FIRST (Instant 0.2s delivery without status msg)
    cached_entry = await media_cache.get(platform_key, media_id, QUALITY_HIGHEST)
    if not cached_entry:
        cached_entry = await media_cache.get(platform_key, media_id, "best")

    if cached_entry and (cached_entry.telegram_file_id or (cached_entry.file_path and Path(cached_entry.file_path).exists())):
        record_cache_hit()
        delivered = await _deliver_cached_media(
            message=message,
            status_msg=None,
            entry=cached_entry,
            user_id=user_id,
            user_mention=user_mention,
            is_group=is_group,
            t_start=t_start,
        )
        if delivered:
            job_queue.finish_job(req_id, success=True)
            logger.info(f"TELEGRAM DELIVERY COMPLETE: {req_id}")
            return

    try:
        status_msg = await message.reply("🔎 Fetching…")
    except Exception:
        status_msg = await message.answer("🔎 Fetching…")

    # 2. Check in-flight concurrent download (Request coalescing - Requirement 8)
    flight_key = f"{platform_key}:{media_id}"
    flight_future = None
    async with IN_FLIGHT_LOCK:
        if flight_key in IN_FLIGHT_DOWNLOADS:
            flight_future = IN_FLIGHT_DOWNLOADS[flight_key]

    if flight_future:
        try:
            await status_msg.edit_text("⏳ Media is already downloading, waiting for result…")
            entry = await asyncio.wait_for(asyncio.shield(flight_future), timeout=180)
            if entry:
                record_cache_hit()
                delivered = await _deliver_cached_media(
                    message=message,
                    status_msg=status_msg,
                    entry=entry,
                    user_id=user_id,
                    user_mention=user_mention,
                    is_group=is_group,
                    t_start=t_start,
                )
                if delivered:
                    job_queue.finish_job(req_id, success=True)
                    logger.info(f"TELEGRAM DELIVERY COMPLETE: {req_id}")
                    return
        except Exception as flight_err:
            logger.debug(f"In-flight wait failed or timed out: {flight_err}")

    # FAST PATH: Single-pass direct download for group messages and platforms without quality options
    # Eliminates 3.5 - 5.0 seconds of redundant metadata preview extraction!
    should_fast_direct_download = is_group or platform_key in ("instagram", "facebook", "tiktok", "twitter", "reddit", "threads")
    if should_fast_direct_download:
        cancel_kb = _build_cancel_keyboard(req_id, user_id)
        await _download_and_deliver(
            message=message,
            status_msg=status_msg,
            url=norm_url,
            platform_key=platform_key,
            media_id=media_id,
            quality=QUALITY_HIGHEST,
            req_id=req_id,
            user_id=user_id,
            user_mention=user_mention,
            is_group=is_group,
            media_info=None,
            cancel_kb=cancel_kb,
            t_start=t_start,
        )
        return

    # 3. Metadata Inspection & Quality Decision Policy
    logger.info("extract started")
    success, media_info, error_msg = await get_media_preview(norm_url)
    logger.info("extract completed")

    if not success or not media_info:
        logger.error(f"Extraction failed for {url}: {error_msg}")
        job_queue.finish_job(req_id, success=False, error=error_msg)
        if not is_group:
            retry_kb = _build_retry_keyboard(req_id, user_id)
            PENDING_REQUESTS[req_id] = {
                "url": norm_url,
                "user_id": user_id,
                "platform_key": platform_key,
                "media_id": media_id,
                "message": message,
            }
            await status_msg.edit_text(error_msg or "❌ Couldn't inspect media link.", reply_markup=retry_kb)
        else:
            await status_msg.edit_text(error_msg or "❌ Couldn't inspect media link.")
            await asyncio.sleep(4)
            try:
                await status_msg.delete()
            except Exception:
                pass
        return

    media_type = getattr(media_info, "media_type", "video")
    logger.info(f"media type detected: {media_type}")

    qualities = media_info.available_qualities
    cancel_kb = _build_cancel_keyboard(req_id, user_id)

    PENDING_REQUESTS[req_id] = {
        "url": norm_url,
        "platform_key": platform_key,
        "media_id": media_id,
        "media_info": media_info,
        "user_id": user_id,
        "user_mention": user_mention,
        "is_group": is_group,
        "message": message,
        "status_msg": status_msg,
        "cancel_kb": cancel_kb,
        "t_start": t_start,
    }

    # QUALITY POLICY DECISION:
    # 1. In groups: auto-download highest quality directly to keep chat clean and fast (Requirement 6)
    # 2. Instagram / Facebook -> single auto-download (no quality buttons)
    # 3. Photos / Carousels -> auto-download directly
    # 4. YouTube & others in private chat:
    #    - If max available >= 1080p -> show genuine quality buttons
    #    - If max available < 1080p -> auto-download highest quality directly
    if not is_group and len(qualities) > 1 and platform_key not in ("instagram", "facebook") and media_type == "video":
        kb = _build_quality_keyboard(qualities, req_id, user_id)
        short_title = (media_info.title[:50] + "...") if len(media_info.title) > 55 else media_info.title
        prompt_text = (
            f"🎬 <b>{short_title}</b>\n"
            f"Select download quality:"
        )
        await status_msg.edit_text(prompt_text, reply_markup=kb)
        return

    # Direct automatic download
    chosen_quality = qualities[0] if qualities else QUALITY_HIGHEST
    logger.info(f"selected format: {chosen_quality}")
    logger.info(f"DOWNLOAD START: {req_id}")
    await status_msg.edit_text("⬇️ Downloading…", reply_markup=cancel_kb if not is_group else None)
    await _download_and_deliver(
        message=message,
        status_msg=status_msg,
        url=norm_url,
        platform_key=platform_key,
        media_id=media_id,
        quality=chosen_quality,
        req_id=req_id,
        user_id=user_id,
        user_mention=user_mention,
        is_group=is_group,
        media_info=media_info,
        cancel_kb=cancel_kb,
        t_start=t_start,
    )


@media_router.message(
    F.chat.type.in_({ChatType.PRIVATE, ChatType.GROUP, ChatType.SUPERGROUP}),
    lambda msg: not ((msg.text and msg.text.startswith("/")) or (msg.caption and msg.caption.startswith("/")))
    and bool(extract_all_media_urls_from_message(msg)),
)
async def media_url_handler(message: Message) -> None:
    """
    Main URL message handler for private chats, groups, and supergroups.
    Automatically detects and processes supported media URLs without requiring commands.
    """
    # ── Diagnostic: confirms update reached this handler (not blocked by Privacy Mode) ──
    text_preview = (message.text or message.caption or "<media>")[:80]
    logger.info(
        f"UPDATE RECEIVED: chat_type={message.chat.type} "
        f"chat_id={message.chat.id} message_id={message.message_id} "
        f"text={text_preview!r}"
    )

    urls = extract_all_media_urls_from_message(message)
    if not urls:
        logger.debug(
            f"NO URL DETECTED: message {message.message_id} in {message.chat.type} contains no supported media URLs"
        )
        return

    is_group = message.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP)
    if is_group:
        logger.info("GROUP MESSAGE RECEIVED")
        record_group_update(message.chat.id)
        record_group_url(urls[0])
    logger.info(f"CHAT TYPE: {message.chat.type}")
    logger.info(f"MESSAGE ID: {message.message_id}")

    # Process sequentially to avoid exceeding concurrency limits
    for idx, u in enumerate(urls[:5]):
        if idx > 0:
            await asyncio.sleep(1.0)
        await _process_single_media_url(message=message, url=u, is_group=is_group)


@media_router.callback_query(F.data.startswith("q:"))
async def quality_choice_callback(callback: CallbackQuery) -> None:
    """Handles quality selection button clicks."""
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer("Invalid request.", show_alert=True)
        return

    chosen_quality = parts[1]
    req_id = parts[2]
    requester_id = int(parts[3])

    logger.info(f"selected format: {chosen_quality}")

    # Enforce callback requester ownership in groups
    if callback.from_user.id != requester_id:
        await callback.answer("⚠️ Only the requester can interact with this button.", show_alert=True)
        return

    req_data = PENDING_REQUESTS.get(req_id)
    if not req_data:
        await callback.answer("⚠️ Request expired. Please send the link again.", show_alert=True)
        return

    user_id = callback.from_user.id
    if job_queue.is_user_busy(user_id):
        await callback.answer("⚠️ Download already in progress.", show_alert=True)
        return

    status_msg = req_data["status_msg"]
    cancel_kb = req_data["cancel_kb"]

    try:
        await status_msg.edit_text("⬇️ Downloading…", reply_markup=cancel_kb)
    except Exception:
        pass
    await callback.answer()

    await _download_and_deliver(
        message=req_data["message"],
        status_msg=status_msg,
        url=req_data["url"],
        platform_key=req_data["platform_key"],
        media_id=req_data["media_id"],
        quality=chosen_quality,
        req_id=req_id,
        user_id=user_id,
        user_mention=req_data["user_mention"],
        is_group=req_data["is_group"],
        media_info=req_data["media_info"],
        cancel_kb=cancel_kb,
        t_start=req_data.get("t_start", time.time()),
    )


@media_router.callback_query(F.data.startswith("cancel:"))
async def cancel_callback_handler(callback: CallbackQuery) -> None:
    """Handles download cancellation requests."""
    parts = callback.data.split(":")
    if len(parts) >= 3:
        req_id = parts[1]
        requester_id = int(parts[2])
        if callback.from_user.id != requester_id:
            await callback.answer("⚠️ Only the requester can cancel this download.", show_alert=True)
            return
        cancel_download(req_id)
        job_queue.cancel_job(req_id)

    await callback.answer("Download cancelled.")
    try:
        if callback.message:
            await callback.message.edit_text("❌ Download cancelled.", reply_markup=None)
    except Exception:
        pass


@media_router.callback_query(F.data.startswith("retry:"))
async def retry_callback_handler(callback: CallbackQuery) -> None:
    """Handles retry attempts after failure."""
    parts = callback.data.split(":")
    if len(parts) >= 3:
        req_id = parts[1]
        requester_id = int(parts[2])
        if callback.from_user.id != requester_id:
            await callback.answer("⚠️ Only the requester can retry this request.", show_alert=True)
            return

    req_data = PENDING_REQUESTS.get(parts[1]) if len(parts) >= 2 else None
    if not req_data:
        await callback.answer("Please send the link again.", show_alert=True)
        return

    await callback.answer()
    if callback.message:
        target_msg = req_data.get("message")
        if target_msg:
            await callback.message.edit_text("🔄 Retrying…")
            target_msg.text = req_data["url"]
            await media_url_handler(target_msg)
        else:
            await callback.message.edit_text("⚠️ Please send the link again.")


@media_router.callback_query(F.data.startswith("aud:"))
async def post_delivery_audio_callback(callback: CallbackQuery) -> None:
    """
    Extracts high-quality MP3 from the local cached video on demand.
    Never re-downloads from remote source.
    """
    parts = callback.data.split(":")
    if len(parts) < 5:
        await callback.answer("Invalid audio request.", show_alert=True)
        return

    plat_key = parts[1]
    media_id = parts[2]
    quality = parts[3]
    requester_id = int(parts[4])

    # Enforce callback requester ownership in groups
    if callback.from_user.id != requester_id:
        await callback.answer("⚠️ Only the requester can interact with this button.", show_alert=True)
        return

    cached_entry = await media_cache.get(plat_key, media_id, quality)
    if not cached_entry:
        await callback.answer("⚠️ Media cache expired. Please send the link again.", show_alert=True)
        return

    # Fast Path: Telegram audio file_id already cached
    if cached_entry.telegram_audio_id and callback.message:
        await callback.answer()
        try:
            await callback.message.reply_audio(
                audio=cached_entry.telegram_audio_id,
                title=cached_entry.title,
                performer=cached_entry.platform.capitalize(),
                duration=int(cached_entry.duration) if cached_entry.duration else 0,
            )
            return
        except Exception:
            pass

    video_path = Path(cached_entry.file_path) if cached_entry.file_path else None
    if not video_path or not video_path.exists():
        await callback.answer("⚠️ Cached video is no longer on disk. Please send link again.", show_alert=True)
        return

    await callback.answer("⏳ Extracting audio…")
    status_msg = None
    try:
        status_msg = await callback.message.reply("⏳ Extracting audio…")
    except Exception:
        pass

    media_cache.acquire_file_use(str(video_path))
    try:
        success, audio_path, err = await extract_audio_from_video(
            video_path=video_path,
            title=cached_entry.title,
            artist=cached_entry.platform.capitalize(),
        )

        if not success or not audio_path:
            if status_msg:
                await status_msg.edit_text(f"❌ {err or 'Audio extraction failed.'}")
            return

        record_audio_extraction()
        audio_file = FSInputFile(audio_path)
        short_title = (cached_entry.title[:50] + "...") if len(cached_entry.title) > 55 else cached_entry.title

        sent_audio = None
        try:
            sent_audio = await callback.message.reply_audio(
                audio=audio_file,
                title=short_title,
                performer=cached_entry.platform.capitalize(),
                duration=int(cached_entry.duration) if cached_entry.duration else 0,
                request_timeout=300,
            )
        except Exception:
            sent_audio = await callback.message.answer_audio(
                audio=audio_file,
                title=short_title,
                performer=cached_entry.platform.capitalize(),
                duration=int(cached_entry.duration) if cached_entry.duration else 0,
                request_timeout=300,
            )

        # Store telegram_audio_id in SQLite for instant repeat delivery
        if sent_audio and sent_audio.audio:
            await media_cache.update_telegram_file_id(
                platform=plat_key,
                media_id=media_id,
                quality=quality,
                file_id=sent_audio.audio.file_id,
                is_audio=True,
            )

        if status_msg:
            try:
                await status_msg.delete()
            except Exception:
                pass

    finally:
        media_cache.release_file_use(str(video_path))
