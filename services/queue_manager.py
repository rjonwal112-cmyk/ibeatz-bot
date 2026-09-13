import asyncio
import enum
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from config import MAX_CONCURRENT_DOWNLOADS

logger = logging.getLogger(__name__)


class JobStatus(enum.Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    DOWNLOADING = "downloading"
    MERGING = "merging"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


@dataclass
class Job:
    """Represents a discrete media download task."""
    job_id: str
    user_id: int
    chat_id: int
    platform: str
    url: str
    quality: str
    status: JobStatus = JobStatus.QUEUED
    progress_percent: str = "0%"
    speed: str = "N/A"
    eta: str = "N/A"
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error_message: Optional[str] = None
    is_primary: bool = False

    def duration_s(self) -> float:
        if self.started_at:
            end = self.completed_at or time.time()
            return end - self.started_at
        return 0.0


class JobQueueManager:
    """
    Production-grade asynchronous job queue and concurrency manager.
    Manages global concurrency limits (MEDIA_MAX_CONCURRENT), FIFO waiting queue,
    automatic promotion on completion/cancellation/failure, and independent job lifecycle tracking.
    """

    def __init__(self, max_concurrent: int = MAX_CONCURRENT_DOWNLOADS):
        self.max_concurrent = max_concurrent
        self._jobs: dict[str, Job] = {}
        self._user_jobs: dict[int, list[str]] = {}
        self._active_jobs: set[str] = set()
        self._waiting_queue: list[tuple[str, asyncio.Event]] = []
        self._lock = asyncio.Lock()

    async def register_job(
        self,
        job_id: str,
        user_id: int,
        chat_id: int,
        platform: str,
        url: str,
        quality: str,
    ) -> Job:
        """Enrolls a new job into the queue registry."""
        async with self._lock:
            is_primary = platform.lower() in ("youtube", "instagram", "facebook")
            job = Job(
                job_id=job_id,
                user_id=user_id,
                chat_id=chat_id,
                platform=platform,
                url=url,
                quality=quality,
                status=JobStatus.QUEUED,
                is_primary=is_primary,
            )
            self._jobs[job_id] = job
            if user_id not in self._user_jobs:
                self._user_jobs[user_id] = []
            if job_id not in self._user_jobs[user_id]:
                self._user_jobs[user_id].append(job_id)
            return job

    def get_job(self, job_id: str) -> Optional[Job]:
        """Retrieves job by ID."""
        return self._jobs.get(job_id)

    async def acquire_slot(
        self,
        job_id: str,
        on_queued: Optional[Any] = None,
    ) -> bool:
        """
        Acquires a global concurrency slot.
        If active jobs >= max_concurrent, enters FIFO waiting queue.
        Returns True when slot is granted, or False if cancelled while queued.
        """
        event = None
        pos = 0
        async with self._lock:
            # Check if cancelled before acquire
            job = self._jobs.get(job_id)
            if job and job.status == JobStatus.CANCELLED:
                logger.info(f"🔴 [QUEUE] Job {job_id} was cancelled before slot acquisition.")
                return False

            if len(self._active_jobs) < self.max_concurrent:
                self._active_jobs.add(job_id)
                self.update_status(job_id, JobStatus.DOWNLOADING)
                logger.info(
                    f"🟢 [QUEUE] Job {job_id} started immediately. "
                    f"active={len(self._active_jobs)}/{self.max_concurrent}, "
                    f"queue_len={len(self._waiting_queue)}"
                )
                return True

            # Enter waiting queue
            event = asyncio.Event()
            self._waiting_queue.append((job_id, event))
            self.update_status(job_id, JobStatus.QUEUED)
            pos = len(self._waiting_queue)
            logger.info(
                f"🟡 [QUEUE] Job {job_id} queued at pos #{pos}. "
                f"active={len(self._active_jobs)}/{self.max_concurrent}, "
                f"queue_len={pos}"
            )

        if on_queued:
            try:
                res = on_queued(pos)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.debug(f"on_queued callback failed for {job_id}: {e}")

        # Wait for promotion or cancellation signal
        await event.wait()

        async with self._lock:
            job = self._jobs.get(job_id)
            if job and job.status == JobStatus.CANCELLED:
                logger.info(
                    f"🔴 [QUEUE] Job {job_id} cancelled while waiting in queue. "
                    f"Passing slot to next queued job."
                )
                self._active_jobs.discard(job_id)
                self._promote_next_locked()
                return False

            self.update_status(job_id, JobStatus.DOWNLOADING)
            logger.info(
                f"🟢 [QUEUE] Job {job_id} promoted to active. "
                f"active={len(self._active_jobs)}/{self.max_concurrent}, "
                f"queue_len={len(self._waiting_queue)}"
            )
            return True

    async def release_slot(self, job_id: str) -> None:
        """Releases concurrency slot and promotes next queued job (async-safe)."""
        async with self._lock:
            self._release_slot_locked(job_id)

    def _release_slot_locked(self, job_id: str) -> None:
        """Internal slot release and promotion inside self._lock."""
        if job_id in self._active_jobs:
            self._active_jobs.discard(job_id)
            logger.info(
                f"⚪ [QUEUE] Job {job_id} slot released. "
                f"active={len(self._active_jobs)}/{self.max_concurrent}, "
                f"queue_len={len(self._waiting_queue)}"
            )
            self._promote_next_locked()

    def _promote_next_locked(self) -> None:
        """Promotes the next non-cancelled job from the FIFO waiting queue."""
        while self._waiting_queue and len(self._active_jobs) < self.max_concurrent:
            next_job_id, event = self._waiting_queue.pop(0)
            next_job = self._jobs.get(next_job_id)
            if not next_job or next_job.status == JobStatus.CANCELLED:
                continue
            # Reserve slot immediately to prevent race conditions
            self._active_jobs.add(next_job_id)
            event.set()
            break

    def is_user_busy(self, user_id: int) -> bool:
        """Returns False to allow concurrent/queued requests per user."""
        return False

    def get_user_active_job(self, user_id: int) -> Optional[Job]:
        """Returns the user's most recently active or queued job if any."""
        job_ids = self._user_jobs.get(user_id, [])
        for jid in reversed(job_ids):
            job = self._jobs.get(jid)
            if job and job.status in (
                JobStatus.QUEUED,
                JobStatus.PROCESSING,
                JobStatus.DOWNLOADING,
                JobStatus.MERGING,
                JobStatus.UPLOADING,
            ):
                return job
        return None

    def update_status(self, job_id: str, status: JobStatus, error: Optional[str] = None) -> None:
        """Transitions job state and records lifecycle timestamps."""
        job = self._jobs.get(job_id)
        if job:
            job.status = status
            if status in (JobStatus.PROCESSING, JobStatus.DOWNLOADING) and not job.started_at:
                job.started_at = time.time()
            elif status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                job.completed_at = time.time()
                if error:
                    job.error_message = error

    def update_progress(self, job_id: str, percent: str, speed: str, eta: str) -> None:
        """Updates truthful progress telemetry."""
        job = self._jobs.get(job_id)
        if job:
            job.progress_percent = percent
            job.speed = speed
            job.eta = eta

    def finish_job(self, job_id: str, success: bool, error: Optional[str] = None) -> None:
        """Marks job completion, cleans user job list, and releases concurrency slot."""
        job = self._jobs.get(job_id)
        if job:
            status = JobStatus.COMPLETED if success else JobStatus.FAILED
            self.update_status(job_id, status, error)
            user_id = job.user_id
            if user_id in self._user_jobs:
                if job_id in self._user_jobs[user_id]:
                    self._user_jobs[user_id].remove(job_id)
                if not self._user_jobs[user_id]:
                    self._user_jobs.pop(user_id, None)

            if success:
                logger.info(
                    f"✅ [QUEUE] Job {job_id} finished successfully. "
                    f"active={len(self._active_jobs)}/{self.max_concurrent}, "
                    f"queue_len={len(self._waiting_queue)}"
                )
            else:
                logger.info(
                    f"❌ [QUEUE] Job {job_id} failed: {error}. "
                    f"active={len(self._active_jobs)}/{self.max_concurrent}, "
                    f"queue_len={len(self._waiting_queue)}"
                )
            self._release_slot_locked(job_id)

    def cancel_job(self, job_id: str) -> bool:
        """Cancels an active or queued job safely and releases concurrency slot."""
        job = self._jobs.get(job_id)
        if not job:
            return False

        self.update_status(job_id, JobStatus.CANCELLED)
        user_id = job.user_id
        if user_id in self._user_jobs:
            if job_id in self._user_jobs[user_id]:
                self._user_jobs[user_id].remove(job_id)
            if not self._user_jobs[user_id]:
                self._user_jobs.pop(user_id, None)

        # Check if in waiting queue
        for idx, (qid, event) in enumerate(self._waiting_queue):
            if qid == job_id:
                self._waiting_queue.pop(idx)
                event.set()
                logger.info(
                    f"🔴 [QUEUE] Queued job {job_id} cancelled. "
                    f"active={len(self._active_jobs)}/{self.max_concurrent}, "
                    f"queue_len={len(self._waiting_queue)}"
                )
                return True

        # If active, release slot and promote next
        if job_id in self._active_jobs:
            logger.info(
                f"🔴 [QUEUE] Active job {job_id} cancelled. "
                f"active={len(self._active_jobs)}/{self.max_concurrent}, "
                f"queue_len={len(self._waiting_queue)}"
            )
            self._release_slot_locked(job_id)
            return True

        return True

    def get_active_jobs_count(self) -> int:
        """Returns the number of currently active jobs."""
        return len(self._active_jobs)

    def get_queue_length(self) -> int:
        """Returns the number of jobs waiting in queue."""
        return len(self._waiting_queue)

    def get_stats(self) -> dict:
        """Returns comprehensive queue telemetry."""
        total = len(self._jobs)
        active = len(self._active_jobs)
        queued = len(self._waiting_queue)
        completed = sum(1 for j in self._jobs.values() if j.status == JobStatus.COMPLETED)
        failed = sum(1 for j in self._jobs.values() if j.status == JobStatus.FAILED)
        cancelled = sum(1 for j in self._jobs.values() if j.status == JobStatus.CANCELLED)
        return {
            "total_jobs": total,
            "active_jobs": active,
            "queue_length": queued,
            "max_concurrent": self.max_concurrent,
            "completed_jobs": completed,
            "failed_jobs": failed,
            "cancelled_jobs": cancelled,
        }


# Global Job Queue Manager
job_queue = JobQueueManager()
