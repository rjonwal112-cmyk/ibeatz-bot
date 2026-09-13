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
    Enforces per-user limits, global download concurrency, job lifecycle transitions,
    and priority scheduling for YouTube, Instagram, and Facebook.
    """

    def __init__(self, max_concurrent: int = MAX_CONCURRENT_DOWNLOADS):
        self.max_concurrent = max_concurrent
        self._jobs: dict[str, Job] = {}
        self._user_jobs: dict[int, str] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent)
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
                is_primary=is_primary,
            )
            self._jobs[job_id] = job
            self._user_jobs[user_id] = job_id
            return job

    def get_job(self, job_id: str) -> Optional[Job]:
        """Retrieves job by ID."""
        return self._jobs.get(job_id)

    def is_user_busy(self, user_id: int) -> bool:
        """Checks if a user already has an active or processing download."""
        job_id = self._user_jobs.get(user_id)
        if not job_id:
            return False
        job = self._jobs.get(job_id)
        if not job:
            return False
        return job.status in (
            JobStatus.QUEUED,
            JobStatus.PROCESSING,
            JobStatus.DOWNLOADING,
            JobStatus.MERGING,
            JobStatus.UPLOADING,
        )

    def get_user_active_job(self, user_id: int) -> Optional[Job]:
        """Returns the user's currently active or queued job if any."""
        job_id = self._user_jobs.get(user_id)
        if not job_id:
            return None
        job = self._jobs.get(job_id)
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
        """Marks job completion and frees the user concurrency slot."""
        job = self._jobs.get(job_id)
        if job:
            status = JobStatus.COMPLETED if success else JobStatus.FAILED
            self.update_status(job_id, status, error)
            self._user_jobs.pop(job.user_id, None)

    def cancel_job(self, job_id: str) -> bool:
        """Cancels an active job safely."""
        job = self._jobs.get(job_id)
        if job:
            self.update_status(job_id, JobStatus.CANCELLED)
            self._user_jobs.pop(job.user_id, None)
            return True
        return False

    def get_active_jobs_count(self) -> int:
        """Returns the number of currently active jobs."""
        return sum(
            1 for j in self._jobs.values()
            if j.status in (
                JobStatus.QUEUED,
                JobStatus.PROCESSING,
                JobStatus.DOWNLOADING,
                JobStatus.MERGING,
                JobStatus.UPLOADING,
            )
        )

    def get_stats(self) -> dict:
        """Returns comprehensive queue telemetry."""
        total = len(self._jobs)
        active = self.get_active_jobs_count()
        completed = sum(1 for j in self._jobs.values() if j.status == JobStatus.COMPLETED)
        failed = sum(1 for j in self._jobs.values() if j.status == JobStatus.FAILED)
        cancelled = sum(1 for j in self._jobs.values() if j.status == JobStatus.CANCELLED)
        return {
            "total_jobs": total,
            "active_jobs": active,
            "completed_jobs": completed,
            "failed_jobs": failed,
            "cancelled_jobs": cancelled,
        }


# Global Job Queue Manager
job_queue = JobQueueManager()
