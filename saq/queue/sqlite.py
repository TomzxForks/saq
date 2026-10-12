"""
SQLite Queue
"""

from __future__ import annotations

import asyncio
import json
import logging
import typing as t
from contextlib import asynccontextmanager
from functools import cached_property
from urllib.parse import urlparse

from saq.errors import MissingDependencyError
from saq.job import (
    Job,
    Status,
)
from saq.queue.base import Queue, logger
from saq.utils import now, now_seconds

if t.TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    from saq.types import (
        CountKind,
        DumpType,
        LoadType,
        QueueInfo,
        WorkerInfo,
    )

try:
    import aiosqlite
except ModuleNotFoundError as e:
    raise MissingDependencyError(
        "Missing dependencies for SQLite. Install them with `pip install saq[sqlite]`."
    ) from e


JOBS_TABLE = "saq_jobs"
STATS_TABLE = "saq_stats"


class SqliteQueue(Queue):
    """
    Queue is used to interact with SQLite.

    SQLite has no pub/sub or `LISTEN`/`NOTIFY` equivalent, so listeners and
    dequeues poll the database. It is a good fit for single machine deployments
    and testing, not for a distributed fleet on a shared network filesystem.

    Args:
        path: path to the SQLite database file. Use ":memory:" for an in-memory
            database, though note that only the queue's own connection can see it.
        name: name of the queue (default "default")
        jobs_table: name of the table SAQ will write jobs to (default "saq_jobs")
        stats_table: name of the table SAQ will write stats to (default "saq_stats")
        dump: lambda that takes a dictionary and outputs bytes (default `json.dumps`)
        load: lambda that takes str or bytes and outputs a python dictionary (default `json.loads`)
        priorities: The priority range to dequeue. (default (0, 32767))
        swept_error_message: The error message to use when sweeping jobs. (default "swept")
        busy_timeout: How long, in milliseconds, to wait for the write lock before
            raising "database is locked". (default 5000)
    """

    @classmethod
    def from_url(cls: type[SqliteQueue], url: str, **kwargs: t.Any) -> SqliteQueue:
        """Create a queue from a sqlite url, e.g. ``sqlite:///queue.db``.

        Follows the usual SQLite url convention: ``sqlite:///relative.db`` is a
        path relative to the working directory, ``sqlite:////abs/path.db`` is
        absolute, and ``sqlite://`` is an in-memory database.
        """
        parsed = urlparse(url)
        path = f"{parsed.netloc}{parsed.path}"
        path = path.removeprefix("/")
        if not path:
            path = ":memory:"
        return cls(path=path, **kwargs)

    def __init__(
        self,
        path: str,
        name: str = "default",
        jobs_table: str = JOBS_TABLE,
        stats_table: str = STATS_TABLE,
        dump: DumpType | None = None,
        load: LoadType | None = None,
        priorities: tuple[int, int] = (0, 32767),
        swept_error_message: str | None = None,
        busy_timeout: int = 5000,
    ) -> None:
        super().__init__(name=name, dump=dump, load=load, swept_error_message=swept_error_message)

        self.path = path
        self.jobs_table = jobs_table
        self.stats_table = stats_table
        self.busy_timeout = busy_timeout
        self._priorities = priorities
        self._connection: aiosqlite.Connection | None = None
        self._connected = False
        self._waiting = 0  # Internal counter of worker tasks waiting for dequeue
        self._dequeue_lock = asyncio.Lock()
        self._tx_lock = asyncio.Lock()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<path='{self.path}', name='{self.name}'>"

    def _get_connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError(
                "SqliteQueue is not connected, call `await queue.connect()` before using it"
            )
        return self._connection

    async def init_db(self) -> None:
        conn = self._get_connection()
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.jobs_table} (
                key TEXT PRIMARY KEY,
                job BLOB NOT NULL,
                queue TEXT NOT NULL,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                group_key TEXT,
                scheduled REAL NOT NULL DEFAULT 0,
                expire_at REAL
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.stats_table} (
                worker_id TEXT PRIMARY KEY,
                stats TEXT,
                metadata TEXT,
                queue_key TEXT,
                expire_at REAL
            )
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self.jobs_table}_status_queue_group_key_idx "
            f"ON {self.jobs_table} (status, queue, group_key)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self.jobs_table}_status_queue_priority_scheduled_idx "
            f"ON {self.jobs_table} (status, queue, priority, scheduled)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self.stats_table}_expire_at_idx "
            f"ON {self.stats_table} (expire_at)"
        )

    async def connect(self) -> None:
        if self._connected:
            return

        self._connection = await aiosqlite.connect(self.path, isolation_level=None)
        # Autocommit mode: transactions are managed explicitly, see `_transaction`.
        await self._connection.execute("PRAGMA journal_mode=WAL")
        await self._connection.execute("PRAGMA synchronous=NORMAL")
        await self._connection.execute(f"PRAGMA busy_timeout={self.busy_timeout}")
        await self.init_db()
        await super().connect()
        self._connected = True

    async def disconnect(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
        self._connected = False

    def serialize(self, job: Job) -> bytes:
        """Ensure serialized job is in bytes because the job column is a BLOB."""
        serialized = self._dump(job.to_dict())
        if isinstance(serialized, str):
            return serialized.encode("utf-8")
        return serialized

    @asynccontextmanager
    async def _transaction(
        self, connection: aiosqlite.Connection | None = None
    ) -> AsyncIterator[aiosqlite.Connection]:
        """Run a block in a transaction, or reuse one already in progress.

        An IMMEDIATE transaction takes the write lock upfront, which avoids the
        lock upgrade deadlock that a deferred read-then-write transaction can hit
        when two connections race.
        """
        if connection is not None:
            yield connection
            return

        # A single connection serves the whole queue, so top-level transactions
        # must be serialized: two overlapping BEGINs on one connection error.
        # Nested calls pass `connection` and reuse the outer transaction instead.
        async with self._tx_lock:
            conn = self._get_connection()
            await conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                await conn.execute("ROLLBACK")
                raise
            else:
                await conn.execute("COMMIT")

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> QueueInfo:
        """
        Returns info on the queue

        Args:
            jobs: Include job info (default False)
            offset: Offset of job info for pagination (default 0)
            limit: Max length of job info (default 10)
        """
        conn = self._get_connection()
        async with conn.execute(
            f"""
            SELECT worker_id, stats, queue_key, metadata
            FROM {self.stats_table}
            WHERE expire_at >= ? AND queue_key = ?
            """,
            (now_seconds(), self.name),
        ) as cursor:
            rows = await cursor.fetchall()

        workers: dict[str, WorkerInfo] = {
            worker_id: {
                "stats": json.loads(stats) if stats else None,
                "metadata": json.loads(metadata) if metadata else None,
                "queue_key": queue_key,
            }
            for worker_id, stats, queue_key, metadata in rows
        }

        queued = await self.count("queued")
        active = await self.count("active")
        incomplete = await self.count("incomplete")

        if jobs:
            async with conn.execute(
                f"""
                SELECT job, status
                FROM {self.jobs_table}
                WHERE status IN ('new', 'deferred', 'queued', 'active')
                  AND queue = ?
                ORDER BY priority, scheduled
                LIMIT ? OFFSET ?
                """,
                (self.name, limit, offset),
            ) as cursor:
                job_rows = await cursor.fetchall()
            deserialized_jobs = (self.deserialize(*row) for row in job_rows)
            jobs_info = [job.to_dict(safe=True) for job in deserialized_jobs if job]
        else:
            jobs_info = []

        return {
            "workers": workers,
            "name": self.name,
            "queued": queued,
            "active": active,
            "scheduled": incomplete - queued - active,
            "jobs": jobs_info,
        }

    async def count(self, kind: CountKind) -> int:
        conn = self._get_connection()
        if kind == "queued":
            query = f"SELECT count(*) FROM {self.jobs_table} WHERE status = 'queued' AND queue = ? AND ? >= scheduled"
            params: tuple[t.Any, ...] = (self.name, now_seconds())
        elif kind == "active":
            query = f"SELECT count(*) FROM {self.jobs_table} WHERE status = 'active' AND queue = ?"
            params = (self.name,)
        elif kind == "incomplete":
            query = (
                f"SELECT count(*) FROM {self.jobs_table} "
                "WHERE status IN ('new', 'deferred', 'queued', 'active') AND queue = ?"
            )
            params = (self.name,)
        else:
            raise ValueError("Can't count unknown type {kind}")

        async with conn.execute(query, params) as cursor:
            result = await cursor.fetchone()
        assert result
        return int(result[0])

    async def schedule(self, lock: int = 1) -> list[str]:
        return await self._dequeue()

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        """Delete jobs and stats past their expiration and sweep stuck jobs"""
        swept = []

        async with self._transaction() as conn:
            now_ts = now_seconds()

            await conn.execute(
                f"""
                DELETE FROM {self.jobs_table}
                WHERE queue = ? AND status IN ('aborted', 'complete', 'failed') AND ? >= expire_at
                """,
                (self.name, now_ts),
            )
            await conn.execute(
                f"DELETE FROM {self.stats_table} WHERE ? >= expire_at",
                (now_ts,),
            )
            async with conn.execute(
                f"""
                SELECT key, job, status
                FROM {self.jobs_table}
                WHERE queue = ? AND status IN ('active', 'aborting')
                """,
                (self.name,),
            ) as cursor:
                rows = await cursor.fetchall()

        for key, job_bytes, status in rows:
            job = self.deserialize(job_bytes, status)
            assert job
            if not job.stuck:
                continue

            swept.append(key)
            logger.info("Sweeping %s", job.info(logger.isEnabledFor(logging.DEBUG)))

            await self.abort(job, error=self.swept_error_message)

            try:
                await job.refresh(abort)
            except asyncio.TimeoutError:
                logger.info("Could not abort job %s", key)

            if job.retryable:
                await self.retry(job, error=self.swept_error_message)
            else:
                await self.finish(job, Status.ABORTED, error=self.swept_error_message)
        return swept

    async def _update(self, job: Job, status: Status | None = None, **kwargs: t.Any) -> None:
        expire_at = kwargs.pop("expire_at", -1)
        connection = kwargs.pop("connection", None)

        async with self._transaction(connection) as conn:
            if not status:
                status = await self.get_job_status(job.key, connection=conn)

            job.status = status or job.status

            if expire_at == -1:
                await conn.execute(
                    f"""
                    UPDATE {self.jobs_table} SET job = ?, status = ?, scheduled = ?
                    WHERE key = ?
                    """,
                    (self.serialize(job), job.status.value, job.scheduled, job.key),
                )
            else:
                await conn.execute(
                    f"""
                    UPDATE {self.jobs_table} SET job = ?, status = ?, scheduled = ?, expire_at = ?
                    WHERE key = ?
                    """,
                    (self.serialize(job), job.status.value, job.scheduled, expire_at, job.key),
                )

    async def job(self, job_key: str) -> Job | None:
        conn = self._get_connection()
        async with conn.execute(
            f"SELECT job, status FROM {self.jobs_table} WHERE key = ? AND queue = ?",
            (job_key, self.name),
        ) as cursor:
            row = await cursor.fetchone()
        if row:
            return self.deserialize(*row)
        return None

    async def jobs(self, job_keys: Iterable[str]) -> list[Job | None]:
        keys = list(job_keys)
        if not keys:
            return []

        conn = self._get_connection()
        placeholders = ",".join("?" * len(keys))
        async with conn.execute(
            f"SELECT key, job, status FROM {self.jobs_table} WHERE key IN ({placeholders})",
            keys,
        ) as cursor:
            rows = await cursor.fetchall()
        results = {row[0]: row[1:] for row in rows}
        return [self.deserialize(*(results.get(key) or [None])) for key in keys]

    async def iter_jobs(
        self,
        statuses: list[Status] | None = None,
        batch_size: int = 100,
    ) -> t.AsyncIterator[Job]:
        if statuses is None:
            statuses = list(Status)

        conn = self._get_connection()
        last_key = ""
        status_values = [status.value for status in statuses]
        placeholders = ",".join("?" * len(status_values))

        while True:
            async with conn.execute(
                f"""
                SELECT key, job, status
                FROM {self.jobs_table}
                WHERE status IN ({placeholders})
                  AND queue = ?
                  AND key > ?
                ORDER BY key
                LIMIT ?
                """,
                (*status_values, self.name, last_key, batch_size),
            ) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                break

            for key, job_bytes, status in rows:
                last_key = key
                job = self.deserialize(job_bytes, status)
                if job:
                    yield job

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        async with self._transaction() as conn:
            status = await self.get_job_status(job.key, connection=conn)
            if not status or status == Status.QUEUED:
                await self.finish(job, Status.ABORTED, error=error, connection=conn)
            else:
                await self.update(job, status=Status.ABORTING, error=error, connection=conn)

    async def dequeue(self, timeout: float = 0.0, poll_interval: float = 0.0) -> Job | None:
        interval = poll_interval if poll_interval > 0.0 else 0.1
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout if timeout > 0.0 else None

        self._waiting += 1
        try:
            while True:
                if self._job_queue.empty():
                    await self._dequeue()

                if not self._job_queue.empty():
                    job = self._job_queue.get_nowait()
                    self._job_queue.task_done()
                    return job

                if deadline is not None and loop.time() >= deadline:
                    return None

                await asyncio.sleep(interval)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            return None
        finally:
            self._waiting -= 1

    async def _dequeue(self) -> list[str]:
        if self._dequeue_lock.locked():
            return []

        scheduled_ids: list[str] = []

        async with self._dequeue_lock, self._transaction() as conn:
            if not self._waiting:
                return scheduled_ids

            async with conn.execute(
                f"""
                    SELECT key, job
                    FROM {self.jobs_table}
                    WHERE status = 'queued'
                      AND queue = ?
                      AND ? >= scheduled
                      AND priority BETWEEN ? AND ?
                      AND (
                        group_key IS NULL
                        OR group_key NOT IN (
                          SELECT DISTINCT group_key
                          FROM {self.jobs_table}
                          WHERE status = 'active' AND queue = ? AND group_key IS NOT NULL
                        )
                      )
                    ORDER BY priority, scheduled
                    LIMIT ?
                    """,
                (
                    self.name,
                    now_seconds(),
                    self._priorities[0],
                    self._priorities[1],
                    self.name,
                    self._waiting,
                ),
            ) as cursor:
                rows = await cursor.fetchall()

            if rows:
                keys = [row[0] for row in rows]
                placeholders = ",".join("?" * len(keys))
                await conn.execute(
                    f"UPDATE {self.jobs_table} SET status = 'active' WHERE key IN ({placeholders})",
                    keys,
                )

            dequeued = now()
            for key, job_blob in rows:
                job = self.deserialize(job_blob, Status.ACTIVE)
                assert job
                job.started = dequeued
                job.touched = dequeued
                await self._update(job, status=Status.ACTIVE, connection=conn)
                self._job_queue.put_nowait(job)
                # only report explicitly scheduled jobs, matching RedisQueue.schedule
                if job.scheduled:
                    scheduled_ids.append(job.id)

        return scheduled_ids

    async def _enqueue(self, job: Job) -> Job | None:
        async with self._transaction() as conn, conn.execute(
            f"""
            INSERT INTO {self.jobs_table} (
              key, job, queue, status, priority, group_key, scheduled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (key) DO UPDATE SET
              job = excluded.job,
              queue = excluded.queue,
              status = excluded.status,
              priority = excluded.priority,
              group_key = excluded.group_key,
              scheduled = excluded.scheduled,
              expire_at = NULL
            WHERE {self.jobs_table}.status IN ('aborted', 'complete', 'failed')
              AND excluded.scheduled > {self.jobs_table}.scheduled
            """,
            (
                job.key,
                self.serialize(job),
                self.name,
                job.status.value,
                job.priority,
                job.group_key,
                job.scheduled or now_seconds(),
            ),
        ) as cursor:
            # rowcount is 0 when the conflicting row is still active and the
            # `DO UPDATE ... WHERE` clause skips the update
            if cursor.rowcount == 0:
                return None

        logger.info("Enqueuing %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        return job

    async def write_worker_info(
        self,
        worker_id: str,
        info: WorkerInfo,
        ttl: int,
    ) -> None:
        async with self._transaction() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self.stats_table} (worker_id, stats, queue_key, metadata, expire_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (worker_id) DO UPDATE SET
                  stats = excluded.stats,
                  queue_key = excluded.queue_key,
                  metadata = excluded.metadata,
                  expire_at = excluded.expire_at
                """,
                (
                    worker_id,
                    json.dumps(info["stats"]),
                    info["queue_key"],
                    json.dumps(info["metadata"]),
                    now_seconds() + ttl,
                ),
            )

    async def get_job_status(
        self,
        key: str,
        connection: aiosqlite.Connection | None = None,
    ) -> Status | None:
        conn = connection or self._get_connection()
        async with conn.execute(
            f"SELECT status FROM {self.jobs_table} WHERE key = ?",
            (key,),
        ) as cursor:
            result = await cursor.fetchone()
        if result:
            return Status(result[0])
        return None

    async def _retry(self, job: Job, error: str | None) -> None:
        next_retry_delay = job.next_retry_delay()
        if next_retry_delay:
            scheduled = now_seconds() + next_retry_delay
        else:
            scheduled = job.scheduled or now_seconds()

        await self.update(job, status=Status.QUEUED, scheduled=scheduled, expire_at=None)

    async def _finish(
        self,
        job: Job,
        status: Status,
        *,
        result: t.Any = None,
        error: str | None = None,
        connection: aiosqlite.Connection | None = None,
    ) -> None:
        key = job.key

        if job.ttl >= 0:
            expire_at = now_seconds() + job.ttl if job.ttl > 0 else None
            await self.update(job, status=status, expire_at=expire_at, connection=connection)
        else:
            async with self._transaction(connection) as conn:
                await conn.execute(f"DELETE FROM {self.jobs_table} WHERE key = ?", (key,))

    @cached_property
    def _job_queue(self) -> asyncio.Queue:
        return asyncio.Queue()
