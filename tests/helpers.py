from __future__ import annotations

import asyncio
import os
import time
import typing as t

import psycopg

from saq.job import Job, Status
from saq.queue import Queue
from saq.queue.postgres import PostgresQueue
from saq.queue.redis import RedisQueue
from saq.queue.sqlite import SqliteQueue


def _xdist_worker_index() -> int:
    """Index of the current pytest-xdist worker, or 0 when running serially."""
    worker = os.environ.get("PYTEST_XDIST_WORKER", "")
    if worker.startswith("gw") and worker[2:].isdigit():
        return int(worker[2:])
    return 0


# Each xdist worker talks to its own Postgres database and Redis logical database
# so a parallel run doesn't have workers dropping each other's schema or flushing
# each other's keys. Serial runs use suffix/index 0 and behave as before.
WORKER_INDEX = _xdist_worker_index()
POSTGRES_TEST_DB = f"test_saq_{WORKER_INDEX}"
POSTGRES_MAINTENANCE_URL = "postgres://postgres@localhost/postgres"


def _redis_url() -> str:
    return f"redis://localhost:6379/{WORKER_INDEX}"


def _postgres_url() -> str:
    return f"postgres://postgres@localhost/{POSTGRES_TEST_DB}"


async def create_redis_queue(url: str | None = None, **kwargs: t.Any) -> RedisQueue:
    queue = t.cast(RedisQueue, Queue.from_url(url or _redis_url(), **kwargs))
    await queue.connect()
    await queue.redis.flushdb()
    return queue


async def create_sqlite_queue(path: str, **kwargs: t.Any) -> SqliteQueue:
    url = "sqlite://" if path == ":memory:" else f"sqlite:///{path}"
    queue = t.cast(SqliteQueue, Queue.from_url(url, **kwargs))
    await queue.connect()
    return queue


async def create_postgres_queue(**kwargs: t.Any) -> PostgresQueue:
    queue = t.cast(PostgresQueue, Queue.from_url(_postgres_url(), **kwargs))
    await queue.connect()
    await asyncio.sleep(0.1)  # Give some time for the tasks to start
    return queue


async def wait_for_job(job: Job, predicate: t.Callable[[Job], bool], timeout: float = 10) -> None:
    """Refresh the job until the predicate holds or the deadline expires.

    Callers still assert on the job afterwards, so a timeout surfaces the actual
    state instead of hiding it. Use this instead of a fixed sleep before asserting
    job state: fixed sleeps are the main source of flakes on loaded CI runners.
    """
    deadline = time.monotonic() + timeout
    while not predicate(job) and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
        await job.refresh()


async def wait_for_status(job: Job, *statuses: Status, timeout: float = 10) -> None:
    """Refresh the job until its status is one of statuses or the deadline expires."""
    await wait_for_job(job, lambda j: j.status in statuses, timeout=timeout)


async def cleanup_queue(queue: Queue) -> None:
    if isinstance(queue, RedisQueue):
        await queue.redis.flushdb()
    await queue.disconnect()


async def setup_postgres() -> None:
    async with await psycopg.AsyncConnection.connect(
        POSTGRES_MAINTENANCE_URL, autocommit=True
    ) as conn:
        # kill connections leaked by previous tests so they can't block the drop
        # or hold the init_db advisory lock; scoped to this worker's database
        await conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE pid <> pg_backend_pid() AND datname = %s",
            (POSTGRES_TEST_DB,),
        )
        await conn.execute(f'DROP DATABASE IF EXISTS "{POSTGRES_TEST_DB}"')
        await conn.execute(f'CREATE DATABASE "{POSTGRES_TEST_DB}"')


async def teardown_postgres() -> None:
    async with await psycopg.AsyncConnection.connect(
        POSTGRES_MAINTENANCE_URL, autocommit=True
    ) as conn:
        await conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE pid <> pg_backend_pid() AND datname = %s",
            (POSTGRES_TEST_DB,),
        )
        await conn.execute(f'DROP DATABASE IF EXISTS "{POSTGRES_TEST_DB}"')
