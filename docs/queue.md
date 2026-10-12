# Queues

SAQ ships with several queue backends, selected by the URL scheme passed to
`Queue.from_url`:

* `redis://` for Redis
* `postgres://` for Postgres
* `sqlite:///path/to/queue.db` for SQLite (`sqlite://` is an in-memory database, and
  `sqlite:////abs/path.db` is an absolute path). SQLite has no pub/sub, so listeners and
  dequeues poll the database, which suits single machine deployments and testing.

Redis queues can be tuned further:

* redis ssl?
* redis db?
* redis pw?


```{eval-rst}
.. autoapifunction:: saq.queue.Queue.from_url
    :noindex:
```
