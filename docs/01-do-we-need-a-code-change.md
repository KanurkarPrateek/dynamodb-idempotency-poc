# 1. Do we need a code change?

There are two starting points, and the answer differs.

## Case A: the workers have no idempotency store at all

**Yes, an application change is required, and it cannot be avoided.**

Only the worker knows when the real work happened: the CRM row was written, the email was sent, the
AI provider was billed. A database table, a queue setting or an infrastructure change cannot see that
moment. So the worker itself must do two things:

1. **Before the work:** claim the job's key. If the key is already DONE, skip the job. If another worker
   holds it, put the message back.
2. **After the work:** mark the key DONE, then delete the SQS message.

That is the whole change. In [`src/worker_example.py`](../src/worker_example.py) it is the lines marked
`# <-- claim store`:

```python
state = store.claim(key)
if state == "done":  delete message; return        # already finished: nothing runs twice
if state == "busy":  put message back; return      # another worker is on it right now
do_work(job)                                       # heartbeat thread calls store.extend(key)
store.done(key)
delete message
```

The job's **key** must come from the producer, for example `meeting-123:summary:v1`. Do not use the SQS
message ID: it changes when a message is replayed from a dead-letter queue, so it cannot recognise a repeat.

## Case B: the workers already use Redis, and we want DynamoDB

It depends on how Redis is used.

**If the worker calls Redis directly** (the usual starting point): yes, a code change. In the system this POC
came from, Redis was called in about seven places: claim, release, heartbeat, mark done, and three calls for
the API rate limit. Each would need rewriting for DynamoDB, which has a different API and different
guarantees.

**If the worker calls a small interface** (what this POC builds): **no code change, configuration only.**

```python
store = claimstore.from_env(lease_seconds, api_limit)   # picks the backend from CLAIM_STORE

store.claim(key)    -> "ok" | "done" | "busy"
store.extend(key)   # heartbeat
store.done(key)
store.release(key)  # hand back on failure or shutdown
store.api_acquire() -> token        # cluster-wide limit on concurrent API calls
store.api_release(token)
```

Switching to DynamoDB is then:

| Step | What |
|---|---|
| 1 | Create the table: partition key `pk` (string), on-demand billing, TTL on `ttl`. The store can also create it on first use. |
| 2 | Allow the worker's IAM role `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem` on that table. |
| 3 | Set `CLAIM_STORE=dynamodb` and `DDB_TABLE=<name>`, then restart the workers. |

## So what is the one-time change?

Introduce the interface once ([`src/claimstore.py`](../src/claimstore.py)) and route every Redis call
through it. After that, any backend is a new class with the same six methods: Redis, DynamoDB, or a
PostgreSQL table. The worker never changes again.

| Starting point | App change needed? |
|---|---|
| No idempotency at all | Yes: add claim / done around the work (unavoidable) |
| Redis called directly | Yes, once: move the calls behind the interface |
| Interface already in place | No: set `CLAIM_STORE` |

When you switch on a live system, read [docs/04](04-switching-from-redis.md) first: keys already marked DONE
in Redis do not exist in DynamoDB.
