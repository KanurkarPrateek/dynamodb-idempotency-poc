# 5. Tests and results

Three levels of evidence, each run against both backends with the same code.

## 5.1 Correctness tests (8 per backend)

[`tests/claim_poc.py`](../tests/claim_poc.py) uses several store instances with different owners to act as
many workers.

| # | Test | What it proves | Redis | DynamoDB |
|---|---|---|---|---|
| 1 | Race | 20 workers claim the same key at the same instant, for 50 keys: exactly one wins each time | Pass | Pass |
| 2 | Done-skip | Every later delivery of a finished job returns `done` | Pass | Pass |
| 3 | Crash takeover | A worker dies holding a claim; after the lease expires another worker takes over | Pass | Pass |
| 4 | Heartbeat | A worker that keeps extending its lease is never taken over, even past the lease length | Pass | Pass |
| 5 | Release | A job handed back on shutdown can be claimed immediately | Pass | Pass |
| 6 | Stale owner | A worker whose lease was taken over cannot release the new owner's claim | Pass | Pass |
| 7 | API limit | 64 threads share 16 slots: never more than 16 calls at once | Pass | Pass |
| 8 | Latency | 1,000 claim + done cycles | see below | see below |

Run locally (Docker) and in Kubernetes: [`results/tests-local.txt`](../results/tests-local.txt),
[`results/tests-kubernetes.txt`](../results/tests-kubernetes.txt).

### Latency per operation

| Where | Backend | claim p50 / p99 | done p50 / p99 |
|---|---|---|---|
| Laptop (Docker) | Redis | 0.08 / 0.23 ms | 0.14 / 0.31 ms |
| Laptop (Docker) | DynamoDB Local | 0.69 / 1.28 ms | 0.65 / 1.13 ms |
| Kubernetes | Redis | 0.18 / 0.29 ms | 0.28 / 0.48 ms |
| Kubernetes | DynamoDB Local | 2.46 / 9.0 ms | 2.28 / 6.92 ms |

Both are tiny next to jobs that take seconds to minutes. DynamoDB Local is a Java emulator, so these numbers
are not a forecast for the real service.

## 5.2 Two bugs the tests found in the original Redis code

The POC started from a worker that already used Redis. Two of its operations failed the tests:

**A finished job could run once more.** On finish the code wrote `done:<key>` and deleted `claim:<key>`. The
next delivery first tried to set `claim:<key>`, which was now empty, so it got "ok" and ran the job again.
Duplicates had never shown up in that system only because the second guards (a unique database key and an
`Idempotency-Key` on API calls) caught them.

**The API limit let 17 calls through instead of 16.** The add, rank and remove steps were separate commands,
so two workers could pass the check at the same moment.

**Fix:** both are now single atomic Lua scripts in `RedisStore`. The claim checks DONE first and sets the
claim in one round trip; the limiter removes expired slots, counts and adds in one step. DynamoDB passed both
tests from the start, because a conditional write is atomic by design.

## 5.3 End to end through SQS

[`tests/sqs_duplicate_demo.py`](../tests/sqs_duplicate_demo.py) uses the real SQS API (ElasticMQ):

- 50 jobs, each **sent twice** (a producer retry);
- a **3-second visibility timeout**, with some jobs taking 4 to 6 seconds, so they are also redelivered while
  still running;
- **6 workers** running [`src/worker_example.py`](../src/worker_example.py).

It counts how many times each job's side effect really ran.

| Where | Backend | SQS deliveries | Unique jobs | Side effect ran more than once | Never completed |
|---|---|---|---|---|---|
| Laptop | Redis | 105 | 50 | **0** | 0 |
| Laptop | DynamoDB | 104 | 50 | **0** | 0 |
| Kubernetes | Redis | 104 | 50 | **0** | 0 |
| Kubernetes | DynamoDB | 102 | 50 | **0** | 0 |

SQS delivered about twice as many messages as there were jobs; every job's side effect still happened exactly
once. Outputs: [`results/sqs-duplicate-demo-local.txt`](../results/sqs-duplicate-demo-local.txt),
[`results/sqs-duplicate-demo-kubernetes.txt`](../results/sqs-duplicate-demo-kubernetes.txt).

## 5.4 Full incident simulation

The claim store was also run inside a larger lab: two copies of a job platform side by side on Kubernetes,
fed identical traffic and failures for 15 minutes. One was the original design (a 30-second visibility timeout,
4 jobs per receive, no idempotency). The other was the fixed design, with **DynamoDB as its claim store**.

The traffic included bursts every 10 minutes, a 2-second database slowdown, two worker pods killed every
45 seconds, and 1% poison jobs.

| Run | Design | Interactive jobs within 30 s | p95 | Duplicate DB rows | Poison jobs parked in DLQ |
|---|---|---|---|---|---|
| 1 | Original | 68.2% | 96.8 s | 839 | none (retried forever) |
| 1 | Fixed, DynamoDB claims | **100%** | **14.9 s** | **0** | 14 |
| 2 | Original | 41.4% | 126.1 s | 1,013 | none (retried forever) |
| 2 | Fixed, DynamoDB claims | **100%** | **14.1 s** | **0** | 12 |

The same fixed design with Redis claims had scored 100% and 0 duplicates in earlier runs, with a p95 of
13 to 15 s. **Swapping the store changed nothing a user would see.** Raw results:
[`results/incident-run-1.json`](../results/incident-run-1.json),
[`results/incident-run-2.json`](../results/incident-run-2.json).

## 5.5 Limits

- DynamoDB Local and ElasticMQ are emulators. They match the real services for these operations, but not for
  latency, throttling, partition limits or TTL timing.
- The incident simulation compresses time (a 30-second target instead of minutes) and simulates the database
  and the AI provider.
