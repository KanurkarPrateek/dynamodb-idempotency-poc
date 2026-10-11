# 4. Switching a live system from Redis to DynamoDB

With the claim-store interface in place, the switch itself is configuration
([docs/01](01-do-we-need-a-code-change.md)). There is one catch on a running system.

## The catch

At the moment you switch, Redis holds:

- `DONE` markers for recently finished jobs;
- claims for jobs that are in progress right now.

The new DynamoDB table holds neither. A job finished just before the switch, and redelivered just after it,
finds no DONE marker and runs again. A job in progress on an old worker can be claimed by a new one.

## Three safe ways to switch

| Option | How | Trade-off |
|---|---|---|
| **Drain first** | Pause producers (or route new jobs to a holding queue), let the queue empty, switch, resume. | Simplest and cleanest; a few minutes of delayed processing. |
| **Rely on the second guards** | Switch directly. `UNIQUE(job_key)` in the database and the `Idempotency-Key` on outside calls stop a repeated job from doing harm. | No pause, but repeats still cost compute during the switch window. |
| **Dual-read for a day** | A small wrapper store checks DynamoDB first and Redis second for `done`, and writes only to DynamoDB. Remove it after 24 hours (the TTL of DONE markers). | No pause and no repeats; one extra class to write and later delete. |

A dual-read wrapper is about 20 lines because it implements the same interface:

```python
class DualReadStore:
    def __init__(self, new, old):
        self.new, self.old, self.name = new, old, "dual"
    def claim(self, key):
        if self.old.r.exists(f"done:{key}"):    # finished before the switch
            return "done"
        return self.new.claim(key)
    def extend(self, key):        self.new.extend(key)
    def done(self, key):          self.new.done(key)
    def release(self, key):       self.new.release(key)
    def api_acquire(self, **kw):  return self.new.api_acquire(**kw)
    def api_release(self, token): self.new.api_release(token)
```

(Not included in `src/` because it is temporary by design.)

## Rollout order

1. Create the table and the IAM permissions; deploy nothing else yet.
2. Run `tests/claim_poc.py` against the real table from inside the VPC.
3. Switch a canary group of workers (`CLAIM_STORE=dynamodb`) and watch duplicate counts and claim latency.
4. Switch the rest, using one of the three options above.
5. After 24 hours, remove the Redis claim keys (or the Redis cluster, if nothing else uses it).

Rollback is the same variable set back to `redis`, with the same catch in the other direction.
