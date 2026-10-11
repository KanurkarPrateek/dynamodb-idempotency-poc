"""Proof-of-concept test suite for the claim store. Runs the same 8 tests against the store chosen by
CLAIM_STORE (redis or dynamodb) and prints one JSON result line at the end.

  1 race          20 workers claim the same key at the same instant -> exactly one wins, for 50 keys
  2 done-skip     a key that is DONE returns "done" to every later delivery
  3 crash         a worker claims and dies (no heartbeat); after the lease runs out a new worker takes over
  4 heartbeat     a worker that keeps extending its lease is never taken over
  5 release       a handed-back key can be claimed immediately by another worker
  6 stale owner   a worker whose lease was taken over cannot release the new owner's claim
  7 api limit     64 threads share 16 API slots; never more than 16 at once
  8 latency       1,000 claim + done cycles, p50 / p99 per operation
"""
import json
import os
import statistics
import threading
import time
import uuid

import claimstore

LEASE = 3  # short lease so the crash test runs fast
results, ok_all = [], True


def store(owner):
    return claimstore.from_env(LEASE, 16, owner=owner)


def check(name, passed, detail):
    global ok_all
    ok_all = ok_all and passed
    results.append({"test": name, "passed": passed, "detail": detail})
    print(f"{'PASS' if passed else 'FAIL'}  {name}: {detail}", flush=True)


def main():
    run = uuid.uuid4().hex[:8]
    admin = store("admin")
    print(f"backend: {admin.name}", flush=True)
    admin.reset()

    # 1 race
    workers = [store(f"w{i}") for i in range(20)]
    bad = 0
    for k in range(50):
        key = f"{run}-race-{k}"
        wins, barrier = [], threading.Barrier(20)

        def go(w):
            barrier.wait()
            if w.claim(key) == "ok":
                wins.append(w.me)
        ts = [threading.Thread(target=go, args=(w,)) for w in workers]
        [t.start() for t in ts]
        [t.join() for t in ts]
        bad += len(wins) != 1
    check("race", bad == 0, f"50 keys x 20 simultaneous workers: {50 - bad} keys had exactly one winner")

    # 2 done-skip
    a, b = workers[0], workers[1]
    key = f"{run}-done"
    first = a.claim(key)
    a.done(key)
    later = [b.claim(key) for _ in range(5)]
    check("done-skip", first == "ok" and all(x == "done" for x in later), f"first={first}, later deliveries={later}")

    # 3 crash and takeover
    key = f"{run}-crash"
    a.claim(key)  # a "dies": never extends, never releases
    immediately = b.claim(key)
    time.sleep(LEASE + 1.5)
    after = b.claim(key)
    check("crash-takeover", immediately == "busy" and after == "ok",
          f"while lease valid: {immediately}; after lease expired: {after}")

    # 4 heartbeat keeps ownership
    key = f"{run}-hb"
    a.claim(key)
    seen = []
    for _ in range(4):
        time.sleep(LEASE / 2)
        a.extend(key)
        seen.append(b.claim(key))
    check("heartbeat", all(x == "busy" for x in seen), f"other worker saw {seen} over {2 * LEASE} s (longer than the lease)")

    # 5 release
    key = f"{run}-rel"
    a.claim(key)
    a.release(key)
    got = b.claim(key)
    check("release", got == "ok", f"after hand-back, another worker got: {got}")

    # 6 stale owner cannot release the new owner's claim
    key = f"{run}-stale"
    a.claim(key)
    time.sleep(LEASE + 1.5)
    b_got = b.claim(key)       # b takes over after a's lease expired
    a.release(key)             # a wakes up late and tries to give the key back
    c_got = workers[2].claim(key)
    check("stale-owner", b_got == "ok" and c_got == "busy",
          f"b took over: {b_got}; after a's late release a third worker sees: {c_got}")

    # 7 API limit
    lock, live, peak, calls = threading.Lock(), [0], [0], [0]

    def caller(i):
        s = workers[i % 20]
        for _ in range(5):
            tok = s.api_acquire()
            with lock:
                live[0] += 1
                peak[0] = max(peak[0], live[0])
            time.sleep(0.05)
            with lock:
                live[0] -= 1
                calls[0] += 1
            s.api_release(tok)
    ts = [threading.Thread(target=caller, args=(i,)) for i in range(64)]
    t0 = time.time()
    [t.start() for t in ts]
    [t.join() for t in ts]
    check("api-limit", peak[0] <= 16, f"{calls[0]} calls by 64 threads, peak concurrent {peak[0]} (limit 16), {time.time() - t0:.1f} s")

    # 8 latency
    lat = {"claim": [], "done": []}
    for i in range(1000):
        key = f"{run}-lat-{i}"
        t = time.time(); a.claim(key); lat["claim"].append(time.time() - t)
        t = time.time(); a.done(key); lat["done"].append(time.time() - t)
    q = lambda xs, p: sorted(xs)[int(len(xs) * p) - 1] * 1000
    detail = {op: {"p50_ms": round(statistics.median(xs) * 1000, 2), "p99_ms": round(q(xs, 0.99), 2)} for op, xs in lat.items()}
    check("latency", True, detail)

    print("RESULT " + json.dumps({"backend": admin.name, "all_passed": ok_all, "results": results}), flush=True)


if __name__ == "__main__":
    main()
