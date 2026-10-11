"""End-to-end demo with a real SQS API (ElasticMQ locally, or Amazon SQS).

1. Send 50 jobs, then send all 50 again (100 messages, 50 unique keys).
2. Use a short visibility timeout so some slow jobs are also redelivered while still running.
3. Run 6 workers (src/worker_example.py) for a while.
4. Count how many times each job's "side effect" really happened.

Pass: every one of the 50 jobs has exactly one side effect, although SQS delivered more than 50 messages.
Run it with CLAIM_STORE=dynamodb and with CLAIM_STORE=redis: the worker code is the same.
"""
import collections
import json
import os
import random
import threading
import time
import uuid

import boto3

os.environ.setdefault("VISIBILITY", "3")
import worker_example  # noqa: E402

sqs = worker_example.sqs
QUEUE_URL = worker_example.QUEUE_URL
effects = collections.Counter()
deliveries = collections.Counter()
lock = threading.Lock()


def do_work(job):
    time.sleep(job["seconds"])          # some jobs are longer than the 3 s visibility timeout
    with lock:
        effects[job["key"]] += 1        # the side effect we must never repeat (CRM write, AI call...)


def counted_handle(store, msg, work):
    with lock:
        deliveries[json.loads(msg["Body"])["key"]] += 1
    return original_handle(store, msg, work)


original_handle = worker_example.handle
worker_example.handle = counted_handle


def main():
    run = uuid.uuid4().hex[:6]
    try:  # local emulators start empty; on AWS the queue already exists
        sqs.create_queue(QueueName=QUEUE_URL.rsplit("/", 1)[1])
    except Exception:
        pass
    sqs.purge_queue(QueueUrl=QUEUE_URL)
    time.sleep(1)
    jobs = [{"key": f"meeting-{i}:summary:v1#{run}", "seconds": random.choice([0.2, 0.5, 1, 4, 6])} for i in range(50)]
    for _ in range(2):                  # every job is sent twice: producer retries happen in real life
        for i in range(0, 50, 10):
            sqs.send_message_batch(QueueUrl=QUEUE_URL, Entries=[
                {"Id": str(n), "MessageBody": json.dumps(j)} for n, j in enumerate(jobs[i:i + 10])])
    workers = [threading.Thread(target=worker_example.run, args=(do_work, f"worker-{n}"), daemon=True) for n in range(6)]
    [w.start() for w in workers]
    deadline = time.time() + 90
    while time.time() < deadline and len(effects) < 50:
        time.sleep(1)
    time.sleep(8)                       # let any late redeliveries arrive and be skipped
    worker_example.stopping.set()

    total_deliveries = sum(deliveries.values())
    repeats = sum(1 for k in effects if effects[k] > 1)
    missing = 50 - len(effects)
    ok = repeats == 0 and missing == 0
    print(f"backend: {os.environ.get('CLAIM_STORE', 'redis')}")
    print(f"messages delivered by SQS: {total_deliveries} for 50 unique jobs")
    print(f"jobs whose side effect ran more than once: {repeats}")
    print(f"jobs never completed: {missing}")
    print("PASS" if ok else "FAIL")
    print("RESULT " + json.dumps({"backend": os.environ.get("CLAIM_STORE", "redis"), "deliveries": total_deliveries,
                                  "unique_jobs": 50, "repeated_effects": repeats, "missing": missing, "passed": ok}))


if __name__ == "__main__":
    main()
