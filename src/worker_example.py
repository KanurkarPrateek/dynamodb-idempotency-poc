"""Minimal SQS worker showing where the claim store plugs in.

The only idempotency-specific lines are the ones marked  # <-- claim store.
Which backend they use (Redis or DynamoDB) is decided by CLAIM_STORE, not by this code.

Flow per message:
  receive -> claim(key) -> work (with heartbeat) -> done(key) -> delete message
  claim says "done" -> skip and delete;  "busy" -> put back for later
"""
import json
import os
import signal
import threading
import time

import boto3

import claimstore

QUEUE_URL = os.environ["QUEUE_URL"]
VISIBILITY = int(os.environ.get("VISIBILITY", "20"))   # SQS visibility timeout, seconds
LEASE = VISIBILITY * 3                                  # claim lease, renewed by the heartbeat

sqs = boto3.client("sqs", endpoint_url=os.environ.get("SQS_ENDPOINT") or None,
                   region_name=os.environ.get("AWS_REGION", "us-east-1"))
stopping = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: stopping.set())


def heartbeat(store, key, receipt, finished):
    """Keep both the SQS message hidden and our claim alive while the job runs."""
    while not finished.wait(VISIBILITY / 3):
        try:
            sqs.change_message_visibility(QueueUrl=QUEUE_URL, ReceiptHandle=receipt, VisibilityTimeout=VISIBILITY)
        except sqs.exceptions.ReceiptHandleIsInvalid:
            pass        # message already redelivered elsewhere; the claim lease still protects the job
        except Exception:
            pass        # transient error: try again on the next beat
        store.extend(key)                                                    # <-- claim store


def handle(store, msg, do_work):
    job = json.loads(msg["Body"])
    key, receipt = job["key"], msg["ReceiptHandle"]   # key is set by the producer, e.g. "meeting-123:summary:v1"

    state = store.claim(key)                                                  # <-- claim store
    if state == "done":                     # already finished by someone: skip, nothing runs twice
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
        return "skipped"
    if state == "busy":                     # another worker is on it right now: try again later
        try:
            sqs.change_message_visibility(QueueUrl=QUEUE_URL, ReceiptHandle=receipt, VisibilityTimeout=10)
        except Exception:
            pass        # receipt expired: SQS will redeliver anyway
        return "busy"

    finished = threading.Event()
    threading.Thread(target=heartbeat, args=(store, key, receipt, finished), daemon=True).start()
    try:
        do_work(job)                        # CRM write, AI call, ... (pass the key as an Idempotency-Key too)
        store.done(key)                                                       # <-- claim store
        try:
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt)
        except Exception:
            pass        # a later redelivery will see DONE and be skipped
        return "done"
    except Exception:
        store.release(key)                                                    # <-- claim store
        sqs.change_message_visibility(QueueUrl=QUEUE_URL, ReceiptHandle=receipt, VisibilityTimeout=0)
        raise
    finally:
        finished.set()


def run(do_work, owner=None):
    store = claimstore.from_env(LEASE, 16, owner=owner)                       # <-- claim store
    while not stopping.is_set():
        msgs = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=2).get("Messages", [])
        for m in msgs:
            handle(store, m, do_work)


if __name__ == "__main__":
    run(lambda job: time.sleep(job.get("seconds", 1)))
