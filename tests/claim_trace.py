"""Narrated simulation: what the DynamoDB idempotency table contains at each step of a job's life.
Uses the real DynamoStore (the same code the workers run) against a DynamoDB endpoint.
Prints the item for the job after every step, so you can see the keys change.
"""
import json
import os
import time
import uuid

os.environ.setdefault("CLAIM_STORE", "dynamodb")
import claimstore  # noqa: E402

LEASE = 4
TABLE = os.environ.get("DDB_TABLE", "job-claims-trace")


def store(owner):
    return claimstore.from_env(LEASE, 16, owner=owner)


def item(s, key):
    it = s.ddb.get_item(TableName=s.table_name, Key={"pk": {"S": f"claim#{key}"}}, ConsistentRead=True).get("Item")
    if not it:
        return "(no item)"
    out = {k: (v.get("S") or v.get("N")) for k, v in it.items()}
    now = time.time()
    if "lease_expires" in out:
        out["lease_expires"] = f"{out['lease_expires']} (in {int(out['lease_expires']) - now:+.0f} s)"
    out.pop("ttl", None)
    return json.dumps(out)


def step(text):
    print(f"\n  {text}", flush=True)


def show(s, key):
    print(f"      DynamoDB item  ->  {item(s, key)}", flush=True)


def title(n, text):
    print(f"\n{'=' * 78}\nSCENARIO {n}: {text}\n{'=' * 78}", flush=True)


def main():
    run = uuid.uuid4().hex[:6]
    a, b, c = store("worker-A"), store("worker-B"), store("worker-C")
    a.reset()
    print(f"Table: {a.table_name}   lease: {LEASE} s   backend: {a.name}")

    title(1, "a normal job")
    key = f"meeting-101:summary:v1#{run}"
    step("Before anything happens:"); show(a, key)
    step(f"worker-A receives the job from SQS and claims it  ->  {a.claim(key)}"); show(a, key)
    step("worker-A works (CRM write, AI call) and renews its lease (heartbeat)")
    time.sleep(1.5); a.extend(key); show(a, key)
    step("worker-A finishes: marks DONE, then deletes the SQS message")
    a.done(key); show(a, key)

    title(2, "the same job is delivered again after it finished")
    step(f"SQS delivers the job again; worker-B claims it  ->  {b.claim(key)}")
    step("worker-B sees DONE: skips the work and deletes the message. Nothing runs twice.")
    show(b, key)

    title(3, "two workers get the same job at the same time (job outlived the visibility timeout)")
    key = f"meeting-102:summary:v1#{run}"
    step(f"worker-A claims it  ->  {a.claim(key)}"); show(a, key)
    step(f"SQS hands the same message to worker-B; worker-B claims  ->  {b.claim(key)}")
    step("worker-B sees another owner: puts the message back for later and does no work")
    show(b, key)
    a.done(key)
    step("worker-A finishes and marks DONE"); show(a, key)
    step(f"worker-B's later retry claims  ->  {b.claim(key)}  (skipped)")

    title(4, "a worker is killed in the middle of a job")
    key = f"meeting-103:summary:v1#{run}"
    step(f"worker-A claims it  ->  {a.claim(key)}"); show(a, key)
    step("worker-A is killed (Spot reclaim). No heartbeat, no DONE, no release.")
    step(f"SQS redelivers; worker-B tries at once  ->  {b.claim(key)}  (lease still valid, so wait)")
    step(f"... {LEASE + 1} s later the lease has expired ...")
    time.sleep(LEASE + 1.2)
    show(b, key)
    step(f"worker-B claims again  ->  {b.claim(key)}  (takes over: owner changes)"); show(b, key)
    step("worker-A 'wakes up' late and tries to release the claim (it should not be able to)")
    a.release(key); show(b, key)
    step(f"worker-C tries to claim  ->  {c.claim(key)}  (still worker-B's)")
    b.done(key)
    step("worker-B finishes and marks DONE"); show(b, key)

    title(5, "a worker hands a job back on shutdown (SIGTERM)")
    key = f"meeting-104:summary:v1#{run}"
    step(f"worker-A claims  ->  {a.claim(key)}"); show(a, key)
    step("worker-A gets SIGTERM, cannot finish in time: releases the claim and sets SQS visibility to 0")
    a.release(key); show(a, key)
    step(f"worker-B picks it up immediately  ->  {b.claim(key)}"); show(b, key)

    title(6, "the API concurrency slots")
    toks = [a.api_acquire() for _ in range(3)]
    slots = a.ddb.scan(TableName=a.table_name, FilterExpression="begins_with(pk, :p)",
                       ExpressionAttributeValues={":p": {"S": "apislot#"}})["Items"]
    step(f"worker-A holds 3 of 16 API slots: {sorted(i['pk']['S'] for i in slots)}")
    for t in toks:
        a.api_release(t)
    left = a.ddb.scan(TableName=a.table_name, FilterExpression="begins_with(pk, :p)",
                      ExpressionAttributeValues={":p": {"S": "apislot#"}})["Count"]
    step(f"after release, slot items left: {left}")

    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
