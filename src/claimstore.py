"""Claim store: the idempotency and API-limit backend for fixed-mode workers.

One interface, several backends, chosen by configuration (CLAIM_STORE=redis|dynamodb).
Workers never call Redis or DynamoDB directly, so switching backends is a config change.

Interface
  claim(key)        -> "ok" (you own it) | "done" (already finished) | "busy" (someone else is on it)
  extend(key)       -> renew your lease while you work (heartbeat)
  done(key)         -> mark finished; later deliveries of this key return "done"
  release(key)      -> give the key back (job failed or handed back on SIGTERM)
  api_acquire()     -> token, waits until one of API_LIMIT slots is free
  api_release(tok)  -> free the slot
Leases expire, so a key or slot held by a worker that died becomes available again.
"""
import os
import random
import socket
import time

HOST = socket.gethostname()


class ClaimStore:
    name = "base"

    def __init__(self, lease_s, api_limit, owner=None):
        self.lease_s, self.api_limit = int(lease_s), int(api_limit)
        self.me = owner or HOST  # identity of this worker; tests pass different owners to act as many workers


# ---------------------------------------------------------------- Redis
class RedisStore(ClaimStore):
    name = "redis"

    def __init__(self, url, lease_s, api_limit, owner=None):
        super().__init__(lease_s, api_limit, owner)
        import redis
        self.r = redis.Redis.from_url(url, socket_timeout=5)
        self._claim_lua = self.r.register_script(
            "if redis.call('EXISTS', KEYS[1]) == 1 then return 2 end "
            "if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'EX', ARGV[2]) then return 1 end "
            "if redis.call('EXISTS', KEYS[1]) == 1 then return 2 end return 0")
        self._sem_lua = self.r.register_script(
            "redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, tonumber(ARGV[2]) - 60) "
            "if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[3]) then "
            "redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1]) return 1 end return 0")

    def claim(self, key):
        # Atomic in one round trip: DONE wins; otherwise take the claim only if nobody holds it.
        res = self._claim_lua(keys=[f"done:{key}", f"claim:{key}"], args=[self.me, self.lease_s])
        return {1: "ok", 2: "done"}.get(int(res), "busy")

    def extend(self, key):
        if self.r.get(f"claim:{key}") == self.me.encode():
            self.r.expire(f"claim:{key}", self.lease_s)

    def done(self, key):
        self.r.set(f"done:{key}", 1, ex=86400)
        self.r.delete(f"claim:{key}")

    def release(self, key):
        if self.r.get(f"claim:{key}") == self.me.encode():
            self.r.delete(f"claim:{key}")

    def api_acquire(self, wait=lambda s: time.sleep(s)):
        token = f"{self.me}-{time.time_ns()}"
        while True:
            # Atomic: drop slots older than 60 s, then take one only if fewer than the limit are held.
            if int(self._sem_lua(keys=["sem:api"], args=[token, time.time(), self.api_limit])):
                return token
            wait(0.2 + random.random() * 0.2)

    def api_release(self, token):
        self.r.zrem("sem:api", token)

    def reset(self):
        self.r.flushdb()


# ---------------------------------------------------------------- DynamoDB
class DynamoStore(ClaimStore):
    """One table, partition key "pk".
    Claim items:   pk="claim#<key>", status IN_PROGRESS|DONE, owner, lease_expires (epoch s), ttl (epoch s)
    API slots:     pk="apislot#<n>", owner, lease_expires
    Every write is a conditional write, so two workers can never both win the same key or slot.
    """
    name = "dynamodb"

    def __init__(self, table, lease_s, api_limit, endpoint=None, region="us-east-1", owner=None):
        super().__init__(lease_s, api_limit, owner)
        import boto3
        from botocore.config import Config
        self.table_name = table
        self.ddb = boto3.client("dynamodb", endpoint_url=endpoint or None, region_name=region,
                                config=Config(retries={"max_attempts": 3, "mode": "standard"},
                                              connect_timeout=3, read_timeout=5, max_pool_connections=64))
        self.CCF = self.ddb.exceptions.ConditionalCheckFailedException
        self._ensure_table()

    def _ensure_table(self):
        try:
            self.ddb.describe_table(TableName=self.table_name)
            return
        except self.ddb.exceptions.ResourceNotFoundException:
            pass
        try:
            self.ddb.create_table(TableName=self.table_name, BillingMode="PAY_PER_REQUEST",
                                  AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                                  KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}])
        except self.ddb.exceptions.ResourceInUseException:
            pass  # another worker created it at the same moment
        self.ddb.get_waiter("table_exists").wait(TableName=self.table_name)
        try:  # real DynamoDB deletes expired items via TTL; DynamoDB Local accepts the call
            self.ddb.update_time_to_live(TableName=self.table_name,
                                         TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"})
        except Exception:
            pass

    @staticmethod
    def _n(x):
        return {"N": str(int(x))}

    def claim(self, key):
        now = time.time()
        try:
            self.ddb.put_item(
                TableName=self.table_name,
                Item={"pk": {"S": f"claim#{key}"}, "status": {"S": "IN_PROGRESS"}, "owner": {"S": self.me},
                      "lease_expires": self._n(now + self.lease_s), "ttl": self._n(now + 86400)},
                # new key, or an IN_PROGRESS key whose owner's lease ran out (owner died)
                ConditionExpression="attribute_not_exists(pk) OR (#s = :ip AND lease_expires < :now)",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":ip": {"S": "IN_PROGRESS"}, ":now": self._n(now)})
            return "ok"
        except self.CCF:
            item = self.ddb.get_item(TableName=self.table_name, Key={"pk": {"S": f"claim#{key}"}},
                                     ConsistentRead=True).get("Item", {})
            return "done" if item.get("status", {}).get("S") == "DONE" else "busy"

    def extend(self, key):
        try:
            self.ddb.update_item(
                TableName=self.table_name, Key={"pk": {"S": f"claim#{key}"}},
                UpdateExpression="SET lease_expires = :t",
                ConditionExpression="#o = :me AND #s = :ip",
                ExpressionAttributeNames={"#o": "owner", "#s": "status"},
                ExpressionAttributeValues={":t": self._n(time.time() + self.lease_s), ":me": {"S": self.me},
                                           ":ip": {"S": "IN_PROGRESS"}})
        except self.CCF:
            pass  # lease was lost; the unique key in the database is the second guard

    def done(self, key):
        self.ddb.put_item(
            TableName=self.table_name,
            Item={"pk": {"S": f"claim#{key}"}, "status": {"S": "DONE"}, "owner": {"S": self.me},
                  "ttl": self._n(time.time() + 86400)})

    def release(self, key):
        try:
            self.ddb.delete_item(
                TableName=self.table_name, Key={"pk": {"S": f"claim#{key}"}},
                ConditionExpression="#o = :me AND #s = :ip",
                ExpressionAttributeNames={"#o": "owner", "#s": "status"},
                ExpressionAttributeValues={":me": {"S": self.me}, ":ip": {"S": "IN_PROGRESS"}})
        except self.CCF:
            pass

    def api_acquire(self, wait=lambda s: time.sleep(s)):
        token = f"{self.me}-{time.time_ns()}"
        while True:
            now = time.time()
            for slot in random.sample(range(self.api_limit), self.api_limit):
                try:
                    self.ddb.put_item(
                        TableName=self.table_name,
                        Item={"pk": {"S": f"apislot#{slot}"}, "owner": {"S": token},
                              "lease_expires": self._n(now + 60)},
                        ConditionExpression="attribute_not_exists(pk) OR lease_expires < :now",
                        ExpressionAttributeValues={":now": self._n(now)})
                    return f"{slot}|{token}"
                except self.CCF:
                    continue
            wait(0.2 + random.random() * 0.2)

    def api_release(self, token):
        slot, owner = token.split("|", 1)
        try:
            self.ddb.delete_item(TableName=self.table_name, Key={"pk": {"S": f"apislot#{slot}"}},
                                 ConditionExpression="#o = :me",
                                 ExpressionAttributeNames={"#o": "owner"},
                                 ExpressionAttributeValues={":me": {"S": owner}})
        except self.CCF:
            pass

    def reset(self):
        try:
            self.ddb.delete_table(TableName=self.table_name)
            self.ddb.get_waiter("table_not_exists").wait(TableName=self.table_name)
        except self.ddb.exceptions.ResourceNotFoundException:
            pass
        self._ensure_table()


# ---------------------------------------------------------------- factory
def from_env(lease_s, api_limit, owner=None):
    kind = os.environ.get("CLAIM_STORE", "redis").lower()
    if kind == "dynamodb":
        return DynamoStore(os.environ.get("DDB_TABLE", "job-claims"), lease_s, api_limit,
                           endpoint=os.environ.get("DDB_ENDPOINT"), region=os.environ.get("AWS_REGION", "us-east-1"),
                           owner=owner)
    return RedisStore(os.environ.get("REDIS_URL", "redis://redis:6379/0"), lease_s, api_limit, owner=owner)
