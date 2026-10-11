"""Wait until Redis, DynamoDB and the SQS endpoint accept connections (used by the Kubernetes Jobs)."""
import os
import socket
import time
import urllib.parse

targets = [os.environ.get("REDIS_URL"), os.environ.get("DDB_ENDPOINT"), os.environ.get("SQS_ENDPOINT")]
for url in filter(None, targets):
    u = urllib.parse.urlparse(url)
    for _ in range(120):
        try:
            socket.create_connection((u.hostname, u.port), timeout=2).close()
            break
        except OSError:
            time.sleep(2)
    else:
        raise SystemExit(f"{url} not reachable")
print("backends ready", flush=True)
