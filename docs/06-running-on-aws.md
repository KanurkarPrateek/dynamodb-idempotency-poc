# 6. Running it on AWS

## Table

```hcl
resource "aws_dynamodb_table" "job_claims" {
  name         = "job-claims"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery { enabled = false }   # claims are short-lived; nothing to restore
}
```

No secondary indexes are needed: every operation reads or writes one item by its key.

## Worker IAM policy

Least privilege: four item operations on this one table.

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"],
    "Resource": "arn:aws:dynamodb:<region>:<account>:table/job-claims"
  }]
}
```

If the table is created by Terraform (recommended), the workers do not need `CreateTable` or
`DescribeTable`. `DynamoStore` tries to create the table only when it is missing; with this policy that call
is simply denied, so create the table first.

## Network

Add a **gateway VPC endpoint** for DynamoDB on the workers' route tables. Traffic then stays inside the VPC
and skips the NAT gateway. Gateway endpoints have no hourly or data-processing charge.

## Worker configuration

| Variable | Value |
|---|---|
| `CLAIM_STORE` | `dynamodb` |
| `DDB_TABLE` | `job-claims` |
| `AWS_REGION` | the workers' region |
| `DDB_ENDPOINT` | **unset** (only for DynamoDB Local) |

Credentials come from the task or pod role (ECS task role, or EKS Pod Identity / IRSA), never from static keys.

## Rough cost

| Item | Per job | At 100,000 jobs a day (~3 M a month) |
|---|---|---|
| Writes: claim + done (+ heartbeats on long jobs) | ~3 | ~10 M writes a month |
| Reads: only when a claim fails | rare | small |
| Storage: items deleted by TTL after 24 h | tiny | under 1 GB |

On-demand pricing puts this in the range of a few dollars a month. That is an estimate from general
knowledge of DynamoDB pricing; confirm it in the AWS Pricing Calculator for your region.

## Production checks

| Risk | Mitigation |
|---|---|
| Sudden spikes throttle an on-demand table | The client retries (botocore standard mode, 3 attempts). Load-test the real table; consider provisioned capacity with auto scaling if traffic is very spiky and predictable. |
| DynamoDB unavailable | Keep the second guards (`UNIQUE(job_key)` in the database, `Idempotency-Key` on outside calls). Alert on claim errors. |
| Clock skew between workers | Leases use each worker's clock. Keep NTP on (default on AWS) and make leases much longer than any expected skew. |
| Hot keys | Every key is a different job, so writes spread across partitions. The 16 API slot items are the only shared items; at very high call rates, use more slots or a token bucket per worker. |

## Metrics to watch

- Claim latency p99 (`claim`, `done`, `extend`) from the worker.
- Claim results: `ok`, `done` (a repeat was skipped), `busy`. A rising `done` rate means SQS is redelivering a lot.
- `ConditionalCheckFailedRequests` and `ThrottledRequests` on the table, from CloudWatch.
- Duplicate side effects, from the database unique-key violations. This should stay at zero.
