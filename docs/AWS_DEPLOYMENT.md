# Enqurious Streaming API — AWS deployment runbook

Everything needed to understand, rebuild, release and tear down the Clickstream Streaming API on AWS.

**Status (October 2026):** taken down while no program is running. Use [Rebuild from scratch](#6-rebuild-from-scratch) to bring it back. The last released version is **v5** (git tag `v5`, merged into `dev` and `main`).

---

## Contents

1. [What the API does](#1-what-the-api-does)
2. [Architecture](#2-architecture)
3. [AWS resources](#3-aws-resources)
4. [DynamoDB](#4-dynamodb)
5. [How streaming works](#5-how-streaming-works)
6. [Rebuild from scratch](#6-rebuild-from-scratch)
7. [Releasing and rolling back](#7-releasing-and-rolling-back)
8. [Tear down](#8-tear-down)
9. [Costs](#9-costs)
10. [Client network whitelist](#10-client-network-whitelist)
11. [Testing approach and load-test results](#11-testing-approach-and-load-test-results)
12. [Release history](#12-release-history)
13. [Known issues and next steps](#13-known-issues-and-next-steps)
14. [Gotchas on this machine](#14-gotchas-on-this-machine)

---

## 1. What the API does

Learners open the web page, pick **Azure Event Hubs** or **Google Pub/Sub**, enter their own stream's details, and click **Start streaming**. The API reads sample Amazon clickstream sessions from S3 and sends them as live events (logins, searches, product views, add-to-cart, checkout) into the learner's stream — one event every 3–5 seconds per session batch, about 50–65 events/minute per learner. The page's **Stream Console** shows every event as it is sent, so learners can match it against what arrives in their own stream.

- Each learner's stream is identified by their **email**.
- The page and API are protected by a shared **access token** (`access_token` request header).
- Streams stop when the learner clicks **Stop**, after **3 hours automatically**, or on an error.

---

## 2. Architecture

```
 Learner's browser
        │  HTTPS (only host the page ever contacts)
        ▼
 ECS Express Mode endpoint  https://cl-<id>.ecs.us-east-1.on.aws
        │  (ALB with managed HTTPS certificate, created by Express Mode)
        ▼
 Fargate task: 1 vCPU / 2 GB, exactly 1 task
 uvicorn main:app, 4 workers, port 8000
        │
        ├── S3  enqurious-clickstream-data        (reads session files)
        ├── DynamoDB  amazon-ecommerce-streaming-api  (one row per job: start/stop/finish)
        ├── DynamoDB  clickstream-job-activity        (live feed, counters, errors, stop flag)
        └── Secrets Manager  clickstream-api/access-token  (injected as ACCESS_TOKEN)
        │
        ▼  outbound, from a non-fixed AWS IP
 Learner's Azure Event Hub (AMQP 5671/5672)  or  Google Pub/Sub (443)
```

Key points:

- **Region: us-east-1 (N. Virginia)** for everything.
- **No AWS keys in the container.** boto3 uses the ECS **task role**. (`constant.py` still reads `aws-access-key-id` env vars for local runs; on ECS they are simply absent.)
- `constant.py` / `auth.py` only load `.env` when `AWS_REGION` is **not** set. Fargate sets `AWS_REGION`, so `.env` is never used in the container (and `.dockerignore` keeps it out of the image).
- **Exactly one task.** Each job runs as a background thread inside the worker process that received the start request. See [Known issues](#13-known-issues-and-next-steps) before scaling out.

---

## 3. AWS resources

All in account `512357470856`, region `us-east-1`.

### Created for this API (safe to delete when taking it down)

| Resource | Name / ID | Notes |
|---|---|---|
| ECS Express Mode service | `clickstream-api` in cluster `default` | Express Mode also auto-created: ALB + HTTPS listener + target group, security groups, ACM certificate, auto-scaling target/policy, deployment alarm, log group `/aws/ecs/default/clickstream-api-3d89`. Deleting the Express service removes these. |
| ECS cluster | `default` | Created for this API. Check nothing else uses it before deleting. |
| ECR repository | `enqurious/clickstream-api` | Image tags `v1`–`v5` (tags = git tags). Scan-on-push enabled. |
| IAM role | `ecsInfrastructureRoleForExpressServices` | Trust: `ecs.amazonaws.com`. Managed policy `AmazonECSInfrastructureRoleforExpressGatewayServices`. Used by Express Mode to manage ALB/SGs. |
| IAM role | `clickstream-api-task-role` | Trust: `ecs-tasks.amazonaws.com` (with `aws:SourceAccount`). Inline policy `s3-dynamodb-access` — see [6.3](#63-iam-roles). |
| IAM inline policy | `clickstream-api-secret` on `ecsTaskExecutionRole` | `secretsmanager:GetSecretValue` on the access-token secret. |
| Secrets Manager secret | `clickstream-api/access-token` | Value = the API access token (same as `ACCESS_TOKEN` in `.env`). ARN suffix was `-cN0Vde`; a recreated secret gets a new suffix. |
| DynamoDB table | `clickstream-job-activity` | On-demand, TTL on `expires_at`. See [DynamoDB](#4-dynamodb). |

### Pre-existing — used by the API, do NOT delete with it

| Resource | Why keep |
|---|---|
| S3 bucket `enqurious-clickstream-data` (us-east-1) | The source data: `session_files_indexes.json` plus ~54,000 session JSON files. Without it there is nothing to stream. |
| DynamoDB table `amazon-ecommerce-streaming-api` | Historical job records since 2024. Cheap to keep (provisioned 1 RCU / 1 WCU). |
| IAM role `ecsTaskExecutionRole` | Shared AWS default role (`AmazonECSTaskExecutionRolePolicy`, `CloudWatchEventsFullAccess`). Only remove our inline policy from it. |
| Other ECR repos (`enqurious/app`, `enqurious/backend`, …) | Unrelated projects. |

> The very first EC2-based version used an `ap-south-1` setup. Nothing from this ECS deployment lives in ap-south-1.

---

## 4. DynamoDB

### 4.1 `amazon-ecommerce-streaming-api` — job records (pre-existing)

| Setting | Value |
|---|---|
| Keys | `email_id` (HASH, S), `called_at` (RANGE, S — `YYYY-MM-DD HH:MM:SS`, container UTC) |
| Capacity | **Provisioned 1 RCU / 1 WCU**, auto-scaling 1–10 |
| Attributes | `status`: `in_progress` · `stopped` · `completed` · `failed` · `interrupted` |

Because it only has 1 WCU, the code writes here **only** at start, stop, finish and failure — never per event. `read_data_by_keys` finds a user's `in_progress` job (used to block a second concurrent stream); `read_latest_by_email` powers "Check its status".

### 4.2 `clickstream-job-activity` — live progress (created in v4)

| Setting | Value |
|---|---|
| Keys | `job_id` (HASH, S) = `"<email>#<called_at>"`, `seq` (RANGE, N) |
| Capacity | **On-demand** (pay per request, cannot throttle at our volume) |
| TTL | `expires_at` — every item auto-deletes 24 h after it is written |

Item layout per job:

- **`seq = 0` — summary:** `email_id`, `cloud_platform`, `target` (hub/topic), `status`, `started_at`, `last_event_at`, `events_sent`, `stop_requested`, `max_duration_seconds`, `finished_at`, `stop_reason` (`user` / `time_limit`), `error_message`, `error_source` (`user` / `server`), `error_detail`.
- **`seq = 1, 2, 3 …` — one item per sent event:** `sent_at`, `batch`, `event_type`, `session_id`, `user_id`, `payload` (exact JSON sent).

Per sent event the job does 2 writes (event item + summary counter). Each open console polls every 2 s (summary `GetItem` + events `Query`). The running job checks the stop flag every 3 s.

### 4.3 Can it handle many learners?

Measured ~48–66 events/min per learner. For **50 learners**: ~80 writes/s and ~65 reads/s against a default on-demand limit of 4,000 writes/s and 12,000 reads/s — about 2%. Cost is about **$0.004 per learner-hour** (50 learners × 3 h ≈ $0.60). Storage is negligible thanks to the 24 h TTL. DynamoDB is **not** the bottleneck; the container is (see [11](#11-testing-approach-and-load-test-results)).

---

## 5. How streaming works

### 5.1 Code map

| File | Role |
|---|---|
| `main.py` | FastAPI app: page, `/favicon.ico`, `/data_generation`, `/stop_processing`, `/jobs/latest`, `/jobs/activity` |
| `before_data_generation.py` | Validates input, releases stale jobs, **connection check before start**, creates the job, starts the background thread |
| `data_generation.py` | `AzureSender` / `GCPSender` (one connection per stream), the 5-batch event loop, stop / time-limit / failure handling |
| `job_activity.py` | `JobActivity` (activity table access) and `JobTracker` (per-run counters, stop flag, time limit, failure reporting) |
| `stream_errors.py` | Turns Azure / GCP exceptions into plain-English messages tagged `user` or `server`; preflight checks |
| `aws_clients.py` | One shared boto3 client per worker for DynamoDB / activity / S3 |
| `dynamod_db.py`, `s3.py`, `auth.py`, `constant.py` | Table helpers, S3 helper, access-token check, settings |
| `templates/index.html`, `static/` | UI (always light theme), JS console, logo, favicon set |
| `Dockerfile`, `.dockerignore`, `deploy.sh` | Container build and release script |
| `modify_config_file.py`, `config.json`, `eventhub_upload.py`, `main_copy.py`, `start.sh` | Leftovers from the EC2 era, no longer used by the running app |

### 5.2 Lifecycle of a stream

1. **Start** — `POST /data_generation` with `cloud_provider`, `email_id` and either `connection_string` + `hub_name` (Azure) or `credentials` (key JSON as a string) + `project_id` + `topic_id` (GCP).
2. **Already running?** If the email has an `in_progress` job that is still alive, the API returns `"Work is in progress!"` with its `job_id` and the UI attaches to it. Jobs with no activity for **180 s** (container restarted) or with no activity record at all (left over from the old EC2 server) are marked `interrupted` and released.
3. **Connection check (preflight)** — before anything is written:
   - Azure: opens a sender link (`create_batch()`) — validates namespace, key, Send permission and hub name without sending data.
   - GCP: refreshes the service-account token — validates the key. (Topic/permission problems surface on the first publish because the Pub/Sub Publisher role cannot read topic metadata.)
   - A failure returns **400 with a readable message**; no job is created.
4. **Job created** — row in the jobs table (`in_progress`) + summary item in the activity table; background thread starts with its own asyncio loop.
5. **Sending** — session files are split round-robin into **5 concurrent batches**. All batches share **one** Azure producer / Pub/Sub publisher. Each event gets `session_id` and a fresh `timestamp`, is sent, then recorded; 3–5 s random gap per batch.
6. **Before every event** the loop checks: stop requested (DynamoDB flag, cached 3 s) → `stopped` (`stop_reason=user`); running ≥ **3 hours** (`MAX_STREAM_SECONDS`) → `stopped` (`stop_reason=time_limit`).
7. **Failure** — the first send error stops all batches, is recorded once (`error_message`, `error_source`, `error_detail`) and the job becomes `failed`. Unexpected errors (S3, DynamoDB, bugs) are reported as `server` with the job id as reference. A failure to *record* an event is logged but never stops the stream.
8. **End** — sender connection closed; both tables updated with the final status. The learner can start again immediately (a new stream starts from the first session; it does not resume).

### 5.3 Error messages learners can see

| Situation | Message (source) |
|---|---|
| Malformed connection string | "The connection string is not in the expected format…" (user) |
| Unknown namespace host | "Could not find the Event Hubs namespace…" (user) |
| Hub name wrong | "Event hub '…' was not found in this namespace…" (user) |
| Wrong key / no Send permission / key rotated | "Azure rejected the credentials…" (user) |
| Firewall / private endpoint on namespace | "Could not connect to Azure Event Hubs…" (user) |
| Bad / deleted GCP key | "The service account key file is not valid…" / "Google rejected the service account key…" (user) |
| Topic missing / no publisher role | "Topic '…' was not found…" / "…not allowed to publish… Pub/Sub Publisher role" (user) |
| Anything else | "…error on our side. Please contact us with reference <job_id>" (server) |

Access keys are masked in `error_detail`.

### 5.4 API endpoints

All paths are on the one host. API calls need the `access_token` header.

| Method | Path | Purpose |
|---|---|---|
| GET | `/`, `/static/*`, `/favicon.ico` | Page, CSS/JS, logo and icons (no auth) |
| POST | `/data_generation` | Start a stream (JSON body) |
| POST | `/stop_processing` | Stop the email's running stream (`{"email_id": …}`) |
| GET | `/jobs/latest?email_id=` | Latest job for an email (reconnect after reload) |
| GET | `/jobs/activity?job_id=&after=&limit=` | Live summary + events with `seq > after` (UI polls every 2 s) |

FastAPI's automatic `/docs` and `/openapi.json` are also live (the UI does not use them).

---

## 6. Rebuild from scratch

Run from Git Bash in the repo root unless noted. Prerequisites: AWS CLI v2 signed in to account `512357470856`, Docker Desktop running, the `ACCESS_TOKEN` value (from your local `.env`).

```bash
export REGION=us-east-1
export ACCOUNT_ID=512357470856
export REGISTRY=$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com
```

### 6.1 Check the pre-existing data

```bash
aws s3api head-object --bucket enqurious-clickstream-data --key session_files_indexes.json
aws dynamodb describe-table --table-name amazon-ecommerce-streaming-api --region $REGION --query Table.TableStatus
```

If the jobs table is missing, recreate it (provisioned 1/1 is enough):

```bash
aws dynamodb create-table --region $REGION --table-name amazon-ecommerce-streaming-api \
  --attribute-definitions AttributeName=email_id,AttributeType=S AttributeName=called_at,AttributeType=S \
  --key-schema AttributeName=email_id,KeyType=HASH AttributeName=called_at,KeyType=RANGE \
  --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1
```

### 6.2 Activity table

```bash
aws dynamodb create-table --region $REGION --table-name clickstream-job-activity \
  --attribute-definitions AttributeName=job_id,AttributeType=S AttributeName=seq,AttributeType=N \
  --key-schema AttributeName=job_id,KeyType=HASH AttributeName=seq,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST
aws dynamodb wait table-exists --region $REGION --table-name clickstream-job-activity
aws dynamodb update-time-to-live --region $REGION --table-name clickstream-job-activity \
  --time-to-live-specification Enabled=true,AttributeName=expires_at
```

### 6.3 IAM roles

Save these three files in a temp folder.

`ecs-trust.json` (infrastructure role):
```json
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs.amazonaws.com"},"Action":"sts:AssumeRole"}]}
```

`task-trust.json` (task role):
```json
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"aws:SourceAccount":"512357470856"}}}]}
```

`task-policy.json` (what the app may touch):
```json
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["s3:ListBucket","s3:GetBucketLocation"],"Resource":"arn:aws:s3:::enqurious-clickstream-data"},
 {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::enqurious-clickstream-data/*"},
 {"Effect":"Allow","Action":["s3:ListAllMyBuckets"],"Resource":"*"},
 {"Effect":"Allow","Action":["dynamodb:GetItem","dynamodb:PutItem","dynamodb:UpdateItem","dynamodb:DeleteItem","dynamodb:Query","dynamodb:Scan","dynamodb:DescribeTable"],"Resource":"arn:aws:dynamodb:us-east-1:512357470856:table/amazon-ecommerce-streaming-api"},
 {"Effect":"Allow","Action":["dynamodb:GetItem","dynamodb:PutItem","dynamodb:UpdateItem","dynamodb:Query","dynamodb:BatchWriteItem"],"Resource":"arn:aws:dynamodb:us-east-1:512357470856:table/clickstream-job-activity"}
]}
```

```bash
aws iam create-role --role-name ecsInfrastructureRoleForExpressServices --assume-role-policy-document file://ecs-trust.json
aws iam attach-role-policy --role-name ecsInfrastructureRoleForExpressServices \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSInfrastructureRoleforExpressGatewayServices

aws iam create-role --role-name clickstream-api-task-role --assume-role-policy-document file://task-trust.json
aws iam put-role-policy --role-name clickstream-api-task-role --policy-name s3-dynamodb-access --policy-document file://task-policy.json

# Execution role is an AWS default; create it only if it no longer exists
aws iam get-role --role-name ecsTaskExecutionRole >/dev/null 2>&1 || {
  aws iam create-role --role-name ecsTaskExecutionRole --assume-role-policy-document \
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
  aws iam attach-role-policy --role-name ecsTaskExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
}
```

### 6.4 Access-token secret

```bash
SECRET_ARN=$(aws secretsmanager create-secret --region $REGION --name clickstream-api/access-token \
  --secret-string "<ACCESS_TOKEN value from .env>" --query ARN --output text)
echo $SECRET_ARN    # note it - the random suffix is new every time

aws iam put-role-policy --role-name ecsTaskExecutionRole --policy-name clickstream-api-secret --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"secretsmanager:GetSecretValue\",\"Resource\":\"$SECRET_ARN\"}]}"
```

> If the secret was deleted with a recovery window, `create-secret` fails with "scheduled for deletion". Restore it with `aws secretsmanager restore-secret --secret-id clickstream-api/access-token` instead.

### 6.5 ECR repository and first image

```bash
aws ecr create-repository --region $REGION --repository-name enqurious/clickstream-api \
  --image-scanning-configuration scanOnPush=true

git checkout v5            # or the release you want; use main for the latest
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REGISTRY
docker build -t $REGISTRY/enqurious/clickstream-api:v5 .
docker push $REGISTRY/enqurious/clickstream-api:v5
```

### 6.6 Cluster and Express service

```bash
aws ecs create-cluster --region $REGION --cluster-name default
```

`express.json` (replace `<SECRET_ARN>`):
```json
{
  "serviceName": "clickstream-api",
  "cluster": "default",
  "executionRoleArn": "arn:aws:iam::512357470856:role/ecsTaskExecutionRole",
  "infrastructureRoleArn": "arn:aws:iam::512357470856:role/ecsInfrastructureRoleForExpressServices",
  "taskRoleArn": "arn:aws:iam::512357470856:role/clickstream-api-task-role",
  "healthCheckPath": "/",
  "cpu": "1024",
  "memory": "2048",
  "scalingTarget": {"minTaskCount": 1, "maxTaskCount": 1},
  "primaryContainer": {
    "image": "512357470856.dkr.ecr.us-east-1.amazonaws.com/enqurious/clickstream-api:v5",
    "containerPort": 8000,
    "environment": [
      {"name": "aws-region", "value": "us-east-1"},
      {"name": "dynamodb-region", "value": "us-east-1"}
    ],
    "secrets": [{"name": "ACCESS_TOKEN", "valueFrom": "<SECRET_ARN>"}]
  }
}
```

```bash
aws ecs create-express-gateway-service --region $REGION --cli-input-json file://express.json \
  --query "service.activeConfigurations[0].ingressPaths[0].endpoint" --output text
```

The command prints the new **HTTPS URL**. First start takes ~5 minutes; watch with:

```bash
aws ecs describe-services --cluster default --services clickstream-api --region $REGION \
  --query "services[0].[runningCount,deployments[0].rolloutState]" --output text
```

Settings that matter and why:

- **Health check path `/`** — Express Mode's default is `/ping`, which this app does not have.
- **Port 8000** — uvicorn's port in the `Dockerfile`.
- **min = max = 1 task** — see [Known issues](#13-known-issues-and-next-steps).
- **Hyphenated env var names** (`aws-region`, `dynamodb-region`) are what `constant.py` reads; ECS accepts them.
- **Default VPC public subnets** (no `networkConfiguration`). Giving Express Mode private subnets would make its load balancer *internal* and take the public URL offline.

### 6.7 Update `deploy.sh` for the new environment

Two values change whenever the service/secret are recreated:

- `SECRET_ARN=` → the new secret ARN from 6.4.
- The final `echo "Done: https://…"` line → the new URL from 6.6.

`SERVICE_ARN` stays the same as long as the service is called `clickstream-api` in cluster `default`.

### 6.8 Verify

```bash
URL=https://<new-endpoint>
curl -s -o /dev/null -w "%{http_code}\n" $URL/                     # 200
curl -s $URL/ | grep -o 'href="https://[^"]*index.css'               # https:// links (proxy headers working)
curl -s -o /dev/null -w "%{http_code}\n" $URL/favicon.ico            # 200
curl -s -X POST $URL/data_generation -H 'Content-Type: application/json' -H "access_token: <token>" \
  -d '{"cloud_provider":"Azure","email_id":"check@enqurious.com","hub_name":"h","connection_string":"Endpoint=sb://dummy.invalid/;SharedAccessKeyName=d;SharedAccessKey=ZHVtbXk="}'
# -> 400 "Could not find the Event Hubs namespace..."  (preflight, task role and secret all working)
```

Then run one real stream from the UI against a real Event Hub and click Stop.

### 6.9 Tell clients

The URL is new, so the whitelist changes — update `docs/network-whitelist.csv` and resend it. To avoid this next time, put the custom domain `stream-api.enqurious.com` in front of the service (already on the whitelist sent to clients; needs one DNS record on `enqurious.com` plus an ACM certificate).

---

## 7. Releasing and rolling back

Git flow: feature branch off `dev` → PR into `dev` → PR `dev` into `main`. Each release gets a git tag that matches its image tag.

```bash
# New release (builds the current code, pushes vN, rolls it out, waits until stable)
git tag -a v6 -m "what changed"
./deploy.sh v6
git push origin v6

# Rollback (vN already in ECR -> redeployed as-is, no rebuild)
./deploy.sh v5
```

- A rollout takes ~10 minutes (~1–2 build/push, ~2 new task + health checks, up to 5 min draining the old task). During the last minutes the page may alternate between old and new version.
- **Every deploy ends any stream running at that moment** (the old container is replaced). Deploy outside learner sessions. Affected jobs are released automatically on the learner's next Start/Stop.
- The URL does **not** change on deploys or rollbacks — only if the service is deleted and recreated.
- Logs: CloudWatch log group `/aws/ecs/default/clickstream-api-<suffix>`.

---

## 8. Tear down

Order matters (service first, then what it used). Everything here is listed in [3](#3-aws-resources) as "created for this API".

```bash
REGION=us-east-1

# 1. Express service - also removes its ALB, target group, security groups, certificate, scaling and alarm
aws ecs delete-express-gateway-service --region $REGION \
  --service-arn arn:aws:ecs:$REGION:512357470856:service/default/clickstream-api

# 2. Cluster (only if nothing else runs in it)
aws ecs list-services --cluster default --region $REGION
aws ecs delete-cluster --cluster default --region $REGION

# 3. Activity table (all items expire within 24 h anyway)
aws dynamodb delete-table --table-name clickstream-job-activity --region $REGION

# 4. Secret (immediately, or drop --force-delete-without-recovery for a 30-day restore window)
aws secretsmanager delete-secret --secret-id clickstream-api/access-token --force-delete-without-recovery --region $REGION

# 5. IAM
aws iam delete-role-policy --role-name ecsTaskExecutionRole --policy-name clickstream-api-secret
aws iam delete-role-policy --role-name clickstream-api-task-role --policy-name s3-dynamodb-access
aws iam delete-role --role-name clickstream-api-task-role
aws iam detach-role-policy --role-name ecsInfrastructureRoleForExpressServices \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSInfrastructureRoleforExpressGatewayServices
aws iam delete-role --role-name ecsInfrastructureRoleForExpressServices

# 6. ECR images (optional - keeping them costs ~$0.10/GB-month and makes rebuild faster)
aws ecr delete-repository --repository-name enqurious/clickstream-api --force --region $REGION

# 7. Log group (optional)
aws logs describe-log-groups --log-group-name-prefix /aws/ecs/default/clickstream-api --region $REGION
```

**Keep:** S3 bucket `enqurious-clickstream-data`, table `amazon-ecommerce-streaming-api`, role `ecsTaskExecutionRole`.

Check nothing is left running:

```bash
aws ecs list-services --cluster default --region $REGION 2>/dev/null
aws elbv2 describe-load-balancers --region $REGION --query "LoadBalancers[?contains(LoadBalancerName,'ecs')].LoadBalancerName"
```

---

## 9. Costs

Approximate, us-east-1.

| While running | Per month |
|---|---|
| Fargate 1 vCPU / 2 GB, 24×7 | ~$36 (2 vCPU / 4 GB ≈ $72) |
| Application Load Balancer | ~$16–20 |
| Secrets Manager secret | $0.40 |
| DynamoDB activity table (on-demand) | ~$0.004 per learner-hour of streaming |
| DynamoDB jobs table (provisioned 1/1) | ~$0.60 |
| CloudWatch logs, ECR storage | under $1 |
| **Total idle** | **≈ $55–60** |

While torn down: only the kept resources — jobs table (~$0.60), S3 data, and ECR images if kept.

Not used (by decision): a **NAT Gateway + Elastic IP** for a fixed outbound IP (~$33/month + $0.045/GB). Only needed if a client must IP-allow-list our server on their Event Hub / Pub/Sub. It would also require moving tasks to private subnets via plain `aws ecs update-service` (not via Express Mode, see 6.6).

---

## 10. Client network whitelist

Clients only need to allow the API host (all paths, HTTPS 443). The file sent to clients is [`docs/network-whitelist.csv`](network-whitelist.csv):

| URL | Notes |
|---|---|
| `https://cl-e5785023185c46e3b4358e06d84a852b.ecs.us-east-1.on.aws/*` | URL of the deployment taken down in Oct 2026 — **replace with the new URL after a rebuild** |
| `https://stream-api.enqurious.com/*` | Future branded address (not set up yet) |

Also tell them to allow the custom request header `access_token`. The page loads nothing from third parties. S3 is read server-side, so clients never need to whitelist it. Learners' Event Hubs must allow public network access (our outbound IP is not fixed).

---

## 11. Testing approach and load-test results

The test scripts lived in a temporary folder and were not saved in the repo; this is how to recreate them.

**Simulated stream (no Event Hub needed).** Run the image locally with a `sitecustomize.py` mounted on `PYTHONPATH` that replaces the senders before the app imports them:

```python
# sim/sitecustomize.py - TEST ONLY
import asyncio, json, random, stream_errors, data_generation

async def _ok_preflight(connection_string, hub_name):
    await asyncio.sleep(0.3)

class _SimSender:
    def __init__(self, platform, params): pass
    async def send(self, data_dict):
        json.dumps(data_dict)
        await asyncio.sleep(random.uniform(0.05, 0.15))
    async def close(self): pass

stream_errors.check_azure_connection = _ok_preflight
data_generation.create_sender = _SimSender
```

```bash
docker run --rm -p 8001:8000 --cpus=1 --memory=2g \
  -v "<path-to>/sim:/sim:ro" -v "$HOME/.aws:/root/.aws:ro" \
  -e PYTHONPATH=/sim:/app -e AWS_REGION=us-east-1 -e aws-region=us-east-1 -e dynamodb-region=us-east-1 \
  -e ACCESS_TOKEN=test clickstream-api:v5
```

Everything except the final send is real (DynamoDB, S3, UI, polling). Use test emails and delete their rows from `amazon-ecommerce-streaming-api` afterwards.

**What was verified for v5:** shared connection per stream, stop, 3-hour auto-stop (tested with a 30 s limit), single failure reporting, server-error reference ids, resilience to event-logging errors; every preflight error case via the real API; UI in headless Chrome (light theme even with OS dark mode, streaming, stop, reconnect after reload, mobile layout); and a real stream to Event Hub `eh-artizent-api` (~66 events/min, Stop worked).

**Load test (50 simulated streams + 50 consoles polling, 1 vCPU / 2 GB, run from India against us-east-1):**

| Metric | Result |
|---|---|
| Streams started | **23 / 50** — 27 start requests timed out after 30 s |
| Events/min per running stream | ~56–61 (healthy) |
| Console poll latency | median ~5 s, p95 ~12 s, max ~28 s (should be < 1 s) |
| CPU | 45–100%, often saturated |
| Memory | levelled at ~1.27 GB of 2 GB |

Cause: request handlers make blocking boto3 calls inside `async` endpoints, so each worker serves one request at a time while waiting on DynamoDB. Running from India exaggerates it, but it would also limit production. **Fix before ~50 concurrent learners** (see below); after the fix, re-run the load test before deciding whether 1 vCPU is enough.

---

## 12. Release history

| Version | Git tag | What changed |
|---|---|---|
| v1 | — | First ECS deployment: Dockerfile, ECS Express service, task role instead of AWS keys, secret for the token, added missing `python-dotenv` |
| v2 | — | `--proxy-headers` so `url_for()` emits `https://` links (CSS/JS were blocked as mixed content) |
| v3 | `v3` | Logo served locally (old enqurious.com URL 404'd), `.webp` MIME type, `deploy.sh` with rollback |
| v4 | `v4` | Live Stream Console, activity table, `/jobs/*` endpoints, connection check before start, readable errors, stop flag in DynamoDB, release of stale/EC2-era jobs, full UI redesign, no third-party assets, Enqurious favicon set |
| v5 | `v5` | 3-hour auto-stop (learners can restart), one connection per stream, non-blocking Pub/Sub publish, shared boto3 clients, always-light UI, client whitelist CSV |

---

## 13. Known issues and next steps

Planned for **v6**:

1. **Blocking AWS calls in async handlers** (load test above). Make the `/jobs/*` and `/stop_processing` handlers plain `def` (FastAPI runs them in a thread pool) and wrap the blocking calls in `/data_generation` with `asyncio.to_thread`. Then re-run the 50-stream load test.
2. **Duplicate rows in the console after Stop.** Clicking Stop during an in-flight refresh starts a second refresh with the same `after`; both append the same events (seen as #118–#122 twice). Events are only *sent* once. Fix in `static/js/index.js`: keep `lastRenderedSeq`, reset it in `attachJob()`, and in `appendEvents()` drop events with `seq <= lastRenderedSeq`.
3. **"Waiting" forever after a container restart.** If the container dies mid-stream, the console shows "Waiting" until the learner presses Start/Stop. `/jobs/activity` should report an `in_progress` job with no event for 180 s as `interrupted`.

Other options, not scheduled:

- **Custom domain** `stream-api.enqurious.com` so the URL survives a rebuild (already whitelisted by clients).
- **Fixed outbound IP** (NAT Gateway) — only if a client must IP-restrict their stream ([Costs](#9-costs)).
- **More than one task** — the stop flag is already in DynamoDB, but jobs live in the worker that started them; test before raising `maxTaskCount`.
- Turn off FastAPI `/docs` and `/openapi.json` if exposing the API shape is a concern.
- Optional DynamoDB on-demand throughput cap on `clickstream-job-activity` as a cost ceiling.
- `README.md` still describes the EC2-era setup.

---

## 14. Gotchas on this machine

- **Pushing to GitHub from Git Bash fails** with "SSL certificate … unable to get local issuer certificate". Push from PowerShell with:
  ```powershell
  git -c http.sslBackend=schannel -c credential.helper= -c "credential.helper=!gh auth git-credential" push origin <branch> <tag>
  ```
- **`docker login` to ECR fails from Windows PowerShell 5.1** (it corrupts piped passwords). Run the `aws ecr get-login-password | docker login …` line from Git Bash (`deploy.sh` already does).
- **`requirements.txt` is UTF-16.** pip reads it fine; when appending a line from PowerShell use `Add-Content -Encoding Unicode`.
- **Express Mode settings that can't change later:** service name, cluster, load balancer configuration, deployment strategy. Network config changes that switch to private subnets make the load balancer internal.
- A Git Bash `aws … --output text` scan that paginates prints one line per page — use `--query` carefully or a short Python/boto3 script for bulk work (that's how the 31,525 activity items were cleared).
