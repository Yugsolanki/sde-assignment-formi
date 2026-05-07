# Post-Call Processing Pipeline — Design Document

**Author:** GLM-5-turbo
**Date:** 2024-01-15

---

## 1. Assumptions

1. **Business differentiation of call outcomes**: Calls ending with confirmed bookings or clear buying signals (e.g., "schedule a demo") represent immediate revenue opportunity and must be analyzed within minutes to trigger downstream actions (CRM updates, follow-up emails). Calls ending with "not interested" or voicemails can tolerate hours of delay without business impact.

2. **Customer SLA variance**: Enterprise customers paying premium rates expect guaranteed processing capacity even during system-wide peak load. Small customers can tolerate deferred processing during bursts.

3. **LLM rate limits are hard constraints**: The 500 requests/min and 90,000 tokens/min limits from the LLM provider are not soft suggestions—exceeding them results in 429 errors that cause cascading failures. The system must never trigger these errors.

4. **Campaign burst patterns are real**: 100,000 calls completing within a 2-4 hour window creates arrival rate variance of 400-800 calls/minute. The system must handle this without manual intervention.

5. **Infrastructure unreliability**: Redis and Celery workers can restart without coordination. Any state stored only in Redis is ephemeral and may be lost.

6. **Recording delivery variance**: Recordings from Exotel arrive asynchronously with high variance (10 seconds to 120+ seconds). The "45 seconds on average" assumption only holds under low load.

7. **No analysis result may ever be permanently lost**: Even if processing is deferred for days due to rate limits or failures, the system must eventually complete it. Silent drops are unacceptable.

8. **All LLM spending must be attributable**: Every token consumed must be traceable to a specific customer, campaign, and interaction for billing and debugging purposes.

9. **Testability requirement**: The system must be testable locally with docker-compose (Postgres + Redis) and mock LLM responses—no real API keys required.

---

## 2. Problem Diagnosis

The current system breaks at scale because it treats post-call processing as a fire-and-forget operation with no awareness of downstream constraints. Here's what's actually broken:

**Root cause: No rate limit enforcement**
The system defines `LLM_TOKENS_PER_MINUTE` and `LLM_REQUESTS_PER_MINUTE` in config, but nothing in the code reads these values before firing LLM requests. When 100K calls arrive, Celery workers blast the LLM API at full speed, trigger 429 errors, and enter retry storms that fill Redis and cause more failures.

**Contributing failures:**

1. **Single queue with no priority**: A 10-second "not interested" call and a confirmed rebook sit in the same queue with the same priority. Business-critical results wait behind junk.

2. **Binary circuit breaker creates cascading failures**: When LLM hits 90% capacity, the circuit breaker freezes ALL dialing for 30 minutes. This doesn't solve the LLM problem—it just stops revenue generation while the queue drains.

3. **Blind recording wait**: `asyncio.sleep(45s)` assumes recordings arrive in a fixed window. Under load, recordings take longer. The system silently skips late recordings with no retry, no alert, no visibility.

4. **Ephemeral task state**: Celery tasks live in Redis. If Redis restarts, in-flight tasks vanish. The "retry queue" is also Redis-backed—double single point of failure.

5. **No per-customer isolation**: Customer A's burst can consume 100% of LLM capacity, starving Customer B who has a contractual SLA.

**Why it works at small scale and breaks at large scale**: At 100 calls/hour, the system never approaches rate limits, recordings usually arrive within 45s, and Redis never restarts. At 100K calls, all these assumptions fail simultaneously.

---

## 3. Architecture Overview

```
POST /session/{sid}/interaction/{iid}/end
            │
    ┌───────▼───────┐
    │ FastAPI       │
    │ Endpoint      │
    └───────┬───────┘
            │
            ├─[Short transcript <4 turns]─► Direct status update (no LLM)
            │                               INSERT postcall_task (status=COMPLETED, recording=SKIPPED)
            │
            └─[Full transcript]──────────► Classify priority (P0/P1/P2)
                                         Estimate tokens
                                         INSERT INTO postcall_tasks (status=QUEUED)
                                         ┌─────────────────────────────────────────┐
                                         │ Durable Task Table (Postgres)           │
                                         │ - id, interaction_id, priority_class    │
                                         │ - customer_id, estimated_tokens         │
                                         │ - status (QUEUED/PROCESSING/COMPLETED)  │
                                         │ - scheduled_at, started_at, completed_at│
                                         │ - recording_status (PENDING/READY/FAIL) │
                                         │ - retry_count, error_log (JSONB)        │
                                         │ - version (optimistic locking)          │
                                         └──────────────┬──────────────────────────┘
                                                        │
              ┌─────────────────────────────────────────┼──────────────────────────┐
              │                                         │                          │
    ┌─────────▼─────────┐                   ┌─────────▼─────────┐      ┌──────────▼─────────┐
    │ Recording Poller  │                   │ Priority Scheduler│      │ Dead Letter Monitor│
    │ (Celery beat)     │                   │ (Celery workers)  │      │ (Celery beat)      │
    │ Every 30 seconds  │                   │ Every 5 seconds   │      │ Every hour         │
    │                   │                   │                   │      │                    │
    │ Query PENDING     │                   │ Query by priority │      │ Query FAILED       │
    │ recording_status  │                   │ + scheduled_at    │      │ Alert to ops       │
    │ Exponential       │                   │ Check rate limit  │      │                    │
    │ backoff: 30s→1h   │                   │ budget in Redis   │      └────────────────────┘
    │ Mark READY/FAIL   │                   │ Acquire budget    │
    └─────────┬─────────┘                   │ Spawn LLM executor│
              │                             └─────────┬─────────┘
              │                                       │
              │                             ┌─────────▼─────────┐
              │                             │ LLM Executor Task │
              │                             │                   │
              │                             │ Call LLM API      │
              │                             │ Record actual     │
              │                             │ token usage       │
              │                             │ Update task status│
              │                             │ Log to llm_usage  │
              │                             └─────────┬─────────┘
              │                                       │
              └───────────────────────────────────────┘
                              │
                   ┌──────────▼──────────┐
                   │ Downstream Triggers │
                   │ - Signal jobs       │
                   │ - Lead stage update │
                   │ - CRM push          │
                   │ Track in task.      │
                   │ downstream_triggers │
                   └─────────────────────┘
```

### Key design decisions

1. **Postgres-backed task queue instead of Redis/Celery queues**: Tasks survive Redis restarts. Postgres is already a dependency. Provides transactional consistency with interaction data.

2. **Explicit 3-tier priority model (P0/P1/P2)**: Business can understand "hot leads vs. cold leads." Simple to implement, debug, and explain to stakeholders. More flexible than single queue, simpler than ML-based dynamic scoring.

3. **Redis for rate limit tracking only**: Sub-millisecond atomic operations for budget checks. Ephemeral is acceptable here—state is re-derivable from `llm_usage_log` table on restart.

4. **Recording status as separate state machine**: Decouples recording fetch (which depends on external provider) from LLM processing (which depends on rate limits). Either can fail independently without blocking the other.

5. **Optimistic locking (version column)**: Enables concurrent schedulers without distributed locks. Simpler than `SELECT FOR UPDATE` for high-throughput queues.

---

## 4. Rate Limit Management

### How you track rate limit usage

**Redis keys with per-minute buckets:**
- `llm:global:tokens:min_{timestamp}` — Global token usage for current minute
- `llm:global:requests:min_{timestamp}` — Global request count for current minute
- `llm:customer:{id}:tokens:min_{timestamp}` — Per-customer token usage

**Atomic acquisition:**
Before each LLM call, executor runs a Redis pipeline:
```python
pipe.incrby(global_tokens_key, estimated_tokens)
pipe.expire(global_tokens_key, 120)  # Keep 2 min for overlap
pipe.incrby(customer_tokens_key, estimated_tokens)
pipe.expire(customer_tokens_key, 120)
pipe.incr(global_requests_key)
pipe.expire(global_requests_key, 120)
await pipe.execute()
```

**Post-call adjustment:**
After LLM response, adjust if actual tokens differ from estimate:
```python
diff = actual_tokens - estimated_tokens
pipe.incrby(global_tokens_key, diff)
pipe.incrby(customer_tokens_key, diff)
```

**Why Redis, not Postgres?** Budget checks happen 500+ times/minute. Redis provides sub-millisecond atomic INCRBY. Postgres would require row-level locks and be 10-50x slower.

### How you decide what to process now vs. defer

**Priority-based scheduling with utilization thresholds:**

| Priority | Process if utilization ≤ | Business justification |
|----------|-------------------------|----------------------|
| P0 (HIGH) | 100% | Always process—revenue at risk |
| P1 (NORMAL) | 80% | Standard processing—defer under heavy load |
| P2 (LOW) | 50% | Can wait—defer early to preserve capacity |

**Scheduler logic (every 5 seconds):**
1. Check current global utilization: `tokens_used / LLM_TOKENS_PER_MINUTE`
2. Determine which priority classes are eligible based on thresholds
3. Query tasks: `SELECT * FROM postcall_tasks WHERE priority_class IN (eligible) AND status IN ('QUEUED', 'DEFERRED') AND scheduled_at <= NOW() ORDER BY priority_class, scheduled_at LIMIT 20`
4. For each task: check per-customer budget, acquire if possible, defer to next minute if not

**Deferral mechanism:**
When budget cannot be acquired:
```sql
UPDATE postcall_tasks 
SET status = 'DEFERRED', 
    scheduled_at = next_minute_boundary,
    version = version + 1
WHERE id = ?
```

Task stays in database, visible in dashboard, will be picked up next cycle.

### What happens when the limit is hit (recovery, not crash)

**Graceful degradation, not cascading failure:**

1. **429 from LLM despite budget checks** (race condition):
   - Extract `Retry-After` header from response
   - Set `scheduled_at = NOW() + retry_after_seconds`
   - Log structured event: `llm_rate_limit_hit{interaction_id, retry_after, customer_id}`
   - Do NOT increment retry_count—this is a scheduling issue, not a processing failure

2. **Sustained high utilization (>90% for >5 minutes)**:
   - Alert fires: `token_budget_utilization_high`
   - P2 tasks accumulate in DEFERRED state (visible in dashboard)
   - P0/P1 continue processing
   - Dialler is NOT frozen—new calls continue, just deferred processing

3. **Redis restart during processing**:
   - Rate limit counters reset to 0
   - Next scheduler cycle re-derives usage from `llm_usage_log` table (last 60 seconds)
   - Brief burst possible—mitigated by conservative utilization thresholds

---

## 5. Per-Customer Token Budgeting

### How you allocate capacity across customers

**Model: Guaranteed budget + shared headroom**

```
customer_configs table:
├── customer_id (PK)
├── token_budget_per_minute (e.g., 20,000)
└── priority_boost (0.0-2.0)
```

- **Guaranteed budget**: Customer's `token_budget_per_minute` is reserved. No other customer can use it.
- **Shared headroom**: `Global limit (90K) - Sum(all guaranteed budgets) = shared pool`
- **Example**: 3 customers with 20K budget each = 60K guaranteed, 30K shared

### What guarantees does a customer with a pre-allocated budget receive?

1. **Guaranteed floor**: Even if all other customers are at peak, this customer can always process up to their budget.
2. **Priority boost**: Customer with `priority_boost=1.5` gets 50% more effective budget (30K instead of 20K).
3. **No starvation**: Other customers' bursts cannot consume this customer's allocation.

### What happens when a customer exceeds their budget?

**Three-tier enforcement:**

1. **Soft limit (100% of budget)**: Tasks deferred to next minute if would exceed
2. **Hard cap (150% of budget)**: Tasks hard-failed if would exceed—prevents runaway customer from affecting others
3. **Burst access**: Between 100-150%, customer can use shared pool at lower priority than other customers' P0 tasks

**Example**: Customer A has 20K budget, has used 18K:
- Request for 3K: Would hit 21K (>100%, <150%) → Allowed if shared pool has capacity, but lower priority than Customer B's P0
- Request for 5K: Would hit 23K (>150%) → Hard rejected, task deferred

### What happens to unallocated headroom?

**Shared pool rules:**
1. Any customer can use shared pool if their guaranteed budget is available
2. Usage is first-come, first-served within priority class
3. No customer can monopolize shared pool—hard cap still applies at 150% of their budget
4. If no customers are using shared pool, it goes unused (safer than over-provisioning)

**Why not redistribute unused budgets dynamically?** Too complex, creates contention, hard to debug. Prefer simple, predictable allocation over theoretical maximum utilization.

---

## 6. Differentiated Processing

### How you determine which calls are time-sensitive

**Hybrid approach: Pattern matching + metadata fallback**

**Layer 1: Transcript pattern matching (automatic)**
The `PriorityClassifier` scans transcripts for regex patterns:

**P0 patterns (buying signals):**
- `/(book|schedule|confirm).* (appointment|demo|meeting)/i`
- `/(yes|sure|definitely).* (interested|want to)/i`
- `/(send|email).* (proposal|information|quote)/i`
- `/(purchase|buy|sign up)/i`
- `/(when can|what time).* (tomorrow|next week)/i`

**P2 patterns (no interest):**
- `/(not interested|no thank|don't call|remove me)/i`
- `/(voicemail|machine|leave a message)/i`
- `/(wrong number|not the right person)/i`

**Layer 2: Conversation metadata (if available)**
```python
if conversation_data.get("disposition") in ["interested", "callback_requested", "appointment_booked"]:
    return P0
if conversation_data.get("disposition") in ["not_interested", "voicemail"]:
    return P2
```

**Layer 3: Default**
- No patterns matched → P1 (NORMAL)

### Why this mechanism?

**Rejected alternatives:**

| Option | Why rejected |
|--------|-------------|
| Flag set by business user | Requires UI, training, delays. Users forget to set flags. |
| ML classifier | Requires training data, cold start problem, hard to debug why a call got P0 vs P1. |
| Duration-based | 30-second call could be "not interested" or "callback requested"—duration doesn't indicate priority. |

**Chosen approach advantages:**
1. **No external dependencies**: Works with data already available
2. **Transparent**: Can explain "this call got P0 because it matched pattern X"
3. **Immediate**: No cold start, works from day one
4. **Debuggable**: Log which pattern matched for every call
5. **Evolvable**: Can add patterns based on production feedback

**Known weakness**: Regex won't catch every buying signal. Mitigated by logging misclassifications and iterating on patterns. Future: could add ML as secondary layer.

---

## 7. Recording Pipeline

### Replacement for `asyncio.sleep(45s)`

**Design: Polling with exponential backoff + persistent state**

**State machine:**
```
PENDING ──(poll success)──► READY
   │
   └──(poll fail)──► PENDING (increment retry_count, set next_poll_at)
                      │
                      └──(retry_count >= 10)──► FAILED
```

**Polling service (Celery beat, every 30 seconds):**
```python
async def poll_pending_recordings():
    tasks = SELECT * FROM postcall_tasks 
            WHERE recording_status = 'PENDING' 
            AND next_poll_at <= NOW() 
            LIMIT 50
    
    for task in tasks:
        recording_url = await fetch_from_exotel(task.call_sid)
        
        if recording_url:
            s3_key = await upload_to_s3(recording_url)
            UPDATE task SET recording_status = 'READY', recording_s3_key = s3_key
        else:
            task.retry_count += 1
            if task.retry_count >= 10:
                UPDATE task SET recording_status = 'FAILED'
            else:
                next_poll = NOW() + backoff(task.retry_count)
                UPDATE task SET retry_count = retry_count + 1, next_poll_at = next_poll
```

**Backoff formula:**
```python
def backoff(attempt):
    # 30s, 60s, 120s, 240s, 480s, 960s... capped at 1 hour
    return min(30 * (2 ** attempt), 3600)
```

### What does a failure look like to the on-call engineer?

**Every recording failure produces three things:**

1. **Structured log event:**
```json
{
  "event": "recording_failed",
  "interaction_id": "uuid",
  "customer_id": "uuid",
  "reason": "max_retries_exceeded",
  "poll_attempts": 10,
  "timestamp": "2024-01-15T10:30:00Z"
}
```

2. **Database record:**
- `postcall_tasks.recording_status = 'FAILED'`
- `postcall_tasks.error_log` contains all 10 poll attempts with timestamps and errors

3. **Alert (if spike):**
- If >5 recording failures in 10 minutes → PagerDuty alert: `recording_failure_spike`
- Indicates Exotel outage, not individual call issues

**Debugging a failed recording:**
1. Query: `SELECT * FROM postcall_tasks WHERE interaction_id = ? AND recording_status = 'FAILED'`
2. Examine `error_log` JSONB to see all 10 poll attempts
3. Check if other recordings failed at same time (Exotel outage vs. individual issue)
4. Manual recovery: `UPDATE postcall_tasks SET recording_status = 'PENDING', retry_count = 0 WHERE id = ?`

**No silent skips.** The old code returned `None` and did nothing. The new code explicitly tracks and alerts on every failure.

---

## 8. Reliability & Durability

### How do you ensure no analysis result is permanently lost?

**Four-layer defense:**

**Layer 1: Durable task storage (Postgres)**
- Every interaction creates a `postcall_tasks` row with status `QUEUED`
- Task persists through Redis restarts, Celery crashes, deployments
- No fire-and-forget—task has explicit lifecycle: QUEUED → PROCESSING → COMPLETED/FAILED

**Layer 2: Optimistic locking prevents concurrent corruption**
```sql
UPDATE postcall_tasks 
SET status = 'PROCESSING', started_at = NOW(), version = version + 1
WHERE id = ? AND version = ?
RETURNING *
```
If version mismatch (another worker claimed it), update returns 0 rows—task not lost, just not claimed by this worker.

**Layer 3: Stale task recovery**
Recovery job (every 5 minutes):
```sql
UPDATE postcall_tasks 
SET status = 'QUEUED', started_at = NULL, version = version + 1
WHERE status = 'PROCESSING' 
  AND started_at < NOW() - INTERVAL '10 minutes'
```
If worker crashes mid-task, task is recovered after 10 minutes.

**Layer 4: Dead letter visibility**
After `max_retries` (5) failures:
- Task status = `FAILED` (not deleted)
- `error_log` JSONB contains all error messages with timestamps
- Hourly job alerts on any FAILED tasks
- Manual recovery: `UPDATE postcall_tasks SET status = 'QUEUED', retry_count = 0 WHERE id = ?`

**Why not use Celery's built-in retry?**
- Celery retries live in Redis—lost on restart
- No visibility into retry history
- No dead letter state
- Our Postgres-backed approach survives all failure modes

---

## 9. Auditability & Observability

### How would you debug a specific failed interaction 3 days after the fact?

**Step-by-step:**

1. **Query interaction:**
```sql
SELECT * FROM interactions WHERE id = 'uuid';
-- Check: status, processing_status, priority_class, postcall_task_id
```

2. **Query task:**
```sql
SELECT * FROM postcall_tasks WHERE interaction_id = 'uuid';
-- Check: status, priority_class, scheduled_at, started_at, completed_at
-- Check: recording_status, recording_s3_key, recording_retry_count
-- Check: estimated_tokens, actual_tokens, retry_count
```

3. **Examine error log:**
```sql
SELECT error_log FROM postcall_tasks WHERE interaction_id = 'uuid';
-- Returns JSONB array: [{"error": "...", "timestamp": "..."}, ...]
```

4. **Check LLM usage:**
```sql
SELECT * FROM llm_usage_log WHERE interaction_id = 'uuid';
-- Check: tokens_used, latency_ms, model, provider
```

5. **Search logs:**
```bash
# All events for this interaction
grep "interaction_id=uuid" /var/log/voicebot/app.log

# Specific failure
grep "event=postcall_analysis_error.*interaction_id=uuid" /var/log/voicebot/app.log
```

All data in Postgres + structured logs. No Redis state to lose.

### What you log (and what fields every log event includes)

**Every log event includes:**
```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "level": "INFO|WARNING|ERROR",
  "event": "<event_type>",
  "interaction_id": "uuid",
  "customer_id": "uuid",
  "campaign_id": "uuid",
  "agent_id": "uuid",
  "task_id": "uuid"  // if applicable
}
```

**Critical events:**

| Event | When | Additional fields |
|-------|------|------------------|
| `interaction_ended` | Webhook received | duration, is_short |
| `postcall_task_created` | Task inserted | priority_class, estimated_tokens |
| `recording_poll` | Each poll attempt | attempt, status |
| `recording_ready` | Upload complete | s3_key |
| `recording_failed` | Max retries | reason, poll_attempts |
| `task_claimed` | Worker claims task | priority, worker_id |
| `llm_call_started` | Before LLM request | estimated_tokens |
| `llm_call_completed` | LLM response | actual_tokens, latency_ms |
| `llm_rate_limit_hit` | 429 received | retry_after |
| `customer_budget_exhausted` | Budget check failed | budget, used |
| `postcall_analysis_complete` | Analysis done | call_stage, entities |
| `task_completed` | All processing done | total_duration_s |
| `task_failed` | Max retries exceeded | errors |
| `task_recovered` | Stale task reset | stale_since |
| `downstream_trigger_sent` | Signal job/CRM | trigger_type |

### Alert conditions

| Alert | Condition | Severity | Response |
|-------|-----------|----------|----------|
| `llm_rate_limit_burst` | >10 rate limit hits in 5 min | P1 | Check budget config, may need to increase limits |
| `recording_failure_spike` | >5 recording failures in 10 min | P1 | Check Exotel status, may be outage |
| `customer_budget_exhausted` | Any customer exhausted >30 min | P2 | Review customer's campaign burst, adjust budget |
| `dead_letter_tasks` | Any tasks in FAILED state | P2 | Review error_log, manual recovery may be needed |
| `processing_backlog_high` | >1000 tasks in QUEUED/DEFERRED | P2 | Normal during bursts, monitor |
| `worker_stuck` | Task in PROCESSING >10 min | P1 | Check worker health, recovery job will reset |
| `token_utilization_high` | Global utilization >90% for >5 min | P2 | Expected during bursts, P2 tasks will defer |

---

## 10. Data Model

```sql
-- Migration: Add post-call processing tables
-- Run: psql -d voicebot -f migrations/001_add_postcall_tables.sql

BEGIN;

-- Customer LLM budget configuration
CREATE TABLE IF NOT EXISTS customer_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL UNIQUE,
    token_budget_per_minute INT NOT NULL DEFAULT 10000,
    priority_boost FLOAT NOT NULL DEFAULT 1.0 CHECK (priority_boost >= 0.0 AND priority_boost <= 2.0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_customer_configs_customer_id 
ON customer_configs(customer_id);

-- Durable post-call task queue
CREATE TABLE IF NOT EXISTS postcall_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    interaction_id UUID NOT NULL,
    priority_class SMALLINT NOT NULL DEFAULT 1 CHECK (priority_class IN (0, 1, 2)),
    customer_id UUID NOT NULL,
    
    status VARCHAR(20) NOT NULL DEFAULT 'QUEUED' 
        CHECK (status IN ('QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', 'DEFERRED')),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    
    recording_status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
        CHECK (recording_status IN ('PENDING', 'READY', 'FAILED', 'SKIPPED')),
    recording_s3_key TEXT,
    recording_retry_count INT NOT NULL DEFAULT 0,
    next_poll_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    estimated_tokens INT NOT NULL,
    actual_tokens INT,
    
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,
    error_log JSONB NOT NULL DEFAULT '[]',
    
    downstream_triggers JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    version INT NOT NULL DEFAULT 1,
    
    CONSTRAINT fk_postcall_interaction FOREIGN KEY (interaction_id) 
        REFERENCES interactions(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_postcall_tasks_priority_scheduled 
ON postcall_tasks(priority_class, scheduled_at) 
WHERE status IN ('QUEUED', 'DEFERRED');

CREATE INDEX IF NOT EXISTS idx_postcall_tasks_recording_poll 
ON postcall_tasks(next_poll_at) 
WHERE recording_status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_postcall_tasks_status 
ON postcall_tasks(status);

CREATE INDEX IF NOT EXISTS idx_postcall_tasks_interaction 
ON postcall_tasks(interaction_id);

CREATE INDEX IF NOT EXISTS idx_postcall_tasks_customer 
ON postcall_tasks(customer_id, status);

-- LLM token usage tracking (for billing)
CREATE TABLE IF NOT EXISTS llm_usage_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    interaction_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    
    tokens_used INT NOT NULL,
    latency_ms INT NOT NULL,
    
    call_stage VARCHAR(50),
    model VARCHAR(100) NOT NULL,
    provider VARCHAR(50) NOT NULL,
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT fk_llm_usage_interaction FOREIGN KEY (interaction_id) 
        REFERENCES interactions(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_llm_usage_customer_date 
ON llm_usage_log(customer_id, created_at);

CREATE INDEX IF NOT EXISTS idx_llm_usage_campaign 
ON llm_usage_log(campaign_id, created_at);

-- Add columns to interactions table
ALTER TABLE interactions 
ADD COLUMN IF NOT EXISTS priority_class SMALLINT DEFAULT 1,
ADD COLUMN IF NOT EXISTS postcall_task_id UUID,
ADD COLUMN IF NOT EXISTS processing_status VARCHAR(20) DEFAULT 'PENDING';

COMMIT;
```

---

## 11. Security

### What data in this system is sensitive?

| Data Type | Location | Sensitivity | Risk if exposed |
|-----------|----------|-------------|-----------------|
| Call transcripts | `interactions.conversation_data.transcript` | HIGH | PII (names, phone numbers mentioned), confidential business discussions |
| Lead PII | `leads.name`, `leads.phone`, `leads.email` | HIGH | Direct personal identifiers, potential for identity theft or spam |
| Call recordings | S3 at `recordings/{interaction_id}.mp3` | HIGH | Voice recordings contain PII, may reveal sensitive business information |
| Analysis results | `interactions.interaction_metadata` | MEDIUM | Extracted entities (may include PII), call outcomes (competitive intelligence) |
| LLM API key | Environment variables, Secrets Manager | CRITICAL | Authentication credential, enables unauthorized LLM usage and charges |

### How do you protect it at rest and in transit?

**Data at Rest:**

1. **Transcripts**: 
   - Enable Postgres TDE (Transparent Data Encryption) at storage level
   - Alternative: Application-level encryption using envelope encryption with KMS
   - Store as encrypted JSONB, decrypt only when needed for LLM call

2. **Recordings**:
   - S3 server-side encryption with KMS customer-managed keys (SSE-KMS)
   - KMS key policy restricts decrypt to application IAM role
   - Enable S3 Object Lock for compliance retention (optional)

3. **Lead PII**:
   - Encrypt `phone` and `email` columns at application level
   - Use envelope encryption: DEK encrypted with KMS, DEK stored in DB
   - Only decrypt when needed for CRM sync or display

4. **LLM API key**:
   - Store in AWS Secrets Manager or HashiCorp Vault
   - Never in code, environment files, or plain environment variables
   - Rotate every 90 days
   - IAM policy restricts access to application role

**Data in Transit:**

1. **Database**: `sslmode=require` in connection string, verify server certificate
2. **S3**: HTTPS with signature v4, enforce in bucket policy
3. **LLM API**: HTTPS enforced by provider
4. **Redis**: Use `rediss://` protocol (TLS), configure certificate validation
5. **Internal services**: mTLS between microservices (if applicable)

**Access Control:**

1. **Database**: 
   - Application role has minimal permissions (SELECT/UPDATE on specific tables)
   - No DROP, DELETE (except via soft-delete pattern)
   - Separate read-only role for dashboards

2. **S3**: 
   - Bucket policy allows access only from application IAM role
   - Deny all non-HTTPS requests
   - Enable access logging

3. **KMS**: 
   - Key policy restricts decrypt to specific roles
   - Enable CloudTrail logging for all KMS API calls

**Audit Logging:**

1. Log all access to PII fields with: who, when, what (query pattern), from where (IP)
2. Log all transcript/recordings downloads with justification
3. Retain audit logs for 1 year (compliance requirement)
4. Alert on anomalous access patterns (e.g., bulk downloads)

**Data Minimization:**

1. Recordings: Retention policy 90 days, then auto-delete via S3 lifecycle
2. Transcripts: Anonymize before long-term archival (replace names/phones with placeholders)
3. Only store extracted entities needed for business logic

---

## 12. API Interface

**Did you change the API contract?**

**No, the API contract remains unchanged:**
```
POST /session/{session_id}/interaction/{interaction_id}/end
```

**Why kept the same:**

1. **Backward compatibility**: Existing telephony provider integration expects this endpoint. Changing it would require coordination with external system.

2. **Separation of concerns**: The endpoint's job is to signal "call ended." How the system processes that signal is an internal implementation detail. The caller doesn't need to know about priorities, rate limits, or budgets.

3. **No additional information needed**: All data required for priority classification (transcript, conversation_data) is already available in the interaction record. No new fields needed.

4. **Idempotency preserved**: The endpoint can be called multiple times safely (interaction status check prevents duplicate processing).

**What changed internally:**

- Old: Endpoint directly spawned Celery task
- New: Endpoint inserts row to `postcall_tasks` table, scheduler picks it up asynchronously

The caller sees same response:
```json
{
  "status": "ok",
  "interaction_id": "uuid",
  "message": "Interaction ended, processing scheduled"
}
```

The difference is "processing scheduled" now means "durable task created" instead of "ephemeral Celery task spawned."

---

## 13. Trade-offs & Alternatives Considered

| Option | Why Considered | Why Rejected / What You Chose Instead |
|--------|---------------|--------------------------------------|
| **RabbitMQ/SQS for task queue** | Higher throughput than Postgres, purpose-built for queues | Chose Postgres: already a dependency, provides transactional consistency with interaction data, simpler infrastructure for local testing. Throughput (100K over hours) doesn't require RabbitMQ. |
| **ML-based priority classification** | More accurate than regex, can learn from data | Chose regex: no training data, cold start problem, hard to debug. Regex is transparent ("this matched pattern X"), ML is opaque. Can add ML as secondary layer later. |
| **Dynamic priority scoring** | More granular than 3 tiers (e.g., 0-100 score) | Chose 3 tiers: simpler to implement, explain to business, debug. Business understands "hot lead vs. cold lead" not "score 87 vs 72". |
| **Redis for task queue** | Faster than Postgres, existing dependency | Rejected: Redis restart loses tasks (the exact problem we're solving). Postgres provides durability. |
| **Webhook from Exotel for recording ready** | No polling needed, instant notification | Chose polling: Exotel may not support webhooks, provider unreliable so polling is more robust. Can add webhook support later if available. |
| **Store recordings in DB** | Simpler than S3, transactional with task | Rejected: Recordings are large (MBs), wrong storage medium. S3 is designed for this. |
| **Real-time LLM streaming for all calls** | Lower latency for all results | Rejected: Doesn't help rate limit problem (still consumes tokens), adds complexity. Only P0 calls need low latency, and they get it via priority queue. |
| **Global circuit breaker (current design)** | Simple to implement | Rejected: Binary freeze causes cascading failure. Chose proportional backpressure based on utilization. |
| **Single queue FIFO** | Simplest possible | Rejected: Doesn't respect business priority. P0 (revenue) waits behind P2 (junk). |
| **No per-customer budgets** | Simpler implementation | Rejected: Customer A's burst starves Customer B who has SLA. Per-customer isolation is business requirement. |
| **Exponential backoff for retries** | Standard best practice | Chose with modifications: Fixed delay for rate limit deferrals (next minute), exponential for recording polls, capped at reasonable max. |
| **Discard tasks after max retries** | Simpler than dead letter | Rejected: Violates "no permanent loss" requirement. Chose explicit FAILED state with visibility. |

---

## 14. Known Weaknesses

1. **Priority classification accuracy**: Regex patterns won't catch every buying signal and may have false positives. A 30-second "not interested" call and a 30-second "callback requested" call look similar to simple patterns. **Mitigation**: Log priority classification with matched pattern, enable manual overrides via admin UI, iterate on patterns based on production feedback.

2. **Single-region dependency**: All components (Postgres, Redis, S3) assumed in one region. Region outage causes full system outage. **Mitigation**: Postgres read replicas for failover, S3 cross-region replication. Not implemented in MVP.

3. **Single scheduler bottleneck**: Current design has one scheduler process claiming tasks. If scheduler is slow, throughput drops. **Mitigation**: `SKIP LOCKED` allows multiple concurrent schedulers (documented but not implemented in MVP). Need to test for correctness before enabling.

4. **Exotel as external dependency**: If Exotel API is down or slow, recording polls fail, recordings may be lost after 10 retries. **Mitigation**: Longer backoff, clear alerting, manual re-processing tool. No workaround for extended Exotel outage.

5. **Token estimation inaccuracy**: Heuristic (1 token ≈ 4 chars) may underestimate, causing rate limit hits despite budget checks. **Mitigation**: Conservative 10% buffer, track actual vs. estimated to refine heuristic over time.

6. **No multi-LLM provider support**: Tied to single provider's rate limits. If provider has outage, all processing stops. **Mitigation**: Architecture supports adding providers (different `provider` field in usage log), but not implemented. Would need request routing logic.

7. **Redis for rate limits is ephemeral**: If Redis restarts during high load, counters reset, brief burst possible before re-deriving from DB. **Mitigation**: Conservative utilization thresholds (defer P2 at 50%, not 90%) provide buffer.

8. **No automated budget adjustment**: Customer budgets are manually configured. If customer doubles their campaign size, budget may be too low. **Mitigation**: Alerts on budget exhaustion, manual adjustment. Could add auto-scaling based on campaign size.

9. **Downstream trigger reliability**: Signal jobs and lead stage updates are fire-and-forget with basic retry. If downstream system is down, triggers may be lost. **Mitigation**: `downstream_triggers` JSONB tracks status, could add retry logic. CRM push specifically needs better handling.

10. **No backpressure to dialler**: System can defer processing indefinitely, but dialler doesn't know. Could keep making calls that queue up forever. **Mitigation**: Dashboard shows backlog, ops can pause dialler. Could add API for dialler to check backlog.

---

## 15. What I Would Do With More Time

1. **Implement CRM push with full reliability** (Priority: HIGH)
   - Add `crm_push_status` to `postcall_tasks`
   - Implement retry with exponential backoff
   - Track push attempts and responses in `downstream_triggers`
   - Add manual retry button in admin UI
   - Reason: Current implementation is fire-and-forget, business requires CRM sync

2. **Add per-customer configuration UI** (Priority: HIGH)
   - Admin page to view/edit `customer_configs`
   - Show current utilization vs. budget
   - Allow adjusting budget without deployment
   - Reason: Ops currently needs DB access to change budgets

3. **Implement multi-scheduler support** (Priority: MEDIUM)
   - Test `SKIP LOCKED` with 2-3 concurrent schedulers
   - Verify no duplicate processing under load
   - Add scheduler health metrics
   - Reason: Removes single point of bottleneck

4. **Add recording fallback for Exotel failures** (Priority: MEDIUM)
   - If Exotel API returns 500 errors, escalate immediately
   - Add secondary recording source if available
   - Implement manual recording upload endpoint
   - Reason: Current design loses recordings after 10 failed polls

5. **Build priority classification feedback loop** (Priority: MEDIUM)
   - Log misclassifications flagged by ops
   - Add "reclassify" action in admin UI
   - Track classification accuracy metrics
   - Iterate on regex patterns based on data
   - Reason: Improve accuracy without ML complexity

6. **Implement dialler backpressure API** (Priority: LOW)
   - Endpoint: `GET /api/v1/processing/backlog?customer_id=X`
   - Returns: queued count by priority
   - Dialler can check before initiating calls
   - Reason: Prevent indefinite backlog growth

7. **Add multi-LLM provider support** (Priority: LOW)
   - Abstract LLM client interface
   - Route requests based on provider availability/cost
   - Track per-provider usage in `llm_usage_log`
   - Reason: Reduce single-vendor dependency

8. **Implement token estimation refinement** (Priority: LOW)
   - Track actual vs. estimated tokens per call
   - Build regression model: tokens = f(transcript_length, entity_count)
   - Use model instead of heuristic
   - Reason: Reduce rate limit hits from estimation errors

9. **Add encryption at rest for transcripts** (Priority: LOW)
   - Implement envelope encryption with KMS
   - Encrypt `conversation_data` before storage
   - Decrypt only when needed for LLM call
   - Reason: Compliance requirement, currently relying on Postgres TDE

10. **Build replay/debugging tool** (Priority: LOW)
    - Admin UI to view full interaction lifecycle
    - Show all log events for an interaction
    - Allow manual retry from any stage
    - Reason: Speed up debugging, currently requires DB queries + log search