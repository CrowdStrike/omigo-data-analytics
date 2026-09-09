# Continuous Monitoring Agent Execution Models

**Page type:** detail page (backlog-style: intro callout, numbered h2 sections, two-column layout table with text left ~45% and canvas right ~55%; last section has two stacked canvases)
**HTML title tag:** Continuous Monitoring Agent Execution Models — Discussion Backlog

**Subtitle:** How work gets distributed across agents that watch, re-profile, and trigger

**Intro callout:** Statistical profiling isn't a one-shot batch job — it's a continuous monitoring process. Data arrives, distributions shift, models degrade, alerts fire. The execution model determines how work gets distributed across agents that watch for changes, re-profile features, and trigger downstream actions.

## 1. Execution Model A: File-Polling Agents

Each agent independently polls the same input directory/file for new work. Simplest model — zero coordination infrastructure.

- **How it works:** Agents watch a shared filesystem path (S3 prefix, directory). When new data files appear, each agent picks one up, processes it, writes results
- **Coordination:** File-level locking or rename-on-claim (`input.csv` → `input.csv.processing`). No central coordinator
- **Advantages:** Dead simple. No message broker. No scheduler. Works with any filesystem. Easy to debug — just look at the files
- **Disadvantages:** Race conditions on claim. No priority ordering. No backpressure — if agents are slow, files pile up with no visibility. No retry semantics without extra logic
- **Failure mode:** Agent dies mid-processing → file stuck in "processing" state forever. Need TTL-based recovery or heartbeat files
- **Best for:** Small scale (< 10 agents), simple pipelines, batch-oriented work where latency doesn't matter

### Visualization (canvas `c1`, 720×300)

Architecture diagram: central shared filesystem box with four agents polling it.

- **Title (bold 17px, `#1a5276`, top center):** "Model A: File-Polling Agents"
- **Center box** (w/2−80, 60, 160×100, fill `rgba(41,128,185,0.1)`, stroke `#2980b9` width 2), bold 15px blue label "Shared FS / S3"; inside, 10px monospace `#333` file names: `data_001.csv`, `data_002.csv`, `data_003.csv`.
- **Four agent boxes** (rounded rects 90×32 radius 5, fill `rgba(39,174,96,0.15)`, stroke `#27ae60` width 1.5, bold 14px green labels): "Agent 1" and "Agent 2" on the left (x=80, y=110 and 190), "Agent 3" and "Agent 4" on the right (x=w−80, same y). Dashed gray `#999` polling lines (dash 3/3) from each agent to the center box.
- **Labels:** orange `#e67e22` 14px "poll" at (170,100) and (w−170,100).
- **Bottom annotation (14px `#555`, centered):** "Each agent scans directory independently. Race on claim. No coordination."

## 2. Execution Model B: Database Task Queue + Scheduler

A central database holds all pending tasks. A scheduler assigns tasks to agents based on availability, priority, and affinity.

- **How it works:** Tasks inserted into DB table with status (pending/assigned/running/done/failed). Scheduler queries for pending tasks, assigns to free agents via row lock + status update
- **Coordination:** Centralized — scheduler has global view. Can implement priority queues, fair scheduling, affinity (agent A always handles feature X)
- **Advantages:** Full visibility (query DB for status). Priority ordering. Retry logic built-in (failed → pending after N attempts). Exactly-once semantics via DB transactions. Rich scheduling policies
- **Disadvantages:** Scheduler is single point of failure. DB becomes bottleneck at high throughput. More infrastructure (DB + scheduler service). Polling from scheduler adds latency
- **Failure mode:** Scheduler dies → no new assignments (but running tasks complete). DB locks contention at scale. Need lease/heartbeat to detect dead agents holding tasks
- **Scaling:** Works well to ~100 agents. Beyond that, DB write contention becomes real. Can shard by feature/dataset
- **Best for:** Medium scale, need for visibility/debugging, complex scheduling policies, regulated environments needing audit trail

### Visualization (canvas `c2`, 720×300)

Architecture diagram: task DB → scheduler → three agents.

- **Title (bold 17px, `#1a5276`, top center):** "Model B: Database Task Queue + Scheduler"
- **Task DB box** (rounded rect 40,60 140×80 radius 8, fill `rgba(142,68,173,0.1)`, stroke `#8e44ad` width 2), bold 15px purple label "Task DB"; inside, 9px monospace `#555` table rows: `| id | status  | agent |`, `| 1  | running | A1    |`, `| 2  | pending | -     |`.
- **Scheduler box** (rounded rect centered, 120×50 radius 6, fill `rgba(230,126,34,0.15)`, stroke `#e67e22` width 2), bold 15px orange label "Scheduler".
- **Arrow** gray `#999` from DB to Scheduler with `#555` arrowhead.
- **Three agent boxes** on the right (rounded rects 90×30 radius 5 at y=60/110/160, fill `rgba(39,174,96,0.15)`, stroke `#27ae60`, bold 14px green labels): "Agent 1", "Agent 2", "Agent 3". Orange `#e67e22` width-1.5 lines from Scheduler to each agent; 13px orange label "assign".
- **Bottom annotation (14px `#555`, centered):** "Scheduler assigns tasks. DB tracks state. Full visibility + retry logic."

## 3. Execution Model C: Message Bus (Competing Consumers)

Tasks published to a message queue. Agents subscribe as competing consumers — when free, pull next message, process it, acknowledge (deletes from queue).

- **How it works:** Producer pushes profiling tasks to queue (Kafka, RabbitMQ, SQS, Redis Streams). Agents pull when ready. Ack on completion removes message. No-ack after timeout → message redelivered
- **Coordination:** Queue handles distribution. No scheduler needed. Agents are self-scheduling — pull only when capacity exists (natural backpressure)
- **Advantages:** Scales horizontally — add agents, they auto-join. Built-in retry (visibility timeout). Backpressure native. Decoupled producers/consumers. High throughput. No single point of failure (clustered brokers)
- **Disadvantages:** At-least-once delivery (must handle duplicates). Message ordering not guaranteed across consumers. No global priority without multiple queues. Harder to get "status of task X" (message is gone once consumed)
- **Failure mode:** Agent dies before ack → message redelivered (good). Poison messages that always fail → dead letter queue. Broker overload if consumers too slow
- **Variants:**
  - **Single queue:** FIFO, all agents equal. Simple but no priority
  - **Priority queues:** Multiple queues (high/medium/low). Agents drain high first
  - **Topic-based:** Route by feature type or dataset. Specialized agents per topic
  - **Stream with consumer groups:** Kafka-style — messages persist, replayable, exactly-once with transactions
- **Best for:** Large scale (100+ agents), high throughput, event-driven architectures, cloud-native environments

### Visualization (canvas `c3`, 720×300)

Architecture diagram: producer → message queue (with visible messages) → three pulling agents.

- **Title (bold 17px, `#1a5276`, top center):** "Model C: Message Bus (Competing Consumers)"
- **Producer box** (rounded rect 30,90 100×40 radius 5, fill `rgba(41,128,185,0.15)`, stroke `#2980b9` width 1.5), bold 14px blue label "Producer".
- **Queue box** (centered, 200×80 at y=70, fill `rgba(231,76,60,0.08)`, stroke `#e74c3c` width 2), bold 15px red label "Message Queue"; inside, four small message chips (38×22, fill `#fef9e7`, stroke `#e67e22`, 8px monospace `#333`): `task_A`, `task_B`, `task_C`, `task_D`.
- **Arrow** blue `#2980b9` from Producer to Queue with arrowhead.
- **Three agent boxes** on the right (rounded rects 90×30 radius 5 at y=60/110/160, fill `rgba(39,174,96,0.15)`, stroke `#27ae60`, bold 14px green labels): "Agent 1", "Agent 2", "Agent 3". Green `#27ae60` width-1.5 pull lines from queue to each agent; 13px green label "pull + ack".
- **Bottom annotation (14px `#555`, centered):** "Agents pull when free. Ack deletes message. Natural backpressure. Scales horizontally."

## 4. Comparison & Hybrid Approaches

No single model fits all scenarios. The right choice depends on scale, latency requirements, failure tolerance, and operational complexity budget.

- **Hybrid: DB + Message Bus:** DB for task metadata and status tracking. Message bus for work distribution. Best of both — visibility of DB, throughput of queue. Tasks created in DB, events pushed to queue, agents update DB on completion
- **Hybrid: File + Message:** Large data stays on filesystem (S3/GCS). Queue carries lightweight task descriptors (pointers to files). Agents pull descriptor, fetch data, process, write results back to FS
- **Scale transitions:** Start with file-polling (prototype). Graduate to DB+scheduler (production). Evolve to message bus (scale). Each transition preserves the task semantics, changes only the distribution mechanism

**Key dimensions for choosing:**

- **Throughput:** File < DB < Message Bus
- **Visibility:** DB > File > Message Bus
- **Complexity:** File < DB < Message Bus
- **Fault tolerance:** Message Bus > DB > File
- **Latency:** Message Bus (push) < DB (poll interval) < File (scan interval)

**For our pipeline specifically:**

- Monitoring agents watch for distribution drift (KS test between windows)
- Shape reclassification triggered when drift exceeds threshold
- Enrichment recalculation after shape change or new data window
- Cascade of dependent re-computations (shape change → bucket change → enrichment change → verdict change)
- Need: idempotent re-profiling, dependency-aware ordering, stale-result eviction

**Key questions** (red-bordered key-point callout): (1) What's the expected throughput — features per minute? (2) Do agents specialize (one per feature) or generalize (any agent handles any feature)? (3) How to handle cascading invalidation — one shape change triggers 5 downstream re-computations? (4) Should monitoring be continuous (streaming) or periodic (cron-like)? (5) What's the acceptable staleness — can a profile be 5 minutes old? 1 hour? (6) How does this interact with the lineage system (idea 18) — does each agent execution create a lineage record? (7) For the DB model — Postgres, Redis, or something domain-specific? (8) Cost model — always-on agents vs scale-to-zero serverless?

### Visualization (canvas `c4`, 720×300)

Text comparison matrix of the three models across five dimensions, color-coded by whether the value is good or bad for that dimension.

- **Title (bold 17px, `#1a5276`, top center):** "Comparison: File vs DB vs Message Bus"
- **Column headers (bold 14px `#1a5276`):** Throughput, Visibility, Complexity, Fault Tol., Latency. Row labels (bold 14px `#1a5276`, left): "File-Poll", "DB+Sched", "Msg Bus". Alternating row background `rgba(26,82,118,0.03)`.
- **Cell values (bold 14px, centered):**
  - File-Poll: Low, Medium, Low, Low, High
  - DB+Sched: Medium, High, Medium, Medium, Medium
  - Msg Bus: High, Low, High, High, Low
- **Color coding:** green `#27ae60` = good, orange `#e67e22` = medium, red `#e74c3c` = bad. For Throughput, Visibility, and Fault Tolerance, High is good (green) and Low is bad (red); for Complexity and Latency, Low is good (green) and High is bad (red); Medium is always orange.
- **Legend (13px green, left):** "● = best for this dimension"
- **Bottom annotation (14px `#555`, centered):** "No single model wins all dimensions. Hybrid approaches combine strengths."

### Visualization (canvas `c5`, 720×300)

Horizontal five-stage cascade chain of what agents actually monitor.

- **Title (bold 17px, `#1a5276`, top center):** "Cascading Re-Computation: What Agents Actually Monitor"
- **Five stage boxes** (rounded rects 100×44 radius 5 at y=70, fill = stage color at 0.12 alpha, stroke = stage color width 2, bold 13px two-line colored labels; connected by `#bbb` arrows):
  - "Data Arrives" (x=70, blue `#2980b9`)
  - "Drift Detected" (x=200, orange `#e67e22`)
  - "Re-profile Shape" (x=340, purple `#8e44ad`)
  - "Re-compute Buckets" (x=480, red `#e74c3c`)
  - "Re-score Enrichment" (x=620, green `#27ae60`)
- **Cascade annotation (bold 14px red, centered below chain):** "Each stage triggers next → cascade of invalidation"
- **Bottom annotations (centered):** 14px `#555`: "Question: One agent per stage? One agent per feature? One agent handles full cascade?"; bold 14px `#1a5276`: "Must be idempotent — same input always produces same output regardless of execution model."

## Regeneration instructions

- **Layout:** backlog detail-page style. `<h1>` (2rem, `#1a5276`, 2px solid `#2980b9` bottom border), `.subtitle` (`#666`, 0.95rem), `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem). Each section is a `.lang-section` (40px bottom margin) with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one `<tr>`: left `td.text-col` (45%) with intro paragraph + bullets/callouts, right `td.viz-col` (55%) with the canvas. Section 4's viz cell stacks two canvases (`c4` then `c5`).
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Inline code:** `code` — background `#e8f0f8`, padding 2px 6px, radius 3px, 0.85em, `#1a5276`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; `ul` 0.92rem with 20px left margin (section 3 has a nested `ul` for variants); canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#2980b9` secondary blue, `#8e44ad` purple.
- **Canvas:** declare intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` via a shared `setupCanvas(id)` helper (`ctx.scale` back to logical coordinates); these charts use `ctx.roundRect` and `-apple-system` font strings (titles bold 17px, body 13-14px, monospace snippets in "SF Mono, monospace").
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
