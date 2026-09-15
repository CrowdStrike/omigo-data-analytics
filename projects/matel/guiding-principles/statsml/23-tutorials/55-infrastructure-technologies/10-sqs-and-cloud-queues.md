# SQS & Cloud Queues

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SQS & Cloud Queues

**Subtitle:** SQS is a message queue you rent by the request — services drop jobs in, workers pull them out, and a crashed worker's job automatically reappears for someone else

## A Mailbox Between the Upload and the Work

**Tags:** `core idea` (blue), `decoupling` (green), `AWS` (orange)

- **The service** — an image-upload API stores each photo, then needs a thumbnail made for it
- **The old way** — the API calls the thumbnailer directly, so one slow resize makes every upload slow
- **The queue** — instead the API sends a tiny message, "make a thumbnail for photo 4127", to SQS
- **The workers** — a pool of thumbnail workers pulls messages and processes them at its own pace
- **Three verbs** — the whole interface is send, receive, delete; SQS shipped with these in 2006
- **No broker** — nobody installs, sizes, or restarts a queue server; AWS runs it as a shared utility

*Example (italic):* At 9:00am a user uploads photo 4127; the API answers in 40 ms and the thumbnail appears seconds later, made by whichever worker was free.

**Key point:** A queue decouples the producer from the consumer — the upload API's job ends at "send", and the thumbnail work happens whenever a worker gets to it.

### Visualization (canvas `c1`, 720×300)

Flow diagram: upload API sends into an SQS queue, two workers receive from it, a dashed return arrow shows delete-after-success.

- **Title (bold 15px, `#1a5276`, top center):** "One Upload, Three Verbs: send → receive → delete".
- **Producer box:** rounded rect at x=30, y=125, 130×50, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` label "upload API".
- **Send arrow:** 3px `#2a78d6` arrow from (160,150) to (235,150), bold 12px `#2a78d6` label "send" above it.
- **Queue box:** rounded rect at x=240, y=115, 210×70, 2px `#1a5276` border, fill `rgba(26,82,118,0.06)`, bold 13px `#1a5276` label "SQS queue" at its top edge; inside, four 26×26 message squares at x = 258, 292, 326, 360 (y=140), fill `rgba(42,120,214,0.25)`, 11px `#2c3e50` labels "4127", "4128", "4129", "4130".
- **Receive arrows:** 3px `#008300` arrows from (455,140) to (530,95) and from (455,160) to (530,205), bold 12px `#008300` label "receive" between them at (462,152).
- **Worker boxes:** rounded rects at x=535, y=70 and x=535, y=185, each 150×50, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px labels "worker 1" and "worker 2".
- **Delete arrow:** dashed `#6b7280` (dash 4/3) 2px arrow from worker 1's bottom edge (610,120) to the queue's top edge (345,115), 12px `#6b7280` label "delete after success" at (430,58).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "the API answers in 40 ms — the resize happens whenever a worker is free".
- **Caption (12px `#444`, bottom right):** "latency illustrative".

## The Crash, the Reappearance, and the Dead-Letter Queue

**Tags:** `worked example` (blue), `visibility timeout` (green), `dead-letter queue` (orange)

- **The timeout** — receiving hides a message for a visibility timeout (30s here); it is not deleted
- **The crash** — worker A receives job 4127 at t=2s and dies 12 seconds in, before calling delete
- **The reappearance** — at t=32s the timeout expires and job 4127 becomes visible again, untouched
- **The retry** — worker B receives it at t=33s, finishes the thumbnail, and deletes it at t=41s
- **The poison pill** — job 4130 (a corrupt file) fails on every attempt and reappears each timeout
- **The DLQ** — with maxReceiveCount 3, the next receive after its third failure moves it to the DLQ

*Example (italic):* Job 4127 survives its worker's crash with zero human action — hidden at 2s, visible again at 32s, done at 41s (timings illustrative).

**Key point:** Delete-after-success plus the visibility timeout gives at-least-once delivery — a message can only vanish once some consumer finished it, and repeat failures drain to the DLQ instead of looping forever.

### Visualization (canvas `c2`, 720×300)

Two-row timeline of message state (visible vs hidden) over 100 seconds: job 4127 recovers from a worker crash; poison job 4130 fails three times and moves to the DLQ.

- **Title (bold 15px, `#1a5276`, top center):** "Visibility Timeout at Work: One Crash Recovered, One Poison Message Drained".
- **Axis:** 2px `#999` baseline at y=250 from x=60 to x=660; x maps 0–100 s at 6 px/s, 12px `#444` tick labels "0s" to "100s" every 20 s.
- **Legend (top left under title, y=52):** 12×12 swatches — solid `#2a78d6` "visible", `rgba(107,114,128,0.35)` "hidden (in flight)", 11px `#444` labels.
- **Row 1 — bold 12px `#1a5276` label "job 4127" at (60,85); state band 18px tall centered on y=105:**
  - visible: `#2a78d6` band 0–2 s (x 60–72)
  - hidden (worker A): `rgba(107,114,128,0.35)` band 2–32 s (x 72–252); bold 12px `#e74c3c` "✗ worker A crashes" above x=144 (t=14 s)
  - visible again: `#2a78d6` band 32–33 s (x 252–258)
  - hidden (worker B): gray band 33–41 s (x 258–306); bold 12px `#008300` "✓ deleted — done" at x=306 (t=41 s)
- **Row 2 — bold 12px `#1a5276` label "job 4130 (poison)" at (60,168); band 18px tall centered on y=190:**
  - visible 0–2 s, hidden 2–32 s with red ✗ at t=7 s; visible 32–34 s, hidden 34–64 s with red ✗ at t=39 s; visible 64–66 s, hidden 66–96 s with red ✗ at t=71 s
  - at t=96 s (x=636): orange `#d95926` filled marker, bold 12px `#d95926` label "→ DLQ (receive count 3)" placed to its left
- **Annotation (bold 13px green `#008300`, near x=330, y=70):** "no human touched job 4127".
- **Caption (12px `#444`, bottom right):** "timings illustrative; timeout 30s, maxReceiveCount 3".

## A Queue as a Utility, Not a Server

**Tags:** `where it's used` (blue), `pay per request` (green)

- **No server** — there is no queue host to patch or page for; SQS was one of AWS's first services (2006)
- **Pay per request** — the published rate is $0.40 per million requests; an idle queue costs nothing
- **Three requests** — one thumbnail job = one send + one receive + one delete = 3 requests
- **Scales without thought** — a 100× upload spike just deepens the queue; no capacity ticket is filed
- **Where you meet it** — ETL fan-out, ML batch scoring, webhook buffering, any producer/consumer split

*Example (italic):* 100M requests a month — enough for 33M thumbnails — costs $40 at the published rate, with no broker on-call rota.

**Key point:** The utility model is the point: the queue is an API you call, not a system you operate — cost tracks usage, and capacity is the cloud provider's problem.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: monthly cost of SQS at three request volumes vs a self-hosted broker's fixed cost.

- **Title (bold 15px, `#1a5276`, top center):** "Pay for Requests, Not for a Server".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; pixel widths linear in dollars (440px = $150), floored at 2px.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "SQS — 1M requests/mo: $0.40": green `#008300` bar width 2
  - "SQS — 10M requests/mo: $4": green bar width 12
  - "SQS — 100M requests/mo: $40": green bar width 117
  - "self-hosted broker — any volume: ~$150/mo": blue `rgba(42,120,214,0.30)` bar width 440, bold 12px `#e74c3c` label "+ patching and on-call" at the bar's end
- **Bar style:** 14px tall, 11px `#444` dollar labels at bar ends (except where the red label sits).
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "an idle queue costs $0 — an idle broker still costs $150".
- **Caption (12px `#444`, bottom right):** "broker cost illustrative; $0.40 per million requests is the published SQS rate".

## At-Least-Once Means Sometimes Twice

**Tags:** `common mistake` (red), `idempotency` (orange)

- **The assumption** — teams assume each message arrives exactly once, in the order it was sent
- **The reality** — a standard queue is at-least-once with best-effort order; duplicates are normal
- **Where dupes come from** — a crash after the work but before the delete replays the whole message
- **Idempotency** — a worker must make re-processing harmless: same input, same result, effects once
- **FIFO option** — FIFO queues offer exactly-once processing and strict order, at lower throughput
- **The test** — ask "what if this handler runs twice on the same message?" before shipping it

*Example (italic):* Worker A crashed after writing thumb_4127.jpg; when worker B replays the job, an existence check turns the duplicate into a no-op.

**Common mistake:** Building consumers that assume exactly-once, in-order delivery on a standard queue — the fix is idempotent handlers (or a FIFO queue), not hoping duplicates never happen.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a naive worker double-processes a redelivered job (user pinged twice) vs an idempotent worker that checks before acting.

- **Title (bold 15px, `#1a5276`, top center):** "At-Least-Once Means Your Worker Must Tolerate Twice".
- **Row 1 (y=95), label 12px `#444` at x=20:** "naive worker"; blue `#2a78d6` rounded box at x=170 labeled "job 4127 arrives again" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "resizes again, 2nd push alert" with bold 12px red "✗ user pinged twice" beneath it.
- **Row 2 (y=205), label:** "idempotent worker"; blue box at x=170 "job 4127 arrives again", 3px arrow to a green `#008300` box at x=370 labeled "thumb_4127.jpg exists?", then arrow to a green box at x=560 labeled "skip — no-op" with bold 12px green "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "duplicates are normal — make the handler safe to re-run".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); job ids, timings (30s timeout, crash at t=14s, delete at t=41s, DLQ at t=96s), and the broker's $150/mo are invented and labeled illustrative; $0.40 per million requests is the published SQS rate, and the request-count arithmetic (3 requests per job, 100M requests → $40) is exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
