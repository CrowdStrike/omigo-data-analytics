# Kafka

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Kafka

**Subtitle:** Kafka stores events in an append-only log that many readers consume at their own pace — reading never deletes, so anyone can replay history

## One Orders Log, Three Independent Readers

**Tags:** `core idea` (blue), `the log` (green), `event streaming` (orange)

- **The topic** — every order event is appended to the end of an `orders` log and never edited in place
- **Three readers** — the payment service, an analytics pipeline, and an email service all read the same log
- **The offset** — each consumer keeps its own bookmark (an offset) marking how far into the log it has read
- **No deletion** — reading an event does not remove it; the log keeps events until retention expires
- **The rewind** — analytics moves its offset back to yesterday and reprocesses the same events again

*Example (italic):* Order #7042 lands at offset 9; payment charges the card, email sends a receipt, and analytics counts it — all three read the same log entry.

**Key point:** Kafka's core abstraction is an append-only log: producers append at the end, each consumer reads at its own pace via its own offset, and events leave only by retention age — never because someone read them.

### Visualization (canvas `c1`, 720×300)

Diagram of one log as a row of offset cells with a producer appending at the right and three consumer pointers parked at different offsets, one rewound.

- **Title (bold 15px, `#1a5276`, top center):** "One Log, Three Consumers, Three Offsets".
- **Log cells:** 10 boxes at y=100, each 54px wide × 36px tall starting at x=90 with 1px gaps (cell i left edge = 90 + i×55); fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, 12px `#2c3e50` centered labels `e0`–`e9`; 11px `#6b7280` offset numbers `0`–`9` centered 12px below each cell.
- **Producer:** dashed 1px `#6b7280` empty cell at x=640 (54×36, dash 4/3); bold 12px violet `#4a3aa7` label "producer appends here" above it at y=85 with a short violet arrow down to the dashed cell.
- **Consumer pointers (2px arrows from label up to cell bottom edge):**
  - green `#008300` arrow to the end of the log past cell offset 9 (tip x=637), bold 12px green label "payment — offset 10 (caught up)" at (x=380, y=185)
  - blue `#2a78d6` arrow to cell offset 7 (tip x=527), bold 12px blue label "email — offset 7" at (x=380, y=220)
  - orange `#d95926` arrow to cell offset 3 (tip x=307), bold 12px orange label "analytics — offset 3" at (x=60, y=255)
- **Rewind marker:** dashed orange `#d95926` horizontal arrow (dash 4/3) at y=160 from x=500 back to x=310, 12px orange label "rewound to replay yesterday" above it.
- **Caption (12px `#444`, bottom right):** "offsets illustrative".

## Offsets and Partitions by Hand

**Tags:** `worked example` (blue), `partitions` (green), `ordering` (orange)

- **Twelve orders** — one morning's orders (illustrative) arrive and are spread across 3 partitions
- **The rule** — partition = customer id mod 3 here; real Kafka hashes the key, but the effect is the same
- **Hand-check** — customer 104: 104 mod 3 = 2 (exact), so every order from 104 lands in partition 2
- **Per-key order** — 104's orders stay in arrival order inside partition 2; across partitions there is no order
- **Consumer group** — three payment workers form one group; each takes one partition, tripling throughput

*Example (italic):* Customer 104's orders #1, #4, #7, #11 all land in partition 2 in that order, while orders #2 and #3 go to partitions 0 and 1.

**Key point:** Partitions buy parallelism without losing what matters — all events with the same key share one partition, so per-key order is preserved while a consumer group splits the partitions among its members.

### Visualization (canvas `c2`, 720×300)

Three horizontal partition lanes with the 12 order events as boxes in arrival order; customer 104's four orders highlighted in one lane.

- **Title (bold 15px, `#1a5276`, top center):** "12 Orders, 3 Partitions: partition = customer id mod 3".
- **Lanes:** baselines 1px `#e5e9ef` at y = 85, 160, 235 from x=95 to x=700; 12px `#444` labels "partition 0", "partition 1", "partition 2" at x=12 on each baseline.
- **Event boxes:** 46px wide × 30px tall sitting on each baseline, first box left edge x=100, 10px horizontal gaps; 12px `#2c3e50` order number centered inside (e.g. "#4"), 11px `#6b7280` customer id centered 11px below the box (e.g. "c104").
- **Partition 0 boxes (left to right):** orders `["#2","#5","#9","#12"]`, customers `["c102","c105","c102","c105"]`, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border.
- **Partition 1 boxes:** orders `["#3","#6","#10"]`, customers `["c103","c106","c103"]`, same blue style.
- **Partition 2 boxes:** orders `["#1","#4","#7","#8","#11"]`, customers `["c104","c104","c104","c101","c104"]`; the four c104 boxes use fill `rgba(0,131,0,0.15)` with 2px `#008300` border, the c101 box uses the blue style.
- **Annotation (bold 12px green `#008300`, at x=420, y=275):** "customer 104's four orders — one partition, in order".
- **Caption (12px `#444`, top right under title):** "order ids illustrative; 104 mod 3 = 2 exact".

## The Backbone of Event-Driven Systems

**Tags:** `where it's used` (blue), `replay` (green)

- **The origin** — Kafka was built at LinkedIn to move activity events between systems, then open-sourced
- **The backbone** — event-driven architectures use topics as the shared record of what happened, in order
- **Stream processors** — tools like Kafka Streams and Flink use topics as their input feed and output sink
- **Late arrivals** — a consumer added long after launch bootstraps by replaying the retained log from offset 0
- **Decoupling** — producers never know how many consumers exist; adding a reader changes nothing upstream

*Example (italic):* A fraud team points a brand-new consumer at `orders` offset 0 and rebuilds its features from the retained week of history — no producer changes at all.

**Key point:** Because the log does not delete on read, any number of independent systems — including ones built long after the events were written — can consume the same history.

### Visualization (canvas `c3`, 720×300)

Fan-out diagram: one retained orders topic on the left feeding four consumer boxes on the right, the last one added later and replaying from offset 0.

- **Title (bold 15px, `#1a5276`, top center):** "One Log Feeds Every System — Including Ones That Don't Exist Yet".
- **Log box:** rounded box (8px radius) at x=60, y=125, 170px wide × 54px tall, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` two-line label "orders topic" / "(7-day retention)".
- **Consumer boxes (right column, each 230px wide × 38px tall, 8px radius, left edge x=440, 12px `#2c3e50` text):**
  - y=48: "payment service", fill `rgba(0,131,0,0.12)`, 2px `#008300` border
  - y=108: "analytics (stream processor)", fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border
  - y=168: "email service", fill `rgba(217,89,38,0.12)`, 2px `#d95926` border
  - y=228: "fraud model — replays from offset 0", fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border
- **Arrows:** 2px lines from the log box's right edge (x=230, y=152) to each consumer box's left edge, colored to match each box; the fraud-model arrow is dashed (dash 5/4) with an 11px `#4a3aa7` label "added later" at its midpoint.
- **Annotation (bold 13px magenta `#d55181`, centered near y=290):** "adding a reader costs the log nothing".
- **Caption (12px `#444`, bottom right):** "retention setting illustrative".

## Not a Queue: Reading Doesn't Delete

**Tags:** `common mistake` (red), `consumer groups` (orange)

- **The confusion** — in a classic queue a delivered message is gone; in Kafka it stays until retention
- **The symptom** — separate services share one consumer group, so each event reaches only one of them
- **What groups mean** — a group splits partitions among its members; it divides work, not audiences
- **The fix** — payment, analytics, and email each use their own group id, so each gets every event
- **Retention, not reads** — events leave the log only when the 7-day retention (illustrative) expires

*Example (italic):* Email joined payment's consumer group "to reuse config"; each order then went to only one of the two services, and half the receipts were silently never sent.

**Common mistake:** Treating Kafka as a queue where reading removes the message. Reads never delete anything — if several services must each see every event, they must consume under different group ids.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: what happens to one order after the first read in a classic queue (gone) vs in a Kafka log (still there for everyone).

- **Title (bold 15px, `#1a5276`, top center):** "Queue vs Log: What Happens After Payment Reads Order #7042".
- **Row 1 (y=90), label 12px `#444` at x=15:** "classic queue"; blue `#2a78d6` rounded box (150×40, 8px radius, fill `rgba(42,120,214,0.15)`) at x=130 labeled "order #7042", 3px `#2c3e50` arrow to a red box (190×40, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border) at x=350 labeled "payment reads — deleted", with bold 12px red `#e74c3c` text "✗ analytics and email never see it" at (x=350, y=145).
- **Row 2 (y=205), label:** "Kafka log"; blue box at x=130 labeled "order #7042 stays at offset 9", then three 2px green `#008300` arrows fanning to three small green boxes (120×32, fill `rgba(0,131,0,0.12)`, 2px `#008300` border) at x=400 and y=170/205/240 labeled "payment ✓", "analytics ✓", "email ✓" (12px `#2c3e50`).
- **Divider:** 1px `#e5e9ef` horizontal line at y=160 from x=15 to x=705.
- **Annotation (bold 13px orange `#d95926`, centered near y=285):** "the log deletes by age, not by read".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and positions above (no randomness); order ids, offsets, customer ids, and the 7-day retention are invented and labeled illustrative; 104 mod 3 = 2 is exact arithmetic standing in for Kafka's key hash. Kafka facts (built at LinkedIn, append-only partitioned replicated log, offset-tracking consumers, retention-based deletion, consumer groups) are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
