# Partial Ordering of Events & Replication Lag

**Page type:** detail page (backlog-style 2-col layout: numbered h2 sections, text left 45%, canvas right 55%, one layout table per section)
**HTML title tag:** Partial Ordering & Replication Lag

**Subtitle:** In a distributed system, a total order over events is something the infrastructure invents, not something that exists. Only causally related events have a real "happened before" — everything else is genuinely unordered, and pipelines that assume otherwise break silently under replication lag.

**Status badge:** TO DISCUSS

## 1. Total Order Is a Fiction

**Events are only partially ordered.** The "happened before" relation is a partial order: it orders some pairs and leaves the rest incomparable.

- **Independent producers** — user A clicks in Tokyo, user B clicks in London. There is no fact about which came first; the pair is concurrent, not merely unknown.
- **Causally related events** — user A searches, then clicks a result. This pair *is* ordered, because the first could have influenced the second.
- **The illusion of timestamps** — stamping every event produces a total order, but the order across concurrent events is an artifact of which clocks were read.
- **Clock skew** — independent nodes disagree by milliseconds at best. Timestamp order can therefore contradict causal order.

**Key-point callout (red accent):** **The trap:** a pipeline that sorts by timestamp and treats the result as ground truth has baked an arbitrary total order onto a partially ordered reality. Any conclusion that depends on the relative order of concurrent events is a conclusion about clock noise.

**Example (italic):** Test: swap two concurrent events in the input and re-run. If the output changes, the output was never about the data.

### Visualization (canvas `c1`, 720×380)

Two-panel diagram: a happened-before DAG (upper) vs a forced wall-clock total order (lower).

- **Title (bold 14px, `#1a5276`, top center):** "Happened-Before (Partial Order) vs Forced Wall-Clock Order".
- **Upper panel label (11px `#1a5276`, left at y=48):** "True structure: only causal edges order events".
- **Two horizontal node timelines** with gray `#ccc` arrows: "Node A" at y=100 and "Node B" at y=175 (labels 10px `#666` at left), lines from x=80 to x=668. Time-to-x mapping: `x = 80 + (t/70)*570`.
- **Events as 6px filled dots `#1a5276`** with bold 11px labels: a1 (t=10) and a2 (t=40) on Node A (labels above); b1 (t=25) and b2 (t=55) on Node B (labels below).
- **Causal message edge** a1 → b1: green `#27ae60` line width 2 with arrowhead, labeled in 10px green: "message: a1 → b1 (real order)".
- **Program-order edges** (green `#27ae60`, width 1.5, arrowheads): a1→a2 along Node A, b1→b2 along Node B.
- **Concurrency marker:** red `#e74c3c` dashed (4/3) line width 1.5 from a2 down to b2; two-line red 10px text at x=80, y=218/232: "a2 ∥ b1 and a2 ∥ b2 — no causal path either way," / "so no true order exists between them".
- **Lower panel label (11px `#1a5276`, left at y=262):** "Sorted by wall-clock timestamp: a total order the system invented".
- **Lower axis:** gray `#ccc` line from (50,310) to (668,310) with arrowhead.
- **Four sequence boxes** (110px wide, 34px tall, at x = 70, 220, 370, 520, y=292) labeled a1, b1, a2, b2 (bold 12px, centered). a1 and b1 ("real" order): fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1.5; a2 and b2: fill `rgba(231,76,60,0.12)`, stroke/text `#e74c3c`.
- **Red 10px annotation** above the b1 box: "swap b1 and a2 here: nothing observable changes".
- **Caption (11px `#888`, bottom center):** "Only the causal edges are facts — every other adjacency in the sorted list is an artifact of the clocks".

## 2. Working With the Order That Actually Exists

**Accept the partial order.** Design analyses that are correct given only the causal edges, with no appeal to a global timeline.

- **Vector clocks / version vectors** — track "happened before" without synchronized clocks, and report concurrency explicitly instead of guessing.
- **Lamport clocks** — cheaper: if `a → b` then `L(a) < L(b)`, but a smaller counter does *not* prove causality, so they cannot detect concurrency.
- **Causal consistency** — guarantees you observe causes before effects while leaving concurrent events unordered.
- **CRDTs** — converge to the same state regardless of delivery order.
- **Commutative operations** — `SUM`, `COUNT`, `MAX`, `SET UNION` are order-insensitive by construction; `LAST_VALUE`, rolling windows and sessionization are not.

**Key-point callout (red accent):** **Key insight:** if the result differs depending on how concurrent events happened to be ordered, that sensitivity is a bug in the analysis, not a property of the data.

### Visualization (canvas `c2`, 720×300)

Two-node timeline diagram showing vector clocks detecting causality that skewed wall clocks reverse.

- **Title (bold 14px `#1a5276`, top center):** "Vector Clocks Resolve What Wall Clocks Cannot".
- **Timelines:** "Node A" at y=90 and "Node B" at y=160 (10px `#666` labels), gray `#ccc` lines from x=85 to x=660 with arrowheads.
- **Events** (6px dots `#1a5276`) with bold 11px name+vector-clock labels and red 9px wall-clock labels:
  - e1 `[1,0]` "clock A: .050" at x=200 on Node A (labels above)
  - e2 `[2,0]` "clock A: .080" at x=450 on Node A (labels above)
  - f1 `[0,1]` "clock B: .010" at x=150 on Node B (labels below)
  - f2 `[1,2]` "clock B: .030" at x=340 on Node B (labels below)
- **Program-order edges:** green `#27ae60` width 1.5 with arrowheads, e1→e2 and f1→f2.
- **Message edge** e1 → f2: green `#27ae60` width 2 with arrowhead, labeled "message" (10px green, at ~x=250, y=128).
- **Verdict lines (10px, left-aligned at x=90):**
  - green `#27ae60` (y=232): "[1,0] ≤ [1,2]  →  e1 happened before f2  (detected correctly)"
  - orange `#e67e22` (y=250): "[2,0] vs [1,2]  →  neither dominates  →  concurrent, left unordered"
  - red `#e74c3c` (y=268): "Wall clock: node B is 40 ms behind, so f2 (.030) sorts before e1 (.050) — a real causal edge reversed"
- **Caption (11px `#888`, bottom center):** "Skew can be bounded but never eliminated; causality needs logical clocks, not timestamps".

## 3. Replication Lag: How It Shows Up

**Lag is staleness, not loss.** The write is committed and durable on the primary; a replica simply has not applied it yet. Every read served from that replica sees an older, internally valid state.

- **Read-your-own-writes failure** — write to the primary, read from a replica, and the value is not there yet.
- **Causal inversion** — an effect becomes visible before its cause, producing a state that never existed on the primary.
- **Split-brain reads** — one query fans out to replicas at different lag; rows from different instants land in one result set.
- **Monotonicity violation** — a second read routed to a further-behind replica returns earlier data than the first. Time appears to move backward.
- **Phantom deletions** — a row is read as present, then the replica applies a delete that was already committed upstream, so the row vanishes retroactively.

**Key-point callout (red accent):** **The insidious part:** lag is variable. Tests pass at zero lag, production fails intermittently at tens of milliseconds, and incidents fail catastrophically at seconds. The bug is not reproducible because the lag is not reproducible.

### Visualization (canvas `c3`, 720×300)

Three-lane sequence diagram: Primary / Replica / Client timelines with a stale read.

- **Title (bold 14px `#1a5276`, top center):** "Replication Lag: A Read That Misses a Committed Write".
- **Timelines:** "Primary" at y=95, "Replica" at y=165, "Client" at y=232 (10px `#666` labels), gray `#ccc` lines from x=90 to x=660 with arrowheads. Time-to-x mapping: `x = 90 + (t/500)*560` (t in ms).
- **Stale window band** on the Replica line from t=100 to t=280: fill `rgba(230,126,34,0.18)`, dashed (3/3) orange `#e67e22` border, 20px tall; orange 10px centered label above: "stale window: replica still serves x = 0".
- **Primary commit:** green `#27ae60` 6px dot at t=100 on Primary, bold 10px green label above: "x = 1 committed".
- **Replica apply:** green 6px dot at t=280 on Replica, bold 10px green label to the right: "x = 1 applied".
- **Replication arrow:** dashed (4/3) gray `#999` width 1.5 from primary commit to replica apply with arrowhead, 10px gray label: "replicate: lag = 180 ms".
- **Client write:** blue `#1a5276` 5px dot at t=90 on Client, line up to Primary, 10px centered blue label below: "write x = 1".
- **Client stale read:** red `#e74c3c` 5px dot at t=200 on Client, red width-2 arrow up to the Replica line, bold 10px red label below: "read → returns x = 0".
- **Time axis ticks (9px `#999`, centered below Client):** 0, 100, 200, 300, 400, "500 ms".
- **Caption (11px `#888`, bottom center):** "Read-your-own-writes broken: the write is durable, the replica has simply not applied it yet".

## 4. Consequences for Data Analysis

**Cross-table reads inherit the worst lag involved.** Joining tables served by replicas at different lag produces a snapshot that was never simultaneously true.

- **Inconsistent joins** — if the child table is fresher than the parent, you get orphan rows (an order whose user is invisible). If the parent is fresher, you get undercounts (a user whose orders are invisible).
- **Counts disagree** — `COUNT` on the parent and `SUM` over the children reconcile only if both sides are at the same instant.
- **Feature store inconsistency** — a feature computed from a current table joined to a lagging one describes a state that never existed, and it will not match at training time.
- **Time-travel artifacts** — an analytics snapshot with some tables at T and others at T−3s has no single point in time it was real.

**Key-point callout (red accent):** **Impact:** nearly every analytical pipeline reads replicas for throughput, so nearly every cross-table result is potentially an inconsistent snapshot unless the read is pinned to one point in time.

**Example (italic):** Mitigation: read a single MVCC snapshot or a versioned table (Delta / Iceberg time travel) rather than several live replicas.

### Visualization (canvas `c4`, 720×300)

Diagram: one query hitting two replicas frozen at different instants.

- **Title (bold 14px `#1a5276`, top center):** "One Query, Two Replicas, Two Different Instants".
- **Query line:** vertical dashed (5/4) blue `#1a5276` width 2 at T (time-to-x mapping `x = 110 + ((sec+4)/4)*520`, sec from −4 to 0), from y=48 to y=205; bold 10px blue right-aligned label: "query issued at T".
- **Two replica rows:** "users replica" at y=100 (1 s behind, visible up to T−1) and "orders replica" at y=170 (3 s behind, visible up to T−3); 10px `#666` labels at left, gray `#ccc` baselines.
  - Visible portion: bar fill `rgba(26,82,118,0.35)`, 18px tall, with 9px `#1a5276` centered label "visible to the query".
  - Not-yet-applied portion: fill `rgba(231,76,60,0.12)` with dashed (3/3) red `#e74c3c` border, 9px red centered label "not applied yet (1 s behind)" / "not applied yet (3 s behind)".
- **Divergence bracket:** orange `#e67e22` width 1.5 horizontal bracket between T−3 and T−1 at y=138 with end ticks; 10px orange centered label above: "2 s of divergence between the two sides of the join".
- **Time axis (9px `#999`, y=220):** labels "T-4s", "T-3s", "T-2s", "T-1s", "T".
- **Outcome lines (10px red `#e74c3c`, left-aligned at x=60):**
  - y=248: "Parent fresher than child (this case): users present, their orders invisible → undercounts"
  - y=266: "Child fresher than parent: orders present, their user invisible → orphan rows"
- **Caption (11px `#888`, bottom center):** "No single instant existed at which this joined result was true".

## 5. Design Patterns for Partial-Order Awareness

**Make out-of-order arrival a normal case** rather than an exception to be prevented.

- **Idempotent processing** — a duplicate or reordered event leaves the result unchanged.
- **Commutative aggregation** — prefer operations whose result is independent of arrival order.
- **Watermarks** — an explicit, machine-readable claim that all events up to some event time are believed to have arrived. Uncertainty becomes a value, not an assumption.
- **Grace periods** — hold a window open past its end so late arrivals still land in the right bucket.
- **Reprocessing** — immutable sources plus deterministic transforms so anything that arrives after the grace period can be corrected, not lost.
- **Consistent snapshots** — one point-in-time read instead of several independent replica reads.

**Key-point callout (red accent):** **The tradeoff:** a longer grace period buys completeness and pays in latency. Both are legitimate choices; an implicit grace period of zero is not.

### Visualization (canvas `c5`, 720×300)

Arrival-time diagram showing a window end, grace period band, watermark, and late events.

- **Title (bold 14px `#1a5276`, top center):** "Watermarks and Grace Periods: When Is \"Enough\" Data In?".
- Time-to-x mapping: `x = 70 + (t/10)*580` (t in seconds, 0–10); arrival axis (gray `#ccc` with arrowhead) at y=165.
- **Grace band:** orange fill `rgba(230,126,34,0.15)` rectangle from t=5 to t=7.5, y=60 to 190; two orange `#e67e22` 10px centered labels: "grace period" / "late arrivals still counted".
- **Window end:** vertical dashed (5/4) blue `#1a5276` width 2 line at t=5 from y=60 to 195; bold 10px blue right-aligned label above: "window [0,5) ends".
- **Watermark:** solid green `#27ae60` width 2 vertical line at t=7.5; bold 10px green left-aligned label above: "watermark passes → emit result".
- **Events** (6px dots on the arrival axis): t=1.2 blue `#1a5276` labeled "on time"; t=2.0, 3.4, 4.1 blue unlabeled; t=5.9 orange `#e67e22` labeled "late, still in window"; t=8.7 red `#e74c3c` labeled "past grace → reprocess" (label offset lower).
- **Axis annotation (10px `#666`, left, above the line):** "events for window [0,5) plotted by arrival time".
- **Ticks (9px `#999`):** 0, 2, 4, 6, 8, "10 s"; 10px `#666` centered axis label: "arrival (processing) time".
- **Caption (11px `#888`, bottom center):** "A longer grace period buys completeness and pays in latency".

## 6. What Must Not Depend on Ordering

**Invariants that must hold for any arrival order** of concurrent events:

- **Correctness** — the final result is identical for every valid interleaving.
- **Completeness** — the system can state when it has enough data, instead of waiting indefinitely.
- **Determinism** — the same set of events yields the same output, regardless of the sequence in which they arrived.

**What may legitimately depend on ordering:**

- **Latency** — out-of-order input delays emission until the watermark advances.
- **Intermediate state** — in-flight aggregates may be temporarily wrong, provided they converge.
- **Efficiency** — sorted input can be processed faster; unsorted input must still be processed correctly.

**Key-point callout (red accent):** **Do not conflate the models:** eventual consistency promises only that replicas converge. It permits an effect to be observed before its cause in the interim. Causal consistency is the strictly stronger guarantee that forbids that inversion, and it is usually the one an analytical pipeline actually needs.

### Visualization (canvas `c6`, 720×300)

Staircase diagram of four consistency models, descending from strongest to weakest.

- **Title (bold 14px `#1a5276`, top center):** "Consistency Ladder: Guarantee vs Coordination Cost".
- **Four boxes** (158×86px), each at `x = 22 + i*172`, `y = 56 + i*34` (stepping down left to right), fill = model color at ~8% alpha (hex + "15"), stroke = model color width 2; contents: bold 12px name, two 10px `#2c3e50` guarantee lines, 9px cost line in model color:
  1. **Linearizable** (`#1a5276`): "one global order," / "reads see latest write" — cost "needs consensus"
  2. **Sequential** (`#1a5276`): "one global order," / "reads may be stale" — cost "total-order broadcast"
  3. **Causal** (`#27ae60`): "cause before effect," / "concurrent left unordered" — cost "version metadata only"
  4. **Eventual** (`#e67e22`): "replicas converge," / "any order until they do" — cost "no ordering promise"
- **Gray `#999` arrows** step down between adjacent boxes.
- **Corner labels (10px `#999`):** top-left "stronger guarantee, more coordination"; bottom-right "weaker guarantee, cheaper and more available".
- **Caption (11px `#888`, bottom center):** "Causal is the weakest model that still forbids seeing an effect before its cause".

## Open questions callout (orange accent, after section 6)

**Open questions for discussion**

- Should the profiling pipeline detect order-dependent operations and flag them (for example, "this rolling window assumes a total order over independent events")?
- How do we set the completeness/timeliness boundary — is the grace period a tunable per pipeline, or derived from observed lag distributions?
- What is the right abstraction for late-arriving events: retraction, recompute, or append-correction?
- Can we inject variable replication lag in test environments to surface ordering bugs before production does?
- Relationship to exactly-once semantics: is at-least-once delivery plus idempotency sufficient in practice?

## Regeneration instructions

- **Layout:** backlog detail page. h1 with bottom border `2px solid #2980b9`, `.subtitle` paragraph, `.status` badge pill ("TO DISCUSS": inline-block, background `#f8f9fa`, border `1px solid #1a5276`, color `#1a5276`, padding 2px 10px, radius 12px, 0.8rem bold). Then one `.card-section` per numbered section, each with an h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` (full width, border-collapse) with one `<tr>`: left `td.text-col` (45%) for text, right `td.viz-col` (55%) for the canvas. The `.questions` callout (background `#f8f9fa`, left border `3px solid #e67e22`, padding 10px 14px, 0.9rem) sits inside the last card-section after the table.
- **Callout styles:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` — italic, `#555`, 0.9rem. `code` — background `#f8f9fa`, border `1px solid #e0e0e0`, padding 1px 5px, radius 3px, 0.85em, `#1a5276`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `strong` in `#1a5276`; ul 0.92rem with 20px left margin. Canvases styled `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes as specced; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; a shared `arrowHead(ctx, x1, y1, x2, y2, color)` helper draws 7px arrowheads. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#888`/`#999`, light gray lines `#ccc`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
