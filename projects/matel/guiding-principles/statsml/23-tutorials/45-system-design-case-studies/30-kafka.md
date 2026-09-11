# Kafka

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Kafka

**Subtitle:** The pipeline-sprawl problem — N data-producing systems wired to M consuming systems meant N×M bespoke pipelines; Kafka put one replayable commit log in the middle and cut it to N+M, and that log became a standard piece of infrastructure

## Every System Wired to Every Other System

**Tags:** `core idea` (blue), `the pipeline problem` (red), `N×M` (orange)

- **The producers** — metrics, app logs, database changes, click events each pour out of a different system
- **The consumers** — the warehouse, search index, monitoring, and recommendations all want that data
- **The wiring** — every producer-consumer pair got its own bespoke pipeline: 4×4 = 16 integrations
- **The fragility** — each pipeline had its own format, its own failure modes, its own backfill script
- **The bill** — adding a 5th consumer means 4 new pipelines (20 total) before it sees any data

*Example (italic):* Engineers at large data-driven companies have blogged that they kept rebuilding the same fragile point-to-point pipeline — the mesh grew as producers × consumers, faster than any team could maintain.

**Key point:** Point-to-point integration cost multiplies: 4 producers and 4 consumers is already 16 pipelines. The fix has to be structural — no amount of pipeline-building keeps up with N×M.

### Visualization (canvas `c1`, 720×300)

Two-panel wiring diagram: the point-to-point mesh (left, 16 crossing lines) vs the same systems around one central commit log (right, 8 lines).

- **Title (bold 15px, `#1a5276`, top center):** "The Same 8 Systems: 16 Pipelines vs 8".
- **Divider:** vertical dashed `#6b7280` (dash 4/3) line at x=360, from y=50 to y=280.
- **Left panel label (bold 13px red `#e74c3c`, centered at x=180, y=58):** "point-to-point: 4×4 = 16".
- **Left boxes:** 4 producer boxes at x=30 (100 wide, 30 tall, 6px radius, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, 11px `#2c3e50` text) at y = 80, 130, 180, 230 labeled "metrics", "app logs", "DB changes", "click events"; 4 consumer boxes same style at x=230 at the same y values labeled "warehouse", "search", "monitor", "recs".
- **Left lines:** 16 lines, 1px `rgba(231,76,60,0.45)`, from every producer's right edge (x=130, box mid-height) to every consumer's left edge (x=230, box mid-height).
- **Right panel label (bold 13px green `#008300`, centered at x=545, y=58):** "one log: 4+4 = 8".
- **Right boxes:** producers at x=385 (same 4 labels, same style, y = 80, 130, 180, 230); one central log box at x=520, y=80, 60 wide, 180 tall, 8px radius, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 11px text "commit log" stacked vertically; consumers at x=605 (same 4 labels, y = 80, 130, 180, 230).
- **Right lines:** 4 arrows (2px `#008300`) from producer right edges into the log's left edge; 4 arrows from the log's right edge to consumer left edges.
- **Caption (12px `#444`, bottom right):** "system names illustrative; pipeline counts exact".

## One Log, Every Reader at Its Own Offset

**Tags:** `worked example` (blue), `offsets` (green), `replay` (orange)

- **The log** — one append-only sequence of records; a producer only ever appends at the end
- **The offset** — record 12 was just appended; each consumer remembers the offset it has read up to
- **Independence** — the warehouse is at offset 12, search at offset 9: search is 12−9 = 3 records behind
- **Replay** — a brand-new consumer starts at offset 3 (or 0) and catches up by reading old records
- **Retention** — records stay for a set window (say 7 days), consumed or not; reading deletes nothing
- **Partitions** — Kafka splits a topic into many such logs so load spreads across machines

*Example (italic):* A new monitoring service comes online, sets its offset to 3, replays history, and is caught up by lunch — no producer changed a line of code.

**Key point:** One write per producer, one independent cursor per consumer — that is how N×M pipelines become N+M integrations, and retention makes replay free.

### Visualization (canvas `c2`, 720×300)

Diagram of one partition: a row of 13 numbered log cells (offsets 0–12) with three consumer offset pointers below, each at a different position.

- **Title (bold 15px, `#1a5276`, top center):** "One Partition: Three Readers, Three Independent Offsets".
- **Log cells:** 13 boxes, 44px wide, 36px tall, starting at x=60, y=100, 2px gap; offsets 0–11 fill `rgba(42,120,214,0.15)` with 1px `#2a78d6` border; offset 12 fill `rgba(0,131,0,0.15)` with 2px `#008300` border; each cell shows its offset number centered, 12px `#2c3e50`.
- **Producer marker:** bold 12px green `#008300` label "producer appends → offset 12" at y=75 above the last cell, with a short 2px green arrow down to the offset-12 cell.
- **Consumer pointers (each a 2px arrow from a label up to the bottom edge of its cell):**
  - warehouse: green `#008300`, arrow to offset-12 cell, 12px label "warehouse — offset 12 (caught up)" right-aligned at the canvas right edge, y=185
  - search: blue `#2a78d6`, arrow to offset-9 cell, 12px label "search — offset 9 (lag 3)" at y=215
  - new monitor: orange `#d95926`, arrow to offset-3 cell, 12px label "new monitor — offset 3 (replaying)" at y=245
- **Annotation (bold 12px violet `#4a3aa7`, right side near y=270):** "each reader keeps its own offset — the log never changes".
- **Caption (12px `#444`, bottom right):** "offsets illustrative; lag 12−9 = 3 exact".

## From In-House Fix to Industry Backbone

**Tags:** `where it's used` (blue), `Kafka` (green), `the log is the truth` (orange)

- **Open-sourced** — the in-house fix was released as Apache Kafka and became a standard tool
- **The math** — with N producers and M consumers, integrations drop from N×M to N+M (exact)
- **Change data capture** — database commit logs stream into Kafka so other systems mirror every change
- **Stream processing** — processors read the log, transform, and write results back as new topics
- **The slogan** — the log is the truth; every database downstream is just a view built from it

*Example (italic):* A search index, a cache, and a warehouse all replay the same change log — three "views" that agree because they were materialized from one shared truth.

**Key point:** Kafka outgrew its pipeline-cleanup origin and became a backbone pattern — change-data-capture, stream processing, and keeping every derived system in sync with the record of what happened.

### Visualization (canvas `c3`, 720×300)

Line chart: integrations needed as a company grows to N producing and N consuming systems — point-to-point (N×M) vs through one log (N+M).

- **Title (bold 15px, `#1a5276`, top center):** "Why It Spread: Integrations as Systems Multiply".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = N systems on each side, 2 to 10, 12px `#444` tick labels every 2; y = integrations 0 to 100, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels.
- **Point-to-point line:** red `#e74c3c` 3px line through N `[2, 3, 4, 5, 6, 7, 8, 9, 10]`, integrations `[4, 9, 16, 25, 36, 49, 64, 81, 100]` (N×M with M=N).
- **Log line:** green `#008300` 3px line through the same N grid, integrations `[4, 6, 8, 10, 12, 14, 16, 18, 20]` (N+M with M=N).
- **Line labels:** bold 12px red "N×M point-to-point" near (x≈8, upper region); bold 12px green "N+M via the log" near (x≈8, just above the green line).
- **Annotation (bold 13px red `#e74c3c`, near x=9, y=70):** "10 systems each side: 100 vs 20".
- **Caption (12px `#444`, bottom right):** "counts exact: N×M and N+M with N=M".

## Not Just Another Message Queue

**Tags:** `common mistake` (red), `queue vs log` (orange)

- **The confusion** — people file Kafka under "message queue"; a queue deletes a message once consumed
- **The queue** — a classic work queue hands each message to one consumer; a second reader gets nothing
- **The log** — records are removed by the retention clock, never by being read
- **The cursor** — a queue tracks delivery per message; a log stores one offset per consumer group
- **The cost** — treating the log as a queue forfeits its point: no replay, no new-consumer backfill

*Example (italic):* A team copies events into a second topic "for the new consumer" — duplicating data the original topic would have replayed from offset 0 for free.

**Common mistake:** Calling Kafka a message queue. A queue destroys messages on consumption; the log retains them, so any number of consumers — including ones written next year — can read the same history independently.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a message through a queue (deleted after one read, second reader blocked) vs the same message in a log (retained, both readers served at their own offsets).

- **Title (bold 15px, `#1a5276`, top center):** "Queue vs Log: Consumed Is Not Deleted".
- **Row 1 (y=95), label 12px `#444` at x=20:** "queue"; blue `#2a78d6` rounded box at x=130 labeled "message m" (12px), 3px arrow to a blue box at x=310 labeled "consumer A reads m", 3px arrow to a red `#e74c3c` box at x=510 labeled "m deleted — B gets nothing" with bold 12px red "✗" beside it.
- **Row 2 (y=205), label:** "log"; blue box at x=130 labeled "m at offset 7, retained", two 3px arrows fanning out to two green `#008300` boxes: one at x=360, y=180 labeled "A reads, offset → 8", one at x=360, y=230 labeled "B reads, offset → 8", then bold 12px green "✓ replay anytime in retention" at x=560, y=205.
- **Box style:** 140–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "the log retains; consumers keep offsets — replay is the feature".
- **Caption (12px `#444`, bottom right):** "offsets illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the arithmetic is exact — 4×4 = 16 vs 4+4 = 8, the N²/2N curve values, and the lag 12−9 = 3; system names, offsets, and the 7-day retention window are invented and labeled illustrative. The pipeline-sprawl-to-unified-log arc and the log-as-source-of-truth framing are widely published; Kafka is named only for its publicly documented behavior (append-only partitioned topics, per-consumer-group offsets, time-based retention) and no undocumented internals are attributed to it.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
