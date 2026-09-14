# Stateful Stream Processing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Stateful Stream Processing

**Subtitle:** A running count per customer lives inside the stream engine as state — checkpoints snapshot that state together with the stream position, so a crash recovers to exactly where it was

## The Counter That Remembers Every Order

**Tags:** `core idea` (blue), `state` (green), `streaming` (orange)

- **The stream** — a web shop's orders arrive one at a time: alice, bob, alice, carol, bob, alice, ...
- **The question** — "is this alice's 4th order today?" must be answered the instant her order arrives
- **Stateless forgets** — a filter or parser looks at one event and moves on; it can't count
- **Stateful remembers** — a counting operator keeps a map {customer → count} between events
- **Where it lives** — that map is state inside the engine's own memory, not a lookup to an outside database
- **After 10 events** — the state reads alice: 4, bob: 3, carol: 2, dave: 1

*Example (italic):* When alice's 10th-position order arrives, the operator reads alice: 3 from its map, writes back alice: 4, and emits "alice, order #4" — no database call.

**Key point:** A stateful operator is one that remembers something between events; the remembered thing (the per-customer counts) is called state, and it lives inside the stream engine.

### Visualization (canvas `c1`, 720×300)

Step chart of alice's running count as 10 order events arrive, with the arriving customer marked under each event slot.

- **Title (bold 15px, `#1a5276`, top center):** "One Operator, Ten Events: Alice's Count Steps 1 → 4".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = event number 1 to 10, 12px `#444` tick labels at every event; y = alice's running count 0 to 5, gridlines `#e5e9ef` at 1/2/3/4.
- **Event markers:** under the baseline at y=262, a 12px `#444` customer letter per event slot: `["A","B","A","C","B","A","D","C","B","A"]`; alice's four slots (events 1, 3, 6, 10) bold green `#008300`, others `#6b7280`.
- **Step line:** green `#008300` 3px step line (horizontal-then-vertical) through alice's count after each event: events `[1,2,3,4,5,6,7,8,9,10]`, counts `[1,1,2,2,2,3,3,3,3,4]`; filled 5px green dots at the four jump points.
- **State box:** rounded box at top right (x≈470, y≈65, 190×58), fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text over three lines: "state after event 10", "{alice:4, bob:3,", " carol:2, dave:1}".
- **Annotation (bold 13px green `#008300`, near event 7, y=95):** "the operator remembers between events".
- **Caption (12px `#444`, bottom right):** "order stream illustrative".

## One Key, One Worker: Splitting the State

**Tags:** `worked example` (blue), `partition by key` (green)

- **Three workers** — the same 10-event stream is too big for one machine, so the engine runs 3 workers
- **The rule** — hash(customer) picks the worker; every event for one customer lands on the same worker
- **The split** — worker 1 gets bob, worker 2 gets alice and dave, worker 3 gets carol
- **Local state** — worker 2 holds {alice: 4, dave: 1} and never needs to ask another worker anything
- **Hand-check** — the three workers' counts sum to 4 + 3 + 2 + 1 = 10, the full event total

*Example (italic):* Alice's 4th order hashes to worker 2, which reads alice: 3 from its own local map and writes alice: 4 — workers 1 and 3 never see an alice event.

**Key point:** State is partitioned by key: each customer's counter lives on exactly one worker, so every update is a local read-and-write with no cross-worker coordination.

### Visualization (canvas `c2`, 720×300)

Flow diagram: the order stream fans out through a hash router into three worker boxes, each showing its local state map.

- **Title (bold 15px, `#1a5276`, top center):** "hash(customer) Routes Each Key to Exactly One Worker".
- **Stream box:** rounded box at x=30, y=125, 130×50, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` two-line label "order stream" / "10 events".
- **Router:** rounded box at x=215, y=125, 130×50, fill `rgba(201,133,0,0.15)`, 12px `#2c3e50` label "hash(customer) % 3"; 3px `#1a5276` arrow from stream box into it.
- **Worker boxes (x=440, 200×56 each, at y=55 / 130 / 205):** fills `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px `#2c3e50` two-line labels: "worker 1 — state {bob: 3}", "worker 2 — state {alice: 4, dave: 1}", "worker 3 — state {carol: 2}"; a 3px arrow from the router to each, arrow colors blue `#2a78d6` / green `#008300` / violet `#4a3aa7`, each arrow carrying an 11px key label ("bob", "alice, dave", "carol") at its midpoint.
- **Annotation (bold 13px violet `#4a3aa7`, bottom center near y=285):** "4 + 3 + 2 + 1 = 10 — every event counted exactly once, all lookups local".
- **Caption (12px `#444`, bottom right):** "3 workers, counts illustrative".

## The Crash Test: Checkpoint at Offset 10

**Tags:** `why it matters` (blue), `checkpointing` (green), `recovery` (orange)

- **The snapshot** — every few seconds the engine writes all state plus the stream offset to durable storage
- **Checkpoint taken** — at offset 10 it saves {alice: 4, bob: 3, carol: 2, dave: 1} + "resume at 11"
- **The crash** — the job crashes at offset 14, after four more events: alice, bob, alice, carol
- **The recovery** — the restarted job loads the checkpoint, restores alice: 4, and replays offsets 11–14
- **The landing** — after replay the state reads alice: 6, bob: 4, carol: 3 — exactly as if no crash happened
- **Without it** — counts restart from zero, or the engine must replay all 14 events from the beginning

*Example (italic):* The restarted job replays only events 11–14 (alice, bob, alice, carol) on top of the offset-10 snapshot and lands on alice: 6 — the same answer the crash interrupted.

**Key point:** A checkpoint pairs a state snapshot with the stream offset it corresponds to; recovery = load snapshot + replay from that offset, so the engine resumes exactly where it was.

### Visualization (canvas `c3`, 720×300)

Timeline of alice's running count over offsets 0–14 with a checkpoint marker, a crash marker, and the recovered path vs the no-checkpoint reset.

- **Title (bold 15px, `#1a5276`, top center):** "Crash at Offset 14: Replay 11–14 From the Checkpoint, Land on alice = 6".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = stream offset 0 to 14, 12px `#444` tick labels every 2; y = alice's count 0 to 7, gridlines `#e5e9ef` at 2/4/6.
- **Count line (pre-crash):** green `#008300` 3px step line through alice-event offsets `[1, 3, 6, 10, 11, 13]`, counts `[1, 2, 3, 4, 5, 6]`, held flat between events out to offset 14.
- **Checkpoint marker:** vertical dashed `#2a78d6` (dash 4/3) line at offset 10, bold 12px blue label at its top: "checkpoint: state + offset 10".
- **Crash marker:** bold 16px red `#e74c3c` "✗" at offset 14 on the count line, 12px red label "the job crashes".
- **Replay band:** light fill `rgba(42,120,214,0.12)` between offsets 10 and 14 from baseline to y=90, 12px `#2a78d6` label "replay 11–14" centered inside it.
- **No-checkpoint path:** red `#e74c3c` 2px dashed line from the crash dropping to count 0 at offset 14, 12px red label "no checkpoint: back to zero" near y=225.
- **Annotation (bold 13px green `#008300`, near offset 5, y=80):** "recovered state = snapshot + 4 replayed events".
- **Caption (12px `#444`, bottom right):** "offsets and counts illustrative".

## State That Never Stops Growing

**Tags:** `common mistake` (red), `state size` (orange), `TTL` (green)

- **The leak** — every first-time customer adds a key; one-time visitors never leave the map
- **The math** — at an illustrative 1 GB of new keys per week, state hits 12 GB by week 12
- **The cost** — checkpoints copy all state, so snapshots and recovery replay both slow down as it grows
- **The fix** — a TTL (time-to-live) expires keys idle for 30 days; dead customers fall out of the map
- **The plateau** — with a 30-day TTL, expiry roughly balances arrivals and state levels off near 4 GB
- **The mistake** — keying state by an unbounded id (session id, request id) with no TTL at all

*Example (italic):* Without a TTL the week-12 checkpoint is 12 GB and recovery takes minutes; with a 30-day TTL it stays near 4 GB — same live counts, a third of the snapshot.

**Common mistake:** Treating engine state as free. Every keyed operator is a map that only grows unless something deletes entries — set a TTL sized to how long a key can matter.

### Visualization (canvas `c4`, 720×300)

Line chart of total state size over 12 weeks: unbounded growth without a TTL vs a plateau with a 30-day TTL.

- **Title (bold 15px, `#1a5276`, top center):** "Keyed State Over 12 Weeks: No TTL Grows Forever, 30-Day TTL Plateaus".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = weeks 0 to 12, 12px `#444` tick labels every 2 weeks; y = state size 0 to 14 GB, gridlines `#e5e9ef` at 4/8/12, 12px `#444` labels "4 GB" / "8 GB" / "12 GB".
- **No-TTL line:** red `#e74c3c` 3px line through weeks `[0, 2, 4, 6, 8, 10, 12]`, GB `[0, 2, 4, 6, 8, 10, 12]` — straight climb; bold 12px red label "no TTL: 12 GB and rising" near its end (week 11, y=85).
- **TTL line:** green `#008300` 3px line through the same weeks, GB `[0, 2, 4, 4.3, 4.2, 4.4, 4.3]` — climbs for ~4 weeks, then flat; bold 12px green label "30-day TTL: ~4 GB steady" near week 8, y=175.
- **TTL kick-in marker:** vertical dashed `#6b7280` (dash 4/3) line at week 4, 12px `#6b7280` label "first keys expire" at its top.
- **Annotation (bold 13px orange `#d95926`, near week 6, y=60):** "checkpoint size and recovery time track state size".
- **Caption (12px `#444`, bottom right):** "GB per week illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 10-event order stream, per-customer counts, offsets, and GB-per-week figures are invented and labeled illustrative; the checkpoint arithmetic (snapshot at offset 10 + replay of events 11–14 giving alice: 6, bob: 4, carol: 3) must stay internally consistent between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
