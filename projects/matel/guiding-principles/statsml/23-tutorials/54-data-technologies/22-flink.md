# Flink

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Flink

**Subtitle:** Flink processes each event the instant it arrives and uses watermarks to know when a time window has seen everything — so it can count late orders correctly

## The Store That Counts Orders as They Happen

**Tags:** `core idea` (blue), `streaming-first` (green), `event-at-a-time` (orange)

- **The task** — an online store counts orders per 5-minute window; the 12:00–12:05 window is due
- **The stream** — orders A–F trickle in one at a time; Flink processes each on arrival, no buffering
- **Streaming-first** — Flink treats batch as a stream that happens to end, the reverse of Spark's heritage
- **Micro-batch contrast** — Spark's classic streaming groups events into small batches and waits for each
- **The payoff** — per-event processing means an order is in the running count milliseconds after arrival

*Example (italic):* Order B arrives at 12:02:30; a 2-minute micro-batch would hold it until 12:04, but Flink adds it to the count on arrival.

**Key point:** Flink is a true event-at-a-time streaming engine — every order updates state the moment it arrives, and bounded (batch) data is just the special case of a stream that ends.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline: the same six order arrivals processed on arrival (Flink lane) vs held until the next micro-batch boundary (micro-batch lane), on a shared 12:00–12:10 time axis.

- **Title (bold 15px, `#1a5276`, top center):** "Six Orders: Processed on Arrival vs Held for the Next Micro-Batch".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "12:00" to "12:10" with 12px `#444` tick labels every 2 minutes; two lane lines 2px `#e5e9ef` at y=110 (labeled "event-at-a-time" bold 12px `#008300` at x=62) and y=190 (labeled "2-min micro-batch" bold 12px `#2a78d6` at x=62).
- **Order dots:** arrival minutes `[1.5, 2.5, 3.5, 6.5, 7.0, 8.0]`, labels `["A","B","C","E","D","F"]`; 7px-radius filled circles on both lanes at the mapped x, green `#008300` on the top lane, blue `#2a78d6` on the bottom lane, 11px `#444` letter labels above each dot.
- **Batch boundaries:** vertical dashed `#6b7280` (dash 4/3) lines at minutes 2, 4, 6, 8, 10 crossing the bottom lane only, 11px `#6b7280` label "batch fires" beside the line at minute 4.
- **Wait arrows:** on the bottom lane, 2px `#d95926` horizontal arrows from each dot to its next boundary (e.g. B at 2.5 → 4.0).
- **Annotation (bold 13px orange `#d95926`, near minute 3, y=225):** "B waits 90s for its batch; the top lane counted it instantly".
- **Caption (12px `#444`, bottom right):** "arrival times illustrative".

## Closing the 12:00 Window When One Order Is Late

**Tags:** `worked example` (blue), `event time` (green), `watermarks` (orange)

- **Two clocks** — each order carries its checkout time (event time); arrival at Flink is processing time
- **The late one** — order D is stamped 12:04 but arrives 12:07 (the phone checked out offline)
- **The question** — when may the 12:00–12:05 window close? Waiting forever is not an answer
- **The watermark** — Flink tracks "watermark = latest event time seen − 2 min": no older events are still coming
- **The close** — F (stamped 12:07:30) arrives at 12:08, watermark passes 12:05, the window fires with count 4
- **Hand-check** — A (12:01), B (12:02), C (12:03), D (12:04) fall in the window; E and F belong to 12:05–12:10

*Example (italic):* D arrives 3 minutes late at 12:07, but the watermark is still only 12:04 — the window is open, so D is counted and the result is 4, not 3.

**Key point:** A watermark is the stream declaring "no events with timestamps older than T are still coming" — it lets event-time windows close correctly even when events arrive out of order.

### Visualization (canvas `c2`, 720×300)

Scatter of processing time (x) vs event time (y) for orders A–F, with the on-time diagonal and the lagging watermark step line showing why late order D still lands in the 12:00–12:05 window.

- **Title (bold 15px, `#1a5276`, top center):** "Event Time vs Processing Time: the Watermark Trails by 2 Minutes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = processing time 0 to 9 min after 12:00, tick labels "12:00"–"12:09" every 3 min (12px `#444`); y = event time 0 to 8 min, gridlines `#e5e9ef` at event minutes 2/4/6, 12px `#444` labels "12:02"/"12:04"/"12:06".
- **On-time diagonal:** 2px `#6b7280` line from (proc 0, event 0) to (proc 8, event 8), 11px `#6b7280` label "on time" along it near proc 5.5.
- **Order dots:** 7px circles at (processing, event) = A `(1.5, 1)`, B `(2.5, 2)`, C `(3.5, 3)`, E `(6.5, 6)`, D `(7, 4)`, F `(8, 7.5)`; A/B/C/E/F blue `#2a78d6`, D magenta `#d55181` with bold 12px magenta label "D — 3 min late" beside it; 11px `#444` letters near the others.
- **Watermark step line:** dashed orange `#d95926` 3px step line through (proc, watermark) points `[1.5, 0]`, `[2.5, 0]`, `[3.5, 1]`, `[6.5, 4]`, `[7, 4]`, `[8, 5.5]` (watermark = max event time seen − 2 min, floored at 0), 12px orange label "watermark" at its right end.
- **Window-end line:** horizontal dashed `#e74c3c` (dash 4/3) line at event time 5, 11px red label "window end 12:05" at x=65.
- **Annotation (bold 13px green `#008300`, near proc 8, y=85):** "watermark passes 12:05 at 12:08 — window fires, count = 4".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative; watermark rule exact".

## A Crash at 12:06 and the Count That Survives

**Tags:** `where it's used` (blue), `keyed state` (green), `exactly-once` (orange)

- **The state** — the running count for window 12:00–12:05 lives in Flink's managed keyed state
- **The barriers** — checkpoint barriers flow with the stream every 2 min, Chandy-Lamport-style snapshots
- **The snapshots** — 12:02 saves count 1, 12:04 saves count 3, 12:06 saves count 3 (D not yet arrived)
- **The crash** — a worker dies at 12:06:48; Flink restores the 12:06 snapshot and rewinds the source
- **The replay** — E, D, F are re-read; only D lands here, so the window still fires with 4, never 5
- **Who needs it** — fraud counters, billing, feature pipelines: any count that must survive failure exactly once

*Example (italic):* Without checkpointed state, the crash at 12:06:48 either loses the count of 3 or double-counts replayed orders — with it, the answer is exactly 4 either way.

**Key point:** Flink pairs managed keyed state with periodic distributed snapshots — on failure it restores the last snapshot and replays the source, giving exactly-once state updates.

### Visualization (canvas `c3`, 720×300)

Step line of the 12:00–12:05 window's running count over processing time, with checkpoint barrier markers, a crash marker, and a restore-and-replay arrow back to the last snapshot.

- **Title (bold 15px, `#1a5276`, top center):** "Checkpoint, Crash, Restore: the Count Ends at Exactly 4".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = processing time "12:00" to "12:10" with 12px `#444` tick labels every 2 minutes; y = running count 0 to 5, gridlines `#e5e9ef` at 1/2/3/4 with 12px `#444` labels.
- **Count step line:** blue `#2a78d6` 3px step line: count rises to 1 at minute 1.5 (A), 2 at 2.5 (B), 3 at 3.5 (C), holds 3, then 4 at 7.0 (D replayed), flat to minute 10.
- **Checkpoint markers:** vertical dashed green `#008300` (dash 4/3) lines at minutes 2, 4, 6 with 11px green labels "ckpt: count 1", "ckpt: count 3", "ckpt: count 3" at their tops (y=62, staggered to avoid overlap).
- **Crash marker:** vertical solid red `#e74c3c` 3px line at minute 6.8, bold 12px red label "crash" at its top; the blue step line drawn dashed between minutes 6.8 and 7.0 to show the gap.
- **Restore arrow:** 2px violet `#4a3aa7` curved arrow from (minute 6.8, y=120) back to the minute-6 checkpoint line, 12px violet label "restore + replay" above it.
- **Annotation (bold 13px green `#008300`, near minute 8.5, y=100):** "replayed D counted once — final count 4".
- **Caption (12px `#444`, bottom right):** "counts and timings illustrative; exactly-once refers to state updates".

## Counting on the Wrong Clock

**Tags:** `common mistake` (red), `processing time` (orange)

- **The confusion** — grouping orders by when they reached Flink instead of when they were placed
- **The live run** — on arrival time, D (placed 12:04, arrived 12:07) is credited to the 12:05–12:10 bucket
- **The wrong answer** — the 12:00–12:05 window reports 3 (A, B, C) and the next window reports 3
- **The replay** — re-running the stream from the log delivers all six orders within seconds of each other
- **Different answers** — replayed arrival buckets hold 6 and 0; the live run said 3 and 3 — nothing matches
- **The fix** — window on the checkout timestamp inside each order; every run then reports 4 and 2

*Example (italic):* Live, arrival-time windows count 3 and 3; a next-morning replay arrives everything at once and counts 6 and 0 — event-time windows count 4 and 2 both times.

**Common mistake:** Windowing on processing time makes the answer depend on delivery speed, so every replay, backfill, or slow network day produces different counts — event time plus watermarks reproduces the same 4 on every run.

### Visualization (canvas `c4`, 720×300)

Grouped horizontal bar chart: counts for the two 5-minute windows under three runs — processing time live, processing time replayed, and event time (any run) — same six orders every time.

- **Title (bold 15px, `#1a5276`, top center):** "Same Six Orders, Three Runs: Only Event Time Repeats Its Answer".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, 60px per order, max width 360 (6 orders); vertical gridlines `#e5e9ef` at 1–6 orders with 11px `#444` labels along the top.
- **Rows (three groups, two bars each, 20px tall, group tops at y = 55, 130, 205; each group has a left-aligned 12px `#444` two-line label at x=20):**
  - "processing time / live run": blue `#2a78d6` bar width 180 (window 12:00–12:05 = 3), aqua `#199e70` bar width 180 (window 12:05–12:10 = 3), 11px `#444` value labels "3" at both bar ends
  - "processing time / replay": blue bar width 360 (6), aqua bar width 0 (0) drawn as a 2px tick with red `#e74c3c` bold 12px label "6 and 0 — replay disagrees"
  - "event time / any run": blue bar width 240 (4), aqua bar width 120 (2), bold 12px green `#008300` label "4 and 2, every run" at the bar ends
- **Bar style:** fills `rgba(42,120,214,0.30)` and `rgba(25,158,112,0.25)` with matching solid 2px borders; 11px legend swatches top right: blue "window 12:00–12:05", aqua "window 12:05–12:10".
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "arrival buckets change with delivery speed; checkout timestamps never do".
- **Caption (12px `#444`, bottom right):** "counts exact given the worked example's timestamps".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); one running example throughout — orders A–F with (processing, event) minutes A(1.5,1), B(2.5,2), C(3.5,3), E(6.5,6), D(7,4), F(8,7.5); order timestamps, counts, and checkpoint timings are invented and labeled illustrative; the watermark arithmetic (watermark = max event time seen − 2 min; window [12:00,12:05) fires when watermark ≥ 12:05, at F's arrival) and the window counts in c4 (processing-time live 3/3, processing-time replay 6/0, event-time 4/2) are exact given those inputs. Engine facts (event-at-a-time model, keyed state, Chandy-Lamport-style checkpoint snapshots, event vs processing time, watermark semantics) are publicly documented Flink behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
