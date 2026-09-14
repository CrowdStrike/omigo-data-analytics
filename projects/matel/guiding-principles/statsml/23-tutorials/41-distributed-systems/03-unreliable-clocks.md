# Unreliable Clocks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Unreliable Clocks

**Subtitle:** Two servers "synchronized" by NTP can still disagree by 100 milliseconds — enough to log an answer before its question and to silently lose your newest write

## The Reply That Arrived Before the Question

**Tags:** `core idea` (blue), `clock skew` (orange), `two servers` (green)

- **The setup** — a coffee shop's order app runs on two servers behind a load balancer, both NTP-synced
- **The question** — a customer's "is my latte ready?" lands on server A at true time 12:00:00.000
- **The reply** — the barista's "yes, ready now" lands on server B 40ms later, at true 12:00:00.040
- **The skew** — server A's clock runs 100ms fast, so it stamps the question 12:00:00.100
- **The scramble** — sorted by timestamp, the merged log shows the reply 60ms before the question
- **The concept** — clocks on different machines never agree exactly; the gap between them is clock skew

*Example (italic):* Reading the merged log top to bottom, the barista appears to answer 60ms before anyone asked.

**Key point:** Two machines never share one clock — "synchronized" means "close", and close can still reorder any events that happen within the skew.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline: the question and reply plotted at their true times on Server A and Server B lanes, with their stamped times, and a merged-log strip below showing the sorted order flipped.

- **Title (bold 15px, `#1a5276`, top center):** "Two Servers, One Chat: the Reply Is Stamped Before the Question".
- **Axes:** true-time axis 2px `#999` at y=210, from x=60 to x=660; scale 3px per ms, ticks every 50ms ("0ms"–"200ms", 12px `#444`).
- **Lanes:** Server A lane line 1px `#e5e9ef` at y=100, Server B lane at y=150; 12px `#444` lane labels "server A (+100ms fast)" and "server B (accurate)" at x=62 above each lane.
- **Question dot:** blue `#2a78d6` 7px radius at (60, 100) — true 0ms on A; bold 12px blue label above: "question — stamped 12:00:00.100".
- **Reply dot:** green `#008300` 7px radius at (180, 150) — true 40ms on B; bold 12px green label above: "reply — stamped 12:00:00.040".
- **Merged-log strip (y=255):** 12px `#444` label "log sorted by stamp:" at x=60; green rounded box at x=220 "1st — reply .040", blue rounded box at x=420 "2nd — question .100"; dashed `#6b7280` (dash 4/3) arrows from each dot to its box, visibly crossing.
- **Annotation (bold 13px magenta `#d55181`, near x=420, y=60):** "sorted by stamp, the answer beats its question by 60ms".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative".

## How a Fast Clock Deletes the Newest Write

**Tags:** `worked example` (blue), `clock drift` (orange), `last write wins` (green)

- **The drift** — server A's quartz clock runs 100 ppm fast: it gains 100 microseconds every second
- **Hand-check** — 100 µs/s × 1,000 s = 100 ms: about 17 minutes after a sync, A is 100ms ahead
- **The first write** — at true 10:00:00.000 an order sets size=large via server A, stamped 10:00:00.100
- **The correction** — 60ms later the customer taps size=medium via server B, stamped 10:00:00.060
- **Last write wins** — the store keeps the "latest" stamp: .100 beats .060, so size=large wins
- **The loss** — the newer write is discarded with no error; the medium simply never happened

*Example (italic):* The customer ordered medium 60ms after large, but the orders table remembers large forever.

**Key point:** Last-write-wins trusts timestamps; a clock only 100ms fast makes an older write look newer, so the real latest write is silently deleted.

### Visualization (canvas `c2`, 720×300)

Two-row flow diagram: the true order of the two writes (top) vs what last-write-wins keeps after comparing stamps (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Last Write Wins: a 100ms-Fast Clock Crowns the Wrong Winner".
- **Row 1 (y=95), 12px `#444` label at x=20:** "what happened"; blue `#2a78d6` rounded box at x=170 labeled "size=large" with 11px sub-label "true .000 → stamp .100"; 3px arrow to a green `#008300` rounded box at x=430 labeled "size=medium" with 11px sub-label "true .060 → stamp .060".
- **Row 2 (y=205), label:** "what LWW keeps"; violet `#4a3aa7` rounded box at x=170 labeled "compare stamps" with 11px sub-label ".100 vs .060", 3px arrow to a red `#e74c3c` rounded box at x=430 labeled "keep size=large" with bold 12px red "✗ the correction is lost" at its right.
- **Box style:** 150–170px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(74,58,167,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the newer write lost because its clock was honest".
- **Caption (12px `#444`, bottom right):** "stamps illustrative; drift math exact (100 ppm × 1,000s = 100ms)".

## Where a 100ms Disagreement Bites

**Tags:** `where it's used` (blue), `logs` (green), `two clock types` (orange)

- **Debugging logs** — merged logs from many machines can put effects before causes; you chase ghosts
- **Databases** — some replicated stores resolve write conflicts by timestamp, so skew becomes data loss
- **The window** — with 100ms of skew, any two events closer than 100ms can appear in either order
- **Two clock types** — time-of-day clocks answer "what time is it"; monotonic clocks answer "how long"
- **NTP steps** — a sync can jump the time-of-day clock backward; a monotonic clock never moves back
- **Durations** — measure elapsed time with the monotonic clock; the wall clock is for humans

*Example (italic):* Just before a sync, A runs 100ms ahead of B, so a request and its response 40ms apart can log in either order.

**Key point:** A timestamp orders events on one machine only; across machines, anything inside the skew window is effectively unordered.

### Visualization (canvas `c3`, 720×300)

Line chart of each server's clock offset from true time over an hour: server A drifts ahead in a sawtooth (NTP pulls it back every ~17 minutes), server B stays flat — the gap between the lines is the disagreement.

- **Title (bold 15px, `#1a5276`, top center):** "Between Syncs, a 100 ppm Clock Drifts 100ms Ahead".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 60, 12px `#444` tick labels every 10 min; y = offset from true time 0 to 120ms, gridlines `#e5e9ef` at 40/80/120 with 12px `#444` labels.
- **Server A sawtooth:** blue `#2a78d6` 3px line through (minute, ms) points `[0,0], [16.7,100], [16.7,0], [33.3,100], [33.3,0], [50,100], [50,0], [60,60]` — three full ramps to 100ms, each cut to 0 by a sync.
- **Server B line:** green `#008300` 3px flat line at 0ms, 12px green label "server B (accurate)" near x=55 min.
- **Sync markers:** vertical dashed `#6b7280` (dash 4/3) lines at minutes 16.7, 33.3, 50; one 12px `#6b7280` label "NTP sync" at the first marker's top.
- **100ms guide:** horizontal dashed `#6b7280` line at 100ms with 12px label "100ms".
- **Annotation (bold 13px orange `#d95926`, near x=30 min, y at 112ms):** "just before each sync, A and B disagree by 100ms".
- **Caption (12px `#444`, bottom right):** "drift rate illustrative; 100 ppm × 1,000s = 100ms exact".

## Wall Clocks Are Not Stopwatches

**Tags:** `common mistake` (red), `monotonic clock` (orange)

- **The mistake** — sorting cross-machine events by wall-clock stamps and treating the order as truth
- **Its twin** — timing code as wall-clock end minus start, as if the clock can't change in between
- **The step-back** — a request starts at wall reading 12:00:00.130; NTP steps the clock back 100ms
- **Negative time** — the end reads 12:00:00.090, so the 60ms request "took" .090 − .130 = −40ms
- **The fix** — the monotonic clock reads 5000.000s then 5000.060s: 60ms, immune to steps
- **The other fix** — order cross-machine events with sequence numbers or logical clocks, not wall time

*Example (italic):* A latency dashboard averaging in a −40ms request quietly understates every percentile it reports.

**Common mistake:** Trusting the subtraction of two wall-clock readings. The wall clock can be adjusted between your two reads; only a monotonic clock guarantees the difference is a duration.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same 60ms request timed with the wall clock (NTP step lands mid-request, duration goes negative) vs the monotonic clock (correct).

- **Title (bold 15px, `#1a5276`, top center):** "Timing a 60ms Request: Wall Clock vs Monotonic Clock".
- **Row 1 (y=95), 12px `#444` label at x=20:** "wall clock"; blue `#2a78d6` rounded box at x=150 labeled "start: 12:00:00.130"; 3px arrow with 11px `#6b7280` label above it "NTP steps clock −100ms"; red `#e74c3c` rounded box at x=410 labeled "end: 12:00:00.090"; bold 12px red at x=585 "duration −40ms ✗".
- **Row 2 (y=205), label:** "monotonic"; blue rounded box at x=150 labeled "start: 5000.000s"; 3px arrow with 11px `#6b7280` label "never steps"; green `#008300` rounded box at x=410 labeled "end: 5000.060s"; bold 12px green at x=585 "duration 60ms ✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the request took 60ms either way — only one clock knows it".
- **Caption (12px `#444`, bottom right):** "clock readings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the chat stamps (question true .000 stamped .100, reply true .040 stamped .040), the LWW stamps (.100 vs .060), the sawtooth points, and the timing reads (.130 → .090 vs 5000.000s → 5000.060s) are invented and labeled illustrative; the drift arithmetic (100 ppm × 1,000 s = 100 ms) and the −40ms/60ms subtractions are exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
