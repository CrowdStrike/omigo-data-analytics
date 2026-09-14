# Akka Streams & Backpressure

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Akka Streams & Backpressure

**Subtitle:** In Akka Streams the slow end sends demand upstream — a consumer that can only handle 1,000 items a second makes the producer emit exactly 1,000 a second, instead of drowning in a queue

## The Pipeline That Asks Before It Is Fed

**Tags:** `core idea` (blue), `reactive streams` (green), `Akka` (orange)

- **The pipeline** — app servers pour log lines into a pipeline that parses them and writes to a database
- **The mismatch** — the servers can emit 5,000 lines/s at peak; the database writes only 1,000 rows/s
- **The pieces** — a `Source` produces the lines, a `Flow` parses them, a `Sink` writes them out
- **The twist** — data flows left to right, but permission to send flows right to left as `request(n)`
- **The name** — this upstream flow of demand is backpressure: nothing is emitted until it is asked for

*Example (italic):* The database sink signals "send me 16"; the source, capable of 5,000/s, emits exactly 16 lines and then waits for the next request.

**Key point:** Backpressure is a protocol, not a buffer — demand signals travel upstream stage by stage, so the slowest stage sets the pace for the whole pipeline.

### Visualization (canvas `c1`, 720×300)

Pipeline diagram: three stage boxes left to right with solid data arrows forward and dashed demand arrows flowing back upstream.

- **Title (bold 15px, `#1a5276`, top center):** "Data Flows Downstream, Demand Flows Upstream".
- **Stage boxes (y=115, 44px tall, 8px radius, 12px bold `#2c3e50` labels centered):** blue `#2a78d6` box at x=60 width 160 "Source — log lines (up to 5,000/s)", violet `#4a3aa7` box at x=290 width 150 "Flow — parse + enrich", green `#008300` box at x=510 width 160 "Sink — DB write (1,000/s)"; fills `rgba(42,120,214,0.15)`, `rgba(74,58,167,0.12)`, `rgba(0,131,0,0.12)`.
- **Data arrows (solid 3px `#2a78d6`, y=130, left to right):** between box edges, 12px `#2a78d6` label "log lines" above each arrow.
- **Demand arrows (dashed 2px `#008300`, dash 5/4, y=185, right to left):** from Sink back to Flow and Flow back to Source, bold 12px `#008300` label "request(16)" under each arrow.
- **Annotation (bold 13px orange `#d95926`, centered near y=235):** "the source may not send until asked — the sink sets the pace".
- **Caption (12px `#444`, bottom right):** "rates illustrative".

## Counting the Demand Tokens

**Tags:** `worked example` (blue), `demand tokens` (green)

- **The request** — the sink's internal buffer holds 16 elements, so it asks upstream for 16 at a time
- **The burst** — the fast source fills those 16 slots almost instantly, then stops: demand is spent
- **The drain** — at 1,000 writes/s the database clears 16 rows in 16 ms, then requests 16 more
- **Hand-check** — 16 ms per cycle means 62.5 cycles/s; 62.5 × 16 = 1,000 lines/s emitted, exactly the sink's speed
- **The cap** — the source never emits its 5,000/s; unspent demand of zero is a hard stop, not a hint

*Example (italic):* Over 80 ms the buffer saw-tooths five times — fill to 16, drain to 0 in 16 ms, refill — moving 80 lines, which is the 1,000/s pace.

**Key point:** Demand is counted in elements, not vague pressure — the source may emit at most as many elements as downstream has requested, so throughput equals the sink's rate by arithmetic.

### Visualization (canvas `c2`, 720×300)

Sawtooth chart of the sink buffer's occupancy over 80 ms: five fill-and-drain cycles, capped at 16 elements.

- **Title (bold 15px, `#1a5276`, top center):** "The Demand Cycle: Fill to 16, Drain in 16 ms, Ask Again".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time 0 to 80 ms, 12px `#444` tick labels every 16 ms at `[0, 16, 32, 48, 64, 80]`; y = elements in buffer 0 to 20, gridlines `#e5e9ef` at 4/8/12/16.
- **Sawtooth line (blue `#2a78d6`, 3px):** through (ms, elements) points `[(0,0), (0.5,16), (16,0), (16.5,16), (32,0), (32.5,16), (48,0), (48.5,16), (64,0), (64.5,16), (80,0)]` — near-vertical fill edges (fast source), linear 16 ms drains (1,000 writes/s).
- **Cap line:** horizontal dashed `#e74c3c` (dash 4/3) 2px line at y-value 16, bold 12px `#e74c3c` label "demand cap = 16" at its right end.
- **Request markers:** small green `#008300` filled circles (r=4) at each refill instant `[0, 16, 32, 48, 64]` ms on the baseline, one bold 12px `#008300` label "request(16)" near the first marker.
- **Annotation (bold 13px violet `#4a3aa7`, near x=48 ms, y=70):** "5 cycles × 16 lines in 80 ms = 1,000 lines/s".
- **Caption (12px `#444`, bottom right):** "buffer size 16 real Akka default, timings illustrative".

## What an Unbounded Mailbox Does Instead

**Tags:** `where it's used` (blue), `mailbox overflow` (red)

- **The actor way** — a plain actor's mailbox accepts every message immediately; senders never wait
- **The arithmetic** — 5,000 lines/s in, 1,000/s out: the mailbox grows by 4,000 messages every second
- **One minute** — after 60 s the mailbox holds 240,000 unprocessed log lines and is still growing
- **The end state** — memory fills, garbage collection thrashes, and the process dies at peak traffic
- **The stream way** — no stage's buffer in the backpressured pipeline exceeds 16, however long the peak lasts

*Example (italic):* At roughly 1 KB per log line, 60 seconds of peak traffic parks about 234 MB of unread messages in the mailbox — an hour would be ~14 GB.

**Key point:** An unbounded mailbox turns a speed mismatch into a memory leak; backpressure turns the same mismatch into a slower producer, which is the only stable outcome.

### Visualization (canvas `c3`, 720×300)

Line chart comparing queued elements over 60 seconds of peak traffic: unbounded mailbox climbing to 240,000 vs stream buffer flat at 16.

- **Title (bold 15px, `#1a5276`, top center):** "60 Seconds at Peak: Mailbox 240,000 Deep, Stream Buffer Still 16".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = seconds 0 to 60, 12px `#444` tick labels every 15 s; y = queued elements 0 to 250,000, gridlines `#e5e9ef` at 62,500/125,000/187,500 with labels "62.5k"/"125k"/"187.5k".
- **Mailbox line (red `#e74c3c`, 3px):** straight line through seconds `[0, 15, 30, 45, 60]`, queued `[0, 60000, 120000, 180000, 240000]` — growth of 4,000/s.
- **Stream line (green `#008300`, 3px):** flat through the same seconds, queued `[16, 16, 16, 16, 16]` — visually hugging the baseline.
- **Labels:** bold 12px red "unbounded mailbox (+4,000/s)" near (x≈30 s, above the red line); bold 12px green "backpressured stream — capped at 16 (flat on this scale)" near (x≈22 s, y just above baseline).
- **Annotation (bold 13px magenta `#d55181`, near x=45 s, y=95):** "the mailbox never says no — until the process dies".
- **Caption (12px `#444`, bottom right):** "rates illustrative; 5,000/s in, 1,000/s out".

## A Bigger Buffer Is Not Backpressure

**Tags:** `common mistake` (red), `buffering` (orange)

- **The reflex** — the pipeline stalls under load, so someone adds a huge buffer stage "to absorb the peak"
- **The arithmetic** — with a 4,000/s surplus, a 1,000-slot buffer fills in 0.25 s; 100,000 slots last 25 s
- **Even huge fails** — a 10-million-slot buffer buys about 42 minutes, then overflows just the same
- **The real fix** — let demand reach the true source: throttle the producers, or drop/fail deliberately
- **The mistake** — treating backpressure as a nuisance to buffer away instead of the signal to slow down

*Example (italic):* The team quadruples the buffer before a traffic spike; the crash arrives 75 seconds later than last time, with four times as many log lines lost.

**Common mistake:** Buffers only move the overflow later and make it bigger. If the surplus rate is positive, every finite buffer fills — the choice is slow down, shed load, or crash, and backpressure is how you pick on purpose.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: time until overflow for growing buffer sizes at a 4,000/s surplus, versus backpressure which never overflows.

- **Title (bold 15px, `#1a5276`, top center):** "Time to Overflow at +4,000/s Surplus: Every Finite Buffer Loses".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; pixel widths schematic (log-feel by hardcoded widths, not a real log axis).
- **Rows (top to bottom at y = 75, 125, 175, 225), each with a left-aligned 12px `#444` label at x=20:**
  - "buffer 1,000 — overflows in 0.25 s": red `#e74c3c` bar width 30
  - "buffer 100,000 — overflows in 25 s": red bar width 130
  - "buffer 10,000,000 — overflows in ~42 min": orange `#d95926` bar width 260
  - "backpressure — never overflows": green `#008300` bar width 420 with a bold 12px green "∞ — producer slowed to 1,000/s" label at the bar end
- **Bar style:** 16px tall, fills `rgba(231,76,60,0.25)` / `rgba(231,76,60,0.25)` / `rgba(217,89,38,0.25)` / `rgba(0,131,0,0.25)` with matching solid 2px borders, 11px `#444` time labels at the ends of the finite bars.
- **Annotation (bold 13px ink `#1a5276`, centered near y=270):** "a buffer delays the overflow; only upstream demand prevents it".
- **Caption (12px `#444`, bottom right):** "overflow times exact for a 4,000/s surplus; bar widths schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 5,000/s and 1,000/s rates and mailbox growth are invented and labeled illustrative; the buffer size 16 is the real Akka Streams default internal buffer; overflow times (0.25 s / 25 s / ~42 min) follow exactly from buffer size ÷ 4,000/s.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
