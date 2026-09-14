# Backpressure

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Backpressure

**Subtitle:** When orders arrive faster than drinks get made, the ticket rail grows without limit — backpressure lets the slow side tell the fast side to ease up

## The Cashier Who Out-Types the Barista

**Tags:** `core idea` (blue), `producer vs consumer` (green), `flow control` (orange)

- **The shop** — one cashier takes orders, one barista makes drinks, tickets wait on a rail between them
- **The rates** — the cashier rings up 4 orders a minute; the barista finishes only 2 drinks a minute
- **The pile-up** — every minute the rail gains 2 tickets that the barista will never catch up on
- **The signal** — a full rail of 10 tickets tells the cashier: stop taking orders until a slot opens
- **The name** — this stop signal flowing backward, from consumer to producer, is backpressure

*Example (italic):* At 9:05 the rail hits 10 tickets; the cashier says "one moment, please" and takes the next order only after the barista clears a drink.

**Key point:** Backpressure is the slow consumer pushing back on the fast producer — instead of letting work pile up without limit, the queue's fullness throttles how fast new work is accepted.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: order flow without backpressure (rail overflowing) vs with backpressure (a stop signal flows backward and the cashier slows to the barista's pace).

- **Title (bold 15px, `#1a5276`, top center):** "The Ticket Rail Between a Fast Cashier and a Slow Barista".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no backpressure"; blue `#2a78d6` rounded box at x=150 labeled "cashier 4/min" (12px), 3px arrow to an orange `#d95926` box at x=340 labeled "rail: 20 tickets, rising", 3px arrow to a green `#008300` box at x=545 labeled "barista 2/min"; bold 12px orange "+2 tickets every minute" above the middle box.
- **Row 2 (y=205), label:** "with backpressure"; blue box at x=150 labeled "cashier 2/min", 3px arrow to a green box at x=340 labeled "rail: 10/10 full", 3px arrow to a green box at x=545 labeled "barista 2/min"; dashed `#d55181` (dash 4/3) arrow curving backward from the rail box to the cashier box with bold 12px magenta label "STOP — rail full".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the full queue talks back — that backward signal is backpressure".
- **Caption (12px `#444`, bottom right):** "rates and ticket counts illustrative".

## Counting Tickets Minute by Minute

**Tags:** `worked example` (blue), `queue length` (green)

- **The surplus** — 4 orders in minus 2 drinks out leaves 2 extra tickets on the rail every minute
- **Ten minutes in** — with no cap, the backlog is (4 − 2) × 10 = 20 tickets at 9:10
- **The wait** — the 20th ticket sits behind 19 others at 2 drinks/min: a 10-minute wait and growing
- **With a cap** — a 10-slot rail fills at minute 5; from then on one order enters per drink finished
- **Capped wait** — no ticket is ever more than 10th in line, so the worst wait is 10 ÷ 2 = 5 minutes

*Example (italic):* By 9:10 the uncapped rail holds 20 tickets and the newest order waits 10 minutes; the capped rail holds 10 and no one waits past 5.

**Key point:** Backlog growth = arrival rate minus service rate, times minutes elapsed — backpressure replaces that unbounded straight line with a fixed ceiling at the queue's capacity.

### Visualization (canvas `c2`, 720×300)

Line chart of tickets on the rail over the first 10 minutes: uncapped backlog climbing forever vs capped backlog flattening at 10 when backpressure kicks in.

- **Title (bold 15px, `#1a5276`, top center):** "Tickets on the Rail: Uncapped Climbs 2/min, Capped Stops at 10".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 10 with 12px `#444` tick labels every 2 minutes; y = tickets 0 to 24, gridlines `#e5e9ef` at 6/12/18.
- **Uncapped line:** orange `#d95926` 3px line through minutes `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, tickets `[0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]`.
- **Capped line:** green `#008300` 3px line through the same minutes, tickets `[0, 2, 4, 6, 8, 10, 10, 10, 10, 10, 10]`.
- **Capacity line:** horizontal dashed `#6b7280` (dash 4/3) line at 10 tickets, 12px `#6b7280` label "rail capacity = 10" at its left end.
- **Marker:** vertical dashed `#6b7280` line at minute 5, 12px `#6b7280` label "cashier throttled" at its top.
- **Annotation (bold 13px green `#008300`, near minute 7.5, above the green line):** "backpressure caps the wait at 5 minutes".
- **Caption (12px `#444`, bottom right):** "rates illustrative, arithmetic exact".

## Where Slow Consumers Bite Real Systems

**Tags:** `where it's used` (blue), `memory` (orange), `streaming` (green)

- **TCP** — every download uses it: the receiver advertises a window and the sender may not exceed it
- **Message queues** — a slow consumer behind a job queue builds lag exactly like the ticket rail
- **Streaming pipelines** — stream processors and reactive-streams libraries ship backpressure built in
- **The failure** — without it, unread items buffer in the process's memory until it runs out and dies
- **The math** — a 280-item/sec surplus at 1 KB per item grows the buffer by about 1 GB every hour

*Example (italic):* A parser reads a feed at 500 msg/s but writes to its database at 220 msg/s, hoarding roughly 1 GB of unwritten rows per hour until the 8 GB box falls over.

**Key point:** Whenever one pipeline stage is slower than the stage feeding it, the difference must go somewhere — into memory, onto disk, or back upstream as a signal to slow down.

### Visualization (canvas `c3`, 720×300)

Line chart of buffer memory over 8 hours: an unbounded in-process buffer climbing 1 GB/hour toward the machine's 8 GB limit vs a bounded queue with backpressure staying flat.

- **Title (bold 15px, `#1a5276`, top center):** "500 msg/s In, 220 msg/s Out: the Buffer Eats 1 GB per Hour".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hours 0 to 8 with 12px `#444` tick labels every 2 hours; y = memory 0 to 10 GB, gridlines `#e5e9ef` at 2.5/5/7.5.
- **Unbounded line:** orange `#d95926` 3px line through hours `[0, 1, 2, 3, 4, 5, 6, 7]`, GB `[0.4, 1.4, 2.4, 3.4, 4.4, 5.4, 6.4, 7.4]`, ending in a red `#e74c3c` filled dot at hour 7.6, 8.0 GB with bold 12px red label "out of memory ~7.6h".
- **Limit line:** horizontal dashed red `#e74c3c` (dash 4/3) line at 8 GB, 12px red label "8 GB machine limit" at its left end.
- **Bounded line:** green `#008300` 3px line through the same hours, GB `[0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4]` — flat, with 12px green label "bounded queue + backpressure" above it.
- **Annotation (bold 13px violet `#4a3aa7`, near hour 3, y=80):** "the 280 msg/s surplus has to live somewhere".
- **Caption (12px `#444`, bottom right):** "rates illustrative; 280 msg/s × 1 KB ≈ 1 GB/hour".

## A Bigger Buffer Is Not a Fix

**Tags:** `common mistake` (red), `buffering` (orange)

- **The reflex** — "the queue is full, so make the queue bigger" is the most common first response
- **The arithmetic** — at a 2-ticket/min surplus, a 10-slot rail fills in 5 min; 100 slots in 50 min
- **Only delayed** — 10× the buffer buys 10× the time before the same overflow, at 10× the wait
- **The real fixes** — slow the producer (backpressure), add baristas, or deliberately drop work
- **Dropping is honest** — load shedding says no at the door; a giant buffer says yes and delivers late

*Example (italic):* Upgrading the rail from 10 to 100 slots keeps 9:05 calm, but by 9:50 it is full again — and the last order now waits 50 minutes for its coffee.

**Common mistake:** Treating a full queue as the queue's problem. The queue is only the messenger — the rate mismatch between producer and consumer is the problem, and only slowing one side or speeding the other fixes it.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: minutes until the rail overflows for three buffer sizes at the same 2-ticket/min surplus, showing that bigger buffers only delay the overflow.

- **Title (bold 15px, `#1a5276`, top center):** "Same 2/min Surplus, Bigger Rail: Overflow Delayed, Never Prevented".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 80, 145, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "10 slots": blue `#2a78d6` bar width 45, 11px `#444` label at bar end "full in 5 min — last order waits 5 min"
  - "100 slots": blue bar width 210, label "full in 50 min — last order waits 50 min"
  - "1,000 slots": orange `#d95926` bar width 440, label at bar end in 11px orange "full in 500 min — waits ~8 hrs"
- **Bar style:** 16px tall, fills `rgba(42,120,214,0.30)` for blue rows, `rgba(217,89,38,0.30)` for the orange row, 2px solid border in the row color.
- **Annotation (bold 13px magenta `#d55181`, centered near y=265):** "every 10× buffer buys 10× the time — and 10× the wait".
- **Caption (12px `#444`, bottom right):** "surplus 2 tickets/min illustrative; fill times exact, bar widths schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the coffee-shop rates (4 orders/min in, 2 drinks/min out) and the pipeline rates (500 msg/s in, 220 msg/s out at 1 KB/item) are invented and labeled illustrative; the derived numbers — 20-ticket backlog at minute 10, 5-minute capped wait, ~1 GB/hour buffer growth, out-of-memory near hour 7.6 on an 8 GB box, and fill times of 5/50/500 minutes for 10/100/1,000 slots — follow exactly from those rates.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
