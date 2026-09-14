# Producer-Consumer

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Producer-Consumer

**Subtitle:** One side makes work, the other side does it, and a fixed-size buffer between them forces the fast side to wait for the slow side — the original backpressure

## The Ticket Rail With Eight Slots

**Tags:** `core idea` (blue), `bounded buffer` (green), `backpressure` (orange)

- **The cashier** — takes coffee orders at the register and clips each ticket onto a rail
- **The barista** — pulls tickets off the other end of the rail, one at a time, and makes the drinks
- **The rail** — holds at most 8 tickets; it is the only thing connecting the two workers
- **Rail full** — the cashier physically cannot clip a ninth ticket, so she stops taking orders
- **Rail empty** — the barista has nothing to pull, so he waits; neither ever talks to the other

*Example (italic):* At the morning rush the rail fills to 8, the cashier tells the next customer "one moment", and the line at the register slows to exactly the barista's pace.

**Key point:** Producer-consumer is two workers sharing a fixed-size buffer: the producer blocks when it is full, the consumer blocks when it is empty — the full rail is a stop signal that flows backward, which is all backpressure means.

### Visualization (canvas `c1`, 720×300)

Flow diagram: cashier box, an 8-slot ticket rail drawn as a row of small squares (6 filled, 2 empty), barista box; a second row shows the rail full with the cashier blocked.

- **Title (bold 15px, `#1a5276`, top center):** "Two Workers, One Rail: the Buffer Is the Only Conversation".
- **Row 1 (y=110), label 12px `#444` at x=20:** "rail has room"; blue `#2a78d6` rounded box at x=80 labeled "cashier adds tickets" (12px), 3px blue arrow to the rail at x=270: eight 32×32 slots, first 6 filled `rgba(42,120,214,0.30)` with 11px `#2c3e50` counts "1"–"6", last 2 empty with `#e5e9ef` borders; 3px green `#008300` arrow to a green box at x=580 labeled "barista pulls tickets".
- **Row 2 (y=215), label:** "rail full — 8/8"; same layout but all 8 slots filled, the cashier→rail arrow replaced by a red `#e74c3c` bar with bold 12px red label "cashier waits", the rail→barista arrow still green with 12px green label "barista keeps working".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=278):** "the full rail pushes back on the producer — no message needed".
- **Caption (12px `#444`, bottom right):** "slot counts illustrative".

## Eight Minutes to a Full Rail

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **The rates** — the cashier takes 3 orders per minute; the barista makes 2 drinks per minute
- **The gap** — the rail gains 3 − 2 = 1 ticket every minute while the cashier runs free
- **Hand-check** — at minute 5: 15 orders taken, 10 drinks made, 15 − 10 = 5 tickets on the rail
- **The wall** — the rail hits its 8-ticket cap at minute 8 (8 slots ÷ 1 ticket per minute)
- **After the wall** — the cashier can only add when the barista removes, so both run at 2 per minute
- **The wait** — the last ticket on a full rail is done 8 × 30s = 4 minutes later (7 ahead + its own)

*Example (italic):* From minute 8 onward the register accepts exactly 2 orders per minute — the barista's pace, set by nobody saying anything.

**Key point:** The buffer converts a rate mismatch into a bounded queue: it absorbs the first 8 tickets of overflow, then throttles the producer down to the consumer's speed forever after.

### Visualization (canvas `c2`, 720×300)

Line chart of tickets on the rail for minutes 0–12: a straight climb from 0 to 8, then flat at the cap, with the cashier's intake rate stepping from 3/min down to 2/min at minute 8.

- **Title (bold 15px, `#1a5276`, top center):** "Rail Fills at +1/min, Caps at 8, Then the Cashier Slows to 2/min".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 12 with 12px `#444` tick labels every 2 minutes; y = tickets on rail 0 to 10, gridlines `#e5e9ef` at 2/4/6/8.
- **Rail line:** blue `#2a78d6` 3px line through minutes `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]`, tickets `[0, 1, 2, 3, 4, 5, 6, 7, 8, 8, 8, 8, 8]`.
- **Cap line:** dashed red `#e74c3c` (dash 4/3) horizontal line at tickets = 8, 12px red label "rail capacity 8" at its left end.
- **Wall marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 8, 12px `#6b7280` label "rail full" at its top.
- **Rate step:** green `#008300` 2px step line on the same axes (read against the same 0–10 scale) at intake per minute `[3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2]`, 12px green label "orders taken/min" near minute 2.
- **Annotation (bold 13px green `#008300`, near minute 9.5, y=80):** "backpressure: intake drops to the barista's 2/min".
- **Caption (12px `#444`, bottom right):** "rates illustrative; blue counts tickets, green is a rate (orders/min) on one scale".

## Every Pipeline Has a Slower Stage

**Tags:** `where it's used` (blue), `queues` (green), `streaming` (orange)

- **Message queues** — a service writes events, another reads them; the broker's queue is the rail
- **ETL jobs** — a reader pulls rows faster than the loader writes them; a bounded batch buffer sits between
- **Thread pools** — requests arrive as fast as users click; a fixed task queue feeds a few worker threads
- **Streaming** — a sensor emits readings nonstop; the pipeline buffers a window and drops or blocks past it
- **The cap rule** — throughput is always the slower stage's rate; the buffer only decides who waits, and where

*Example (italic):* With the barista at 2 drinks per minute, a rail of 8 or a rail of 800 both serve exactly 2 customers per minute — the bigger rail just makes ticket 800 wait 400 minutes.

**Key point:** A data scientist meets producer-consumer anywhere two stages run at different speeds — the bounded buffer is what keeps the fast stage honest instead of letting work pile up out of sight.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: producer capacity, consumer capacity, and actual system throughput, showing the system pinned to the slower stage regardless of buffer size.

- **Title (bold 15px, `#1a5276`, top center):** "The Buffer Never Raises Throughput — the Slow Stage Sets It".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 400 = 3 per minute; 12px `#444` scale labels "1/min", "2/min", "3/min" below at x = 383, 517, 650.
- **Rows (top to bottom at y = 80, 135, 190, 245), each with a left-aligned 12px `#444` label at x=20:**
  - "cashier capacity": blue `#2a78d6` bar width 400 (3/min)
  - "barista capacity": green `#008300` bar width 267 (2/min)
  - "throughput, rail = 8": aqua `#199e70` bar width 267 (2/min)
  - "throughput, rail = 800": aqua `#199e70` bar width 267 (2/min), bold 12px orange `#d95926` label "same 2/min" at the bar end
- **Bar style:** 22px tall, fills at 0.85 alpha, 11px `#444` value labels ("3/min", "2/min") at bar ends.
- **Annotation (bold 13px magenta `#d55181`, right side near y=48):** "buffer size changes the wait, never the speed".
- **Caption (12px `#444`, bottom left):** "rates illustrative".

## The Unbounded Queue Trap

**Tags:** `common mistake` (red), `unbounded buffer` (orange)

- **The temptation** — "the cashier keeps blocking, so let's just use a rail with no limit"
- **What happens** — the rail grows by 1 ticket every minute, forever, because 3 in beats 2 out
- **The hidden cost** — by minute 60 there are 60 tickets, and the newest customer waits 60 × 30s = 30 minutes
- **The crash** — in software the queue is memory; an unbounded queue is an out-of-memory error on a delay
- **The tell** — a "healthy" service whose queue length only ever grows is already failing, just quietly

*Example (italic):* The bounded rail caps the worst start-to-done time at 4 minutes; the unbounded rail reaches a 30-minute wait within the first hour and keeps getting worse.

**Common mistake:** Treating the buffer's block as the bug and removing the bound. The block IS the feature — it surfaces the rate mismatch immediately, instead of converting it into unbounded memory and unbounded latency.

### Visualization (canvas `c4`, 720×300)

Line chart over 60 minutes: unbounded queue length climbing linearly to 60 vs bounded queue flat at 8 after minute 8.

- **Title (bold 15px, `#1a5276`, top center):** "Unbounded Rail: +1 Ticket Every Minute, Forever".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 60 with 12px `#444` tick labels every 15 minutes; y = tickets waiting 0 to 60, gridlines `#e5e9ef` at 15/30/45.
- **Unbounded line:** red `#e74c3c` 3px line through minutes `[0, 15, 30, 45, 60]`, tickets `[0, 15, 30, 45, 60]` — a straight diagonal.
- **Bounded line:** green `#008300` 3px line through minutes `[0, 8, 15, 30, 45, 60]`, tickets `[0, 8, 8, 8, 8, 8]` — flat at the cap after minute 8.
- **Labels:** bold 12px red "no limit — 60 tickets, 30-min wait" near (x≈40 min, y above the red line); bold 12px green "capped at 8 — 4-min worst wait" near (x≈35 min, y just above the green line).
- **Annotation (bold 13px violet `#4a3aa7`, near x=18 min, y=70):** "the queue is memory: this line is a slow-motion crash".
- **Caption (12px `#444`, bottom right):** "growth exact for 3/min in, 2/min out; scenario illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 3/min producer rate, 2/min consumer rate, and 8-slot rail are invented and labeled illustrative; everything else is arithmetic on them — rail counts `[0..8]` capped at 8, intake step `3→2` at minute 8, throughput bars pinned at 2/min, unbounded growth `[0,15,30,45,60]`, worst-case waits 4 min (8 × 30s) and 30 min (60 × 30s).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
