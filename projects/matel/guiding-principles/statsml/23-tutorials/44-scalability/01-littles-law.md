# Little's Law

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Little's Law

**Subtitle:** items = rate × time — one identity that sizes every queue, from a coffee shop line to a server's thread pool

## Counting Heads in the Coffee Shop

**Tags:** `core idea` (blue), `L = λW` (green), `queues` (orange)

- **The shop** — customers walk into a coffee shop at a steady 2 per minute all morning
- **The stay** — each customer spends about 5 minutes inside: order, wait, pick up, leave
- **The count** — stand at the door and count heads at any moment: about 10 people inside
- **The identity** — items in system = arrival rate × time in system: L = λW, so 2 × 5 = 10
- **No fine print** — it holds for any stable system, whatever the arrival or service pattern

*Example (italic):* At 2 customers/min staying 5 min each, a headcount at 9:15, 10:40, or 11:55 hovers around the same 10 people.

**Key point:** Little's Law says the average number in a system equals arrival rate times average time in system — an identity that needs no assumptions about distributions.

### Visualization (canvas `c1`, 720×300)

Line chart of the coffee shop headcount over one hour, wobbling around the L = λW prediction of 10.

- **Title (bold 15px, `#1a5276`, top center):** "Headcount All Morning: 2/min × 5 min ≈ 10 People Inside".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 60 with 12px `#444` tick labels every 10 min; y = people inside 0 to 16, gridlines `#e5e9ef` at 4/8/12.
- **Headcount line:** blue `#2a78d6` 3px line with 4px dots through minutes `[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]`, headcounts `[10, 11, 9, 12, 8, 10, 11, 9, 12, 8, 10, 10, 10]` (averages exactly 10).
- **Prediction line:** green `#008300` dashed (dash 6/4) horizontal 2px line at y for 10, 12px green label "L = λW = 2 × 5 = 10" above it at the right end.
- **Annotation (bold 13px `#1a5276`, near minute 20, y=80):** "wobbles, but the average is pinned at 10".
- **Caption (12px `#444`, bottom right):** "headcounts illustrative; the identity is exact for true averages".

## Ten Customers, Checked by Hand

**Tags:** `worked example` (blue), `arrivals vs departures` (green)

- **Two counters** — track cumulative arrivals and cumulative departures minute by minute
- **Arrivals** — at 2/min: 10 in by minute 5, 20 by minute 10, 60 by minute 30
- **Departures** — the same curve shifted right 5 minutes: 10 out by minute 10, 50 by minute 30
- **Vertical gap** — arrivals minus departures at any minute = people currently inside = 10
- **Horizontal gap** — the sideways distance between the curves = each customer's 5-minute stay

*Example (italic):* By minute 15, 30 customers have arrived and 20 have left — 30 − 20 = 10 inside, exactly λW.

**Key point:** The vertical gap between the arrival and departure curves is L, the horizontal gap is W — the two gaps are locked together by the arrival rate λ.

### Visualization (canvas `c2`, 720×300)

Two cumulative step-style lines (arrivals and departures) whose vertical and horizontal gaps make Little's Law visible.

- **Title (bold 15px, `#1a5276`, top center):** "Cumulative Arrivals vs Departures: the Two Gaps of L = λW".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 30, 12px `#444` tick labels every 5 min; y = cumulative customers 0 to 60, gridlines `#e5e9ef` at 15/30/45.
- **Arrivals line:** blue `#2a78d6` 3px line through minutes `[0, 5, 10, 15, 20, 25, 30]`, counts `[0, 10, 20, 30, 40, 50, 60]`, 12px blue label "arrived" near its upper end.
- **Departures line:** green `#008300` 3px line through the same minutes, counts `[0, 0, 10, 20, 30, 40, 50]`, 12px green label "left" near its lower end.
- **Vertical gap marker:** violet `#4a3aa7` 2px double-arrow segment at minute 15 spanning counts 20 to 30, bold 12px violet label "gap = 10 inside (L)".
- **Horizontal gap marker:** orange `#d95926` 2px double-arrow segment at count 30 spanning minutes 15 to 20, bold 12px orange label "gap = 5 min stay (W)".
- **Caption (12px `#444`, bottom right):** "steady 2/min arrivals, 5-min stays — counts exact by construction".

## From Coffee Shop to Thread Pool

**Tags:** `where it's used` (blue), `capacity sizing` (green)

- **Same law** — swap customers for requests: 100 requests/s arriving, each taking 0.2 s end to end
- **The concurrency** — L = 100 × 0.2 = 20 requests are in flight at any moment, on average
- **The pool** — a thread pool, connection pool, or queue must hold that L, or work backs up
- **Scaling check** — double traffic to 200 req/s at the same 0.2 s and in-flight doubles to 40
- **Stability caveat** — the law assumes a stable system; if arrivals outrun capacity, W has no finite average

*Example (italic):* A service at 100 req/s with 0.2 s latency needs room for 20 concurrent requests — a 30-thread pool is fine until traffic hits 200 req/s and 40 are in flight.

**Key point:** Little's Law turns two numbers every dashboard already shows — throughput and latency — into the concurrency you must provision for.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of in-flight requests (λ × 0.2 s) at four traffic levels, against a dashed 30-thread pool limit.

- **Title (bold 15px, `#1a5276`, top center):** "In-Flight Requests at 0.2 s Latency: λ × W vs a 30-Thread Pool".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = concurrent requests 0 to 50, gridlines `#e5e9ef` at 10/20/30/40; x labels 12px `#444` under each bar.
- **Bars (70px wide, centered at x = 140, 280, 420, 560):** rates `[25, 50, 100, 200]` req/s giving concurrency `[5, 10, 20, 40]`; first three fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, the 200 req/s bar fill `rgba(231,76,60,0.25)` with 2px `#e74c3c` border; bold 13px value labels ("5", "10", "20", "40") above each bar.
- **Pool limit:** red `#e74c3c` dashed (dash 6/4) 2px horizontal line at the 30 level, 12px red label "pool size = 30" at its left end.
- **Annotation (bold 13px red `#e74c3c`, above the 200 req/s bar):** "40 in flight — the pool saturates".
- **Caption (12px `#444`, bottom right):** "rates illustrative; concurrency = rate × 0.2 s exactly".

## W Is the Whole Stay, Not Just the Service

**Tags:** `common mistake` (red), `time in system` (orange)

- **The slip** — plugging in service time alone when W means total time in system, queue included
- **The split** — a request needs 0.05 s of actual work but spends 0.2 s inside once waiting is counted
- **The undercount** — 100 × 0.05 = 5 counts only requests being served, not the 15 sitting in queue
- **The truth** — 100 × 0.2 = 20 requests hold a slot at any moment; the pool must fit all 20
- **The rule** — W starts when the item enters and ends when it leaves; measure the whole stay

*Example (italic):* A connection pool sized to 5 from the 0.05 s work time runs dry immediately — 20 requests hold connections when the full 0.2 s stay is counted.

**Common mistake:** Feeding Little's Law the service time instead of the time in system. The law is an identity, not an approximation — feed it the wrong W and the exact answer to the wrong question undersizes the pool 4×.

### Visualization (canvas `c4`, 720×300)

Two horizontal bars comparing pool sizing from service time vs full time in system, against the true in-flight count.

- **Title (bold 15px, `#1a5276`, top center):** "Same Law, Wrong W: 100 req/s Sized Two Ways".
- **Layout:** left-aligned 12px `#444` row labels at x=20, bars start at x=230, max width 400 mapping 0–20 requests; true-need marker at the 20 mark.
- **Row 1 (y=100):** label "W = 0.05 s (service only)"; red `#e74c3c` solid bar width 100 (value 5), bold 13px red value label "5" at bar end, 12px red note "15 requests left waiting" to its right.
- **Row 2 (y=180):** label "W = 0.2 s (whole stay)"; green `#008300` solid bar width 400 (value 20), bold 13px green value label "20" at bar end.
- **Need marker:** violet `#4a3aa7` dashed (dash 4/3) vertical 2px line at the 20 mark from y=60 to y=230, bold 12px violet label "true in-flight = 20" at its top.
- **Bar style:** 26px tall, 4px corner radius, subtle matching fills `rgba(231,76,60,0.15)` / `rgba(0,131,0,0.12)` behind the solid value bars.
- **Annotation (bold 13px orange `#d95926`, centered near y=265):** "W is door-to-door time — queue wait counts".
- **Caption (12px `#444`, bottom right):** "0.05 s / 0.2 s split illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); coffee shop headcounts and traffic rates are invented and labeled illustrative; every derived concurrency is the exact product λ × W (2×5=10, 25×0.2=5, 50×0.2=10, 100×0.2=20, 200×0.2=40, 100×0.05=5) so text and chart numbers always agree.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
