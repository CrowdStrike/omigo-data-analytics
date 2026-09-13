# Batch Scoring vs Real-Time Inference

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Batch Scoring vs Real-Time Inference

**Subtitle:** You can compute predictions ahead of time for everyone, or on the spot for whoever asks — the choice is a three-way trade between cost, latency, and freshness

## Two Ways to Hand Out Tomorrow's Coupon

**Tags:** `core idea` (blue), `serving paths` (green), `when the work happens` (orange)

- **The app** — a coffee-shop loyalty app shows each customer one personalized coupon when they open it
- **Batch path** — a 3 AM job scores all 60,000 customers, writes a results table; the app just looks up
- **Real-time path** — nothing is precomputed; when a customer opens the app, a model server scores them
- **Same model** — both paths run the identical model; what differs is *when* the work happens
- **The trade** — batch does the work before the question, real-time does it at the moment of the question

*Example (italic):* Maria opens the app at 8:15 AM — batch hands her a coupon computed at 3 AM; real-time computes one from what she did seconds ago.

**Key point:** Batch scoring answers before anyone asks; real-time inference answers when someone asks — everything else about the trade-off follows from that one difference.

### Visualization (canvas `c1`, 720×300)

Two-lane flow diagram: the batch pipeline on the top lane (overnight job → results table → instant lookup) and the real-time pipeline on the bottom lane (app open → model server → response), same model box style in both.

- **Title (bold 15px, `#1a5276`, top center):** "Same Model, Two Serving Paths".
- **Lane labels (bold 13px, left-aligned at x=20):** "BATCH" in blue `#2a78d6` at y=75; "REAL-TIME" in orange `#d95926` at y=195.
- **Batch lane (boxes centered on y=95):** three rounded 46px-tall boxes with 2px blue `#2a78d6` borders, fill `rgba(42,120,214,0.10)`, bold 12px `#2c3e50` two-line labels — "3 AM job / scores all 60,000" (x 85–225), "results / table" (x 295–405), "app opens: / lookup, 5 ms" (x 475–665); 2px blue arrows with arrowheads between boxes.
- **Real-time lane (boxes centered on y=215):** three rounded 46px-tall boxes with 2px orange `#d95926` borders, fill `rgba(217,89,38,0.10)` — "app opens: / request" (x 85–225), "model server / scores 1 customer" (x 295–405), "coupon back / in 80 ms" (x 475–665); 2px orange arrows between boxes.
- **Clock marks (11px `#6b7280`, above each lane's first box):** "work done at 3 AM" over the batch lane, "work done at 8:15 AM" over the real-time lane.
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=285):** "batch: work before the question — real-time: work at the question".
- **Caption (12px `#444`, bottom right):** "illustrative — one coffee-shop app, two pipelines".

## Counting the Dollars: 60,000 Scores, 6,000 Opens

**Tags:** `worked example` (blue), `cost math` (green)

- **Batch bill** — one nightly job scores all 60,000 customers on rented machines: $6 per night
- **Real-time bill** — a model server must stay on all day waiting for requests: $24 per day
- **Wasted work** — only 6,000 customers open the app next day, so 54,000 batch scores go unused
- **Per used score** — batch: $6 / 6,000 opens = $1 per 1,000 used; real-time: $24 / 6,000 = $4 per 1,000
- **Still cheaper** — batch throws away 9 of every 10 scores and still costs a quarter as much per use
- **Why** — one big overnight run packs the hardware full; an always-on server mostly sits idle

*Example (italic):* $6 buys 60,000 batch scores of which 6,000 get used; $24 buys exactly the 6,000 real-time scores that were asked for — batch wins $1 to $4 per 1,000 used.

**Key point:** Cost per used score: batch $6/6,000 = $1 per 1,000, real-time $24/6,000 = $4 per 1,000 — bulk overnight work beats an idle always-on server even with 90% waste.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart with two groups on the x axis — "total cost per day" and "cost per 1,000 used scores" — each holding a blue batch bar and an orange real-time bar on a shared dollar axis.

- **Title (bold 15px, `#1a5276`, top center):** "Daily Bill and Cost per Used Score (60,000 customers, 6,000 opens)".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y axis dollars 0 to 25 with 12px `#444` tick labels "$0", "$5", "$10", "$15", "$20", "$25" and light `#e5e9ef` gridlines every $5; x axis two group labels (12px `#444`, centered under groups): "total cost per day" (group center x≈220), "cost per 1,000 used scores" (group center x≈510).
- **Bars (each 60px wide, 20px gap within a group):** group 1 — batch blue `#2a78d6` bar at $6, real-time orange `#d95926` bar at $24; group 2 — batch blue bar at $1, real-time orange bar at $4; bar fills at 0.75 alpha with solid 2px same-color top edge.
- **Value labels (bold 13px, same color as bar, centered above each bar):** "$6", "$24", "$1", "$4".
- **Legend (12px, top right, y≈55):** blue swatch "batch", orange swatch "real-time".
- **Annotation (bold 12px `#1a5276`, near x=330, y=100, two lines):** "54,000 of 60,000 batch scores never get used —" / "and batch is still 4× cheaper per use".
- **Caption (12px `#444`, bottom right):** "illustrative prices".

## The Triangle: Cost, Latency, Freshness — Pick Two

**Tags:** `where it's used` (blue), `trade-off triangle` (green), `rule of thumb` (orange)

- **Three wants** — low cost, low latency (answer fast), high freshness (answer built from recent data)
- **The catch** — no serving setup gets all three for free; picking two decides the architecture
- **Batch corner-pair** — cheap and fast to serve ($1 per 1,000, 5 ms lookup) but up to 24 h stale
- **Real-time pair** — fresh and fast (80 ms, uses seconds-old behavior) but 4× the cost per use
- **Fresh-and-cheap pair** — compute on request but reply later (queued jobs); nobody waits online
- **Match the job** — fraud checks need fresh + fast; a weekly email blast is happily batch

*Example (italic):* The coupon app ships batch coupons to everyone but calls the real-time path for one case — a brand-new signup who has no row in last night's table.

**Key point:** Ask "how stale can this prediction be before it's wrong?" — if the answer is hours, batch; if seconds, pay for real-time; if nobody is waiting, queue it.

### Visualization (canvas `c3`, 720×300)

Triangle diagram: the three wants at the corners, each edge labeled with the serving pattern that delivers its two endpoints, and a small dot on each edge for an example workload.

- **Title (bold 15px, `#1a5276`, top center):** "The Serving Triangle: Any Edge Is Cheap, the Third Corner Costs".
- **Triangle:** 3px `#1a5276` lines joining vertices at (360, 70), (150, 250), (570, 250); fill `rgba(26,82,118,0.05)`.
- **Corner labels (bold 13px `#1a5276`):** "FRESHNESS" centered above (360, 70); "LOW LATENCY" below-left of (150, 250); "LOW COST" below-right of (570, 250).
- **Edge labels (bold 12px, placed just outside each edge midpoint):** left edge (freshness–latency) in orange `#d95926`: "real-time inference"; bottom edge (latency–cost) in blue `#2a78d6`: "batch + table lookup"; right edge (freshness–cost) in green `#008300`: "queued / async scoring".
- **Workload dots (7px, matching edge color, 11px `#444` label beside each):** "fraud check" on the left edge midpoint (255, 160); "nightly coupons" on the bottom edge midpoint (360, 250); "report on request" on the right edge midpoint (465, 160).
- **Annotation (bold 12px violet `#4a3aa7`, centered inside the triangle near (360, 200)):** "pick an edge — the far corner is the price".
- **Caption (12px `#444`, bottom right):** "illustrative placement of example workloads".

## The Mix-Up: Batch Isn't the Slow One

**Tags:** `common mistake` (red), `latency vs freshness` (orange)

- **The instinct** — "real-time" sounds fast and "batch" sounds slow, so people assume batch users wait
- **Reality** — the batch answer was finished at 3 AM; serving it is a 5 ms table lookup
- **Real-time is slower to serve** — running the model per request takes 80 ms, sixteen times longer
- **The real gap** — batch answers are old (up to 24 h since scoring); real-time answers are seconds old
- **Name it right** — batch vs real-time is a *freshness* choice; both can feel instant to the user

*Example (italic):* Maria's batch coupon appears in 5 ms but ignores the espresso she bought an hour ago; a real-time coupon takes 80 ms and knows about it.

**Common mistake:** Rejecting batch because "users can't wait for a batch job". Nobody waits for it — it already ran. The question to ask is whether a prediction up to 24 hours old is still the right prediction.

### Visualization (canvas `c4`, 720×300)

Two-dot scatter on a staleness-vs-serving-latency plane: one blue dot for batch (old but served fast) and one orange dot for real-time (fresh but served slower), showing the two systems sit in opposite corners.

- **Title (bold 15px, `#1a5276`, top center):** "The Real Trade Is Age, Not Speed".
- **Axes:** origin x=80, baseline y=240, plot width 570, plot height 170; x axis = age of the prediction when served, 0 to 24 h, 12px `#444` tick labels "0 h", "6 h", "12 h", "18 h", "24 h"; y axis = time to serve one request, 0 to 100 ms, 12px `#444` tick labels "0", "25", "50", "75", "100 ms"; light `#e5e9ef` gridlines at each tick.
- **Real-time dot:** 9px orange `#d95926` dot at (age 0 h, 80 ms), plotted just right of the y axis; bold 12px orange two-line label to its right: "real-time: 80 ms to serve," / "seconds old".
- **Batch dot:** 9px blue `#2a78d6` dot at (age 12 h, 5 ms); bold 12px blue two-line label above it: "batch lookup: 5 ms to serve," / "up to 24 h old".
- **Age whisker:** horizontal dashed blue (dash 4/3) line through the batch dot from age 0 to 24 h at y for 5 ms, small vertical end ticks; 11px `#6b7280` label under its right end: "score ages until the next 3 AM run".
- **Annotation (bold 13px violet `#4a3aa7`, centered near (400, 90)):** "batch answers arrive faster — they're just older".
- **Caption (12px `#444`, bottom right):** "illustrative — coffee-shop coupon app numbers".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all numbers are the hardcoded literals above (no randomness): 60,000 customers, 6,000 daily opens, $6/night batch, $24/day real-time, $1 vs $4 per 1,000 used scores, 5 ms lookup vs 80 ms model call, staleness 0–24 h; text and chart values must stay identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
