# Latency vs Throughput

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Latency vs Throughput

**Subtitle:** Latency is how long ONE order takes; throughput is how MANY orders finish per hour — a coffee shop can improve one without touching the other

## One Cup vs Cups per Hour

**Tags:** `core idea` (blue), `two axes` (green), `coffee shop` (orange)

- **The shop** — one barista makes every drink herself, start to finish, no helpers
- **One order** — your latte takes 2 minutes from "one latte please" to cup in hand
- **The hour** — working nonstop at 2 minutes a cup, she finishes 30 cups by the top of the hour
- **Latency** — the 2 minutes is latency: the time one single order spends in the system
- **Throughput** — the 30 cups/hour is throughput: how many orders complete per unit time
- **Different questions** — "how long is my wait?" and "how much can the shop sell?" are separate axes

*Example (italic):* You order at 9:00:00 and hold your latte at 9:02:00 — 2 minutes of latency; by 10:00 the counter shows 30 cups sold — 30/hour of throughput.

**Key point:** Latency measures the journey of one item; throughput measures the flow of all items. Knowing one tells you surprisingly little about the other.

### Visualization (canvas `c1`, 720×300)

Cumulative-cups line over one hour: the slope of the line IS the throughput, and the height of one step IS the latency of a single cup.

- **Title (bold 15px, `#1a5276`, top center):** "One Barista, One Hour: the Slope Is Throughput, One Step Is Latency".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 60 with 12px `#444` tick labels every 10 min; y = cups completed 0 to 30, gridlines `#e5e9ef` at 10/20/30.
- **Cumulative line:** blue `#2a78d6` 3px line through minutes `[0, 10, 20, 30, 40, 50, 60]`, cups `[0, 5, 10, 15, 20, 25, 30]` — a straight ramp.
- **Latency bracket:** aqua `#199e70` 2px horizontal bracket under the first segment from minute 0 to minute 2 at y just above baseline, bold 12px `#199e70` label "1 cup = 2 min (latency)".
- **Slope callout:** dashed `#6b7280` (dash 4/3) right-triangle on the line near minute 40 (run 10 min, rise 5 cups), 12px `#6b7280` label "5 cups / 10 min".
- **Annotation (bold 13px blue `#2a78d6`, near minute 15, y=75):** "slope = 30 cups/hour (throughput)".
- **Caption (12px `#444`, bottom right):** "cup times illustrative".

## Second Barista or Batch Brewer: Same 60 Cups, Very Different Waits

**Tags:** `worked example` (blue), `hand-check` (green)

- **Option A** — hire a second barista: two people each make a cup in 2 minutes, side by side
- **A's numbers** — latency stays 2 min per cup; throughput doubles to 2 × 30 = 60 cups/hour
- **Option B** — buy a batch brewer: it makes 12 cups at once, but one batch takes 12 minutes
- **B's numbers** — 60 / 12 = 5 batches an hour × 12 cups = 60 cups/hour; the first cup waits 12 min
- **The comparison** — both options hit 60 cups/hour, yet B's latency is 12 min vs A's 2 min: 6× worse
- **Hand-check** — redo it: A = 2 baristas × 30; B = 5 batches × 12; both 60, only the wait differs

*Example (italic):* At 9:00 both shops take your order; shop A hands you a cup at 9:02, shop B at 9:12 — and by 10:00 each has sold exactly 60 cups.

**Key point:** Two systems with identical throughput (60 cups/hour) can have wildly different latency (2 min vs 12 min) — you must measure both axes to compare them.

### Visualization (canvas `c2`, 720×300)

Two side-by-side bar panels for the three setups (1 barista, 2 baristas, batch brewer): left panel latency in minutes, right panel throughput in cups/hour.

- **Title (bold 15px, `#1a5276`, top center):** "Same Throughput, 6× the Wait: Two Baristas vs the Batch Brewer".
- **Left panel (latency):** plot area x=60 to x=340, baseline y=245, plot height 170; y = minutes 0 to 12, gridlines `#e5e9ef` at 4/8/12; 13px `#444` panel label "latency per cup (min)" at top; three 60px-wide bars at values `[2, 2, 12]` — "1 barista" blue `#2a78d6`, "2 baristas" green `#008300`, "batch brewer" orange `#d95926`; bold 12px value labels "2", "2", "12" above bars; 12px `#444` category labels below baseline.
- **Right panel (throughput):** plot area x=410 to x=690, same baseline and height; y = cups/hour 0 to 60, gridlines at 20/40/60; panel label "throughput (cups/hr)"; same three categories and colors with bar values `[30, 60, 60]` and bold 12px value labels "30", "60", "60".
- **Annotation (bold 13px magenta `#d55181`, centered between panels near y=60):** "B matches A's 60 cups/hr — at 6× the latency".
- **Caption (12px `#444`, bottom right):** "minutes and cup counts illustrative".

## Where an Engineer Meets Both Axes

**Tags:** `where it's used` (blue), `batching` (green), `serving models` (orange)

- **APIs** — a user feels latency (ms per request); capacity planning buys throughput (requests/sec)
- **Model serving** — GPUs love batches: grouping requests raises throughput but each request waits
- **The numbers** — batch 1: 50 preds/sec at 20 ms; batch 32: 480/sec at 95 ms including queue wait
- **The trade** — going 1 → 32 multiplies throughput by 9.6 while multiplying latency by about 4.8
- **Pipelines** — a nightly batch job has huge throughput and hours of latency; streaming flips that
- **Pick by need** — a fraud check needs low latency; a weekly report only needs enough throughput

*Example (italic):* An ML team raises the serving batch size from 1 to 32, cuts the GPU bill, and then fields complaints that every single prediction got 75 ms slower.

**Key point:** Batching is the classic trade: it buys throughput by making individual items wait — a good deal for offline scoring, a bad one for a click-time fraud check.

### Visualization (canvas `c3`, 720×300)

Dual-axis line chart over batch size: throughput (left axis, blue, rising fast) and per-request latency (right axis, orange, rising too) — both go up together.

- **Title (bold 15px, `#1a5276`, top center):** "Bigger Batches: More Predictions per Second, Longer Wait per Prediction".
- **Axes:** origin x=65, baseline y=245, plot width 580, plot height 180; x = batch size with tick labels `[1, 4, 8, 16, 32]` (12px `#444`, evenly spaced categories); left y = predictions/sec 0 to 500, gridlines `#e5e9ef` at 125/250/375/500, 12px blue `#2a78d6` axis label "preds/sec"; right y (labels at x=660) = latency ms 0 to 100, 12px orange `#d95926` axis label "ms".
- **Throughput line:** blue `#2a78d6` 3px line with 4px dots through batch sizes `[1, 4, 8, 16, 32]`, predictions/sec `[50, 160, 260, 380, 480]`.
- **Latency line:** orange `#d95926` 3px line with 4px dots through the same batch sizes, latency ms `[20, 30, 40, 60, 95]` (plotted on the right-axis scale).
- **Point labels:** bold 11px at the endpoints only — blue "480/s" near the last throughput point, orange "95 ms" near the last latency point.
- **Annotation (bold 13px violet `#4a3aa7`, near batch 8, y=70):** "throughput ×9.6, latency ×4.8 — you pay for capacity in wait".
- **Caption (12px `#444`, bottom right):** "serving numbers illustrative".

## More Baristas Never Brew Your Cup Faster

**Tags:** `common mistake` (red), `scaling` (orange)

- **The confusion** — people say "make it faster" for both axes, then fix the wrong one
- **The test** — hiring baristas 1 → 4 lifts throughput 30 → 60 → 90 → 120 cups/hour, in a straight line
- **The flat line** — your own latte still takes 2 minutes with 4 baristas; latency never moves
- **Same trap** — adding web servers raises requests/sec but leaves one slow query exactly as slow
- **Bandwidth too** — a 10× fatter internet pipe moves more data; it does not cut the round-trip ping
- **Latency fixes** — only a shorter path, less work per item, or faster work per item cut the wait

*Example (italic):* The shop hires a fourth barista and hits 120 cups/hour, yet a reviewer still writes "waited 2 whole minutes for one latte" — because that number never changed.

**Common mistake:** Throwing parallelism at a latency complaint. Adding workers scales throughput; it cannot shorten the path one single item travels — that takes making the work itself faster.

### Visualization (canvas `c4`, 720×300)

Two lines over number of baristas 1–4: throughput climbing linearly, latency dead flat — the visual signature of the confusion.

- **Title (bold 15px, `#1a5276`, top center):** "Hiring Baristas: Throughput Climbs, Your Wait Never Moves".
- **Axes:** origin x=65, baseline y=245, plot width 580, plot height 180; x = number of baristas, tick labels `[1, 2, 3, 4]` evenly spaced (12px `#444`); left y = cups/hour 0 to 120, gridlines `#e5e9ef` at 30/60/90/120, 12px green `#008300` axis label "cups/hr"; right y (labels at x=660) = minutes 0 to 4, 12px orange `#d95926` axis label "min".
- **Throughput line:** green `#008300` 3px line with 4px dots through baristas `[1, 2, 3, 4]`, cups/hour `[30, 60, 90, 120]`, bold 12px green label "throughput" above its right end.
- **Latency line:** orange `#d95926` 3px flat line through the same x positions, minutes `[2, 2, 2, 2]` (right-axis scale), bold 12px orange label "your wait: 2 min, always" above its right end.
- **Annotation (bold 13px orange `#d95926`, near baristas=2, y=170):** "no number of baristas makes one cup brew faster".
- **Caption (12px `#444`, bottom right):** "cup times illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); coffee-shop numbers (2 min/cup, 30 cups/hr, latency `[2, 2, 12]` min, throughput `[30, 60, 60]` and `[30, 60, 90, 120]` cups/hr) and model-serving numbers (throughput `[50, 160, 260, 380, 480]` preds/sec, latency `[20, 30, 40, 60, 95]` ms over batch sizes `[1, 4, 8, 16, 32]`) are invented and labeled illustrative; the arithmetic (2 × 30 = 60, 5 batches × 12 = 60, ×9.6 and ×4.8 ratios) is exact for those numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
