# Lambda vs Kappa Architecture

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Lambda vs Kappa Architecture

**Subtitle:** Two ways to serve fresh numbers from a stream of events — run batch and stream pipelines side by side (lambda), or make one stream job do everything and replay it when needed (kappa)

## One Dashboard, Two Ways to Feed It

**Tags:** `core idea` (blue), `batch + stream` (green), `data pipelines` (orange)

- **The shop** — a 12-store coffee chain wants a live "orders today" dashboard plus exact daily totals
- **Two speeds** — the dashboard needs answers in seconds; the official totals can wait until night
- **Lambda** — run two pipelines: a nightly batch job for truth, a stream job for the live approximation
- **Kappa** — run one stream job over an append-only order log; recompute by replaying that same log
- **The choice** — lambda buys speed-plus-accuracy with two codebases; kappa keeps a single codebase

*Example (italic):* Every order — latte, 9:02am, store #7 — lands exactly once in the event log; the only question is how many programs read it.

**Key point:** Lambda answers "fast or correct?" with "both, via two parallel layers merged at serving time"; kappa answers it with one stream job whose replay IS the batch.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the lambda architecture (events fan out to a batch layer and a speed layer, merged by a serving layer) above the kappa architecture (events into a log, one stream job, with a dashed replay arrow back to the log's start).

- **Title (bold 15px, `#1a5276`, top center):** "Lambda: Two Parallel Pipelines — Kappa: One Job Plus Replay".
- **Row 1 (lambda, centered on y=90), label bold 12px `#2c3e50` "LAMBDA" at x=20:** blue `#2a78d6` rounded box at x=90 labeled "order events" (12px); 2px `#6b7280` arrows fanning to two boxes at x=270 — blue box at y=55 "batch layer — nightly, exact" and aqua `#199e70` box at y=115 "speed layer — live, approx"; both arrow into a violet `#4a3aa7` box at x=520, y=90 "serving layer merges".
- **Row 2 (kappa, centered on y=215), label bold 12px `#2c3e50` "KAPPA" at x=20:** blue box at x=90 "order events" → green `#008300` box at x=270 "append-only log" → aqua box at x=440 "one stream job" → violet box at x=600 "serving table"; dashed `#6b7280` (dash 5/4) curved arrow from the stream job back to the log labeled 12px `#6b7280` "replay to recompute".
- **Box style:** 110–150px wide, 38px tall, 8px radius, fills at 0.15 alpha of the border color, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "lambda: two codebases to keep in sync — kappa: one".
- **Caption (12px `#444`, bottom right):** "flow schematic — no measured numbers".

## Counting Tuesday's 4,200 Orders Twice

**Tags:** `worked example` (blue), `late events` (orange)

- **The live count** — the speed layer adds 1 per order event; by midnight it shows 4,180 orders
- **Late arrivals** — 20 mobile-app orders sync hours late, after the phones reconnect to wifi
- **The batch pass** — the nightly job rereads the full orders table and counts 4,200
- **Hand-check** — 4,200 batch minus 4,180 live = 20 late orders the speed layer never saw in time
- **The kappa way** — replay the log through the same stream job next morning: it also lands on 4,200

*Example (italic):* The 11pm dashboard says 4,180 and Wednesday's report says 4,200 — both are "right" for the moment they were computed.

**Key point:** The speed layer quietly undercounts whenever data arrives late; the correction comes from a batch recompute (lambda) or a log replay (kappa) — the same 4,200 either way.

### Visualization (canvas `c2`, 720×300)

Cumulative line chart of Tuesday's live order count climbing to 4,180, with a green marker at 4,200 showing the overnight recompute finding the 20 late orders.

- **Title (bold 15px, `#1a5276`, top center):** "Live Count Ends at 4,180 — the Recompute Finds 4,200".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hour of day 0 to 24 with 12px `#444` tick labels "0h"–"24h" every 4 hours; y = cumulative orders 0 to 4,400, gridlines `#e5e9ef` at 1,100 / 2,200 / 3,300.
- **Speed-layer line:** blue `#2a78d6` 3px line through hours `[0, 4, 8, 12, 16, 20, 24]`, cumulative orders `[0, 310, 1240, 2480, 3390, 3980, 4180]`, with a blue dot and 12px blue label "live: 4,180" at the final point.
- **Recompute marker:** green `#008300` filled dot at (hour 24, 4,200) with a dashed green (dash 4/3) horizontal guide line from x=60 to the dot at y(4,200), 12px green label "batch / replay: 4,200" above the guide near the right edge.
- **Annotation (bold 13px green `#008300`, near hour 14, y=70):** "+20 late orders found overnight".
- **Caption (12px `#444`, bottom right):** "order counts illustrative".

## Where the Two Codebases Bite a Data Scientist

**Tags:** `where it's used` (blue), `training/serving skew` (red), `feature stores` (green)

- **Feature stores** — "orders in the last hour" is computed in batch for training, in stream for serving
- **Lambda drift** — the same window logic written twice (SQL and stream code) slowly disagrees
- **Skew** — the model trains on the batch value but scores with the stream value at request time
- **Kappa fix** — one job computes the feature once: live output now, replayed output for training sets
- **Real cost** — every metric with two definitions becomes a meeting about whose number is right

*Example (italic):* At 7pm the batch feature says 320 orders/hour while the live stream feature says 312 — the model trained on one and serves on the other.

**Key point:** Lambda's price is the same logic maintained twice; when the copies drift, models inherit training/serving skew — kappa removes the second copy instead of reconciling it.

### Visualization (canvas `c3`, 720×300)

Paired bar chart of one feature — "orders in the last hour" — computed by the batch pipeline vs the stream pipeline at six checkpoints through the day, showing a small but real disagreement.

- **Title (bold 15px, `#1a5276`, top center):** "One Feature, Two Pipelines: 'Orders in the Last Hour'".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = six checkpoint groups labeled "9am 11am 1pm 3pm 5pm 7pm" (12px `#444`, centered under each pair); y = orders/hour 0 to 350, gridlines `#e5e9ef` at 100 / 200 / 300.
- **Batch bars:** blue `#2a78d6` fill `rgba(42,120,214,0.65)`, 28px wide, values `[140, 210, 260, 235, 300, 320]`, 11px blue value labels on top.
- **Stream bars:** aqua `#199e70` fill `rgba(25,158,112,0.65)`, 28px wide, drawn 32px right of each batch bar, values `[140, 208, 255, 236, 297, 312]`, 11px aqua value labels on top.
- **Legend (12px, top left inside plot):** blue swatch "batch (training)", aqua swatch "stream (serving)".
- **Annotation (bold 13px magenta `#d55181`, near the 7pm pair, y=80):** "same feature, two answers — skew up to 8".
- **Caption (12px `#444`, bottom right):** "feature values illustrative".

## Kappa Doesn't Mean the History Is Gone

**Tags:** `common mistake` (red), `replay` (orange)

- **The fear** — "stream-only" sounds like numbers vanish once processed and old bugs stay forever
- **The log** — kappa keeps the raw event log for weeks; the stream job's output table is disposable
- **The fix path** — fix the code, start a second job at offset 0, and let it rebuild the whole table
- **The swap** — when the rebuilt table catches up, point the dashboard at it and drop the old one
- **The real limit** — replay only reaches as far back as the log's retention window allows

*Example (italic):* A tax-rounding bug ships on day 1 and is found on day 30; replaying 126,000 logged orders through the fixed job rebuilds every daily total in about 20 minutes.

**Common mistake:** Treating kappa as lambda-minus-correctness. The batch layer isn't deleted — it's replaced by replaying the retained log through the one stream codebase.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the misconception (a found bug leaves history wrong forever) vs the kappa reality (replay the retained log through the fixed job, then swap tables).

- **Title (bold 15px, `#1a5276`, top center):** "Fixing 30 Days of Wrong Totals by Replaying the Log".
- **Row 1 (y=95), label 12px `#444` "the fear" at x=20:** blue `#2a78d6` rounded box at x=150 labeled "bug found on day 30" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "old totals wrong forever" with bold 12px red "✗ not true" to its right.
- **Row 2 (y=205), label 12px `#444` "the reality" at x=20:** blue box at x=130 "fix the stream job", 3px arrow to a green `#008300` box at x=330 "replay 126,000 events from offset 0", arrow to a green box at x=560 "new table live in ~20 min" with bold 12px green "✓ swap".
- **Box style:** 130–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "the log is the backup; replay is the batch".
- **Caption (12px `#444`, bottom right):** "event counts and replay time illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the order counts (live 4,180 / recompute 4,200 / 20 late), the cumulative curve `[0, 310, 1240, 2480, 3390, 3980, 4180]`, the batch-vs-stream feature values `[140, 210, 260, 235, 300, 320]` / `[140, 208, 255, 236, 297, 312]`, and the 126,000-event, ~20-minute replay are all invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
