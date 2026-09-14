# Tail Latency

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Tail Latency

**Subtitle:** The 99th percentile matters more than the average — when a page fans out to 100 backend calls, most users meet the slowest 1%

## One Service, Two Very Different Stories

**Tags:** `core idea` (blue), `percentiles` (green), `latency` (orange)

- **The page** — a product page on a shopping site fans out to 100 backend services in parallel
- **One backend** — the reviews service answers 1,000 requests; most take about 20ms
- **The average** — mean latency is ~65ms, and the dashboard proudly reports "65ms, healthy"
- **The tail** — sort the 1,000 requests: the 500th is 20ms (p50), the 950th is 250ms (p95), the 990th is 1s (p99)
- **The definition** — pX is the value X% of requests beat; the slowest few define the "tail"

*Example (italic):* The reviews service averages 65ms, yet 10 of its 1,000 requests take a full second — the average never mentions them.

**Key point:** The average is dragged toward the bulk of fast requests; percentiles (p50, p95, p99) read the sorted list directly and are the only honest way to describe the slow end.

### Visualization (canvas `c1`, 720×300)

Histogram of 1,000 requests to the reviews service by latency bucket, with the average and the p50/p95/p99 markers showing how far apart they sit.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Requests to the Reviews Service: the Average Hides the Tail".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = six latency buckets with 12px `#444` labels "0–25ms", "25–50", "50–100", "100–300", "300–1000", "≥1000ms"; y = request count 0 to 600, gridlines `#e5e9ef` at 150/300/450.
- **Bars:** fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, counts `[520, 280, 120, 50, 20, 10]`; last two bars filled `rgba(217,89,38,0.30)` with 2px `#d95926` edge (the tail); 12px `#444` count labels on top of each bar.
- **Percentile markers:** vertical dashed `#6b7280` (dash 4/3) lines at the p50, p95, p99 bucket positions with bold 12px labels "p50 = 20ms" (`#008300`), "p95 = 250ms" (`#c98500`), "p99 = 1s" (`#e74c3c`) staggered near the top.
- **Average marker:** vertical solid 2px `#4a3aa7` line inside the second bucket, bold 12px violet label "average ≈ 65ms".
- **Annotation (bold 13px `#d95926`, above the tail bars):** "30 requests out of 1,000 live out here".
- **Caption (12px `#444`, bottom right):** "latency histogram illustrative; percentiles read off it exactly".

## Fan Out to 100 Calls and the Tail Finds You

**Tags:** `worked example` (blue), `fan-out` (green), `exact math` (orange)

- **The fan-out** — the product page waits for all 100 parallel backend calls before it can render
- **One call** — each backend is fine 99% of the time; only 1 request in 100 hits its 1-second p99
- **The multiply** — the page avoids every slow call only if all 100 are fast: 0.99 × 0.99 × ... = 0.99^100
- **The number** — 0.99^100 ≈ 0.366, so only ~37% of page loads dodge the tail entirely
- **The flip** — ~63% of page loads hit at least one 1-second call, and the page waits for the slowest
- **Hand-check** — at 10 calls it is 0.99^10 ≈ 0.904; at 50 calls, 0.99^50 ≈ 0.605; the decay is relentless

*Example (italic):* A backend event that is rare for any one service (1 in 100) becomes the majority experience (63 of 100 page loads) once the page depends on 100 of them.

**Key point:** Fan-out amplifies the tail: with N parallel calls the chance of a clean page load is 0.99^N, so a 1%-rare slow response dominates user experience long before N reaches 100. This is exact arithmetic, not an estimate.

### Visualization (canvas `c2`, 720×300)

Decay curve of P(no call hits its p99) = 0.99^N as the fan-out N grows from 1 to 100, with the 50% crossover and the N=100 endpoint called out.

- **Title (bold 15px, `#1a5276`, top center):** "Chance the Page Avoids Every Slow Call: 0.99^N".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = number of parallel calls N, 12px `#444` tick labels at 1/20/40/60/80/100; y = probability 0 to 1, gridlines `#e5e9ef` at 0.25/0.50/0.75 with 12px labels "25%", "50%", "75%".
- **Curve:** blue `#2a78d6` 3px line through points N `[1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]`, probability `[0.990, 0.951, 0.904, 0.818, 0.740, 0.669, 0.605, 0.547, 0.495, 0.448, 0.405, 0.366]`.
- **Crossover marker:** vertical dashed `#6b7280` (dash 4/3) line at N=69 down to the curve, 12px `#6b7280` label "N=69: coin flip".
- **Endpoint dot:** 5px radius red `#e74c3c` dot at (100, 0.366), bold 13px red annotation to its upper left: "100 calls: only 37% of pages stay fast — 63% hit the tail".
- **Caption (12px `#444`, bottom right):** "0.99^N is exact — no simulation, no estimate".

## Hedged Requests: Racing the Slow Replica

**Tags:** `where it's used` (blue), `mitigation` (green)

- **The idea** — don't wait out a straggler: if a call hasn't answered by its p95 (250ms), send a duplicate to another replica
- **The race** — take whichever copy answers first and cancel the loser
- **The cost** — only the slowest 5% of calls ever spawn a hedge, so total load rises by at most ~5%
- **The payoff** — a request now hits the tail only if both replicas are slow at once, which is far rarer
- **The record** — the technique is described in Dean & Barroso's "The Tail at Scale" (CACM, 2013)

*Example (italic):* A reviews call stuck at 250ms fires a hedge; the second replica answers in 30ms, and the user sees ~280ms instead of a full second.

**Key point:** Hedged requests trade a small, bounded amount of extra load (~5%) for a collapsed tail — the p99 stops being one server's bad second and becomes the minimum of two draws.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart of the reviews-service percentiles for the product page, without vs with hedging, showing the tail collapsing while p50 stays put.

- **Title (bold 15px, `#1a5276`, top center):** "Hedge After 250ms: p99 Falls from 1s to ~280ms".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = latency in ms 0 to 1000, gridlines `#e5e9ef` at 250/500/750 with 12px `#444` labels; x = three groups labeled "p50", "p95", "p99" (12px `#444`, centered under each group).
- **Bars per group (24px wide, 12px gap):** "no hedge" fill `rgba(42,120,214,0.30)` edge 2px `#2a78d6`, heights for `[20, 250, 1000]` ms; "hedged" fill `rgba(0,131,0,0.30)` edge 2px `#008300`, heights for `[20, 240, 280]` ms; 12px `#444` value labels ("20ms", "250ms", "1s", "20ms", "240ms", "280ms") on top of each bar.
- **Legend (top left, 12px):** blue swatch "no hedge", green swatch "hedged (fire duplicate at 250ms)".
- **Annotation (bold 13px green `#008300`, beside the p99 group):** "tail cut ~3.5×, for ~5% extra requests".
- **Caption (12px `#444`, bottom right):** "hedged percentiles illustrative; hedge trigger = the p95 (250ms)".

## Never Average Percentiles Across Servers

**Tags:** `common mistake` (red), `aggregation` (orange)

- **The setup** — the reviews service runs on two servers: A handles 900 requests, B handles 100
- **Each reports** — server A's p99 is 200ms; server B is degraded and its p99 is 1,200ms
- **The shortcut** — the dashboard averages the two: (200 + 1200) / 2 = 700ms "fleet p99"
- **The truth** — merge the raw 1,000 requests and re-sort: nearly all of B's requests sit in the slowest 100, so the true fleet p99 lands around 1,100ms
- **The rule** — a percentile is a position in a sorted list; positions from different lists cannot be averaged
- **The fix** — aggregate histograms (or raw samples) per server, merge them, then compute the percentile once

*Example (italic):* The averaged "700ms p99" understates the real ~1,100ms fleet p99 by a third — and hides that one server is the entire problem.

**Common mistake:** Averaging p99s (or any percentiles) across servers, shards, or time windows. The average of the percentiles is not the percentile of the merged data — ship histograms, not pre-computed percentiles.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart contrasting each server's own p99, the wrong averaged number, and the true merged fleet p99.

- **Title (bold 15px, `#1a5276`, top center):** "Fleet p99: Average of Percentiles vs Percentile of Merged Data".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 440 representing 1,200ms; light 1px `#e5e9ef` vertical gridlines at 300/600/900ms with 11px `#6b7280` labels along the bottom.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "server A p99 (900 req)": blue `#2a78d6` bar, width for 200ms (≈73px), 11px label "200ms" at bar end
  - "server B p99 (100 req)": orange `#d95926` bar, width for 1,200ms (440px), label "1,200ms"
  - "average of the two p99s": red `#e74c3c` bar, width for 700ms (≈257px), bold 12px red label "700ms ✗ wrong"
  - "true merged p99": green `#008300` bar, width for 1,100ms (≈403px), bold 12px green label "~1,100ms ✓"
- **Bar style:** 16px tall, fills at 0.30 alpha with solid 2px edges in the row color.
- **Annotation (bold 13px magenta `#d55181`, bottom center near y=270):** "percentiles are positions in a sorted list — merge the data, then take the position".
- **Caption (12px `#444`, bottom right):** "server latencies illustrative; the averaging error is structural, not a fluke of these numbers".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the c2 fan-out probabilities are exact values of 0.99^N and must stay labeled exact; the c1 histogram, c3 hedged percentiles, and c4 per-server latencies are invented and labeled illustrative; text numbers (20ms / 250ms / 1s / 65ms / 37% / 63% / 700ms / ~1,100ms) must match the chart numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
