# North Star & Guardrail Metrics

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** North Star & Guardrail Metrics

**Subtitle:** Pick one metric that captures delivered value and push it hard — while a short list of guardrail metrics makes sure the push never hurts the user

## One Metric to Grow, Four Metrics to Guard

**Tags:** `core idea` (blue), `north star` (green), `guardrails` (orange)

- **The product** — a music-streaming app where every team wants to move a different number
- **The north star** — one metric everyone optimizes: weekly hours listened per user
- **Why that one** — it captures value the user actually received, and it leads next quarter's revenue
- **The guardrails** — skip rate, subscription cancellations, app crashes, support contacts
- **The deal** — any change may push the north star only if no guardrail gets worse

*Example (italic):* A feature that adds listening hours but triples skips is not a win — the guardrail vetoes it before launch.

**Key point:** A north star is the one metric to maximize; guardrails are the metrics you commit in advance not to harm while maximizing it.

### Visualization (canvas `c1`, 720×300)

Box diagram: the single north star metric feeding revenue on top, a row of four guardrail metrics standing beneath it with "must not worsen" duties.

- **Title (bold 15px, `#1a5276`, top center):** "One Metric to Push, Four Metrics That Must Not Break".
- **North star box:** green `#008300` rounded box at x=40, y=65, width 300, height 52, fill `rgba(0,131,0,0.12)`, bold 13px `#008300` text "NORTH STAR: weekly hours listened" with 11px `#444` subtext "maximize this".
- **Revenue box:** blue `#2a78d6` rounded box at x=480, y=65, width 200, height 52, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text "revenue, next quarter"; 3px `#008300` arrow from the north star box pointing to it, 11px `#6b7280` label "leads" above the arrow.
- **Guardrail row (y=185, each box width 155, height 56, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border):** boxes at x = 40, 213, 386, 559 labeled "skip rate", "cancellations", "app crashes", "support contacts" (12px `#2c3e50`), each with 11px `#d95926` subtext "must not worsen".
- **Bracket:** 2px `#6b7280` horizontal line at y=165 spanning x=40 to x=714, bold 12px `#6b7280` label "GUARDRAILS — do no harm" centered above it.
- **Annotation (bold 13px orange `#d95926`, bottom center near y=275):** "push one number, protect the other four".

## The Autoplay Test That Guardrails Caught

**Tags:** `worked example` (blue), `A/B test` (green)

- **The change** — an aggressive autoplay queues a new track the instant one ends, no confirmation
- **The headline** — weekly hours listened rise 8%: 5.0 → 5.4 hours per user in the test arm
- **The catch** — skip rate jumps 30%: 12% → 15.6% of tracks skipped, +3.6 points
- **The slow bleed** — monthly cancellations tick up 15%: 2.0% → 2.3%; support contacts rise 5%
- **Hand-check** — 15.6 / 12 = 1.30, so the +30% skip jump is exact from the two rates

*Example (italic):* The dashboard headline says "+8% hours listened"; two guardrails in the red say users are being force-fed music they skip and then quit over.

**Key point:** The north star measured hours delivered, not hours wanted — the guardrails caught value destruction the headline metric hid.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of percent change per metric in the autoplay test arm: the north star up 8%, guardrails up 30 / 15 / 0 / 5 percent.

- **Title (bold 15px, `#1a5276`, top center):** "Autoplay Test: +8% Hours Listened, but Two Guardrails in the Red".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = percent change 0 to 30, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels "+10%" "+20%" "+30%".
- **Bars (width 70, centered at x = 130, 240, 350, 460, 570; height = value/30 × 180):** hours listened +8 (green `#008300`, 48px), skip rate +30 (red `#e74c3c`, 180px), cancellations +15 (red `#e74c3c`, 90px), app crashes 0 (2px mute `#6b7280` stub), support contacts +5 (orange `#d95926`, 30px).
- **Labels:** 12px `#444` metric names under each bar at y=265; bold 12px value labels ("+8%", "+30%", "+15%", "0%", "+5%") in each bar's color just above its top.
- **Annotation (bold 13px red `#e74c3c`, near x=300, y=55):** "guardrails veto the launch".
- **Caption (12px `#444`, bottom right):** "all changes illustrative; skip math exact (12% → 15.6%)".

## Why One Number, and Why It Must Lead Revenue

**Tags:** `where it's used` (blue), `leading indicator` (green)

- **Focus** — one shared metric stops ten teams from optimizing ten conflicting numbers
- **User-first** — a good north star counts value delivered to users, not value extracted from them
- **The lead** — hours hit index 121 in month 5; revenue reaches 121 two months later, in month 7
- **Public examples** — Facebook tracked DAU/MAU-style engagement; Airbnb tracked nights booked
- **The test** — if the metric can rise while users get less value, it is not a north star

*Example (italic):* Nights booked works for Airbnb because a booked night is value for guest, host, and company at once — the metric and the mission point the same way.

**Key point:** A north star earns its job by leading revenue and reflecting user benefit; a metric that does neither is just a vanity number with a title.

### Visualization (canvas `c3`, 720×300)

Two-line chart over 12 months: hours listened (index) moving first, revenue (index) tracing the same path about two months behind.

- **Title (bold 15px, `#1a5276`, top center):** "Hours Listened Moves ~2 Months Before Revenue".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 11 with 12px `#444` tick labels every 2 months; y = index 100 to 150, gridlines `#e5e9ef` at 110/120/130/140.
- **Hours line:** green `#008300` 3px line through months 0–11, index `[100, 103, 107, 112, 116, 121, 125, 130, 134, 139, 143, 148]`.
- **Revenue line:** blue `#2a78d6` 3px line through the same months, index `[100, 100, 101, 103, 107, 112, 116, 121, 125, 130, 134, 139]`.
- **Labels:** bold 12px green "hours listened" near month 3 above the green line; bold 12px blue "revenue" near month 8 below the blue line.
- **Lead marker:** horizontal dashed `#6b7280` (dash 4/3) segment at index 121 connecting month 5 (green) to month 7 (blue), bold 12px violet `#4a3aa7` label "same level, 2 months apart" above its left end.
- **Caption (12px `#444`, bottom right):** "index series illustrative".

## Optimizing a Single Unguarded Metric

**Tags:** `common mistake` (red), `Goodhart's law` (orange)

- **The trap** — a metric with no guardrails invites features that game it instead of serving users
- **The slide** — a year of autoplay-style ships: each adds a little listening and a lot of skipping
- **The peak** — hours listened crest at index 117 in month 5 while cancellations quietly climb
- **The fall** — by month 9 hours drop below the starting level; the gamed gains were borrowed
- **The bill** — monthly cancellations more than triple over the year, from 2.0% to 6.3%

*Example (italic):* Every single launch that year "moved the north star" — and the product ended the year with fewer hours and triple the churn.

**Common mistake:** Treating the north star as the whole scoreboard. One number optimized without guardrails becomes the target, stops measuring value, and gets driven straight into user harm.

### Visualization (canvas `c4`, 720×300)

Dual-axis line chart of the unguarded year: hours listened (left axis) rising then collapsing, cancellations (right axis) climbing the entire time.

- **Title (bold 15px, `#1a5276`, top center):** "A Year Without Guardrails: the North Star Peaks, Churn Never Stops".
- **Axes:** origin x=60, baseline y=245, plot width 590, plot height 180; x = months 0 to 11 with 12px `#444` tick labels every 2 months; left y = hours index 80 to 120, gridlines `#e5e9ef` at 90/100/110; right y (labels at x=670, 12px `#e74c3c`) = cancellations 0% to 7%.
- **Hours line:** green `#008300` 3px line through months 0–11, index `[100, 104, 108, 112, 115, 117, 116, 112, 106, 99, 92, 86]`.
- **Cancellations line:** red `#e74c3c` 3px line through months 0–11, percent `[2.0, 2.1, 2.2, 2.4, 2.7, 3.1, 3.6, 4.2, 4.8, 5.4, 5.9, 6.3]`.
- **Peak marker:** vertical dashed `#6b7280` (dash 4/3) line at month 5, 12px `#6b7280` label "peak: index 117" at its top.
- **Baseline cross:** small `#008300` open circle where the hours line falls to 99 at month 9, 11px `#444` label "below start".
- **Annotation (bold 13px red `#e74c3c`, near month 7, upper right):** "cancellations triple while the dashboard looks great".
- **Caption (12px `#444`, bottom right):** "both series illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); hours, skip, cancellation, crash, support, index, and churn figures are invented and labeled illustrative; the +30% skip-rate arithmetic (12% → 15.6%) is exact. Facebook and Airbnb north stars are cited only at the publicly known concept level — no internal figures.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
