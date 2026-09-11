# Choosing a Metric

**Page type:** detail page (tutorial layout: h2 card-sections, two-column table 50% text / 50% viz per section)
**HTML title tag:** Choosing a Metric

**Subtitle:** One primary metric, chosen before launch, decides the test — a few guardrail metrics watch for damage

## Three Ways to Score the Same Button Test

Tags: `core idea` (blue), `running example` (green)

- **Clicks** — green wins big: 9.6% vs 8.0% of visitors click the button (+20%)
- **Purchases** — green wins modestly: 3.4% vs 3.1% of visitors buy (+10%)
- **Revenue** — green wins barely: $4.76 vs $4.65 per visitor (+2.4%)
- **Same test** — one experiment, three different headlines depending on the metric
- **The choice** — which number decides is itself a decision, made before launch

*Example (italic):* A shinier button attracts extra clicks from people who were never going to buy.

**Key point:** The metric is not a detail — it defines what "winning" means. Pick it before you see any numbers.

### Visualization (canvas `c1`, 720×300)

Three-panel grouped bar chart: the same test scored by clicks, purchases, and revenue per visitor.

- **Title (bold 16px, `#1a5276`, centered, y=24):** "One Test, Three Headlines (per 10,000 visitors per arm)".
- **Panels (210px wide, 28px gaps, centered; each with bold 13px ink panel title, thin `#999` baseline, chart height 160 from y=62, y-scale max 11):**
  1. "Clicks on button": blue 8.0% (sub "800"), green 9.6% (sub "960"), lift label "+20%"
  2. "Purchases": blue 3.1% (sub "310"), green 3.4% (sub "340"), lift label "+10%"
  3. "Revenue / visitor": blue $4.65 (sub "$46.5k"), green $4.76 (sub "$47.6k"), lift label "+2.4%"
- **Bars:** 62px wide, 46px in-panel gap, 75% alpha; blue arm `#2a78d6` labeled "blue", green arm `#008300` labeled "green"; bold 12px colored value labels above bars ("8%", "9.6%", "3.1%", "3.4%", "$4.65", "$4.76"); mute sub-counts below labels; bold orange (`#d95926`) 14px lift label centered under each panel.
- **Bottom annotation (bold orange 13px, centered):** "the deeper the metric, the smaller the lift — pick which one decides BEFORE launch".

## Picking the Primary: Close to the Goal, but Stable

Tags: `worked example` (green), `rule of thumb` (blue)

- **Too shallow** — clicks are easy to move without making any money
- **Too noisy** — revenue per visitor jumps whenever one visitor places a huge order
- **Just right** — purchase rate: every buyer counts as exactly 1, no order dominates
- **Enough events** — 310 and 340 purchases per arm is plenty to compare
- **Rule of thumb** — the deepest metric that is still stable enough to measure

*Example (italic):* Green's revenue lead is $47,600 − $46,500 = $1,100 — one $900 order in the blue arm nearly wipes it out.

**Key point:** Purchase rate is the primary here: close to the money, one vote per visitor, and no single order can swing it.

### Visualization (canvas `c2`, 720×300)

Histogram: order values in the blue arm, showing a heavy tail that makes revenue noisy.

- **Title (bold 16px, `#1a5276`, centered, y=24):** "Order Values, Blue Arm (310 orders) — Why Revenue Is Noisy".
- **Data:** buckets `['$0-100', '$100-200', '$200-300', '$300-500', '$500-900', '$900+']` with counts `[95, 120, 60, 25, 8, 2]`; y-scale max 130 with mute labels "120" and "60".
- **Axes:** padding top 56 / bottom 60 / left 64 / right 30; `#999` L-axes; mute 12px bucket labels below bars.
- **Bars:** 74px wide, 72% alpha; first four buckets blue `#2a78d6`, last two ("$500-900", "$900+") orange `#d95926`; bold 12px count labels above bars.
- **Annotations:** bold orange 13px centered at ~82% width near the top: "one order out here" / "~ green's whole $1,100 lead"; bold blue 13px at ~58% width upper-mid area (clear of the tall bars): "purchase rate: each of these counts as exactly 1".
- **Caption (mute 12px, bottom center):** "orders (illustrative)".

## Guardrails: Metrics That Must Not Get Worse

Tags: `where it's used` (orange), `rule of thumb` (green)

- **Purpose** — guardrails never pick the winner; they veto a winner that causes damage
- **Refund rate** — blue 1.2%, green 1.3% of purchases refunded: within normal wobble
- **Page load** — 1.8s on both versions: the new button did not slow the page
- **Support tickets** — 4.1 vs 4.2 per 1,000 visitors: flat
- **Veto rule** — also set upfront: "do not ship if refunds rise more than 0.5 points"

*Example (italic):* A brighter "BUY NOW" can lift purchases and refunds — people click, buy, then regret.

**Key point:** The primary metric answers "did it work?"; guardrails answer "did it break anything?". Both lists are written before launch.

### Visualization (canvas `c3`, 720×300)

Horizontal bar dashboard: one bold primary-metric row plus three faint guardrail rows.

- **Title (bold 16px, `#1a5276`, centered, y=24):** "The Decision Dashboard: One Driver, Three Watchers".
- **Rows (52px tall from y=60; right-aligned labels at x=238; vertical grid-color zero line at x=250; bar length = frac × 210px, 18px tall; delta label after the bar; mute 12px note at x=510):**
  1. "PRIMARY: purchase rate" (bold ink label) — green (`#008300`, 80% alpha) bar frac 0.83, bold green delta "+10%", note "drives the decision"
  2. "Guardrail: refund rate" — mute gray (`#6b7280`, 50% alpha) bar frac 0.10, delta "+0.1pt", note "within wobble (veto at +0.5pt)"
  3. "Guardrail: page load" — mute bar frac 0.02, delta "0.0s", note "1.8s on both versions"
  4. "Guardrail: support tickets" — mute bar frac 0.08, delta "+0.1", note "4.1 vs 4.2 per 1,000 visitors"
- **Separator:** grid-color (`#e5e9ef`) horizontal line between the primary row and the guardrail rows.
- **Bottom annotation (bold orange `#d95926` 13px, centered):** "primary is up and no guardrail crossed its veto line → green is shippable".

## The Trap: Choosing the Metric After the Results

Tags: `common mistake` (red)

- **Many metrics** — track 10 numbers and one will look good purely by chance
- **Story time** — "clicks are up 20%, call that the win" is choosing after the fact
- **Write it down** — the primary metric goes in the test plan before launch
- **Everything else** — other movements are hypotheses for the next test, not wins

*Example (italic):* On a do-nothing change, "add-to-wishlist" came out "up significantly" — pure luck among 10 metrics.

**Key point:** If the winning metric was picked after looking at results, the test proved nothing — this trap has a whole family of pitfalls pages of its own.

### Visualization (canvas `c4`, 720×300)

Diverging bar chart: 10 metric deltas on an A/A test around a zero line, one escaping the chance band by luck.

- **Title (bold 16px, `#1a5276`, centered, y=24):** "10 Metrics on a Do-Nothing Change (A/A test)".
- **Data:** labels `['clicks', 'purch.', 'revenue', 'signups', 'bounce', 'search', 'wishlist', 'shares', 'time', 'returns']`; deltas (% lift, illustrative) `[-3, 2, -1, 4, 1, -2, 12, 0, -4, 3]`; symmetric y-scale ±14 around a mid-height zero line.
- **Chance band:** ±8% band filled rgba(107,114,128,0.10) with dashed mute (`#6b7280`, dash 5/4) top and bottom edges; mute axis labels "0%", "+8%", "-8%" at left.
- **Axes:** padding top 56 / bottom 66 / left 64 / right 30; `#999` zero line; mute 12px metric labels along the bottom.
- **Bars:** 42px wide; "wishlist" (delta +12) drawn green `#008300` at 85% alpha with bold green "+12%" label above it; all others mute gray at 45% alpha.
- **Annotations (bold green 13px, centered at ~30% width near the top):** "nothing changed, yet "wishlist" looks like a win —" / "each escapes ~1 in 10 by luck, so 10 metrics ≈ 1 false win".
- **Caption (mute 12px, bottom center):** "grey band = range chance alone produces (illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). Page: `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, four `.card-section` blocks each with `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with one `<tr>`: `.text-col` (50%) and `.viz-col` (50%) holding a 720×300 canvas.
- **Text column structure:** `.tags` pill row (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem bold pills, 10px radius), `<ul>` (0.92rem) of one-line bullets opening with `<b>` (`#1a5276`), italic `.example` (`#555`, 0.9rem), `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px.
- **Canvas palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes (720×300); shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded — no `Math.random()`; invented numbers labeled "illustrative" in captions. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
