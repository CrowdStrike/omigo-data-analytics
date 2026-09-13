# Inference Latency Budgets

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Inference Latency Budgets

**Subtitle:** A latency budget is the total time a model is allowed at answer time — and every feature fetch spends from it, which is why a 50ms model cannot afford a 200ms feature

## Fifty Milliseconds to Pick a Coupon

**Tags:** `core idea` (blue), `serving time` (green), `time budget` (orange)

- **The app** — a pizza app picks a discount coupon the instant a customer opens the checkout page
- **The clock** — the whole page must appear within 300ms, or customers start abandoning carts
- **The slice** — network, login, cart, and rendering claim 250ms, leaving the coupon model just 50ms
- **The budget** — that 50ms is the model's entire allowance: fetch every feature AND do the math
- **The catch** — the model's best feature, 90-day average spend, is a 200ms database query

*Example (italic):* At 8ms of pure math the model looks fast, but its 50ms allowance must also cover every ingredient it asks for.

**Key point:** An inference latency budget is the total wall-clock time a model gets at answer time — feature fetches spend from it just like the model math does.

### Visualization (canvas `c1`, 720×300)

Single thick horizontal stacked bar showing the 300ms checkout page budget split among its five stages, with the coupon model's 50ms slice highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "One Checkout, 300ms Total — the Model's Slice Is 50ms".
- **Bar:** one horizontal stacked bar 46px tall at y=130, from x=60 to x=660 (width 600 = 300ms, scale 2px/ms); segments left to right: network 60ms (`#6b7280`), login check 30ms (`#199e70`), cart service 80ms (`#2a78d6`), coupon model 50ms (`#d95926`), page render 80ms (`#4a3aa7`).
- **Segment labels:** 12px `#444` label above each segment ("network 60", "login 30", "cart 80", "model 50", "render 80"), staggered on two heights (y=105 and y=88) so none overlap.
- **Axis:** 2px `#999` baseline at y=245 from x=60 to x=660; tick marks and 12px `#444` labels "0ms", "100ms", "200ms", "300ms" at x=60, 260, 460, 660.
- **Highlight:** 2px `#d95926` bracket under the model segment from x=400 to x=500 at y=190.
- **Annotation (bold 13px orange `#d95926`, two lines, centered near x=450, y=215):** "the model's whole world: 50ms —" / "features included".
- **Caption (12px `#444`, bottom right):** "illustrative — a typical checkout page budget".

## Adding Up the Milliseconds

**Tags:** `worked example` (blue), `feature fetch` (green)

- **The wish list** — the model wants three features: cart contents, loyalty tier, 90-day average spend
- **Cheap features** — cart contents ride in with the request (2ms); loyalty tier sits in a cache (5ms)
- **The expensive one** — 90-day average spend means a live warehouse query: 200ms on a good day
- **Add it up** — 2 + 5 + 200, plus 8ms of model math = 215ms, over four times the 50ms budget
- **The fix** — compute average spend every night, store it in a fast lookup: 5ms instead of 200ms
- **New total** — 2 + 5 + 5 + 8 = 20ms, comfortably inside the budget with 30ms to spare

*Example (italic):* Same model, same feature value — fetched live it costs 200ms, precomputed overnight it costs 5ms.

**Key point:** 2 + 5 + 200 + 8 = 215ms fails a 50ms budget; precomputing turns it into 2 + 5 + 5 + 8 = 20ms — and the model itself never changed.

### Visualization (canvas `c2`, 720×300)

Two horizontal stacked bars on a shared millisecond axis — the live-query plan (215ms) versus the precomputed plan (20ms) — with the 50ms budget drawn as a vertical line both must beat.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Get the Same Feature: 215ms vs 20ms".
- **Axis:** horizontal 2px `#999` line at y=250 from x=200 to x=680 (width 480 = 240ms, scale 2px/ms); tick labels "0", "50", "100", "150", "200" (12px `#444`) below, "ms" after the last.
- **Legend (11px `#444`, one row at y=62, left-aligned at x=200):** color swatches with labels — cart contents `#2a78d6`, loyalty cache `#199e70`, spend feature `#e74c3c` live / `#008300` precomputed, model math `#4a3aa7`.
- **Row 1 (bar 26px tall at y=110), 12px `#444` label at x=20:** "live warehouse query"; stacked from x=200: cart 2ms blue, loyalty 5ms aqua, warehouse query 200ms red `#e74c3c`, model math 8ms violet (total 215ms = 430px); bold 12px red end label beside the bar on two lines: "215ms" / "4× over".
- **Row 2 (bar 26px tall at y=180), label at x=20:** "precomputed lookup"; stacked from x=200: cart 2ms blue, loyalty 5ms aqua, feature-store lookup 5ms green `#008300`, model math 8ms violet (total 20ms = 40px); bold 12px green end label "20ms — 30ms to spare".
- **Budget line:** vertical dashed `#1a5276` (dash 4/3) at 50ms (x=300) from y=75 to the axis; bold 12px `#1a5276` label "budget: 50ms" at its top.
- **Annotation (bold 13px red `#e74c3c`, near x=440, y=90):** "one slow feature blows the whole budget".
- **Caption (12px `#444`, bottom right):** "illustrative timings — same trained model in both rows".

## Timeouts, Tails, and the Fallback Coupon

**Tags:** `where it's used` (blue), `tail latency` (orange), `timeouts` (green)

- **The timeout** — when the 50ms runs out, the page ships without the model and shows a generic coupon
- **The tail** — the precomputed plan averages 20ms, but 1 request in 100 takes 48ms, 1 in 1,000 takes 85ms
- **Tail budgeting** — teams promise the budget at p99, not the average; the slow cases are real customers
- **Stacked tails** — every extra service call brings its own slow days, so chains of calls multiply the risk
- **Design pressure** — the budget decides the feature list, so it belongs in the design, not the launch review

*Example (italic):* A plan that fits the budget "on average" still hands 1 in every 1,000 customers the fallback coupon.

**Key point:** Set and test the budget at the tail: an average of 20ms with a p99.9 of 85ms still means real customers hit the timeout.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of the precomputed plan's latency percentiles against the 50ms budget line — the typical request is fine, the tail is not.

- **Title (bold 15px, `#1a5276`, top center):** "Budgets Live at the Tail: p99 Must Fit, Not the Average".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = latency 0 to 100ms with light `#e5e9ef` gridlines at 25, 50, 75 and 12px `#444` labels "25ms", "50ms", "75ms" on the left.
- **Bars (width 70px, centered at x = 170, 310, 450, 590), heights from hardcoded values `[20, 31, 48, 85]` ms:** p50 = 20, p90 = 31, p99 = 48 filled green `#008300`; p99.9 = 85 filled red `#e74c3c`; bold 12px value label above each bar ("20ms", "31ms", "48ms", "85ms"); 12px `#444` category labels "p50 (≈ mean)", "p90", "p99", "p99.9" below the baseline.
- **Budget line:** horizontal dashed `#1a5276` (dash 4/3) at 50ms (y=150) across the plot; bold 12px `#1a5276` label "budget 50ms" at its right end.
- **Annotation (bold 13px red `#e74c3c`, two lines, near x=470, y=95):** "1 in 1,000 checkouts still times out" / "→ fallback coupon".
- **Caption (12px `#444`, bottom right):** "illustrative — latency percentiles of the precomputed plan".

## The 8ms Model That Takes 215ms to Answer

**Tags:** `common mistake` (red), `benchmarking` (orange)

- **The benchmark trap** — timing model.predict() in a notebook measures the math (8ms), not the answer
- **The real wait** — in production the same model answers in 215ms because features are fetched live
- **The share** — the model math is roughly 4% of the wait; feature fetches are nearly all the rest
- **Wrong fix** — shrinking the model from 8ms to 4ms saves 4ms; fixing the feature fetch saves 195ms
- **Right habit** — benchmark the full request path, features included, before promising a budget

*Example (italic):* A team distilled their model to half its size and shaved 4ms — the 200ms warehouse call was untouched.

**Common mistake:** Reporting the model's math time as its latency. The budget is spent on the whole answer path, and feature fetches usually dominate it.

### Visualization (canvas `c4`, 720×300)

Two vertical bars: what the notebook benchmark times (8ms of model math) beside what the customer actually waits for (the full 215ms stack), with matching segment colors from `c2`.

- **Title (bold 15px, `#1a5276`, top center):** "The Notebook Says 8ms; the Customer Waits 215ms".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = latency 0 to 240ms with light `#e5e9ef` gridlines at 50, 100, 150, 200 and 12px `#444` labels on the left.
- **Bar 1 (width 120, centered x=240):** single violet `#4a3aa7` bar of 8ms; bold 12px value label "8ms" above; 12px `#444` label below the baseline: "notebook benchmark (model math only)".
- **Bar 2 (width 120, centered x=500):** stacked from the baseline up: model math 8ms violet `#4a3aa7`, cart contents 2ms blue `#2a78d6`, loyalty cache 5ms aqua `#199e70`, warehouse query 200ms red `#e74c3c` (total 215ms); bold 12px value label "215ms" above; 12px `#444` label below: "production request (features fetched live)".
- **Segment labels:** 11px `#444` label "warehouse 200ms" centered inside the red segment; the three thin segments get one shared 11px note to the right with a thin pointer line: "math + cheap features: 15ms".
- **Annotation (bold 13px orange `#d95926`, two lines, centered near x=370, y=80):** "model math is ~4% of the wait —" / "features are the rest".
- **Caption (12px `#444`, bottom right):** "illustrative — same trained model in both bars".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red is used only for genuine over-budget/alarm elements (the 200ms warehouse segment, the p99.9 bar).
- **Data:** all millisecond values are the hardcoded literals above (no randomness); the same numbers appear in text and charts — page budget split 60/30/80/50/80 = 300, live plan 2+5+200+8 = 215, precomputed plan 2+5+5+8 = 20, percentiles `[20, 31, 48, 85]`. All are invented and every chart carries an "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
