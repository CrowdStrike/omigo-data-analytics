# Zero-Inflated Data

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Zero-Inflated Data

**Subtitle:** When a giant spike of zeros sits next to an ordinary hill, treat "did it happen at all?" and "how much, when it did?" as two separate questions

## One Delivery App, One Giant Bar at Zero

**Tags:** `core idea` (blue), `zero spike` (orange), `two processes` (green)

- **The app** — a delivery app has 1,000 signed-up users; last month 600 of them placed zero orders
- **The spike** — the histogram shows a giant bar at 0 next to a modest hill running from 1 to 8 orders
- **Two stories** — the spike answers "did they order at all?"; the hill answers "how many, if they did?"
- **The name** — data with far more zeros than any single count curve expects is called zero-inflated
- **Median zero** — with 600 zeros out of 1,000, the median user placed exactly zero orders

*Example (italic):* Marketing asked "how many orders does a typical user place?" — for most users the honest answer is zero.

**Key point:** The spike at zero is its own process, not the left edge of the hill. Describe "whether it happened" and "how much" as two separate things.

### Visualization (canvas `c1`, 720×300)

Single histogram of orders per user (0–8) with the zero bar in magenta towering over a blue hill for 1+.

- **Title (bold 15px, `#1a5276`, top center):** "Orders per User Last Month: 1,000 Delivery-App Users".
- **Data:** order counts 0–8, users per count `[600, 76, 100, 91, 63, 38, 19, 9, 4]`.
- **Axes:** origin x=60, plot width 620, baseline y=245, chart height 185, y scale 0–650; x labels "0"–"8" 12px `#444` below each bar; y-axis 2px `#1a5276` with ticks at 0/200/400/600 (11px `#6b7280`).
- **Bars:** zero bar fill `rgba(213,81,129,0.55)` with 2px `#d55181` outline; bars 1–8 fill `rgba(42,120,214,0.45)`; bold 12px count label above each bar (magenta `#d55181` for "600", blue `#2a78d6` for the rest).
- **Annotations:** magenta `#d55181` bold 13px, two lines near the zero bar: "600 of 1,000 users" / "the spike is its own story"; blue `#2a78d6` bold 12px above the hill: "an ordinary hill for everyone else".
- **Caption (12px `#444`, bottom right):** "one month of orders, all 1,000 users (illustrative)".

## Splitting 1,000 Users into Whether and How Much

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Step 1: whether** — 400 of 1,000 users placed at least one order, so the chance of activity is 0.40
- **Step 2: how much** — the 400 active users placed 1,200 orders in total, an average of 3.0 each
- **Recombine** — overall mean = 0.40 × 3.0 = 1.2 orders per user, a number that describes nobody
- **Check by hand** — 1,200 orders ÷ 1,000 users = 1.2; the split just shows where the 1.2 comes from
- **Two levers** — growth can mean more users ordering at all, or actives ordering more; 1.2 hides which

*Example (italic):* If actives grow from 400 to 500 users at the same 3.0 average, the overall mean jumps to 1.5 with no active user changing at all.

**Key point:** Overall mean = (share who do it at all) × (average among those who do). Report both parts — the product alone answers neither question.

### Visualization (canvas `c2`, 720×300)

Dual-panel: whether-split bars (left) and the actives-only histogram with its mean line (right), separated by a dashed divider at x=300.

- **Title (bold 15px, `#1a5276`, top center):** "Whether (left) × How Much (right) = 1.2 Orders per User".
- **Left panel (whether):** axis origin x=55, width 190, baseline y=235, chart height 165, y scale 0–650; two bars — "0 orders" 600 users, fill `rgba(213,81,129,0.55)`, and "1+ orders" 400 users, fill `rgba(0,131,0,0.4)`; bold 12px count labels above bars; labels 12px `#444` below; caption 12px `#444` "40% of users are active".
- **Right panel (how much, actives only):** axis origin x=345, width 330, baseline y=235, chart height 165, y scale 0–110; bars for 1–8 orders, users `[76, 100, 91, 63, 38, 19, 9, 4]`, fill `rgba(0,131,0,0.4)`; x labels "1"–"8" 12px `#444`; green `#008300` dashed (dash 5/4) vertical line at x-position of 3.0 orders, bold 12px green label "mean of actives = 3.0"; caption "400 active users, 1,200 orders".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=300 from y=38 to h-30.
- **Takeaway (bold 13px `#1a5276`, bottom center):** "overall 1.2 = 0.40 chance active × 3.0 orders per active".

## Why One Curve Cannot Fit This Shape

**Tags:** `where it's used` (blue), `model fit` (orange), `failure mode` (red)

- **The lazy fit** — a single Poisson curve with mean 1.2 predicts only 301 zeros; the data has 600
- **The squeeze** — buying more zeros makes the curve overpredict ones (361 vs 76) and starve the tail
- **The fix** — a zero-inflated or hurdle model fits two parts: a zero coin flip plus a count curve
- **Everywhere** — insurance claims, doctor visits, purchases, defects, and rainy days all spike at zero
- **Payoff** — the two-part model separates users who could be activated from actives worth nudging

*Example (italic):* A demand forecast built on the single-curve fit undercounted inactive users by half and overpromised order growth.

**Key point:** When the zeros are roughly double what your count model predicts, stop bending one curve — model "zero or not" and "how many" separately.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart comparing observed users per order count against a single Poisson(1.2) fit for counts 0–8.

- **Title (bold 15px, `#1a5276`, top center):** "Observed Users vs a Single Poisson(1.2) Fit".
- **Data:** order counts 0–8; observed `[600, 76, 100, 91, 63, 38, 19, 9, 4]`; Poisson(1.2) prediction `[301, 361, 217, 87, 26, 6, 1, 0, 0]` (×1,000 users, rounded).
- **Axes:** origin x=60, plot width 620, baseline y=240, chart height 175, y scale 0–650; x labels "0"–"8" 12px `#444`; y ticks 0/200/400/600 (11px `#6b7280`).
- **Bars:** per count, observed bar fill `rgba(42,120,214,0.5)` left, Poisson bar fill `rgba(217,89,38,0.5)` right, 2px gap within the pair; bold 12px labels "600" (blue) and "301" (orange) above the zero pair only.
- **Legend (top right, 12px):** blue swatch "observed", orange swatch "Poisson(1.2) fit".
- **Annotation:** magenta `#d55181` bold 13px, two lines over counts 0–1: "curve buys zeros it can't afford:" / "301 vs 600 at zero, 361 vs 76 at one".
- **Caption (12px `#444`, bottom right):** "same overall mean 1.2, completely different shape".

## Not All Zeros Mean the Same Thing

**Tags:** `common mistake` (red), `two kinds of zero` (orange)

- **Not missing** — a zero is a real measurement, not a blank; deleting zero rows invents a busy user base
- **Two kinds** — some zeros are "gone" (uninstalled, no payment set up); others are active but quiet
- **The split** — of the 600 zeros, roughly 450 look gone and 150 look quiet (illustrative estimate)
- **Different fixes** — gone users need reactivation; quiet actives need a nudge — same zero, different action
- **Log trap** — log(0) does not exist, so "just take logs" silently drops or distorts the entire spike

*Example (italic):* A team dropped the zeros "to clean the data" and reported 3.0 orders per typical user — 2.5 times the true overall average of 1.2.

**Common mistake:** Treating every zero the same — or deleting them. Zeros carry the "whether" half of the story; a zero-inflated model estimates how many are structural ("gone") versus just quiet.

### Visualization (canvas `c4`, 720×300)

One wide stacked bar splitting the 600 zero-order users into "gone" and "quiet" segments, with an action label under each segment.

- **Title (bold 15px, `#1a5276`, top center):** "Inside the Zero Bar: 600 Users, Two Different Stories (illustrative)".
- **Stacked bar:** at y=110, from x=70, total width 580, height 36; violet segment `rgba(74,58,167,0.55)` width 435px (450 users), aqua segment `rgba(25,158,112,0.55)` width 145px (150 users); 2px `#fff` seam between segments; bold 13px white count labels "450" and "150" centered inside each segment.
- **Segment labels (above the bar, bold 12px):** violet `#4a3aa7` over the left segment: "gone — uninstalled or never set up payment"; aqua `#199e70` over the right segment: "active but quiet this month".
- **Action labels (below the bar, y=175, bold 12px):** violet "fix: reactivation campaign" under the left segment; aqua "fix: a nudge or reminder" under the right segment; thin 1px `#999` connector line from each label to its segment.
- **Note (12px `#444`, y=225, centered):** "a zero-inflated model estimates this split from the data — it is not visible in the histogram alone".
- **Takeaway (bold 13px magenta `#d55181`, centered at y=280):** "same zero in the spreadsheet, two different customers — model whether and how much".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded and internally consistent: 1,000 users = 600 zeros + 400 actives; active histogram `[76, 100, 91, 63, 38, 19, 9, 4]` sums to 400 users and 1,200 orders (mean 3.0); overall mean 1.2 = 0.40 × 3.0; Poisson(1.2) fit `[301, 361, 217, 87, 26, 6, 1, 0, 0]`; zero-bar split 450 + 150 = 600 (illustrative).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
