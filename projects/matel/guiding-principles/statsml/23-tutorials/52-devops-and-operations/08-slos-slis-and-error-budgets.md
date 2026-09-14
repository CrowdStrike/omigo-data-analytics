# SLOs, SLIs & Error Budgets

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SLOs, SLIs & Error Budgets

**Subtitle:** Reliability becomes a number you agree on — measure it (SLI), target it (SLO), contract it (SLA), and spend the allowed failure like a budget

## Three Numbers, Three Different Jobs

**Tags:** `core idea` (blue), `SLI / SLO / SLA` (green), `Google SRE` (orange)

- **The service** — a shopping site's checkout API handles 100 million requests every month
- **The SLI** — the measured thing: last month 99.92% of checkout requests succeeded under 300ms
- **The SLO** — the internal target the team commits to: 99.9% of requests succeed over any 30 days
- **The SLA** — the external contract with customers: 99.5%, with service credits owed below it
- **The ordering** — the SLA is kept looser than the SLO, so the target trips before the penalties do
- **The source** — the framework is published Google SRE doctrine, from the Site Reliability Engineering book

*Example (italic):* Last month checkout measured 99.92% (SLI) against a 99.9% target (SLO) and a 99.5% contract (SLA) — three numbers doing three different jobs.

**Key point:** An SLI is what you measure, an SLO is the internal target you set on it, and an SLA is the looser external promise with penalties — untangling the three is step one of the SRE reliability framework.

### Visualization (canvas `c1`, 720×300)

Number-line chart placing the month's three numbers on one success-rate scale, with colored zones showing which promise each region breaks.

- **Title (bold 15px, `#1a5276`, top center):** "Three Numbers on One Scale: Measured, Target, Contract".
- **Axis:** horizontal 2px `#999` line at y=170 from x=60 to x=660, mapping 99.4% (x=60) linearly to 100.0% (x=660); 12px `#444` tick labels every 0.1% below the line.
- **Zone bands (20px tall, sitting on the axis):** red fill `rgba(231,76,60,0.12)` from 99.4% to 99.5% (x 60–160); orange fill `rgba(230,126,34,0.15)` from 99.5% to 99.9% (x 160–560); green fill `rgba(0,131,0,0.12)` from 99.9% to 100.0% (x 560–660).
- **SLA marker:** vertical 3px red `#e74c3c` line at x=160 (99.5%), bold 12px red label "SLA 99.5% — contract, credits owed below" at y=215.
- **SLO marker:** vertical 3px ink `#1a5276` line at x=560 (99.9%), bold 12px `#1a5276` label "SLO 99.9% — internal target" at y=120.
- **SLI marker:** filled green `#008300` dot radius 6 at x=580 (99.92%) on the axis, bold 12px green label "SLI measured: 99.92%" at y=80 with a short 1.5px `#008300` pointer line down to the dot.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=255):** "the SLA is kept looser than the SLO — the target trips first".
- **Caption (12px `#444`, bottom right):** "measured value illustrative; scale exact".

## A Budget of 100,000 Errors to Spend

**Tags:** `worked example` (blue), `error budget` (green)

- **The flip** — 99.9% success means 0.1% failure is ALLOWED, on purpose; failure becomes a budget
- **The arithmetic (exact)** — 100,000,000 requests/month × 0.1% = 100,000 errors to spend
- **The drip** — background failures burn about 300 errors a day, 9,000 across the 30-day month
- **The spends** — release v41 costs 8,000, a database migration 25,000, a chaos drill 12,000
- **The balance** — 9,000 + 8,000 + 25,000 + 12,000 = 54,000 spent, leaving 46,000 on day 30

*Example (italic):* The day-12 migration burns 25,000 errors in one afternoon — a quarter of the budget, spent deliberately and still within target.

**Key point:** The error budget reframes reliability from "never fail" — impossible, and infinitely expensive — to "fail within budget": 100,000 errors a month the team may deliberately spend.

### Visualization (canvas `c2`, 720×300)

Step-line burn-down of the month's error budget: a slow background drip plus three labeled vertical drops for the release, the migration, and the chaos drill.

- **Title (bold 15px, `#1a5276`, top center):** "One Month's Error Budget: 100,000 Errors to Spend".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = day 0 to 30 with 12px `#444` tick labels every 5 days; y = errors remaining 0 to 100,000, gridlines `#e5e9ef` at 25,000 / 50,000 / 75,000 with labels "25k" / "50k" / "75k" / "100k".
- **Burn-down line:** blue `#2a78d6` 3px step line through (day, remaining) points `[0,100000], [3,99100], [3,91100], [12,88400], [12,63400], [22,60400], [22,48400], [30,46000]` — shallow drip segments (300/day) joined by three vertical cliffs.
- **Drop labels (bold 12px, beside each cliff):** orange `#d95926` "release v41 −8,000" near day 3; magenta `#d55181` "db migration −25,000" near day 12; violet `#4a3aa7` "chaos drill −12,000" near day 22.
- **Annotation (bold 13px green `#008300`, near day 25, y=140):** "day 30: 46,000 left — the month ends within budget".
- **Caption (12px `#444`, bottom right):** "budget arithmetic exact (100M × 0.1% = 100,000); individual spends illustrative".

## What the Budget Buys: Permission to Ship

**Tags:** `where it's used` (blue), `freeze policy` (green), `burn rate` (orange)

- **The purchase** — releases, migrations, and chaos experiments all spend budget; that is their price tag
- **Healthy budget** — plenty remaining means ship fast, take risks, run the experiment now
- **Exhausted budget** — zero remaining triggers a feature freeze; only reliability work merges
- **The peace treaty** — the freeze policy is written down in advance, ending the eternal dev-vs-ops fight
- **The nines tax** — each added nine costs roughly 10×: a 99.99% SLO shrinks the budget to 10,000
- **The alarm** — burn-rate alerting pages when budget burns far faster than the steady month-long pace

*Example (italic):* An incident empties the budget on day 8 and nobody argues — the pre-agreed policy freezes features and the whole team works on reliability.

**Key point:** The budget makes the speed-vs-reliability trade-off explicit and pre-agreed: spend it on shipping while it is healthy, stop and repair when it runs out.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: the same written policy routing a healthy budget to "move fast" and an exhausted budget to "feature freeze".

- **Title (bold 15px, `#1a5276`, top center):** "Budget State Decides: Ship Fast or Freeze and Fix".
- **Row 1 (y=95), 12px `#444` label at x=20:** "budget healthy"; green-bordered rounded box at x=170 labeled "46,000 errors left" (fill `rgba(0,131,0,0.12)`), 3px green `#008300` arrow to a box at x=430 labeled "ship releases, migrate, run chaos drills" with bold 12px green "✓ move fast".
- **Row 2 (y=205), label:** "budget exhausted"; red-bordered box at x=170 labeled "0 errors left" (fill `rgba(231,76,60,0.12)`), 3px red `#e74c3c` arrow to a box at x=430 labeled "feature freeze — reliability work only" with bold 12px red "✗ no launches".
- **Box style:** 150–210px wide, 40px tall, 8px radius, 12px `#2c3e50` text; action boxes fill `rgba(42,120,214,0.15)` with 1.5px `#2a78d6` border.
- **Mid label (12px `#6b7280`, centered between rows at y=150):** "same written policy, agreed before the quarter — no debate at incident time".
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the budget turns dev-vs-ops from an argument into arithmetic".

## The Vanity SLO Nobody Enforces

**Tags:** `common mistake` (red), `measure the user` (orange)

- **The poster** — a 99.99% target chosen to sound impressive, with no freeze policy ever attached
- **The proxy** — measuring the load balancer, not the user: DNS failures and client timeouts never appear
- **The split** — the server-side SLI reads 99.96% while users actually experience 99.62%
- **The hidden toll** — 99.62% means 3,800 failed checkouts per million that the dashboard never shows
- **The honest start** — set the first SLO at current measured performance, not at an aspiration
- **The test** — an SLO nobody would freeze features for is decoration, not an objective

*Example (italic):* For six straight months the dashboard shows the 99.9% SLO comfortably met, while users miss it every single month and nothing changes.

**Common mistake:** Treating the SLO as a marketing number. If it is not measured where users are and does not trigger the freeze policy, it is a vanity target — the framework only works when the budget has teeth.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart of six months: the server-side proxy SLI clears the SLO line every month while the user-measured SLI misses it every month.

- **Title (bold 15px, `#1a5276`, top center):** "The Proxy Passes While Users Fail: Six Months of a Vanity SLO".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y = success rate mapping 99.4% (baseline) to 100.0% (top), gridlines `#e5e9ef` at 99.6% and 99.8% with 12px `#444` labels; x = months "Jan"–"Jun", group centers at x = 118, 214, 310, 406, 502, 598 with 12px `#444` labels below.
- **SLO line:** dashed (4/3) 2px `#1a5276` horizontal line at 99.9% with bold 12px ink label "SLO 99.9%" at its left end.
- **Bars (30px wide, 6px gap within each pair, drawn up from the 99.4% baseline):** load-balancer SLI, fill `rgba(0,131,0,0.35)` with 1.5px `#008300` border, values `[99.96, 99.95, 99.97, 99.96, 99.95, 99.96]`; user-measured SLI, fill `rgba(231,76,60,0.30)` with 1.5px `#e74c3c` border, values `[99.62, 99.71, 99.55, 99.68, 99.60, 99.66]`.
- **Legend (12px, top-left inside plot at x=85, y=60):** green square "load balancer SLI", red square "user-measured SLI".
- **Annotation (bold 13px red `#e74c3c`, right side near y=95):** "users miss the SLO all six months — nobody freezes anything".
- **Caption (12px `#444`, bottom right):** "axis truncated at 99.4%; monthly values illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the budget arithmetic (100,000,000 × 0.1% = 100,000, and the 54,000-spent / 46,000-left ledger) is exact, as is the 10× shrink to 10,000 at 99.99%; the measured 99.92% SLI, the drip rate, the three spend sizes, and the six monthly SLI pairs are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
