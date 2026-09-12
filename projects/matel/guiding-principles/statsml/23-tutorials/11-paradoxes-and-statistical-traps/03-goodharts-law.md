# Goodhart's Law

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Goodhart's Law

**Subtitle:** When a measure becomes a target, people optimize the number instead of the thing it measured — and the number quietly stops meaning anything

## One Help Desk, One Bonus, One Broken Number

**Tags:** `core idea` (blue), `metric as target` (orange), `gaming` (red)

- **The help desk** — a support team closes about 40 tickets a day, and customers rate them 4.5/5
- **The metric** — "tickets closed per day" tracked real work honestly for six straight months
- **The target** — management announces a bonus for any agent closing 60+ tickets a day
- **The gaming** — agents split one issue into three tickets and close hard ones without a fix
- **The break** — closures jump from 40 to 65 a day while satisfaction slides from 4.5 to 3.1

*Example (italic):* Once the bonus landed, a "closed" ticket meant "an agent clicked close", not "a customer got helped".

**Key point:** This is Goodhart's law: when a measure becomes a target, it ceases to be a good measure. The number tracked the work only while nobody was paid to move the number.

### Visualization (canvas `c1`, 720×300)

Dual-line chart over 12 months: tickets closed per day (left scale, rising) and customer satisfaction (right scale, falling), with a dashed vertical line where the target was announced.

- **Title (bold 15px, `#1a5276`, top center):** "Tickets Closed vs Customer Satisfaction, 12 Months (illustrative)".
- **Data:** months labeled "M1"–"M12"; closures per day `[38, 40, 39, 41, 40, 42, 55, 60, 63, 65, 66, 65]`; satisfaction `[4.5, 4.5, 4.4, 4.5, 4.6, 4.5, 4.1, 3.8, 3.5, 3.3, 3.2, 3.1]`.
- **Layout:** plot origin x=60, width 590, baseline y=245, chart height 185; month labels 12px `#444` below baseline.
- **Left axis (closures):** scale 0–70, ticks every 10, labels 12px `#444` at x=52 right-aligned; blue `#2a78d6` 3px line with 4px dots.
- **Right axis (satisfaction):** scale 0–5, ticks 0/1/2/3/4/5, labels 12px `#444` at x=658; magenta `#d55181` 3px line with 4px dots.
- **Target marker:** dashed `#bdc3c7` (dash 4/3) vertical line at the M7 x-position from y=40 to y=245; orange `#d95926` bold 12px label "bonus: 60+/day announced" beside it near the top.
- **Annotations:** blue bold 12px "closures up 63%" near the end of the blue line; magenta bold 12px "satisfaction 4.5 → 3.1" near the end of the magenta line.
- **Caption (12px `#444`, bottom left):** "same team, same customers — only the incentive changed".

## Where the Extra 25 Closures Came From

**Tags:** `worked example` (blue), `gaming` (red)

- **Before** — 40 closures a day, and a ticket audit found all 40 were genuinely resolved issues
- **After** — 65 closures a day, but the same audit finds only 36 are genuine fixes
- **Splitting** — 14 closures come from single issues logged and closed as three separate tickets
- **Dumping** — 9 closures are hard tickets marked closed with no fix at all
- **Churning** — 6 closures are reopened tickets closed a second time for double credit
- **Net effect** — the metric rose from 40 to 65 while real solved work fell from 40 to 36

*Example (italic):* An agent hitting 65 closures did less genuine work (36 fixes) than an agent who used to log 40.

**Key point:** The gap between the number and the work is exactly what the gaming manufactured: 29 of the 65 daily closures are splits, dumps, and re-closes — noise wearing the metric's uniform.

### Visualization (canvas `c2`, 720×300)

Two horizontal stacked bars decomposing daily closures before vs after the target, with a color legend for the four closure types.

- **Title (bold 15px, `#1a5276`, top center):** "Anatomy of a Day's Closures: Before vs After the Target".
- **Data:** before = 40 genuine; after = 36 genuine + 14 split duplicates + 9 closed unsolved + 6 re-closed reopens (total 65).
- **Scale:** bars start at x=130, max width 540 mapped to 0–70 tickets; ticks at 0/10/20/30/40/50/60/70 with 11px `#444` labels along a 1px `#999` axis line at y=225.
- **Before bar:** y=95, 28px tall, row label bold 12px `#444` "before (40/day)" left of the bar; single segment fill `rgba(0,131,0,0.5)` with green `#008300` bold 12px "40 genuine" centered inside.
- **After bar:** y=160, 28px tall, row label "after (65/day)"; segments in order — genuine 36 `rgba(0,131,0,0.5)`, splits 14 `rgba(217,89,38,0.55)`, dumps 9 `rgba(213,81,129,0.55)`, re-closes 6 `rgba(201,133,0,0.55)`; each segment's count in bold 12px matching-color text inside or just above it.
- **Legend (y=250, left-aligned row):** 10px color squares + 12px `#444` labels: "genuine fix", "split duplicates", "closed unsolved", "re-closed reopens".
- **Annotation:** magenta `#d55181` bold 13px above the after bar's right end: "only 36 of 65 are real work".

## Every Proxy Metric Is One Target Away From This

**Tags:** `where it's used` (blue), `proxy metrics` (orange)

- **Proxies** — nearly every KPI is a stand-in: clicks for interest, closures for customers helped
- **The link** — closures tracked customers-helped at 0.9 correlation until the bonus; then 0.2
- **Optimizing** — the moment pay, promotion, or a model optimizes the proxy, gaming pressure begins
- **ML twist** — a model trained hard on a proxy label learns the gaming automatically, at scale
- **Clickbait** — a feed rewarded on CTR breeds headlines that earn clicks and regret, not value
- **Schools** — teaching to the test raises scores while the learning the scores proxied stalls

*Example (italic):* A feed model rewarded on clicks learned that outrage gets clicked — so the click stopped meaning "the user liked this".

**Key point:** What Goodhart breaks is the correlation between proxy and goal. Measured before targeting, the link is real; the act of targeting is what snaps it.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: proxy-to-goal correlation for four domains, one bar pair per domain (before targeting vs after), on a 0–1 scale.

- **Title (bold 15px, `#1a5276`, top center):** "Proxy–Goal Correlation Before vs After Targeting (illustrative)".
- **Data:** domains `["tickets closed", "CTR", "test scores", "ER wait times"]`; before `[0.9, 0.7, 0.8, 0.6]`; after `[0.2, 0.2, 0.3, 0.1]`.
- **Layout:** plot origin x=60, baseline y=235, chart height 170, plot width 590; y scale 0–1 with gridlines `#e5e9ef` at 0.25/0.5/0.75/1.0 and 12px `#444` labels at x=52; four groups evenly spaced, each with two 44px bars 8px apart.
- **Bars:** before fill `rgba(42,120,214,0.55)` with 2px `#2a78d6` top edge; after fill `rgba(217,89,38,0.55)` with 2px `#d95926` top edge; each bar's value (e.g. "0.9") bold 12px in its edge color, centered above.
- **Domain labels:** 12px `#444` centered below each group at y=255.
- **Legend (top right, inside plot):** 10px squares + 12px `#444` labels "before targeting" (blue), "after targeting" (orange).
- **Annotation:** magenta `#d55181` bold 13px, centered over the gap between groups 2 and 3 at y=70: "targeting is what snaps the link".

## The Fix Is Not a Better Metric

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Not a bad metric** — closures tracked the work honestly for six months; the target broke it
- **Swapping fails** — make satisfaction the target instead and agents start begging for 5 stars
- **Pair it** — track closures alongside reopen rate and audited-fix rate so gaming shows up
- **The signal** — reopens jumped 5% to 22% and audited genuine fixes fell 100% to 55%
- **Alert line** — a simple 10% reopen threshold would have flagged the gaming in month 8
- **Loose grip** — numbers used for insight stay honest far longer than numbers tied to bonuses

*Example (italic):* One month after pairing closures with reopen rate, the 22% reopen spike exposed the ticket-splitting scheme.

**Common mistake:** Blaming the metric and hunting for a perfect replacement — any single number, once targeted, invites the same gaming; a pair of opposing metrics is the real defense.

### Visualization (canvas `c4`, 720×300)

Dual-line percent chart over the same 12 months: reopen rate rising and audited genuine-fix rate falling, with the target marker and a dashed alert threshold.

- **Title (bold 15px, `#1a5276`, top center):** "Two Counter-Metrics Catch What the Target Metric Hides".
- **Data:** months "M1"–"M12"; reopen rate % `[5, 5, 6, 5, 5, 5, 9, 14, 18, 22, 21, 22]`; audited genuine-fix rate % `[100, 100, 100, 100, 100, 100, 85, 70, 62, 57, 56, 55]`.
- **Layout:** plot origin x=60, width 590, baseline y=245, chart height 185; y scale 0–100% with ticks every 25 (labels 12px `#444` at x=52); month labels 12px `#444` below baseline.
- **Lines:** genuine-fix rate green `#008300` 3px with 4px dots; reopen rate magenta `#d55181` 3px with 4px dots.
- **Target marker:** dashed `#bdc3c7` (dash 4/3) vertical line at the M7 x-position from y=40 to y=245, labeled 11px `#6b7280` "target announced".
- **Alert threshold:** dashed orange `#d95926` (dash 4/3) horizontal line at the 10% level across the plot; orange bold 12px label "reopen alert: 10%" above its left end.
- **Annotations:** magenta bold 12px "reopens 5% → 22%" near the end of the magenta line; green bold 12px "genuine fixes 100% → 55%" near the end of the green line.
- **Caption (12px `#444`, bottom left):** "same 12 months as the closures chart — the pair tells the true story".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays — no `Math.random()`; invented figures carry an "illustrative" label in the chart title or caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
