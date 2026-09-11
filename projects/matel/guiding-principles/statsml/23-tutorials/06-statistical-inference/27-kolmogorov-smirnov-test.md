# Kolmogorov-Smirnov Test

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Kolmogorov-Smirnov Test

**Subtitle:** Turn two samples into "what fraction has arrived by now" curves — the K-S statistic is the biggest vertical gap between them, so it compares whole shapes, not just averages

## Two Routing Algorithms, One Suspicious Average

**Tags:** `core idea` (blue), `running example` (green), `same mean, different shape` (orange)

- **The trial** — a pizza app tests new delivery routing: 10 orders each way, timed in minutes
- **Old routing** — 24, 26, 27, 28, 29, 30, 31, 32, 34, 36: every pizza lands near the 30-minute mark
- **New routing** — 14, 16, 18, 20, 22, 36, 38, 40, 42, 44: half arrive fast, half arrive very late
- **The averages** — old mean 29.7, new mean 29.0 minutes; the dashboard reports "no change"
- **The shapes** — customers disagree: new routing splits them into delighted and furious halves

*Example (italic):* A 29-minute average can mean "always about 30" or "half in 18, half in 40" — the same number, a very different pizza experience.

**Key point:** An average is one number squeezed out of a distribution, and two very different shapes can share it. The K-S test compares the entire distributions, not their summaries.

### Visualization (canvas `c1`, 720×300)

Dot-strip plot: each delivery time as a dot on a shared minutes axis, old routing on the top row, new routing on the bottom row, with dashed mean markers almost on top of each other.

- **Title (bold 15px, `#1a5276`, top center):** "20 Pizza Deliveries: Same Average, Different Shape".
- **Data:** old routing minutes `[24, 26, 27, 28, 29, 30, 31, 32, 34, 36]` (mean 29.7); new routing minutes `[14, 16, 18, 20, 22, 36, 38, 40, 42, 44]` (mean 29.0).
- **Axis:** horizontal 2px `#999` line at y=245 from x=60 to x=660; minutes scale 10–50, ticks every 5 with 12px `#444` labels ("10 min" ... "50 min" on the ends, bare numbers between).
- **Old row:** heading bold 12px blue `#2a78d6` "old routing (mean 29.7)" at left x=60, y=100; 7px blue `#2a78d6` dots at each value, centered on y=115.
- **New row:** heading bold 12px orange `#d95926` "new routing (mean 29.0)" at y=170; 7px orange `#d95926` dots at each value, centered on y=185.
- **Mean markers:** two vertical dashed lines (dash 4/3), blue at 29.7 and orange at 29.0, from y=90 to y=245; shared bold 12px ink `#1a5276` label "means 0.7 min apart" above them at y=82.
- **Annotation (bold 13px magenta `#d55181`, right side y=170):** "one tight cluster vs two clusters — averages can't see this".

## Stack the Deliveries into Two Staircases

**Tags:** `worked example` (blue), `empirical CDF` (green)

- **The curve** — for each minute t, plot the fraction of that group's pizzas already delivered by t
- **One step per pizza** — with 10 deliveries the staircase climbs 0.1 per arrival, from 0 up to 1
- **Read a gap** — at minute 23 the new routing sits at 5/10 = 0.50 delivered; the old still at 0.00
- **The statistic** — D is the biggest vertical gap between the staircases: here D = 0.50 at 22–24 min
- **Scan every step** — the runner-up gap is 0.40 at minute 35, where old is at 0.9 and new at 0.5

*Example (italic):* By minute 23, half the new-routing pizzas were already delivered but not a single old-routing pizza — that head start is the 0.50 gap.

**Key point:** D is always a fraction between 0 and 1: it says "at some point, the two groups disagree by 50 percentage points about how much has already happened." No formula fitting, just sort and compare.

### Visualization (canvas `c2`, 720×300)

Two empirical-CDF step curves on one axis, with a bold vertical double-headed arrow marking the maximum gap D = 0.50 at minute 23.

- **Title (bold 15px, `#1a5276`, top center):** "Fraction Delivered by Minute t: the Biggest Gap Is D = 0.50".
- **Data:** old routing sorted `[24, 26, 27, 28, 29, 30, 31, 32, 34, 36]`; new routing sorted `[14, 16, 18, 20, 22, 36, 38, 40, 42, 44]`; each curve steps up by 0.1 at each value (draw as right-continuous staircases from (10, 0) to (50, 1)).
- **Axes:** origin x=60, baseline y=250, plot width 600, plot height 190 (top y=60); x scale minutes 10–50, ticks every 5, 12px `#444` labels; y scale 0–1, gridlines `#e5e9ef` at 0.25/0.5/0.75/1.0 with 12px `#444` labels "0", "0.25", "0.5", "0.75", "1.0".
- **Curves:** old routing blue `#2a78d6` 3px staircase; new routing orange `#d95926` 3px staircase; small in-plot legend top-left, 12px, colored squares + "old routing" / "new routing".
- **D arrow:** vertical double-headed arrow, 3px magenta `#d55181`, at x = minute 23, from the old curve (fraction 0.00, y=250) to the new curve (fraction 0.50, y=155); bold 13px magenta label beside it: "D = 0.50".
- **Secondary marker:** thin dashed `#6b7280` vertical segment at minute 35 from 0.5 to 0.9 with 11px `#6b7280` label "next-biggest gap 0.40".
- **Caption (12px `#444`, bottom right):** "each step = one pizza (10 per group)".

## Is a 0.50 Gap Big Enough?

**Tags:** `rule of thumb` (green), `where it's used` (blue)

- **The bar** — at 5% significance, two equal samples of size n need D above roughly 1.36 × √(2/n)
- **Ten each** — n = 10 puts the bar at 0.61, so the observed 0.50 falls short: too few pizzas to be sure
- **More data, lower bar** — the bar drops to 0.43 at n = 20, 0.27 at n = 50, and 0.19 at n = 100
- **A month of orders** — if the 0.50 gap holds at 100 deliveries per routing, it clears 0.19 easily
- **Day job** — data scientists run K-S to spot feature drift, compare A/B outcome shapes, and vet model scores

*Example (italic):* One evening of 10+10 pizzas cannot certify the gap; the same 0.50 gap over 100+100 deliveries is overwhelming evidence.

**Key point:** The K-S bar shrinks like 1/√n, so small samples only detect huge shape differences. Report D together with the sample sizes, never D alone.

### Visualization (canvas `c3`, 720×300)

Line chart of the 5% critical value of D against sample size per group, with a dashed horizontal line at the observed D = 0.50 crossing the curve between n = 10 and n = 20.

- **Title (bold 15px, `#1a5276`, top center):** "The Significance Bar Drops as Samples Grow (5% level, equal groups)".
- **Data:** n per group `[10, 20, 50, 100, 200]`; critical D = 1.36 × √(2/n) → `[0.61, 0.43, 0.27, 0.19, 0.14]`.
- **Axes:** origin x=70, baseline y=245, plot width 570, plot height 180 (top y=65); x positions evenly spaced for the five n values (categorical spacing, NOT proportional), 12px `#444` labels "n=10", "n=20", "n=50", "n=100", "n=200"; y scale 0–0.7, gridlines `#e5e9ef` at 0.1 steps, 12px `#444` labels.
- **Curve:** green `#008300` 3px line through the five points, 5px green dots, each dot labeled with its value (bold 12px green, above the dot): "0.61", "0.43", "0.27", "0.19", "0.14".
- **Observed line:** horizontal dashed magenta `#d55181` (dash 5/4) at D = 0.50 across the plot, bold 12px magenta label "observed D = 0.50" at its left end.
- **Annotations:** bold 12px `#6b7280` "not provable at n=10" near the first point above the dashed line; bold 12px green "clearly detectable from n=20 up" near the n=50 point below the dashed line.
- **Caption (12px `#444`, bottom center):** "critical D ≈ 1.36 × √(2/n) for two equal groups".

## The Mistake: Trusting the t-test Here

**Tags:** `common mistake` (red), `t-test vs K-S` (orange)

- **t-test verdict** — on the pizza data t ≈ 0.18: means of 29.7 vs 29.0 look identical, so "ship it"
- **K-S verdict** — D = 0.50 says the shapes disagree sharply; with enough data it flags the change
- **Different questions** — the t-test asks "did the average move?"; K-S asks "did the shape change at all?"
- **K-S blind spot** — the gap test is most sensitive mid-distribution and weakest out in the tails
- **Fine print** — classic K-S assumes continuous data; heavy ties from rounding make it conservative

*Example (italic):* The routing change created three 40-minute-plus deliveries where the old routing had none, yet the t-test reported "no significant difference" — it was only ever asked about the mean.

**Common mistake:** Reading a t-test's "no significant difference" as "the distributions are the same." The t-test only checks the average; variance, gaps, and split shapes sail straight past it.

### Visualization (canvas `c4`, 720×300)

Dual panel split by a vertical dashed divider at x=360: what the t-test sees (two overlapping mean estimates) vs what K-S sees (the two staircases and the 0.50 gap), from the same 20 pizzas.

- **Title (bold 15px, `#1a5276`, top center):** "The Same 20 Pizzas Through Two Tests".
- **Left panel (t-test view):** heading bold 13px `#444` "what the t-test sees" at top; horizontal minutes axis at y=235 from x=55 to x=335, scale 20–40, ticks at 20/25/30/35/40 (12px `#444`); two mean dots with horizontal ±2×SE whisker bars: old routing blue `#2a78d6` dot at 29.7 with whiskers 27.4–32.0 drawn at y=120, new routing orange `#d95926` dot at 29.0 with whiskers 21.4–36.6 at y=175; 12px labels "old: 29.7" and "new: 29.0" beside the dots; bold 12px `#6b7280` verdict below: "means overlap — t ≈ 0.18, no difference found".
- **Right panel (K-S view):** heading bold 13px `#444` "what K-S sees" at top; miniature version of the c2 chart: axes origin x=405, baseline y=235, width 270, height 145 (top y=90), x scale 10–50, y scale 0–1; the same two 2px staircases (blue old, orange new, same data as c2); magenta `#d55181` 3px vertical double arrow at minute 23 from fraction 0.00 to 0.50 with bold 13px magenta label "D = 0.50"; bold 12px green `#008300` verdict below: "shapes disagree by 50 points".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Caption (12px `#444`, bottom center):** "one dataset, two questions: 'did the mean move?' vs 'did the shape change?'".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** four canvases `c1`–`c4`, intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays (no randomness); ECDF staircases are computed directly from the two sorted 10-value arrays. In regenerated HTML, any card links would use `.html` extensions (this page has no links).
