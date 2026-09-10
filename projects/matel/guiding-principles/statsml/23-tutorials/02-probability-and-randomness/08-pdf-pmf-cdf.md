# PDF, PMF, CDF

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** PDF, PMF, CDF

**Subtitle:** Three faces of one distribution — bars for countable outcomes (PMF), a curve whose area carries the probability (PDF), and a running total that answers every "at most" question (CDF)

## Counting Cups: The PMF

**Tags:** `core idea` (blue), `discrete outcomes` (green), `worked example` (orange)

- **The café** — Beanline Café logs 100 morning orders; every order buys 1 to 5 cups of coffee
- **The counts** — 45 orders buy 1 cup, 30 buy 2, 15 buy 3, 7 buy 4, and 3 buy 5 — total 100
- **The PMF** — divide by 100: P(1)=0.45, P(2)=0.30, P(3)=0.15, P(4)=0.07, P(5)=0.03
- **Read it directly** — the bar at 2 cups IS a probability: P(exactly 2 cups) = 0.30
- **Must sum to 1** — 0.45+0.30+0.15+0.07+0.03 = 1.00; every order lands on exactly one bar

*Example (italic):* Pick a random morning order: there is a 30% chance it is exactly 2 cups — read straight off the height of the second bar.

**Key point:** A PMF lists countable outcomes and gives each its own probability bar. Heights are probabilities you can read directly, and the bars always sum to 1.

### Visualization (canvas `c1`, 720×300)

Single bar chart of the cups-per-order PMF: five bars whose heights are probabilities.

- **Title (bold 15px, `#1a5276`, top center):** "Cups per Order at Beanline Café: the PMF (100 orders)".
- **Data:** cups `[1, 2, 3, 4, 5]`, probabilities `[0.45, 0.30, 0.15, 0.07, 0.03]`.
- **Axes:** origin x=70, plot width 580, baseline y=240, chart height 180; y scale 0–0.5 with ticks 0, 0.1, 0.2, 0.3, 0.4, 0.5 (12px `#444`, light `#e5e9ef` gridlines); x labels "1 cup" … "5 cups" 12px `#444` below baseline.
- **Bars:** 70px wide, evenly spaced, fill `rgba(42,120,214,0.45)` with 2px `#2a78d6` top edge; bold 12px `#2a78d6` value label above each bar ("0.45", "0.30", "0.15", "0.07", "0.03").
- **Annotation (bold 13px green `#008300`, upper right):** "bars sum to 1.00 — each height IS a probability".
- **Caption (12px `#444`, bottom right):** "discrete outcomes: read probability straight off the bar".

## Measuring Wait Time: Why the PDF Is a Density

**Tags:** `core idea` (blue), `continuous data` (orange), `area = probability` (green)

- **New question** — the same café measures wait time in minutes: 3.1, 2.47, 5.082 — any decimal
- **No bars fit** — P(wait = exactly 3.000000 min) is 0; a single point has zero width
- **Density instead** — the PDF is a curve; probability lives in the AREA under it, not the height
- **The shape** — waits rise to a peak density of 0.25 at 2 minutes, then taper to zero by 8
- **Shaded slice** — P(2 ≤ wait ≤ 4) = area from 2 to 4 ≈ 0.42, so about 42% of customers

*Example (italic):* Ask "what fraction wait between 2 and 4 minutes?" and shade that slice of the curve — the shaded area is about 0.42.

**Key point:** For continuous measurements only ranges have probability. The PDF's height is a density; the area under the curve over a range is the probability of that range.

### Visualization (canvas `c2`, 720×300)

One PDF curve for wait time with the 2–4 minute slice shaded and a zero-probability point marked.

- **Title (bold 15px, `#1a5276`, top center):** "Wait Time PDF: Probability Is the Area, Not the Height".
- **Data (piecewise-linear density, hardcoded):** x `[0, 0.5, 1, 1.5, 2, 3, 4, 5, 6, 7, 8]` minutes, density `[0, 0.0625, 0.125, 0.1875, 0.25, 0.208, 0.167, 0.125, 0.083, 0.042, 0]` (triangle rising to 0.25 at 2, falling to 0 at 8; total area = 1).
- **Axes:** origin x=60, plot width 600, baseline y=240, chart height 180; y scale 0–0.30 with ticks 0, 0.1, 0.2, 0.3; x ticks 0–8 minutes, labels 12px `#444`.
- **Curve:** blue `#2a78d6` 3px line through the data points.
- **Shaded region:** under the curve from x=2 to x=4, fill `rgba(0,131,0,0.25)`; bold 13px green `#008300` label inside the shade: "area ≈ 0.42".
- **Zero-point marker:** dashed magenta `#d55181` (dash 4/3) vertical line at x=3 from baseline to the curve; bold 12px magenta annotation, two lines: "P(exactly 3.000 min) = 0" / "zero width → zero area".
- **Caption (12px `#444`, bottom right):** "piecewise-linear density (illustrative); total area under the curve = 1".

## The CDF: One Curve for Every "At Most" Question

**Tags:** `core idea` (blue), `worked example` (orange), `where it's used` (green)

- **Running total** — the CDF F(x) answers a single question: what is P(value ≤ x)?
- **Cups version** — stack the PMF bars: F(1)=0.45, F(2)=0.75, F(3)=0.90, F(4)=0.97, F(5)=1.00
- **Staircase** — a discrete CDF jumps at each outcome; each jump equals that outcome's PMF bar
- **Wait version** — the continuous CDF climbs smoothly: F(4 min) = 0.67 of customers are done
- **Free median** — where the smooth curve crosses 0.5 is the median wait: about 3.1 minutes
- **Always climbs** — every CDF starts at 0, never decreases, and ends at exactly 1

*Example (italic):* "90% of orders are 3 cups or fewer" and "67% of customers wait 4 minutes or less" are both single reads off a CDF.

**Key point:** The CDF is the shared face of PMF and PDF — one non-decreasing curve from 0 to 1 that answers every "at most" question for counts and measurements alike.

### Visualization (canvas `c3`, 720×300)

Dual-panel CDF: staircase CDF of cups per order (left) and smooth CDF of wait time (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Two CDFs: a Staircase for Counts, a Smooth Climb for Time".
- **Left panel (staircase):** axis origin x=55, plot width 280, baseline y=245, chart height 185; y scale 0–1 with ticks 0, 0.5, 1; x positions cups 1–5. Data: F `[0.45, 0.75, 0.90, 0.97, 1.00]`. Blue `#2a78d6` 3px horizontal steps (each step runs from its cup value to the next), 5px filled dot at the left end of each step; dashed `#bdc3c7` guide from y=0.90 across to the step at 3 cups; bold 12px blue annotation "F(3) = 0.90: 90% are ≤ 3 cups"; caption 12px `#444` "cups per order (jumps = PMF bars)".
- **Right panel (smooth):** axis origin x=400, plot width 280, same baseline/height; x scale 0–8 minutes; data points x `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, F `[0, 0.06, 0.25, 0.48, 0.67, 0.81, 0.92, 0.98, 1.00]`; green `#008300` 3px curve. Dashed `#bdc3c7` guides at F=0.67 meeting x=4 (bold 12px green label "F(4) = 0.67") and at F=0.5 meeting x≈3.1 (bold 12px orange `#d95926` label "median ≈ 3.1 min"); caption 12px `#444` "wait time (slope = PDF height)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Height-Above-1 Confusion

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The trap** — reading PDF height as probability; a density can sit far above 1 and be fine
- **Espresso shots** — pull times cluster tightly between 0.40 and 0.50 minutes, peaking at 0.45
- **Height 20** — squeezing a total area of 1 into a 0.1-minute-wide range pushes the peak to 20
- **Change units** — the same shots in seconds (24–30 s) peak at density 0.33 — same distribution
- **Only area counts** — peaks 20 and 0.33 describe identical shots; both areas equal exactly 1

*Example (italic):* A density of 20 at 0.45 minutes does not mean "2,000% probability" — it means the probability is packed into a very narrow range.

**Common mistake:** Treating the y-axis of a PDF as probability. Height depends on the measurement units and can exceed 1; only areas are probabilities, and the total area is always 1.

### Visualization (canvas `c4`, 720×300)

Dual-panel PDF of the same espresso pull times in two units — minutes (peak 20) vs seconds (peak 0.33) — split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same Shots, Two Units: PDF Height Changes, Area Stays 1".
- **Left panel (minutes):** axis origin x=55, plot width 280, baseline y=235, chart height 175; x scale 0.30–0.60 min with tick labels "0.40", "0.45", "0.50"; y scale 0–22 with ticks 0, 10, 20. Triangle density through `(0.40, 0)`, `(0.45, 20)`, `(0.50, 0)`, blue `#2a78d6` 3px line, fill `rgba(42,120,214,0.25)`; dashed magenta `#d55181` horizontal line at y=1 labeled bold 12px magenta "density = 1"; bold 13px blue annotation "peak = 20, far above 1"; caption 12px `#444` "area = ½ × 0.1 × 20 = 1".
- **Right panel (seconds):** axis origin x=400, plot width 280, same baseline/height; x scale 18–36 s with tick labels "24", "27", "30"; y scale 0–0.4 with ticks 0, 0.2, 0.4. Triangle density through `(24, 0)`, `(27, 0.333)`, `(30, 0)`, green `#008300` 3px line, fill `rgba(0,131,0,0.2)`; bold 13px green annotation "same shots: peak = 0.33"; caption 12px `#444` "area = ½ × 6 × 0.333 = 1".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Takeaway (bold 13px magenta `#d55181`, centered at y=290):** "height follows the units — the area is 1 in both panels".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
