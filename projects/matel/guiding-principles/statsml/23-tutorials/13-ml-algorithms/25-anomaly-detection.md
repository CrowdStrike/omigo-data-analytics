# Anomaly Detection

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Anomaly Detection — Isolation Forests

**Subtitle:** An isolation forest finds weird points by slicing data with random cuts — a point sitting alone gets fenced off in two or three cuts, while a point in the crowd takes many

## One 3 A.M. Payment in a Day of Coffees

**Tags:** `core idea` (blue), `isolation forest` (green), `unsupervised` (orange)

- **The shop** — a coffee shop logs 21 card payments in one day: coffees of $4–$12.50, 7 a.m. to 8 p.m.
- **The oddball** — one payment is $52 at 3 a.m., far from every other point on both hour and amount
- **The trick** — slice the data with random cuts; a point sitting alone gets fenced off in very few cuts
- **The name** — an isolation forest builds many random trees and counts how many cuts strand each point
- **No labels** — nobody marked anything as fraud in advance; the data's own shape does the flagging

*Example (italic):* Ask the barista to circle the weird sale and they point at the 3 a.m. $52 instantly — an isolation forest mechanizes that instinct.

**Key point:** Anomaly detection scores how much each point stands apart from the rest. Isolation forests measure it by one thing only: how quickly random cuts leave a point alone.

### Visualization (canvas `c1`, 720×300)

Scatter plot of one day of card payments (hour of day vs amount) with the lone 3 a.m. outlier standing far from the cluster.

- **Title (bold 15px, `#1a5276`, top center):** "One Day of Card Payments: 21 Points, One Oddball".
- **Axes:** origin x=60, baseline y=250, x-axis width 630 (to x=690), chart height 205 (top y=45); x scale hours 0–24 with ticks every 3h labeled "0h" … "24h" (12px `#444`); y scale $0–$60 with ticks at 0, 20, 40, 60 labeled "$0", "$20", "$40", "$60"; light grid lines `#e5e9ef` at y ticks.
- **Normal points (blue `#2a78d6`, 5px dots):** (hour, $) = `[[7,4.5],[7.5,5],[8,6.5],[8,4],[8.5,7],[9,5.5],[9.5,8],[10,6],[11,9],[12,11],[12.5,12],[13,10],[14,7.5],[15,6],[16,8.5],[17,9.5],[18,11],[18.5,12.5],[19,10.5],[20,9]]`.
- **Outlier:** magenta `#d55181` 7px dot at (3, 52) with bold magenta 13px label "$52 at 3 a.m." to its right.
- **Annotation (bold green `#008300` 12px, above the cluster):** "the crowd: coffees $4–$12.50, 7 a.m.–8 p.m.".
- **Caption (12px `#444`, bottom right):** "illustrative one-day sample".

## Counting the Cuts to Fence Off a Point

**Tags:** `worked example` (blue), `random splits` (green)

- **Cut 1** — pick a random feature and value: "hour < 10.8" leaves the 3 a.m. point with few neighbors
- **Cut 2** — "amount > 22" inside that slice fences the $52 point off completely: isolated in 2 cuts
- **Typical point** — the 12:30 $12 coffee sits in the dense middle and survives about 9 cuts
- **Repeat** — build 100 trees, each with different random cuts, and average each point's cut count
- **The averages** — 3 a.m. $52 needs 2.6 cuts on average; the coffees need 8.1 to 9.2

*Example (italic):* Redo it by hand: draw any vertical line near dawn and any horizontal line above $22 — the $52 point is alone after just those two strokes.

**Key point:** Weird points isolate fast. Averaging the cut count over many random trees turns "fast" into a stable number you can rank every point by.

### Visualization (canvas `c2`, 720×300)

Dual panel: the two random cuts drawn on the scatter (left) and a bar chart of average cuts-to-isolate per point (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Two Cuts Strand the Oddball; the Crowd Takes ~9".
- **Left panel (scatter with cuts):** origin x=55, width 280, baseline y=245, chart height 185; same scales as `c1` (hours 0–24, $0–$60); the 20 blue `#2a78d6` 4px dots and the magenta `#d55181` 6px dot at (3, 52) from `c1`.
- **Cut lines:** green `#008300` dashed (dash 5/4) vertical line at hour=10.8 with bold green 12px label "cut 1: hour < 10.8"; orange `#d95926` dashed horizontal line at $22 drawn only left of the cut-1 line, bold orange 12px label "cut 2: amount > 22"; shade the isolated region (hour < 10.8, amount > 22) with `rgba(213,81,129,0.10)`.
- **Left caption (12px `#444`):** "one tree: 2 cuts isolate the $52 point".
- **Right panel (bars):** origin x=400, width 280, baseline y=245, chart height 185; y scale 0–10 with ticks 0, 5, 10; five bars with 11px labels below and bold 12px values above: "3 a.m. $52" = 2.6 (fill `rgba(213,81,129,0.55)`), "9 a.m. $5.50" = 8.1, "12:30 $12" = 9.2, "6 p.m. $11" = 8.7, "8 p.m. $9" = 8.4 (fill `rgba(42,120,214,0.45)`).
- **Right caption (12px `#444`):** "average cuts to isolate, over 100 random trees".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Turning Cut Counts into a 0-to-1 Score

**Tags:** `worked example` (blue), `rule of thumb` (green), `where it's used` (orange)

- **The formula** — score = 2^(−average cuts ÷ 5.2), where 5.2 is about the expected cuts for 21 points
- **Reading it** — near 1 means isolated fast (anomalous); near or below 0.5 means deep in the crowd
- **The oddball** — 2.6 cuts gives 2^(−2.6/5.2) = 2^(−0.5) ≈ 0.71, far above every coffee
- **The coffees** — 8.1 to 9.2 cuts give scores 0.29 to 0.34, all comfortably ordinary
- **The threshold** — a cutoff like 0.55 turns scores into flags; it sets how much lands in review
- **Where it's used** — fraud screens, sensor-fault alerts, intrusion detection, data-quality checks

*Example (italic):* Ranking the day's payments by score puts the 3 a.m. $52 first at 0.71, with the nearest coffee at 0.34 — a clean gap to draw a line through.

**Key point:** The score is relative to the data you gave it: 0.71 means "easy to fence off among these 21 points", not "fraudulent" in any absolute sense.

### Visualization (canvas `c3`, 720×300)

Horizontal ranked bar chart of anomaly scores for five payments, with a dashed review-threshold line at 0.55.

- **Title (bold 15px, `#1a5276`, top center):** "Anomaly Scores: One Clear Standout".
- **Score axis:** horizontal, score 0 at x=170 to score 1 at x=650; ticks at 0, 0.5, 1 labeled "0", "0.5", "1" (12px `#444`) along a baseline at y=255; light grid line `#e5e9ef` at 0.5.
- **Bars (20px tall, rows starting y=70, spacing 36):** left-aligned 12px `#444` labels at x=160 (right-aligned): "3 a.m. $52" = 0.71 (fill `rgba(213,81,129,0.55)`), "9 a.m. $5.50" = 0.34, "8 p.m. $9" = 0.33, "6 p.m. $11" = 0.31, "12:30 $12" = 0.29 (fill `rgba(42,120,214,0.45)`); bold 12px score value at each bar's end (magenta for the first, blue for the rest).
- **Threshold:** orange `#d95926` dashed (dash 5/4) vertical line at score 0.55 from y=50 to y=250, bold orange 12px label "review threshold 0.55" at the top.
- **Caption (12px `#444`, bottom center):** "score = 2^(−avg cuts / 5.2); 5.2 ≈ expected cuts for 21 random points".

## Flagged Is Not the Same as Fraud

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Flag ≠ fraud** — the forest only says "this point was easy to fence off"; it never says why
- **New data** — next month adds three Friday catering orders near $45; all three get flagged too
- **Small clusters** — a handful of repeats still isolates fast; rare-and-legit scores like rare-and-bad
- **Masking** — if fraud becomes common, a dense fraud cluster stops looking anomalous at all
- **Threshold games** — dropping the cutoff from 0.55 to 0.45 can multiply the review queue overnight

*Example (italic):* The shop's owner nearly blocked the catering account — three legitimate $44–$47 evening orders scored almost as high as the 3 a.m. mystery charge.

**Common mistake:** Treating every flagged point as bad. High score means rare and isolated in this dataset — the model measures isolation, not intent, so flags need human review or labels downstream.

### Visualization (canvas `c4`, 720×300)

The `c1` scatter one month later: the 3 a.m. outlier plus a small circled cluster of legitimate catering orders that also gets flagged.

- **Title (bold 15px, `#1a5276`, top center):** "Next Month: Two Flags, Only One Problem".
- **Axes:** identical to `c1` — origin x=60, baseline y=250, width 630, height 205, hours 0–24, $0–$60, same ticks and grid.
- **Normal points:** the same 20 blue `#2a78d6` 4px dots as `c1`.
- **Outlier:** magenta `#d55181` 7px dot at (3, 52), bold magenta 12px label "3 a.m. $52 — worth a call".
- **Catering cluster:** orange `#d95926` 6px dots at `[[19.5,44],[20,45],[20.5,47]]` enclosed by a dashed orange ellipse (center hour 20, $45.5; radii ≈ 1.6h × $6), bold orange 12px label "Friday catering: flagged, but routine".
- **Annotation (bold ink `#1a5276` 12px, upper middle):** "the forest sees isolation, not intent".
- **Caption (12px `#444`, bottom right):** "illustrative; same axes as the first chart".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
