# The Curse of Dimensionality

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table layout text left 50% / canvas right 50%)
**HTML title tag:** The Curse of Dimensionality

**Subtitle:** Add enough columns to a table and every row ends up far from every other row — "nearest" stops meaning anything

## A Customer Table That Keeps Growing Columns

Tags: `core idea` (blue), `high dimensions` (orange), `running example` (green)

- **The setup** — a shop finds "similar customers" by distance on the columns of its table
- **2 columns** — spend and visits: customers form visible groups, neighbors make sense
- **The upgrade** — someone adds 98 more columns: page views, dwell times, one per product...
- **The surprise** — with 100 mostly-junk columns, everyone becomes roughly equally far apart
- **The curse** — each new column adds a bit of difference; 100 small bits drown the signal

*Example (italic):* Ten friends on a bench sit close; scatter the same ten across a city and nobody has a neighbor.

**Key point:** **Curse of dimensionality:** as columns grow, data gets sparse and distances bunch together — the geometry that similarity search relies on quietly dissolves.

### Visualization (canvas `c1`, 720×300)

Two-panel diagram: the same 10 customers on a 1-D line vs scattered in a 2-D square, split by a dashed vertical divider at x=340.

- **Title (bold 15px `#1a5276`, center):** "The Same 10 Customers: Cozy in 1 Column, Lonely in 2".
- **Divider:** vertical dashed light-gray `#bdc3c7` line (dash 4/3) from y=40 to h−15.
- **Left panel:** horizontal gray `#999` line (y=150, from x=55 to x=300) with 10 blue `#2a78d6` 6px dots at relative positions `[0.05, 0.14, 0.25, 0.33, 0.44, 0.55, 0.63, 0.75, 0.86, 0.95]`. Caption below (12px `#444`): "1 column: spend". Annotation above (bold 13px green `#008300`): "everyone has a close neighbor".
- **Right panel:** 195×195 gray-outlined square at (420, 55) with the same 10 blue 6px dots at relative (x, y) positions `[0.08,0.85], [0.25,0.30], [0.42,0.62], [0.60,0.12], [0.78,0.44], [0.15,0.55], [0.90,0.78], [0.52,0.90], [0.70,0.68], [0.33,0.08]`. Caption above the square: "2 columns: spend × visits". Annotation (bold 13px orange `#d95926`, two lines at bottom): "same 10 rows, mostly empty space —" / "100 columns: everyone is alone".

## Three Customers, Two Column Counts, One Collapse

Tags: `worked example` (green), `arithmetic` (blue)

- **The cast** — B is A's twin (differs 0.1 per column); C differs 0.9 on two columns
- **With 2 columns** — dist(A,B) = √(0.1²+0.1²) = 0.14; dist(A,C) = √(0.9²+0.9²) = 1.27
- **Clear verdict** — the stranger is 9× farther than the twin; "nearest" works
- **With 100 columns** — B's 100 tiny gaps: √(100 × 0.01) = 1.00
- **C barely changes** — √(2 × 0.81 + 98 × 0.01) = √2.60 = 1.61
- **Verdict gone** — the stranger is now only 1.6× farther; one noisy column could flip it

*Example (italic):* Every number above is a square, a sum, and a square root — redo it on paper in a minute.

**Key point:** **The mechanism:** 98 irrelevant columns each add a little distance to the twin but almost nothing to the stranger's lead — contrast shrinks from 9× to 1.6×.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: distance from A for the twin and the stranger, at 2 vs 100 columns.

- **Title (bold 15px `#1a5276`, center):** "Distance From A: the Twin's Advantage Collapses".
- **Groups:** "2 columns" and "100 columns". Twin B distances `[0.14, 1.00]` in green `#008300`; stranger C distances `[1.27, 1.61]` in violet `#4a3aa7`. Bar width 70, 6px inner gap; value labels (bold 12px `#222`) above bars; group labels bold 13px below. Padding: top 55, bottom 60, left 65, right 180.
- **Y-axis:** 0 to 1.8 with gridlines (`#e5e9ef`) and labels at 0.0/0.5/1.0/1.5 (12px `#666`).
- **Ratio callouts (bold 13px, top of each group):** "stranger 9× farther" green over group 1; "only 1.6× farther" magenta `#d55181` over group 2.
- **Legend (right, x = w−168):** green swatch "B — the twin", violet swatch "C — the stranger" (12px `#222`).
- **Annotation (bold 12px magenta, under legend, two lines):** "98 junk columns almost" / "erase a 9× difference".

## Space Empties Out Faster Than Data Can Fill It

Tags: `sparsity` (blue), `rule of thumb` (green)

- **Coverage goal** — keep 10 example customers along every column's range
- **1 column** — 10 customers cover the spend line comfortably
- **2 columns** — the spend × visits square needs 10 × 10 = 100 customers
- **3 columns** — the cube needs 1,000; by 10 columns you need 10 billion
- **No dataset keeps up** — so in high dimensions, every customer sits in empty space

*Example (italic):* A 100-column table would need more rows than there are atoms in the universe to stay "dense".

**Key point:** **Rule of thumb:** data requirements grow multiplicatively with columns, not additively — that is why "just add more features" eventually makes neighbors meaningless.

### Visualization (canvas `c3`, 720×300)

Bar chart on a log scale: rows needed to keep 10-per-column coverage as columns grow.

- **Title (bold 15px `#1a5276`, center):** "Customers Needed for 10-per-Column Coverage (log scale)".
- **Bars:** x labels (number of columns) `1, 2, 3, 6, 10`; bar heights are log10 of rows needed `[1, 2, 3, 6, 10]` on a scale max of 11; value labels above bars (bold 12px `#222`): "10", "100", "1,000", "1 million", "10 billion". First three bars blue `#2a78d6`, last two orange `#d95926`. Bar width 70, evenly spaced. Padding: top 55, bottom 60, left 75, right 40.
- **Y-axis:** gridlines and labels "10^1", "10^3", "10^6", "10^10" (12px `#666`).
- **X caption (12px `#444`):** "number of columns".
- **Annotation (bold 14px orange, top center):** "each bar is on a log scale — the true growth is a wall, not a slope".

## Where It Bites: kNN, Clustering, and Anomaly Scores

Tags: `distance breakdown` (red), `where it's used` (blue), `common mistake` (orange)

- **Distance histograms** — in 2D, pairwise distances spread wide; in 100D they pile in a spike
- **kNN breaks** — when nearest and farthest differ by 10%, the "vote" is basically random rows
- **k-means breaks** — clusters need dense clumps; sparse space has none to find
- **Anomalies vanish** — everything is far from everything, so nothing stands out as unusual
- **The mistake** — blaming the model and tuning k, when the distance itself carries no signal
- **The fixes** — drop irrelevant columns, or compress with PCA before any distance-based method

*Example (italic):* A fraud team added 300 features "for safety" and watched their kNN detector decay to coin flips.

**Key point:** **Diagnosis first:** plot the histogram of pairwise distances. A narrow spike means no distance-based method will work until you shrink the columns.

### Visualization (canvas `c4`, 720×300)

Overlaid histograms of pairwise distances: wide 2-D spread vs a narrow 100-D spike.

- **Title (bold 15px `#1a5276`, center):** "Histogram of All Pairwise Distances (scaled, illustrative)".
- **Bins:** 20 bins over relative distance 0..2 (distance ÷ average distance); y scale max 60. Padding: top 55, bottom 60, left 65, right 175.
- **2-column series (blue `#2a78d6`, alpha 0.45):** counts `[2, 5, 9, 13, 16, 17, 16, 14, 12, 10, 8, 7, 6, 5, 4, 3, 2, 2, 1, 1]`.
- **100-column series (orange `#d95926`, alpha 0.6):** counts `[0, 0, 0, 0, 0, 0, 0, 1, 4, 18, 42, 55, 38, 12, 2, 0, 0, 0, 0, 0]`.
- **X-axis:** tick labels "0", "0.5", "1", "1.5", "2" (12px `#222`); caption "distance ÷ average distance" (12px `#444`).
- **Annotations (bold 13px):** orange, two lines near the spike: "100 columns: a spike —" / "nearest ≈ farthest"; blue at mid-left: '2 columns: wide spread, "near" is real'.
- **Legend (right, x = w−162):** swatch `rgba(42,120,214,0.6)` "2 columns"; swatch `rgba(217,89,38,0.7)` "100 columns" (12px `#222`).

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, bottom border 2px solid `#2980b9`), `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: `.text-col` (50%) text, `.viz-col` (50%) canvas 720×300.
- **Text cell structure:** `.tags` row of pill spans first, then `<ul>` of one-line bullets each opening with `<b>` term (bold terms colored `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. blue = `rgba(26,82,118,0.12)`/`#1a5276`; green = `rgba(39,174,96,0.15)`/`#27ae60`; red = `rgba(231,76,60,0.12)`/`#e74c3c`; orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 logical; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
