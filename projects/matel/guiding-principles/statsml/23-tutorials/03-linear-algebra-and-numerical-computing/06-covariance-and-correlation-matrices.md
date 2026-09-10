# Covariance & Correlation Matrices

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Covariance & Correlation Matrices

**Subtitle:** Every pairwise "do these two move together?" answer, packed into one square table — the single object that PCA decomposes and a Kalman filter updates

## Three Numbers a Coffee Shop Logs Every Day

**Tags:** `core idea` (blue), `pairwise co-movement` (green), `one object` (orange)

- **The log** — each day the shop records temperature, iced drinks sold, and hot drinks sold
- **Move together** — hotter days sell more iced drinks: (temp, iced) rises and falls as one pair
- **Move opposite** — hot drinks drop on hot days: the (temp, hot) pair pulls in opposite ways
- **Covariance** — one signed number per pair: positive = move together, negative = move opposite
- **The matrix** — all pairwise covariances stacked into one square table: one object, every pair

*Example (italic):* On the five days logged, the 35 °C day sold 55 iced and only 25 hot — both pairs behave exactly as the matrix will say.

**Key point:** A covariance matrix is nothing exotic — it is every pairwise "do these two move together?" answer arranged in one square grid.

### Visualization (canvas `c1`, 720×300)

Three side-by-side scatter panels showing the three variable pairs from the same five days: one rising pair, two falling pairs.

- **Title (bold 15px, `#1a5276`, top center):** "Five Days at the Coffee Shop: Every Pair, Side by Side".
- **Data (5 days):** temp `[20, 30, 25, 35, 15]` °C; iced `[30, 55, 40, 55, 20]` cups; hot `[55, 35, 45, 25, 40]` cups.
- **Panels:** three equal panels, axis origins x=55, x=290, x=525, each width 170, baseline y=240, chart height 160; panel headings bold 12px `#1a5276` centered above each: "temp vs iced", "temp vs hot", "iced vs hot".
- **Left panel:** x = temp (scale 10–40), y = iced (scale 10–70); five 6px dots in blue `#2a78d6`; green `#008300` bold 12px annotation "move together (+)".
- **Middle panel:** x = temp (10–40), y = hot (10–70); five 6px dots in orange `#d95926`; orange bold 12px annotation "move opposite (−)".
- **Right panel:** x = iced (10–70), y = hot (10–70); five 6px dots in magenta `#d55181`; magenta bold 12px annotation "move opposite (−)".
- **Axis labels:** 11px `#6b7280` min/max tick labels only on each axis; no gridlines beyond a light `#e5e9ef` frame.
- **Caption (12px `#444`, bottom center):** "same five days, three pairings — one sign per pair".

## Building the 3×3 Table by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Means first** — temp averages 25 °C, iced 40 cups, hot 40 cups across the five days
- **Deviations** — subtract each mean: temp deviations are −5, +5, 0, +10, −10
- **Multiply pairwise** — temp × iced deviation products: 50, 75, 0, 150, 200 — all positive
- **Average them** — cov(temp, iced) = 475 / 5 = 95; a negative average would flag "opposite"
- **Diagonal** — a variable paired with itself gives its variance: 50, 190, 100 down the diagonal
- **Symmetric** — cov(iced, temp) is the same 95, so the table mirrors across the diagonal

*Example (italic):* cov(temp, hot) works out to −250 / 5 = −50 — the minus sign is the "opposite" flag, computed from five rows of arithmetic.

**Key point:** Each cell is just "average of (deviation × deviation)" — five rows of arithmetic per pair fills the whole 3×3 table.

### Visualization (canvas `c2`, 720×300)

Left half: the hand computation of cov(temp, iced) as a drawn table. Right half: the finished 3×3 covariance matrix as a colored grid.

- **Title (bold 15px, `#1a5276`, top center):** "One Cell by Hand, Then the Whole Matrix".
- **Left table (from x=45, y=60, five data rows, row height 30):** header row bold 12px `#1a5276`: "day | temp dev | iced dev | product"; rows 12px `#2c3e50`: `1 | −5 | −10 | 50`, `2 | +5 | +15 | 75`, `3 | 0 | 0 | 0`, `4 | +10 | +15 | 150`, `5 | −10 | −20 | 200`; column x-positions 60, 130, 210, 290; thin `#e5e9ef` row separator lines.
- **Sum line (bold 13px, green `#008300`, below table):** "sum 475 ÷ 5 = 95".
- **Right matrix grid (3×3, cell 78×52, top-left at x=445, y=78):** row/column labels bold 12px `#1a5276` "temp / iced / hot" on top and left; cell values bold 14px centered: row1 `50, 95, −50`; row2 `95, 190, −90`; row3 `−50, −90, 100`; positive cells fill `rgba(42,120,214,0.18)`, negative cells fill `rgba(217,89,38,0.18)`, diagonal cells fill `rgba(26,82,118,0.28)` with white value text; 1px `#e5e9ef` cell borders.
- **Annotation (bold 12px, blue `#2a78d6`, arrowed from sum line to the row1-col2 cell):** "this one cell".
- **Caption (12px `#444`, bottom center):** "diagonal = variances; off-diagonal = pairwise covariances; matrix is symmetric".

## Why 95 Means Nothing on Its Own

**Tags:** `common mistake` (red), `correlation` (blue), `unit-free` (green)

- **Units rule it** — relog temperature in °F and cov(temp, iced) jumps from 95 to 171 — same days
- **The fix** — divide each cell by both standard deviations: sd(temp)=7.1, sd(iced)=13.8, sd(hot)=10
- **Correlation** — 95 / (7.1 × 13.8) ≈ 0.97; every rescaled value lands between −1 and +1
- **Read it** — 0.97 = near-lockstep, −0.71 and −0.65 = strong opposite moves, diagonal = exactly 1
- **Comparable** — correlation cells compare across pairs and datasets; raw covariance cells cannot

*Example (italic):* In °F the covariance is 171, in °C it is 95, yet the correlation is 0.97 either way — the relationship never changed.

**Common mistake:** Reading a big covariance as a strong relationship. Its size is mostly units; only correlation's −1 to +1 scale measures strength.

### Visualization (canvas `c3`, 720×300)

Two 3×3 heatmap grids side by side: the covariance matrix (mixed magnitudes) and the correlation matrix (everything in −1..1), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same Data, Two Matrices: Covariance vs Correlation".
- **Left grid (covariance, cell 78×52, top-left at x=80, y=80):** labels "temp / iced / hot" bold 12px `#1a5276` top and left; values bold 14px: row1 `50, 95, −50`; row2 `95, 190, −90`; row3 `−50, −90, 100`; positive fill `rgba(42,120,214,0.18)`, negative fill `rgba(217,89,38,0.18)`, diagonal `rgba(26,82,118,0.28)` with white text; heading bold 13px `#444` above: "covariance (units: °C·cups, cups², ...)".
- **Right grid (correlation, same cell size, top-left at x=440, y=80):** same labels; values bold 14px: row1 `1.00, 0.97, −0.71`; row2 `0.97, 1.00, −0.65`; row3 `−0.71, −0.65, 1.00`; positive fill `rgba(0,131,0,0.18)`, negative fill `rgba(213,81,129,0.18)`, diagonal `rgba(0,131,0,0.35)` with white text; heading bold 13px `#444` above: "correlation (unit-free, −1 to +1)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=42 to h-14.
- **Takeaway (bold 13px green `#008300`, bottom center):** "swap °C for °F: left matrix changes, right matrix does not".

## One Object That PCA and Kalman Both Eat

**Tags:** `where it's used` (blue), `PCA` (orange), `Kalman filter` (green)

- **PCA input** — PCA eigendecomposes this matrix; the eigenvectors become the new axes
- **One direction** — for (temp, iced) the eigenvalues are ≈238 and ≈2: one line carries ~99% of spread
- **Kalman input** — a Kalman filter carries a covariance matrix as its running "how unsure am I?" state
- **Update step** — each new measurement shrinks the uncertainty ellipse; the matrix is that ellipse
- **Everywhere** — portfolio risk, GPS tracking, and sensor fusion all pass this same object around

*Example (italic):* A phone's GPS blends satellite and accelerometer readings by updating one small covariance matrix many times per second.

**Key point:** The covariance matrix is the standard handoff format for "spread and co-movement" — PCA reads it once, a Kalman filter updates it constantly.

### Visualization (canvas `c4`, 720×300)

Left panel: the (temp, iced) scatter with its principal axis drawn through the mean. Right panel: a Kalman uncertainty ellipse before and after a measurement. Dashed divider at x=380.

- **Title (bold 15px, `#1a5276`, top center):** "Two Consumers of the Same Matrix: PCA and Kalman".
- **Left panel (PCA):** axis origin x=55, width 250, baseline y=245, chart height 175; x = temp (scale 10–40), y = iced (scale 10–70); five 6px blue `#2a78d6` dots at (20,30), (30,55), (25,40), (35,55), (15,20); mean point (25,40) as an 8px ink `#1a5276` dot; violet `#4a3aa7` 3px line with arrowheads through the mean from data point (17, 24.2) to (33, 55.8) — the eigenvector direction (1, 1.98); violet bold 12px annotation, two lines: "eigenvector = main axis," / "~99% of the spread".
- **Left caption (12px `#444`):** "PCA: eigen-decompose the 2×2 block [[50, 95], [95, 190]]".
- **Right panel (Kalman):** dashed ink `#1a5276` ellipse (dash 5/4), center (545, 150), radii rx=85, ry=55, with 5px ink center dot and 12px `#444` label "before measurement" above; solid green `#008300` 2px ellipse, center (560, 158), radii rx=34, ry=22, with 5px green center dot and green bold 12px label "after measurement" below; green bold 13px annotation near the bottom: "the covariance IS the uncertainty ellipse".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=380 from y=40 to h-12.
- **Caption (12px `#444`, bottom right):** "Kalman: each reading shrinks the matrix (illustrative ellipse sizes)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
