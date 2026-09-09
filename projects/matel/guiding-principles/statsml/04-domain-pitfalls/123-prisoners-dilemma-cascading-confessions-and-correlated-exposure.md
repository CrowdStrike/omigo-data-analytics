# Prisoner's Dilemma / Cascading Confessions & Correlated Exposure

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** 123. Prisoner's Dilemma / Cascading Confessions & Correlated Exposure

**Subtitle:** One confession exposes an entire network — records modeled as independent are actually totally correlated, and the data only describes those who talked.

Note: all canvases on this page are declared `width="720" height="300"` in HTML, but each chart's JS resizes its own canvas to an effective 720×200 (backing store = rendered width × dpr, CSS 720px × 200px).

## One Capture Exposes Entire Network

- Single arrest cascades through fraud rings, insider threats
- Network topology revealed from one cooperating node
- Risk model assumes independence — reality is total correlation

**Example:** One money mule caught with phone records — leads to 14 accomplices within 72 hours, entire ring collapses.

### Visualization (canvas `c1`, 720×200)

Tree diagram of a network cascade from one captured node.

- **Title (17px, `#1a5276`, at 230,20):** "Network Cascade from Single Capture".
- **Nodes (15px-radius circles, `#333` outline):** root (360,50) red `#e74c3c`; children (250,90) and (470,90), grandchildren (180,140), (320,140), (420,140), (530,140) — all non-root nodes yellow-orange `#f39c12`. Blue `#2980b9` edges (width 2) connect root→children and each child→two grandchildren.
- **Labels (14px):** red "Captured" at (380,55); orange "Exposed (72hrs)" at (380,145).

## Cooperator-Defector Data Asymmetry

- Confessors generate volumes of data; silent ones remain invisible
- Database overrepresents cooperative criminals
- Risk profiles built only on caught/talking subset

**Example:** 80% of fraud intelligence comes from 20% who cooperated — remaining 80% of network has no data profile.

### Visualization (canvas `c2`, 720×200)

Two horizontal bars contrasting data volume by source type.

- **Title (17px, `#1a5276`, at 250,25):** "Intelligence Data by Source Type".
- **Bars (x=100, height 45):** blue `#2980b9` 400 wide at y=60, white label "Cooperators (20% of network) = 80% of data"; red `#e74c3c` 100 wide at y=120, white label "Silent (80%)" with `#555` 15px continuation "= 20% of data" beside the bar.
- **Annotation (15px, red, at 350,175):** "Massive blind spot in risk models".

## Plea Bargains Bias Severity Data

- Cooperators get reduced charges — appear less severe
- Sentencing data underestimates true offense severity
- Recidivism models trained on deflated severity scores

**Example:** Mastermind pleads to misdemeanor, gets "low severity" label. Mule refuses plea, gets felony — model thinks mule is worse.

### Visualization (canvas `c3`, 720×200)

Paired horizontal bars per role: true severity vs recorded severity (inverted).

- **Title (17px, `#1a5276`, at 230,25):** "True Severity vs Recorded Severity".
- **Rows (role labels `#555` at x=10, bars start x=140, height 18, spaced 50px from y=55):** Mastermind — true 95 (green `#27ae60`, width = value×3), recorded 25 (red `#e74c3c`, 22px below); Lieutenant — true 70, recorded 45; Mule — true 30, recorded 80.
- **Legend (x=500):** green 15×15 swatch "True", red swatch "Recorded" (`#333` text).

## Network Topology Invisible Until First Domino

- Connections unobservable pre-confession
- Graph structure only revealed retroactively
- Prevention impossible without visibility

**Example:** Five years of undetected fraud — first arrest reveals a 30-node network that was invisible to all monitoring systems.

### Visualization (canvas `c4`, 720×200)

Before/after split: invisible dashed ghost nodes vs a revealed hub-and-spoke network.

- **Title (17px, `#1a5276`, at 260,20):** "Before vs After First Arrest".
- **Before (left):** gray `#bbb` 15px label "Before: 0 nodes visible" at (80,50); six dashed-outline circles (`#ccc`, dash 3/3, radius 10) at x = 100 + i×40, y alternating 90/120.
- **After (right):** red `#e74c3c` label "After: 30 nodes revealed" at (400,50); hub-and-spoke — center dot (radius 9, dark red `#c0392b`) at (500,120) with 12 red spokes radiating to 6px-radius red nodes on an ellipse (rx=60, ry=55).

## Correlated Risk — Simultaneous Exposure

- One exposure = entire ring simultaneously at risk
- Portfolio theory fails — no diversification within ring
- Loss distribution has extreme tail from correlation

**Example:** Bank models each account independently — one SAR triggers review of all 12 linked accounts same day, $4.2M total exposure.

### Visualization (canvas `c5`, 720×200)

Two loss-distribution curves: modeled bell curve vs actual heavy-tailed curve.

- **Title (17px, `#1a5276`, at 200,25):** "Loss Distribution: Independent vs Correlated".
- **Independent curve (blue `#2980b9`, width 3):** Gaussian over 50 points, x = 80 + i×12, y = 180 − exp(−((i−25)/10)²)×130 — symmetric bell centered mid-plot.
- **Correlated curve (red `#e74c3c`):** shifted-left Gaussian y = 180 − exp(−((i−15)/8)²)×100 with an added fat right tail (for i > 35, y decreases by (i−35)×4, clamped at y=40).
- **Labels:** blue "Independent (modeled)" at (420,160); red "Correlated (actual)" at (420,185).

## Investigation Bias Toward Bottom of Chain

- Mules caught at point of contact; masterminds insulated
- Enforcement data skewed toward low-level operatives
- Risk models profile the visible, miss the dangerous

**Example:** 90% of arrests are mules/runners. Organizers rarely appear in data. Models trained on arrests miss the actual threat.

### Visualization (canvas `c6`, 720×200)

Horizontal bar chart of arrest share by chain level.

- **Title (17px, `#1a5276`, at 280,25):** "Arrests by Chain Level".
- **Rows (labels `#555` at x=10, bars start x=180, height 30, width = pct×7, spaced 38px from y=50):** Mules/Runners 65% red `#e74c3c`; Mid-level 22% yellow-orange `#f39c12`; Lieutenants 10% blue `#2980b9`; Organizers 3% dark blue `#1a5276`. Percent labels (`#333`) to the right of each bar.

## Retroactive Reclassification

- "Independent events" become "coordinated ring" after confession
- Historical data invalidated retroactively
- Model performance metrics are fiction (they were never independent)

**Example:** 15 "unrelated" fraud cases reclassified as one ring — the model's 15 correct independent detections were actually 1 correlated event.

### Visualization (canvas `c7`, 720×200)

Before/after bar comparison of detection counts.

- **Title (17px, `#1a5276`, at 170,25):** "Detection Count: Before vs After Reclassification".
- **Bars (x=150, height 50):** blue `#2980b9` 300 wide at y=60, white label "Before: 15 independent detections"; red `#e74c3c` 20 wide at y=130 with `#333` label "After: 1 correlated event" beside it.
- **Caption (15px, `#555`, at 250,195):** "Model accuracy drops from 93% to 47% overnight".

## RICO Cascade — Exponential Evidence Production

- Each confession produces evidence against others
- Evidence grows exponentially with each cooperator
- Domino dynamics make partial exposure impossible

**Example:** First confession names 3. Each of those names 3 more. Within 4 rounds: 1 + 3 + 9 + 27 = 40 implicated from single arrest.

### Visualization (canvas `c8`, 720×200)

Exponential line chart of cumulative people implicated per round.

- **Title (17px, `#1a5276`, at 240,25):** "RICO Cascade: Implicated per Round".
- **Data:** per-round new `[1, 3, 9, 27]`; cumulative plotted `[1, 4, 13, 40]` at x = 120 + i×160, y = 180 − cumulative×3.5; red `#e74c3c` line (width 3) with 7px-radius red dots.
- **Labels (15px):** `#555` "Round 1"…"Round 4" at y=195 under each point; red cumulative value (1, 4, 13, 40) 15px above each dot.

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a single-row full-width table; left `<td>` (40%) holds `.obj-title` + bullet list + bold-labeled example paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; bullets 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** 8 canvases with HTML attributes `width="720" height="300"`; each chart's IIFE individually sets backing store to 720×200 × `window.devicePixelRatio` to 720px × 200px, and calls `ctx.scale` so drawing stays in logical coordinates. Base chart font variable `fontSize = 17` px -apple-system.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c` (dark red `#c0392b`), orange `#f39c12`, gray `#555`/`#333`/`#bbb`/`#ccc`.
- In regenerated HTML, any card/page links use `.html` extensions.
