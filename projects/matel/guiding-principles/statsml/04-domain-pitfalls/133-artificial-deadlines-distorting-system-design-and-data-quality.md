# Artificial Deadlines Distorting System Design & Data Quality

**Page type:** detail page (h2 section heading per pitfall, each followed by a one-row two-column obj-table: text left ~40%, canvas right ~60%)
**HTML title tag:** 133. Artificial Deadlines Distorting System Design & Data Quality

**Subtitle:** Deadline pressure turns "temporary" shortcuts into permanent holes in data quality, metrics, and system architecture.

## Pipeline Corners Cut

- "Must launch by Q4" → data pipeline corners cut → gaps become PERMANENT
- Nobody goes back to fix "temporary" shortcuts post-launch

**Example:** Missing validation steps skipped for speed become permanent holes in data quality that compound over years.

### Visualization (canvas `c1`, 720×200)

Line chart of data quality declining through a deadline and never recovering.

- **Title (17px `#1a5276`, at 10,25):** "Data Quality Over Time (Deadline Pressure)".
- **Axes:** blue `#2980b9` (width 2) — x-axis from (50,180) to (700,180), y-axis from (50,180) to (50,40).
- **Quality line:** red `#e74c3c` (width 2) through points `(50,60), (150,65), (250,80), (350,110), (400,140), (500,155), (600,160), (700,165)` (steady decline).
- **Deadline marker:** dashed red vertical line (dash 5/5) from (370,40) to (370,180), labeled "Deadline" at (372,50).
- **X labels (13px `#7f8c8d`):** "Pre-deadline" at (80,195); "Launch" at (350,195); "Post-launch (never fixed)" at (520,195).

## Polluted Training Data

- Growth hacks to hit targets pollute training data
- 60% of "new users" from incentivized signup that churn in 30 days
- But their data is in your model FOREVER

**Example:** Referral bonus drives fake signups whose behavioral patterns permanently skew user engagement models.

### Visualization (canvas `c2`, 720×200)

Paired bar chart per period: organic vs incentivized user share.

- **Title (17px `#1a5276`, at 10,25):** "User Composition: Organic vs Incentivized".
- **Data:** labels `["Week 1", "Week 2", "Week 3", "Week 4", "Month 2", "Month 3"]`; organic `[30, 32, 35, 38, 40, 42]`; incentivized `[70, 65, 50, 30, 10, 3]` (%).
- **Bars:** green `#27ae60` (organic) 30px wide at x = 100 + i·105; red `#e74c3c` (incentivized) 30px wide adjacent at x+32; height = value·1.8, baseline y=180 (blue `#2980b9` axis line from 80 to 680).
- **Labels:** period names in 12px `#7f8c8d` at y=195. Legend (13px): green "Organic" at (600,60); red "Incentivized (churn)" at (600,80).

## Cherry-Picked Metrics

- Metrics cherry-picked to show improvement by review date
- That definition becomes official going forward
- Chosen for optics not accuracy

**Example:** "Active user" redefined as "opened app once" instead of "completed action" to hit OKR, then that definition becomes permanent.

### Visualization (canvas `c3`, 720×200)

Bar chart of the same metric under increasingly loose definitions.

- **Title (17px `#1a5276`, at 10,25):** "Metric Definition Shopping".
- **Axes:** blue `#2980b9` (width 2), x-axis (50,180)→(700,180), y-axis (50,40)→(50,180).
- **Data:** definitions `["Strict", "Medium", "Loose", "Loosest"]`, values `[12, 28, 45, 72]` (%).
- **Bars:** 80px wide at x = 120 + i·150, height = value·1.8, baseline y=180; first three bars `#3498db`, last bar red `#e74c3c`.
- **Labels (13px `#2c3e50`):** definition names at y=195; percentage values above bars.
- **Annotation (13px red `#e74c3c`):** "\"Chosen for review\" →" at (530,70).

## Incomplete Data Collection at Launch

- Feature "launched" = flag flipped but data collection incomplete for weeks
- First 6 weeks of data is garbage nobody removes from training set

**Example:** Event tracking added 3 weeks post-launch; early data missing key fields but mixed into training pipeline unchanged.

### Visualization (canvas `c4`, 720×200)

Rising completeness curve with a shaded garbage-data zone at the start.

- **Title (17px `#1a5276`, at 10,25):** "Data Completeness After Feature Launch".
- **Axes:** blue `#2980b9` (width 2), x-axis (50,180)→(700,180), y-axis (50,40)→(50,180).
- **Completeness line:** green `#27ae60` (width 2) — values `[10, 15, 25, 40, 55, 70, 82, 90, 95, 97, 98, 99]` (%) at x = 80 + i·52, y = 180 − value·1.4.
- **Garbage zone:** red `#e74c3c` rect at (80,40) size 260×140 at 20% opacity, labeled in red 13px "Garbage data zone (still in training set)" at (90,55).
- **X label (13px `#7f8c8d`):** "Weeks post-launch →" at (300,195).

## Permanent "Temporary" Workarounds

- "Temporary" workaround under deadline pressure
- Still in production 3 years later
- Hardcoded values, skipped validation, wrong timezone

**Example:** Hardcoded UTC-5 offset "just for now" still converting timestamps incorrectly 4 years later across 12 downstream services.

### Visualization (canvas `c5`, 720×200)

Three horizontal bars whose widths encode planned vs actual workaround lifespans.

- **Title (17px `#1a5276`, at 10,25):** "Workaround Lifespan vs Original Intent".
- **Axis:** blue `#2980b9` x-axis from (50,180) to (700,180).
- **Bars (at y=100, 50px tall, x = 120 + i·200, width = days/8 capped at 150):** "Planned: 2wks" (14 days) in `#3498db`; "Actual: 6mo" (180 days) in `#f39c12`; "Reality: 3yrs" (1095 days, capped) in red `#e74c3c`. Labels in 13px `#2c3e50` above each bar at y=90.
- **Caption (12px `#7f8c8d`):** "Bar width = relative time in production" at (200,175).

## Calcified Technical Debt

- Rushed architecture: "we'll fix it after launch"
- Technical debt calcifies
- Every future feature built ON TOP of the broken foundation

**Example:** Denormalized schema chosen for speed; 3 years later, 40 services depend on it and migration estimated at 18 months.

### Visualization (canvas `c6`, 720×200)

Exponential curve of cost-to-change over quarters.

- **Title (17px `#1a5276`, at 10,25):** "Technical Debt Accumulation".
- **Axes:** blue `#2980b9` (width 2), x-axis (50,180)→(700,180), y-axis (50,40)→(50,180).
- **Curve:** red `#e74c3c` (width 2) from (50,170), quadratic growth: points at x = 50 + i·54, y = 170 − i²·1.1 for i = 1..12.
- **Labels (12px `#7f8c8d`):** "Quarters after rushed launch →" at (250,195); "Cost to change (exponential)" at (500,50).
- **Annotation (11px red `#e74c3c`):** "Each feature built on broken foundation" at (400,70).

## Bug Propagation Through Models

- Testing skipped for deadline → bugs discovered by USERS not QA
- Data from buggy period trains the NEXT model
- Bugs propagate through model generations

**Example:** Price display bug showed wrong values for 2 weeks; user click data from that period trained recommendation model to favor mispriced items.

### Visualization (canvas `c7`, 720×200)

Five-stage flow diagram of a bug propagating into the next model generation.

- **Title (17px `#1a5276`, at 10,25):** "Bug Propagation: Model Generations".
- **Stages:** boxes (120×40, centered at y=100, x = 60 + i·135) labeled in white 11px: "Bug in Prod", "Users Hit Bug", "Buggy Data", "Model v2 Trained", "Model v2 Has Bug". First three boxes red `#e74c3c`, last two purple `#8e44ad`. Arrows ("→" in `#2c3e50`) connect consecutive boxes.
- **Caption (13px `#7f8c8d`):** "Cycle repeats: bugs compound through each model generation" at (100,165).

## Phantom Stabilization Period

- Post-launch "stabilization period" = 4-8 weeks of degraded quality data
- Everyone pretends it doesn't exist in the training set

**Example:** First 6 weeks after launch had 30% error rate in event logging; that data still in training pipeline because "removing it is too hard."

### Visualization (canvas `c8`, 720×200)

Rising quality curve with a shaded degraded stabilization zone at the start.

- **Title (17px `#1a5276`, at 10,25):** "Data Quality: Stabilization Period Reality".
- **Axes:** blue `#2980b9` (width 2), x-axis (50,180)→(700,180), y-axis (50,40)→(50,180).
- **Stabilization zone:** red `#e74c3c` rect at (50,40) size 250×140 at 30% opacity.
- **Quality line:** green `#27ae60` — values `[30, 35, 42, 50, 60, 72, 85, 92, 95, 97, 98, 98]` at x = 70 + i·53, y = 180 − value·1.4.
- **Annotations (13px red):** "4-8 weeks: \"stabilization\"" at (60,55); "(degraded data everyone ignores)" at (60,72). X label `#7f8c8d`: "Weeks →" at (350,195).

## Regeneration instructions

- **Layout:** for each of the 8 pitfalls, an `<h2>` section heading (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table`: full-width table, left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + an `<p><strong>Example:</strong> ...</p>` paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px. Arrows in bullets are HTML entities (&rarr;) in the source; the h1 uses `&amp;` for "&".
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. Unused `.philosophy` class: background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"` but each chart's IIFE sets the canvas to a 720×200 logical size — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), CSS size fixed at 720×200 px, `ctx.scale` back to logical coordinates. (This page uses per-chart setup IIFEs rather than one shared loop.) All chart coordinates above are in the 720×200 space. Chart titles 17px, labels 11–13px, `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`, purple `#8e44ad`, gray text `#7f8c8d`/`#2c3e50`.
- Card links elsewhere referencing this page use the `.html` extension in regenerated HTML.
