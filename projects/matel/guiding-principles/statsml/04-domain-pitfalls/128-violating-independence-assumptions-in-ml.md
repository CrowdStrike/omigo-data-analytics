# Violating Independence Assumptions in ML

**Page type:** detail page (h2 heading per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 128. Violating Independence Assumptions in ML

**Subtitle:** Rows that share users, time, geography, clusters, or households are not independent — and random splits turn that dependence into inflated accuracy.

## Same User in Train AND Test

**Obj-title:** Same User in Train AND Test

- Model memorizes user-specific patterns, not generalizable ones
- Accuracy inflated by 10-20%
- Deploy to new users → performance collapses

**Example:** User A has 50 rows in train, 5 in test. Model learns "user A always clicks sports" — test accuracy 95%. New user accuracy: 72%.

### Visualization (canvas `c1`, declared 720×300, script renders at 720×200)

Two-bar accuracy comparison: leaked split vs proper split.

- **Title (17px `#1a5276`):** "Accuracy: Leaked vs Proper Split" at (10,25). Axis frame `#2980b9` width 1: (80,40)–(80,170)–(500,170).
- **Bars (80px wide, baseline y=170):** `#e74c3c` at (130,55) height 115 with value "95%" (14px `#333`) above; `#27ae60` at (300,95) height 75 with value "72%".
- **X labels (12px `#333`, two lines each):** "Same users" / "in both sets" under the left bar; "New users" / "(proper)" under the right bar.
- **Annotation:** "10-20% inflation!" in 13px `#e74c3c` at (480,75) with a red pointer line from (470,72) to (385,90).

## Temporal Leakage — Future in Training

**Obj-title:** Temporal Leakage — Future in Training

- Random split on time-series puts future data in training set
- Model "predicts" past from future — not real prediction
- Performance drops 30-50% with proper temporal split

**Example:** Stock prediction model trained on random 80/20 split shows 85% accuracy. Proper walk-forward split: 55% accuracy (barely above random).

### Visualization (canvas `c2`, declared 720×300, script renders at 720×200)

Two horizontal strips of 10 colored blocks each comparing random vs temporal splits along a time axis.

- **Title (17px `#1a5276`):** "Random Split vs Temporal Split" at (10,25). "Time →" in 13px `#666` at (320,195).
- **Random split row** (label "Random split:" 13px `#333` at (10,60)): 10 blocks 48×22 at x = 130 + i*55, y=47, colors `[#2980b9, #e74c3c, #2980b9, #e74c3c, #2980b9, #e74c3c, #2980b9, #2980b9, #e74c3c, #2980b9]` (train/test interleaved).
- **Temporal split row** (label "Temporal split:" at (10,110)): 10 blocks at y=97; first 7 `#2980b9` (train), last 3 `#e74c3c` (test).
- **Legend (12px):** `#2980b9` square at (520,135) with "Train"; `#e74c3c` square at (580,135) with "Test", text `#333`.
- **Annotation (12px `#e74c3c`):** "Random: 85% acc | Temporal: 55% acc" at (200,170).

## Correlated Features Treated as Independent

**Obj-title:** Correlated Features Treated as Independent

- Multicollinearity splits importance randomly between correlated features
- One feature looks weak when it's actually strong (correlated with another)
- Feature selection becomes unreliable

**Example:** "Temperature" and "ice cream sales" both predict "pool attendance." Model assigns 50% importance to each randomly — dropping either barely hurts, but both matter.

### Visualization (canvas `c3`, declared 720×300, script renders at 720×200)

Horizontal feature-importance bars with two correlated features flagged as unstable.

- **Title (17px `#1a5276`):** "Feature Importance: Correlated Features" at (10,25). Axis frame `#2980b9` width 1: (150,40)–(150,180)–(680,180).
- **Rows (labels 12px `#333`):** "Temperature" at (40,65); "Ice Cream Sales" at (40,105); "Day of Week" at (55,145).
- **Bars (22px tall, starting x=150):** Temperature `#e67e22` width 200 at y=50, annotated "~50% (unstable)"; Ice Cream Sales `#e67e22` width 180 at y=90, annotated "~45% (unstable)"; Day of Week `#2980b9` width 300 at y=130, annotated "Stable". Annotations 12px `#333`.
- **Correlation link:** dashed `#e74c3c` curve (dash 3/3, width 1.5) connecting the ends of the two orange bars, labeled "r = 0.92" and "(importance splits randomly)" in 11px `#e74c3c` around (385–450, 85).

## Clustered Data — Students Within Schools

**Obj-title:** Clustered Data — Students Within Schools

- Observations within a cluster are MORE similar than across clusters
- Standard errors underestimated (false confidence)
- Effective sample size much smaller than N

**Example:** 1000 students in 10 schools. Effective N ≈ 50, not 1000. p-values look significant but aren't with proper clustering adjustment.

### Visualization (canvas `c4`, declared 720×300, script renders at 720×200)

Cluster scatter: 10 tight color-coded point clusters representing schools.

- **Title (17px `#1a5276`):** "Clustered Data: Effective N << Actual N" at (10,25). Subtext (13px `#333`): "10 schools, 100 students each = 1000 observations" at (80,50).
- **Clusters:** 10 clusters, centers at cx = 80 + (c mod 5)×130, cy = 80 + floor(c/5)×60; each drawn as 8 dots (radius 4) jittered around the center via cos/sin offsets. Cluster colors: `#2980b9`, `#e74c3c`, `#27ae60`, `#e67e22`, `#8e44ad`, `#1abc9c`, `#d35400`, `#2c3e50`, `#f39c12`, `#16a085`.
- **Annotations:** "Effective N ≈ 50" / "(not 1000!)" in 14px `#e74c3c` at (520,100)/(520,120); "Within-cluster similarity" / "reduces information" in 12px `#666` at (500,150)/(500,166).

## Network Effects — Users Influence Each Other

**Obj-title:** Network Effects — Users Influence Each Other

- User A's behavior depends on friends' behavior
- Can't treat users as independent data points
- A/B tests contaminated by social spillover

**Example:** User in control group sees friends (in treatment) posting about new feature → control user adopts behavior anyway. Treatment effect underestimated.

### Visualization (canvas `c5`, declared 720×300, script renders at 720×200)

Social network graph with treatment and control nodes intermixed and connected.

- **Title (17px `#1a5276`):** "Network Spillover in A/B Tests" at (10,25).
- **Nodes (radius 12, white letter label):** `[{150,100,T},{220,70,T},{280,120,C},{350,80,T},{320,150,C},{200,150,C},{400,130,C},{450,90,T}]` — T nodes `#2980b9`, C nodes `#e74c3c`.
- **Edges (`#bbb`, width 1):** `[0-1, 1-2, 1-5, 2-3, 2-4, 3-7, 4-6, 5-4]`.
- **Legend (12px):** `#2980b9` square at (500,60) with "Treatment"; `#e74c3c` square at (500,80) with "Control", text `#333`.
- **Annotations (13px `#e74c3c`):** "Control users see friends" at (480,130); "using feature → adopt it!" at (480,148); "Treatment effect" at (480,172); "UNDERESTIMATED" at (480,188).

## Autocorrelation in Time-Series

**Obj-title:** Autocorrelation in Time-Series

- Today's value depends on yesterday's value
- Random train/test split creates adjacent pairs across sets
- Model exploits proximity, not pattern

**Example:** Temperature prediction: random split puts Monday-train, Tuesday-test. Model just learns "tomorrow ≈ today" — 98% accuracy. Real 7-day forecast: 60%.

### Visualization (canvas `c6`, declared 720×300, script renders at 720×200)

Temperature time-series line with points alternately colored train/test to show adjacent leakage.

- **Title (17px `#1a5276`):** "Autocorrelation: Adjacent Values Leak" at (10,25). Axis frame `#2980b9` width 1: (60,40)–(60,170)–(680,170).
- **Series line (`#1a5276`, width 2):** temps `[68, 70, 72, 74, 73, 71, 69, 67, 65, 63, 64, 66, 68, 70, 72, 75, 77, 79, 80, 78]`; x = 80 + i*30, y = 170 − (temp − 60)×5.5.
- **Point colors (radius 5):** split mask `[0,1,0,1,0,0,1,0,1,0,0,1,0,0,1,0,0,1,0,1]` — 1 = test `#e74c3c`, 0 = train `#2980b9`.
- **Legend (12px):** `#2980b9` square at (500,40) with "Train"; `#e74c3c` square at (560,40) with "Test", text `#333`.
- **Annotations (12px `#e74c3c`):** "Adjacent points in different sets!" at (400,70); "\"Tomorrow ≈ Today\" = free accuracy" at (400,88).

## Geographic Proximity — Spatial Autocorrelation

**Obj-title:** Geographic Proximity — Spatial Autocorrelation

- Nearby locations have similar outcomes
- Random split puts neighbors in both train and test
- Model memorizes location clusters, not causal patterns

**Example:** Housing price model: houses on same street in both sets. Model learns "this street = $500K" without understanding WHY. New neighborhood: 40% error.

### Visualization (canvas `c7`, declared 720×300, script renders at 720×200)

Map grid with two geographic price clusters, each containing both train and test points.

- **Title (17px `#1a5276`):** "Geographic Proximity: Neighbors Leak" at (10,25).
- **Grid:** light `#ddd` lines (width 0.5), 9 vertical at x = 80 + i*50 from y=45 to 185, 9 horizontal at y = 45 + i*20 from x=80 to 480.
- **High-price cluster (points radius 7):** `[[130,65],[160,75],[145,90],[170,60],[185,85]]` — first 3 train `#2980b9`, last 2 test `#e74c3c`; labeled "$500K area" (12px `#1a5276`) at (110,115).
- **Low-price cluster:** `[[300,130],[330,140],[315,155],[340,125],[355,145]]` — same 3/2 train/test coloring; labeled "$200K area" at (285,175).
- **Annotations (13px `#e74c3c`):** "Neighbors in both train & test" at (490,80); "→ memorizes locations" at (490,100); "→ new neighborhood: 40% error" at (490,120).
- **Legend (11px):** `#2980b9` square at (490,145) with "Train"; `#e74c3c` square at (550,145) with "Test", text `#333`.

## Family/Household Members in Both Sets

**Obj-title:** Family/Household Members in Both Sets

- Same household in train and test
- Model learns household traits, not individual predictive features
- Shared environment creates artificial correlation

**Example:** Credit risk model: parent in train, child in test. Same address, similar spending patterns. Model appears to predict risk but just memorizes households.

### Visualization (canvas `c8`, declared 720×300, script renders at 720×200)

Three household boxes, each containing member nodes split across train and test.

- **Title (17px `#1a5276`):** "Household Leakage in Train/Test" at (10,25).
- **Households (boxes 110×90 stroked `#2980b9` width 2, label 11px `#666` above):** "Household A" at (100,100) with members P, C1, C2; "Household B" at (320,100) with P, C1; "Household C" at (520,100) with P, C1, C2. Member circles radius 12 at 35px horizontal spacing, white 10px letter labels; first member (P) `#2980b9` (train), remaining members `#e74c3c` (test).
- **Legend (12px):** `#2980b9` square at (200,165) with "In Train"; `#e74c3c` square at (290,165) with "In Test", text `#333`.
- **Annotations (12px `#e74c3c`):** "Same address, spending, behavior" at (380,170); "→ model learns households, not individuals" at (380,188).

## Regeneration instructions

- **Layout:** for each pitfall: an `<h2>` section heading (1.4em, `#1a5276`, 2px solid `#2980b9` bottom border), then a `.obj-table` (full-width, border-collapse) containing one `<tr>`; left `<td>` (40%) holds `.obj-title` div + `<ul>` bullets + a `<p><strong>Example:</strong> ...</p>` paragraph; right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page style:** body -apple-system/BlinkMacSystemFont/'Segoe UI' sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; p 0.95em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. A `.philosophy` class exists (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but is unused on this page. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"`; a shared setup loop over all canvases sets each backing store to 720×200 × `window.devicePixelRatio` to 720×200px, and calls `ctx.scale` so drawing stays in logical coordinates; per-chart IIFEs then draw. Title font 17px -apple-system, sans-serif.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, plus cluster accents `#1abc9c`/`#d35400`/`#2c3e50`/`#f39c12`/`#16a085`; grays `#666`/`#333`/`#bbb`/`#ddd`.
- Note: in regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
