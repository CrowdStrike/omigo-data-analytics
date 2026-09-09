# Proxy Metrics Divorced from Purpose (Health/Fitness)

**Page type:** detail page (h2 heading per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 126. Proxy Metrics Divorced from Purpose (Health/Fitness)

**Subtitle:** Health and fitness numbers quantify what is easy to measure while losing the outcome they were meant to track.

## Calories In/Out Ignores Nutrition Quality

**Obj-title:** Calories In/Out Ignores Nutrition Quality

- 1500 cal of candy ≠ 1500 cal of balanced meals
- Deficit "works" for weight loss regardless of source
- But health outcomes diverge dramatically

**Example:** Two people eat 1500 cal/day — one from processed food, one from whole foods. Same weight loss, vastly different metabolic health markers.

### Visualization (canvas `c1`, declared 720×300, script renders at 720×200)

Horizontal paired bar chart comparing candy vs balanced diet across health dimensions.

- **Title (17px `#1a5276`):** "Calorie Equivalence Fallacy" at (10,25).
- **Rows** (labels in 13px `#666` at x=10, y = 50 + i*28): Weight Loss, Energy, Inflammation, Micronutrients, Gut Health.
- **Bars** (starting at x=130, width = value×2.5, 10px tall): candy (`#e74c3c`) values `[90, 85, 20, 10, 15]`; balanced (`#27ae60`, 12px lower) values `[90, 80, 85, 95, 90]`.
- **Legend (12px):** `#e74c3c` square at (500,50) with "1500 cal candy"; `#27ae60` square at (500,68) with "1500 cal balanced", text `#333`.

## Sleep Score — Composite Number Hiding What Matters

**Obj-title:** Sleep Score — Composite Number Hiding What Matters

- Aggregates deep sleep, REM, total duration, interruptions
- Score of 80 could mean very different sleep architectures
- Masks whether the problem is falling asleep vs staying asleep

**Example:** Score 80 with 20% deep sleep + 10% REM vs score 80 with 10% deep + 25% REM — same number, different recovery implications.

### Visualization (canvas `c2`, declared 720×300, script renders at 720×200)

Two stacked horizontal composition bars: same score, different sleep architecture.

- **Title (17px `#1a5276`):** "Same Score = 80, Different Architecture" at (10,25).
- **Categories/colors:** Deep Sleep `#1a5276`, REM `#2980b9`, Light Sleep `#85c1e9`, Awake `#e74c3c`.
- **Person A (Score 80)** (label 14px `#333` at (50,55)): stacked bar at y=65, 30px tall, starting x=50, segment widths = pct×5 for `[20, 10, 60, 10]`.
- **Person B (Score 80)** (label at (50,130)): stacked bar at y=140 for `[10, 25, 55, 10]`.
- **Legend (11px):** color squares at (520, 50 + i*18) with category names in `#333`.

## Step Count — Shuffling ≠ Exercise

**Obj-title:** Step Count — Shuffling ≠ Exercise

- 10K steps of slow walking < 30 min HIIT at 500 steps
- Intensity completely ignored by step counters
- Encourages low-effort movement over effective training

**Example:** Office worker pacing hallways hits 10K steps; gym-goer doing heavy squats logs 500 steps but gets far greater cardiovascular and muscular benefit.

### Visualization (canvas `c3`, declared 720×300, script renders at 720×200)

Two-bar comparison of steps vs actual fitness benefit.

- **Title (17px `#1a5276`):** "Steps vs Actual Fitness Benefit" at (10,25).
- **Axis labels (13px `#666`):** "Steps" at (30,190); "Fitness Benefit" at (10,50). Axis frame `#2980b9` width 1: (80,40)–(80,180)–(680,180).
- **Bars:** `#e74c3c` at (150,70) 60×110 (tall steps bar); `#27ae60` at (350,90) 60×90.
- **Labels (12px `#333`):** "10K slow steps" at (125,195); "500 steps + HIIT" at (325,195). "Low intensity" in `#e74c3c` at (130,60); "High intensity" in `#27ae60` at (330,82).

## BMI — Muscular Athletes Classified "Obese"

**Obj-title:** BMI — Muscular Athletes Classified "Obese"

- Body composition completely ignored
- Only uses height and weight
- Dwayne Johnson = "obese" by BMI

**Example:** A bodybuilder at 5'10", 230 lbs has BMI 33 (obese), yet has 12% body fat. A sedentary person at same BMI may have 35% body fat.

### Visualization (canvas `c4`, declared 720×300, script renders at 720×200)

Scatter plot of BMI vs body fat % with two highlighted outliers at the same BMI.

- **Title (17px `#1a5276`):** "BMI vs Actual Body Fat %" at (10,25). Axis frame `#2980b9` width 1: (80,40)–(80,170)–(680,170); labels (12px `#666`) "BMI" at (370,190), "Body Fat %" at (10,100).
- **Scatter points (`#2980b9`, radius 5):** `[[200,140],[250,120],[300,100],[350,80],[400,60],[450,90],[500,130],[550,150]]`.
- **Highlighted points (radius 8):** `#e74c3c` at (480,55) labeled "Bodybuilder: BMI 33, Fat 12%" (12px `#333` at (500,60)); `#e67e22` at (480,140) labeled "Sedentary: BMI 33, Fat 35%" (at (500,145)).

## Heart Rate Zones — Individual Variation Ignored

**Obj-title:** Heart Rate Zones — Individual Variation Ignored

- Max HR formulas (220 - age) off by ±15 BPM per person
- "Fat burning zone" is largely a myth
- Genetic variation in cardiac output not accounted for

**Example:** Two 40-year-olds: one has max HR of 165, another 195. Same "zone 2" prescription is completely wrong for one of them.

### Visualization (canvas `c5`, declared 720×300, script renders at 720×200)

Bar chart of individual max heart rates against the formula line.

- **Title (17px `#1a5276`):** "Max HR: Formula vs Reality (Age 40)" at (10,25). Axis frame `#2980b9` width 1: (80,50)–(80,170)–(650,170).
- **Formula line:** horizontal dashed `#e74c3c` (dash 5/5) at y=90 from x=80 to x=650, labeled "Formula: 180 BPM" in 13px `#e74c3c` at (520,87).
- **Bars (`#2980b9`, 40px wide):** at (150,60) height 110; (300,100) height 70; (450,75) height 95.
- **Labels (11px `#333`):** "Person A: 195" at (140,55); "Person B: 165" at (290,95); "Person C: 188" at (440,70). "±15 BPM range" in `#666` at (530,120).

## VO2 Max Estimates from Watches

**Obj-title:** VO2 Max Estimates from Watches

- Inaccurate by 10-15% vs lab test
- Yet treated as precise fitness measure
- Varies by wrist position, skin tone, motion artifacts

**Example:** Watch says VO2 max = 45 ml/kg/min. Lab test shows 38. User believes they're "above average" when they're actually average for their age.

### Visualization (canvas `c6`, declared 720×300, script renders at 720×200)

Grouped bar chart: watch-estimated vs lab-measured VO2 max for 8 subjects.

- **Title (17px `#1a5276`):** "Watch VO2 Max vs Lab Measured" at (10,25). Axis frame `#2980b9` width 1: (80,40)–(80,175)–(680,175); x label "Subjects" (12px `#666`) at (350,195).
- **Data:** watch = `[45, 52, 38, 48, 55, 42, 50, 46]`; lab = `[38, 44, 35, 40, 47, 38, 42, 41]`. Bars 25px wide at x = 120 + i*65 (watch `#2980b9`) and offset +27px (lab `#27ae60`), height = value×2.5, baseline y=175.
- **Legend (12px):** `#2980b9` square at (550,45) with "Watch estimate"; `#27ae60` square at (550,63) with "Lab measured", text `#333`.

## "Active Calories Burned" — Wildly Inaccurate

**Obj-title:** "Active Calories Burned" — Wildly Inaccurate

- Wearables have 30-50% error on calorie estimates
- People eat back "earned" calories that were never burned
- Creates false surplus leading to weight gain

**Example:** Watch says 500 cal burned during run. Actual: 320 cal. Person eats 500 cal post-workout snack, netting +180 cal instead of zero.

### Visualization (canvas `c7`, declared 720×300, script renders at 720×200)

Grouped bar chart: reported vs actual calories burned by activity.

- **Title (17px `#1a5276`):** "Reported vs Actual Calories Burned" at (10,25). Axis frame `#2980b9` width 1: (80,45)–(80,170)–(650,170).
- **Activities (labels 11px `#333` at y=185):** Run, Cycle, Weights, Walk, HIIT.
- **Data:** reported = `[500, 400, 350, 250, 600]` (`#e74c3c`); actual = `[320, 280, 180, 200, 420]` (`#27ae60`). Bars 35px wide at x = 120 + i*105 and offset +38px, height = value×0.22, baseline y=170.
- **Legend (12px):** `#e74c3c` square at (550,50) with "Watch says"; `#27ae60` square at (550,68) with "Actual", text `#333`.

## Body Weight as Progress Metric

**Obj-title:** Body Weight as Progress Metric

- Fluctuates 2-5 lbs/day from water, food, sodium
- Weekly trend needed but people react to daily noise
- Muscle gain + fat loss = no scale change but huge progress

**Example:** Person gains 1 lb of muscle, loses 1 lb of fat in a week. Scale unchanged — they feel "stuck" despite excellent body recomposition.

### Visualization (canvas `c8`, declared 720×300, script renders at 720×200)

Line chart: noisy daily weigh-ins overlaid with a smooth downward weekly trend.

- **Title (17px `#1a5276`):** "Daily Weight vs Trend (lbs)" at (10,25). Axis frame `#2980b9` width 1: (60,40)–(60,170)–(680,170).
- **Daily series (line `#bbb` width 1, dots `#aaa` radius 3):** `[182, 184, 181, 183, 185, 181, 182, 180, 183, 181, 179, 182, 180, 178]`; x = 80 + i*42, y = 170 − (value − 175)×12.
- **Trend series (line `#e74c3c` width 2.5):** `[183, 182.8, 182.5, 182.3, 182, 181.7, 181.5, 181.2, 181, 180.7, 180.4, 180.2, 179.8, 179.5]`, same scaling.
- **Legend (12px):** "Daily weigh-in" in `#aaa` at (550,55); "Weekly trend" in `#e74c3c` at (550,72).

## Regeneration instructions

- **Layout:** for each pitfall: an `<h2>` section heading (1.4em, `#1a5276`, 2px solid `#2980b9` bottom border), then a `.obj-table` (full-width, border-collapse) containing one `<tr>`; left `<td>` (40%) holds `.obj-title` div + `<ul>` bullets + a `<p><strong>Example:</strong> ...</p>` paragraph; right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page style:** body -apple-system/BlinkMacSystemFont/'Segoe UI' sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; p 0.95em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. A `.philosophy` class exists (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but is unused on this page. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"`; a shared setup loop over all canvases sets each backing store to 720×200 × `window.devicePixelRatio` to 720×200px, and calls `ctx.scale` so drawing stays in logical coordinates; per-chart IIFEs then draw. Title font 17px -apple-system, sans-serif.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, light blue `#85c1e9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#666`/`#333`/`#aaa`/`#bbb`.
- Note: in regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
