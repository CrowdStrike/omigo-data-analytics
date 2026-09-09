# Food Science & Nutrition Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text + example callout left 50%, canvas right 50%)
**HTML title tag:** Food Science Data Pitfalls

**Subtitle:** Nutrition research rests on self-reported diet data, lifestyle confounding, and large individual biological variation — making most observational findings far weaker than they appear.

## Self-Reported Diet is 30-50% Inaccurate

**Self-Reported Diet is 30-50% Inaccurate**

- **Systematic underreporting:** Calories underreported 20-50%, worst for high-calorie snacks, desserts, alcohol
- **Social desirability bias:** Vegetables and fruit get overreported; fast food and sweets underreported
- **Memory failures:** People forget snacks, beverages, and portion sizes within 24-hour recall
- **Portion size misestimation:** Visual estimates run off by 50-100%, worst for calorie-dense foods
- **Evidence base crisis:** Most nutrition epidemiology rests on these fundamentally flawed self-reports

**Example (callout box):** In validation studies using doubly-labeled water (gold standard), obese subjects underreported calorie intake by an average of 47%. A food diary showing 1,800 kcal/day actually reflected 2,700 kcal/day measured consumption. This level of error makes causal inference impossible.

### Visualization (canvas `canvas1`, 720×240)

Paired bar chart of self-reported vs measured intake per category with error annotations.

- **Title (bold 17px `#2c3e50`, centered, y=25):** "Self-Reported vs. Measured Dietary Intake".
- **Data** (reported / measured, category labels on two lines below):
  - Calories (kcal/day): 1800 / 2400
  - Vegetables (servings): 4.0 / 2.5
  - Fruit (servings): 3.5 / 2.0
  - Alcohol (drinks/wk): 3 / 7
  - Snacks (items/day): 1.5 / 4.5
- **Layout:** chart from y=50 to y=height−40, all bars scaled against max value 2600; five equal-width category slots; reported bar (left, `#1a5276`) and measured bar (right, `#e67e22`), each 45px wide with a 10px gap; horizontal gridlines `#ddd` at 6 levels.
- **Value labels:** white bold 15px on each bar; bold 14px red `#e74c3c` error annotation above each pair: "25% error", "60% error", "75% error", "57% error", "67% error" (computed as |measured−reported|/measured).
- **Legend (bottom center, 16px):** `#1a5276` swatch "Reported"; `#e67e22` swatch "Measured".

## Healthy-User Bias (Confounding by Lifestyle)

**Healthy-User Bias (Confounding by Lifestyle)**

- **Lifestyle clustering:** Healthy eaters also exercise more, smoke less, earn more, access better care
- **Impossible to isolate:** No observational study separates broccoli from the health-conscious bundle
- **Socioeconomic confounding:** Food choices correlate strongly with education, income, and health literacy
- **Reverse causation:** Healthy people may change diet because they already feel better, not the reverse
- **Regression inadequacy:** Even extensive statistical adjustment cannot control unmeasured confounds

**Example (callout box):** A study finds people who eat kale have 30% lower heart disease. But kale eaters also average 5 gym visits per week, earn $85k vs $45k, and have college degrees. After adjusting for 20 covariates, the kale effect drops to 8% and becomes non-significant. Was there ever a kale effect?

### Visualization (canvas `canvas2`, 720×240)

Causal-graph diagram: six confounders feeding both exposure and outcome, with a dashed uncertain direct path.

- **Title (bold 17px `#2c3e50`, centered, y=25):** "Healthy-User Bias: Which Path is Causal?".
- **Exposure node:** filled `#1a5276` circle (radius 50) at center, white bold two-line label "Eats" / "Broccoli".
- **Outcome node:** filled `#27ae60` circle (radius 55) near bottom center, white bold two-line label "Good Health" / "Outcome".
- **Confounder nodes** (small 8px red `#e74c3c` dots with 14px `#2c3e50` two-line labels above; positions around the top and sides): "Exercises 4x/week", "Doesn't Smoke", "Higher Income", "Regular Checkups", "Sleeps 7-8hrs", "Lower Stress".
- **Edges:** solid red `#e74c3c` lines (width 2.5) from each confounder to BOTH the exposure node and the outcome node; the direct exposure→outcome path is a dashed gray `#95a5a6` line (width 3, dash 8/6) marked with a bold 28px gray "?".
- **Bottom annotation (bold 14px red, centered):** "Confounding paths (cannot be controlled)".

## Food Frequency Questionnaires are Unreliable

**Food Frequency Questionnaires are Unreliable**

- **Recall bias:** "How often did you eat X in the past year?" draws aspirational answers, not behavior
- **Low test-retest reliability:** Same person, same FFQ 1 month apart correlates only 0.5-0.7 (should be >0.9)
- **Temporal aggregation errors:** People cannot accurately average consumption over months or years
- **Context-dependent recall:** Answers shift with recent meals, time of day, and current hunger state
- **Foundation of epidemiology:** Despite poor reliability, FFQs remain the primary tool in large cohorts

**Example (callout box):** In a reproducibility test, participants completed the same FFQ twice, 4 weeks apart. One person reported eating vegetables 5 servings/day in March, then 2 servings/day in April. Another reported 3 alcoholic drinks per week, then 7. The measurement error is often larger than the effect size being studied.

### Visualization (canvas `canvas3`, 720×240)

Scatter plot of FFQ report 1 vs report 2 with perfect and actual fit lines.

- **Title (bold 17px `#2c3e50`, centered, y=25):** "FFQ Test-Retest Reliability (Same Person, 1 Month Apart)".
- **Axes:** margins left 80, right 40, top 50, bottom 60; both axes 0–10 with `#ddd` gridlines at every integer; x label "FFQ Report 1 (servings/week)"; rotated y label "FFQ Report 2 (1 month later)".
- **Points:** 30 random points (`#1a5276`, radius 5) generated per render around the line `y = 0.55·x + 1.5` with uniform noise ±1.5, x in ~[1,9] (clamped to [0.5, 9.5]) — the scatter is regenerated randomly each load, simulating r=0.55; plus one fixed red outlier (radius 7) at (5, 2) labeled in 13px red: "5 servings → 2 servings" / "(same person!)".
- **Lines:** dashed green `#27ae60` diagonal (dash 8/4, width 2) = perfect correlation; solid red `#e74c3c` fitted line (width 2.5) with slope 0.55, intercept 1.5.
- **Legend (top, 15px):** dashed green line "Perfect (r=1.0)"; solid red line "Actual (r=0.55)".
- **Annotation (right, red):** bold 18px "r = 0.55" over 14px "(should be > 0.9)".

## Bioavailability ≠ Nutrient Content

**Bioavailability ≠ Nutrient Content**

- **Absorption varies wildly:** Identical nutrient content yields 10-90% absorption by matrix and preparation
- **Food pairing effects:** Vitamin C raises iron absorption 4x, while tea and coffee cut it by 50-90%
- **Individual variation:** Gut health, genetics, and microbiome drive 10-fold differences in extraction
- **Misleading labels:** Databases and labels list total content, not bioavailable amount — false precision
- **Cooking transforms nutrients:** Heat, pH, and processing can destroy, create, or unlock nutrients

**Example (callout box):** A spinach salad contains 3mg iron per serving. But only 2-20% is absorbed depending on: raw vs cooked (cooking breaks cell walls), consumed with vitamin C source (+300%), or consumed with coffee (-90%). A model using "3mg iron intake" is off by an order of magnitude.

### Visualization (canvas `canvas4`, 720×240)

Horizontal bar chart of iron absorption percentage by consumption context vs the label claim.

- **Title (bold 17px `#2c3e50`, centered, y=25):** "Iron Absorption from Spinach: Bioavailability vs. Label Claims".
- **Bars** (right-aligned 15px labels at left; bars from x=230 to x=width−50 scaled to 100%; row height 30 starting y=55; bold 15px percent labels, white inside bar or dark beside small bars):
  - Raw spinach alone — 5%, red `#e74c3c`
  - Raw + vitamin C — 20%, orange `#e67e22`
  - Cooked alone — 15%, orange `#e67e22`
  - Cooked + vitamin C — 30%, green `#27ae60`
  - With tea/coffee — 2%, red `#e74c3c`
  - Label claims (100%) — 100%, gray `#95a5a6`, drawn as a dashed outline rectangle (dash 6/4) instead of a filled bar
- **Reference line:** dashed gray `#95a5a6` vertical line at the 100% mark spanning the first five rows.
- **Annotations (centered, red):** bold 15px "20x variation in actual absorption"; 14px "Models using label values are off by an order of magnitude".

## Gut Microbiome Individuality (N=1 Biology)

**Gut Microbiome Individuality (N=1 Biology)**

- **Dramatic glucose variability:** The same food produces 3-4x different blood sugar spikes across people
- **Microbiome-dependent metabolism:** Gut bacteria composition sets how foods are broken down and absorbed
- **Population averages mislead:** Glycemic index tables show average response; individual variation is enormous
- **Personalized nutrition needed:** One-size-fits-all dietary guidelines fail because biology varies so much
- **Between-person > between-food variance:** Who eats matters more than what, for metabolic response

**Example (callout box):** In a continuous glucose monitoring study, 5 people ate identical bananas. Person A spiked to 180 mg/dL (pre-diabetic range). Person B barely moved. Person C had delayed spike at 60 minutes. Person D had a double peak. Person E's response changed based on time of day. Average glycemic index: meaningless.

### Visualization (canvas `canvas5`, 720×240)

Multi-line glucose response chart for five people eating the same banana, with normal and spike zones.

- **Title (bold 17px `#2c3e50`, centered, y=25):** "Glucose Response to Same Food (Banana, Same Portion)".
- **Axes:** margins left 70, right 40, top 50, bottom 50; x = time 0–120 minutes (labels every 30); y = blood glucose 0–200 mg/dL (gridlines `#ddd` at 7 levels); x label "Time (minutes)"; rotated y label "Blood Glucose (mg/dL)".
- **Zones:** above 140 shaded `rgba(231, 76, 60, 0.1)` labeled "Spike zone (>160)" in 13px red; 70–140 band shaded `rgba(39, 174, 96, 0.1)` labeled "Normal (70-140)" in green.
- **Curves** (9 evenly-spaced points over 0–120 min, width 2.5):
  - Person A `#1a5276` (big spike at 30 min): `[100, 120, 160, 180, 150, 120, 100, 95, 90]`
  - Person B `#e74c3c` (moderate rise): `[95, 105, 120, 140, 135, 120, 105, 95, 90]`
  - Person C `#27ae60` (barely moves): `[100, 102, 105, 108, 105, 103, 100, 98, 97]`
  - Person D `#e67e22` (delayed spike): `[95, 100, 105, 110, 140, 165, 150, 120, 100]`
  - Person E `#9b59b6` (double peak): `[100, 130, 150, 130, 140, 160, 140, 110, 95]`
- **Legend (top right of plot, 14px):** colored line swatch + "Person A" … "Person E".
- **Annotation (bold 14px red, bottom center):** "5 people, identical banana: 2-3x variation in glucose spike".

## Processing Dramatically Changes Nutrients

**Processing Dramatically Changes Nutrients**

- **Cooking destroys and creates:** Heat degrades vitamin C (50-80% loss) but raises lycopene bioavailability 3-5x
- **Storage effects:** Fresh spinach loses 50% of folate after 8 days refrigerated, 90% after a week at room temp
- **Processing concentrates/dilutes:** Tomato paste holds 10x the lycopene of fresh tomatoes, but less vitamin C
- **Database aggregation errors:** Composition tables list "tomato" or "spinach" with no preparation method
- **Recipe complexity:** Multi-ingredient dishes compound uncertainty across every ingredient

**Example (callout box):** A nutrition database entry for "tomato, 100g" could mean: fresh (23 kcal, 13mg vitamin C, 3mg lycopene), canned (32 kcal, 9mg vitamin C, 5mg lycopene), sauce (70 kcal, 4mg vitamin C, 19mg lycopene), or sun-dried (258 kcal, 39mg vitamin C, 46mg lycopene). Models treat these as interchangeable.

### Visualization (canvas `canvas6`, 720×240)

Grouped bar chart of tomato nutrients across four processing methods.

- **Title (bold 17px `#2c3e50`, centered, y=25):** "Tomato Nutrient Profile by Processing Method (per 100g)".
- **Data** (fresh / canned / sauce / sun-dried, all scaled to a shared max of 300):
  - Vitamin C (mg): 13 / 9 / 4 / 39
  - Lycopene (mg): 3 / 5 / 19 / 46
  - Fiber (g): 1.2 / 1.0 / 1.5 / 12.3
  - Calories (kcal): 18 / 25 / 55 / 258
- **Layout:** chart from y=55 to y=height−65 with `#ddd` gridlines at 7 levels; four category groups each containing four 28px-wide bars with 4px gaps; colors Fresh `#27ae60`, Canned `#e67e22`, Sauce `#e74c3c`, Sun-dried `#9b59b6`; bold 11-12px value labels (white inside tall bars, dark above short bars); two-line 14px category labels below.
- **Legend (bottom center, 15px):** color swatches for "Fresh", "Canned", "Sauce", "Sun-dried".
- **Annotation (bold 14px red, centered, y=45):** "Same "tomato" in database: 14x calorie variation, inverted nutrient profiles".

## Regeneration instructions

- **Layout:** standard detail-page structure — h1, `.subtitle`, then one `<h2>` per pitfall, each followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` + `<ul>` of labeled bullets + an `.example` callout div, right `<td>` (60%, centered) holds one canvas. Even table rows background `#fafcfe`.
- **Callouts:** `.example` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 10px 12px, 0.9em, with a bold "Example:" lead. `.philosophy` style (background `#f0f4f8`, left border `4px solid #2980b9`) is defined but unused.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvases:** six canvases `canvas1`–`canvas6`, each 720×240. The original page draws directly at 720×240 without devicePixelRatio scaling; when regenerating, apply the project-standard `window.devicePixelRatio` backing-store scaling (multiply canvas width/height by dpr, `ctx.scale` back to logical coordinates). Each chart begins by filling a white background. Chart fonts sans-serif, 11-18px (bold 17px titles); title/axis text color `#2c3e50`, gridlines `#ddd`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#9b59b6`, grays `#2c3e50`/`#95a5a6`/`#ddd`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
