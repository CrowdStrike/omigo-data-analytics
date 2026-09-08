# Pandemic Waves / Epidemic Data

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one h2 + one-row table per pitfall)
**HTML title tag:** 108. Pandemic Waves / Epidemic Data

**Subtitle:** Exponential growth, geographic propagation, variant mutations, testing artifacts, behavioral interventions — data patterns that broke every existing model simultaneously.

## Exponential Growth → Plateau → New Variant → Repeat

**Exponential Growth → Plateau → New Variant → Repeat**

- Each wave begins with exponential rise (R>1), then peaks as hospitals fill and behavior changes
- Decline follows (immunity + restrictions), then a plateau that holds until the next wave starts
- New variant (Delta, Omicron) resets immunity → new wave
- Models trained on Wave 1 fail on Wave 2 (different variant, different behavior, different immunity level)
- Each wave is a DIFFERENT epidemic with DIFFERENT parameters

**Example:** A model fit to Alpha's 4-week doubling time predicted Delta would peak at 50K cases; Delta's 2-week doubling time meant it peaked at 150K before behavior changed.

### Visualization (canvas `c1`, 720×300; drawing occupies top ~200px)

Multi-wave epidemic curve with variant labels.

- **Background:** `#f8f9fa`; L-shaped `#ccc` axes (y-axis at x=50 from y=10 to 170, x-axis to x=710); axis labels (13px `#555`): "Cases" (5,20), "Time (months)" (bottom center).
- **Waves:** three asymmetric peak curves (power-1.5 rise, power-1.2 fall from baseline y=165), each stroked 2.5px with a 10%-alpha area fill of the same color and a bold label near its peak: Alpha (x 60-230, peak x=140, peak y=100) `#e74c3c`; Delta (230-440, peak 340, peak y=55) `#8e44ad`; Omicron (440-680, peak 560, peak y=30) `#2980b9`. Successive peaks are higher.
- **Headline (bold 17px `#1a5276` at 140,18):** "Each wave = different epidemic, different parameters".

## Testing Capacity Creates Artificial "Cases"

**Testing Capacity Creates Artificial "Cases"**

- March 2020: 5000 tests/day → 500 "cases." June 2020: 500,000 tests/day → 30,000 "cases"
- Did cases increase 60×? Or did DETECTION increase 100×?
- "Cases" = f(true infections × test availability × test-seeking behavior)
- More testing = more detected cases without more actual disease

**Example:** A university mandating weekly testing for 40K students reported 800 "cases" — a neighboring university with no testing reported 12. Same epidemic, 67× difference in data.

### Visualization (canvas `c2`, 720×300; drawing occupies top ~200px)

Three-line chart: tests, reported cases, true infections.

- **Background:** `#f8f9fa` with `#ccc` L-axes as in chart 1.
- **Tests/day line:** green `#27ae60` width 2.5, ramping steadily upward across the chart (rise 120px with a small sine wobble).
- **Reported Cases line:** red `#e74c3c` width 2.5, tracking the tests line closely (rise 100px, offset sine wobble).
- **True Infections (est.) line:** orange `#f39c12` width 2 dashed (dash 6/4), roughly flat with a gentle slow sine oscillation around 40-60px above baseline.
- **Legend (14px, top right):** green "Tests/day"; red "Reported Cases"; orange "True Infections (est.)".
- **Headline (bold 15px `#1a5276` at 100,18):** "Cases track testing capacity, not disease spread".

## Reporting Lag (Cases Today = Infections 2 Weeks Ago)

**Reporting Lag (Cases Today = Infections 2 Weeks Ago)**

- Infected Monday → symptoms Thursday → tested Friday → result Sunday → reported Tuesday. 8-day lag minimum
- "Today's numbers" describe LAST WEEK's reality
- Policy made on today's data responds to a situation from 2 weeks ago
- By the time a surge appears in data, it's been surging for 2 weeks already

**Example:** A governor imposed restrictions on Day 20 based on surging case data — but infections had already peaked on Day 14 and were declining; the "surge" in reports was the lag catching up.

### Visualization (canvas `c3`, 720×300; drawing occupies top ~200px)

Two identical bell curves shifted by the reporting lag.

- **Background:** `#f8f9fa` with `#ccc` L-axes.
- **Actual Infections curve:** red `#e74c3c` width 2.5 Gaussian (amplitude 120px, centered at 40% of width).
- **Reported Cases curve:** blue `#2980b9` width 2.5, the same Gaussian shifted right by 80px (the lag).
- **Lag arrow:** dashed `#555` horizontal arrow between the two peaks (from x=280 to x=360 at y=145) with 13px label "10-14 day lag" above it.
- **Legend (14px, top right):** red "Actual Infections"; blue "Reported Cases".
- **Headline (bold 15px `#1a5276` at 120,18):** "Today's data = last week's reality".

## Behavioral Intervention Breaks the Model (Lockdowns)

**Behavioral Intervention Breaks the Model (Lockdowns)**

- Exponential growth predicted: 1M cases by April. Lockdown imposed March 15. Cases plateau at 200K
- Media: "models were wrong, they predicted 1M" — NO, the model predicted WITHOUT intervention
- The intervention WORKED, making the model's prediction look wrong
- The model was correct about the counterfactual nobody can observe

**Example:** Imperial College projected 500K UK deaths without intervention; actual deaths were 130K WITH lockdowns. Critics called the model "wrong" — it was correct about the scenario it modeled.

### Visualization (canvas `c4`, 720×300; drawing occupies top ~200px)

Diverging projection vs actual curves at a lockdown line.

- **Background:** `#f8f9fa` with `#ccc` L-axes.
- **Lockdown marker:** vertical dashed dark-red `#c0392b` line (dash 8/4, width 2) at x=280, labeled 14px "Lockdown" at top.
- **Model projection:** dashed red `#e74c3c` (dash 6/4, width 2) exponential curve y = baseline − min(150, 10·e^(4.5t)) across the full width — keeps rising to the cap.
- **Actual curve:** solid blue `#2980b9` width 2.5, identical exponential up to the lockdown x, then flattens to a plateau just ~10% above the lockdown-day level.
- **Labels (14px):** red "Model: 1M (no intervention)" at (440,30); blue "Actual: 200K (with lockdown)" at (440,110).
- **Headline (bold 15px `#1a5276` at 60,18):** "Intervention makes correct model look "wrong"".

## Geographic Hotspot Propagation

**Geographic Hotspot Propagation**

- NYC → New Jersey → Connecticut → rest of US over 6 weeks
- Each region's wave starts 2-4 weeks after its neighbor
- Model trained on NYC: accurate for NYC. Applied to Texas 6 weeks later: wrong parameters
- Each geography is a DIFFERENT epidemic delayed in time, not the same epidemic at different stages

**Example:** NYC peaked in April 2020 with 30% subway-rider transmission; Sun Belt states peaked in July with 60% indoor-AC transmission — same virus, completely different dynamics.

### Visualization (canvas `c5`, 720×300; drawing occupies top ~200px)

Five staggered identical bell curves, one per region.

- **Background:** `#f8f9fa` with `#ccc` L-axes.
- **Curves:** identical Gaussians (amplitude 80px, width 300px, peak at 35% of each span), 2.2px stroke, offset progressively rightward: NYC `#e74c3c` offset 0; NJ `#8e44ad` offset 50; CT `#2980b9` offset 100; PA `#27ae60` offset 150; TX `#f39c12` offset 210.
- **Legend (13px, top right at x=540):** color swatch + region name, stacked vertically.
- **Headline (bold 15px `#1a5276` at 60,18):** "Same virus, staggered waves: each region offset 2-4 weeks".

## Prior Immunity Data Invalidated by Mutations

**Prior Immunity Data Invalidated by Mutations**

- Delta: escapes 30% of vaccine immunity. Omicron: escapes 70%
- "Immune" from vaccination in June ≠ "immune" in December
- Immunity wanes AND virus evolves → "protected" changes every 3-6 months
- Any model using "vaccination status" as binary (yes/no) is wrong — effectiveness decays over time AND variant

**Example:** Israel (90% vaccinated by June 2021) saw a massive Delta wave in August because 2-dose Pfizer dropped from 95% to 39% effectiveness against Delta infection over 5 months.

### Visualization (canvas `c6`, 720×300; drawing occupies top ~200px)

Vaccine-effectiveness decay curve with step drops at variant arrivals.

- **Background:** `#f8f9fa` with `#ccc` L-axes; y-axis labels (13px `#555`): 95% (y≈45), 50% (y≈100), 20% (y≈145); x-axis label "Months post-vaccination".
- **Effectiveness line:** blue `#2980b9` width 2.5 starting high (y=40) and declining gently (slope 0.2/px), with a sharp 35px vertical drop at x=350 (Delta) then gentler decline (0.15/px), and another 30px drop at x=530 (Omicron) then slope 0.1/px, floored at y = h−40.
- **Variant markers:** vertical dashed red `#e74c3c` lines (dash 5/3) at x=350 and x=530, labeled bold red 14px "Delta" and "Omicron" at top.
- **Headline (bold 15px `#1a5276` at 80, bottom, y=188):** "Vaccine effectiveness: waning + variant escape".

## Survivorship in Severity Data

**Survivorship in Severity Data**

- Early 2020: "Case fatality rate = 5%." But only HOSPITALIZED patients were tested
- Actual infection fatality rate: 0.3-0.5%
- Asymptomatic/mild cases (80% of infections): untested, uncounted
- Selection bias in testing → massively inflated severity estimates → panic

**Example:** Italy reported 7.2% CFR in March 2020 because it tested almost exclusively hospital patients; later seroprevalence studies showed IFR was 0.5-1.0% — a 7-14× overestimate.

### Visualization (canvas `c7`, 720×300; drawing occupies top ~200px)

Iceberg diagram: visible tested cases vs hidden infections.

- **Background:** `#f8f9fa`; water fill `rgba(41,128,185,0.15)` below the waterline at y=70 with a solid blue `#2980b9` 2px waterline; small blue 13px label at (460,65): "--- water line (testing threshold) ---".
- **Iceberg tip (above water):** red `#e74c3c` triangle from (w/2−60, 70) up to (w/2, 20) down to (w/2+60, 70); white bold labels "Tested" and "(severe)".
- **Iceberg body (below water):** gray `#7f8c8d` trapezoid widening down to (w/2±150, 180); white labels "Untested: asymptomatic/mild" and "(80% of all infections)".
- **Left annotations (14px `#c0392b`):** "CFR = 5% (visible only)" (20,35); "IFR = 0.3-0.5% (all infections)" (20,55).
- **Headline (bold 15px `#1a5276` at 400,18):** "Selection bias: only severe cases visible".

## Hospital Capacity as Confounding Threshold

**Hospital Capacity as Confounding Threshold**

- Below capacity: patient admitted → treated → recovers → "low mortality"
- Above capacity: same patient → no bed → no ventilator → dies → "high mortality"
- The SAME disease has DIFFERENT outcomes depending on hospital load
- A region's "wave severity" is partially a measurement of hospital capacity, not virus lethality

**Example:** Lombardy Italy (overwhelmed hospitals) had 3× the mortality rate of Veneto (same virus, adequate capacity) — the difference was beds, not biology.

### Visualization (canvas `c8`, 720×300; drawing occupies top ~200px)

Mortality spiking once ICU capacity is exhausted.

- **Background:** `#f8f9fa` with `#ccc` L-axes.
- **Capacity marker:** vertical dashed red `#e74c3c` line (dash 8/4, width 2) at x=380, labeled 14px "ICU 100%" at top.
- **ICU Occupancy line:** orange `#f39c12` width 2, linear ramp rising 130px across the chart.
- **Mortality Rate line:** purple `#8e44ad` width 2.5, flat/low before the capacity line, then rising steeply (saturating exponential, up to 100px) after it.
- **Legend (14px, top right):** orange "ICU Occupancy"; purple "Mortality Rate".
- **Headline (bold 15px `#1a5276` at 60,18):** "Mortality spikes when capacity exhausted".

## Asymptomatic Spread Creates Invisible Transmission

**Asymptomatic Spread Creates Invisible Transmission**

- 40-50% of transmission from people with NO symptoms — they never test, never appear in data
- The epidemic propagates through people INVISIBLE to the surveillance system
- All case data represents the VISIBLE minority of infections
- Contact tracing catches symptomatic → symptomatic links but misses the asymptomatic bridges

**Example:** A nursing home outbreak traced 47 cases to a single asymptomatic staff member who passed 6 negative symptom screenings over 2 weeks while actively transmitting.

### Visualization (canvas `c9`, 720×300; drawing occupies top ~200px)

Transmission network of visible and invisible nodes.

- **Background:** `#f8f9fa`.
- **Nodes:** 14 fixed-position nodes — 6 symptomatic (larger, radius 10, solid green `#27ae60` with `#1e8449` outline) at (120,50), (280,40), (450,60), (600,50), (200,150), (520,155); 8 asymptomatic (radius 8, translucent red `rgba(231,76,60,0.5)` with `#c0392b` outline) at (180,95), (350,100), (380,150), (500,100), (260,130), (440,130), (100,130), (620,120).
- **Edges:** 16 fixed transmission links; any edge touching an asymptomatic node drawn dashed translucent red `rgba(231,76,60,0.4)` width 2 (dash 4/3), symptomatic-to-symptomatic edges solid translucent green `rgba(39,174,96,0.6)` width 1.5. Most paths route through the invisible red nodes.
- **Legend (14px `#333`, bottom):** green dot "Symptomatic (visible)"; translucent red dot "Asymptomatic (invisible, 40-50% of spread)".
- **Headline (bold 15px `#1a5276` at 200,18):** "Most transmission through invisible nodes".

## Pandemic Fatigue Changes Behavior Over Time (Non-Stationary Compliance)

**Pandemic Fatigue Changes Behavior Over Time (Non-Stationary Compliance)**

- Month 1: 90% mask compliance, 95% lockdown compliance. Month 6: 50% masks. Month 12: 20% masks
- The SAME policy has DIFFERENT effect sizes over time because human behavior degrades
- Models assuming constant intervention effectiveness: wrong
- Each month's "lockdown" is a WEAKER intervention than the previous month's as compliance erodes
- Same policy name, different actual behavior — the label stays fixed while the real effect decays

**Example:** UK's first lockdown (March 2020) reduced mobility by 70%; the third lockdown (January 2021) with identical rules reduced mobility by only 25% — same law, 3× less effect.

### Visualization (canvas `c10`, 720×300; drawing occupies top ~200px)

Compliance decay curves under an unchanged policy.

- **Background:** `#f8f9fa` with `#ccc` L-axes; y-axis labels (13px `#555`): 100% (y≈35), 50% (y≈95), 0% (y≈165); x-axis labels: "Month 1" (x≈80), "Month 6" (x≈300), "Month 12" (x≈530).
- **Policy markers:** faint dashed gray `rgba(149,165,166,0.5)` vertical lines (dash 3/3) at x = 100, 200, 350, 500, with gray 13px note "Policy unchanged" at bottom right.
- **Mask Compliance curve:** blue `#2980b9` width 2.5, exponential decay y = 30 + 130·(1 − e^(−2.5t)).
- **Lockdown Compliance curve:** red `#e74c3c` width 2.5, faster decay y = 25 + 135·(1 − e^(−3.5t)).
- **Legend (14px, top right):** blue "Mask Compliance"; red "Lockdown Compliance".
- **Headline (bold 15px `#1a5276` at 100,18):** "Same policy, declining effect: pandemic fatigue".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table` layout — one `h2` per pitfall followed by a single-row table: left `<td>` (40%) with `.obj-title` div (repeating the section title), a `<ul>` of bullets, and one `<p><strong>Example:</strong> …</p>`, right `<td>` (60%, centered) with one `<canvas width="720" height="300">` (the setup helper redraws at 720×200 logical size and fixes CSS size to 720×200). Ten sections total. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`; p 0.95em `#333`. `.philosophy` callout style defined but unused. No nav bar, no back/home links.
- **Canvas:** one outer IIFE with a `setupCanvas(id)` helper (const/arrow style) that scales the backing store by `window.devicePixelRatio`, sets CSS size 720×200, `ctx.scale` back to logical coordinates; base chart font 17px system sans, titles bold at 15px, canvas backgrounds `#f8f9fa` with `#ccc` axes.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c` (dark red `#c0392b`), orange `#f39c12`, purple `#8e44ad`, gray `#555`/`#7f8c8d`/`#95a5a6`.
- Note: in regenerated HTML, any card/page links use `.html` extensions (this page has none).
