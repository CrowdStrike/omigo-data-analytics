# Validation & Calibration

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** Validation & Calibration — How a Simulation Earns Trust

**Subtitle:** A simulation is trustworthy only if it beats a dumb baseline on data it never saw, and its stated probabilities come true at the stated rate.

**Intro callout (blue-left-border box):** The difference between weather models and most other simulations is not the physics — it is that weather forecasts are graded against reality every single day, and probability forecasting has been scored formally since Brier's 1950 verification work. Validation is the discipline of arranging that grading for models that do not get it for free: rewind and forecast, beat a baseline, check the calibration, and hold out whole events.

## 1. Hindcasting: rewind and forecast

The cleanest test of a simulation is to rewind time: initialize the model on a past date using only the data available then, forecast forward, and compare against what actually happened.

- **Rewind the clock:** Initialize the model on a past date as if it were today.
- **Old information only:** The model may use only data that existed on that date.
- **Forecast forward:** Run the forecast and score it against the recorded outcome.
- **Strict hygiene:** Any leak from the future inflates measured skill.
- **Subtle leaks count:** Even tuning choices informed by later data are leaks.
- **Many rewinds:** Repeating this over hundreds of dates gives a skill distribution.
- **Operational gate:** Large hindcast sets are how new model versions get approved.

Key point: Hindcasting only works under strict information hygiene — one leaked observation from after the initialization date and the measured skill is fiction.

### Visualization (canvas `c1`, 720×360)

Timeline with a rewind point, a red no-peeking barrier at the initialization date, a forecast curve running forward, the actual curve overlaid, and the error gap shaded between them.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Hindcast: initialize in the past, forecast forward, grade against reality"
- **Timeline axis:** 1.5px `#999` horizontal line at y=280 from x=50 to x=670; filled `#1a5276` circle radius 5 on the axis at (260, 280); bold 10px `#1a5276` centered "init date (past)" at (260, 300); 10px `#999` centered "history" at (150, 300) and "verification period" at (480, 300).
- **No-peeking barrier:** dashed (5/4) 2px `#e74c3c` vertical line at x=260 from y=50 to y=280; bold 11px `#e74c3c` left-aligned "NO PEEKING" at (268, 64); 10px `#666` left-aligned "no data after this date enters the model" at (268, 80).
- **Actual curve:** 2px `#555` polyline over the full span through (60, 180), (110, 168), (160, 185), (210, 172), (260, 178), (330, 165), (400, 185), (470, 172), (540, 195), (610, 185), (650, 200).
- **Hindcast curve:** 2.5px `#1a5276` polyline starting at the actual value (260, 178) through (330, 172), (400, 165), (470, 148), (540, 155), (610, 138), (650, 142) — drifting away from the actual.
- **Shaded error gap:** filled `rgba(231,76,60,0.18)` polygon between the two curves from x=260 to x=650 (forecast points forward, actual points reversed back).
- **Curve labels:** bold 10px `#1a5276` left-aligned "hindcast" at (654, 140); 10px `#666` left-aligned "actual" at (654, 204); bold 10px `#e74c3c` centered "error gap" at (500, 125) with a thin 1px `#e74c3c` connector from (500, 130) down to (505, 162).
- **Caption (12px `#999`, centered, y = h−14):** "Hundreds of rewound forecasts, scored honestly, are how a model version earns operations"

## 2. Skill means beating a baseline

Raw accuracy flatters every forecaster, because two free forecasts — the historical average and "tomorrow equals today" — already score surprisingly well on most systems.

- **Climatology is free:** The historical average for the date is a forecast that costs nothing.
- **Persistence is free:** Predicting that tomorrow equals today is often embarrassingly strong.
- **Skill is the gap:** Skill score measures improvement over the baseline, not accuracy alone.
- **Physics is no excuse:** An intensity model that loses to persistence is worthless.
- **Lead time matters:** Baselines are hardest to beat at short leads and long horizons.
- **Honest baselines:** Choose the baseline an honest skeptic would choose, not the weakest one.
- **Report both:** Publish model error and baseline error side by side, always.

Key point: A forecast has skill only relative to a baseline — raw accuracy without the free-forecast comparison is a vanity number.

### Visualization (canvas `c2`, 720×360)

Grouped bar chart of forecast error for persistence, climatology, and the model at two lead times, with the model barely beating the baselines at the long lead.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Forecast error vs free baselines — the model's edge shrinks with lead time"
- **Axes:** 1.5px `#999` vertical line from (70, 70) to (70, 290) and horizontal baseline at y=290 from x=70 to x=660; 10px `#666` label "forecast error" rotated −90° centered at (52, 180); lower-is-better note 10px `#999` left-aligned at (76, 64): "lower is better".
- **Legend (top right, starting (470, 56), 16px spacing):** 12×12 swatches with 10px `#666` left-aligned labels — fill `#ccc` stroke `#999` "persistence (tomorrow = today)"; fill `#fff` stroke 1.5px `#e67e22` "climatology (historical average)"; fill `rgba(26,82,118,0.35)` stroke `#1a5276` "simulation model".
- **Group 1 — "short lead (24 h)" (bold 11px `#555` centered at (220, 310)):** bars 60 wide on baseline y=290 — persistence at x=110 height 90 (fill `#ccc`, 1px `#999` stroke); climatology at x=190 height 110 (fill `#fff`, 1.5px `#e67e22` stroke); model at x=270 height 45 (fill `rgba(26,82,118,0.35)`, 1.5px `#1a5276` stroke).
- **Group 2 — "long lead (5 days)" (bold 11px `#555` centered at (520, 310)):** persistence at x=410 height 175; climatology at x=490 height 150; model at x=570 height 140 — same styles as group 1.
- **Annotation:** bold 11px `#e74c3c` centered at (560, 108): "barely better than a free forecast"; thin 1px `#e74c3c` connector from (580, 114) down to (600, 146).
- **Caption (12px `#999`, centered, y = h−14):** "Skill is the gap between the model bar and the baseline an honest skeptic would pick"

## 3. Calibration: do your 70%s happen 70% of the time

Accuracy asks whether the forecast was right; calibration asks whether the stated probabilities were honest — group every "70% chance" forecast ever issued and check the observed frequency.

- **Group by stated probability:** Collect all forecasts that said 70% and count the outcomes.
- **Reliability diagram:** Plot stated probability against observed frequency per bin.
- **The diagonal is perfect:** Calibrated forecasts sit on the stated = observed line.
- **Overconfidence sags:** Overconfident models bow below the diagonal at high probabilities.
- **Sharpness still matters:** Always predicting the base rate is calibrated but useless.
- **The real goal:** Sharp and calibrated — confident forecasts that are right at the stated rate.
- **Recalibration:** Systematic bias can be corrected post-hoc by remapping probabilities.

Key point: A model that says 90% when the event happens 70% of the time is lying by a fixed amount every day — the reliability diagram makes that lie visible, and recalibration can remove it.

### Visualization (canvas `c3`, 720×380)

Reliability diagram: diagonal reference for perfect calibration, an overconfident curve sagging below it at high probabilities, dot sizes indicating forecast counts, and an annotation at the worst bin.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Reliability diagram — stated probability vs observed frequency"
- **Axes:** 1.5px `#999` lines — vertical from (180, 70) to (180, 310), horizontal from (180, 310) to (540, 310). Ticks 10px `#999`: x labels "0", "0.5", "1.0" centered at (180, 326), (360, 326), (540, 326); y labels "0", "0.5", "1.0" right-aligned at (172, 314), (172, 194), (172, 74). Axis titles 11px `#666`: "stated probability" centered at (360, 346); "observed frequency" rotated −90° centered at (140, 190).
- **Mapping:** probability p maps to x = 180 + 360p and y = 310 − 240p.
- **Diagonal:** dashed (5/4) 1.5px `#27ae60` line from (180, 310) to (540, 70); 10px `#27ae60` label "perfect calibration" rotated −33.7° centered near (350, 178) along the line.
- **Overconfident curve:** 2.5px `#e74c3c` polyline through the (stated, observed) pairs (0.1, 0.12), (0.3, 0.28), (0.5, 0.42), (0.7, 0.58), (0.9, 0.70) → points (216, 281), (288, 243), (360, 209), (432, 171), (504, 142).
- **Count dots:** filled `#e74c3c` circles at those five points, radii 4, 6, 8, 7, 5 (dot size = number of forecasts in the bin).
- **Annotation:** bold 11px `#e74c3c` left-aligned two lines at (520, 100): "says 90%," and at (520, 116): "happens 70%"; thin 1px `#e74c3c` connector from (516, 112) to (508, 136).
- **Dot-size legend (left, starting (60, 250)):** filled `#e74c3c` circle radius 3 at (68, 250) with 10px `#666` left-aligned "few forecasts" at (80, 254); filled `#e74c3c` circle radius 7 at (68, 276) with "many forecasts" at (80, 280).
- **Caption (12px `#999`, centered, y = h−14):** "Below the diagonal is overconfidence — the goal is sharp AND calibrated, not just one"

## 4. Tuning vs overfitting the planet

Physical models have free parameters that get tuned to reproduce history — and a model tuned until it fits history perfectly has usually memorized it rather than understood it.

- **Free parameters:** Cloud physics, mixing rates, and contact rates all get tuned to history.
- **Compensating errors:** Two wrong components can cancel to match the target metric.
- **Divergence later:** Cancelling errors match history together, then diverge out of sample.
- **Validate components:** Check each component against its own observations, not just the headline.
- **Hold out whole events:** Reserve a hurricane season or an epidemic wave, not random points.
- **Correlated errors:** Random-point holdouts leak because errors are correlated in time.
- **Perfect fit is a warning:** A flawless historical fit usually means memorization.

Key point: The headline metric can be matched by wrong components canceling each other — hold out entire events and validate each component against its own data, or the model has memorized the planet instead of learning it.

### Visualization (canvas `c4`, 720×380)

Two model curves fitting a historical series almost identically, then diverging sharply in the forecast region beyond an end-of-training line, with the actual curve continuing between them.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Identical on history, opposite in the future — the signature of compensating errors"
- **Forecast region:** filled `#f8f9fa` rectangle (430, 50) 240×270; 10px `#999` centered "unseen future" at (550, 66).
- **Axis:** 1.5px `#999` horizontal line at y=320 from x=50 to x=670; 10px `#999` centered "time →" at (360, 338).
- **End-of-training line:** dashed (5/4) 2px `#999` vertical line at x=430 from y=50 to y=320; bold 11px `#666` centered "end of training data" at (430, 44).
- **Actual history:** 2px `#555` polyline through (60, 200), (105, 185), (150, 205), (195, 190), (240, 210), (285, 195), (330, 215), (385, 200), (430, 205).
- **Model A:** 2px `#1a5276` polyline hugging the actual within ±4px on history — (60, 197), (105, 188), (150, 202), (195, 187), (240, 213), (285, 192), (330, 218), (385, 197), (430, 203) — then diverging upward through (490, 180), (550, 150), (610, 125), (655, 108).
- **Model B:** 2px `#e67e22` polyline also hugging the actual — (60, 203), (105, 183), (150, 208), (195, 193), (240, 207), (285, 198), (330, 212), (385, 203), (430, 208) — then diverging downward through (490, 235), (550, 262), (610, 285), (655, 300).
- **Actual continuation:** dashed (4/3) 2px `#555` polyline from (430, 205) through (490, 198), (550, 210), (610, 200) to (655, 206), running between the two model curves.
- **Curve labels (10px, left-aligned at x=658):** `#1a5276` "model A" at y=108; `#555` "actual" at y=206; `#e67e22` "model B" at y=300.
- **Annotation:** bold 11px `#e74c3c` centered at (240, 120): "compensating errors — both match the target metric"; thin 1px `#e74c3c` connector from (240, 126) down to (245, 200).
- **Caption (12px `#999`, centered, y = h−14):** "Two tuned models, one history, two futures — hold out whole events to tell them apart"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above. The h1 carries no index number.
- **Bullet style:** each bullet is a bold label + one short complete sentence that fits on one line at normal page width; split dense content into more bullets rather than longer ones. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 360/360/380/380 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`. Every canvas gets a bold 14px `#1a5276` centered title at y=22 and a 12px `#999` centered caption at y = h−14.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`, `rgba(231,76,60,0.18)`.
- No nav bar, no back/home links, no cross-references to other pages, no item counts in text.
