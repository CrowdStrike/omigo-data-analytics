# Pitfall: Incorrect Values That Pass Every Check

**Page type:** detail page (card-section layout: h2 per section, two-column table with text left 50% / canvas right 50%)
**HTML title tag:** Incorrect Values That Pass Every Check

**Subtitle:** The value was wrong at the source — nothing in the pipeline broke, the number was never right.

## The Problem

**Tags:** `the trap` (red), `bad values` (blue)

- **Wrong at the source** — nothing in the pipeline broke; the value was never right
- **Sentinels** — -999, 0, 9999, and 1900-01-01 are missing data wearing a number's clothes
- **Null audits pass** — isnull() sees a real number, so a missing-value report shows zero problems
- **Type-valid nonsense** — age 200, quantity -3, and 140% clear every schema check
- **Form defaults** — a dropdown left on its first option becomes the column's modal value
- **Manual entry** — one fat-fingered digit turns 70 kg into 700 kg with no trace left behind
- **Sensor drift** — an uncalibrated probe reports confidently wrong readings for weeks
- **Means poisoned** — three -999 rows out of twelve drag a mean of 40.0 down to -219.75

*Example:* A 12-row age column stores -999 three times, so the mean reads -219.75 instead of 40.0 and the null audit finds nothing.

**Impact:** The missing-value report shows zero problems while the column mean is off by hundreds — the check that would catch it was never written.

### Visualization (canvas `c1`, 720×300)

Bar chart of a 12-row age column with three sentinel rows, printing the naive and cleaned means computed from the plotted literals.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Sentinel Values Poison the Mean (Illustrative Example)".
- **Data (hardcoded literal array, order preserved):** `[34, 41, 29, -999, 52, 38, -999, 45, 61, 27, -999, 33]`.
- **Geometry:** zero line at y=170 (1px `#999`) spanning x=55 to x=690; 12 bars, pitch 635/12 ≈ 52.9, bar width 34, bar center = 55 + 52.9·i + 26.45; positive scale = 110/70 px per unit.
- **Valid bars:** fill `rgba(26,82,118,0.35)`, 1px `#1a5276` stroke, drawn upward from the zero line; value label 9px `#444` centered above each bar.
- **Sentinel bars:** drawn downward from y=170 to y=205, fill `rgba(231,76,60,0.6)`, 2px `#e74c3c` stroke, with a dashed (3,2) 1px `#e74c3c` line across their bottom edge marking the truncated axis; bold 9px `#e74c3c` label "-999" at y=218.
- **Axis caption:** 10px `#666` centered at y=234: "12 rows of customer_age — 3 sentinels, 0 nulls".
- **Computed results (computed in JS from the literal array at render time, never hardcoded):**
  - bold 12px `#e74c3c`, centered, y=256: "Naive mean of all 12 rows: " + naive.toFixed(2) → −219.75
  - bold 12px `#27ae60`, centered, y=274: "Cleaned mean of 9 valid rows: " + clean.toFixed(1) → 40.0
  - 10px `#666`, centered, y=291: "isnull() count = 0 — the null audit reports no problem".
- **Arithmetic:** valid sum 34+41+29+52+38+45+61+27+33 = 360, n = 9, mean = 40.0. Naive sum 360 + 3·(−999) = 360 − 2997 = −2637, n = 12, mean = −219.75.

## Why It Happens

**Tags:** `root cause` (orange), `validation gaps` (blue)

- **Type, not plausibility** — schema validation asks "is it an INT", never "can it be 200"
- **No range contract** — columns ship without a declared min or max, so nothing can be violated
- **Legacy sentinels** — -999 predates nullable columns and survives every feed nobody rewrites
- **Silent defaults** — a form's first option is submitted by silence, never by an actual choice
- **No feedback loop** — the person typing the value never sees the model that consumes it
- **Sensors fail soft** — drift stays plausible, so no alarm separates the reading from the truth
- **Models have no physics** — to a tree, age 200 is just a large number with unusual leverage
- **Aggregates hide it** — a poisoned mean still returns a number, so the dashboard looks healthy

*Example:* A warehouse scale drifts high for six weeks, so every shipment weight in that window is type-valid, in range, and wrong.

**Root Cause:** Validation confirms the type and stops there — no layer in the pipeline holds an opinion about which values are physically possible.

### Visualization (canvas `c2`, 720×300)

Mockup of a validation report: a checklist table where every check is green and every value is impossible.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Validation Report: All Checks Green, All Values Absurd".
- **Header row (bold 10px `#666`, y=48):** "column = value" left-aligned at x=25; centered headers "type" x=265, "not null" x=345, "range rule" x=425, "verdict" x=505; "origin" left-aligned at x=570.
- **Header rule:** 1px `#ccc` line from x=20 to x=700 at y=54.
- **Six rows (hardcoded literal array), pitch 27, first baseline y=76:**
  | column = value | type | origin |
  |---|---|---|
  | customer_age = 200 | INT | manual entry |
  | order_qty = -5 | INT | manual entry |
  | completion_pct = 140 | FLOAT | bad formula |
  | signup_date = 1900-01-01 | DATE | form default |
  | tenure_months = -999 | INT | sentinel |
  | session_secs = -12 | INT | clock skew |
- **Per row:** 11px `#444` "column = value" at x=25; bold 11px `#27ae60` "✓" centered at x=265 with the type name in 9px `#666` just below it; bold 11px `#27ae60` "✓" centered at x=345; 10px `#e67e22` "none" centered at x=425 (no bound declared); bold 10px `#27ae60` "PASS" centered at x=505; 9px `#666` origin at x=570. Alternate row background `rgba(26,82,118,0.04)` spanning x=20 to x=700.
- **Summary strip:** (20,232) 680×34, fill `rgba(39,174,96,0.08)`, 1px `#27ae60` stroke; bold 10px `#27ae60` three items evenly placed: "type errors: 0", "isnull() count: 0", "rows rejected: 0" — all literal zeros, matching the rows above (no row fails, none is null).
- **Computed footer:** bold 11px `#e74c3c` centered at y=284: rows.length + " impossible values, 0 failures — the type is correct, the number is not" (count computed from the row array length → 6).

## The Correct Approach

**Tags:** `the fix` (green), `plausibility` (blue)

- **Bounds beside the schema** — declare min, max, and units per column next to its type
- **Sentinel registry** — list each source's known placeholders and map them to real nulls at ingest
- **Convert before imputing** — a sentinel that reaches the imputer is averaged in as a real value
- **Spike detection** — one value holding an implausible share of the mass is a default or sentinel
- **Rule of thumb** — review any single value holding above ~20% of a continuous column's rows
- **Second source** — cross-check a suspect column against an independent system, not itself
- **Fail loudly at ingest** — reject the batch and alert a human rather than silently clipping
- **Never clip quietly** — clamping 200 to 100 hides the defect and keeps the bad row in training

*Example:* In a tenure column the default 0 holds 1,240 of 2,900 rows (42.8%), which a spike check catches long before training.

**Fix:** Give every column a plausibility contract and a sentinel list, then fail the batch at ingest when a value violates either one.

### Visualization (canvas `c3`, 720×300)

Value-frequency histogram whose first bar is a default, with the spike share and the threshold computed from the plotted counts.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Spike Detector: a Default Hiding in job_tenure_months".
- **Data (hardcoded literals):** months `[0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60]`, counts `[1240, 210, 260, 190, 230, 150, 170, 110, 140, 90, 110]`.
- **Geometry:** baseline y=232 (1px `#999`, x=55 to x=692); 11 bars, pitch 637/11 ≈ 57.9, bar width 40, center = 55 + 57.9·i + 28.95; scale = 160 / max(counts) px per row, max computed in JS.
- **Bars:** the `0` bar filled `rgba(231,76,60,0.6)` with 2px `#e74c3c` stroke; all others `rgba(26,82,118,0.35)` with 1px `#1a5276` stroke. Bold 10px `#e74c3c` label above the spike bar showing its count and share, both computed at render time → "1,240 (42.8%)".
- **Threshold line:** dashed (4,3) 1.5px `#e67e22` horizontal line at count = 0.20 × total, from x=55 to x=692, with a 9px `#e67e22` label above its right end: "20% of rows = 580" (value computed in JS).
- **Axis labels:** 9px `#666` month values centered under each bar at y=246; 10px `#666` caption "months of reported tenure" centered at y=262.
- **Computed footer:** bold 11px `#e74c3c` centered at y=282: "One value holds 42.8% of 2,900 rows — almost always a default or a sentinel" (share and total computed in JS); 10px `#666` centered at y=295: "Illustrative Example — flag single-value mass above 20% at ingest".
- **Arithmetic:** total = 1240+210+260+190+230+150+170+110+140+90+110 = 2,900. Share = 1240/2900 = 42.76% → 42.8%. Threshold = 0.20 × 2,900 = 580, and 1,240 > 580.

## Regeneration instructions

- **Layout:** `.card-section` per section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (border-collapse, full width) with one `<tr>`: `td.text-col` (50%) holding `.tags` pills + `<ul>` bullets + `.example` italic paragraph + `.key-point` callout; `td.viz-col` (50%) holding the canvas. Never narrow the viz column below 50% — shrink a chart with the canvas's own max-width instead.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. No nav bar, no back/home links, no cross-page links.
- **Bullets:** bold colored label + a short phrase, ~90-100 characters including the label, 5-8 per section; one line is the target, and a wrap is acceptable when a fact would otherwise be lost.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `li b` colored `#1a5276`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart data:** all chart data is hardcoded literal arrays — the counts and the sentinel positions carry the lesson. No `Math.random()` and no seeded draw is needed here. Every statistic printed beside a chart (means, counts, share, threshold) is computed in JS from the plotted literals at render time.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card links use `.html` extensions.
