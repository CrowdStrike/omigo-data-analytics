# Pitfall: Wrong Missing Data Assumptions

**Page type:** detail page (card-section layout: h2 per section, two-column table with text left 45% / canvas right 55%)
**HTML title tag:** Wrong Missing Data Assumptions

**Subtitle:** Treating all missing data the same when 'why it's missing' matters.

## The Problem

**Tags:** `the trap` (red), `missing data` (blue)

- **Blanket imputation** — mean or median fill is applied without asking why values are missing
- **Missingness is signal** — the absence of a value often predicts the target by itself
- **Medical** — a test never ordered usually means the patient looked healthy enough to skip it
- **Financial** — a blank income field can mean unemployment or concealment, both higher risk
- **Behavioral** — a missing review usually means the customer never engaged with the product
- **Sensors** — a NULL reading often coincides with equipment failure, the very event to detect

*Example:* A loan model imputes the 30% missing second incomes with the median, erasing a 22% vs 9% default-rate gap.

**Impact:** Imputing an informative gap tells the model "missing = average" and replaces a real risk signal with noise.

### Visualization (canvas `c1`, 720×300)

Two-bar comparison of default rate for rows with the second income present vs missing.

- **Title (bold 14px, `#1a5276`, centered):** "Default Rate: Second Income Present vs. Missing".
- **Bars:** width 120, baseline y=250, height = rate × 160 × 10:
  - Present bar at x=200: 9%, fill `rgba(39, 174, 96, 0.6)`, 2px `#27ae60` border; bold 16px green value "9%" above; labels below: "Second Income" / "Present" (12px `#333`) and "n = 4,200" (10px `#666`).
  - Missing bar at x=520: 22%, fill `rgba(231, 76, 60, 0.6)`, 2px `#e74c3c` border; bold 16px red value "22%" above; labels below: "Second Income" / "MISSING" and "n = 1,800".
- **Baseline:** dashed (4,3) 1px `#999` horizontal line from x=100 to x=650 at y=250.
- **Bottom annotation (bold 12px red, centered):** "2.4x higher default rate when missing. Imputing destroys this signal!".

## Why It Happens

**Tags:** `root cause` (orange), `missing mechanisms` (blue)

- **Nuisance framing** — tutorials treat missing values as a cleanup step, not a modeling decision
- **MCAR** — missingness unrelated to anything; rare in real data and the only safe case to impute
- **MAR** — missingness depends on observed variables; can be modeled and imputed with care
- **MNAR** — missingness depends on the unobserved value itself; the absence IS the feature
- **Default assumption** — pipelines silently assume MCAR while real data is usually MAR or MNAR

*Example:* "Time on product page" is NULL for the 15% of drive-by visitors, yet mean imputation makes them look engaged.

**Root Cause:** You assume MCAR and impute the median, but the data is MNAR — absence itself signaled low risk, and the fill hides it.

### Visualization (canvas `c2`, 720×300)

Three labeled boxes explaining the three missing-data mechanisms.

- **Title (bold 14px, `#1a5276`, centered):** "Three Mechanisms of Missing Data".
- **Three boxes** 180×180 at y=60, centered at x=120, 360, 600, white fill with 3px colored border; each contains a bold 14px colored heading, an 11px `#333` description, a bold 10px colored safety verdict, and a 10px `#666` example:
  1. **MCAR** — green `#27ae60`; description: "Missing Completely / At Random"; verdict: "Safe to impute"; example: "Sensor battery died".
  2. **MAR** — orange `#e67e22`; description: "Missing / At Random"; verdict: "Impute cautiously"; example: "Young people skip / landline phone".
  3. **MNAR** — red `#e74c3c`; description: "Missing Not / At Random"; verdict: "DO NOT impute"; example: "High earners hide / income".
- **Bottom annotation (bold 11px red, centered):** "Most real-world missing data is MAR or MNAR. Assuming MCAR destroys signal.".

## The Correct Approach

**Tags:** `the fix` (green), `indicators` (blue)

- **Test first** — compare target rates for missing vs present rows before choosing any strategy
- **Indicator variable** — add a binary is_missing flag for every feature with gaps
- **Detection** — a target-rate gap over ~10 points hints at informative missingness, not a rule
- **Native handling** — trees like XGBoost and LightGBM route missing values without imputation
- **Careful imputation** — prefer model-based or multiple imputation over a blanket mean fill
- **Keep the flag** — retain the indicator so the model knows an imputed value was fabricated

*Example:* An is_new_customer flag for the 40% with missing purchase history captures their 18% vs 8% default rate and lifts accuracy 7%.

**Fix:** If missingness predicts the target, add a missing indicator and either skip imputation (tree models) or impute and keep the flag.

### Visualization (canvas `c3`, 720×300)

Decision-tree flowchart for handling missing data.

- **Title (bold 14px, `#1a5276`, centered):** "Missing Data Handling Decision Tree".
- **Start box:** 160×40 centered at top (y=50), white fill, 2px `#2980b9` border, 11px blue text: "Feature has" / "missing values". Blue arrow down.
- **Decision box:** 200×50 centered (y=110), white fill, 2px orange `#e67e22` border, bold 11px orange text: "Does target rate differ" / "missing vs. non-missing?".
- **Left branch ("No", 9px green label):** green `#27ae60` 2px elbow connector to a 140×50 box at (80, 180), white fill, 2px green border: bold "No: Likely MCAR", then "Safe to impute" / "(mean/median)".
- **Right branch ("Yes", 9px red label):** red `#e74c3c` 2px elbow connector to a 160×80 box at (490, 180), white fill, 2px red border: bold "Yes: Informative!", then "Create \"is_missing\"" / "indicator feature" / "Use tree models OR" / "impute + keep indicator".
- **Bottom line (bold 11px `#1a5276`, centered):** "Always test if missingness predicts the target before imputing.".

## Regeneration instructions

- **Layout:** `.card-section` per section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (border-collapse, full width) with one `<tr>`: `td.text-col` (45%) holding `.tags` pills + `<ul>` bullets + `.example` italic paragraph + `.key-point` callout; `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `li b` colored `#1a5276`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card links use `.html` extensions.
