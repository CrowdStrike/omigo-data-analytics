# Impute All Missing Values with Column Mean

**Page type:** detail page (card-section layout: h2 section headings, two-column layout table with text left 45% / canvas right 55%)
**HTML title tag:** Impute All Missing Values with Column Mean

**Subtitle:** Destroys the signal that absence carries — missingness is often the strongest predictor

## The Anti-Pattern

Treating all NULLs as "data collection failure" and filling them with the population average destroys missing-as-signal. The absence of a value often encodes domain-critical information that no imputation can recover.

**Key-point callout (red left border):** Filling NULLs with the mean collapses distinct subpopulations into a single artificial cluster at the center of the distribution.

*Domain examples where this destroys signal:* (italic `.example` lead-in)

- **Healthcare:** A missing lab result often means "doctor didn't order it because patient is healthy" — imputing the population mean makes healthy patients look sick.
- **Cybersecurity:** A missing log entry may indicate deliberate log deletion by an attacker — imputing normal activity hides the intrusion signal.

### Visualization (canvas `c1`, 720×300)

Bar chart of 10 records where imputed NULLs all collapse to the mean value.

- **Data:** values `[72, 45, 88, 53, 60, 60, 35, 60, 91, 60]`; imputed flags `[false, false, false, false, true, true, false, true, false, true]` (bars 5, 6, 8, 10 are imputed and all equal 60).
- **Axes:** margins left 60 / bottom 40 / top 20 / right 20; y from 0 to 100 with gray (`#666`, 11px) labels at 0, 25, 50, 75, 100 and light gridlines `#f0f0f0`; axis lines `#ccc`. Rotated y-axis title "Value" and x-axis title "Records", both `#1a5276` 12px.
- **Bars:** 8px gaps; observed bars filled `rgba(26,82,118,0.35)`; imputed bars filled solid red `#e74c3c` with `#c0392b` border (width 1.5) and a red 9px label "NULL→mean" above each.
- **Mean line:** horizontal dashed orange line (`#e67e22`, dash 6/4, width 2) at value 60, labeled bold orange 11px "mean = 60" near the right end above the line.
- **Legend (top-left inside plot):** `rgba(26,82,118,0.35)` swatch + "Observed values"; red `#e74c3c` swatch + "Imputed (all collapse to mean)" (11px, text `#2c3e50`).

## The Design Pattern

Create an `is_missing` binary feature alongside any imputation. Classify the missingness mechanism before choosing a strategy:

- **MCAR** (Missing Completely at Random) — safe to impute, missingness is unrelated to data.
- **MAR** (Missing at Random) — conditional on observed variables; use conditional imputation.
- **MNAR** (Missing Not at Random) — absence IS the feature; the missingness indicator alone carries the signal.

**Key-point callout (red left border):** Test whether missingness predicts the target variable — it is often a stronger predictor than the imputed value itself.

### Visualization (canvas `c2`, 720×300)

Drawn data table showing a `val` column with NULLs alongside an `is_missing` indicator column, plus a mechanism-classification annotation.

- **Table:** at (80, 20), two columns 140px wide each, header row 30px tall filled `#1a5276` with white bold 13px headers "val" and "is_missing"; 8 data rows 28px tall.
- **Rows (val / is_missing):** 72/0, 45/0, NULL/1, 88/0, NULL/1, NULL/1, 35/0, 91/0. NULL rows get background `rgba(39, 174, 96, 0.15)` with gray `#999` "NULL" text and bold green `#27ae60` "1" in the indicator column; other rows alternate `#f9f9f9`/white with `#2c3e50` text. Row separators `#e0e0e0`, outer border `#1a5276` width 1.5, column divider `#e0e0e0`.
- **Below table:** downward green arrow (`#27ae60`) pointing from under the `is_missing` column, then bold green 14px text: "is_missing = strongest predictor!".
- **Right-side annotation** (starting ~40px right of table): `#1a5276` 12px heading "Classify mechanism:", then 11px lines in `#2c3e50`: "MCAR → safe to impute", "MAR  → conditional impute", and bold green `#27ae60` "MNAR → absence IS signal".

## Regeneration instructions

- **Layout:** anti-pattern-pairs detail page: h1 with 2px `#2980b9` bottom border, `.subtitle`, then two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"), each an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) above a `table.layout` with one row: `td.text-col` (45%) and `td.viz-col` (55%).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `ul` 0.92rem; `.example` italic `#555` 0.9rem. No nav bar, no back/home links.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and `ctx.scale` back to logical coordinates via a shared `setup(id)` helper.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
