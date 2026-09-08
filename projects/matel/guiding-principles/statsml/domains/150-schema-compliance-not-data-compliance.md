# Schema Compliance ≠ Data Compliance

**Page type:** detail page (h2 section headers, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 150. Schema Compliance ≠ Data Compliance

**Subtitle:** Schema matching is rubber-stamping. Necessary but not sufficient. Data types match, formats align, columns exist — and the underlying distributions have shifted completely. New customer segments, new flows, new behaviors silently break every model trained on the old distribution. Nothing "broke" — the data just stopped meaning what it used to mean.

## Callout (philosophy box)

**The core problem:** Schema alignment is a necessary but nowhere near sufficient condition for data compatibility. A new feature can follow the exact same schema — same column names, same types, same format — while introducing a completely different population with different distributions, different correlations, and different causal relationships. This is a SILENT failure: no schema validation catches it, no type check flags it, no pipeline errors. The data just quietly becomes non-representative of what the model learned.

## New Customer Segment Enters — Same Schema, Different Distribution

**Feature Attracts New Users Who Behave Nothing Like Existing Users**

- **The scenario:** Product adds a free tier; `signup`, `pageview`, `purchase` log unchanged.
- **New population:** Free-tier users churn far more often and convert far less than paid users.
- **Behavior differs too:** Browsing patterns, geographies, and devices all differ from paid.
- **The silent failure:** A model trained on paid users now scores a different population.
- **No alert fires:** Accuracy sags, but the data still "looks normal" structurally.
- **Why schema checks miss it:** `purchase_amount` float, `session_duration` integer, `country` string.
- **Distribution shift:** Free-tier mode sits at $12 while the paid mode stays at $45.
- **Blended mean moves:** Mixing 60% free with 40% paid pulls the mean from $45 to $26.
- **The compounding effect:** Retraining on mixed old and new data suits neither segment well.
- **Everyone loses:** Paid users get free-tier-diluted recommendations, free users paid-biased ones.

### Visualization (canvas `c1`, 720×300)

Two overlaid probability-density curves on L-shaped axes, `#f9f9f9` background. Illustrative Example — the two mixture components are constructed, not measured.

- **Title (bold 13px `#1a5276`, top center):** "purchase_amount distribution: before vs after free tier".
- **Axes:** `#333` width 1.5; margins left 50, right 30, top 45, bottom 50. X domain 0–100 evaluated on the integer grid 0..100, with x-axis ticks and `$0`/`$25`/`$50`/`$75`/`$100` labels.
- **Density function:** true normal pdf `exp(-0.5((x-m)/s)^2) / (s*sqrt(2*pi))`, so mixture weights are genuine population shares. Both curves are scaled by one common factor: `0.85 * plotH / (max of both arrays)`.
- **Before array:** single normal, mean 45, sd 12. Drawn solid blue `#3498db`, width 2.5.
- **After array:** mixture `0.6 * N(12, 8) + 0.4 * N(45, 12)` — 60% free tier, 40% paid. Drawn dashed red `#e74c3c` (dash 5/5), width 2.5.
- **Means are COMPUTED at render time** as the density-weighted mean of each drawn array (`sum(w*x)/sum(w)` over the same integer grid that is plotted), then printed with `toFixed(0)`. Do NOT hardcode the means — the before array yields $45 and the after array yields $26.
- **Labels:** bold 11px centered — blue `#3498db` "Before: paid only (mean $" + computed + ")" near the top at 55% of plot width; red `#e74c3c` "After: 60% free + 40% paid (mean $" + computed + ")" at 25% of plot width, ~35% down the plot.
- **Caption (bottom center, italic 12px `#555`):** "Illustrative Example. Schema unchanged. Types match. Distribution completely different. Model trained on blue, sees red."

## New Flow Changes the Meaning of Existing Events

**Same Event Name, Completely Different User Intent**

- **The scenario:** App adds "quick checkout" (one-tap); `purchase` still fires with the same schema.
- **What changed:** Quick-checkout buys mean less deliberation, more regret, and higher returns.
- **Category skew:** Purchased categories skew toward impulse items, not researched ones.
- **Before the feature:** `purchase` meant "researched, compared, decided, bought" = high intent.
- **After the feature:** `purchase` now ALSO means "saw button, tapped, might return it."
- **Same name, new meaning:** The model scores impulse purchases as deliberate choices.
- **The recommendation poison:** One impulse novelty buy teaches "user likes novelty items!"
- **Two failures, one shift:** The user returns the original AND resents the bad recommendations.
- **Why it's invisible:** Schema, name, and types unchanged — only SEMANTICS, which nothing validates.

### Visualization (canvas `c2`, 720×300)

Two side-by-side comparison boxes with a center badge, `#f9f9f9` background. Illustrative Example — the return rates are constructed for contrast, not measured.

- **Title (bold 13px `#1a5276`, top center):** ""purchase" event — same name, different intent after quick-checkout".
- **Left box** (280×160 at left margin 40, y = 60): fill `rgba(39,174,96,0.1)`, stroke green `#27ae60` width 2. Heading (bold 12px green, centered): "Before: "purchase" meant". Bulleted list (11px `#333`, left-aligned, 22px line spacing): "• Researched product", "• Compared alternatives", "• Deliberate decision", "• Low return rate (5%)", "• High intent signal".
- **Right box** (280×160, right-aligned with right margin 40): fill `rgba(231,76,60,0.1)`, stroke red `#e74c3c` width 2. Heading (bold 12px red, centered): "After: "purchase" ALSO means". List: "• Saw button, tapped", "• Zero deliberation", "• Impulse / accident", "• High return rate (25%)", "• Low/no intent signal".
- **Center badge (bold 12px orange `#e67e22`, three stacked centered lines between the boxes):** "SAME EVENT NAME" / "SAME SCHEMA" / "DIFFERENT MEANING".
- **Caption (bottom center, italic 12px `#555`):** "Illustrative Example. No type check, schema check, or format check catches a semantic shift."

## Feature Changes the Correlation Structure — Model Assumptions Break

**Features That Were Correlated Are No Longer (or Vice Versa)**

- **Before:** `session_duration` and `pages_viewed` correlated at r=0.85, reinforcing each other.
- **After new feature (infinite scroll):** More time spent, but fewer distinct pages on one long page.
- **Correlation collapse:** Correlation drops to r=0.20, but the model assumes they move together.
- **Off-manifold inputs:** "High time + few pages" is common now, but the model never trained there.
- **The multicollinearity shift:** `time` and `pages` were quasi-redundant signals for "engaged".
- **Now near-independent:** Barely a shared dimension left, yet no retrain fired — the SCHEMA is unchanged.
- **Feature interaction death:** The learned "high time AND high pages → high purchase" rule is wrong.
- **Prediction damage:** "High time AND low pages" dominates; prediction drops for scroll users.

### Visualization (canvas `c3`, 720×300)

Two side-by-side scatter plots, `#f9f9f9` background. Illustrative Example — points are synthetic. **Deterministic: the data comes from a seeded Park-Miller LCG, never `Math.random()`, so the figure and its labels are identical on every load and on every resize redraw.**

- **Title (bold 13px `#1a5276`, top center):** "session_duration vs pages_viewed — correlation breaks after infinite scroll". Below it at the left edge, italic 10px `#555`: "Illustrative Example".
- **Seeded PRNG (shared page helper):** `function lcg(seed) { var s = seed; return function () { s = (s * 16807) % 2147483647; return s / 2147483647; }; }`.- **Correlation helper (shared page helper):** `pearson(pts)` computes Pearson r over an array of `[x, y]` pairs by the standard covariance-over-root-product-of-variances formula.
- **Point-generation formula (identical for both panels, only the blend weight `k` and the seed differ):** for `i` in 0..59 draw `u1 = rand()` then `u2 = rand()`, and emit `x = 0.1 + 0.8*u1`, `y = 0.1 + 0.8*(k*u1 + (1-k)*u2)`. Blending x's own uniform with an independent uniform sets the correlation directly and needs no clamping, so no points are distorted at the panel edges.
- **Left panel (before):** seed **20250150**, blend weight **k = 0.615**. Build the 60-point array first, then draw it. Computed Pearson r = 0.8519, which prints as **0.85**. Dots: 3px radius, fill `rgba(52,152,219,0.5)`.
- **Right panel (after infinite scroll):** seed **20250151**, blend weight **k = 0.14** — a deliberately weak but genuine dependence, not two independent uniforms (independent uniforms would give r ≈ 0, contradicting the stated r = 0.20). Computed Pearson r = 0.1982, which prints as **0.20**. Dots: 3px radius, fill `rgba(231,76,60,0.5)`.
- **Panel headings are COMPUTED at render time, never hardcoded:** left (bold 11px `#1a5276`, centered above) `'Before (r=' + pearson(leftPts).toFixed(2) + ')'`; right (bold 11px `#e74c3c`) `'After infinite scroll (r=' + pearson(rightPts).toFixed(2) + ')'`. If a future edit changes a seed or a `k`, the heading follows the data automatically.
- **Layout:** margins left/right 50, top 45, bottom 45; each scatter panel is (plot width)/2 − 20 wide, outlined `#333` width 1.
- **Axis labels (10px `#333`):** "session_duration →" centered below each panel; "pages →" rotated vertically at each panel's left edge.

## Survivorship Shift — Who You See Changes Without Schema Changing

**New Feature Retains Different Users — Data "Improves" But It's Selection Bias**

- **The scenario:** New onboarding cuts signup drop-off from 40% never returning to only 20%.
- **Who survives changed:** Schema unchanged, but you now retain the less-engaged segment that used to go.
- **The metrics illusion:** "Average engagement per user went DOWN after the feature!" Panic.
- **The arithmetic:** Per 100 signups, 60 engaged survive before; after, 60 engaged plus 20 casuals.
- **Denominator, not numerator:** Engaged users hold at 8.2 sessions/week; the newly retained casuals sit at 1.0.
- **Mean falls mechanically:** (60×8.2 + 20×1.0) / 80 = 6.4 sessions/week, a 22% drop with zero behavior change.
- **The model confusion:** The churn model learned "<3 sessions in first week = churn" on old data.
- **Same signal, new meaning:** Onboarding spreads sessions over 2 weeks, so <3-session users stay.
- **The A/B test contamination:** Control and treatment both draw on a new base population.
- **Baseline invalidated:** Your "expected" baseline is the old population, so shifts look causal.

### Visualization (canvas `c4`, 720×300)

Two horizontal bars comparing average engagement before/after, `#f9f9f9` background. Illustrative Example — the cohort counts and per-segment rates are constructed.

- **Title (bold 13px `#1a5276`, top center):** "Who survives changed — metrics look worse but users are the same".
- **Model constants (declared once at the top of the draw function):** cohort 100 signups; engaged survivors 60 at 8.2 sessions/week; casual survivors newly retained after the feature 20 at 1.0 sessions/week.
- **All printed statistics are COMPUTED from those constants at render time, never hardcoded:** before average = 8.2; after average = `(60*8.2 + 20*1.0) / 80` = 6.4; the drop = `(1 - after/before) * 100` = 22%; before churn = `(100-60)/100` = 40%; after churn = `(100-60-20)/100` = 20%. Print averages with `toFixed(1)` and percentages with `toFixed(0)`.
- **Layout:** left margin 180 (room for row labels), right margin 40, top 55; bar height 50, gap 30; bar scale = width − margins.
- **Bar lengths are proportional to the values they encode.** The top bar occupies 80% of max width; the bottom bar occupies `0.80 * after / before` of max width (= 62.4%). A bar whose length does not track its own number is a chart that lies.
- **Top bar:** row label (11px `#1a5276`, right-aligned, two lines): "Before: only engaged" / "users survived (40% churn)" — the 40% computed. Bar solid green `#27ae60`, outlined `#333`. In-bar label (bold 12px white, centered): "Avg engagement: 8.2 sessions/week" with the value computed.
- **Bottom bar:** row label: "After: casuals also retained" / "(only 20% churn)" — computed. Bar solid orange `#e67e22`, outlined `#333`. In-bar label: "Avg engagement: 6.4 sessions/week", computed. To the right of the bar (bold 12px red `#e74c3c`, left-aligned): ""Engagement dropped 22%!"" with the percentage computed.
- **Bottom notes (centered):** green `#27ae60` 11px: "Reality: the 60 engaged users still average 8.2. You added 20 casuals to the denominator." (counts and rate computed). Then italic 12px `#555`: "Illustrative Example. Population shift masquerading as metric degradation. Schema is identical."

## The Gradual Drift Nobody Notices

**New Feature Doesn't Shift Distributions Overnight — It Shifts Them A Fraction Of A Percent Per Week**

- **The insidious version:** Adoption creeps up week over week, each step inside normal variance.
- **No alert fires:** Distributions shifted a lot, but SO GRADUALLY no window comparison tripped.
- **The boiling frog:** Accuracy degrades 0.1% per week, so 26 weeks compounds to 2.6% cumulative.
- **What the detector watches:** Week-over-week shift stays flat at 0.1%, far under a 1.5% alert threshold.
- **The mismatch:** Cumulative drift passes 1.5% around day 105, but nothing compares cumulative to the threshold.
- **Unpinnable onset:** Each individual week looked fine, so you cannot say WHEN it started.
- **The attribution problem:** By month 6, campaigns, seasonal shifts, and other features changed too.
- **Needs day-one tracking:** Isolating the feature needs feature-specific drift metrics from day one.
- **Why standard drift detection fails:** Detectors compare a "recent window" to a "reference window".
- **Point-in-time blindness:** A per-window shift under threshold never trips — track cumulative drift instead.

### Visualization (canvas `c5`, 720×280)

Line chart: a slowly rising cumulative-drift line against the flat per-window signal the detector actually watches and a flat alert threshold, `#f9f9f9` background. Illustrative Example — the drift rate and threshold are constructed.

- **Title (bold 13px `#1a5276`, top center):** "Cumulative drift outruns the per-window signal a detector compares".
- **Model constants:** horizon 182 days (26 weeks); accuracy degradation 0.1% per week, i.e. `perDay = 0.1/7`; alert threshold 1.5% shift between adjacent 7-day windows. Y axis max 3.0%.
- **Axes:** L-shaped `#333` width 1.5; margins left 50, right 30, top 50, bottom 50. Y ticks with labels `0%`, `1%`, `2%`, `3%`; y-axis title (rotated, 10px `#555`): "accuracy drift".
- **Cumulative line:** red `#e74c3c` width 2.5; 183 points over days 0–182, `drift = day * perDay`, scaled against the 3.0 axis max. **The endpoint value is COMPUTED** (`perDay * 182` = 2.6) and printed, never hardcoded.
- **Per-window line:** solid orange `#e67e22` width 2 at a constant 0.1% — the week-over-week delta the detector sees. Label (10px orange, left-aligned above it): "What the detector compares: 0.1% per window" with the value computed.
- **Alert threshold:** horizontal dashed green line (`#27ae60`, dash 5/5, width 2) at 1.5% on the same scale, labeled (11px green, left-aligned above the line): "Alert threshold: 1.5% window-over-window".
- **Crossing marker:** the day cumulative drift passes the threshold is **computed** as `round(1.5 / perDay)` = day 105; draw a thin gray dashed vertical guide there and label it (10px `#555`, left-aligned, ~20px above the x-axis): "cumulative crosses 1.5% at day 105" with the threshold and day computed.
- **Daily check markers (10px green `#27ae60`, centered just below the x-axis):** "✓ ok" at days 30, 60, 90, 120, 150, 180, plus tick labels for those days.
- **Cumulative annotation (right-aligned at top right of plot):** bold 11px red "Cumulative: 2.6% drift" (computed) with 10px second line "(no window comparison ever exceeded threshold)".
- **Caption (bottom center, italic 12px `#555`):** "Illustrative Example. Each window: "within normal variance." After 26 weeks: significant drift. The frog boiled slowly."

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle` + `.philosophy` callout, then one `h2` per section, each followed by a `.obj-table` (full-width table, single `<tr>`): left `<td>` (50%) holds `.obj-title` div + `<ul>` of bullets, right `<td>` (50%, centered) holds the canvas.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border, padding-bottom 8px; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Immediately after `setup`, the script defines two shared helpers used by the chart functions: `lcg(seed)` (seeded Park-Miller LCG) and `pearson(pts)` (Pearson r over `[x, y]` pairs). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms) — which is exactly why the data must be seeded, or a resize would silently change the figure.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, light blue `#3498db`/`rgba(52,152,219,0.5)`, gray text `#555`/`#333`; chart backgrounds `#f9f9f9`.
- **Determinism rule (applies to every chart on this page):** no chart may call `Math.random()`. Any generated data uses the shared seeded `lcg(seed)` helper with a fixed, documented seed, so every load and every resize redraw produces byte-identical figures.
- **Computed-label rule (applies to every chart on this page):** every statistic printed on a chart — correlations, means, averages, percentages, drop sizes, crossing days, cumulative totals — is derived in JS from the exact array or constants the chart draws, then formatted with `toFixed`. No statistic is written as a string literal. This is what keeps prose, labels, and pixels reconciled when a parameter changes.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
