# Pitfall: Data Survivorship Bias

**Page type:** detail page (three `.card-section` blocks, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Data Survivorship Bias

**Subtitle:** Training data contains only records that survived retention policies, systematically missing deleted or archived cases

## The Problem

Tags: `the trap` (red), `survivorship` (blue)

- **Biased remainder** — deletions leave only successful or benign cases in the training data
- **Compliance deletion** — GDPR erases closed accounts, removing the churned users to learn from
- **Purged failures** — failed transactions vanish after 90 days, leaving only successes
- **Cleanup removal** — spam and abuse cases are deleted, erasing an abuse model's positive labels
- **Survivor-only view** — live tables hold active accounts, not the full historical population

*Example:* A churn model trained on current customers predicts 5% churn while actual churn is 18%.

**Impact:** The model learns only from survivors, so it systematically underestimates failure and churn.

### Visualization (canvas `c1`, 720×300)

Timeline line chart: true population vs surviving records diverging after a deletion event.

- **Title (bold 14px `#1a5276`, top center):** "Data Over Time — Survivors vs Full Population".
- **Timeline axis:** gray `#999` horizontal line, left=80 to right=660, at y=240; tick labels (11px `#666`): "t=0" at left, "t=6mo" at midpoint, "t=12mo" at right.
- **True population curve (red `#e74c3c`, width 2):** wavy line y = top + 80 − 20t + 15·sin(8t) across the plot; solid for the first half, then dashed (dash 4/4) at 0.3 alpha after t=0.5 (the deleted portion). Labels (11px): "True population" near the left, faded "(deleted)" near the right end.
- **Survivors curve (green `#27ae60`, width 2.5):** identical to the true curve up to t=0.5, then shifted down 30px for the second half (the biased remainder). Label: "Survivors (biased sample)".
- **Deletion event:** vertical dashed orange line (`#e67e22`, dash 6/3, width 3) at the midpoint, with bold 12px orange label "Deletion Event" above it.
- **Annotation (11px `#e74c3c`, lower right):** "Model trained here misses negative cases".

## Why It Happens

Tags: `root cause` (orange), `deletion` (blue)

- **Correlated deletion** — records are removed non-randomly, in ways tied to the outcome itself
- **User deletion** — dissatisfied users close accounts, so churners disappear from the data
- **Compliance purging** — retention rules purge inactive records; inactivity is an outcome signal
- **ETL cleanup** — jobs drop outlier rows, which often include the rare failures that matter
- **Log rotation** — short-lived error records rotate away before reaching the warehouse
- **Success-only retention** — failed transactions are discarded, leaving no negative examples

*Example:* A fraud model trains on a year of logs, but failed auth attempts are purged after 30 days.

**Root Cause:** Deletion correlates with the target variable — survivorship bias is selection bias applied over time.

### Visualization (canvas `c2`, 720×300)

Before/after bar comparison of the negative-outcome share, with an arrow showing negatives disappearing.

- **Title (bold 14px `#1a5276`, top center):** "Outcome Distribution — Full vs Surviving Records".
- **Bars (100px wide, baseline y=240, max height 160):** the negative share is drawn as a bar from the baseline — fill `#e74c3c` at 0.35 alpha, stroke `#e74c3c` width 2, bold 14px red percentage centered inside.
  - "Original Data" at x=200: negatives 30% (positives 70%)
  - "After Deletion" at x=520: negatives 12% (positives 88%)
- **Labels:** two-line 11px `#444` set labels below the baseline; red 11px "Failures / Churned" above each bar.
- **Arrow:** horizontal orange `#e67e22` arrow (width 2.5 with filled arrowhead) from x=290 to x=430 at y=140, with bold 12px orange caption above: "Negative cases disappear".
- **Baseline:** thin gray `#999` line from x=80 to x=660 at y=240.

## The Correct Approach

Tags: `the fix` (green), `archiving` (blue)

- **Archive first** — copy records to cold storage before any deletion policy runs
- **Snapshot training data** — train from point-in-time snapshots, not live survivor tables
- **Document policies** — note which retention deletions correlate with predicted outcomes
- **Track deletions** — log deletion timestamps so systematic training-data gaps are detectable
- **Separate pipelines** — keep compliance deletion and ML data apart so neither distorts the other

*Example:* A lending platform archives applications for 7 years after accounts close, so training data covers approved, denied, and defaulted loans.

**Fix:** Archive outcome-labeled data in immutable snapshots before any purge, on a pipeline that compliance deletion never touches.

### Visualization (canvas `c3`, 720×300)

Flow diagram of the correct workflow: snapshot/archive before compliance deletion, on separate paths.

- **Title (bold 14px `#1a5276`, top center):** "Correct Workflow — Archive Before Deletion".
- **Boxes (white fill, colored stroke width 3, 12px two-line centered labels in `#2c3e50`):**
  - "Production Data" — 120×50 at (80, 70), stroke `#1a5276`
  - "Snapshot + Archive" — 140×50 at (260, 70), stroke `#27ae60`
  - "ML Training Data" — 120×50 at (460, 70), stroke `#27ae60`
  - "Compliance Deletion" — 140×50 at (260, 170), stroke `#e67e22`
- **Arrows:** gray `#666` width 2: Production Data → Snapshot + Archive → ML Training Data (horizontal), and Snapshot + Archive → Compliance Deletion (vertical).
- **Path labels:** bold 11px green `#27ae60` "Immutable snapshot" above the top row; bold 11px orange `#e67e22` "Separate path" beside the vertical arrow.
- **Footnotes (11px `#666`, bottom left):** "ML pipeline uses archived data (all outcomes)" and "Compliance deletion does not affect training set".

## Regeneration instructions

- **Layout:** three `.card-section` divs, each with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, td padding 12px, vertical-align top): left `td.text-col` 45% holds `.tags` pills + `<ul>` bullets + `.example` + `.key-point`; right `td.viz-col` 55% holds one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Text blocks:** `<ul>` 0.92rem with `<b>` lead words in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem, with a `<strong>` lead ("Impact:", "Root Cause:", "Fix:").
- **Canvas:** each 720×300 intrinsic, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled via a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#444`/`#666`.
