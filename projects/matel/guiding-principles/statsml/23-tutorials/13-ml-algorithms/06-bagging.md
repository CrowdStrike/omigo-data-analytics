# Bagging

**Page type:** detail page (tutorial card-sections: h2 + two-column table.layout, text left 50% / canvas right 50%)
**HTML title tag:** Bagging

**Subtitle:** Train 100 trees on reshuffled copies of the same loans and let them vote — errors that disagree cancel, so the committee is steadier than any member

## A Hundred Loan Officers, Each Dealt a Reshuffled File

**Tags:** `core idea` (blue), `running example` (green)

- **One tree is jumpy** — swap a few of the 20 loans and its whole question list can change
- **The trick** — build 100 trees, each on its own bootstrap resample of the same 20 loans
- **Bootstrap** — draw 20 loans WITH replacement: some appear twice or thrice, some never
- **Left out** — each resample skips about a third of the loans (6 of 20 in the draw shown)
- **The vote** — a new applicant is judged by all 100 trees; the majority answer wins

*Example (italic):* Officer #1's file holds loan 5 three times and never saw loan 2 — so #1 grows a slightly different tree.

**Key point callout:** **Bagging = bootstrap + aggregating:** the same data resampled 100 ways, 100 trees, one vote at the end.

### Visualization (canvas `c1`, 720×300)

Bar chart: times each of the 20 loans was drawn in one bootstrap resample.

- **Title (bold 15px, `#1a5276`, top center):** "Officer #1's Bootstrap Resample: 20 Draws From the 20 Loans"
- **Data (counts per loan id 1–20, sums to 20, 6 zeros):** `[2, 0, 1, 1, 3, 0, 1, 2, 0, 1, 1, 0, 2, 1, 0, 1, 2, 1, 1, 0]`
- **Axes:** L-shaped `#999`; y scaled to max 3.4 with gray tick labels 0–3; x tick labels 1–20 (11px `#2c3e50`); x axis label "loan id (the 20 original loans)", rotated y label "times drawn" (gray 12px). Padding: top 52, bottom 66, left 62, right 30.
- **Bars:** drawn loans as blue bars — fill `rgba(42,120,214,0.4)`, stroke blue `#2a78d6` width 1.5, bold 12px blue count label above each. Never-drawn loans (count 0): hollow 14px-tall rectangle at the baseline stroked orange `#d95926` width 2, no fill.
- **Annotations:** bold 13px orange above the plot: "6 of 20 loans never drawn (orange) — each officer sees a different world"; bold 12px blue near loan 5's bar: "loan 5 drawn 3x".

## Five Officers Vote — Do the Arithmetic

**Tags:** `worked example` (green), `arithmetic` (blue)

- **Each is mediocre** — suppose every officer is wrong on 30% of applicants, independently
- **Majority of 5 wrong** — needs 3 or more mistakes to line up on the same applicant
- **The math** — 10(.3)³(.7)² + 5(.3)⁴(.7) + (.3)⁵ = 0.132 + 0.028 + 0.002
- **Result** — the committee errs 16% of the time; every member erred 30%
- **More voters** — 25 independent officers: about 2%; solo mistakes get outvoted

*Example (italic):* Three officers err on Alice, two others on Bob, never together — each mistake loses the vote.

**Key point callout:** **The fine print:** the vote only cancels errors that disagree — the whole trick rests on the trees erring on different applicants.

### Visualization (canvas `c2`, 720×300)

Dual line chart: committee error vs number of voters, independent vs correlated.

- **Title (bold 15px, `#1a5276`, top center):** "Committee Error vs Number of Voters (each wrong 30% alone)"
- **X axis:** voter counts `[1, 3, 5, 11, 25, 101]` evenly spaced, 12px `#2c3e50` labels, gray axis label "number of voting officers"; **Y axis:** 0–34% scale, gray tick labels 0%/10%/20%/30%, rotated label "committee error, %". L-shaped `#999` axes; padding top 50, bottom 52, left 62, right 180.
- **Series (width-3 lines with 4px dots):**
  - independent errors, solid green `#008300`: `[30, 22, 16, 8, 1.75, 0]` (binomial math)
  - correlated trees, dashed violet `#4a3aa7` (dash 6/4): `[30, 26, 24, 22, 21, 20]` (illustrative)
- **Marker at n=5:** short dashed green drop-line (dash 3/3) from the 16% point to the axis; bold 13px green labels "5 voters: 16%" / "(the worked example)".
- **Annotation (bold 13px violet, mid-chart):** "independent votes cancel — copies don't"
- **Legend (right margin, 12px):** green swatch "independent errors"; violet swatch "correlated trees" with 11px gray "(illustrative)" beneath.

## Why the Average Is Steadier Than Any One Tree

**Tags:** `where it's used` (blue), `variance` (green)

- **Rerun test** — train a single tree 10 times on 10 resamples; score one borderline applicant
- **Single trees** — the approval score bounces from 0.20 to 0.85 across reruns
- **Bagged** — average 100 trees and rerun: the score sits near 0.55 every time
- **Variance** — that bounce is variance; averaging independent-ish opinions divides it down
- **Same as polling** — one voter is noisy, the average of many is stable

*Example (italic):* Ask one friend to guess the jellybeans in the jar, then average 100 guesses — the average wins.

**Key point callout:** **What bagging attacks:** variance — the slice of error that comes from the model changing its mind whenever the sample changes.

### Visualization (canvas `c3`, 720×300)

Dot-strip comparison: predicted approval scores across 10 retrainings, single tree row vs bagged row, on a shared 0–1 axis.

- **Title (bold 15px, `#1a5276`, top center):** "One Borderline Applicant, Scored After 10 Retrainings"
- **Rows (row labels bold 13px `#1a5276`, right-aligned in a 200px left margin):**
  - "single tree" at y=110, orange `#d95926` dots: `[0.25, 0.80, 0.40, 0.70, 0.30, 0.85, 0.55, 0.20, 0.75, 0.60]`
  - "bagged (100 trees)" at y=190, aqua `#199e70` dots: `[0.53, 0.56, 0.55, 0.54, 0.57, 0.55, 0.56, 0.54, 0.55, 0.56]`
  - Dots radius 7, alpha 0.75, alternately offset ±7px vertically to reduce overlap.
- **Axis:** shared horizontal axis at y=245, 0–1 with tick labels 0.00/0.25/0.50/0.75/1.00 (gray 12px) and light `#ddd` vertical gridlines; axis label "predicted approval score".
- **Threshold:** dashed magenta `#d55181` vertical line at 0.50 (dash 6/4, width 2), labeled bold 12px magenta "approve if > 0.50" above.
- **Annotations (bold 13px):** orange above the single-tree row: "4 of 10 reruns flip the decision"; aqua below the bagged row: "the average barely moves — never flips".

## What Bagging Cannot Fix

**Tags:** `common mistake` (red), `bias vs variance` (orange)

- **Shared blind spots survive** — if every tree lacks the key feature, all 100 vote wrong together
- **Bias stays** — averaging the same mistake just repeats it with more confidence
- **No overfit from more trees** — 1,000 trees are safe; they just stop adding anything
- **Still correlated** — every tree sees the same strong feature and grabs it first, so votes agree too much
- **The next fix** — random forests break that correlation by hiding features at each split

*Example (italic):* A hundred copies of the same newspaper is still one opinion.

**Key point callout:** **The limit:** bagging cuts variance, not bias — and cuts less than the arithmetic promised, because real trees are never fully independent.

### Visualization (canvas `c4`, 720×300)

Stacked bar chart: bias + variance error decomposition, single tree vs bagged.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Error Comes From (illustrative decomposition)"
- **Bars (130px wide, 120px gap):**
  - "single deep tree" — bias 6% + variance 18% (total error 24%)
  - "bagged, 100 trees" — bias 6% + variance 4% (total error 10%)
  - Bias slice (bottom): fill `rgba(74,58,167,0.45)`, stroke violet `#4a3aa7` width 2, in-bar label bold 12px violet "bias 6%". Variance slice (top): fill `rgba(217,89,38,0.40)`, stroke orange `#d95926`, in-bar label bold 12px orange "variance 18%" / "variance 4%".
  - Below each bar: bold 13px `#1a5276` bar label, then 12px `#2c3e50` "total error 24%" / "total error 10%".
- **Axes:** L-shaped `#999`, y scaled to max 28% with gray tick labels 0%/10%/20%; rotated y label "error on new applicants, %". Padding: top 56, bottom 60, left 80, right 200.
- **Arrow:** green `#008300` width 2.5 diagonal arrow (with filled triangular head) from the first bar's variance slice down to the second bar's, showing the shrink.
- **Right-margin annotations (bold 13px):** green "voting shrinks" / "only the variance slice"; violet "the bias slice" / "does not move".

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (per `tutorials/CLAUDE.md`, modeled on `most-powerful-signals/07-social-graph-connections.html`): h1 + `.subtitle`, then four `.card-section` blocks, each `<h2>` + `table.layout` with `.text-col` (50%) holding text and `.viz-col` (50%) holding one canvas.
- **Left column structure per section:** `.tags` pill row, then `<ul>` of one-line bullets each opening with `<b>bold term</b> —`, one italic `.example` paragraph, one `.key-point` callout with a `<strong>` lead.
- **Tag pill classes:** `.tag.blue` bg `rgba(26,82,118,0.12)` text `#1a5276`; `.tag.green` bg `rgba(39,174,96,0.15)` text `#27ae60`; `.tag.red` bg `rgba(231,76,60,0.12)` text `#e74c3c`; `.tag.orange` bg `rgba(230,126,34,0.15)` text `#e67e22`. Pills 0.72rem, weight 600, padding 2px 10px, radius 10px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px bottom border `#2980b9`; section h2 1.3rem `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 0.95rem; bullets 0.92rem with `li b` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` bg `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas JS:** shared palette object `P` (blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`); shared `setup(id)` helper sized 720×300 that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded (no `Math.random()`); invented curves labeled "illustrative". Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette reference:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card/grid links use `.html` extensions (this page has no links).
