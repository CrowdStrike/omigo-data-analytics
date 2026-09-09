# Court Cases / Legal Analytics: Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: bullets + example callout left ~40%, canvas right ~60%)
**HTML title tag:** Court Cases / Legal Analytics - Data Pitfalls

**Subtitle:** Litigation data records only what survives to a public verdict — settlements, pleas, seals, and appeals silently remove or rewrite most of the distribution.

## Settlement Invisibility (95% Settle)

- 95% of civil cases settle before trial, creating massive selection bias
- Only the 5% that go to trial produce observable outcomes
- Settled cases have no public verdict data, hiding most of the distribution
- Models trained on trial outcomes are trained on the extreme tail
- Settlement amounts are confidential, creating a data desert

**Example:** A model predicting case outcomes trained only on trial verdicts shows 60% plaintiff win rate. But plaintiffs only go to trial when they're very confident—true win rate across all disputes is ~25%.

### Visualization (canvas `canvas1`, 720×240)

Vertical funnel of case attrition with side annotations.

- **Title (centered over funnel, bold 17px `#1a5276`):** "Case Attrition Funnel".
- **Stages (top to bottom; rounded 36px-tall bars centered at 42% width, fading blue `rgba(41,128,185,alpha)` with alpha 1.0, 0.82, 0.64, 0.46; white bold 13px labels inside; bold 13px `#2c3e50` percentage to the right of each bar):**
  - "1000 Filed Cases" — relative width 0.92 — "100%"
  - "200 Past Discovery" — 0.58 — "20%"
  - "50 At Trial" — 0.28 — "5%"
  - "5 Verdicts" — 0.11 — "0.5%"
- **Connectors:** faint blue trapezoids (`rgba(41,128,185,0.12)`) between consecutive stages plus small gray `#7f8c8d` downward arrows.
- **Highlight:** red dashed rectangle (`#e74c3c`, dash 6/4, width 2.5) around the final "5 Verdicts" stage, with a red arrow from the two-line bold 14px red annotation on the right: "Only this is in" / "your dataset!".
- **Left annotations (italic 12px gray `#7f8c8d`, right-aligned):** "~800 settle" / "(confidential)" beside stage 2; "~150 settle" / "pre-trial" beside stage 3.

## Judge Assignment Effects

- Random judge assignment creates natural experiments but also hidden confounders
- Judges have vastly different sentencing patterns (up to 3x variance)
- Case types cluster by courthouse/calendar, creating selection × judge interaction
- Judge experience, background, and case exposure create systematic patterns
- Not controlling for judge assignment leads to omitted variable bias

**Example:** Judge A sentences drug offenses avg 36 months, Judge B avg 14 months. A study comparing outcomes by defendant race that pools across judges confounds judge-assignment randomness with sentencing disparity.

### Visualization (canvas `canvas2`, 720×240)

Bar chart of average sentence length by judge.

- **Title (centered, bold 17px `#1a5276`):** "Sentencing Variance Across Judges".
- **Data:** Judges A-H with sentences (months): `[42, 14, 36, 22, 28, 48, 18, 30]`; mean 29.75.
- **Axes:** `#bdc3c7` L-axes; y labels "0 mo" to "50 mo" in steps of 10 (11px `#7f8c8d`) with `#ecf0f1` gridlines; y scale max 55; x labels "Judge A" … "Judge H" (12px `#2c3e50`).
- **Bars:** rounded tops (4px), width 62% of slot; color linearly interpolated from green `rgb(39,174,96)` (14 mo, lenient) to red `rgb(231,76,60)` (48 mo, harsh) by value; bold 12px `#2c3e50` value above each bar.
- **Mean line:** horizontal dashed `#1a5276` (dash 7/4, width 2) at 29.75, labeled bold 12px "Mean: ~30 mo" near right end.
- **Bottom annotation (centered, bold 13px red):** "3.4x range between judges (14 mo vs 48 mo)".

## Plea Bargain Distortion

- 97% of federal cases end in plea bargains, not trials
- Plea bargains are negotiated, not adjudicated—they reflect bargaining power, not guilt
- Charge stacking creates artificial leverage for prosecutors
- Original charges vs. pled-down charges create measurement confusion
- Innocent defendants plead guilty (estimated 2-8%) due to risk aversion

**Example:** A dataset shows 97% "guilty" rate for federal charges. This doesn't mean 97% were guilty—it means 97% accepted a deal rather than risk trial with mandatory minimums.

### Visualization (canvas `canvas3`, 720×240)

Two stacked horizontal proportion bars: disposition split, then plea-bargain breakdown.

- **Title (centered, bold 17px `#1a5276`):** "Federal Case Disposition".
- **Main bar (full width minus margins, 48px tall, rounded ends):** orange `#e67e22` segment 97% labeled inside in white bold 14px "Plea Bargain: 97%"; blue `#2980b9` segment 3% labeled outside in bold 12px blue "Trial 3%".
- **Section header (13px `#2c3e50`):** "Breakdown of plea bargains:".
- **Breakdown bar (40px tall, spanning the plea width):** orange `#e67e22` 94% labeled in white 12px "Accepted deal (likely guilty) ~94%"; red `#e74c3c` 6% with white diagonal hatching (`rgba(255,255,255,0.5)` lines, 7px spacing).
- **Annotation:** red upward arrow from below pointing at the red segment; centered red text — bold 13px "Estimated innocent who pled guilty (2-8%)" and 12px "Risk aversion + mandatory minimums = false guilty pleas".

## Outcome Coding Ambiguity

- Binary win/lose coding masks complex partial outcomes
- Plaintiff "wins" but gets 5% of amount claimed—is that a win?
- Multiple claims in one case can go different ways
- Damages awarded vs. damages collected are very different numbers
- Coding depends on who does it and when—low inter-rater reliability

**Example:** In a medical malpractice case, plaintiff asks for $5M. Jury awards $200K. Database codes this as "plaintiff win." But the plaintiff spent $300K in legal fees—net loss of $100K. The same case is a "win" in one dataset and "loss" in another.

### Visualization (canvas `canvas4`, 720×240)

Gradient outcome-spectrum bar with a binary threshold line and stacked outcome dots below.

- **Title (centered, bold 17px `#1a5276`):** "Outcome Spectrum vs. Binary Coding".
- **Spectrum bar (28px tall, rounded, `#bdc3c7` border):** horizontal gradient with stops — 0: `#e74c3c`, 0.2: `#e67e22`, 0.4: `#f1c40f`, 0.65: `#82e0aa`, 1: `#27ae60`. End labels bold 11px: left `#c0392b` "Defense Win ($0)", right `#1e8449` "Full Plaintiff Win ($5M)".
- **Threshold:** vertical dashed purple `#8e44ad` line (dash 5/3, width 2.5) at 1% of the bar width, labeled bold 11px purple: "Binary: >$0 = \"Win\"".
- **Dot plot header (12px `#2c3e50`, centered):** "Actual case outcomes distributed along the spectrum:".
- **Dots:** 26 outcomes as fractions of the claim, clustered at the low end: `[0, 0, 0, 0, 0, 0, 0.01, 0.02, 0.025, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10, 0.12, 0.15, 0.18, 0.22, 0.28, 0.35, 0.44, 0.55, 0.68, 0.82, 0.95]`; 5px-radius circles stacked vertically when they share a column; colored by position — 0: `#e74c3c`; <0.15: `#e67e22`; <0.4: `#f1c40f`; <0.7: `#82e0aa`; else `#27ae60`; faint dark outlines.
- **Bracket:** purple `#8e44ad` bracket under the 1%-12% region, labeled bold 11px purple: "Coded \"win\" but net loss after legal fees".

## Appeals Retroactively Change Outcomes

- 10-20% of cases are appealed; ~20% of appeals succeed
- An appellate reversal changes the "outcome" in your dataset retroactively
- Time lag between trial verdict and appeal resolution: 1-3 years
- Point-in-time snapshots can record opposite outcomes for same case
- Longitudinal analysis must account for outcome instability

**Example:** Your dataset records Case #4521 as "defendant wins" (trial verdict: Nov 2019). Appeal reversal in Mar 2021 changes it to "plaintiff wins." Same case appears opposite in two time-snapshots.

### Visualization (canvas `canvas5`, 720×240)

Gantt-style timeline of six cases, some flipping color at an appeal-reversal marker; two snapshot lines.

- **Title (centered, bold 17px `#1a5276`):** "Outcome Instability: Appeals Change History".
- **Time axis:** horizontal `#2c3e50` 2px line near the bottom with ticks and 11px labels for years 2018-2024.
- **Cases (4px-thick horizontal lines with 5px endpoint dots; 11px `#2c3e50` labels at left; positions as fractions of the axis):**
  - Case #1201: 0.00→0.75, blue `#2980b9` throughout (defendant wins).
  - Case #4521: 0.08→0.92, blue `#2980b9` until reversal at 0.50, then red `#e74c3c` — orange `#f39c12` diamond (outline `#d35400`) at the reversal point.
  - Case #3087: 0.17→0.67, red `#e74c3c` throughout.
  - Case #5590: 0.33→1.00, red `#e74c3c` until reversal at 0.72, then blue `#2980b9` — reversal diamond.
  - Case #2244: 0.05→0.58, blue `#2980b9` throughout.
  - Case #6103: 0.42→0.88, green `#27ae60` throughout.
- **Snapshots:** vertical dashed purple `#8e44ad` lines (dash 5/4, width 2) at 0.33 and 0.72, labeled bold 11px purple at top: "Snapshot 1", "Snapshot 2".
- **Legend (top-right, 10px `#2c3e50`):** blue square "Def. wins"; red square "Pl. wins"; orange diamond "Reversal".

## Sealed Records and Survivorship Gaps

- Sealed cases (juvenile, national security, sealed settlements) create systematic gaps
- Sealed cases are not random—they are systematically different from public cases
- High-profile cases with powerful defendants are more likely to be sealed
- Juvenile records sealed by default—entire demographics invisible
- Cases involving trade secrets, sexual assault settlements often sealed

**Example:** In corporate misconduct research, cases where the defendant is a major corporation are 4x more likely to be sealed than cases against small companies. Your "public" dataset systematically underrepresents powerful defendants.

### Visualization (canvas `canvas6`, 720×240)

Waffle chart of 100 cases (10×10 grid) plus sealing-rate mini-bars on the right.

- **Title (centered, bold 17px `#1a5276`):** "Sealed vs. Public Cases (100 Case Sample)".
- **Grid:** 10×10 cells (17px, 3px gaps). 70 public cells solid blue `#2980b9`. 30 sealed cells gray `rgba(149,165,166,0.45)` with `#7f8c8d` borders and an X drawn through each — sealed cells clustered in grid columns 6-9 of rows 0-7 (indices 6-9, 16-19, 26-29, 36-39, 46-49, 56-59, 66-69, 76-77) to show corporate bias.
- **Cluster highlight:** red dashed rectangle (`#e74c3c`, dash 5/3, width 2) around the sealed cluster (columns 6-9, rows 0-7).
- **Legend (right of grid, 12px `#2c3e50`):** blue square "Public cases (70%)"; gray X'd square "Sealed cases (30%)".
- **Sealing rates section (right side):** heading bold 14px red "Sealing Rates by Type:"; "Corporate defendants:" with a red `#e74c3c` mini-bar at 45% of 200px labeled white bold 11px "45% sealed"; "Individual defendants:" with a green `#27ae60` mini-bar at 12% labeled "12%"; beside it bold 15px red "4x disparity!".
- **Note (italic 11px gray `#7f8c8d`, two lines):** "Your \"public\" dataset systematically" / "underrepresents powerful defendants."

## Regeneration instructions

- **Layout:** one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by its own single-row `.obj-table`: left `<td>` (40%) with `.obj-title`, `<ul>` bullets, and an `.example` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 10px 14px, 0.9em, bold "Example:" lead in `#1a5276`); right `<td>` (60%, centered) with the canvas. Even table rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px. A `.philosophy` callout style is defined but unused. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="240"` per chart; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, yellow `#f1c40f`, grays `#7f8c8d`/`#2c3e50`/`#bdc3c7`/`#ecf0f1`.
- **Links:** in regenerated HTML, any card links use `.html` extensions (this page has none).
