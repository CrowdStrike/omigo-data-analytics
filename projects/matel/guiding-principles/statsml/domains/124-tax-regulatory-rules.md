# Tax / Financial Reporting / Regulatory Rule Engines

**Page type:** detail page (h2 heading per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 124. Tax / Financial Reporting / Regulatory Rule Engines

**Subtitle:** The data doesn't change — the RULES for interpreting it change. Every number is correct only within the context of which rule version was applied.

## Laws Change Retroactively

**Obj-title:** Laws Change Retroactively

- New tax code passed in December applies to entire current year
- All data collected Jan-Nov must be RE-INTERPRETED under rules that didn't exist when collected
- Historical "tax liability" for Q1 was computed under old rules → now WRONG under new rules
- Must recompute everything retroactively

**Example:** December legislation retroactively changes Q1-Q3 tax calculations that were filed months ago.

### Visualization (canvas `c1`, declared 720×300, script renders at 720×200)

Timeline chart: monthly values under old rules vs recomputed values under new rules, with a rule-change marker at December.

- **Timeline axis:** horizontal line `#333` (width 2) from x=50 to x=680 at y=140; 12 tick marks with month labels Jan–Dec in `#555` at x = 60 + i*55, labels at y=160.
- **Original (old rules) line:** `#2980b9`, width 2; values `[100, 95, 105, 98, 102, 97, 103, 99, 101, 96, 104, 100]` plotted as y = 140 − value*0.8.
- **Rule change marker:** vertical dashed line `#c0392b` (dash 5/3, width 3) at x=665 from y=30 to y=140, labeled "New Law" in `#c0392b` centered above at y=22.
- **Recomputed (new rules) line:** `#e74c3c`, width 2; values `[80, 76, 84, 78, 82, 77, 83, 79, 81, 76, 84, 80]`, same scaling.
- **Legend:** `#2980b9` square at (50,175) with text "Original (old rules)"; `#e74c3c` square at (250,175) with text "Recomputed (new rules)", text in `#333`.
- **Font:** 17px -apple-system, sans-serif.

## Same Income, Different Classification Under Different Rules

**Obj-title:** Same Income, Different Classification Under Different Rules

- $100K income: Old rules → fully taxable at 22%
- New rules: $20K qualifies for new deduction → effective rate 18%
- State rules: different deductions apply
- International treaty: different portions exempt
- Same number, 4 different "correct" tax amounts depending on jurisdiction+rule version

**Example:** $100K income yields $22K, $18K, $19.5K, or $15K in tax depending on which rule set is applied.

### Visualization (canvas `c2`, declared 720×300, script renders at 720×200)

Branching diagram: one source box fanning into 4 colored result boxes.

- **Source box:** filled `#1a5276` rect at (30,75) 120×40, white centered text "$100K Income".
- **Branches** (line from (150,95) to (300, branchY+15), arrowhead, then filled label box 180×28 at x=310 with white left-aligned text):
  - "Old Rules: $22K" — `#2980b9`, y=30
  - "New Rules: $18K" — `#27ae60`, y=75
  - "State Rules: $19.5K" — `#8e44ad`, y=120
  - "Treaty: $15K" — `#e67e22`, y=165
- **Right-side note (15px, `#333`):** "Same income, 4 \"correct\" answers" at (520,100).
- **Font:** 17px -apple-system, sans-serif (note text 15px).

## Rule Engine Versioning Required

**Obj-title:** Rule Engine Versioning Required

- Must store: data + rule_version_applied + timestamp_of_computation
- 2022 data processed under 2022 rules ≠ 2022 data reprocessed under 2023 amended rules
- BOTH are "correct" for different purposes (original filing vs amended return)
- Without versioning: can't reproduce past computations, can't explain differences, can't audit

**Example:** Same data produces different results when processed under v2022 rules vs v2023 amended rules.

### Visualization (canvas `c3`, declared 720×300, script renders at 720×200)

Version matrix: 3×3 grid of data year × rule version with dollar results.

- **Headers (17px `#1a5276`, centered):** "Rule Version Applied" at (400,20); "Data Year" at (60,100).
- **Column headers (14px `#2980b9`):** "v2022 Rules", "v2023 Rules", "v2024 Rules" at x = 250 + i*160, y=45.
- **Row headers (`#1a5276`, right-aligned at x=140):** "2022 Data", "2023 Data", "2024 Data" at y = 85 + i*45.
- **Cell values (15px):** rows × columns = `[["$45,200","$43,800","$42,100"],["--","$51,300","$49,700"],["--","--","$48,500"]]`.
- **Cell background tints** (fill at 0.15 alpha, 110×30 rects): row 1: `#27ae60`, `#e67e22`, `#e74c3c`; row 2: `#ccc`, `#27ae60`, `#e67e22`; row 3: `#ccc`, `#ccc`, `#27ae60`. Text `#333` normally, `#999` for `--` cells.

## Mid-Year Rate Changes

**Obj-title:** Mid-Year Rate Changes

- Tax rate changes July 1 — same paycheck gets different multiplier before and after
- Annual aggregation must handle the split (6 months × old rate + 6 months × new rate)
- Many systems compute annually then prorate → slight error that compounds across millions of taxpayers
- Withholding tables lag rate changes by weeks → over/under-withhold during transition

**Example:** Rate drops from 24% to 22% mid-year; improper annual proration creates systematic $50-$200 errors per taxpayer.

### Visualization (canvas `c4`, declared 720×300, script renders at 720×200)

Step-function chart: tax rate at 24% for first half of year, dropping to 22% at July 1.

- **Axes:** `#333` width 2; x-axis from (60,170) to (680,170), y-axis from (60,170) to (60,20). Rotated y-axis label "Tax Rate" in `#333`.
- **Month labels (`#555`):** Jan, Mar, May, Jul, Sep, Nov at x = 100 + i*105, y=188.
- **Old rate segment:** shaded rect `rgba(41,128,185,0.2)` from (60,50) 310×120; top line `#2980b9` width 3 from (60,50) to (370,50); label "24%" in `#2980b9` at (65,44).
- **New rate segment:** shaded rect `rgba(39,174,96,0.2)` from (370,70) 310×100; top line `#27ae60` width 3 from (370,70) to (680,70); label "22%" in `#27ae60` at (375,64).
- **Change marker:** vertical dashed `#c0392b` (dash 5/4, width 2) at x=370 from y=20 to y=170, labeled "Jul 1: Rate Change" centered at (370,15); solid `#c0392b` step segment from (370,50) to (370,70).
- **Font:** 17px -apple-system, sans-serif.

## Cross-Jurisdiction Conflicts

**Obj-title:** Cross-Jurisdiction Conflicts

- Employee works in State A, lives in State B, company HQ in State C
- Federal: one set of rules
- State A: wants tax on income earned there
- State B: wants tax on resident's total income
- Credit for taxes paid elsewhere calculated differently by each state
- Same $1 of income: potentially taxed 3 times or 0 times

**Example:** Remote worker owes tax to 3 states simultaneously on the same income with no clear resolution.

### Visualization (canvas `c5`, declared 720×300, script renders at 720×200)

Venn diagram: three overlapping circles (radius 65) representing jurisdictions claiming the same income.

- **Circles:** State A (Work) center (280,90) fill `rgba(41,128,185,0.25)`; State B (Live) center (360,90) fill `rgba(39,174,96,0.25)`; State C (HQ) center (320,140) fill `rgba(142,68,173,0.25)`; each stroked with the same color at 0.8 alpha, width 2.
- **Labels (14px `#1a5276`):** "State A (Work)" at (235,45); "State B (Live)" at (405,45); "State C (HQ)" at (320,195).
- **Center overlap (13px `#c0392b`):** two lines "$1 taxed" / "3 times?" at (320,100)/(320,115).
- **Right-side explanation (15px `#333`, left-aligned at x=500):** "Each jurisdiction claims" / "right to tax the same" / "income. Credits differ" / "by state calculation." then in `#c0392b`: "Triple taxation or" / "zero tax possible."

## Audit Requires Reproducing PAST Rules on PAST Data

**Obj-title:** Audit Requires Reproducing PAST Rules on PAST Data

- IRS audits 2021 return in 2024 — must apply 2021 rules (not 2024 rules) to 2021 data
- If system only stores current rules → can't reproduce 2021 computation
- "Why did you deduct $X?" → need to show: under 2021 rule §179(d)(1) with 2021 limits ($1.05M), this qualified
- Under 2024 rules it wouldn't — but that's irrelevant; audit is about 2021

**Example:** System must reconstruct computation using 3-year-old rule set to survive audit.

### Visualization (canvas `c6`, declared 720×300, script renders at 720×200)

Timeline with an audit box reaching back in time to a past filing box.

- **Timeline:** `#333` width 2 line from (50,120) to (680,120); year markers "2021", "2022", "2023", "2024" in `#555` at x = 120 + i*160, y=140.
- **2021 filing box:** `#2980b9` rect at (80,55) 100×45, white 14px centered text: "2021 Filing" / "Rule: §179(d)(1)".
- **2024 audit box:** `#c0392b` rect at (520,30) 130×45, white 14px text: "2024 Audit" / "Needs 2021 rules".
- **Back-reaching arrow:** dashed `#c0392b` (dash 6/4, width 2) from (520,52) to (180,77), with solid `#c0392b` arrowhead at the filing box.
- **Crossed-out note:** "2024 rules: $1.16M limit" in 15px `#999` centered at (440,170), struck through with a `#c0392b` line from (350,166) to (535,166).
- **Correct note:** "Must use 2021 rules ($1.05M)!" in `#27ae60`, left-aligned at (80,170).

## Deduction Eligibility Changes Annually (Phase-Outs, Caps, Sunsets)

**Obj-title:** Deduction Eligibility Changes Annually (Phase-Outs, Caps, Sunsets)

- 2020: mortgage interest deduction on first $750K
- 2025: provision sunsets, reverts to $1M limit (Tax Cuts and Jobs Act expiry)
- Model trained on 2018-2024 data will be WRONG in 2026 when rules change
- Income phase-outs change annually with inflation adjustments → eligibility threshold moves every year

**Example:** ML deduction model fails when TCJA sunsets in 2025 despite no new legislation — the change was pre-programmed.

### Visualization (canvas `c7`, declared 720×300, script renders at 720×200)

Bar chart: deduction cap by year with a sunset cliff jump at 2025.

- **Axes:** `#333` width 2; x-axis (60,170)–(680,170), y-axis (60,170)–(60,20). Y labels (13px `#555`, right-aligned at x=55): "$1M" at y=50, "$750K" at y=95, "$500K" at y=140.
- **Year labels (12px `#555`):** 2018–2026 at x = 100 + i*68, y=185.
- **Bars:** 40px wide at x = 80 + i*68; years 2018–2024 (i<7): height 75, color `#2980b9`; years 2025–2026: height 120, color `#e74c3c`; baseline y=170.
- **Sunset annotation:** vertical dashed `#c0392b` (dash 5/3, width 2) at x=556 from y=20 to y=170; 14px `#c0392b` centered text "TCJA Sunset" at (610,18) and "Reverts to $1M" at (610,34).
- **Legend (13px):** `#2980b9` square at (200,15) with "$750K cap (TCJA)"; `#e74c3c` square at (340,15) with "$1M cap (post-sunset)", text `#333`.

## Financial Reporting Standards (GAAP/IFRS) Change Interpretation

**Obj-title:** Financial Reporting Standards (GAAP/IFRS) Change Interpretation

- 2019: operating leases off-balance-sheet
- 2020 (ASC 842): same leases now ON balance sheet
- Company's "total liabilities" jumped 30% overnight — not because debt increased but because REPORTING RULES changed
- Year-over-year comparison: meaningless without noting the rule change
- ML model trained on pre-2020 balance sheets will misinterpret post-2020 data

**Example:** Same company, same leases — reported liabilities go from $500M to $650M purely due to ASC 842 adoption.

### Visualization (canvas `c8`, declared 720×300, script renders at 720×200)

Before/after balance-sheet comparison with two boxed panels and an arrow between them.

- **Title (17px `#1a5276`, centered at (360,22)):** "Same Company — Reported Liabilities".
- **Left panel (2019):** `#f0f0f0` rect at (80,40) 200×140, stroked `#2980b9` width 2; header "2019 (Pre-ASC 842)" in 15px `#1a5276` at (180,58); liability bar `#2980b9` at (130,75) 100×85 with white "$500M" label; caption 12px `#555`: "Leases: OFF balance sheet" at (180,172).
- **Right panel (2020):** `#f0f0f0` rect at (440,40) 200×140, stroked `#e74c3c` width 2; header "2020 (Post-ASC 842)" at (540,58); stacked bar: `#2980b9` at (490,75) 100×65 labeled "$500M" (white), `#e74c3c` at (490,140) 100×30 labeled "+$150M" (white 13px); total "= $650M total" in 15px `#c0392b` at (540,188).
- **Arrow:** `#c0392b` width 3 from (310,100) to (410,100) with filled arrowhead; "+30%" in 17px `#c0392b` centered at (360,88); 13px `#555` notes "Rule change only" at (360,130) and "(no new debt)" at (360,145).

## Regeneration instructions

- **Layout:** for each pitfall: an `<h2>` section heading (1.4em, `#1a5276`, 2px solid `#2980b9` bottom border), then a `.obj-table` (full-width, border-collapse) containing one `<tr>`; left `<td>` (40%) holds `.obj-title` div + `<ul>` bullets + an `<p><strong>Example:</strong> ...</p>` paragraph; right `<td>` (60%, centered) holds the canvas. Even table rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page style:** body -apple-system/BlinkMacSystemFont/'Segoe UI' sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; p 0.95em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. A `.philosophy` class exists (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but is unused on this page. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"`, but each chart's IIFE sets the backing store to 720×200 × `window.devicePixelRatio` to 720×200px, and calls `ctx.scale` so drawing stays in logical coordinates. Default font 17px -apple-system, sans-serif.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, dark red `#c0392b`, orange `#e67e22`, purple `#8e44ad`, gray text `#555`/`#666`/`#333`.
- Note: in regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
