# Data Annotation / Human Labeling

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + one-row table per pitfall)
**HTML title tag:** 135. Data Annotation / Human Labeling

**Subtitle:** Human labels are not ground truth — annotator disagreement, ambiguous instructions, incentives, fatigue, and cultural bias set a quality ceiling that every downstream model inherits.

## Inter-Annotator Disagreement

- 3 labelers, 3 answers
- Agreement rate = model ceiling
- If humans agree 70% → model can't reliably exceed 70%

**Example:** Sentiment task with 68% inter-annotator agreement; model trained to 71% accuracy is actually at ceiling, not underperforming.

### Visualization (canvas `c1`, 720×200)

Grouped bar chart: human agreement vs model accuracy across task difficulty.

- **Title (17px, `#1a5276`):** "Agreement Rate = Model Ceiling".
- **Axes:** blue `#2980b9`, width 2 — x-axis (50,180)–(700,180), y-axis (50,40)–(50,180).
- **Categories:** Easy, Medium, Hard, Ambiguous (x = 100 + i*160, labels 12px `#2c3e50` at y=195).
- **Bars (50px wide, scale 1.5px per %):** light blue `#3498db` human agreement `[95, 82, 68, 45]`; red `#e74c3c` model accuracy `[94, 80, 70, 52]` offset +55px.
- **Legend (12px, at x=550):** light blue "Human agreement" (y=60); red "Model accuracy" (y=80).

## Instruction Ambiguity

- "Toxic if offensive" — offensive TO WHOM?
- Each labeler interprets differently
- Labels reflect LABELER not truth

**Example:** "That's sick" labeled toxic by 2/5 annotators (older demographic), non-toxic by 3/5 (younger). Label = demographics of workforce.

### Visualization (canvas `c2`, 720×200)

Bar chart: toxicity-label rate by annotator age group.

- **Title (17px, `#1a5276`):** "Label Variance by Annotator Demographics".
- **Baseline:** blue `#2980b9` x-axis (50,180)–(700,180).
- **Bars (red `#e74c3c`, 90px wide, scale 2.2px per %):** groups "Age 18-25", "Age 26-40", "Age 41-55", "Age 55+" with toxic rates `[15, 28, 45, 62]`, at x = 120 + i*150; group label (12px `#2c3e50`) at y=195, percent value above each bar.
- **Caption (13px, gray `#7f8c8d`, at 400,55):** "% labeling \"That's sick\" as toxic".

## Cultural/Language Bias

- US team labeling Japanese content
- Sarcasm, formality, humor = culture-dependent
- Systematic errors on non-US content

**Example:** Japanese keigo (formal speech) misinterpreted as "cold/negative" by English-speaking annotators unfamiliar with register norms.

### Visualization (canvas `c3`, 720×200)

Bar chart: error rate by annotator-culture → content-culture pair.

- **Title (17px, `#1a5276`):** "Error Rate by Content Culture vs Annotator Culture".
- **Baseline:** blue `#2980b9` x-axis (50,180)–(700,180), width 2.
- **Bars (90px wide, scale 3.2px per %):** pairs US→US 8%, US→UK 15%, US→JP 42%, US→BR 35%, at x = 120 + i*150; fill red `#e74c3c` when error > 30%, else orange `#f39c12`; pair label (12px `#2c3e50`) at y=195, "N% err" above each bar.
- **Caption (12px, gray `#7f8c8d`, at 300,55):** "Annotator culture → Content culture".

## AI-Generated Labels

- Fast + cheap + hallucinates on edge cases (the ones that matter most)
- Creates false confidence — looks "labeled" but subtly wrong

**Example:** GPT-labeled dataset 95% accurate on easy cases, 40% on edge cases. Edge cases are 5% of data but 80% of model errors in production.

### Visualization (canvas `c4`, 720×200)

Two-bar comparison: AI label accuracy on easy vs edge cases.

- **Title (17px, `#1a5276`):** "AI Label Accuracy: Easy vs Edge Cases".
- **Baseline:** blue `#2980b9` x-axis (50,180)–(700,180), width 2.
- **Bars (150px wide, scale 1.5px per %):** green `#27ae60` at x=150, 95%; red `#e74c3c` at x=420, 40%. White in-bar value labels (14px): "95%" at (210,100), "40%" at (480,140).
- **Bar captions (13px, `#2c3e50`, y=195):** "Easy cases (95% of data)"; "Edge cases (5% of data,".
- **Extra annotation (12px, red `#e74c3c`, at 430,55):** "80% of production errors)".

## Annotator Fatigue

- Hour 1 accuracy 90%, hour 4 accuracy 65%
- "Just click something" mode
- But data doesn't track WHEN label was made

**Example:** 8-hour shift; last 2 hours produce 30% of labels but contain 60% of errors. No timestamp metadata to filter them out.

### Visualization (canvas `c5`, 720×200)

Declining line chart: accuracy over an 8-hour shift with an acceptability threshold.

- **Title (17px, `#1a5276`):** "Annotator Accuracy Over Shift Duration".
- **Axes:** blue `#2980b9`, width 2 — x-axis (50,180)–(700,180), y-axis (50,40)–(50,180).
- **Series (red `#e74c3c`, width 2):** accuracy `[92, 90, 87, 82, 75, 70, 66, 63]`, points at x = 80 + i*80, y = 180 − value*1.6.
- **Threshold:** dashed orange `#f39c12` (dash 4/4) horizontal line at the 70% level, labeled "Acceptable threshold (70%)" (12px orange, at x=500 just above the line).
- **X labels (12px, gray `#7f8c8d`, y=195):** "Hour 1    Hour 2    Hour 3    Hour 4    Hour 5    Hour 6    Hour 7    Hour 8".

## Payment Incentive Structure

- Per-label = rush
- Per-hour = slow
- Neither = correct
- Economic structure directly determines quality

**Example:** Per-label team: 500 labels/hr, 70% accuracy. Per-hour team: 80 labels/hr, 92% accuracy. Cost per CORRECT label: nearly identical.

### Visualization (canvas `c6`, 720×200)

Grouped bar chart: labeling speed vs accuracy for three payment models.

- **Title (17px, `#1a5276`):** "Payment Model: Speed vs Accuracy Tradeoff".
- **Baseline:** blue `#2980b9` x-axis (50,180)–(700,180), width 2.
- **Groups (x = 120 + i*210):** Per-label (500/hr, 70%), Per-hour (80/hr, 92%), Hybrid+QA (200/hr, 88%).
- **Bars (60px wide):** light blue `#3498db` speed bars scaled 0.28px per label/hr; green `#27ae60` accuracy bars scaled 1.5px per %, offset +70px. Labels (11px `#2c3e50`): model name at y=195, "N/hr" above speed bar, "N%" above accuracy bar.
- **Legend (12px, x=550):** light blue "Speed (labels/hr)" (y=80); green "Accuracy %" (y=100).

## Skill Mismatch

- Medical images by non-doctors
- Legal text by non-lawyers
- Code by non-programmers
- Cheap but systematically wrong on domain-specific items

**Example:** Radiology images labeled by crowd workers: 95% correct on obvious cases, 20% on subtle findings that radiologists catch. Model learns to miss subtle pathology.

### Visualization (canvas `c7`, 720×200)

Grouped bar chart: domain expert vs crowd worker accuracy by case difficulty.

- **Title (17px, `#1a5276`):** "Domain Expert vs Crowd Worker Accuracy".
- **Baseline:** blue `#2980b9` x-axis (50,180)–(700,180), width 2.
- **Categories:** Obvious, Moderate, Subtle, Expert-only (x = 100 + i*160, labels 11px `#2c3e50` at y=195).
- **Bars (45px wide, scale 1.5px per %):** green `#27ae60` expert `[98, 95, 88, 82]`; red `#e74c3c` crowd `[95, 78, 45, 20]` offset +50px.
- **Legend (12px, x=560):** green "Domain expert" (y=60); red "Crowd worker" (y=80).

## Label Quality Metrics

- Cohen's kappa (agreement)
- Flip rate (self-consistency)
- Gold-set accuracy
- Time-per-label
- Disagreement patterns
- Label entropy per item

**Example:** Annotator with 95% gold-set accuracy but 30% flip rate on re-labeling = inconsistent despite seeming accurate. Random alignment with gold set.

### Visualization (canvas `c8`, 720×200)

Horizontal progress-bar dashboard of four quality metrics.

- **Title (17px, `#1a5276`):** "Quality Metrics Dashboard".
- **Rows (starting y=55, 38px apart):** metric name (13px `#2c3e50`, x=60), track bar light gray `#ecf0f1` (200,y,400×22), filled portion proportional to value with percent label at x=610.
  - Cohen's κ: 0.72 (target ≥0.8) — fill orange `#f39c12` (below target).
  - Flip rate: 0.15 (target ≤0.1) — fill red `#e74c3c` (above target; lower-is-better metric).
  - Gold acc: 0.91 (target ≥0.9) — fill green `#27ae60`.
  - Time/label: 0.65 (target ≤0.5) — fill red `#e74c3c` (lower-is-better metric).
- **Caption (11px, gray `#7f8c8d`, at 200,195):** "Green = acceptable | Orange/Red = needs attention".

## Temporal Label Decay

- What was "acceptable content" in 2020 ≠ 2024
- Labels on old data become outdated as social norms shift

**Example:** Content moderation labels from 2019 allow terms now considered harmful in 2024. Model trained on old labels produces outdated moderation decisions.

### Visualization (canvas `c9`, 720×200)

Declining line chart: label validity over years with a relabeling threshold.

- **Title (17px, `#1a5276`):** "Label Validity Decay Over Time".
- **Axes:** blue `#2980b9`, width 2 — x-axis (50,180)–(700,180), y-axis (50,40)–(50,180).
- **Series (red `#e74c3c`, width 2):** validity % `[98, 95, 90, 83, 75, 68, 60, 52]`, points at x = 80 + i*80, y = 180 − value*1.5.
- **Threshold:** dashed orange `#f39c12` (dash 4/4) horizontal line at the 70% level, labeled "Relabeling threshold" (12px orange, x=520).
- **X labels (12px, gray `#7f8c8d`, y=195):** "2019    2020    2021    2022    2023    2024    2025    2026"; y-axis meaning at (300,55): "% labels still valid by current standards".

## Majority Vote Hiding Bias

- 3 of 5 labelers agree (majority = "correct")
- But: all 3 share the same cultural bias
- The 2 dissenters from different culture were actually right
- Majority = popular opinion not ground truth

**Example:** African American Vernacular English labeled "informal/incorrect" by 3/5 annotators from same background; 2 linguist annotators correctly label it as valid dialect.

### Visualization (canvas `c10`, 720×200)

Diagram: five annotator squares with majority-vs-truth annotations.

- **Title (17px, `#1a5276`):** "Majority Vote vs Ground Truth".
- **Squares (30×30, at y=55):** three light blue `#3498db` squares (x=80,120,160) marked "A" (11px light blue), two red `#e74c3c` squares (x=210,250) marked "B" (white).
- **Side text (13px, `#2c3e50`, x=300):** "3 annotators: Label A (same cultural background)"; "2 annotators: Label B (different culture, domain experts)".
- **Result text (14px, `#2c3e50`, x=80):** "Majority vote result: A (WRONG)" (y=130); "Ground truth: B" (y=155).
- **Verdict (13px, red `#e74c3c`, at 80,180):** "Majority = popular opinion ≠ ground truth".
- **Caption (12px, gray `#7f8c8d`, at 400,180):** "Shared bias amplified by majority rule".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) per pitfall, followed by a full-width single-row table; left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + an "**Example:**" paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `ul` 0.9em; `.philosophy` class defined (background `#f0f4f8`, left border 4px `#2980b9`) but unused on this page. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare 720×300, but each chart's IIFE explicitly resets to 720×200 — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), CSS size fixed at 720×200px, `ctx.scale` back to logical coordinates. One self-invoking function per chart (no shared setup helper). Titles 17px, labels 11-14px, -apple-system font stack.
- **Palette:** primary blue `#1a5276`, axis blue `#2980b9`, light blue `#3498db`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`, grays `#7f8c8d`/`#2c3e50`/`#ecf0f1`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
