# Employee Surveys — Structural & Statistical Problems

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** 122. Employee Surveys — Structural & Statistical Problems

**Subtitle:** Mandatory employee surveys produce compliant, re-identifiable, and politically filtered data rather than honest signal.

Note: all canvases on this page are declared `width="720" height="300"` in HTML, but each chart's JS resizes its own canvas to an effective 720×200 (backing store = rendered width × dpr, CSS 720px × 200px).

## Forced Participation = Compliant Not Honest

- Mandatory completion drives checkbox behavior
- Social desirability bias amplified under compulsion
- Response quality indistinguishable from genuine engagement

**Example:** Company mandates 100% completion — gets 100% responses but 45% are straight-lined (same answer every question).

### Visualization (canvas `c1`, 720×200)

Pie chart of response pattern distribution with side legend.

- **Title (17px, `#1a5276`, at 250,25):** "Response Pattern Distribution".
- **Pie:** center (200,120), radius 70. Slices: Thoughtful 30% green `#27ae60`; Straight-lined 45% red `#e74c3c`; Random 10% yellow-orange `#f39c12`; Socially Desirable 15% purple `#8e44ad`.
- **Legend (swatches at x=400, `#333` text):** "Thoughtful (30%)", "Straight-lined (45%)", "Random (10%)", "Socially Desirable (15%)".

## Re-identification from Small Teams

- 1 of 3 in a role = "anonymous" is fiction
- Demographic intersections create unique fingerprints
- Managers can deduce respondents from writing style

**Example:** Only one senior female engineer in the London office — her "anonymous" feedback is trivially identifiable.

### Visualization (canvas `c2`, 720×200)

Declining line chart of re-identification risk vs team size, with dots.

- **Title (17px, `#1a5276`, at 230,25):** "Team Size vs Re-identification Risk".
- **Data:** team sizes `[2, 3, 5, 8, 12, 20, 50]` with risk % `[95, 85, 60, 35, 20, 8, 2]`; points at x = 80 + i×90, y = 180 − risk×1.5; red `#e74c3c` line (width 3) with 5px-radius red dots at each point.
- **X labels (14px, `#555`):** "n=2" … "n=50" below each point at y=195.

## Aggregation Paradox

- Company-wide results too abstract to act on
- Team-level results identify individuals
- No granularity level is both useful and safe

**Example:** HR reports "company engagement: 72%" — meaningless. Team of 4 reports 25% dissatisfied — everyone knows who.

### Visualization (canvas `c3`, 720×200)

Crossing lines of usefulness (rising) vs privacy (falling) along a granularity axis.

- **Title (17px, `#1a5276`, at 220,25):** "Granularity vs Usefulness & Privacy".
- **Axis band:** blue `#2980b9` filled rect 550×30 at (80,60).
- **Usefulness line (green `#27ae60`, width 3):** values `[50, 100, 200, 350, 500]` (of 500 max) at x = 80 + i×138, y = 150 − (value/500)×80.
- **Privacy line (red `#e74c3c`):** values `[500, 400, 280, 130, 50]`, same mapping.
- **Labels (15px):** `#555` "Company-wide" at (80,195), "Individual" at (570,195); green "Usefulness" at (500,100); red "Privacy" at (500,140).

## Insufficient N When Segmented

- Department x level x location = 4 people per cell
- Statistical significance impossible at useful granularity
- Confidence intervals wider than the scale itself

**Example:** Slicing 500 employees by 5 departments, 4 levels, 3 locations yields cells averaging 8 people — most under 5.

### Visualization (canvas `c4`, 720×200)

Histogram of segment cell sizes with a minimum-n threshold line.

- **Title (17px, `#1a5276`, at 210,25):** "Cell Sizes After Segmentation (n=500)".
- **Bars:** cell sizes `[2, 3, 4, 5, 6, 8, 10, 12, 15, 22, 35]` at x = 80 + i×55, 40px wide, height = (size/35)×140, baseline y=180; bars with size < 5 red `#e74c3c`, others blue `#2980b9`; size value (13px `#333`) below each bar at y=195.
- **Threshold:** dashed red `#e74c3c` (dash 5/5) horizontal line at the n=5 height, from x=60 to x=680, labeled red 14px "min n=5 threshold" at x=550 just above the line.

## Mandatory Does Not Equal Meaningful

- Clock-completion vs thoughtful response indistinguishable
- 2-minute completions treated same as 15-minute ones
- No validity check distinguishes engagement levels

**Example:** Median completion time: 3 minutes for 40-question survey. Thoughtful minimum: 12 minutes. 70% are speed-clicking.

### Visualization (canvas `c5`, 720×200)

Histogram of completion times colored by speed band, with thoughtful threshold line.

- **Title (17px, `#1a5276`, at 240,25):** "Survey Completion Time (minutes)".
- **Bars:** minute bins `[1, 2, 3, 4, 5, 7, 9, 12, 15, 20]` with counts `[5, 25, 30, 20, 8, 5, 3, 2, 1, 1]`; bars 50px wide at x = 80 + i×62, height = count×5, baseline y=180. Colors: bins < 5 min red `#e74c3c`, 5–9 min yellow-orange `#f39c12`, ≥ 10 min green `#27ae60`.
- **Threshold:** dashed green `#27ae60` (dash 5/5) vertical line at the 12-minute bin (x = 80 + 7×62), from y=40 to y=180, labeled green 14px "Thoughtful threshold (12 min)" at (450,50).
- **Annotation:** red 14px "70% below threshold" at (100,195).

## Results Weaponized by Management

- Low scores used punitively against team leads
- Employees learn honesty has consequences
- Future surveys reflect learned helplessness

**Example:** Manager fired after low engagement scores — next survey, team rates everything 4+/5 out of fear, not improvement.

### Visualization (canvas `c6`, 720×200)

Grouped bar chart of engagement scores before vs after a manager was fired.

- **Title (17px, `#1a5276`, at 170,25):** "Engagement Scores: Before & After Manager Fired".
- **Dimensions (x = 80 + i×130, 13px `#555` labels at y=195):** Culture, Leadership, Growth, Balance, Overall.
- **Data (bars 45px wide, height = score×30, baseline y=180):** before `[2.8, 3.1, 2.5, 3.0, 2.7]` red `#e74c3c`; after `[4.2, 4.5, 4.1, 4.4, 4.3]` yellow-orange `#f39c12` (offset +50px).
- **Legend (x=560):** red swatch "Before", orange swatch "After" (`#333` text).

## Cross-Org Comparison Meaningless

- Different instruments measure different constructs
- Cultural response styles vary (US vs Japan scoring norms)
- Timing effects (post-layoff vs post-bonus)

**Example:** Company A uses 5-point scale, Company B uses 7-point. "Both at 72%" means entirely different things.

### Visualization (canvas `c7`, 720×200)

Two equal-width bars showing the same percentage from different scales.

- **Title (17px, `#1a5276`, at 240,25):** 'Same "72%" — Different Instruments'.
- **Bars (200×40 at x=100, white labels inside):** blue `#2980b9` at y=60, "Co. A: 3.6/5 = 72%"; purple `#8e44ad` at y=120, "Co. B: 5.0/7 = 72%".
- **Side labels (15px, `#555`, x=400):** "Midpoint = 60%" (y=80), "Midpoint = 57%" (y=140); red `#e74c3c` "Not comparable despite same percentage" at (350,185).

## Response Quality Degrades with Frequency

- Quarterly surveys = declining quality each quarter
- Survey fatigue is measurable and progressive
- Completion time drops, straight-lining increases

**Example:** Q1 avg completion: 11 min. Q2: 7 min. Q3: 4 min. Q4: 3 min. Same questions, decreasing thought.

### Visualization (canvas `c8`, 720×200)

Grouped bar chart per quarter: completion time falling, straight-line rate rising.

- **Title (17px, `#1a5276`, at 210,25):** "Response Quality Degradation by Quarter".
- **Quarters (x = 100 + i×160, 15px `#555` labels at y=195):** Q1–Q4.
- **Data (bars 50px wide, baseline y=180):** avg minutes `[11, 7, 4, 3]` blue `#2980b9` (height = value×10); straight-line % `[10, 25, 45, 60]` red `#e74c3c` (offset +60px, height = value×2).
- **Legend (x=20):** blue 12×12 swatch "Avg min", red swatch "Straight-line %" (`#333` text).

## Political Interpretation — Results Serve Organizational Narrative

- **Cherry-picked presentation:** HR presents "engagement up 3%!" to the board as the headline number.
- **Hidden:** 3 of 5 dimensions declined; the one that improved was "free snacks" — not meaningful engagement.
- **Benchmark manipulation:** the claim "We're above industry average!" rests on a chosen comparison set.
- **Who is in the benchmark:** companies in different industries, sizes, and geographies — chosen to flatter, not inform.
- **Timing the survey:** run it right after bonus payout or team offsite → scores artificially high.
- **Timing after layoffs:** blame lands on "market conditions" not management — WHEN you survey = political choice.
- **Question design as narrative control:** "How satisfied are you with our NEW wellness program?" (leading).
- **Never asked:** "Would you prefer higher pay instead of wellness perks?" — the answer is known and inconvenient.
- **Public image curation:** Glassdoor, "Best Places to Work" awards — all derived from survey-like instruments.
- **Gamed by design:** companies actively game those instruments; internal results are curated for external PR.
- **Marketing, not diagnosis:** the metric becomes marketing material rather than a diagnostic tool.

**The fundamental problem:** The entity COMMISSIONING the survey (leadership), INTERPRETING the results (HR), and DECIDING what to publish (comms) all have incentives to present a positive narrative. The survey is not a neutral thermometer — it's a political instrument whose output is filtered through organizational power structures before anyone sees it.

### Visualization (canvas `c9`, 720×200)

One score bar with three audience-specific interpretation rows.

- **Title (bold 17px, `#1a5276`, centered at 360,20):** "Same Score, Different Narrative Depending on Audience".
- **Score bar:** translucent blue `rgba(41,128,185,0.4)` rect 160×30 at (280,40), centered `#333` label "Score: 3.6 / 5.0".
- **Interpretation rows (audience bold at x=60, spin text `#333` at x=200, rows spaced 35px from y=85):** green `#27ae60` "To Board:" / '"Above industry benchmark (3.4)"'; orange `#e67e22` "To Employees:" / '"Room for improvement, we hear you"'; red `#e74c3c` "Actual:" / "Dropped from 3.9 last year. 3 of 5 dims declined."

## Investor & Morale Pressure Distorts All Metric Interpretation

- **Investor pressure:** the "engagement score" must go up every quarter, exactly like a revenue number.
- **Missed quarter:** if it doesn't rise → "something is wrong with management" is the conclusion drawn.
- **The fix is cosmetic:** redefine metric, change benchmark, adjust methodology — keep the line going up.
- **Score becomes the product:** the SCORE is what gets delivered upward, not the insight behind it.
- **Employee morale circular trap:** share results showing low scores → morale drops even further.
- **Hiding does not help either:** hide the low scores → nothing changes → morale drops anyway.
- **Selective transparency:** "We're transparent about our survey!" (but only the good parts get shared).
- **Earnings-call mentality:** a quarterly survey is treated as quarterly performance to be reported.
- **Miss expectations:** stock-equivalent consequences follow — board scrutiny, leadership changes.
- **Same incentive as financial earnings:** manage the NUMBER, not the underlying reality behind it.
- **Positive interpretation bias:** score drops 5% → "still within normal range," nothing to act on.
- **Upside framing:** score rises 2% → "significant improvement from our initiatives!" is announced.
- **Asymmetric interpretation:** good news amplified, bad news minimized — the framing is not symmetric.
- **Same delta, two readings:** an identical change is interpreted differently depending on direction.
- **Comparison shopping for good news:** internal score dropped? Compare to industry, which is worse.
- **Next frame:** the industry comparison dropped? Compare to the "top quartile of peers" instead.
- **And the next:** those peers look bad too? Switch the framing to "improvement trajectory" instead.
- **Always a flattering frame:** some comparison always makes the number look acceptable enough.

**The meta-problem:** Organizations treat employee surveys like public companies treat earnings — the number must always look good or go up. This creates EXACTLY the same distortions as financial reporting: managed metrics, creative interpretation, and a widening gap between reported reality and actual reality. The survey stops measuring employee sentiment and starts measuring the organization's ability to manage perceptions.

### Visualization (canvas `c10`, 720×200)

Rising reported-score line vs flat dashed reality line, annotated with methodology tricks.

- **Title (bold 17px, `#1a5276`, centered at 360,20):** "Metric Must Go Up Every Quarter (Like Revenue)".
- **Reported line (green `#27ae60`, width 3):** quarterly scores `[3.5, 3.6, 3.7, 3.8, 3.9, 4.0, 4.1, 4.2]`, x = 80 + i×80, y = 180 − (value − 3.0)×100; right-aligned green label "Reported (redefined each Q)" at (680,50).
- **Reality line (red `#e74c3c`, width 3, dash 5/3):** near-flat from (80,130) to (640,135); right-aligned red label "Reality (unchanged)" at (680,140).
- **Trick annotations (14px, `#666`, centered two-line labels at x = 160 + i×140, y=170/183):** "new / benchmark", "removed / Q", "changed / scale", "excluded / group".
- **Bottom caption (bold red, centered at 360,198):** 'Each "improvement" = methodology change, not actual improvement'.

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a single-row full-width table; left `<td>` (40%) holds `.obj-title` + bullet list + bold-labeled example/summary paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`. The last two sections use bold-labeled bullets (`<strong>` lead-in per bullet) instead of plain bullets.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; bullets 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** 10 canvases with HTML attributes `width="720" height="300"`; each chart's IIFE individually sets backing store to 720×200 × `window.devicePixelRatio` to 720px × 200px, and calls `ctx.scale` so drawing stays in logical coordinates. Base chart font variable `fontSize = 17` px -apple-system; canvases c9/c10 use `textAlign` (center/left/right) explicitly.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, gray `#555`/`#333`/`#666`.
- In regenerated HTML, any card/page links use `.html` extensions.
