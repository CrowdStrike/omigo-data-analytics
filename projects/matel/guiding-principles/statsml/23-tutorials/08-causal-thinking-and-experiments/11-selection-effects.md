# Selection Effects

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%, tag pills above bullets)
**HTML title tag:** Selection Effects

**Subtitle:** Before you read what the data says, ask how the data got there — who is IN the dataset is often the real finding.

## 92% satisfied — because the unhappy 40% already left

Tags: `core idea` (blue), `who gets measured` (orange)

- **The survey** — sent to active users; results come back: 92% satisfied
- **The filter** — of 10,000 customers who signed up, 4,000 quit before the survey went out
- **Who answers** — only the 6,000 who stayed; the ones who left never saw the email
- **The circularity** — staying signals satisfaction; the sample pre-answers the question
- **The finding** — "92%" measures the filter (who remained), not the customer base

*Example:* The people best placed to explain the 4,000 departures are exactly the people the survey cannot reach.

**Key point:** The dataset was selected by the very thing being measured. Whenever entry into the data depends on the outcome, the average is rigged before any math happens.

### Visualization (canvas `c1`, 720×300)

Flow diagram: the customer base splits into stayers (surveyed) and leavers (never reached).

- **Title (bold 16px, ink `#1a5276`, top center):** "The Survey Only Sees Who Survived the Filter".
- **Left block:** rectangle at (60, 60), 240×150, split 60/40 vertically: top 60% filled `rgba(42,120,214,0.22)` with blue `#2a78d6` stroke, bold blue 13px label "6,000 still active"; bottom 40% filled `rgba(107,114,128,0.20)` with mute `#6b7280` stroke, bold mute 13px label "4,000 already left". Above the block, 12px text: "10,000 customers who signed up".
- **Survey box:** white rectangle at (450, 70), 210×62 with green `#008300` 2.5px stroke; bold green 15px "\"92% satisfied\"" and 12px "measured on stayers only".
- **Arrows:** solid blue 2.5px arrow (filled head) from the stayers block to the survey box, labeled bold blue 12px "survey email". Dashed mute 2.5px (dash 6/5) line from the leavers block toward the survey box, interrupted by a red `#e74c3c` 3px X mark; bold red 13px label: "never receive the survey".
- **Caption (bold magenta `#d55181` 13px, bottom center):** "the filter (staying) is correlated with the answer (satisfaction)".

## Adding the missing 4,000 back in

Tags: `worked example` (green), `by hand` (blue)

- **Stayers** — 6,000 active users, 92% satisfied = 5,520 happy people
- **Leavers** — 4,000 who churned; suppose exit interviews show only 20% were satisfied = 800
- **Everyone** — (5,520 + 800) / 10,000 = 63.2% satisfied across all customers
- **The gap** — the survey overstates satisfaction by 29 points, with zero dishonest answers
- **The lever** — the more unhappy people leave, the better the survey looks

*Example:* Nobody lied and nothing was miscounted — 92% and 63% are both true, about different groups of people.

**Key point:** One multiplication per group and one division: the whole correction is arithmetic. The hard part is remembering the 4,000 exist.

### Visualization (canvas `c2`, 720×300)

Three-bar chart: satisfaction of stayers, leavers, and everyone.

- **Title (bold 16px, ink, top center):** "Satisfaction: Surveyed, Missing, and Everyone".
- **Axes:** horizontal `#999` baseline; padding top 55, bottom 62, left 65, right 40; y scale max 100%.
- **Bars (width 120, 0.65 alpha, bold 14px colored value labels above, bold 12px labels + mute 12px sublabels below):**
  - "stayers (6,000)" / "what the survey sees": 92%, green `#008300`.
  - "leavers (4,000)" / "never surveyed": 20%, mute `#6b7280`.
  - "all 10,000" / "(5,520 + 800) / 10,000": 63.2%, blue `#2a78d6`.
- **Reference line:** dashed blue (dash 5/4, width 1.5) horizontal line across the plot at the 63.2% level.
- **Annotation (bold magenta `#d55181` 13px, top center at y=48):** "29 points of the \"finding\" is just who was reachable".

## The metric that improves while the business bleeds

Tags: `where it's used` (blue), `costly mistake` (red)

- **The dashboard** — quarterly satisfaction: 90% → 91% → 92% → 92%; trend looks great
- **The base** — active customers over the same quarters: 9,000 → 8,000 → 7,000 → 6,000
- **The mechanism** — each departure removes a likely-unhappy voice from next quarter's survey
- **Same trap elsewhere** — app-store ratings, NPS of current users, "power users love feature X"
- **The tell** — a satisfaction metric that rises as the population shrinks deserves suspicion

*Example:* Twyman's law in action: the nicest-looking number on the dashboard was the symptom of the churn problem.

**Key point:** Track the denominator next to the rate. A rising score over a shrinking base can mean the product is improving — or that the dissatisfied are exiting the sample.

### Visualization (canvas `c3`, 720×300)

Combo chart: shrinking customer-base bars with a rising satisfaction line, Q1–Q4.

- **Title (bold 16px, ink, top center):** "Satisfaction Rises as the Unhappy Exit the Sample".
- **Axes:** L-shaped `#999` axes; padding top 55, bottom 50, left 70, right 190.
- **Bars (mute `#6b7280` at 0.4 alpha, width 74, left scale 0–10,000, count labels inside the bar tops, quarter labels below):** Q1 9,000; Q2 8,000; Q3 7,000; Q4 6,000.
- **Line (green `#008300`, width 3, dots radius 5, bold 13px % labels above points; y scaled 88–94 onto the chart):** 90%, 91%, 92%, 92%.
- **Legend (right side, 12px):** mute swatch "active customers"; green line swatch "survey satisfaction".
- **Annotation (bold magenta `#d55181` 13px, right side, three lines):** "3,000 customers gone," / "score up 2 points —" / "the metric rewards churn".

## The confusion: a bigger sample does not fix it

Tags: `common mistake` (red), `bias vs noise` (orange)

- **The instinct** — "500 responses feels thin; survey all 6,000 actives to be sure"
- **The result** — 500 gives ~92%, 2,000 gives ~92%, all 6,000 gives exactly 92%
- **Why** — sample size shrinks random noise; the missing 4,000 are not noise, they are bias
- **Converging on wrong** — more biased data just gives a more precise wrong answer
- **The real fix** — chase the leavers: exit interviews, churn-cohort outreach, weighting

*Example:* A census of the survivors is still a survey of survivors.

**Key point:** Precision and correctness are different axes. No sample size rescues a sample drawn from the wrong pool — fix who gets in, not how many.

### Visualization (canvas `c4`, 720×300)

Bar chart with shrinking error bars: measured satisfaction stays ~92% at every sample size while the true value sits at 63%.

- **Title (bold 16px, ink, top center):** "More Respondents, Same Wrong Answer".
- **Axes:** L-shaped `#999` axes; padding top 55, bottom 55, left 70, right 45; y scale max 100.
- **Truth line:** dashed blue `#2a78d6` (dash 6/5, width 2) horizontal line at 63.2%, bold blue 12px label: "truth incl. leavers: 63%".
- **Bars (orange `#d95926` at 0.6 alpha, width 120, bold orange 13px value labels above, 12px size labels below):** sample sizes "500", "2,000", "6,000 (all actives)" with measured values 91.6%, 92.1%, 92.0% and error bars (`#2c3e50`, width 2, capped) of ±2.4, ±1.2, and 0 percentage points respectively.
- **Annotations:** bold red `#e74c3c` 13px at top: "the error bars shrink; the 29-point bias does not move". Mute 12px bottom center: "survey respondents (drawn from actives only)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` in `#666` 0.95rem, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%) holding the canvas.
- **Left column structure:** `.tags` row of colored pill spans (0.72rem, 600 weight, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem) beginning with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal reset; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`; shared `setup(id)` helper with fixed 720×300 logical size that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
