# Domain Pitfalls: Employee Churn

**Page type:** detail page (h2 section headings, each followed by a one-row two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** Domain Pitfalls: Employee Churn

**Subtitle:** Modeling traps in employee attrition prediction — feedback loops, dishonest labels, and unobservable counterfactuals.

## Self-Fulfilling Prophecy

**Obj-title:** The Prediction Causes the Outcome

- **The loop:** Model predicts "will leave" → the manager quietly assigns less important projects.
- **The result:** The employee feels undervalued by the reduced scope → and eventually leaves.
- **Inverted causality:** The prediction caused the outcome, not the other way around.
- **Deployment rule:** Deployed churn models must be invisible, or they create the churn they predict.

### Visualization (canvas `canvas1`, 720×200 — declared 720×300 in markup, resized to 720×200 by the setup helper)

Feedback-loop diagram: four circular nodes connected left-to-right with a dashed return arrow.

- **Nodes:** circles of radius 38 centered at x = 100, 270, 440, 610, y = 100; each with 15%-alpha fill and solid 2px outline of its color; two-line bold 12px centered labels in the same color:
  - "Model predicts / \"will leave\"" — `#2980b9`.
  - "Manager / de-prioritizes" — `#e67e22`.
  - "Employee feels / undervalued" — `#e74c3c`.
  - "Employee / leaves" — `#c0392b`.
- **Forward arrows:** gray `#7f8c8d` lines (width 2) with filled arrowheads between consecutive nodes.
- **Feedback arrow:** dashed `#c0392b` curve (dash 5/4, width 2) looping under the row from the last node back to the first, with a filled `#c0392b` arrowhead at the first node.
- **Caption (italic 13px `#c0392b`, bottom center):** "prediction causes the outcome (feedback loop)".

## Exit Interview Bias

**Obj-title:** "Great Company, Just Found a Better Opportunity" Is Polite Fiction

- **Reported vs real:** The real reasons — toxic manager, no growth, underpaid — rarely appear in the exit interview.
- **PR-safe answers:** Departing employees give reasons that protect references and burn no bridges.
- **Data quality:** Labels sourced from exit interviews are systematically dishonest, not just noisy.

### Visualization (canvas `canvas2`, 720×200 — declared 720×300, resized by setup helper)

Two side-by-side bar groups: reported exit-interview reasons vs anonymous-survey reality.

- **Group headers (bold 15px `#1a5276`):** "Exit Interview (Reported)" at (60,20); "Anonymous Survey (Real)" at (400,20).
- **Categories and colors:** Better opportunity `#3498db`; Toxic manager `#e74c3c`; No growth `#f39c12`; Underpaid `#9b59b6`.
- **Reported values (left group, bars 55px wide at 80px pitch from x=60, baseline y=175, scaled to max 50% over 130px, 0.4-alpha fill with solid outline):** 45%, 25%, 20%, 10%.
- **Real values (right group from x=400, 0.8-alpha fill):** 10%, 35%, 30%, 25%.
- **Value labels:** 12px `#2c3e50` percent above each bar.
- **Legend (bottom row at y=188):** color square + category label for all four categories (11px `#2c3e50`).

## Counterfactual Unknown

**Obj-title:** Retention Offer Given, Employee Stayed — Would They Have Stayed Anyway?

- **The scenario:** Employee gets a $10K raise and stays — but maybe they weren't actually leaving, just interviewing.
- **Wasted spend:** You may have spent $10K on an employee who was never actually going to churn.
- **No measurement:** You cannot measure what would have happened had the offer never been made.
- **The fix:** Without a randomized holdout, the ROI of retention offers is unknowable.

### Visualization (canvas `canvas3`, 720×200 — declared 720×300, resized by setup helper)

Decision tree of observed vs unobservable outcomes, with a text summary on the right.

- **Root (bold 13px `#1a5276`, centered at 180,25):** "Employee flagged \"at risk\"".
- **Branches:** solid `#2980b9` line (width 2) to a green `#27ae60` box (40,65, 120×28) labeled "$10K offer given" in white 12px; dashed `#2980b9` line to a gray `#95a5a6` box (220,65, 120×28) labeled "No offer (unknown)".
- **Offer-given outcomes (solid `#27ae60` branch lines):** "Stayed / (observed)" in `#27ae60`; "Would have / stayed anyway?" in `#e67e22`; below them bold 12px `#c0392b` "$10K wasted?".
- **No-offer outcomes (dashed `#95a5a6` branch lines, 11px `#95a5a6`):** "Stayed?" and "Left?"; a large bold 50px question mark "?" in `rgba(231,76,60,0.2)` behind them.
- **Right summary (from x=420):** bold 14px `#1a5276` "The counterfactual problem:"; then 13px `#4a5568` "We observe: offer + stayed" and "We cannot observe:"; then 13px `#e74c3c` "  no offer + stayed (wasted $)" and "  no offer + left (good spend)"; then italic 12px `#7f8c8d` "Without randomized holdout," / "ROI of retention offers is unknowable".

## "Flight Risk" Label Harms Retention

**Obj-title:** The Label Creates the Condition It Describes

- **Exclusion:** Tagged as flight risk → excluded from the promotion pipeline — "they're leaving anyway, don't invest."
- **Detection:** The employee notices reduced opportunities and actually starts looking.
- **Net effect:** The intervention triggered by the label produces the very outcome the label predicted.

### Visualization (canvas `canvas4`, 720×200 — declared 720×300, resized by setup helper)

Horizontal event timeline with alternating above/below labels plus a declining engagement line.

- **Title (bold 13px `#1a5276`, top center):** "Label creates the condition it describes".
- **Timeline:** gray `#bdc3c7` horizontal line (width 2) at y=95 from x=50 to x=680.
- **Events (6px dots with connector stubs and two-line 12px labels in the event color, alternating above/below):**
  - x=100 "Tagged / \"flight risk\"" — `#e74c3c` (above).
  - x=230 "Skipped for / promotion" — `#e67e22` (below).
  - x=370 "Fewer key / projects" — `#f39c12` (above).
  - x=500 "Employee / notices gap" — `#8e44ad` (below).
  - x=620 "Actually / starts looking" — `#c0392b` (above).
- **Engagement line:** solid green `#27ae60` line (width 2) through the event x positions at descending heights (y = 130 + [30,35,50,75,90] × 0.6), labeled italic 11px `#27ae60` "Engagement" / "(declining)" at the right edge.

## Internal Transfer ≠ Churn

**Obj-title:** Moving From Eng to Product Is Not Attrition

- **Scope confusion:** The employee "left engineering" but never actually left the company payroll.
- **Naive label:** A naive churn definition counts that internal mobility as a permanent headcount loss.
- **Required distinction:** True churn (exit) must be separated from internal movement in the label definition.
- **The framing:** Internal mobility is healthy — don't penalize it as attrition.

### Visualization (canvas `canvas5`, 720×200 — declared 720×300, resized by setup helper)

Sankey-style flow diagram from an Engineering box to two destinations, with a metrics comparison.

- **Header (bold 13px `#1a5276`, centered at 180,20):** "Engineering Dept (100 people)".
- **Source box:** blue `#3498db` (0.2-alpha fill, 2px outline) at (30,35, 140×130) labeled bold "ENG" and "100 headcount".
- **Flow 1 (internal):** thick green `#27ae60` curved band (width 8, 0.4 alpha) to a green box (320,30, 120×55) labeled bold 12px "Product (8)" and 11px "INTERNAL MOVE".
- **Flow 2 (churn):** thicker red `#e74c3c` curved band (width 12, 0.4 alpha) to a red box (320,105, 120×55) labeled "Left Company (12)" and "TRUE CHURN".
- **Metrics (right, from x=500):** bold 13px `#1a5276` "Naive model:" with 12px `#e74c3c` "Churn = 20% (8+12)"; bold "Correct model:" with 12px `#27ae60` "True churn = 12%" and "Internal mobility = 8%".
- **Caption (italic 12px `#7f8c8d`, bottom center):** "Internal mobility is healthy — do not penalize it as attrition".

## Boomerang Employees

**Obj-title:** 15-20% of "Churned" Employees Return Within 3 Years

- **Wrong assumption:** The model treats departure as permanent — someone who left 2 years ago may want to come back.
- **Reality:** "Churn" is far less permanent than models assume.
- **The fix:** Model return probability alongside departure probability.

### Visualization (canvas `canvas6`, 720×200 — declared 720×300, resized by setup helper)

Cumulative return curve over years since departure, contrasted with the model's zero-return assumption.

- **Axes:** gray `#7f8c8d` axes (width 1.5); x from 0y to 5y (ticks each year, 110px per year from x=70, labels 11px `#4a5568`), rotated y label "% Returned"; x label "Years since departure"; y ticks 0-20% in 5% steps (y = 165 − pct × 7) with light `#ecf0f1` gridlines.
- **Data (years, cumulative % returned):** (0,0), (0.5,3), (1,7), (1.5,11), (2,14), (2.5,16), (3,18), (3.5,19), (4,19.5), (5,20).
- **Series:** blue `#2980b9` line (width 2.5) with area fill `rgba(41,128,185,0.15)` down to zero.
- **Highlight band:** dashed red `#e74c3c` rectangle (dash 3/3) with fill `rgba(231,76,60,0.1)` covering 15-20% from x=2.5y to 5y; annotations bold 12px `#e74c3c` "15-20% return" and 11px "within 3 years".
- **Assumption line:** dashed gray `#95a5a6` horizontal line (dash 6/4, width 1.5) at 0%, labeled italic 11px `#95a5a6` right-aligned: "Model assumption: 0% return (permanent loss)".

## Regeneration instructions

- **Layout:** standard domains detail page (139-style): h1, `.subtitle` paragraph, then per pitfall an unnumbered `<h2>` followed by a one-row `.obj-table` — left `<td>` (40%) with `.obj-title` + `<ul>` bullets (each bullet starts with a bold `<strong>` label), right `<td>` (60%, centered) with one `<canvas>`. No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-table` cells border `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout style defined but unused.
- **Canvas:** markup declares `width="720" height="300"`; a shared `setupCanvas(id)` helper overrides to 720×200 CSS pixels and scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), default font 17px system sans-serif.
- **Palette:** primary blue `#1a5276`, secondary blues `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, dark red `#c0392b`, purple `#8e44ad`/`#9b59b6`, grays `#7f8c8d`/`#95a5a6`/`#4a5568`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
