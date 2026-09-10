# Silence After Noticing Data Issues

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%; four separate one-row tables, each row pairing one or more titled text blocks with a canvas)
**HTML title tag:** Silence After Noticing Data Issues — Common Bad Practices

**Subtitle:** Deliberate Inaction — You see the problem. You say nothing. Because raising it would delay YOUR project, reset YOUR timeline, or invalidate YOUR previous results.

## Row 1: The Practice / Why It's Uniquely Damaging / The Incentive Structure

**The Practice**

- You notice a tracking bug — event timestamps are off by 2 hours for a subset of users. Your model uses time-of-day features. Reporting means: investigation (+1 week), fix (+1 week), 2 months of data potentially invalid.
- You stay silent. Ship your model. The bug corrupts data for everyone downstream.

**Why It's Uniquely Damaging**

- **Compounding:** Data collection bugs ACCUMULATE damage daily. Unlike code bugs (instant once deployed), each day of silence = another day of bad data in the pipeline.
- **Irreversible:** You can't un-collect bad data. Once it's in the warehouse, the feature store, the training set — removing it means re-processing everything.
- **Invisible:** Bad data doesn't crash systems. It silently degrades accuracy, shifts distributions, biases decisions. Nobody else notices for weeks.

**The Incentive Structure**

- **Reporting costs YOU:** Timeline resets, results invalid, launch delayed.
- **Silence costs OTHERS:** Other teams train on bad data. But that's their problem later.
- **Diffusion:** "Someone else will notice." Each excuse = another day of corruption.

### Visualization (canvas `c1`, 720×340)

Line chart: cost to fix grows linearly with days of silence, against a flat "report immediately" baseline.

- **Background:** full-canvas light gray `#f9f9f9`.
- **Title (bold 13px, top center, `#1a5276`):** "Cost of silence accumulates daily — data damage is linear with time".
- **Plot area:** margins left 55, right 20, top 36, bottom 44; `#333` 1.5px axes (left and bottom).
- **X axis:** "Days of silence" (11px `#333`, centered below); tick labels "0d", "7d", "14d", "30d", "60d", "90d" positioned proportionally on a 0–90 scale, each with a light `#e0e0e0` vertical gridline.
- **Y axis:** rotated label "Cost to fix (re-collect + retrain)" (11px `#333`).
- **Cost line:** red `#e74c3c`, width 3, straight from origin rising linearly to 88% of plot height at day 90; area under it filled `rgba(231,76,60,0.1)`.
- **Baseline:** green `#27ae60` dashed horizontal line (dash 5/5, width 2.5) near the plot bottom (at 92% of plot height), labeled left-aligned in bold 11px green: "Report immediately: 1-2 day fix".
- **Annotations (red 10px, 4px dots on the cost line with labels to the right):** day 7 "7 days bad data"; day 30 "Model trained on corrupted data"; day 60 "Business decisions wrong"; day 90 "Full re-collection needed".

## Row 2: Example 1: Biased A/B Test / Example 2: Label Leakage in Training

**Example 1: Biased A/B Test**

- You notice control group has 2× more power users than treatment. The test has been running 3 weeks.
- Reporting means restarting (-4 weeks). You say nothing. The biased "lift" ships as a product decision.
- **Damage:** Product team invests 2 quarters building on a feature that doesn't actually convert. Opportunity cost: entire roadmap built on a phantom signal.

**Example 2: Label Leakage in Training**

- You notice a feature in the training set is derived from the target variable (e.g., "days_since_churn" used to predict churn). Model AUC is 0.98 — suspiciously perfect.
- Reporting means your published results are wrong. The model demo'd to leadership was fake. You say nothing.
- **Damage:** Model deployed to production performs at 0.62 AUC. Leadership loses confidence in ML org. The "why didn't we catch this?" postmortem never surfaces that someone DID notice.

### Visualization (canvas `c2`, 720×320)

Bar chart: three conversion-rate bars exposing a phantom lift from population imbalance.

- **Background:** full-canvas light gray `#f9f9f9`.
- **Title (bold 13px, top center, `#1a5276`):** "Biased A/B Test: Phantom Lift from Population Imbalance".
- **Plot area:** margins left 55, right 20, top 38, bottom 40; `#333` bottom axis; rotated y label "Conversion rate" (11px `#333`); horizontal `#e8e8e8` gridlines at 0%, 5%, 10%, 15% with right-aligned 10px `#666` labels; y scale 0–18%.
- **Bars (width = plotW/5, spaced 50px apart starting 40px in; value labels bold 12px `#333` above each bar; two-line group labels 10px `#555` below the axis):**
  - "Control (2× power users)" — 12%, fill `rgba(26,82,118,0.5)`, border `#1a5276` 2px.
  - "Treatment (normal mix)" — 15%, fill `rgba(231,76,60,0.6)`, border `#e74c3c` 2px.
  - "True Treatment (corrected)" — 12%, fill `rgba(39,174,96,0.6)`, border `#27ae60` 2px.
- **Phantom-lift arrow:** red `#e74c3c` dashed line (dash 4/3, width 2) from the top of the Control bar to the top of the Treatment bar, annotated in bold 11px red, two lines: "\"25% lift!\"" / "(fake)".
- **Green annotation (bold 11px `#27ae60`, below the top of the third bar):** "Actual lift: 0%".

## Row 3: Example 3: Duplicate Events in Tracking / Example 4: Stale Join Key

**Example 3: Duplicate Events in Tracking**

- Mobile SDK sends events twice under poor connectivity. You notice conversion rates are inflated ~15%. Your dashboard metrics look great — your VP just praised the numbers.
- Reporting means your Q3 "growth" was fake. You say nothing.
- **Damage:** Headcount planning uses inflated metrics. Paid acquisition budget doubles based on phantom ROI. 6 months later the truth surfaces during an audit — credibility destroyed.

**Example 4: Stale Join Key**

- A slowly-changing dimension (user_segment) hasn't been refreshed in 4 months. Your churn model uses segment as a feature — it's matching users to segments they LEFT months ago.
- Reporting means the feature store team admits a gap. You say nothing because your model still "passes validation."
- **Damage:** Retention campaigns target wrong segments. $200K in marketing spend wasted on users who aren't actually at-risk — they already churned or were never in that segment.

### Visualization (canvas `c3`, 720×320)

Stacked area chart: number of downstream systems/teams affected over 12 weeks of silence.

- **Background:** full-canvas light gray `#f9f9f9`.
- **Title (bold 13px, top center, `#1a5276`):** "Blast Radius: Downstream Teams Affected by Silent Data Bug".
- **Plot area:** margins left 55, right 20, top 38, bottom 40; `#333` 1.5px axes; x label "Weeks since you noticed" with tick labels 1–12; rotated y label "Affected systems / teams"; y scale 0–16.
- **Stacked layers (bottom to top), weekly values for weeks 1–12:**
  - "Your model" — `rgba(231,76,60,0.7)`: `[1,1,1,1,1,1,1,1,1,1,1,1]`.
  - "Feature store consumers" — `rgba(230,126,34,0.6)`: `[0,0,1,2,2,3,3,3,4,4,4,4]`.
  - "Downstream dashboards" — `rgba(241,196,15,0.5)`: `[0,0,0,1,2,2,3,4,4,5,5,6]`.
  - "Business decisions" — `rgba(142,68,173,0.4)`: `[0,0,0,0,0,1,1,2,3,3,4,5]`.
- **Legend (top right, 12px color squares, 10px `#333` labels):** the four layer names in the order above.

## Row 4: Example 5: Survivorship Bias in Cohort / The Pattern

**Example 5: Survivorship Bias in Cohort**

- Your "customer satisfaction" model only trains on users who completed onboarding. 40% drop off before that. You notice the dataset excludes everyone who had a bad experience early.
- Reporting means the "95% satisfaction" metric your CEO quoted at the board meeting is wrong.
- **Damage:** Product team never fixes onboarding because metrics say users are happy. Churn continues. Board makes acquisition decisions based on inflated retention narrative.

**The Pattern**

Every example shares the same structure:

- **Detection:** One person notices (you)
- **Calculation:** Reporting costs you personally
- **Silence:** You rationalize inaction
- **Propagation:** Bad data/decisions compound for months
- **Postmortem:** "How did nobody catch this?" (Someone did.)

### Visualization (canvas `c4`, 720×320)

Cost-crossover line chart: total cost of fixing the data issue vs time since discovery — the "speak up now" line stays low and flat while the "stay silent" line compounds through rework, wrong decisions, and model retraining.

- **Background:** full-canvas light gray `#f9f9f9`.
- **Title (bold 13px, top center, `#1a5276`):** "Cost Crossover: Speak Up Now vs Stay Silent".
- **Caption (italic 11px `#666`, centered under the title):** *"Same bug, two choices — the only variable is when it gets surfaced."*
- **Plot area:** margins left 55, right 20, top 50, bottom 44; `#333` 1.5px axes (left and bottom).
- **X axis:** "Weeks since you noticed the issue" (11px `#333`, centered below); integer tick labels 0–12 (11px `#333`), each with a light `#e0e0e0` vertical gridline.
- **Y axis:** rotated label "Total cost to fix ($K)" (11px `#333`); scale 0–300 with horizontal `#e8e8e8` gridlines at $0K/$50K/$100K/$150K/$200K/$250K/$300K and right-aligned 11px `#666` labels.
- **Speak-up line (green `#27ae60`, width 3):** weekly values for weeks 0–12: `[12, 12, 13, 13, 14, 14, 14, 15, 15, 15, 16, 16, 16]` — the fix cost is paid once up front and barely drifts. End label above the line, right-aligned in bold 12px green: "Speak up now: cost stays flat".
- **Stay-silent line (red `#e74c3c`, width 3, area under it filled `rgba(231,76,60,0.08)`):** weekly values for weeks 0–12: `[2, 4, 7, 11, 16, 24, 36, 54, 80, 115, 158, 212, 280]` — starts cheaper (no disruption), then compounds. End label above the line, right-aligned in bold 12px red: "Stay silent: cost compounds".
- **Crossover marker:** orange `#e67e22` dashed vertical guide (dash 4/4, width 1.5) at week 4 rising from the x-axis, with a 5px orange dot where the silent line passes the speak-up line; label to the right of the guide in bold 11px orange, two lines: "Crossover: week 4 —" / "silence becomes the costlier path".
- **Milestone dots (red `#e74c3c`, 4px, on the silent line, 11px red labels):** week 7 "Rework: analyses re-run" (label right of the dot); week 10 "Models retrained on bad data" (label left of the dot).
- **Insight annotation (bold 13px red `#e74c3c`, upper-left of the plot, two left-aligned lines):** "By week 12, silence costs ~17× more —" / "and the gap widens every week."

## Regeneration instructions

- **Layout:** four separate `.obj-table` tables, each with a single `<tr>`; left `<td>` (40%) holds one or more `.obj-title` blocks (subsequent ones get `style="margin-top:14px;"`) with `<ul>` lists (Row 4 also has a plain `<p>` "Every example shares the same structure:" before its final list); right `<td>` (60%, centered) holds the canvas. Note: on this page `.obj-table td` uses padding 14px 18px and `vertical-align: top` (tighter than the usual 20px 24px middle-aligned variant).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes per chart (c1 720×340, c2 720×320, c3 720×320, c4 720×320), all with `#f9f9f9` full-canvas background fills; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark red `#c0392b`, purple `#8e44ad`, neutral gray `#95a5a6`, yellow accent `rgba(241,196,15,0.5)`, gray text `#666`/`#333`/`#555`.
