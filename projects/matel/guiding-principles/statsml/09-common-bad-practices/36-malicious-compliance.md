# Malicious Compliance

**Page type:** detail page (two-column obj-table layout: text left 40%, canvas right 60%, one row per section)
**HTML title tag:** Malicious Compliance — Common Bad Practices

**Subtitle:** Organizational Anti-Pattern — Follow the directive to the letter while deliberately ignoring its spirit. The literal execution is technically correct but produces the opposite of the intended outcome. "You told me exactly what to do. I did exactly that."

## The Practice

- Receive a directive, policy, or requirement with a clear intent.
- Identify a gap between the literal wording and the actual goal.
- Execute the literal wording perfectly — knowing it will produce a harmful, absurd, or useless result.
- When challenged: "I did exactly what was asked. See the spec/ticket/policy."
- Intent: force the issuer to either write better requirements, or suffer the consequences of their own vague directive.

### Visualization (canvas `c1`, 720×300)

Two-panel line chart over one quarter: both paths to "CTR up 20%" hit the target — the second panel shows what the literal path destroyed on the way.

- **Title (bold 14px, top center, `#1a5276`):** "Two ways to hit \"CTR +20%\"".
- **Shared x-axis:** weeks 1-13 of the quarter (ticks at W1/W4/W7/W10/W13, 11px `#666`), left margin 55px, right margin 25px.
- **Top panel ("CTR vs baseline (%)", y from +0% to +40%, panel area y≈45-150):** panel label left-aligned bold 11px `#333` above the panel. Two lines, week w = 0…12:
  - Spirit path (green `#27ae60`, width 2): `w * 22 / 12` (steady climb to +22%).
  - Letter path (red `#e74c3c`, width 2): `w * 35 / 12` (climbs to +35% on clickbait).
  - Dashed gray `#999` horizontal target line at +20%, right-aligned 11px gray label "target +20%". Both lines cross it — a small green and a small red dot at each line's crossing.
- **Bottom panel ("Retention vs baseline (%)", y from −45% to +15%, panel area y≈170-260):** same x-axis. Two lines:
  - Spirit path (green, width 2): `w * 8 / 12` (drifts up to +8%).
  - Letter path (red, width 2): `-(w * w) * 40 / 144` (accelerating collapse to −40%).
  - Solid `#ccc` zero line.
- **Legend (top-right of the top panel, 11px):** green swatch "Spirit: better content", red swatch "Letter: clickbait + spam".
- **Insight annotation (bold 13px red `#e74c3c`, centered in the bottom panel's empty upper-right):** "Both hit the CTR target." / "Only one destroyed the product."
- **Caption (bottom center, italic 12px `#666`):** "Illustrative quarter — the compliance report shows only the top panel."

## Real-World Examples

**"Increase Click-Through Rate"** (example box)
Stakeholder says: "CTR must go up 20% this quarter." Team adds clickbait titles, misleading thumbnails, and notification spam. CTR rises 35%. Retention drops 40%, refund rate triples. "We hit the CTR target as requested."

**"All Models Must Be Retrained Quarterly"** (example box)
Policy mandates quarterly retraining. Team retrains with identical data, identical hyperparameters, identical architecture. Logs show "model retrained on 2026-04-01." Performance: unchanged. Compute cost: $14,000 per run. Compliance checkbox: ✓.

**"Report Must Include Confidence Intervals"** (example box)
Reviewer demands CIs on all estimates. Analyst reports 99.99% CI: [−∞, +∞]. Technically correct. Communicates nothing. "You said include confidence intervals. Here they are."

**"Document All Decisions"** (example box)
New policy: every technical decision needs a written ADR (Architecture Decision Record). Engineer writes: "Decision: use Python. Reason: we already use Python. Alternatives considered: not using Python. Outcome: use Python." For every micro-decision. 400 ADRs filed in one sprint. System is now unsearchable.

**"No Deployments Without Passing Tests"** (example box)
Critical hotfix blocked by a flaky integration test unrelated to the change. Engineer deletes the flaky test. All tests pass. Deploys. "No failing tests — policy satisfied."

**"Reduce P0 Incident Count"** (example box)
Team reclassifies incidents from P0 to P1. Dashboard shows P0s dropping. Actual reliability: unchanged. "P0 count is down 60% quarter-over-quarter."

### Visualization (canvas `c2`, 720×340)

Multi-line chart over 8 quarters ("Quarterly Retraining: Compliance vs Value").

- **Title (bold 15px, top center, `#1a5276`):** "Quarterly Retraining: Compliance vs Value".
- **Chart area:** left 90, right w−40, top 50, bottom 280; light gray `#ccc` L-axes.
- **X axis:** 8 points labeled Q1, Q2, Q3, Q4, Q1, Q2, Q3, Q4 (12px `#666`), with 11px year markers "Year 1" under points 1-2 and "Year 2" under points 5-6.
- **Y axis labels (right-aligned):** "0%" at bottom, "50%" at middle, "100%" at top.
- **Series (all width 3, 4px dots in series color):**
  - Compliance score, green `#27ae60`: `[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]` (flat at 100%).
  - Model accuracy, blue `#2980b9`: `[0.72, 0.72, 0.71, 0.72, 0.71, 0.70, 0.71, 0.70]` (flat middle).
  - Cumulative wasted compute ($), red `#e74c3c`: `[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]` (rising).
- **Legend (top right, 14px swatches):** "Compliance score" (green), "Model accuracy" (blue), "Cumulative wasted compute ($)" (red).
- **Annotations (bold 13px):** red "$112K burned. Zero improvement." above the cost line at point 7; green "\"100% compliant\"" below the compliance line at point 7.
- **Caption (italic 13px `#666`, bottom center):** "Policy says retrain. We retrain. Policy doesn't say retrain with new data."

## In ML/Data Science

- **"Use cross-validation":** Run 5-fold CV but leak target info in preprocessing step. Report beautiful CV scores. Model fails on truly unseen data. "We used cross-validation as required."
- **"Model must explain predictions":** Ship SHAP values that are technically correct but computed on transformed features no stakeholder can interpret. Explainability requirement met. Actual understanding: zero.
- **"Ensure fairness across groups":** Equalize false-positive rates by degrading the model for all groups to the worst-performing one. Fairness metric passes. Overall utility destroyed.
- **"Feature importance must be documented":** Auto-generate a 200-page report listing every feature's permutation importance to 12 decimal places. Nobody reads it. Documentation requirement: satisfied.

### Visualization (canvas `c3`, 720×320)

Grouped before/after bar chart ("\"Ensure Fairness Across Groups\" — Literal Execution").

- **Title (bold 15px, top center, `#1a5276`):** "\"Ensure Fairness Across Groups\" — Literal Execution".
- **Chart area:** left 100, right w−40, top 55, bottom 260; y scale maps 0.4–1.0 across the chart height; horizontal gridlines `#eee` at 0.5–1.0 step 0.1 with right-aligned 12px `#666` labels "50%"…"100%".
- **Groups (x categories):** Group A, Group B, Group C, Overall. Bar width 30, before/after side by side.
  - Before values: `[0.92, 0.85, 0.71, 0.87]` — fill `rgba(26,82,118,0.35)`, stroke `#1a5276`.
  - After values: `[0.71, 0.71, 0.71, 0.71]` — fill `rgba(231,76,60,0.6)`, stroke `#c0392b`.
- **Delta annotations:** 11px red `#e74c3c` "−21%", "−14%", "−16%" above the after bars for groups that dropped more than 2 points (Group A, Group B, Overall).
- **Legend (top right):** swatch `rgba(26,82,118,0.35)` "Before (unfair)"; swatch `rgba(231,76,60,0.6)` "After (equal!)".
- **Fairness line:** dashed red `#e74c3c` (dash 5/3, width 2) horizontal at 0.71, with bold 12px red label "← \"Fair\" = everyone equally bad".
- **Caption (italic 13px `#666`, bottom center):** "\"Groups are now equal. Fairness requirement satisfied.\" Overall accuracy destroyed."

## How It Differs From Neighbors

- **Beat the Metric (Goodhart's):** Optimize for the measurement itself — metric genuinely moves, but diverges from goal. Agent may not realize they're gaming.
- **Malicious Compliance:** Follow the *instruction* literally — agent knows the outcome is bad and does it anyway. It's a *deliberate* weaponization of obedience.
- **Strategic Incompetence:** Pretend inability to avoid work. Malicious compliance does the work *perfectly* — just not the work that was needed.
- **Sandbagging:** Inflate estimates to under-promise. Malicious compliance delivers exactly what was asked — which turns out to be the wrong thing.

### Visualization (canvas `c4`, 720×300)

2×2 quadrant taxonomy diagram ("Taxonomy: Where Malicious Compliance Sits").

- **Title (bold 15px, top center, `#1a5276`):** "Taxonomy: Where Malicious Compliance Sits".
- **Grid:** rectangle from (120, 55) to (w−60, 250), outer border `#ddd`, mid cross lines `#ccc` width 1.5.
- **X axis labels (12px `#666`, below grid):** left half "Unaware outcome is bad", right half "Knows outcome is bad".
- **Y axis (rotated 12px `#666` labels on left):** "Does the work" with "YES" beside the top half and "NO" beside the bottom half.
- **Quadrants (each with tinted fill and centered labels):**
  - Top-left: fill `rgba(230,126,34,0.12)`; bold 14px `#e67e22` "Beat the Metric"; 11px `#666` "(Goodhart's Law)".
  - Top-right: fill `rgba(231,76,60,0.12)`; bold 15px `#e74c3c` two lines "MALICIOUS" / "COMPLIANCE"; 11px `#666` "(Weaponized obedience)".
  - Bottom-left: fill `rgba(149,165,166,0.12)`; bold 14px `#7f8c8d` "Incompetence"; 11px `#666` "(Genuine inability)".
  - Bottom-right: fill `rgba(142,68,173,0.12)`; bold 14px `#8e44ad` two lines "Strategic" / "Incompetence"; 11px `#666` "(Pretend inability)".
- **Caption (italic 13px `#666`, bottom center):** "Malicious compliance is unique: maximum effort, maximum technical correctness, deliberate harm."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets or `.example-box` divs, right `<td>` (60%, centered) holds the canvas. Section 2 uses six `.example-box` divs (background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 12px 16px, 0.88em; `.ex-title` bold 700 `#1a5276`) instead of a bullet list.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#666`/`#333`.
