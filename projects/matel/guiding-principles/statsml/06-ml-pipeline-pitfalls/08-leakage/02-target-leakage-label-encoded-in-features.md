# Pitfall: Target Leakage (Label Encoded in Features)

**Page type:** detail page (card-sections with h2 headings; each section is a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Target Leakage (Label Encoded in Features)

**Subtitle:** A feature is a direct proxy or consequence of the label

## The Problem

Tags: `the trap` (red pill), `label proxy` (blue pill)

- **The trap** — a star feature is a disguised copy of the label, set after the outcome happened
- **Reversed causation** — "treatment_given=chemo" predicts cancer because the diagnosis caused it
- **Answer key** — the model reads the label through the proxy instead of learning to predict
- **Telltale symptoms** — near-perfect accuracy with importance dominated by a single feature
- **No generalization** — the model is useless on new data where the proxy is absent or empty

*Example:* In a loan-default model, "collection_calls_made" is 0 for good loans and positive for defaults — but the calls happen after the default.

**Impact:** Offline metrics are inflated and the model delivers no value in production, because the one feature carrying the signal is empty at prediction time.

### Visualization (canvas `c1`, 720×300)

Two-row box-and-arrow diagram contrasting assumed vs actual causal direction.

- **Title (bold 14px `#1a5276`, top center):** "Causal Direction: What the Model Assumes vs Reality".
- **Top row (green `#27ae60`):** heading "What Model Assumes" centered; left 160×80 white box (green stroke) labeled "Feature" / "treatment_given" / "= chemo" at x=120; solid green arrow (width 3, filled arrowhead) labeled "predicts" pointing right to a matching box labeled "Target" / "has_cancer" / "= yes" at x=440. Row starts at y=70.
- **Bottom row (red `#e74c3c`):** heading "Reality (Reversed Causation)" centered; left box labeled "Target" / "has_cancer" / "= yes"; solid red arrow labeled "causes" pointing right to a box labeled "Feature" / "treatment_given" / "= chemo". Row starts at y=180.
- **Bottom message (bold red, centered):** "The feature is a consequence of the target, not a predictor. Perfect training accuracy, useless in production."

## Why It Happens

Tags: `root cause` (orange pill), `causation` (blue pill)

- **Seductive signal** — the feature really does track the label in history, so it looks great
- **Correlation confusion** — perfect correlation says nothing about which way causation flows
- **Consequence columns** — some fields are populated only after the outcome is determined
- **Blind joins** — pipelines join tables without checking the temporal order of events
- **Lab-only feature** — the column exists historically but will be missing at prediction time
- **Offline blindness** — evaluation reuses the same snapshot, so it cannot expose the leak

*Example:* In a churn model, "account_closed_date" is non-null only for churned customers and yields 0.99 AUC — but closure is churn itself.

**Root Cause:** Nobody asks which direction causation flows — a feature perfectly correlated with the target is suspicious, not impressive.

### Visualization (canvas `c2`, 720×300)

Three-scenario causal diagram: legitimate predictor, target leakage, confounded.

- **Title (bold 14px `#1a5276`, top center):** "The Causation Trap: Feature is a Consequence".
- **Scenario 1 (green `#27ae60`, y=55):** left label "1. Legitimate predictor"; 100×30 white box "Feature" at x=320 with a green arrow (width 2, filled arrowhead) to a "Target" box at x=480.
- **Scenario 2 (red `#e74c3c`, y=135):** left label "2. Target leakage"; "Target" box at x=320 with a red arrow (reversed direction of causation) to a "Feature" box at x=480.
- **Scenario 3 (orange `#e67e22`, y=215):** left label "3. Confounded"; a 100×25 "Hidden Variable" box centered above (at x≈345, y=200), with two orange arrows fanning down-left to a "Feature" box (x=320, y=240) and down-right to a "Target" box (x=480, y=240).
- **Bottom message (bold red, centered):** "Only scenario 1 is valid. Scenarios 2 and 3 produce misleading accuracy."

## The Correct Approach

Tags: `the fix` (green pill), `causal audit` (blue pill)

- **Reasoning exercise** — reconstruct when each feature's value arises relative to the outcome
- **Causal direction check** — only features whose values are set before the outcome are legitimate
- **Remove consequences** — drop any feature the target determines, whatever accuracy it adds
- **Trace generation** — draw a timeline of when each column receives its value, step by step
- **Suspicion trigger** — a lone feature with AUC above ~0.95 earns a mandatory causal review
- **Domain review** — experts confirm each feature truly exists before the outcome occurs

*Example:* After removing "collection_calls_made", the loan-default model drops from 0.98 to 0.76 AUC — the 0.76 is the real performance.

**Fix:** Trace the causal chain of every high-power feature; if target → feature, it is leakage regardless of correlation strength — remove it and accept the accuracy drop.

### Visualization (canvas `c3`, 720×300)

Timeline plus feature-audit checklist.

- **Title (bold 14px `#1a5276`, top center):** "Correct: Causal Direction Audit".
- **Timeline:** horizontal `#333` axis (width 2) from x=80 to x=640 at y=100, with three vertical tick markers (width 3, ±20px) and bold labels above: "Feature Value Set" (`#1a5276`) at x=180, "Prediction Point" (`#e67e22`) at x=360, "Outcome Determined" (`#e74c3c`) at x=540.
- **Checklist (starting y=150):** bold blue heading "Feature Audit Checklist:".
  - Valid rows (green `#27ae60` "✓" at x=90, feature name in `#333`, green note "(set before prediction)" at x=250): income_level, credit_score, debt_to_income.
  - Invalid rows (red `#e74c3c` "✗", red note "(set after outcome — REMOVE)"): collection_calls, account_closed_date.
  - Rows 22px apart.
- **Bottom message (bold green, centered):** "Rule: If feature value is determined after the outcome, it is leakage. Remove it."

## Regeneration instructions

- **Layout:** h1 with 2px `#2980b9` bottom border, `.subtitle` paragraph, then three `.card-section` divs. Each section has an `h2` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with one row: `td.text-col` (45%, top-aligned) holding `.tags` pills + `<ul>` bullets + `.example` italic line + `.key-point` callout; `td.viz-col` (55%) holding the canvas.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.red` `rgba(231,76,60,0.12)`/`#e74c3c`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`.
- **Bullets:** `<li><b>Label</b> — sentence</li>`; `li b` colored `#1a5276`; list 0.92rem. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem, with bold lead word.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`. Canvases have `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** all three canvases declare intrinsic 720×300; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
