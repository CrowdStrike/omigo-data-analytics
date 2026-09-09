# Banking Operations

**Page type:** detail page (one h2 per pitfall, each followed by a one-row two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** Banking Operations - Domain-Specific Pitfalls

**Subtitle:** Statistical pitfalls in banking data systems — settlement timing, regulatory noise, and the gap between displayed and actual state.

## Real-time vs Batch Settlement

**Displayed Balance ≠ Settled Balance for 1-3 Days**

- **Two clocks:** The transfer shows "pending" instantly; the money actually moves 1-3 days later.
- **Both systems right:** The display and the settlement ledger disagree on temporal reality, not on facts.
- **The danger zone:** Every read between display and settlement returns an unsettled number.
- **Modeling cost:** A feature named `balance` silently means different things depending on read time.

### Visualization (canvas `canvas1`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Settlement timeline with a highlighted danger zone.

- **Background:** light gray `#f9f9f9` fill.
- **Title (top left, bold 17px, `#1a5276`):** "Settlement Timeline".
- **Timeline:** dark `#333` horizontal line (width 2) at y=100 from x=60 to x=680, with an open arrowhead at the right end.
- **Markers (dots radius 8):**
  - x=120: blue `#2980b9` dot; bold "T=0" below (in `#1a5276`), "Initiated" above (15px).
  - x=200: green `#27ae60` dot; bold "Displayed" above, "(instant)" below.
  - x=520: purple `#8e44ad` dot; bold "Actually Settled" above, "(+2 days)" below.
- **Danger zone:** rectangle from x=200 to x=520 spanning y=55–145, fill `rgba(231,76,60,0.15)`, dashed `#e74c3c` border (dash 5/3, width 1.5); bold red 17px label "DANGER ZONE" above the zone and 14px caption "Data inconsistent between systems" below its center.

## Anti-money Laundering Over-filing

**~90% of SARs Are Filed Defensively, Not Suspiciously**

- **The incentive:** Penalties punish under-filing, so compliance files on anything arguable.
- **What the label means:** "Suspicious" mostly encodes officer anxiety about regulators.
- **What a model learns:** Predicting filings, not predicting crime — the target is the wrong variable.
- **Signal-to-noise:** Roughly 1 genuine case per 10 filings buries the real pattern.

### Visualization (canvas `canvas2`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Stacked horizontal bar showing SAR filing composition.

- **Background:** `#f9f9f9`.
- **Title (top left, bold 17px, `#1a5276`):** "SAR Filing Composition".
- **Bar (x=100, y=50, 500×50, `#333` border):** left 90% segment filled `#e74c3c` with white bold 17px label "90% Defensive / CYA Noise"; right 10% segment filled `#27ae60` with white 12px label "10%".
- **Legend (swatches 20×20 at x=150):** red swatch + "Defensive filings (avoid regulatory penalty)"; green swatch + "Genuine suspicious activity (~10%)" — text `#333` 17px.
- **Caption (bottom center, italic 14px, `#888`):** "Signal is buried in compliance-driven noise".

## Multi-currency Settlement Timing

**Three Valid FX Rates, Three Different Amounts, One Transaction**

- **The timeline:** Trade Monday, settle Wednesday, report Friday — each date has its own rate.
- **Illustrative spread:** The same trade values at $10,200 / $10,350 / $10,180.
- **No canonical answer:** "How much was this?" has three defensible answers that disagree.
- **Reconciliation trap:** Joining systems that picked different rate dates produces false breaks.

### Visualization (canvas `canvas3`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Three-bar chart: same transaction valued at three FX dates.

- **Background:** `#f9f9f9`.
- **Title (top left, bold 17px, `#1a5276`):** "Same Transaction, Different FX Rates".
- **Bars (width 130, gap 50, from x=130, baseline y=175, scale $10,000–$10,500 over 120px):**
  - "Monday (Trade)" = $10,200, fill `#2980b9`.
  - "Wednesday (Settle)" = $10,350, fill `#8e44ad`.
  - "Friday (Report)" = $10,180, fill `#e67e22`.
- **Value labels:** bold 17px in each bar's color above the bar ("$10,200", "$10,350", "$10,180"); day labels 14px `#333` below.
- **Y axis:** thin `#999` line with right-aligned 13px `#666` labels "$10,000" (baseline) and "$10,500" (top).
- **Inequality annotations:** dashed red `#e74c3c` horizontal segments (dash 4/3, width 1.5) between adjacent bars at mid-height, each topped with a bold red 17px "≠".

## KYC Data Staleness

**"Verified Customer" Means Verified at a Date Long Past**

- **What decays:** Address, income, employer, and risk profile all drift after verification.
- **Silent decay:** No field flags that the record has diverged from the person.
- **Risk on a ghost:** Scoring stale KYC scores someone who no longer exists.
- **What's missing:** A verification-age feature, so the model can discount old records.

### Visualization (canvas `canvas4`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Exponential decay curve of KYC accuracy over 5 years.

- **Background:** `#f9f9f9`.
- **Title (top left, bold 17px, `#1a5276`):** "KYC Accuracy Decay Over Time".
- **Axes:** `#333` L-shaped axes, origin (80, 165), width 580, height 120. Y labels (14px `#666`, right-aligned): "100%", "60%", "40%", "0%". X labels with tick marks: "0 yr" through "5 yr".
- **Decay curve:** red `#e74c3c` line width 3 following `accuracy = 0.38 + 0.62·exp(-0.35·years)` from 100% at year 0 to ~49% at year 5 (asymptote ~38%).
- **Threshold line:** dashed orange `#e67e22` horizontal line (dash 6/4, width 1) at 60%, labeled "Unreliable threshold" (14px orange, near right end above the line).
- **Markers:** green `#27ae60` dot (radius 6) at the start with bold green label "100%"; red `#e74c3c` dot at the end with bold red right-aligned label "~49%" (computed from the decay formula at 5 years).

## Check Hold / Provisional Credit

**Provisional Credit Shows Money That May Not Exist**

- **The sequence:** Bank credits the account, the check bounces days later, the credit reverses.
- **Fictional balance:** During the hold, the displayed balance overstates real funds.
- **Schema gap:** Nothing distinguishes settled money from provisional credit in the data.
- **Downstream effect:** Any model reading "current balance" mid-hold trains on numbers that got retracted.

### Visualization (canvas `canvas5`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Step-line balance timeline with a fictional-balance zone.

- **Background:** `#f9f9f9`.
- **Title (top left, bold 17px, `#1a5276`):** "Balance Timeline with Provisional Credit".
- **Axes:** `#333` axes, origin (80, 170), width 580, height 120; scale $0–$4,000. Y labels (13px `#666`): "$4,000", "$3,500", "$1,000", "$0". X labels: "Day 0" through "Day 5" (six labels over a 0–6 day scale).
- **Balance line (width 3):**
  - Day 0→1 at $1,000: solid blue `#2980b9`.
  - Day 1 vertical jump $1,000→$3,500: dashed green `#27ae60` (dash 4/3).
  - Day 1→3 at $3,500: solid red `#e74c3c` (provisional).
  - Day 3 vertical drop $3,500→$1,000: dashed red (dash 4/3, width 2).
  - Day 3→5 at $1,000: solid blue.
- **Fictional zone:** `rgba(231,76,60,0.12)` rectangle from Day 1 to Day 3 spanning from $3,800 down to the baseline, with bold red 14px label "FICTIONAL BALANCE" centered above it.
- **Markers (dots radius 5):** blue at (Day 0, $1,000); green at (Day 1, $3,500); red at (Day 3, $1,000).
- **Annotations (17px):** "$1,000" in blue near Day 0; "+$2,500 credit" in green near Day 1 peak; "Bounced!" in red right-aligned near the Day 3 drop (at ~$2,200 height).

## Regulatory Reporting Consistency

**One Number, Three Regulators, Three Definitions of "Exposure"**

- **No canonical definition:** Each regulator's "exposure" has a different scope and netting rule.
- **Internal mismatch:** The warehouse figure matches none of the three filed numbers.
- **Permanent approximation:** Reconciliation settles at "approximately right," never at equal.
- **Consequence:** Any single queried value is simultaneously correct and wrong depending on the definition.

### Visualization (canvas `canvas6`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Four-bar chart: the same "exposure" reported as four different numbers.

- **Background:** `#f9f9f9`.
- **Title (top left, bold 17px, `#1a5276`):** "Same "Exposure" — Four Different Numbers".
- **Bars (width 100, gap 45, from x=105, baseline y=170, max 180 over 110px):**
  - "Regulator A" = $142M, fill `#2980b9`.
  - "Regulator B" = $158M, fill `#8e44ad`.
  - "Regulator C" = $137M, fill `#e67e22`.
  - "Internal" = $151M, fill `#7f8c8d`.
- **Gridlines:** five light `#ddd` horizontal lines (width 0.5) across the bar area; y-axis unit label "$M" (13px `#666`).
- **Value labels:** bold 17px in each bar's color above the bar ("$142M", "$158M", "$137M", "$151M"); names 14px `#333` below.
- **Inequality marks:** bold 20px red `#e74c3c` "≠" between each adjacent pair of bars at mid-height.
- **Caption (centered below bar labels, italic 14px red):** "All measuring the same thing — none agree".

## Regeneration instructions

- **Template/layout:** domains detail page. h1 + `.subtitle`, then per pitfall an `<h2>` (blue `#1a5276`, 1.4em, bottom border `2px solid #2980b9`) followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` (1.05em, weight 600, `#1a5276`, a one-line punchline) followed by a `<ul>` of labeled one-line bullets (`ul` 0.9em `#333`, 20px left margin; `li` 4px vertical margin; `<strong>` label + short phrase); right `<td>` (60%, centered) with the canvas. Even table rows have background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `strong` in `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but unused on this page. No nav bar, no back/home links.
- **Canvases:** six canvases (`canvas1`–`canvas6`) declare `width="720" height="300"` attributes in the HTML, but the shared `setupCanvas(id)` helper overrides each to 720×200 CSS pixels and scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All charts paint a `#f9f9f9` background; default label font 17px `-apple-system, sans-serif`.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; supporting colors `#2980b9`, `#8e44ad`, `#7f8c8d`, `#c0392b`, grays `#666`/`#333`/`#888`/`#999`/`#ddd`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
