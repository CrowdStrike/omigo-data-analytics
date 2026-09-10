# Pitfall: Proxy Variables (Hidden Sensitive Attributes)

**Page type:** detail page (card-section layout: h2 per section, two-column table with text left 45% / canvas right 55%)
**HTML title tag:** Proxy Variables (Hidden Sensitive Attributes)

**Subtitle:** Model learns protected attributes through correlated features despite explicit removal.

## The Problem

**Tags:** `the trap` (red), `fairness` (blue)

- **Removal is not protection** — dropping race, gender, or age leaves correlated stand-ins behind
- **Geography** — zip code correlates around r=0.85 with race in many segregated US cities
- **Names** — a first name alone predicts gender with roughly 95% accuracy
- **Career history** — job title plus years of experience together act as a proxy for age
- **Financial trail** — credit score and income correlate with several protected classes
- **The result** — the model reconstructs the attribute and discriminates while looking compliant

*Example:* A credit model removes gender, but cosmetics purchase frequency (F=94%) and automotive parts (M=89%) reconstruct it.

**Impact:** Removing the protected attribute does not prevent discrimination — the model reaches the same disparate outcomes through correlated proxies.

### Visualization (canvas `c1`, 720×300)

Correlation-network diagram: removed protected attribute feeding proxy features that feed the model decision.

- **Title (bold 14px, `#1a5276`, centered):** "Protected Attribute → Proxy Features → Model Decision".
- **Left node:** dashed (6,4) red `#e74c3c` circle (radius 50, 3px stroke) at (150, 150) with red text lines: "Protected" / "Attribute" / "(REMOVED)".
- **Middle proxy boxes** (120×40, white fill, 2px `#2980b9` border, blue `#1a5276` label) at x=350: "Zip Code" (y=80), "First Name" (y=150), "Purchase Pattern" (y=220).
- **Orange edges** (2px `#e67e22`) from the circle to each proxy box, each labeled midway in 10px orange with its correlation: "r=0.85" (Zip Code), "r=0.92" (First Name), "r=0.78" (Purchase Pattern).
- **Right node:** 140×50 box at x≈570 (white fill, 3px green `#27ae60` border) with bold green text: "MODEL" / "DECISION".
- **Blue edges** (2px `#2980b9`) from each proxy box to the model box.

## Why It Happens

**Tags:** `root cause` (orange), `correlations` (blue)

- **Embedded attributes** — protected traits are woven into the social structure of the data
- **Residential segregation** — zip code and neighborhood encode race via segregated housing
- **Behavioral fingerprints** — purchases, browsing, and product preferences differ by gender
- **Career timelines** — job sequences, tenure, and seniority track age closely
- **Downstream features** — neutral-looking features caused by protected traits inherit the link
- **Many pathways** — the correlations are strong, systematic, and too numerous to prune away

*Example:* Remove "age" but keep "years since account creation" — a feature that correlates with age at r=0.92.

**Root Cause:** If a remaining feature X correlates strongly with protected attribute Y and predicts the outcome, the model implicitly learns Y through X.

### Visualization (canvas `c2`, 720×300)

Bar chart of approval rate by protected group showing disparate impact.

- **Title (bold 14px, `#1a5276`, centered):** "Approval Rate by Protected Group (Gender Removed From Features)".
- **Bars:** width 100, baseline y=260, max height 180 (height = rate × 180):
  - "Group A" at x=200: 82%, green `#27ae60` (rate > 0.75).
  - "Group B" at x=400: 54%, red `#e74c3c` (rate ≤ 0.60).
  - "Group C" at x=600: 68%, orange `#e67e22` (0.60 < rate ≤ 0.75).
  - Fill at 0.7 alpha with 2px solid border in the same color; bold 14px percentage label above each bar ("82%", "54%", "68%"); 12px `#444` group label below.
- **Fairness threshold line:** dashed (6,4) 2px `#2980b9` horizontal line at the 70% level from x=100 to x=700, labeled left in 11px blue: "Fairness target: 70%".
- **Annotation (bold 12px red, centered near top):** "28% gap between groups despite removing protected attribute!".

## The Correct Approach

**Tags:** `the fix` (green), `auditing` (blue)

- **Exclusion is not enough** — fairness must be measured on outcomes, not on the feature list
- **Correlation audit** — check each remaining feature against the protected attributes
- **Reconstruction test** — predict the attribute from features; AUC around 0.7 flags proxies
- **Disparate impact check** — compare approval and rejection rates across protected groups
- **Fairness constraints** — train with demographic parity or equalized odds constraints
- **Debiasing methods** — use adversarial debiasing or reweighting when constraints fall short

*Example:* A loan model approves 82% of group A but 54% of group B despite removing race; the audit finds zip code (r=0.85) as the proxy.

**Fix:** Audit for disparate impact on protected groups and, if found, apply fairness-constrained optimization, reweighting, or adversarial training.

### Visualization (canvas `c3`, 720×300)

Proxy-detection flow diagram: remaining features into a reconstruction model that predicts the protected attribute.

- **Title (bold 14px, `#1a5276`, centered):** "Proxy Detection: Can We Predict Protected Attribute From Remaining Features?".
- **Left column** headed "Remaining Features" / "(after removal)" (12px blue, centered at x=150): five stacked boxes 140×25 (white fill, 1.5px `#2980b9` border, 10px blue labels): "Zip Code", "First Name", "Purchase", "Device", "Payment".
- **Middle box:** 120×60 at (320, 140), white fill, 3px orange `#e67e22` border, bold orange text: "Reconstruction" / "Model". Blue arrow (2px `#2980b9`) from the feature stack into it.
- **Right box:** 160×60 at (500, 140), white fill, 3px red `#e74c3c` border, bold red text: "Predicted Gender" and 11px "AUC = 0.89". Blue arrow from the model into it.
- **Verdict (centered, red):** bold 13px "PROXIES DETECTED: Model can reconstruct protected attribute", then 11px "Remaining features rank gender with AUC 0.89 — far above the 0.5 chance level".

## Regeneration instructions

- **Layout:** `.card-section` per section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (border-collapse, full width) with one `<tr>`: `td.text-col` (45%) holding `.tags` pills + `<ul>` bullets + `.example` italic paragraph + `.key-point` callout; `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `li b` colored `#1a5276`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card links use `.html` extensions.
