# Correlation vs Causation vs Co-occurrence

**Page type:** detail page (single long doc: numbered h2 sections, each an obj-table row with text left ~40% and canvas right ~60%; final section is a summary table plus philosophy callout)
**HTML title tag:** Correlation vs Causation vs Co-occurrence

**Subtitle:** Three relationships that look identical in data but support completely different decisions. Confusing them is the most expensive mistake in data science.

## 1. The Three Relationships

- **Co-occurrence:** A and B show up together, with no direction and no mechanism. A patient has both a fever and a rash — related or coincidence?
- **Correlation:** A and B move together systematically, but the data still can't say which drives which. Ice cream sales and drownings rise and fall together all year.
- **Causation:** Intervening on A changes B. This is the only relationship that supports "do X to achieve Y."
- **The dividing line:** Correlation is enough for prediction; causation is required for intervention. Most business decisions are interventions.

### Visualization (canvas `c1`, 720×280)

Horizontal bar ladder: three levels of relationship strength, each a wider bar than the last.

- **Title (bold 17px `#1a5276`, top center):** "Same Data, Three Possible Meanings".
- **Bars:** three horizontal bars starting at x=160, y = 55 + i×60, 38px tall, width = fraction × 420:
  - Co-occurrence — fraction 0.4, color `#e67e22`, example text inside: "fever + rash in one patient", italic "supports: flag for review".
  - Correlation — fraction 0.7, color `#2980b9`, example: "ice cream sales ↔ drownings", italic "supports: predict".
  - Causation — fraction 1.0, color `#27ae60`, example: "price cut → more units sold", italic "supports: act / intervene".
- **Bar style:** fill in level color at 25% alpha, 2px stroke in level color; level name right-aligned at x=150 in bold 15px level color; example text `#333` 14px at x=170; "supports:" line italic 14px in level color.
- **Caption (bottom center, bold 15px `#e74c3c`):** "Each level up permits stronger decisions — and requires stronger evidence."

## 2. Co-occurrence — Chance or Real?

- With enough items, some pairs co-occur by pure chance. The baseline is **expected co-occurrence = P(A) × P(B) × n**.
- Judge by **lift = observed / expected**. Lift near 1 means the pair is independent; lift above 2–3 means a real association worth investigating.
- "Diapers + beer" in the same cart has lift near 1.2 — both are just bought by parents on one big shopping trip. "Fever + rash" in the same patient has lift near 5 — a genuine clinical link.
- Even high lift only means "not independent." It says nothing about which causes which, or whether a third factor drives both.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: expected vs observed co-occurrence for three item pairs, with lift labels.

- **Title (bold 17px `#1a5276`, top center):** "Observed vs Expected Co-occurrence".
- **Data:** pairs (expected, observed): Diapers + Beer (exp 7.2, obs 8.5, lift 1.2); Fever + Rash (exp 2.4, obs 12, lift 5.0); Login + Exfil (exp 2.8, obs 3, lift 1.1).
- **Geometry:** baseline `#1a5276` at y=240 from x=70 spanning 3 groups of width 190; bars 55px wide, group offset 25px, bar heights scaled to max value 14 over 180px.
- **Colors:** expected bar fill `rgba(26,82,118,0.35)`; observed bar `#27ae60` when lift > 2, else `#e67e22`.
- **Labels:** above each bar, gray `#666` 13px: "exp 7.2" / "obs 8.5" etc.; pair name `#1a5276` 14px below baseline; lift line bold 15px (green `#27ae60` if lift > 2, else `#999`): "lift 1.2× — chance", "lift 5.0× — real", "lift 1.1× — chance".
- **Caption (bottom center, `#555` 14px):** "Observed ≈ expected means independence. Only a large gap is a real association."

## 3. Correlation — Real r, Hidden Driver

- Monthly ice cream sales and drowning deaths correlate at r ≈ 0.85 — a completely real correlation, yet neither causes the other.
- Color the same points by season and the trick is exposed: within summer alone, or winter alone, the correlation is ≈ 0. Hot weather drives both variables.
- For **prediction** this correlation is still useful — ice cream sales genuinely forecast drowning risk. For **intervention** it is worthless — banning ice cream saves nobody.

### Visualization (canvas `c3`, 720×340)

Scatter plot of two season clusters showing a between-cluster correlation with zero within-cluster slope.

- **Title (bold 17px `#1a5276`, top center):** "r = 0.85 Overall — r ≈ 0 Within Each Season".
- **Axes:** `#1a5276` 2px L-shape, origin (70, 280), width 590, height 220; x-axis label "Ice Cream Sales →" centered below; y-axis label "Drownings →" rotated vertical on the left. Labels 15px `#1a5276`.
- **Winter cluster:** 30 dots (4px radius) `rgba(41,128,185,0.55)`, x uniform in [origin+40, origin+190], y uniform in [baseline−30, baseline−90] (seeded PRNG, mulberry32 seed 11); no internal slope.
- **Summer cluster:** 30 dots `rgba(230,126,34,0.6)`, x uniform in [origin+370, origin+540], y uniform in [baseline−130, baseline−195]; no internal slope.
- **Overall trend line:** dashed red `#e74c3c` (dash 6/4, width 2) from (origin+30, baseline−40) to (origin+560, baseline−185).
- **Within-cluster flat lines:** solid gray `#555` width 2 — winter at y=baseline−60 from x=origin+40 to origin+190; summer at y=baseline−162 from x=origin+370 to origin+540.
- **Labels (bold 14px):** "Winter (r ≈ 0)" in `#2980b9`; "Summer (r ≈ 0)" in `#e67e22`; "overall r = 0.85" in `#e74c3c`, right-aligned near top right.
- **Caption (bottom center, `#555` 14px):** "The correlation lives entirely between the clusters — heat drives both variables."

## 4. Spurious Correlation — Trends Fool r

- Any two series that both trend upward correlate strongly: US science spending tracks suicides by hanging at r ≈ 0.99 over a decade — pure coincidence.
- The fix is to **detrend**: correlate the year-over-year changes instead of the levels. For spurious pairs the r collapses to ≈ 0.
- The multiple-comparisons trap makes it worse: 1,000 variables give ~500,000 pairs, so thousands will pass p < 0.05 by chance alone.
- **Rule:** a correlation without a mechanism that was plausible *before* seeing the data is evidence of nothing.

### Visualization (canvas `c4`, 720×320)

Two-panel comparison: raw trending time series (left) vs scatter of year-over-year changes (right).

- **Title (bold 17px `#1a5276`, top center):** "Raw Levels: r = 0.99 — Year-over-Year Changes: r ≈ 0".
- **Left panel:** axes at origin (55, 250), width 280, height 190, `#1a5276` 2px. Two 12-point rising lines (width 2.5) climbing ~140px across the panel with small seeded jitter (mulberry32 seed 7, ±0.5 scaled ×14px): blue `#2980b9` line labeled "Science spending" and red `#e74c3c` line labeled "Suicides by hanging" (labels 14px, top-left of panel). Below panel, bold 14px `#333` centered: "Raw levels: r = 0.99".
- **Right panel:** axes at origin (400, 250), width 265, height 190. Scatter of the 11 year-over-year delta pairs from the same jitter series, dots `rgba(26,82,118,0.55)` radius 5, centered on panel midpoint (deltas ×130 horizontal, ×90 vertical) — visually patternless. Dashed gray `#999` horizontal midline (dash 4/3, width 1.5). Below panel, bold 14px `#333`: "Changes: r ≈ 0".
- **Caption (bottom center, `#555` 14px):** "The shared upward trend was the entire "relationship." Detrending removes it."

## 5. Confounders — The Hidden Third Variable

- **The pattern:** a hidden C causes both A and B, so A and B correlate and A appears to cause B.
- **Hospital mortality:** the trauma center shows higher death rates than the community hospital — because it receives the sickest patients. Without risk adjustment, the best hospitals look worst.
- **Education → income:** partially causal, but parental wealth boosts both education and income, so the raw correlation overstates the true effect of schooling.
- **In ML:** a confounded feature predicts fine, but the model misleads the moment someone uses it to decide what to change.

### Visualization (canvas `c5`, 720×300)

Confounder DAG: three labeled circle nodes with solid causal arrows and a dashed spurious edge.

- **Title (bold 17px `#1a5276`, top center):** "Hidden C Drives Both — A Appears to Cause B".
- **Nodes:** filled circles radius 28 with white bold 18px letter inside and a 14px sub-label 48px below in node color:
  - C at (360, 75), `#8e44ad`, sub-label "severity of patients".
  - A at (180, 190), `#2980b9`, sub-label "trauma center".
  - B at (540, 190), `#e74c3c`, sub-label "mortality".
- **Causal arrows:** solid `#8e44ad`, width 2.5, filled arrowheads: C→A and C→B.
- **Spurious edge:** dashed red `#e74c3c` (dash 5/4, width 2) horizontal line from A to B, labeled above in 14px red: 'spurious "A causes B"'.
- **Caption (bottom center, `#555` 14px):** "Sickest patients go to the trauma center AND die more often — the best hospital looks worst."

## 6. Direction — Which Causes Which?

- Correlation is symmetric; causation is directional. The same r is produced by A→B, B→A, or C→both, and the data alone cannot orient the arrow.
- **Support tickets ↔ churn:** bad experience causes tickets then churn — or engaged users file tickets and stay, while silent users just leave. Tickets might *prevent* churn.
- **Ads seen ↔ purchases:** ads drive buying — or high-intent users browse more pages and therefore see more ads. The direction decides whether ad spend works or is wasted.
- Orienting the arrow needs at least one of: temporal ordering, an intervention, or a known mechanism.

### Visualization (canvas `c6`, 720×300)

Three-row diagram: variable pairs joined by a bidirectional arrow, each with two competing causal stories.

- **Title (bold 17px `#1a5276`, top center):** "Same r, Opposite Stories".
- **Rows:** at y = 52 + i×76, each row on a light band `rgba(26,82,118,0.06)` (full width minus 30px margins, 66px tall). Per row: left term bold 15px `#2980b9` right-aligned at x=195; "↔" bold 18px `#e67e22` at x=222; right term bold 15px `#27ae60` at x=250; then two story lines starting x=370 — "Story 1: ..." in `#333` 14px and "Story 2: ..." in `#e74c3c` 14px.
  1. Support tickets ↔ Churn — Story 1: bad product → tickets → churn; Story 2: tickets resolve issues → LESS churn.
  2. Ads seen ↔ Purchases — Story 1: ads → buying; Story 2: intent → browsing → more ads seen.
  3. Onboarding done ↔ Retention — Story 1: onboarding → sticky users; Story 2: motivated users → finish onboarding.
- **Caption (bottom center, `#555` 14px):** "Both stories produce identical data. Each pair demands opposite interventions."

## 7. Establishing Causation

- **The gold standard is randomization:** an RCT (in tech, an A/B test) breaks the link between treatment and every confounder, measured or not.
- **The gap is real money:** observationally, "users of Feature X retain 3×." The A/B test shows forcing users through X improves retention only 1.2×. The other 1.8× was selection — engaged users find X, X doesn't create engaged users.
- When you can't randomize, use quasi-experimental designs — each one a step weaker (table below).

Embedded summary table (in the left cell, below the bullets):

| Method | Idea |
|--------|------|
| **RCT / A/B test** | Random assignment removes all confounders |
| **Regression discontinuity** | Compare near-identical cases on either side of a sharp cutoff |
| **Instrumental variable** | Something that moves A but touches B only through A |
| **Difference-in-differences** | Treated group's change minus control group's change |
| **Observational + controls** | Adjust for confounders you measured — blind to the rest |

### Visualization (canvas `c7`, 720×320)

Difference-in-differences line chart: treated vs control vs counterfactual retention over time.

- **Title (bold 17px `#1a5276`, top center):** "Difference-in-Differences: Only the Extra Jump Is Causal".
- **Axes:** `#1a5276` 2px L-shape at origin (70, 270), width 580, height 215; x label "Time →" centered below, y label "Retention →" rotated vertical left; labels 15px `#1a5276`.
- **Treatment marker:** vertical dashed orange `#e67e22` line (dash 5/4, width 1.5) at x = origin+270, labeled at top in bold 13px orange: "feature launched".
- **Control line:** solid `#2980b9` width 2.5 through (origin+20, base−40) → (launch, base−68) → (origin+545, base−96) — steady background trend.
- **Treated line:** solid `#27ae60` width 2.5: parallel pre-trend (origin+20, base−72) → (launch, base−100), then steeper post-launch segment (launch, base−100) → (origin+545, base−183).
- **Counterfactual:** dashed gray `#999` (dash 6/4, width 2) from (launch, base−100) to (origin+545, base−128) — the treated group's parallel-trend continuation.
- **Causal-effect marker:** red `#e74c3c` vertical arrow at the right edge from the counterfactual endpoint (base−128) up to the treated endpoint (base−180), with bold 14px red two-line label "causal" / "effect" to its left.
- **Series labels:** bold 14px — "treated" in `#27ae60` and "control" in `#2980b9` near the left ends; "counterfactual (parallel trends)" in `#999` 13px along the dashed line.
- **Caption (bottom center, `#555` 14px):** "Both groups share the background trend. Treated change minus control change isolates the true effect."

## 8. When Does It Matter?

Full-width summary table (no canvas in this section):

| Task | Needs | Why |
|------|-------|-----|
| Predict who will churn | **Correlation** (green `#27ae60`) | You only need signal, not mechanism |
| Decide what to do about churn | **Causation** (red `#e74c3c`) | You need to know which intervention actually works |
| Recommend similar products | **Co-occurrence** (green `#27ae60`) | Bought-together signal is enough |
| Allocate ad budget | **Causation** (red `#e74c3c`) | Did the ad cause the purchase, or would they have bought anyway? |
| Fraud risk scoring | **Correlation** (green `#27ae60`) | Flagging suspicious patterns doesn't require mechanism |
| Decide which feature to build | **Causation** (red `#e74c3c`) | Will building X actually move Y? |

## Callout (philosophy box, end of page)

**One sentence:** Co-occurrence means "both present," correlation means "move together," causation means "one drives the other" — correlation is enough to predict, but only causation is safe to act on.

## Regeneration instructions

- **Layout:** single long detail page. h1 + `.subtitle`, then numbered h2 sections (each h2 has an `id` anchor: definitions, cooccurrence, correlation, spurious, confounders, direction, testing, practical). Sections 1–7 each use a `.obj-table`: full-width table, one `<tr>` with left `<td>` (40%) holding a `<ul>` of bullets (section 7 also embeds a `.summary-table` below its bullets) and right `<td>` (60%, centered) holding the canvas. Section 8 is a standalone `.summary-table` followed by a `.philosophy` callout.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with bottom border `2px solid #2980b9`, padding-bottom 6px; subtitle `#666` 1.05em; bullets 0.9em `#333`; `strong` in `#1a5276`. `.obj-table` cells border `1px solid #e0e0e0`, padding 12px 18px, vertical-align middle. `.obj-title` style exists (1.05em, weight 600, `#1a5276`) but section titles are h2 headings here. No nav bar, no back/home links.
- **Summary tables:** `.summary-table` — full width, 0.9em, th background `#f0f4f8` color `#1a5276` padding 10px 14px left-aligned, td padding 10px 14px, borders `1px solid #e0e0e0`. In section 8, the "Needs" column values are wrapped in `<strong style="color:...">` (green `#27ae60` for Correlation/Co-occurrence, red `#e74c3c` for Causation).
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Canvases:** declare intrinsic `width`/`height` attributes as given per chart (all 720 wide; heights 280/300/340/320/300/300/320); CSS `width: 100%`, max-width 620px, centered. Scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Random scatter uses a seeded mulberry32 PRNG (seed 11 for c3, seed 7 for c4) so renders are deterministic; arrows drawn by a shared helper with 10px filled arrowheads. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#555`/`#666`/`#999`.
