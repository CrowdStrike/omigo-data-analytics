# Mathematical Costume

**Page type:** detail page (four separate obj-table blocks, each a two-column row: text left 40%, canvas right 60%)
**HTML title tag:** Mathematical Costume — Common Bad Practices

**Subtitle:** Taking a metric everyone already understands, applying a monotonic transformation (logs, powers, constants), and presenting it as a novel signal. The ranking doesn't change. The decisions don't change. Only the formula gets longer.

## Section 1: The Core Trick: Same Signal, Different Transform

A metric carries information. A monotonic transformation of that metric carries *the same* information — it ranks every item identically. Adding logs, exponents, or multipliers makes a formula look new without adding signal.

**Example 1:** "Interaction Propensity Score"

Formula block (monospace): `IPS = 2^(log₂(clicks) − log₂(impressions))`

Arrow (orange, centered): ↓ simplify ↓

Formula block: `= 2^(log₂(clicks/impressions))`

Arrow: ↓ that's just ↓

Plain block (green): clicks / impressions = CTR. The logs and power cancel out perfectly. Same number, more Greek.

**Example 2:** "Normalized Engagement Index"

Formula block: `NEI = exp(α·ln(clicks) − α·ln(views) + β)`

Arrow: ↓ simplify ↓

Formula block: `= e^β · (clicks/views)^α`

Arrow: ↓ that's just ↓

Plain block: CTR raised to a power and scaled by a constant. Same ranking as CTR. α and β add zero information.

**Example 3:** "Content Resonance Factor"

Formula block: `CRF(post) = likes + 2×comments + 3×shares`

Arrow: ↓ that's just ↓

Plain block: Weighted sum of reactions. The weights (1, 2, 3) are made up. Rank-equivalent to likes + shares for most distributions.

### Visualization (canvas `c1`, 720×340)

Scatter plot: fancy metric ("UES") vs plain CTR, nearly perfect correlation.

- **Titles (left-aligned at plot left):** bold 13px `#1a5276` '"User Engagement Score" vs. plain CTR — are they different?'; 11px `#555` "Each dot is one page. If they carry the same info, dots form a line."
- **Margins:** top 45, right 30, bottom 50, left 70.
- **Data:** 60 points; ctr = 0.01 + (i/60)×0.14 for i=0..59; ues = ctr + deterministic noise `(((i*7+13) % 19) − 9)/600`. Both axes scale 0–0.16.
- **Grid:** `#f0f0f0` lines at 0%, 5%, 10%, 15% on both axes with 9px `#888` percent labels.
- **Axis labels (11px `#444`):** x "CTR (the metric everyone already knows)"; y (rotated) '"UES" (the dressed-up version)'.
- **Points:** 4px circles, fill `rgba(26,82,118,0.5)`.
- **Correlation line:** dashed (5/3) red `#e74c3c` width 2 diagonal from (0.01, 0.01) to (0.15, 0.15).
- **Annotation (centered at w/2+100):** bold 14px red "r = 0.98"; then 12px red "Same signal." / "One has a name."

## Section 2: Why It Works (Socially)

- **Nobody will challenge it** — questioning a formula risks looking uninformed. Easier to nod along.
- **It's technically correct** — the formula does compute what it says. You can't call it "wrong."
- **Names create ownership** — "our RWAI model" becomes a team artifact. It gets meetings, documentation, a roadmap.
- **Presentation rewards novelty** — "we use click-through rate" doesn't get invited to the all-hands. "We developed a User Engagement Score" does.

**Why it's harmful:**

- New team members can't understand "the model" without a decoder ring — when the answer is "it's just CTR"
- Blocks simpler alternatives — "we already have UES" prevents someone from pointing out it's a default metric
- Nobody can tell if it's failing because nobody knows what it actually measures
- Culture shifts toward "fancier name = better work"

### Visualization (canvas `c2`, 720×340)

Horizontal bar "ladder" chart: same metric with escalating packaging vs perceived impressiveness.

- **Title (bold 13px `#1a5276`, top center):** "Same Metric — Escalating Packaging".
- **Bars:** start x=180, max width 480, height 38, vertical gap 52 starting at y=42. Each bar: fill = level color + `22` alpha suffix, stroke level color width 2; bold 11px `#222` right-aligned label left of the bar; 11px level-color text inside the bar.
  - "What it is" — '"people who click more, buy more"' — width 8%, `#27ae60`
  - "Spreadsheet" — "clicks / views" — width 15%, `#2ecc71`
  - "Renamed" — '"User Engagement Score"' — width 40%, `#e67e22`
  - "Formalized" — "UES(u) = Σ clicks / Σ views" — width 65%, `#e74c3c`
  - "Sold internally" — '"our proprietary engagement model"' — width 92%, `#c0392b`
- **X-axis label (11px `#555`, centered below bars):** "← less packaging                  Perceived Impressiveness                  more packaging →".
- **Constant-information marker:** vertical dashed (4/3) `#1a5276` line width 2.5 at 8% of bar width spanning from the first to the last bar; bold 11px `#1a5276` labels to its right: "← actual information content" / "   (identical at every level)".

## Section 3: How to Spot a Costume

**The test:** Ask "what does this look like without any math?" If the plain-English version makes everyone say "oh, that's just [familiar thing]" — it was a costume, not a contribution.

**Real innovation** changes what you can DO — it handles edge cases, produces different rankings, works where the simple version fails.

**Costume innovation** changes what you CALL it — same rankings, same decisions, same top-10 list, different notation.

**Quick check:** Compute the correlation between the fancy metric and the simple one. If r > 0.95 — they're the same signal wearing different clothes.

**In reviews, ask:** "What does this beat that CTR doesn't?" If there's no measurable improvement — strip the costume off. Use the simple version. Call it what it is.

### Visualization (canvas `c3`, 720×340)

Exponential decay curve: probability anyone challenges the formula vs formality of presentation.

- **Title (bold 13px `#1a5276`, top center):** "Will Anyone Challenge It?".
- **Margins:** top 48, right 40, bottom 48, left 80; L-shaped `#333` axes width 1.5.
- **Y label (rotated, 11px `#444`):** 'Chance someone asks "isn\'t this just...?"'; y ticks (10px `#888`): "High", "Medium", "Low" at top/middle/bottom with `#f0f0f0` gridlines.
- **X label (11px `#444`):** "How formal the presentation looks".
- **Curve:** red `#e74c3c` width 3; y = exp(−3.2·x) for x in 0–1 mapped onto the plot (high at left, decaying to near zero at right).
- **Zone annotations (5px filled dot + bold 11px title + 11px `#555` subtitle):**
  - x=0.07, y=0.82, `#27ae60`: "Whiteboard chat:" / '"that\'s just clicks/views"'
  - x=0.40, y=0.28, `#e67e22`: "Design doc:" / '"I think this is reducible..."'
  - x=0.75, y=0.06, `#e74c3c`: "All-hands presentation:" / "(nobody speaks up)"
- **Caption (bold 11px `#1a5276`, bottom center):** "The metric doesn't improve. Questioning it just becomes socially risky."

## Section 4: Real Innovation vs. Costume

- **Real example:** TF-IDF vs raw word count. TF-IDF adjusts for document length and word rarity. It ranks documents DIFFERENTLY than raw count. You get better search results. That's real value.
- **Costume example:** "Content Resonance Factor" vs (likes + shares). Same top posts. Same bottom posts. Same decisions. Just a fancier name.
- **The test:** Run both metrics. Sort your items by each. If the top 20 list is the same — the formula added nothing. If the lists diverge — the formula might be genuinely useful.

### Visualization (canvas `c4`, 720×300)

Side-by-side rank-comparison diagram: costume (same order) vs real (different order).

- **Title (bold 13px `#1a5276`, top center):** "Does the Fancy Version Rank Things Differently?".
- **Left panel (x=50, width 280):** heading bold 12px red "✗ Costume: same order"; column headers 10px `#555` "by CTR" and 'by "UES"'. Five rows (row height 40): left boxes (85×26, fill `rgba(26,82,118,0.08)`) and right boxes (fill `rgba(231,76,60,0.08)`) both listing "#1 Page A" … "#5 Page E" in identical order (11px `#222`); straight horizontal red `#e74c3c` connector lines (width 1.2) between each pair. Footer bold 10px red: "Same list → no value added by the formula".
- **Right panel (x=390, width 280):** heading bold 12px green "✓ Real: different order"; column headers "by word count" and "by TF-IDF". Left column boxes (fill `rgba(26,82,118,0.08)`): "#1 Doc A" … "#5 Doc E" in order A, B, C, D, E; right column boxes (fill `rgba(39,174,96,0.08)`): order C, A, E, D, B. Crossing blue `#2980b9` connector lines (width 1.2) map each doc's old rank to its new rank. Footer bold 10px green: "Different list → different decisions → genuine improvement".

## Regeneration instructions

- **Layout:** four separate `.obj-table` tables, each containing a single two-column `<tr>`: left `<td>` (40%) with `.obj-title` + paragraphs/bullets (Section 1 also uses `.formula` blocks, `.arrow` lines, and `.plain` blocks), right `<td>` (60%, centered) with one canvas.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#1a1a1a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#444` 1.0em; `p` `#222` 0.95em; `ul` 0.9em `#222`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. Note this page uses slightly darker body text (`#1a1a1a`/`#222`/`#444`) than sibling pages.
- **Special classes:** `.formula` — 'Courier New' monospace, background `#f4f4f4`, padding 8px 14px, radius 4px, inline-block, border `1px solid #ddd`, color `#111`, 0.95em. `.plain` — background `#e8f8e8`, padding 6px 12px, radius 4px, inline-block, border `1px solid #c3e6c3`, color `#1a5a1a`, 0.93em. `.arrow` — display block, centered, color `#e67e22`, 1.1em.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276` (scatter fill `rgba(26,82,118,0.5)`, box fill `rgba(26,82,118,0.08)`), green `#27ae60`/`#2ecc71`, red `#e74c3c`/`#c0392b`, orange `#e67e22`, link blue `#2980b9`, gray text `#555`/`#444`.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
