# Sample Sufficiency

**Page type:** detail page (TOC box + numbered h2 sections, each an obj-table with concept row + example row: text left 45%, canvas right 55%)
**HTML title tag:** Sample Sufficiency

**Subtitle:** Before claiming "this bucket is 90% positive," verify you have enough data to make that claim reliable.

## Table of Contents

1. Small Samples Lie (#problem)
2. Minimum Sample Requirements (#minimums)
3. The Base Rate Trap (#baserate)
4. Confidence Intervals (#ci)
5. Requirements by Shape (#shape)
6. When You Don't Have Enough (#insufficient)

## 1. Small Samples Lie

**Same Ratio, Completely Different Confidence**

- 7 pos, 1 neg → "87.5% purity" — p=3.5% by chance. Borderline.
- 70 pos, 10 neg → "87.5% purity" — p=10⁻¹². Rock solid.
- Same ratio. First is noise, second is signal.

**Rule:** Never trust purity from fewer than 30 data points.

### Visualization (canvas `c1a`, 720×280)

Two stacked proportion bars comparing n=8 vs n=80 at the same ratio.

- **Bars:** 400px wide, 40px tall, at x=60, y=20 and y=95. Each split green/red by pos fraction: pos segment `rgba(39,174,96,0.5)`, neg segment `rgba(231,76,60,0.5)`, 1px `#999` outline.
  - Bar 1: "n=8: 7 pos / 1 neg" (bold 17px `#333` label above), verdict text at x=480 in red `#e74c3c`: "p=0.035 borderline".
  - Bar 2: "n=80: 70 pos / 10 neg", verdict in green `#27ae60`: "p=10⁻¹² definitive".

## Example: Cholesterol [280-400 mg/dL]

- Pilot study (n=8): 7 heart disease, 1 healthy → "87% pos!"
- Full data (n=80): 56 disease, 24 healthy → actually 70%
- The pilot was a fluke — 8 samples can't estimate a ratio

### Visualization (canvas `c1b`, 720×280)

Small-box-to-big-box comparison with arrow.

- **Title (bold 17px `#1a5276`, left):** "Cholesterol [280-400]: Pilot Fluke vs Full Data".
- **Pilot box:** 70×50px red rectangle `rgba(231,76,60,0.4)` at (60, 45); centered 17px `#333` captions below: "Pilot n=8" / "\"87% pos\"".
- **Arrow:** dark 2px line with triangle head from x=145 to x=187 at y=70.
- **Full-data box:** 250×50px green rectangle `rgba(39,174,96,0.4)` at (200, 45); captions: "Full n=80" / "Actually 70% (CI [59%,79%])".
- **Verdict (bold 17px `#e74c3c`, bottom center):** "Pilot was a fluke! 8 samples ≠ reliable estimate".

## 2. Minimum Sample Requirements

**Per-Bucket Minimums**

- **Purity >90%:** n ≥ 50, minority ≥ 5
- **Purity >95%:** n ≥ 100, minority ≥ 5
- **Differs from base rate:** n ≥ 30, each class ≥ 5
- **Feature separates:** ≥ 30 per class total

### Visualization (canvas `c2a`, 720×280)

Four colored rounded requirement rows (fill at 10% alpha of row color, 1.5px stroke, radius 4, 32px tall, 40px pitch starting y=15; claim in 17px `#333` at x=60, bold colored n requirement centered at x=550):

1. "Purity >90%" → "n≥50" — `#2980b9`
2. "Purity >95%" → "n≥100" — `#8e44ad`
3. "Differs from base" → "n≥30" — `#27ae60`
4. "Feature separates" → "≥30/class" — `#e67e22`

## Example: Capital Gain (Adult Census)

- [$15k+]: n=289, 248 pos → 86%, CI [81%, 90%] ✓ solid
- [$7k-$15k]: n=28, 22 pos → 79%, CI [59%, 92%] ✗ too wide
- Same-ish ratio but second is untrustworthy

### Visualization (canvas `c2b`, 720×280)

Two horizontal CI interval bars (4px thick lines with 4px endpoint dots and a 5px point-estimate dot; x scale maps proportion 0.40→x=200 to 1.05→x=650, i.e. `x = 200 + (p-0.4)/0.65*450`).

- **Title (bold 17px `#1a5276`, left):** "Capital Gain: n=289 (solid) vs n=28 (too few)".
- **Bar 1 (green `#27ae60`, y=55):** label "[$15k+] n=289", CI 0.81–0.90, point 0.86.
- **Bar 2 (red `#e74c3c`, y=110):** label "[$7k-15k] n=28", CI 0.59–0.92, point 0.79.
- **X-axis labels (17px `#999`, bottom):** "60%" at x=200, "70%" at x=320, "80%" at x=440, "90%" at x=560.

## 3. The Base Rate Trap

**Compare to Base Rate, Not 50%**

- Enrichment = bucket_rate / overall_rate
- \> 2.0× = interesting. > 5.0× = strong. ≈ 1.0 = noise.
- "35% pos" looks good but if base is 24% → only 1.46×

### Visualization (canvas `c3a`, 720×280)

Enrichment scale with colored dots on a 0–6× axis (left margin 80, right margin 40; dot x = margin + (enrichment/6)·plot width, 8px radius; label to the right in 17px `#333`):

- **Title (bold 17px `#1a5276`, left):** "Enrichment = Bucket Rate / Base Rate".
- Dots (staggered vertically at y = 60, 88, 116, 144):
  - "0.3× → strong neg" — `#2980b9`
  - "1.0× → no signal" — `#bbb`
  - "2.0× → interesting" — `#e67e22`
  - "5.0× → strong!" — `#27ae60`
- **Base rate marker:** vertical dashed red line (`#e74c3c`, dash 4/3) at the 1.0× position, with centered red label "base rate" at the bottom.

## Example: Adult Census (base rate 24%)

- Age [17-25]: 10% pos → enrichment 0.42× (depleted)
- Age [35-50]: 35% pos → enrichment 1.46× (mild, not actionable)
- Education≥13 + Age>50: 72% pos → 3.0× (strong!)
- Hours [1-25]: 8% pos → 0.33× (strong negative)

### Visualization (canvas `c3b`, 720×280)

Horizontal bar chart of enrichment by bucket (bars start at x=250, width = (enr/3.5)·plot width·0.6, 24px tall, 40% alpha fill; right-aligned 17px `#333` label at x=240; bold colored value label after each bar):

- **Title (bold 17px `#1a5276`, left):** "Adult Census: Enrichment by Feature (base=24%)".
- Bars (32px pitch starting y=45):
  1. "Age [17-25]" — 0.42× — `#2980b9`
  2. "Age [35-50]" — 1.46× — `#e67e22`
  3. "Edu≥13 + Age>50" — 3.00× — `#27ae60`
  4. "Hours [1-25]" — 0.33× — `#2980b9`
- **Base rate marker:** vertical dashed red line (`#e74c3c`, dash 4/3) at the 1.0× position.

## 4. Confidence Intervals

**Report a Range, Not a Point Estimate**

- CI width shrinks with √n — need 4× data to halve width
- Trust only if CI lower bound > base rate
- n=10: CI [55%-97%] useless. n=100: CI [77%-91%] good.

### Visualization (canvas `c4a`, 720×280)

Four horizontal CI bars for the same 85% purity at growing n (4px lines with 3px endpoint dots; x scale `x = 180 + (p-0.4)/0.65*440`; right-aligned "n=" labels at x=165; 33px pitch starting y=45):

- **Title (bold 17px `#1a5276`, left):** "85% Purity — CI Width Shrinks with n".
- n=10: CI 0.55–0.97 — red `#e74c3c`
- n=30: CI 0.68–0.95 — orange `#e67e22`
- n=100: CI 0.77–0.91 — green `#27ae60`
- n=500: CI 0.82–0.88 — green `#27ae60`
- **True value line:** vertical 2px line `rgba(30,132,73,0.3)` at p=0.85.

## Example: SalePrice [$300k+] (Ames)

- n=88: 75 high-quality, 13 not → 85%, CI [76%, 92%]
- CI lower (76%) well above base rate (55%) → trustworthy
- If only n=12: same 85% but CI [55%, 97%] → useless

### Visualization (canvas `c4b`, 720×280)

Two CI bars with annotations (5px lines; x scale `x = 180 + (p-0.4)/0.65*440`):

- **Title (bold 17px `#1a5276`, left):** "SalePrice [$300k+]: n=88 vs hypothetical n=12".
- **n=88 (green `#27ae60`, y=65):** CI 0.76–0.92; right annotation "CI [76%, 92%] ✓ above base rate (55%)".
- **n=12 (red `#e74c3c`, y=110):** CI 0.55–0.97; right annotation "CI [55%, 97%] ✗ lower bound = base rate".
- **Base rate marker:** vertical dashed gray line (`#999`, dash 3/3) at p=0.55 spanning y=50–130, centered gray label "base=55%" below.

## 5. Requirements by Shape

**More Complex Shape = More Data Needed (Usually)**

- **Bell:** 30 per class (t-test, CLT)
- **Skewed:** 20 per class (Mann-Whitney)
- **Spike:** expected ≥ 5 per cell (chi-squared)
- **Bimodal:** 20 per peak per class = 80+ total
- **Tail:** 20 in tail (tails are small by definition)

**The inverse is also true:** If you get tight CIs with small n, the shape is very well-defined. A feature where n=40 gives CI [82%, 94%] has such clean separation that it doesn't need more data — the signal is strong and low-variance. Small-n + tight CI = robust shape, not insufficient data.

### Visualization (canvas `c5a`, 720×280)

Horizontal bar chart of minimum n by shape (bars start at x=180, width = (n/100)·350, 30px tall, 30% alpha fill with 1px stroke; bar color `#8e44ad` when n≥50 else `#2980b9`; shape label in 17px `#333` at x=40, bold colored "n≥" value after the bar, gray test name at x=550; 40px pitch starting y=15):

- "Bell" — n≥30 — "t-test"
- "Skewed" — n≥20 — "Mann-Whitney"
- "Spike" — n≥20 — "chi-squared"
- "Bimodal" — n≥80 — "per-peak"

## Example: Hemoglobin (Bimodal — Anemia)

- Left peak [7-11]: need ≥20 pos AND ≥20 neg here alone
- Right peak [12-17]: same requirement independently
- n=50 total → ~25 per peak → marginal
- n=200 total → ~100 per peak → solid

### Visualization (canvas `c5b`, 720×280)

Bimodal histogram with per-peak requirement labels.

- **Title (bold 17px `#1a5276`, left):** "Hemoglobin (Bimodal): Need 20 per peak per class = 80+ total".
- **Bins (15):** `[3, 8, 18, 30, 25, 12, 5, 4, 5, 12, 25, 30, 18, 8, 3]`, scale max 32; margins: left 40, right 30, top 40, bottom 40.
- **Bar colors:** bins 0-6 (left peak) `rgba(39,174,96,0.4)`, bins 7+ (right peak) `rgba(41,128,185,0.4)`.
- **Labels (17px, centered under each peak):** green `#27ae60` "Peak 1: need ≥20 pos + ≥20 neg"; blue `#2980b9` "Peak 2: need ≥20 pos + ≥20 neg".

## 6. When You Don't Have Enough

**Options for Insufficient Data**

- **Widen:** Merge adjacent bins until n ≥ 30
- **Inconclusive:** "Don't know" is honest
- **Weak evidence:** Suggestive, combine with others
- **Don't fake:** No claims from n=10

### Visualization (canvas `c6a`, 720×280)

Four colored rounded strategy rows (fill at 8% alpha, 1.5px stroke, radius 4, 32px tall, 40px pitch starting y=10; bold colored icon at x=60, 17px `#333` label at x=90):

1. "⊕" "Widen bucket (merge)" — `#27ae60`
2. "?" "Mark inconclusive" — `#2980b9`
3. "~" "Weak evidence only" — `#e67e22`
4. "✗" "DON'T fake it" — `#e74c3c`

## Example: Lot Area [>50k sqft] (Ames)

- Only 60 homes above 50k sqft
- Merge [50k-100k] + [100k-215k] → n=60 → meets min
- Result: 48 above-median price → 80%, CI [68%, 89%] ✓
- Keeping [100k-215k] alone: n=8, CI [47%, 99%] → garbage

### Visualization (canvas `c6b`, 720×280)

Before/after bin-merge diagram.

- **Title (bold 17px `#1a5276`, left):** "Lot Area [>50k sqft]: Merge Bins to Get Sufficient n".
- **Before:** two 100×50px red boxes `rgba(231,76,60,0.3)` with 1px `#e74c3c` outline at (60,50) and (180,50); centered captions "[50k-100k]" / "n=52" and "[100k-215k]" / "n=8 ✗".
- **Arrow:** dark 2px arrow from x=310 to x=362 at y=75, labeled "merge" above.
- **After:** one 200×50px green box `rgba(39,174,96,0.3)` with 2px `#27ae60` outline at (380,50); caption "[50k-215k]" plus bold green line "n=60 → 80%, CI [68%,89%] ✓".

## Callout (philosophy box)

**The principle:** Statistical claims require statistical evidence. A purity of 90% from 8 samples is a guess. From 80 samples it's a measurement. The pipeline refuses to promote guesses to decisions.

## Regeneration instructions

- **Layout:** single long page. h1, `.subtitle`, a `.toc` box (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, bold "Table of Contents" heading + ordered anchor list `#problem`, `#minimums`, `#baserate`, `#ci`, `#shape`, `#insufficient`), then six numbered h2 sections. Each section is one `.obj-table` with TWO rows: row 1 = concept (`.obj-title` + bullets left, canvas right), row 2 = example (`.obj-title` "Example: …" + bullets left, canvas right). Left `<td>` 45%, right `<td>` 55% centered; even rows background `#fafcfe`. Page ends with a `.philosophy` callout.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`; subtitle `#666` 1.05em; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** all canvases 720×280; intrinsic `width`/`height` attributes, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart text uses 17px -apple-system. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, neutral `#bbb`, text grays `#666`/`#333`/`#999`.
- In regenerated HTML, any card/page links use `.html` extensions.
