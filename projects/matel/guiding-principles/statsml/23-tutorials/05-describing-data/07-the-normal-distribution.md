# The Normal Distribution

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** The Normal Distribution

**Subtitle:** The bell curve — why so many measurements pile up around a middle and taper off evenly on both sides

## Measure 1,000 Women, Get a Bell

**Tags:** `bell curve` (blue), `core idea` (green)

- **The setup** — measure the heights of 1,000 adult women and tally them in 3 cm bins
- **The pile-up** — 334 of them land in the two middle bins around 165 cm
- **The taper** — each step away from the middle holds fewer women, evenly on both sides
- **Symmetry** — 155 cm is about as common as 175 cm; mean and median both sit at 165 cm
- **Why a bell** — height is many small pushes (genes, diet) added up; sums of small effects bell out

*Example:* Only 15 of the 1,000 women are shorter than 150 cm — and only 15 are taller than 180 cm.

**Key point:** A normal distribution is fully described by two numbers: where the middle sits (mean 165 cm) and how wide the spread is (standard deviation 7 cm).

### Visualization (canvas `c1`, 720×300)

Histogram of 1,000 heights in 3 cm bins with mean marker.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Women's Heights, Tallied in 3 cm Bins (illustrative)".
- **Data:** 14 bins with edges 144, 147, 150, 153, 156, 159, 162, 165, 168, 171, 174, 177, 180, 183, 186 and counts `[5, 10, 27, 56, 96, 139, 167, 167, 139, 96, 56, 27, 10, 5]` (sum 1,000).
- **Axes:** L-shaped axis (`#999`), padding top 48 / bottom 46 / left 55 / right 20; y count scale 0–180 with labels 0, 50, 100, 150 and light gridlines `#e5e9ef`; x labeled with every other bin edge (144, 150, 156, 162, 168, 174, 180); axis title "height (cm)".
- **Bars:** the two middle bins (counts 167) solid blue `#2a78d6`; all other bars `rgba(42,120,214,0.45)`; count labels above bars with count ≥ 27 (12px `#2c3e50`).
- **Mean marker:** vertical dashed magenta line (`#d55181`, width 2, dash 6/4) at the 165 cm boundary; labels to the right: bold 13px magenta "mean = median = 165 cm" and bold 13px orange "334 of 1,000 in the middle two bins".

## The 68–95–99.7 Rule, Checked by Hand

**Tags:** `worked example` (green), `68-95-99.7` (blue)

- **The two numbers** — mean 165 cm, standard deviation (sd) 7 cm; that is all you need
- **One sd** — 165 ± 7 gives 158–172 cm; about 68% of women (≈680 of 1,000) land here
- **Two sd** — 165 ± 14 gives 151–179 cm; about 95% (≈950 of 1,000) land here
- **Three sd** — 165 ± 21 gives 144–186 cm; about 99.7% (≈997 of 1,000) land here
- **Check it** — each band is just mean minus k×sd to mean plus k×sd; no other math needed

*Example:* Outside 151–179 cm you expect only ~50 women in 1,000 — about 25 on each side.

**Rule of thumb:** 68% within 1 sd, 95% within 2 sd, 99.7% within 3 sd — the same three percentages for every normal curve, whatever the units.

### Visualization (canvas `c2`, 720×300)

Smooth bell curve with 68 / 95 / 99.7 shaded bands.

- **Title (bold 15px, `#1a5276`, top center):** "Mean 165, SD 7: the Same Three Bands Every Time".
- **Curve:** standard normal shape `exp(-z²/2)` plotted from z = −3.5 to +3.5, stroke ink `#1a5276` width 3; x axis maps z to heights 140.5–189.5.
- **Shaded bands under the curve, widest first:** |z|≤3 fill `rgba(217,89,38,0.15)` (orange), |z|≤2 fill `rgba(0,131,0,0.18)` (green), |z|≤1 fill `rgba(42,120,214,0.28)` (blue).
- **Axis:** baseline (`#999`) with labels at each sd step: 144, 151, 158, 165, 172, 179, 186 (12px `#6b7280`); axis title "height (cm)".
- **Band labels (bold 13px, centered at the mean):** blue "68% inside 158–172" (upper), green "95% inside 151–179" (lower), orange "99.7% inside 144–186" (below the axis).
- **Annotation (bold 12px magenta, upper right at z≈1.45):** "~950 of 1,000 women in the green band".

## Where You Meet It: Z-Scores and "Is This Weird?"

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Z-score** — (value − mean) ÷ sd; a 179 cm woman is (179 − 165) ÷ 7 = 2 sd above
- **Rarity for free** — z = 2 means taller than ~97.7% of women, without any new survey
- **Common ground** — averages, error terms, and measurement noise all tend toward bells
- **Anomaly cutoffs** — "flag anything past 3 sd" works because 3 sd is a 1-in-370 event
- **Comparisons** — z-scores let you compare a height to a test score to a load time on one scale

*Example:* A 186 cm woman is z = 3 — roughly 1 in 740 women is that tall or taller.

**Key point:** The bell curve turns "how unusual is this value?" into one subtraction and one division — that is why z-scores are everywhere in data work.

### Visualization (canvas `c3`, 720×300)

Bell curve with z-score markers at z=2 and z=3; area below z=2 shaded.

- **Title (bold 15px, `#1a5276`, top center):** "Z-Score: How Many SDs from the Mean Am I?".
- **Curve:** same bell `exp(-z²/2)` from z = −3.5 to +3.5, ink `#1a5276` width 3; area under the curve left of z=2 filled `rgba(42,120,214,0.18)`.
- **Axis:** baseline (`#999`) with dual labels at z = −3…+3: height value (144…186) and "z=−3"…"z=3" beneath (12px `#6b7280`); axis title "height (cm) and its z-score".
- **Markers** (vertical dashed lines, width 2, dash 6/4, two-line bold 12px label to the right):
  - green `#008300` at z=2: "179 cm → z = 2" / "taller than ~97.7%"
  - orange `#d95926` at z=3 (lower start): "186 → z = 3" / "1 in 740"
- **Annotation (left side, centered at z≈−2.1):** bold 13px magenta "z = (179 − 165) / 7 = 2" with 12px muted line below: "one subtraction, one division".

## The Mistake: Assuming Everything Is a Bell

**Tags:** `common mistake` (red), `skew` (orange)

- **Heights, yes** — bounded, symmetric, driven by many small added effects: bell-shaped
- **Incomes, no** — long right tail; the mean sits well above the median, unlike any bell
- **Broken rule** — on skewed data "mean ± 2 sd" can dip below zero: an impossible income
- **Look first** — plot the histogram before using z-scores or sd-based cutoffs
- **Quick test** — if mean and median disagree badly, stop treating the data as normal

*Example:* Applying the 68–95–99.7 rule to incomes "predicts" households earning less than nothing.

**Common mistake:** The rule of thumb 68–95–99.7 is a property of the bell shape, not of data in general — check the shape before you borrow the percentages.

### Visualization (canvas `c4`, 720×300)

Two-panel comparison (dashed divider at x=360): symmetric bell (heights) vs right-skewed curve (incomes).

- **Title (bold 15px, `#1a5276`, top center):** "Heights Are a Bell. Incomes Are Not.".
- **Divider:** dashed light-gray vertical line `#bdc3c7` (dash 4/3).
- **Left panel (x=35, width 295, baseline y=235, height 150):** bell curve `exp(-z²/2)` from z = −3.2 to 3.2, stroke blue `#2a78d6` width 3, fill under `rgba(42,120,214,0.14)`. Labels: bold 13px blue "heights: symmetric" (top); bold 12px green "mean = median — rule of thumb works" (below baseline); 12px muted "height (cm)".
- **Right panel (x=390, width 295):** right-skewed curve `g(t) = t^1.4 · exp(−2.2t)` for t in 0–4.5, normalized to peak, stroke orange `#d95926` width 3, fill `rgba(217,89,38,0.14)`.
  - **Markers (dashed, width 2, dash 5/4, illustrative positions):** green median line at t≈0.85 labeled bold 12px "median"; magenta mean line at t≈1.25 labeled "mean, dragged right".
  - Labels: bold 13px orange "incomes: long right tail" (top); bold 12px red `#e74c3c` "mean ± 2 sd dips below $0 — nonsense band" (below baseline); 12px muted "income (illustrative)".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each starting with `<b>` term in `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, radius 4px; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; a shared `bell(z) = exp(-z²/2)` helper drives the curve charts. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
