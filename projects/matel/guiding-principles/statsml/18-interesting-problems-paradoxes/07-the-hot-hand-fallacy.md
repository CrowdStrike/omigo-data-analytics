# The Hot Hand Fallacy

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** The Hot Hand Fallacy

**Subtitle:** Everyone 'knows' basketball players don't get hot. For 30 years, the proof was a test that was mathematically rigged against finding streaks — even if they existed.

## Callout (philosophy box)

**Why this is fascinating:** The debunking was itself debunked. Not because of new data, but because a Princeton mathematician found a bias in the original test statistic. The error went unnoticed for 30 years across one of the most-cited papers in behavioral economics. This is not about whether streaks exist — it's about how a flawed null distribution can systematically hide real effects.

## Section: What GVT Claimed (1985)

**Obj-title:** The Famous Debunking

Gilovich, Vallone, and Tversky studied Philadelphia 76ers players shooting over multiple games. They measured:

- **P(Hit | previous shot was Hit)** ≈ 51%
- **P(Hit | previous shot was Miss)** ≈ 50%

No significant difference. Conclusion: the "hot hand" is a cognitive illusion. Fans and players believe in streaks, but the data shows no evidence that making a shot increases the probability of making the next one.

This became one of the foundational examples of human irrationality in behavioral economics textbooks.

### Visualization (canvas `gvtChart`, 720×380)

Two-bar comparison of conditional hit rates.

- **Title (bold 15px `#1a5276`, centered):** "GVT's Conclusion: No Difference"; subtitle line (12px `#666`): "(comparing against 50% baseline)".
- **Bars:** 100px wide, 80px gap, centered horizontally; y scale 0–65% over chart height (canvas height − 100), baseline 50px above bottom.
  - "Hit after Hit": value 0.51, color `#1a5276` (fill is the color at 35% alpha — hex suffix `59`; 2px solid border in the color).
  - "Hit after Miss": value 0.50, color `#e67e22` (same fill/border scheme).
- **Value labels:** bold 16px `#1a5276` percentage ("51%", "50%") above each bar.
- **X labels:** 13px `#333`, each label split into one word per line ("Hit"/"after"/"Hit" etc.) below the bar.
- **Y-axis:** thin `#999` vertical line left of the bars; labels 11px `#666` right-aligned at 0/13/26/39/52/65% (i.e. 6 ticks at fifths of 65%).

## Section: The Hidden Bias (Miller & Sanjurjo, 2018)

**Obj-title:** The Subtle Selection Bias

Here's the problem. Consider a simple example: flip a fair coin 3 times. Now ask:

**"Among all flips that followed Heads, what proportion were also Heads?"**

Intuition says 50% — after all, each flip is independent. But this is wrong.

By conditioning on "previous flip was Heads," you create a selection bias. Sequences with more Heads early are MORE LIKELY to contribute to your sample. But these same sequences are LESS LIKELY to have Heads in the remaining positions (regression to the mean within finite sequences).

Math box:

**All 8 sequences of 3 coin flips:**

`HHH` `HHT` `HTH` `HTT`
`THH` `THT` `TTH` `TTT`

**Per-sequence proportion of H-after-H** (TTH and TTT are excluded — no flip follows an H):

`HHH` → 2/2 = 1
`HHT` → 1/2 = ½
`HTH` → 0/1 = 0
`HTT` → 0/1 = 0
`THH` → 1/1 = 1
`THT` → 0/1 = 0

**Expected proportion:** `(1 + ½ + 0 + 0 + 1 + 0) / 6 = 5/12 ≈ 0.417`

(red, weight 600:) NOT 0.50 as GVT assumed!

Closing paragraph: GVT computed this proportion separately for each player — one sequence each — so the relevant null is the average of per-sequence proportions, and that average is biased below the unconditional probability. GVT compared their observed ~51% to an expected 50%, when they should have compared it to ~45-48% (depending on sequence length).

### Visualization (canvas `biasChart`, 720×400)

Diagram of the 8 three-flip sequences with per-sequence proportions and the bias calculation.

- **Title (bold 15px `#1a5276`, centered):** "The Selection Bias in 3-Flip Sequences".
- **Explanation line (12px `#555`, left-aligned):** "Looking at only flips that follow H creates a downward bias:".
- **Sequence boxes (8 boxes, 75px wide × 100px tall, in a row at y=90, x from 30 to 625 in 85px steps):**
  - Included sequences (HHH, HHT, HTH, HTT, THH, THT): background `#f0f4f8` (HHH gets `#e8f8f5`), border 2px `#2980b9` (HHH gets `#27ae60`); bold 14px monospace sequence label in `#1a5276`; 11px lines "H-after-H: n/t" and "prop = p" — HHH: 2/2, prop 1; HHT: 1/2, prop 1/2; HTH: 0/1, prop 0; HTT: 0/1, prop 0; THH: 1/1, prop 1; THT: 0/1, prop 0.
  - Excluded sequences (TTH, TTT): background `#e0e0e0`, border 1px `#ccc`, gray `#999` sequence label, 11px two-line note "(no flip" / "after H)".
- **Calculation box (background `#f8fafb`, 1px `#d0d0d0` border, at y=220, full row width):** 13px `#333` lines "Per-sequence proportions: 1, 1/2, 0, 0, 1, 0   (TTH, TTT excluded)" and "Average over the 6 sequences: (1 + 1/2 + 0 + 0 + 1 + 0) / 6 = 5/12"; bold 14px `#1a5276` "Expected proportion: 5/12 ≈ 0.417"; 13px `#e74c3c` "Below 0.50 — the per-player statistic is biased against streaks".
- **Footer strip (background `#f0f0f0`, 11px `#666`, centered):** "Pooling all flips gives 4/8 = 0.50 (unbiased); averaging per player, as GVT did, gives 5/12 ≈ 0.417."

## Section: The Corrected Picture

**Obj-title:** The Real Hot Hand Effect

After applying the bias correction discovered by Miller & Sanjurjo:

- **Expected hit-after-hit rate** (bias-corrected null): ~45-48%
- **Observed hit-after-hit rate** (original data): ~51%
- **Hot hand effect size**: ~4 percentage points

The effect is small but statistically significant. Players do shoot slightly better after making a shot. It's not a large effect — certainly not as dramatic as fans perceive — but it's real.

The original GVT study didn't fail to find the hot hand because it was too small. It failed because the test statistic was biased AGAINST finding it.

### Visualization (canvas `correctedChart`, 720×380)

Two-bar comparison of the bias-corrected null vs the observed rate, with an effect-size arrow.

- **Title (bold 15px `#1a5276`, centered):** "After Bias Correction: Small but Real Effect"; subtitle line (12px `#27ae60`): "(statistically significant)".
- **Bars:** 110px wide, 100px gap, centered; y scale 0–60%, baseline 50px above bottom.
  - "Expected (null) / with bias correction": value 0.47, color `#7f8c8d` (fill at 35% alpha — hex suffix `59`; 2px border).
  - "Observed / hit-after-hit rate": value 0.51, color `#27ae60` (same scheme).
- **Value labels:** bold 17px in bar color ("47%", "51%") above each bar; two-line 12px `#333` x labels below.
- **Y-axis:** `#999` line left of bars; 11px `#666` labels at 0/12/24/36/48/60%.
- **Effect arrow:** dashed red (`#e74c3c`, dash 5/5, width 2) horizontal line at the 49% level between the two bars, with a red arrowhead pointing right; bold 13px `#e74c3c` label above it: "~4pp effect".

## Closing callout (philosophy box)

**The statistical lesson:** A biased test statistic doesn't just miss small effects. It actively provides evidence AGAINST the truth. For 30 years, the hot hand was considered one of the strongest examples of human irrationality. It was actually one of the strongest examples of how subtle sampling bias can hide in plain sight. The bias arises from selecting on past outcomes in finite sequences — something that appears innocuous but creates a systematic downward pull on conditional probabilities. This is a reminder to always verify that your null distribution actually represents the null hypothesis you think you're testing.

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle`, opening `.philosophy` callout, three `h2` sections (unnumbered) each holding a `.obj-table` (one `<tr>`: left `<td>` 45% with `.obj-title` + paragraphs/bullets/`.math-box`, right `<td>` 55% centered canvas), closing `.philosophy` callout.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; p 0.95em `#333`; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; obj-table cells `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links.
- **Component styles:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em. `.sequence` — inline-block Courier New monospace pill, background `#e8f4f8`, padding 4px 8px, radius 3px, 0.88em; `.highlight` variant adds background `#ffeaa7`. The per-sequence proportion list in the math box uses `.sequence.highlight` pills.
- **Canvases:** `gvtChart` 720×380, `biasChart` 720×400, `correctedChart` 720×380 (declared via width/height attributes). The `setupCanvas(canvas)` helper reads `getBoundingClientRect`, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray `#7f8c8d`, text grays `#666`/`#555`/`#333`. Bar fills are the series color at 35% alpha (append `59` to the hex).
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
