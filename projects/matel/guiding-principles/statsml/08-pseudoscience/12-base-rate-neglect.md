# Base Rate Neglect

**Page type:** detail page (single-row obj-table layout: text left ~40%, two stacked canvases right ~60%)
**HTML title tag:** Base Rate Neglect — Pseudoscience in Data Analysis

**Subtitle:** Ignoring how rare or common something is before interpreting a test

## Section: Ignoring How Rare/Common Something Is Before Interpreting a Test

- **Medical screening:** "Test is 99% accurate! Patient tested positive!" But if the disease affects 1 in 10,000 people, ~99% of positives are healthy people — the "99% accurate" feels definitive while the base rate makes it nearly meaningless.
- **Fraud detection:** "Our model catches 95% of fraud!" But fraud is 0.1% of transactions, so a 5% false positive rate on the 99.9% legitimate ones produces ~53× more false alarms than real catches — only 1.9% of flagged transactions are fraud.
- **Hiring:** "Our AI screening tool identifies top performers with 80% accuracy!" But only 5% of applicants are top performers, so a 20% false positive rate on the other 95% dominates — 83% of the candidates you advance are average, and you still drop 1 in 5 top performers.
- **Security alerts:** "This threat detector has only 1% false positive rate!" With 10 million events per day, that is 100,000 false alerts daily — the base rate of non-threats turns a good-sounding number into a noise cannon.

**Why it's pseudoscience:** Reporting test accuracy without the base rate is meaningless — P(disease | positive) depends on P(disease), and Bayes' theorem is not optional; a "99% accurate" test on a rare event is almost always wrong when it fires.

### Visualization (canvas `c1`, 720×340)

Horizontal stacked-bar breakdown of 10,000 tested people, showing false positives dwarfing the single true positive. Every count and percentage is computed in JS at render time from the 2×2 table (`N = 10000`, `base = 1/10000`, `sens = 0.99`, `spec = 0.99`) — nothing is hardcoded.

Derived table (rounded to whole people): sick = 1, healthy = 9,999, TP = 1, FN = 0, FP = 100, TN = 9,899. All positives = TP + FP = 101. TN + FP + TP = 9,899 + 100 + 1 = 10,000, so the arithmetic closes.

- **Title (bold 17px, top center, `#1a5276`):** "\"99% Accurate Test!\" — But What's the Base Rate?".
- **Setup line (`#333` 17px, left-aligned at (50, 55)):** "10,000 people. Disease affects 1 in 10,000. Test: 99% accurate." — the counts are interpolated from the computed variables.
- **Green bar:** filled rect `#27ae60` at (50, 70), 500×30, with centered white bold 17px label: "9,899 healthy — test correctly says negative" (count = computed TN).
- **Orange bar:** filled rect `#e67e22` at (50, 110), width scaled from FP (floor 100px), centered white label "100 false positives" (count = computed FP).
- **Red sliver:** filled rect `#e74c3c` immediately right of the orange bar, width = FP-proportional (floor 5px), with red 17px left-aligned label: "← 1 true positive (0 missed)".
- **Annotation (bold red 17px, centered at y=175):** "You test positive → 99.0% chance you're HEALTHY. The base rate dominates." — the share is `FP / (TP + FP)` = 100/101 = 99.0%.
- **Caption lines (gray `#555` 17px, centered at y=200, 225, 250):** "P(disease | positive) = 1/101 = 0.99%   (exact Bayes: 0.98%)." / "\"99% accurate\" is almost always WRONG when it fires on a rare event." / "Bayes' theorem is not optional. Ignoring base rate = guaranteed wrong conclusions.".
- **Footnote (gray `#888` 13px, centered at y=275):** "Illustrative Example. Counts rounded to whole people: 9,899 + 100 + 1 = 10,000.".
- **Bayes check:** PPV = (0.99 × 0.0001) / (0.99 × 0.0001 + 0.01 × 0.9999) = 0.000099 / 0.0100989 = 0.98%. The rounded-count version (1/101 = 0.99%) is printed alongside it, so both are shown rather than conflated. The earlier "≈ 1/101 ≈ 1%" phrasing was the same order of magnitude but asserted rather than derived.

### Visualization (canvas `c2`, 720×300)

Population dot plot of the positives only: one true positive lost among the false-positive dots. The dot count is the computed FP, so the dot population *is* the stated base rate rather than an unrelated 100.

- **Population rectangle:** light gray fill `#ecf0f1`, dark border `#2c3e50` width 2, at (40, 20), spanning width−80 by height−60.
- **Title (bold 17px `#2c3e50`, centered at y=15):** "The 101 people who tested POSITIVE (out of 10,000 tested, prevalence 0.01%)" — the 101 is the computed TP + FP.
- **False positives:** one orange dot per computed false positive — 100 dots (`rgba(230,126,34,0.8)`, radius 3), placed with a **seeded Park–Miller LCG** (`lcg(20250112)`), never `Math.random()`. Ranges unchanged: x = 60 + rnd()×(width−160), y = 40 + rnd()×(height−100). The scatter is identical on every load and on every resize redraw.
- **True positive:** one red filled dot (`#e74c3c`, radius 6) at the canvas center, ringed by a red stroked circle of radius 12, width 2.
- **Legend labels (bold 16px):** orange text near bottom right: "100 false positives (orange dots)" (count = dots actually drawn); red text near bottom left: "1 true positive (red circle)".
- **Message (bold 18px `#2c3e50`, centered near bottom):** "\"Test positive\" → 99.0% chance you are a FALSE POSITIVE" — computed as drawn / (drawn + TP) = 100/101.
- **Dot-population reconciliation:** each dot is one person among the positives, not one person among the 10,000 tested. The title states this explicitly so the "1 in 10,000" prose and the ~101 drawn marks do not contradict each other.

## Regeneration instructions

- **Determinism:** all generated chart data uses a seeded Park–Miller LCG helper declared after `setup(id)`; `Math.random()` must not appear in chart code. Seed used: `20250112` (`c2` dot scatter). `c1` draws no random data.
- **Computed labels:** the 2×2 table, PPV, false-positive share, and bar widths are all derived in JS from `N`, `base`, `sens`, `spec`; a `ppv(sens, spec, base)` helper returns the exact Bayes value. No statistic beside generated data is hardcoded.
- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title`, bullets, and the "Why it's pseudoscience" paragraph; right `<td>` (60%, centered) holds two stacked canvases (`c1` 720×340, `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; paragraphs `#333` 0.95em; `ul` 0.9em `#333` with 6px item spacing; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`/`#333`, dark slate `#2c3e50`, light gray `#ecf0f1`.
- In regenerated HTML, any card links use `.html` extensions.
