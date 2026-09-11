# Maximum Likelihood

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Maximum Likelihood

**Subtitle:** A coin shows 7 heads in 10 flips — try a few settings for the coin's heads rate and keep the one that makes what you saw most probable

## Three Dial Settings for One Coin

**Tags:** `core idea` (blue), `running example` (green)

- **The data** — one coin, flipped 10 times, comes up heads 7 times
- **The unknown** — the coin's true heads rate p, a dial we cannot see directly
- **The trick** — try candidate settings (p = 0.3, 0.5, 0.7) and score each one
- **The score** — how probable does this setting make exactly what we saw?
- **The winner** — p = 0.7 gives 7-heads-in-10 a 27% chance, the highest of the three

*Example:* A vending machine ate 7 of your 10 coins — you'd guess it fails about 70% of the time. That guess is maximum likelihood.

**Maximum likelihood:** pick the parameter value that makes the data you actually saw most probable.

### Visualization (canvas `c1`, 720×300)

Bar chart: probability of seeing 7 heads in 10 flips under each of three candidate p values.

- **Title (bold 15px `#1a5276`, top center):** "Chance of Seeing 7 Heads in 10 Flips, at Each Dial Setting"
- **Data (3 bars):** labels "p = 0.3", "p = 0.5", "p = 0.7" with values 0.009, 0.117, 0.267 (from 120·p⁷·(1−p)³); value labels above bars as "0.9%", "11.7%", "26.7%" (bold 13px `#2c3e50`).
- **Colors:** orange `#d95926`, blue `#2a78d6`, green `#008300`; winning bar (p=0.7) full opacity, others alpha 0.55. Bar width 110px, gap 95px, first bar at x=130.
- **Axes:** y scale max 0.30 with labels 0%, 10%, 20%, 30% (12px gray `#6b7280`, right-aligned); gray `#999` axis lines; baseline y=240, chart height 175, left pad 70.
- **Annotation (bold 13px green `#008300`):** "p = 0.7 explains the data ~30x better than p = 0.3"
- **X-axis caption (12px gray, centered):** "candidate heads rate of the coin"

## Scoring the Settings by Hand

**Tags:** `worked example` (green), `arithmetic` (blue)

- **The formula** — chance of 7 heads in 10 flips = 120 × p⁷ × (1−p)³
- **The 120** — counts the different orders 7 heads and 3 tails can arrive in
- **Try p = 0.3** — 120 × 0.3⁷ × 0.7³ ≈ 0.009 — under a 1% chance
- **Try p = 0.5** — 120 × 0.5⁷ × 0.5³ ≈ 0.117 — about 12%
- **Try p = 0.7** — 120 × 0.7⁷ × 0.3³ ≈ 0.267 — about 27%, the peak
- **No coincidence** — the full curve peaks exactly at 7/10; the sample fraction IS the MLE

*Example:* Slide the dial from 0 to 1: the score climbs, tops out at 0.7, then falls away.

**Hand-checkable:** for coin-style data the MLE is just heads ÷ flips — the formal method lands exactly on the common-sense answer.

### Visualization (canvas `c2`, 720×300)

Line chart: the full likelihood curve 120·p⁷·(1−p)³ over p ∈ [0,1], with the three tried settings marked as dots and a dashed drop line at the peak.

- **Title (bold 15px `#1a5276`, top center):** "The Whole Curve: 120 × p⁷ × (1−p)³ for Every p"
- **Curve:** blue `#2a78d6`, 3px wide, evaluated at 101 points p = 0.00…1.00 using lik(p) = 120·p⁷·(1−p)³ (deterministic formula, no randomness).
- **Axes:** x ticks 0.0 through 1.0 in steps of 0.1; y scale max 0.30 with labels 0%, 10%, 20%, 30%; gray `#999` axis lines; padding top 50, bottom 55, left 70, right 35. X-axis label "candidate heads rate p" (12px gray, centered); rotated y-axis label "chance of the observed 7/10".
- **Peak marker:** dashed (5/4) green `#008300` 1.5px vertical line from baseline to the curve at p = 0.7.
- **Tried points:** 6px-radius dots at (0.3, 0.009) orange `#d95926`, (0.5, 0.117) blue `#2a78d6`, (0.7, 0.267) green `#008300`; labeled "0.9%" (12px orange), "11.7%" (12px blue), and "peak: 26.7% at p = 0.7 = 7/10" (bold 13px green).

## Where a Data Scientist Meets It

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Hidden in .fit()** — logistic regression and many models train by maximizing likelihood
- **Familiar answers** — the sample mean and sample fraction are both MLEs in disguise
- **More data, sharper peak** — 70 heads in 100 flips: same answer 0.7, much narrower curve
- **Peak width = confidence** — a flat curve says many settings explain the data equally well
- **Logs in practice** — software adds log-likelihoods instead of multiplying tiny numbers

*Example:* A spam model picking weights that make the observed spam/not-spam labels most probable is running this exact coin logic at scale.

**Key point:** the estimate is where the curve peaks; how sure you should be is how sharply it peaks — 10 flips and 100 flips give the same 0.7 but very different sharpness.

### Visualization (canvas `c3`, 720×300)

Two overlaid normalized likelihood curves (each scaled to peak = 1): wide curve for 7/10 flips vs narrow curve for 70/100 flips, both peaking at 0.7.

- **Title (bold 15px `#1a5276`, top center):** "Same Best Guess, Sharper Peak: 7/10 Flips vs 70/100 Flips"
- **Curves (3px wide, 199 points p = 0.005…0.995):** normalized likelihood normLik(p, H, T) = exp(H·ln(p/0.7) + T·ln((1−p)/0.3)) — blue `#2a78d6` for H=7, T=3 (wide); violet `#4a3aa7` for H=70, T=30 (narrow).
- **Axes:** x ticks 0.0 through 1.0 in steps of 0.1 (12px gray); gray `#999` axis lines; padding top 50, bottom 55, left 70, right 35. X-axis caption: "candidate heads rate p (curves scaled so each peak = 1)".
- **Peak line:** dashed (5/4) green `#008300` 1.5px full-height vertical line at p = 0.7, labeled above in bold 13px green: "both peak at 0.7".
- **Annotations:** bold 13px blue at left: "10 flips: wide — 0.5 to 0.9 all plausible"; bold 13px violet at right, two lines: "100 flips: narrow —" / "evidence pins p down".

## What the Winner Does Not Prove

**Tags:** `common mistake` (red), `caution` (orange)

- **Not proof** — a perfectly fair coin shows exactly 7 heads in 10 flips about 12% of the time
- **Best, not true** — MLE says 0.7 explains the data best, not that 0.7 is the truth
- **Likelihood ≠ probability of p** — 0.267 is the chance of the data given p, not of p itself
- **Thin evidence wobbles** — 10 flips is a rough estimate; 100 flips pins it down far better
- **Overfitting cousin** — chasing likelihood with a very flexible model memorizes noise

*Example:* Calling a stranger's coin rigged after 7 heads in 10 flips would be a losing bet — fair coins do that all the time.

**The confusion:** "0.7 is the most likely value" is shorthand — strictly, 0.7 is the value that makes the data most likely. From 10 flips, a fair coin remains entirely plausible.

### Visualization (canvas `c4`, 720×300)

Bar chart of the Binomial(10, 0.5) probability mass function (fair coin, 10 flips), with k=7 highlighted.

- **Title (bold 15px `#1a5276`, top center):** "What a Perfectly FAIR Coin Does in 10 Flips"
- **Data (11 bars, k = 0…10, percent):** `[0.1, 1.0, 4.4, 11.7, 20.5, 24.6, 20.5, 11.7, 4.4, 1.0, 0.1]` (hardcoded Binomial(10, 0.5) pmf). Value labels shown above bars with pmf ≥ 4%, e.g. "4.4%", "11.7%", "20.5%", "24.6%".
- **Colors:** k=7 bar magenta `#d55181` at full opacity with a bold magenta value label; all other bars blue `#2a78d6` at alpha 0.45 with gray labels. Bar width 62% of slot.
- **Axes:** y scale max 28% with labels 0%, 10%, 20% (12px gray); gray `#999` axis lines; padding top 52, bottom 58, left 70, right 35. k values 0–10 under bars (12px `#2c3e50`).
- **Annotation (bold 13px magenta, two lines at upper right):** "a fair coin shows exactly 7 heads ~12% of the time" / "— the MLE of 0.7 is a best guess, not proof"
- **X-axis caption (12px gray, centered):** "number of heads in 10 flips when the true p = 0.5"

## Regeneration instructions

- **Template:** tutorial detail page (see `tutorials/CLAUDE.md`). h1 + `.subtitle`, then 4 `.card-section` blocks, each an `<h2>` followed by `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` line, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. Bullets 0.92rem, `li b` colored `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvases:** 720×300 intrinsic, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart palette object `P`: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
