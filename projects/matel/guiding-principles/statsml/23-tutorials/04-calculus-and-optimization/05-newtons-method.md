# Newton's Method

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Newton's Method

**Subtitle:** Guess, measure the slope, and jump to where the tangent hits zero — using curvature turns a slow guessing game into a solver that doubles its correct digits every step

## A Square Patio and a Clever Guess

**Tags:** `core idea` (blue), `tangent line` (green), `root finding` (orange)

- **The patio** — a landscaper needs the side of a square patio with area 20 m²; that side is √20
- **First guess** — try x₀ = 5: a 5 m side gives 25 m², so the miss is f(5) = 5² − 20 = 5
- **The tangent** — the miss curve f(x) = x² − 20 has slope f′(5) = 10 at the guess
- **Slide down** — follow the tangent to zero: x₁ = 5 − 5/10 = 4.5, within 3 cm of √20
- **The rule** — Newton's method repeats xₙ₊₁ = xₙ − f(xₙ)/f′(xₙ) until the miss is ~zero

*Example (italic):* One more turn gives x₂ = 4.5 − 0.25/9 ≈ 4.4722; the true side is 4.4721 m — matched to a tenth of a millimeter.

**Key point:** Newton's method upgrades "guess and check" to "guess, measure the slope, and jump straight to where the tangent says zero is."

### Visualization (canvas `c1`, 720×300)

Single-panel curve chart: the miss curve f(x) = x² − 20 with the tangent at the first guess x₀ = 5 sliding down to the improved guess x₁ = 4.5.

- **Title (bold 15px, `#1a5276`, top center):** "One Newton Step: Slide Down the Tangent to Zero".
- **Data:** curve y = x² − 20 sampled at x = 3.5 → 5.5 in steps of 0.05 (deterministic formula, no randomness); guess point (5, 5); tangent line y = 10x − 45 drawn from x = 4.35 to x = 5.45; new point (4.5, 0); true root (4.4721, 0).
- **Layout:** plot area x = 60 → 660, y = 45 → 260; x range 3.5–5.5, y range −9 → 11; zero line (f = 0) 1.5px `#999` across the plot, x tick labels 3.5, 4.0, 4.5, 5.0, 5.5 (12px `#444`) below it.
- **Curve:** blue `#2a78d6` 3px. **Tangent:** orange `#d95926` dashed (dash 6/4) 2px.
- **Dots:** green `#008300` 6px at (5, 5) labeled bold 12px green "x₀ = 5, f = 5"; orange `#d95926` 6px at (4.5, 0) labeled bold 12px orange "x₁ = 4.5"; magenta `#d55181` 6px at (4.4721, 0) labeled bold 12px magenta "√20 ≈ 4.4721".
- **Annotation (bold 13px green, near tangent midpoint):** "tangent slope 10 → step = 5/10".
- **Caption (12px `#444`, bottom left):** "f(x) = x² − 20; the tangent at x = 5 hits zero at x = 4.5".

## Doubling the Digits Every Turn

**Tags:** `worked example` (blue), `quadratic convergence` (green)

- **The table** — the iterates run 5 → 4.5 → 4.47222 → 4.4721360, and √20 = 4.4721360
- **The errors** — the misses shrink 0.53 → 0.028 → 0.000086 → 0.0000000008
- **Doubling digits** — correct digits go 0.3 → 1.6 → 4.1 → 9.1, roughly doubling per step
- **Why so fast** — each error is about the previous one squared (÷ 2√20), so tiny gets tinier
- **The rival** — bisection halves [4, 5] each step: 0.5, 0.25, 0.125, 0.0625 — a digit per ~3 steps

*Example (italic):* Three Newton turns pin the patio side to 4.4721360 m; bisection would need about 30 halvings to match that.

**Key point:** This is quadratic convergence — the error roughly squares each iteration, so correct digits double; halving methods only add digits at a fixed slow rate.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart of correct digits per iteration, Newton (green) vs bisection (blue), with the Newton iterates printed above their bars.

- **Title (bold 15px, `#1a5276`, top center):** "Correct Digits per Iteration: Newton vs Bisection".
- **Data:** iterations 0–3; Newton digits `[0.3, 1.6, 4.1, 9.1]` (from errors 0.53, 0.028, 0.000086, 0.0000000008); bisection digits `[0.3, 0.6, 0.9, 1.2]` (from interval widths 0.5, 0.25, 0.125, 0.0625 starting at [4, 5]); Newton iterate labels "5", "4.5", "4.47222", "4.4721360".
- **Layout:** axis origin x = 70, width 560, baseline y = 245, chart height 190; y scale 0–10 digits, horizontal gridlines every 2 digits in `#e5e9ef` with 12px `#6b7280` labels.
- **Bars:** one pair per iteration, bar width 42, 8px gap within a pair, pairs evenly spaced; Newton fill `rgba(0,131,0,0.5)`, bisection fill `rgba(42,120,214,0.45)`; iteration labels "iter 0" → "iter 3" 12px `#444` below the baseline.
- **Iterate labels:** bold 12px green `#008300` above each Newton bar: "5", "4.5", "4.47222", "4.4721360".
- **Annotations:** magenta `#d55181` bold 13px near the tall Newton bar: "digits ≈ double each step"; blue `#2a78d6` bold 12px over the bisection bars: "bisection: +0.3 digits per step".
- **Caption (12px `#444`, bottom left):** "digits = −log10(error); true value √20 = 4.4721360".

## From Finding Zeros to Finding Bottoms

**Tags:** `where it's used` (blue), `minima` (green), `curvature` (orange)

- **New question** — the lowest point of a curve is where its slope is zero: a root of g′(x)
- **Same trick** — apply Newton to the slope: xₙ₊₁ = xₙ − g′(xₙ)/g″(xₙ), slope over curvature
- **The bowl** — for g(x) = (x − 4)² + 3, from x = 1: g′ = −6, g″ = 2, so x₁ = 1 − (−6)/2 = 4
- **One jump** — on a parabola the curvature describes the bowl exactly, so one step lands it
- **The contrast** — gradient descent from 1 (rate 0.2) crawls 1 → 2.2 → 2.92 → 3.35 → 3.61 → 3.77
- **Where it lives** — logistic regression, XGBoost's second-order steps, and scipy optimizers

*Example (italic):* After six moves gradient descent still sits at x = 3.77; Newton read the curvature and hit x = 4 on move one.

**Key point:** The slope says which way is downhill; the curvature says how far away the bottom is. Using both is exactly what makes Newton fast.

### Visualization (canvas `c3`, 720×300)

Bowl-shaped cost curve with two paths to the minimum: gradient descent's chain of small blue steps along the curve vs Newton's single green jump.

- **Title (bold 15px, `#1a5276`, top center):** "Finding the Bottom: Gradient Descent Crawls, Newton Jumps".
- **Data:** curve g(x) = (x − 4)² + 3 sampled at x = 0.5 → 7.5 in steps of 0.1; gradient-descent points x `[1, 2.2, 2.92, 3.352, 3.611, 3.767]` with g `[12, 6.24, 4.17, 3.42, 3.15, 3.05]` (learning rate 0.2); Newton jump from (1, 12) to (4, 3).
- **Layout:** plot area x = 60 → 660, y = 45 → 255; x range 0.5–7.5, y range 2.5 → 13; x tick labels 1–7 (12px `#444`); zero-decoration y-axis with 12px `#6b7280` label "cost g(x)" rotated on the left.
- **Curve:** ink `#1a5276` 2.5px. **GD path:** blue `#2a78d6` 5px dots joined by 2px blue segments with a small arrowhead at each segment end. **Newton:** green `#008300` 3px straight arrow from (1, 12) to (4, 3) with arrowhead, 7px green dot at (4, 3).
- **Annotations:** blue bold 12px above the GD dots: "gradient descent: 6 steps, still at 3.77"; green bold 13px below the Newton arrow: "Newton: x₁ = 1 − (−6)/2 = 4, done in one".
- **Caption (12px `#444`, bottom left):** "g(x) = (x − 4)² + 3; gradient descent uses rate 0.2".

## When the Tangent Lies

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Flat slope** — starting the patio search at x = 0.5, f′ = 1, so x₁ = 0.5 + 19.75 = 20.25
- **Wrong hill** — near x = 0.5, h(x) = x⁴/4 − 2x² curves downward (h″ = −3.25), the bowl lies
- **The step** — from 0.5 Newton moves to ≈ −0.08, toward the hilltop, away from the valley at 2
- **Zero divide** — where the slope (or curvature) is exactly zero, the formula divides by zero
- **The fix** — real optimizers damp the step, check it went downhill, or fall back to gradient

*Example (italic):* An analyst's fit "diverged" because Newton, started on a downward-curving shoulder, marched toward a maximum.

**Common mistake:** Trusting the raw Newton step everywhere. It is only as good as the local tangent — flat slopes launch it far away, and negative curvature steers it to hilltops instead of valleys.

### Visualization (canvas `c4`, 720×300)

Dual-panel failure gallery: a near-flat tangent launching the iterate off the chart (left) and negative curvature steering the step toward a hilltop (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways Newton Goes Wrong".
- **Left panel (flat-slope overshoot):** curve f(x) = x² − 20 sampled at x = 0 → 7 step 0.1; plot area x = 55 → 335, y = 45 → 250; x range 0–7, y range −22 → 30; zero line 1.5px `#999` with x tick labels 0, 2, 4, 6; blue `#2a78d6` 3px curve; green `#008300` 6px dot at (0.5, −19.75) labeled bold 12px "start x₀ = 0.5"; orange `#d95926` dashed (6/4) 2px tangent y = x − 20.25 from x = 0.5 rising right and exiting the panel edge with an arrowhead; orange bold 12px annotation "x₁ = 20.25 → off the chart"; caption 12px `#444` "slope at 0.5 is only 1: a 19.75-unit leap".
- **Right panel (wrong curvature):** curve h(x) = x⁴/4 − 2x² sampled at x = −2.8 → 2.8 step 0.1; plot area x = 400 → 680, same vertical extent; x range −2.8 → 2.8, y range −4.5 → 1; green `#008300` 6px dots at the valleys (−2, −4) and (2, −4) labeled bold 12px "true minima"; orange `#d95926` 6px dot at (0.5, −0.48) labeled "start"; magenta `#d55181` 2.5px arrow from (0.5, −0.48) to (−0.08, −0.01) at the hilltop; magenta bold 12px annotation, two lines: "h″ < 0 here:" / "step climbs to the peak"; caption 12px `#444` "from 0.5 Newton steps to −0.08".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Curves are drawn from the stated closed-form formulas at the stated fixed sample steps — no randomness anywhere.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
