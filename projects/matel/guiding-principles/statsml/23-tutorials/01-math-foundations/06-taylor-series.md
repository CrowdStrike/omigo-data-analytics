# Taylor Series

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Taylor Series

**Subtitle:** Zoom in on any smooth curve and it turns into a polynomial — sin(x) ≈ x is the famous one-term version, excellent for small angles and badly wrong for big ones

## The Swing That Physicists Cheat On

**Tags:** `core idea` (blue), `sin(x) ≈ x` (green), `zooming in` (orange)

- **The swing** — a pendulum's motion involves sin(angle), an awkward curve with no simple algebra
- **The cheat** — for small angles, physicists just replace sin(x) with plain x and move on
- **Zoom in** — near zero the sine curve and the straight line y = x are visually identical
- **Check it** — sin(0.5) is 0.4794 while the cheat says 0.5, a gap of just 0.02
- **Far out** — at x = 1.5 the line reads 1.50 but sin gives 0.997, a gap of 0.50

*Example (italic):* A swing pushed 10° off vertical: the shortcut says 0.1745, true sin says 0.1736 — a clock built on the cheat keeps near-perfect time.

**Key point:** Every smooth function looks like a straight line up close. A Taylor series is that zoom-in made precise: a polynomial that copies the function's value and slope at one chosen point.

### Visualization (canvas `c1`, 720×300)

Single-panel line chart overlaying y = sin(x) and the line y = x on one axis, with vertical gap markers at x = 0.5 (tiny gap) and x = 1.5 (large gap).

- **Title (bold 15px, `#1a5276`, top center):** "sin(x) and the Line y = x: Twins Near Zero".
- **Axes:** origin x=60, plot width 600, baseline y=255, chart height 200; x range 0–1.8 (xMax), y range 0–1.9 (yMax); 1px `#999` L-shaped axes; x ticks at 0, 0.5, 1.0, 1.5 with 12px `#444` labels; axis caption 12px `#444` "angle x (radians)" centered at bottom.
- **Line y = x:** blue `#2a78d6` 2px dashed (dash 6/4) from (0,0) to (1.8,1.8).
- **Sine curve:** ink `#1a5276` 3px solid, plotted as 90 segments of Math.sin over 0–1.8.
- **Gap marker at x = 0.5:** green `#008300` 2px vertical segment from y=0.4794 to y=0.5; green bold 12px left-aligned label "at 0.5: gap only 0.02" at 8px right of the marker, 8px above Y(0.5).
- **Gap marker at x = 1.5:** orange `#d95926` 2px vertical segment from y=0.997 to y=1.5; orange bold 13px right-aligned label "at 1.5: gap = 0.50" at 10px left of the marker, height Y(1.25).
- **Curve labels (bold 12px, left-aligned at X(1.55)):** blue "y = x (the cheat)" at Y(1.72); ink "y = sin(x)" at Y(0.90).

## Adding Terms One at a Time

**Tags:** `worked example` (blue), `the formula` (green)

- **Target** — compute sin(0.5) = 0.479426 with no sine button, using only +, −, and ×
- **One term** — the guess x gives 0.5, which is off by 0.0206
- **Two terms** — x − x³/6 gives 0.479167, off by only 0.0003
- **Three terms** — x − x³/6 + x⁵/120 gives 0.479427, off by 0.000002
- **The formula** — f(x) ≈ f(a) + f′(a)(x−a) + f″(a)(x−a)²/2 + ...; each term fixes one more bend

*Example (italic):* Redo it by hand: 0.5³ = 0.125, divide by 6 to get 0.0208, subtract from 0.5 — that is 0.479167, already four digits of sin(0.5).

**Key point:** Each added term matches one more derivative at the center, and near the center every term shrinks the error by orders of magnitude. Calculators and math libraries compute sin exactly this way.

### Visualization (canvas `c2`, 720×300)

Line chart overlaying three successive Taylor polynomials on the true sine curve, showing each extra term hugging sin(x) farther out.

- **Title (bold 15px, `#1a5276`, top center):** "Each Extra Term Hugs the Sine Curve Longer".
- **Axes:** origin x=60, plot width 600, baseline y=255, chart height 195; x range 0–2.2 (xMax), y range 0–1.6 (yMax); 1px `#999` axes; x ticks at 0, 0.5, 1.0, 1.5, 2.0 with 12px `#444` labels; axis caption "x (radians)".
- **Curves** (each drawn over 110 segments, segments clipped when y < −0.05 or y > yMax): p1(x) = x in blue `#2a78d6` 2px dashed (dash 6/4); p3(x) = x − x³/6 in green `#008300` 2px solid; p5(x) = x − x³/6 + x⁵/120 in orange `#d95926` 2px solid; Math.sin in ink `#1a5276` 3.5px solid drawn last.
- **Legend** (top-left at x = origin+16, starting at Y(1.5), rows 20px apart; each row a 22×4 color swatch rect plus bold 12px label in the same color): "sin(x) — the target" (ink), "1 term:  x" (blue), "2 terms: x − x³/6" (green), "3 terms: x − x³/6 + x⁵/120" (orange).
- **Guide line at x = 0.5:** dashed `#bbb` 1px (dash 3/3) vertical from baseline up to Y(0.62).
- **Annotation (violet `#4a3aa7` bold 12px, left-aligned 6px right of the guide, two lines):** "at x = 0.5: guesses 0.5 → 0.4792 → 0.479427" at Y(0.30), "(true sin: 0.479426)" at Y(0.20).
- **Peel-off annotations (bold 12px):** blue left-aligned "1 term peels off first" at X(0.95), Y(1.12); orange right-aligned "3 terms still close past 2" at X(2.15), Y(1.02).

## Where the Shortcut Breaks

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Local deal** — a Taylor approximation is a contract about one neighborhood, never the whole curve
- **Small angles** — at 10° the shortcut sin(x) ≈ x is off by 0.5%; at 20° it is off by about 2%
- **Big angles** — at 60° it overshoots by 21% and at 90° by 57%; the physics answer is junk there
- **Rule of thumb** — the swing shortcut is trusted below roughly 15°; clock pendulums stay under 5°
- **Fixing it** — going farther out needs more terms, or a fresh series re-centered at a nearer point

*Example (italic):* A simulation of a wild 60° swing built on sin(x) ≈ x is 21% off before it even starts — the shortcut was never rated for that angle.

**Common mistake:** Using a Taylor shortcut far from its center. The error is not roughly constant — it grows like the first dropped term, here x³/6, so doubling the angle multiplies the absolute error by about eight and the percentage error by about four.

### Visualization (canvas `c3`, 720×300)

Bar chart of the relative error of sin(x) ≈ x at seven swing angles, color-graded from green (trusted) through yellow to orange (broken), with a trusted-zone bracket over the first two bars.

- **Title (bold 15px, `#1a5276`, top center):** "How Wrong Is sin(x) ≈ x? Error by Swing Angle".
- **Data:** angles `['5°', '10°', '20°', '30°', '45°', '60°', '90°']`; error values `[0.13, 0.51, 2.06, 4.72, 11.07, 20.92, 57.08]`; bar labels `['0.1%', '0.5%', '2.1%', '4.7%', '11%', '21%', '57%']`; bar colors `[green, green, yellow, yellow, orange, orange, orange]` (green `#008300`, yellow `#c98500`, orange `#d95926`).
- **Axes:** origin x=70, plot width 590, baseline y=245, chart height 180, scale max 60; 1px `#999` axes; caption 12px `#444` "swing angle" centered at bottom.
- **Bars:** 7 slots of width pw/7; each bar inset 0.18 of the slot, 0.64 of the slot wide; fill at globalAlpha 0.55 in the bar's color; value label bold 13px in the bar's color 8px above the bar; angle label 12px `#444` 18px below the baseline.
- **Trusted-zone bracket:** green 2px down-facing bracket spanning the first two bar slots (from bw×0.1 to bw×1.9), drawn near the chart top (14–26px below the top); green bold 12px left-aligned label "trusted zone: under ~15°, error below 1%" starting at bw×2.1.
- **Annotation (orange bold 13px, centered at x = origin + pw×0.62, y = baseline−105):** "doubling the angle multiplies the error ≈ 4×".

## The Same Trick All Over Data Science

**Tags:** `where it's used` (blue), `linearization` (orange)

- **Log returns** — ln(1 + r) ≈ r is why a 5% return and its 0.0488 log-return get swapped freely
- **Interest** — eˣ ≈ 1 + x turns continuous compounding into simple interest for small rates
- **Gradient descent** — each step trusts a one-term Taylor view: locally the loss is a plane
- **Newton's method** — a two-term view: model the loss as a parabola, jump to its bottom
- **Same fine print** — a 100% return has ln(2) = 0.693, not 1.0; big steps break local models too

*Example (italic):* ln(1.05) = 0.0488 ≈ 0.05, but ln(2.00) = 0.693 — the small-return shortcut quietly fails on a stock that doubles.

**Key point:** Every "linearize and solve" move in data science is a short Taylor series in disguise, and it inherits the same fine print: valid near the center, unreliable far away.

### Visualization (canvas `c4`, 720×300)

Line chart overlaying the true log-return curve y = ln(1 + r) and its one-term shortcut y = r, with a marker at 5% (tiny gap) and a gap line at 100% (large gap).

- **Title (bold 15px, `#1a5276`, top center):** "Log Returns: ln(1 + r) ≈ r Is a One-Term Taylor Series".
- **Axes:** origin x=70, plot width 580, baseline y=250, chart height 195; x range 0–1.0 (xMax), y range 0–1.05 (yMax); 1px `#999` axes; x ticks at 0, 0.25, 0.5, 0.75, 1.0 labeled "0%", "25%", "50%", "75%", "100%" in 12px `#444`; axis caption "simple return r".
- **Shortcut line y = r:** blue `#2a78d6` 2px dashed (dash 6/4) from (0,0) to (1,1).
- **True curve:** green `#008300` 3px solid, Math.log(1 + r) plotted over 100 segments from r=0 to 1.
- **Marker at r = 0.05:** aqua `#199e70` filled 5px dot at (0.05, 0.0488); aqua bold 12px left-aligned label "5% return: ln(1.05) = 0.0488 — gap 0.0012" at 10px right, 10px above the dot.
- **Gap at r = 1.0:** magenta `#d55181` 2px vertical segment from y=0.693 to y=1.0; magenta bold 13px right-aligned label, two lines at 12px left of the segment: "100% return: ln(2) = 0.693," at Y(0.86), "shortcut says 1.0 — gap 0.307" at Y(0.79).
- **Curve labels (bold 12px, left-aligned at X(0.62)):** blue "shortcut: y = r" at Y(0.70); green "true: y = ln(1 + r)" at Y(0.42).

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
