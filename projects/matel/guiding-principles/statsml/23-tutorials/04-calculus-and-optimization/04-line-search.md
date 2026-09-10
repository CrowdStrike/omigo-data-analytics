# Line Search

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Line Search

**Subtitle:** Gradient descent knows which way is downhill but not how far to go — a line search takes a bold trial step and shrinks it until the altitude drop is good enough

## A Foggy Hike Downhill

**Tags:** `core idea` (blue), `step size` (green), `gradient descent` (orange)

- **The hill** — a foggy hillside has altitude 1.5x² meters at position x from the valley floor
- **The hiker** — she stands at x = 4, altitude 24 m, and feels a slope of 12 under her boots
- **The stride rule** — gradient descent moves a distance of slope × step size, so her stride is 12t
- **Timid** — step size t = 0.05 moves her to x = 3.4, altitude 17.3 m: safe but painfully slow
- **Reckless** — t = 1 flings her to x = −8 on the far wall, altitude 96 m: worse than the start
- **Just right** — t = 0.25 lands at x = 1, altitude 1.5 m: nearly the valley floor in one stride

*Example (italic):* With fog hiding the valley, the only way to judge a stride is to take it and check the altimeter.

**Key point:** The downhill direction comes free from the slope, but the step size t is a choice. Line search is how gradient descent makes that choice — by testing actual altitudes.

### Visualization (canvas `c1`, 720×300)

Valley cross-section with the hiker's start point and three candidate landing points for step sizes t = 0.05, 0.25, and 1.

- **Title (bold 15px, `#1a5276`, top center):** "One Hiker, Three Candidate Strides on the Hill f(x) = 1.5x²".
- **Data:** hill curve y = 1.5x² sampled at x = −9 to 6 in steps of 0.25 (deterministic formula, no randomness); start point (4, 24); landings: t = 0.05 → (3.4, 17.34), t = 0.25 → (1, 1.5), t = 1 → (−8, 96).
- **Axes:** x from −9 to 6 mapped to px 60 → 680; y from 0 to 105 mapped to baseline y=255 up to y=55; x-axis 1px `#999` at y=255 with tick labels at x = −8, −4, 0, 4 (12px `#444`); y-axis label "altitude (m)" 12px `#444` rotated at left.
- **Curve:** ink `#1a5276` 2px line.
- **Start:** 7px dot `#2c3e50` at (4, 24) with bold 13px `#2c3e50` label "start: x = 4, 24 m".
- **Landings:** 6px dots with 2px straight arrows from the start dot — blue `#2a78d6` to (3.4, 17.34) labeled bold 12px "t = 0.05 → 17.3 m"; green `#008300` to (1, 1.5) labeled "t = 0.25 → 1.5 m"; magenta `#d55181` to (−8, 96) labeled "t = 1 → 96 m".
- **Caption (12px `#444`, bottom):** "stride = slope × t = 12t meters; fog means she only learns the altitude after stepping".

## Backtracking: Halve Until It Drops Enough

**Tags:** `worked example` (blue), `backtracking` (green), `Armijo rule` (orange)

- **Start bold** — try t = 1 first: from x = 4 she lands at x = −8, altitude 96, a 72 m climb — fail
- **The bar** — Armijo demands a drop of at least c × t × slope²; with c = 0.3 that is 43.2t meters
- **Halve it** — t = 0.5 reaches x = −2, altitude 6: an 18 m drop, short of the 21.6 m bar — fail
- **Halve again** — t = 0.25 reaches x = 1, altitude 1.5: a 22.5 m drop beats the 10.8 m bar — pass
- **Cheap trials** — each test reads only the altimeter (one function value); no new slope is needed

*Example (italic):* Three altimeter readings — 96, 6, then 1.5 — settled the whole question; t = 0.25 was accepted.

**Key point:** Backtracking means start with a big t and multiply by ½ until the Armijo test passes. The bar shrinks with t, so a small enough step always passes eventually.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: for each of the three backtracking trials, the required drop (Armijo bar) next to the actual drop, with negative drops drawn below the zero line.

- **Title (bold 15px, `#1a5276`, top center):** "Backtracking: Required Drop (Armijo Bar) vs Actual Drop".
- **Data:** trials t = [1, 0.5, 0.25]; required drops [43.2, 21.6, 10.8] m; actual drops [−72, 18, 22.5] m; verdicts [fail, fail, pass].
- **Zero line:** 2px `#999` at y=150 from x=70 to x=660; scale 1.0 px per meter (positive drops drawn upward, negative downward).
- **Groups:** centered at x = 190, 390, 590; each group two 52px-wide bars 12px apart; required bar fill `rgba(42,120,214,0.35)` with 1px `#2a78d6` border; actual bar fill `rgba(0,131,0,0.5)` on pass, `rgba(213,81,129,0.5)` on fail.
- **Value labels (bold 12px):** "need ≥ 43.2" blue and "went UP 72" magenta (below the negative bar) for trial 1; "need ≥ 21.6" blue and "18 — short" magenta for trial 2; "need ≥ 10.8" blue and "22.5 — pass" green for trial 3.
- **Group labels (12px `#444`, y=280):** "trial 1: t = 1", "trial 2: t = 0.5", "trial 3: t = 0.25".
- **Annotation (bold 13px green `#008300`, top right):** "the bar shrinks with t — a small enough step always passes".

## Why a Fixed Step Size Fails

**Tags:** `where it's used` (blue), `divergence` (red), `rule of thumb` (green)

- **Fixed big** — a constant t = 0.8 overshoots harder each step: 24 → 47 → 92.2 → 180.7 m, diverging
- **Fixed small** — a constant t = 0.02 always descends, yet after 6 steps still sits at 11.4 m
- **Backtracking** — re-chosen steps hit 1.5 m, then 0.09, then 0.006: the floor in three strides
- **No universal t** — the safe step size depends on curvature, and curvature changes as you move
- **Everywhere** — L-BFGS, conjugate gradient, and Newton's method all wrap a line search inside

*Example (italic):* Same hill, same 24 m start: one walker blows up, one crawls, and the line-search walker is home in three strides.

**Key point:** A step size that works early can diverge later, and one that never diverges crawls. Re-choosing t at every step via line search is what makes descent reliable.

### Visualization (canvas `c3`, 720×300)

Line chart of altitude per iteration for three step-size strategies on the same hill from the same start.

- **Title (bold 15px, `#1a5276`, top center):** "Same Hill, Three Step-Size Strategies (altitude per iteration)".
- **Data:** fixed t = 0.8 altitudes `[24, 47, 92.2, 180.7]` (iterations 0–3, then off-chart); fixed t = 0.02 altitudes `[24, 21.2, 18.7, 16.6, 14.6, 12.9, 11.4]` (iterations 0–6); backtracking altitudes `[24, 1.5, 0.09, 0.006]` (iterations 0–3).
- **Axes:** iterations 0–6 at px x = 70 + i × 98; altitude 0–190 mapped to baseline y=250 up to y=55; y ticks 0, 50, 100, 150 with 12px `#444` labels; iteration numbers 0–6 12px `#444` below the baseline; 1px `#e5e9ef` horizontal gridlines at the y ticks.
- **Fixed big series:** orange `#d95926` 3px line, 4px dots, ending in a small upward arrowhead past iteration 3 with bold 12px orange label "t = 0.8 fixed: diverging (354 next)".
- **Fixed small series:** blue `#2a78d6` 3px line, 4px dots, bold 12px blue label at its right end "t = 0.02 fixed: still 11.4 m".
- **Backtracking series:** green `#008300` 3px line, 4px dots, bold 13px green label "backtracking: at the floor by step 3".
- **Caption (12px `#444`, bottom):** "hill f(x) = 1.5x², start x = 4; backtracking values from the worked example above".

## Good Enough, Not Best

**Tags:** `common mistake` (red), `sufficient decrease` (orange)

- **A 1-D slice** — freeze the direction and altitude becomes a curve in t: φ(t) = 24 − 144t + 216t²
- **The promise** — the tangent at t = 0 predicts a 144 m drop per unit step (that is slope²)
- **The relaxed line** — Armijo keeps 30% of the promise: accept t whenever φ(t) ≤ 24 − 43.2t
- **A wide zone** — every t up to 0.467 passes the test; there is no single magic step to find
- **Not the best** — the truly best step is t = 1/3 (altitude 0); backtracking stops at t = 0.25
- **Down ≠ enough** — t = 0.5 does lower altitude (24 → 6) yet fails: 18 m is under the 21.6 m bar

*Example (italic):* Backtracking accepted t = 0.25 without ever knowing the perfect step was 1/3 — good enough is the whole point.

**Common mistake:** Thinking a line search hunts for the exactly-best step, or that any decrease at all is acceptable. Armijo defines a wide zone of sufficient steps and takes the first trial that lands inside it.

### Visualization (canvas `c4`, 720×300)

The classic Armijo picture: the 1-D slice φ(t), its tangent "promise" line, the relaxed Armijo line, a shaded accept zone, and the three backtracking trial points.

- **Title (bold 15px, `#1a5276`, top center):** "The Armijo Picture: a Zone of Good-Enough Steps".
- **Data:** curve φ(t) = 24 − 144t + 216t² sampled at t = 0 to 1 in steps of 0.02 (deterministic formula); tangent line 24 − 144t; Armijo line 24 − 43.2t; accept-zone boundary t = 0.467; trials (1, 96) fail, (0.5, 6) fail, (0.25, 1.5) pass; true minimizer (1/3, 0).
- **Axes:** t from 0 to 1 mapped to px 60 → 680; altitude 0–100 mapped to baseline y=255 up to y=55 (2.0 px per meter); x tick labels at t = 0, 0.25, 0.5, 0.75, 1 (12px `#444`); 1px `#999` axis lines.
- **Accept band:** rectangle from t = 0 to t = 0.467, y=55 to y=255, fill `rgba(0,131,0,0.07)`, bold 12px green label "accept zone: t ≤ 0.467" near its top.
- **Curve:** ink `#1a5276` 3px.
- **Tangent:** dashed (dash 5/4) `#6b7280` 2px from t = 0 to t = 0.167 (clipped at altitude 0), 12px `#6b7280` label "the promise: −144 per unit t".
- **Armijo line:** dashed (dash 5/4) green `#008300` 2px from t = 0 to t = 0.556 (clipped at altitude 0), bold 12px green label "Armijo line: 24 − 43.2t".
- **Trials:** 7px dots — magenta `#d55181` at (1, 96) labeled bold 12px "t = 1: fail" and at (0.5, 6) labeled "t = 0.5: fail"; green `#008300` at (0.25, 1.5) labeled bold 12px "t = 0.25: pass"; small violet `#4a3aa7` diamond at (0.333, 0) labeled 12px "true best t = 1/3".
- **Caption (12px `#444`, bottom):** "pass = curve under the green line; backtracking takes the first trial inside the zone".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
