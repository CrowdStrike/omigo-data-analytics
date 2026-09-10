# Convexity

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Convexity

**Subtitle:** A convex cost curve is one bowl with one valley — walk downhill from anywhere and you land at the best answer; a wavy curve can trap you in a dip that isn't the bottom

## One Food Truck, One Bowl-Shaped Cost

**Tags:** `core idea` (blue), `one valley` (green), `bowl shape` (orange)

- **The truck** — a food truck preps q sandwiches each morning; demand hovers near 40
- **Too few** — prepping 20 misses sales, so the expected daily cost is $33
- **Too many** — prepping 60 wastes food, costing $29; the sweet spot is 40 at $16
- **One valley** — costs at preps 10–70 run $48, 33, 22, 16, 20, 29, 42: down, then up, once
- **Convex** — a curve shaped like this bowl is called convex: it has exactly one valley
- **The contrast** — a wavy curve with dips at 20, 40, and 60 has three valleys to choose from

*Example (italic):* The truck's cost falls every step from 10 to 40 preps and rises every step after — there is nowhere to get confused about where the bottom is.

**Key point:** Convex means bowl-shaped: the cost goes down, bottoms out once, and goes up. One valley is what makes the best answer easy to find.

### Visualization (canvas `c1`, 720×300)

Dual-panel line chart: the convex prep-cost bowl (left) vs a wavy non-convex cost curve (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One Valley vs Three: Convex Cost vs Wavy Cost".
- **Data (left, convex):** preps `[10, 20, 30, 40, 50, 60, 70]`, cost `[48, 33, 22, 16, 20, 29, 42]`.
- **Data (right, non-convex):** same preps, cost `[40, 18, 30, 12, 26, 15, 38]`.
- **Left panel:** axis origin x=55, width 280, baseline y=245, chart height 185, y scale $0–$55; blue `#2a78d6` 3px line with 4px dots; prep labels 12px `#444` below baseline; green bold 12px annotation "one valley: bottom at 40, $16" with a 6px green `#008300` dot on (40, 16); caption 12px `#444` "sandwiches prepped vs expected daily cost ($, illustrative)".
- **Right panel:** axis origin x=400, width 280, same baseline/height and y scale; orange `#d95926` 3px line with 4px dots; magenta `#d55181` bold 12px annotation "three valleys — which dip is the real bottom?" pointing at the dips at 20 ($18), 40 ($12), 60 ($15); caption "same axes, wavy cost".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Chord Test by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Pick two plans** — prep 20 (cost $33) and prep 60 (cost $29); draw the straight line between them
- **The chord** — halfway along, at prep 40, the straight line sits at ($33 + $29) / 2 = $31
- **The curve** — the actual cost at prep 40 is $16, which is $15 below the chord
- **The test** — convex means every such chord sits on or above the curve, for every pair of points
- **Formal line** — f(mid) ≤ average of f at the ends; blending two plans never beats the blend of costs
- **Wavy fails** — on the wavy curve, the chord from 20 ($18) to 60 ($15) dips below the humps at 30 and 50

*Example (italic):* Averaging the 20-prep and 60-prep plans lands you at 40 preps, where the true cost $16 beats the $31 the straight line predicted.

**Key point:** The formal definition is just the picture: a curve is convex when no chord ever ducks under it. Check it with two points and one midpoint, by hand.

### Visualization (canvas `c2`, 720×300)

Single-panel chart: the convex prep-cost bowl with one dashed chord from (20, 33) to (60, 29), highlighting the midpoint gap between chord and curve.

- **Title (bold 15px, `#1a5276`, top center):** "The Chord Test: the Straight Line Stays Above the Bowl".
- **Data:** preps `[10, 20, 30, 40, 50, 60, 70]`, cost `[48, 33, 22, 16, 20, 29, 42]`; chord endpoints (20, 33) and (60, 29); midpoint on chord (40, 31); midpoint on curve (40, 16).
- **Layout:** axis origin x=70, width 560, baseline y=245, chart height 185, y scale $0–$55; prep labels 12px `#444` below baseline; y-axis dollar ticks at 0, 20, 40 (12px `#6b7280`).
- **Curve:** blue `#2a78d6` 3px line with 4px dots.
- **Chord:** magenta `#d55181` dashed (dash 6/4) 2px line from (20, 33) to (60, 29); 5px magenta dots at both endpoints with bold 12px labels "$33" and "$29".
- **Midpoint gap:** violet `#4a3aa7` vertical 2px line at prep 40 from y(31) down to y(16), small arrowheads both ends; bold 13px violet label "chord $31 vs curve $16" to its right; 6px green `#008300` dot on (40, 16).
- **Takeaway (bold 13px green `#008300`, bottom center):** "convex: every chord sits on or above the curve".

## Why Downhill Is Enough

**Tags:** `where it's used` (blue), `local = global` (green), `failure mode` (red)

- **Downhill walker** — start anywhere on the bowl, always step toward lower cost: you reach $16 at 40
- **No trap** — on a convex curve, any point you can't improve locally is the global bottom
- **Wavy trap** — the wavy curve dips at 20 ($18) and 60 ($15); a walker from the left parks at $18
- **True bottom** — the wavy curve's real minimum is $12 at 40 — the parked walker never sees it
- **Thousands of valleys** — real ML losses live in high dimensions with countless dips, not three
- **Why we care** — gradient descent is exactly this walker; convexity is its guarantee of success

*Example (italic):* Two walkers dropped at preps 10 and 70 on the bowl both slide to the same $16 bottom; on the wavy curve they end up in two different dips.

**Key point:** Local = global is the payoff of convexity: cheap downhill steps find the best answer. Without it, where you start decides where you finish.

### Visualization (canvas `c3`, 720×300)

Dual-panel chart: the convex bowl with two downhill walkers converging (left) vs the wavy curve with a walker stuck in a side dip (right), divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Downhill from Anywhere: Guaranteed on a Bowl, a Gamble on Waves".
- **Data (left):** preps `[10, 20, 30, 40, 50, 60, 70]`, cost `[48, 33, 22, 16, 20, 29, 42]`; walker starts at (10, 48) and (70, 42).
- **Data (right):** same preps, cost `[40, 18, 30, 12, 26, 15, 38]`; stuck walker at (20, 18), true bottom at (40, 12).
- **Left panel:** axis origin x=55, width 280, baseline y=245, chart height 185, y scale $0–$55; blue `#2a78d6` 3px line with 4px dots; two aqua `#199e70` 6px walker dots at the start points with short 2px aqua arrows along the curve pointing toward (40, 16); 7px green `#008300` dot at (40, 16); green bold 12px annotation "both walkers reach $16"; caption 12px `#444` "convex: any start, same bottom".
- **Right panel:** axis origin x=400, width 280, same baseline/height and scale; orange `#d95926` 3px line with 4px dots; red `#e74c3c` 7px dot at (20, 18) with red bold 12px annotation, two lines: "stuck at $18," / "true bottom $12 unseen"; 6px green dot at (40, 12); caption "non-convex: start decides finish".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Where the Bowl Story Breaks

**Tags:** `common mistake` (red), `caveats` (orange)

- **No bottom** — convex does not promise a minimum exists: a cost sliding 46, 37, 30, ... 8 never lands
- **Flat shelf** — a flat-bottomed bowl is still convex, but a whole range of plans ties for best
- **Relaxation** — when the real curve is wavy, solvers often swap in the tightest bowl beneath it
- **Bound, not answer** — the relaxed bowl bottoms at $10 here: a floor on the true cost, sometimes exact
- **Everyday cases** — L1 in place of L0, hinge loss in place of 0-1 loss: both are convex relaxations

*Example (italic):* "Prep fewer, cost keeps falling" sounds convex and is — but if the curve never turns up, there is no best prep count to find.

**Common mistake:** Hearing "convex" as "one unique answer that exists". Convexity rules out fake valleys; it does not guarantee a bottom exists, nor that only one point sits there.

### Visualization (canvas `c4`, 720×300)

Three mini-panels in one canvas: a convex curve with no minimum (left), a flat-bottomed bowl (middle), and a wavy curve with its dashed convex relaxation underneath (right).

- **Title (bold 15px, `#1a5276`, top center):** "Three Caveats: No Bottom, Flat Shelf, Relaxed Bowl".
- **Panel A (no minimum):** axis origin x=45, width 190, baseline y=240, chart height 165, y scale 0–50; x steps `[0..8]`, cost `[46, 37, 30, 24, 19, 15, 12, 10, 8]`; blue `#2a78d6` 3px line, 3px dots; blue bold 12px annotation "keeps falling — no bottom to reach"; caption 12px `#444` "convex, minimum never attained".
- **Panel B (flat shelf):** axis origin x=280, width 190, same baseline/height and scale; x steps `[0..8]`, cost `[40, 26, 15, 10, 10, 10, 10, 16, 30]`; aqua `#199e70` 3px line, 3px dots; yellow `#c98500` bold 12px annotation "a whole shelf ties at $10" with a 2px yellow underline segment beneath the four flat points; caption "convex, many best answers".
- **Panel C (relaxation):** axis origin x=515, width 190, same baseline/height and scale; preps `[10, 20, 30, 40, 50, 60, 70]`, wavy cost `[38, 16, 28, 10, 24, 14, 36]` as orange `#d95926` 3px line with 3px dots; dashed (dash 5/4) violet `#4a3aa7` 2px relaxation line through hull points `(10, 38), (20, 16), (40, 10), (60, 14), (70, 36)`; violet bold 12px annotation "tightest bowl underneath: floor = $10"; caption "convex relaxation of a wavy cost".
- **Dividers:** dashed `#bdc3c7` (dash 4/3) vertical lines at x=258 and x=493 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data arrays are hardcoded literals — no randomness; invented dollar figures carry an "illustrative" caption label.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
