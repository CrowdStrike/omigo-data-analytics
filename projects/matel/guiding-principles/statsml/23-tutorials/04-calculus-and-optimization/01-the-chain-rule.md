# The Chain Rule

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Chain Rule

**Subtitle:** The end-to-end sensitivity of a pipeline is the product of each stage's local slope — this is the rule backpropagation mechanizes through a neural net

## One More Ad Dollar, Three Stages Later

**Tags:** `core idea` (blue), `pipeline` (green), `local slope` (orange)

- **The shop** — an online store buys ads: spend makes clicks, clicks make orders, orders make revenue
- **Local slope** — each stage has one number: 5 clicks per $1, 0.02 orders per click, $40 per order
- **The chain rule** — the end-to-end slope of the pipeline is the product of the local slopes
- **Multiply out** — 5 × 0.02 × 40 = 4, so each extra ad dollar returns about $4 of revenue
- **Units cancel** — clicks/$ times orders/click times $/order leaves plain $ out per $ in

*Example (italic):* Ask "what does one more ad dollar buy?" and the pipeline answers by multiplication: 5 × 0.02 × 40 = $4.

**Key point:** Sensitivity flows through a pipeline by multiplying, stage by stage. Know each local slope and the end-to-end slope is just their product — that is the chain rule.

### Visualization (canvas `c1`, 720×300)

Pipeline flow diagram: four stage boxes connected by labeled slope arrows, with the multiplied end-to-end slope bracketed underneath.

- **Title (bold 15px, `#1a5276`, top center):** "The Ad Pipeline: Local Slopes Multiply Into One End-to-End Slope".
- **Boxes:** four rounded rects 120×60 at x = 40, 215, 390, 565, all at y=95; fill `rgba(42,120,214,0.12)`, border 2px blue `#2a78d6`; bold 13px `#1a5276` centered labels "ad spend ($)", "clicks", "orders", "revenue ($)".
- **Arrows:** 3px `#6b7280` arrows between consecutive boxes at y=125; bold 13px slope labels above each arrow: "×5 clicks per $" (green `#008300`), "×0.02 orders per click" (orange `#d95926`), "×$40 per order" (violet `#4a3aa7`).
- **Bracket:** 2px `#008300` bracket from x=40 to x=685 at y=195 (10px drop ticks at both ends).
- **Result annotation (bold 14px green `#008300`, centered at y=230):** "end-to-end slope = 5 × 0.02 × 40 = $4 revenue per $1 of ads".
- **Caption (12px `#6b7280`, centered at y=270):** "each arrow carries one local slope; the chain rule multiplies them along the path".

## Nudging the Budget by $10

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The nudge** — raise the ad budget from $200 to $210 and watch the change ripple through
- **Stage 1** — +$10 spend × 5 clicks per $ = +50 clicks (1,000 → 1,050)
- **Stage 2** — +50 clicks × 0.02 orders per click = +1 order (20 → 21)
- **Stage 3** — +1 order × $40 per order = +$40 revenue ($800 → $840)
- **Check** — $40 gained / $10 nudged = 4, exactly the product 5 × 0.02 × 40
- **In symbols** — dR/dS = dR/dO × dO/dC × dC/dS; the derivatives chain like the stages do

*Example (italic):* Every base number is the previous stage times its slope: $200 spend → 1,000 clicks → 20 orders → $800 revenue.

**Key point:** You can verify the chain rule with arithmetic: push a small nudge in at the front, multiply by one local slope per stage, and the output change is the product of all of them.

### Visualization (canvas `c2`, 720×300)

Four mini bar-pair panels, one per stage, each showing the before/after value with its delta, joined by multiplier arrows.

- **Title (bold 15px, `#1a5276`, top center):** "A $10 Budget Nudge Ripples Through Every Stage".
- **Data:** stage headings "spend $", "clicks", "orders", "revenue $"; before `[200, 1000, 20, 800]`, after `[210, 1050, 21, 840]`, deltas "+$10", "+50", "+1", "+$40".
- **Panels:** four panels 140px wide at x = 40, 210, 380, 550; baseline y=235, chart height 145; each panel scaled to its own max × 1.15 (units differ per stage).
- **Bars:** two bars per panel, 44px wide with a 14px gap; before fill `rgba(42,120,214,0.45)`, after fill `rgba(0,131,0,0.5)`; value labels 12px `#444` above each bar; stage heading bold 12px `#1a5276` below baseline; delta bold 13px green `#008300` above the panel at y=52.
- **Multiplier arrows:** bold 13px orange `#d95926` labels "×5", "×0.02", "×$40" centered in the gaps between panels at y=150, each with a small 2px `#d95926` arrow underneath.
- **Takeaway (bold 13px `#1a5276`, bottom center at y=290):** "$40 out / $10 in = 4 = 5 × 0.02 × 40".

## Why Backprop Is Just This Rule

**Tags:** `where it's used` (blue), `backprop` (green), `failure mode` (red)

- **Backprop** — training a neural net is the chain rule run backwards, one local slope per layer
- **Deep pipelines** — a 50-layer net is this ad pipeline with 50 stages; the products get extreme
- **Vanishing** — ten stages of ×0.6 leave 0.6^10 ≈ 0.006; early layers barely feel the error
- **Exploding** — ten stages of ×1.5 give 1.5^10 ≈ 58; updates blow up instead of fading
- **Everywhere else** — elasticities, unit conversions, and error propagation chain the same way

*Example (italic):* The same multiplication that turned $10 into $40 turns a gradient into 0.6% of itself after ten timid layers.

**Key point:** Backprop is not a separate algorithm — it is bookkeeping for the chain rule. Long products of local slopes are also why deep nets suffer vanishing and exploding gradients.

### Visualization (canvas `c3`, 720×300)

Dual-panel bar chart: the running product of local slopes across 10 stages, for a per-stage slope of 0.6 (left) and 1.5 (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Ten-Stage Pipelines: Slopes of 0.6 Vanish, Slopes of 1.5 Explode".
- **Left panel (×0.6):** products `[0.6, 0.36, 0.216, 0.13, 0.078, 0.047, 0.028, 0.017, 0.01, 0.006]` for stages 1–10; axis origin x=55, width 280, baseline y=240, chart height 170, y scale 0–0.65; bars fill `rgba(42,120,214,0.45)`; stage numbers 1–10 in 11px `#444` below bars; magenta `#d55181` bold 12px annotation, two lines: "after 10 stages" / "only 0.6% left"; caption 12px `#444` "running product of ×0.6 slopes".
- **Right panel (×1.5):** products `[1.5, 2.25, 3.38, 5.06, 7.59, 11.39, 17.09, 25.63, 38.44, 57.67]`; axis origin x=400, width 280, same baseline/height, y scale 0–60; bars fill `rgba(217,89,38,0.5)`; orange `#d95926` bold 13px annotation "×58 after 10 stages"; caption "running product of ×1.5 slopes".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Adding Slopes Instead of Multiplying

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The trap** — adding the local slopes: 5 + 0.02 + 40 = 45.02, a nonsense number with mixed units
- **Why it fails** — clicks per $ and $ per order are different things; a sum of them means nothing
- **Multiplied truth** — 5 × 0.02 × 40 = 4, and the units cancel neatly into $ out per $ in
- **Weakest link** — drop the order rate to 0.001 and the chain gives 5 × 0.001 × 40 = 0.2
- **Reading rule** — a pipeline is only as sensitive as its product; one flat stage flattens everything

*Example (italic):* A team celebrated the "$40 per order" stage while conversion sat near zero — the end-to-end slope was 0.2, not 45.

**Common mistake:** Summing local slopes, or assuming the biggest one dominates. Slopes along a chain multiply, so one near-zero stage strangles the whole pipeline no matter how strong the others are.

### Visualization (canvas `c4`, 720×300)

Side-by-side comparison: the three local slopes combined by addition (left, struck out) vs by multiplication (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Add the Slopes vs Multiply the Slopes".
- **Slope tiles:** three rounded rects 72×34 per side, bold 13px centered labels "5", "0.02", "40".
- **Left panel (wrong):** heading bold 13px red `#e74c3c` "wrong: add" at y=60; tiles at x = 55, 155, 255, y=95, fill `rgba(213,81,129,0.12)`, border 2px magenta `#d55181`; bold 16px `#444` "+" between tiles; result bold 14px red `#e74c3c` "= 45.02 — units don't mix" at y=170 with a 2px red strike-through line across it.
- **Right panel (right):** heading bold 13px green `#008300` "right: multiply" at y=60; tiles at x = 400, 500, 600, y=95, fill `rgba(0,131,0,0.10)`, border 2px green `#008300`; bold 16px `#444` "×" between tiles; result bold 14px green `#008300` "= 4 ($ out per $ in)" at y=170.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to y=210.
- **Weak-link annotation (bold 13px magenta `#d55181`, centered at y=255):** "swap 0.02 for 0.001 and the product falls to 0.2 — one weak stage strangles the chain".
- **Caption (12px `#6b7280`, centered at y=285):** "slopes along a chain multiply; only the product has meaning".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
