# The Bias-Variance Tradeoff

**Page type:** detail page (tutorial: 4 card-sections, each h2 + two-column layout table, text left 50% / canvas right 50%)
**HTML title tag:** The Bias-Variance Tradeoff

**Subtitle:** A dart thrower can miss by aiming crooked (bias) or by wobbling (variance) — models miss the same two ways, and fixing one usually feeds the other

## Four Dart Throwers, Two Kinds of Miss

**Tags:** `core idea` (blue), `running example` (green)

- **Bias** — the aim is crooked: throws land off-center in the same direction, every time
- **Variance** — the arm wobbles: throws scatter widely around wherever the aim points
- **Best thrower** — aim centered and hand steady: tight cluster on the bull
- **Worst thrower** — crooked aim and shaky hand at once
- **Model version** — a rigid model aims crooked; a flexible one wobbles with its training data

*Example:* Five throws that all land 4 cm left are not unlucky — the sight is bent; scattered throws are a shaky hand.

**Key point:** **Two different diseases:** bias is being wrong the same way every time; variance is being wrong a different way every time. A single throw cannot tell you which one you have.

### Visualization (canvas `c1`, 720×300)

The classic 2×2 of bias and variance: four dartboards side by side, five darts each.

- **Title (bold 16px, `#1a5276`, top center):** "Same Bull, Four Throwers (5 darts each)".
- **Boards:** four boards centered at x = 105, 275, 445, 615, cy=138, outer radius 55; each has 3 concentric rings (alternating fill `#f2f5f8` / `#fff`, stroke `#b9c4cd`) and an ink `#1a5276` bull dot radius 4.
- **Darts (radius 4.5, offsets from center in px):**
  - Board 1 — green `#008300`, "low bias, low variance" / "steady and true": `[[2,-3], [-4,2], [3,4], [-2,-5], [5,1]]`.
  - Board 2 — orange `#d95926`, "high bias, low variance" / "steady but crooked": `[[-27,-24], [-22,-19], [-25,-26], [-29,-20], [-23,-23]]`.
  - Board 3 — violet `#4a3aa7`, "low bias, high variance" / "true on average, wild": `[[-30,18], [25,-28], [-12,-33], [33,22], [5,36]]`.
  - Board 4 — magenta `#d55181`, "high bias, high variance" / "crooked AND wild": `[[-44,-28], [-8,-38], [-36,5], [-2,-8], [-26,-36]]`.
- **Labels:** first label line bold 12px in the board's color, second line gray `#6b7280` 12px, both centered below each board.
- **Annotation (magenta bold 13px, bottom center):** "bias = where the cluster sits; variance = how wide the cluster spreads".

## Scoring Three Throwers by Hand

**Tags:** `worked example` (green), `arithmetic` (blue)

- **Thrower A, steady but crooked** — lands on average 4 cm left, wobble only 1 cm
- **Thrower B, centered but wild** — average dead on the bull, wobble 5 cm
- **Thrower C, a little of both** — 2 cm off on average, 2 cm of wobble
- **The scoring rule** — typical squared miss = bias² + wobble²
- **Totals** — A: 16 + 1 = 17, B: 0 + 25 = 25, C: 4 + 4 = 8 — C wins

*Example:* B "aims true" and still loses to C — perfect average aim is worthless if every single throw is wild.

**Key point:** **Hand-checkable:** error splits into two squared pieces that simply add. Minimizing the total, not either piece alone, is the whole game.

### Visualization (canvas `c2`, 720×300)

Stacked bar chart: bias² + wobble² for throwers A, B, C.

- **Title (bold 16px, `#1a5276`, top center):** "Typical Squared Miss = Bias² + Wobble² (cm²)".
- **Bars:** three stacks, labels "A: steady, crooked", "B: centered, wild", "C: a little of both"; bias² segments `[16, 0, 4]` in orange `#d95926` (alpha 0.55, 2px stroke, skipped when 0), wobble² segments `[1, 25, 4]` stacked on top in violet `#4a3aa7` (same style); bar width 120, gap 70, first bar x=115.
- **Axes:** y from 0 to 28 with labels 0–25 every 5 (gray `#6b7280` 12px); baseline y=235, chart height 170, left pad 70; L-shaped `#999` axis.
- **Segment labels (white bold 12px, centered in segments):** "bias² 16" / "bias² 4" (only where >0) and "wobble² 25" / "wobble² 4"; segments too short for an inside label (<16px, i.e. "wobble² 1") get the label beside the bar in the segment color (violet) instead.
- **Totals (bold 14px above each stack):** "total 17", "total 25", "total 8" — C's total in green `#008300`, others in text `#2c3e50`; thrower labels 12px below baseline.
- **Annotation (green bold 13px, upper right):** "C wins with 8 — balanced beats both purists".

## The Complexity Slider and the U-Shaped Curve

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Slide toward simple** — a rigid model: crooked aim (high bias), barely any wobble
- **Slide toward complex** — a flexible model: aim straightens, wobble explodes
- **Bias² falls, variance rises** — the two curves move in opposite directions
- **Test error is their sum** — so it traces a U: here lowest at complexity 5
- **The tradeoff** — past the bottom of the U, every gain in aim costs more in wobble

*Example:* Moving from complexity 5 to 8 cuts bias² from 6 to 2.5 but grows variance from 6 to 18 — a bad trade.

**Key point:** **Rule of thumb:** tuning a model is choosing a spot on this slider. The best spot is never zero bias or zero variance — it is the bottom of the U.

### Visualization (canvas `c3`, 720×300)

Three-line chart: bias² falls, variance rises, their sum traces a U with a marked minimum.

- **Title (bold 16px, `#1a5276`, top center):** "Slide the Complexity Dial (illustrative error units)".
- **Data (complexity 1–10):**
  - bias²: `[36, 25, 16, 9, 6, 4, 3, 2.5, 2, 1.8]` — orange `#d95926`, dashed 6/4, width 3.
  - variance: `[1, 1.5, 2.5, 4, 6, 9, 13, 18, 24, 31]` — violet `#4a3aa7`, dashed 6/4, width 3.
  - total (sum): `[37, 26.5, 18.5, 13, 12, 13, 16, 20.5, 26, 32.8]` — magenta `#d55181`, solid, width 3; minimum at complexity 5.
- **Axes:** y from 0 to 40, labels every 10 (gray 12px); x ticks 1–10; padding top 52, bottom 55, left 70, right 170; L-shaped `#999` axis.
- **Minimum marker:** green `#008300` dot radius 7 at (complexity 5, total 12) with dashed green drop-line (5/4, width 1.5) to the x-axis; bold 13px green label "bottom of the U: complexity 5, total 12" to the right below the point.
- **Legend (right side, x=w−158):** orange swatch "bias² (crooked aim)"; violet swatch "variance (wobble)"; magenta swatch "total test error" (12px).
- **X-axis caption (gray 12px, bottom center):** "model complexity (1 = rigid, 10 = very flexible)".

## The Confusion: More Data Fixes Only One of Them

**Tags:** `common mistake` (red), `caution` (orange)

- **More data calms wobble** — variance shrinks as training rows grow
- **More data cannot unbend aim** — a too-simple model stays crooked at any size
- **Flexible + big data** — the wobbly model drops from 30% to 10% error with more rows
- **Rigid + big data** — the simple model flatlines near 20% error forever
- **So diagnose first** — buying data for a bias problem buys nothing

*Example:* A thousand practice throws steady a shaky hand; they do nothing for a bent sight.

**Key point:** **The confusion:** bias here is not "biased data" and variance is not "the data varies" — both describe the model's errors, not the dataset.

### Visualization (canvas `c4`, 720×300)

Two-line learning-curve chart: test error vs training rows for a flexible and a rigid model.

- **Title (bold 16px, `#1a5276`, top center):** "What More Training Rows Buy (illustrative)".
- **Data (x labels "100", "200", "400", "800", "1,600", "3,200"):**
  - flexible (high variance): `[30, 24, 19, 15, 12, 10]` — violet `#4a3aa7`, solid, width 3, dots radius 4.
  - rigid (high bias): `[21, 20.5, 20, 20, 20, 20]` — orange `#d95926`, solid, width 3, dots radius 4.
- **Axes:** y from 0 to 35, labels 0–30% every 10 (gray 12px); padding top 52, bottom 55, left 70, right 185; L-shaped `#999` axis.
- **Annotations (bold 13px):** orange "rigid model: stuck near 20% — bias floor" above the flat line; violet "flexible model: 30% → 10%" below the falling line.
- **Legend (right side, x=w−172):** violet swatch "wobbly (high variance)"; orange swatch "crooked (high bias)" (12px).
- **Headline (magenta `#d55181` bold 13px, centered near top of plot):** "practice tames wobble; it cannot unbend a crooked aim".
- **X-axis caption (gray 12px, bottom center):** "training rows".

## Regeneration instructions

- **Template:** tutorials topic page (per `tutorials/CLAUDE.md`, social-graph reference skeleton). `<h1>` (no index number), `.subtitle` line, then 4 `.card-section` blocks, each an `<h2>` with 2px `#2980b9` bottom border plus a `table.layout` row: left `td.text-col` (50%) with `.tags` pills, 5 one-line bullets (each opening with `<b>` in `#1a5276`), an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) with one canvas 720×300.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; h2 1.3rem `#1a5276`. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic `#555` 0.9rem. ul 0.92rem. Canvas: `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas scaling:** shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; intrinsic width/height attributes as given per chart. All data arrays hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links would use `.html` extensions.
