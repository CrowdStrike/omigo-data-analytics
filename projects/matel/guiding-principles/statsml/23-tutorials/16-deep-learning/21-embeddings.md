# Embeddings

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Embeddings

**Subtitle:** Turn words, users, or products into short lists of numbers so that similar things land close together — then "similar" becomes measurable

## Every Movie Becomes a Point on a Map

**Tags:** `core idea` (blue), `running example` (green), `vectors` (orange)

- **The task** — a streaming app must answer: which movies are like the one you just watched?
- **The trick** — give every movie two numbers, like coordinates on a map (illustrative)
- **Axis one** — how action-heavy, from 0 (romance) to 1 (explosions)
- **Axis two** — how serious, from 0 (light) to 1 (heavy drama)
- **The payoff** — similar movies land near each other; "similar" is now a distance
- **The name** — that list of numbers is the movie's embedding, or vector

*Example (italic):* "Robot War" at (0.9, 0.2) sits beside "Space Battle" at (0.8, 0.3) and far from "Summer Love" at (0.1, 0.9).

**Key point:** An embedding turns a thing into a point, so vague questions like "what's similar?" become distance measurements.

### Visualization (canvas `c1`, 720×300)

Scatter plot of seven movies on a 2D "movie map" with two dashed cluster halos.

- **Title (bold 15px `#1a5276`, top center):** "The Movie Map: Two Numbers per Movie (illustrative)".
- **Movie points (7px dots, name labeled above each in bold 12px of the dot color):**
  - Robot War (0.9, 0.2) — orange `#d95926`
  - Space Battle (0.8, 0.3) — orange `#d95926`
  - Laser Dawn (0.85, 0.45) — orange `#d95926`
  - Summer Love (0.1, 0.9) — magenta `#d55181`
  - Wedding Bells (0.15, 0.75) — magenta `#d55181`
  - Paris Letters (0.25, 0.85) — magenta `#d55181`
  - The Quiet Case (0.45, 0.55) — violet `#4a3aa7`
- **Axes:** L-shaped `#999` axes; padding top 50, bottom 56, left 70, right 40; x-axis label (12px `#444`, bottom center): "action-heavy →"; y-axis label (rotated −90°): "serious →".
- **Cluster halos:** dashed ellipses (dash 5/4, 1.5px) — orange around the sci-fi trio labeled "sci-fi corner" (bold 12px orange); magenta around the romance trio labeled "romance corner" (bold 12px magenta).
- **Top annotation (bold 12px violet `#4a3aa7`):** "neighbors on the map = movies fans of one tend to like the other".

## Measuring the Gap Between Two Movies by Hand

**Tags:** `worked example` (green), `by hand` (blue)

- **Two points** — Robot War (0.9, 0.2) and Space Battle (0.8, 0.3)
- **Differences** — 0.9 − 0.8 = 0.1 and 0.2 − 0.3 = −0.1
- **Distance** — √(0.1² + 0.1²) = √0.02 ≈ 0.14: close neighbors
- **Now the far pair** — Robot War vs Summer Love (0.1, 0.9): √(0.8² + 0.7²) = √1.13 ≈ 1.06
- **Recommend** — a Robot War fan gets Space Battle (0.14 away), not Summer Love (1.06 away)

*Example (italic):* School geometry — the straight-line distance formula — is the whole recommendation engine here.

**Key point:** Once things are points, "most similar" is literally "smallest distance" — a computation, not a judgment call.

### Visualization (canvas `c2`, 720×300)

Scatter with two distance lines drawn and labeled: the near pair and the far pair.

- **Title (bold 15px `#1a5276`, top center):** "Near Pair 0.14, Far Pair 1.06 — Straight-Line Distance".
- **Axes:** same L-shaped `#999` axes and padding as c1; x-axis label (12px `#444`): "action-heavy →".
- **Points (8px dots, bold 12px labels):** "Robot War (0.9, 0.2)" — orange `#d95926`; "Space Battle (0.8, 0.3)" — orange; "Summer Love (0.1, 0.9)" — magenta `#d55181`.
- **Near line:** solid green `#008300`, 3px, Robot War to Space Battle; label (bold 13px green): "√(0.1² + 0.1²) ≈ 0.14".
- **Far line:** dashed magenta `#d55181` (dash 6/4, 2px), Robot War to Summer Love; label (bold 13px magenta): "√(0.8² + 0.7²) ≈ 1.06".
- **Bottom annotation (bold 13px green, centered):** "recommendation = pick the smallest distance: Space Battle wins".

## The Same Trick Runs Words, Users, and Products

**Tags:** `where it's used` (blue), `representation` (green)

- **Words** — trained on text, "king" and "queen" land close; "king" and "carrot" far apart
- **Directions mean things** — king − man + woman lands nearest to queen (word2vec's famous demo)
- **Users too** — place viewers in the same space as movies; recommend the nearest movies
- **Search and dedup** — find lookalike products, near-duplicate photos, similar support tickets
- **Learned, not chosen** — real embeddings are trained from behavior, not typed in by hand

*Example (italic):* A viewer whose point drifts toward the sci-fi cluster starts seeing sci-fi on the home screen.

**Key point:** Anything with usage data can be embedded — one geometric space where similarity, search, and recommendation are all the same operation.

### Visualization (canvas `c3`, 720×300)

Schematic word-space diagram of the king/queen analogy with parallel direction arrows.

- **Title (bold 15px `#1a5276`, top center):** "Directions Carry Meaning: king − man + woman ≈ queen".
- **Word points (7px dots, bold 13px labels above):** king (190, 110) blue `#2a78d6`; queen (330, 82) violet `#4a3aa7`; man (210, 218) blue; woman (350, 190) violet.
- **Arrows:** two aqua `#199e70` 2.5px arrows with filled heads — man → woman and king → queen (nearly parallel).
- **Arrow caption (bold 12px aqua, centered, two lines):** "the two arrows are (nearly) the same arrow:" / "a "male → female" direction learned from text alone".
- **Contrast point:** carrot (600, 210), yellow `#c98500` dot, labeled "carrot" (bold 13px); below it (12px `#6b7280`): "unrelated words sit far away".
- **Side annotation (bold 13px magenta `#d55181`, two lines near top right):** "same geometry powers users," / "products, songs, photos".

## The Confusion: the Numbers Have No Names

**Tags:** `common mistake` (red), `interpretation` (orange)

- **The mistake** — asking "what does dimension 7 mean?" of a learned embedding
- **Our map lied a little** — "action" and "serious" axes were hand-picked to teach the idea
- **Real ones** — 100–1000 learned numbers per item; individual dimensions are not labeled
- **What is real** — distances and directions between points; those carry all the meaning
- **Practical rule** — compare embeddings to each other; never read one number in isolation

*Example (italic):* Dimension 7 of "queen" is 0.31 — alone it tells you nothing; "queen" sitting near "king" tells you plenty.

**Key point:** An embedding is meaningful only relative to other embeddings — the coordinates are unlabeled, the geometry is the message.

### Visualization (canvas `c4`, 720×300)

Split panel: labeled 2D teaching map on the left, an unlabeled 16-cell vector strip on the right; dashed vertical divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px `#1a5276`, top center):** "Teaching Map vs Real Embedding".
- **Left panel:** header (bold 13px `#2c3e50`): "our teaching map: 2 named axes". Small L-shaped `#999` axes (from (90,80) down to (90,200) across to (280,200)); one orange `#d95926` 7px dot at (240, 165) labeled "Robot War (0.9, 0.2)" (bold 12px); axis labels (12px `#444`): "action-heavy →" and rotated "serious →"; footer (bold 12px green `#008300`): "every number readable".
- **Right panel:** header (bold 13px `#2c3e50`): "real embedding of "queen": first 16 of 300 numbers". A 8×2 strip of 40×34 cells starting at (385, 84) with values `[0.31, -0.12, 0.87, -0.44, 0.05, 0.62, -0.71, 0.18, -0.29, 0.55, 0.09, -0.83, 0.41, -0.06, 0.73, -0.37]`; positive cells `rgba(42,120,214,a)`, negative cells `rgba(217,89,38,a)` with alpha = |v|×0.75 + 0.08; value printed in each cell (bold 11px, white when |v| > 0.55 else `#2c3e50`); dimension labels "d1"…"d16" below each cell (11px `#6b7280`).
- **Right-panel annotations:** bold 12px red `#e74c3c`: "no dimension has a name — values illustrative"; bold 12px violet `#4a3aa7`: "meaning lives in distances to other words, not in any cell".
- **Bottom annotation (bold 13px magenta `#d55181`, centered):** "compare embeddings to each other; never read one number alone".

## Regeneration instructions

- **Template/layout:** tutorials topic-page skeleton. `<h1>` (no index number) + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%), both 12px padding, top-aligned.
- **Left column structure:** `.tags` row of colored pill spans (`.tag` — 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) whose `<strong>` prefix is "Key point:".
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `movies` array of `{name, x, y, col}` (the seven points above) plus a `mapXY` coordinate mapper feeds c1 and c2. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions (this page has none).
