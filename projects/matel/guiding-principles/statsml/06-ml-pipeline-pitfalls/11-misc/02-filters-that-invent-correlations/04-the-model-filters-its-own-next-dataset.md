# The Model Filters Its Own Next Dataset

**Page type:** detail page (three card-sections, each a two-column layout table: text left 50%, canvas right 50%)
**HTML title tag:** The Model Filters Its Own Next Dataset — Filters That Invent Correlations

**Subtitle:** Once a model decides who gets shown, approved, or reviewed, the next training set is a sample the model itself chose — and each retrain narrows it further.

## The Model Becomes the Door

**Tags:** `feedback loop` (red), `the mechanism` (blue)

- **Deployment is a filter** — the live model decides which rows are acted on at all
- **No action, no outcome** — only a shown item can be clicked, so unshown items have no label
- **Credit example** — only an approved application can repay or default; refusals stay outcome-free
- **Fraud example** — only a flagged transaction gets investigated, so only flagged rows get verdicts
- **The next training set** — it is the set of rows the previous model chose to let through
- **The gate tightens** — each retrain trusts the gate's own past preferences a little more
- **What makes it worse** — this filter is learned, not written, so no query records its condition
- **Rejects leave forever** — a row refused today is never labelled, and no later retrain recovers it

*Example:* A recommender shows the top-ranked items, the click log records only those items, and next month's model is fitted on that log — so it learns from a population its own ranking selected. (Illustrative Example.)

**Key idea:** The filter column here is the model's own score, and the score is a function of every feature — which makes deployment the single most powerful collider filter in the pipeline, and the least documented.

### Visualization (canvas `c1`, 720×340) — schematic, no data points, no numbers

A cycle diagram of the closed retraining loop, with a clearly marked dead-end for the rejected rows.

- **Seeded helper (present verbatim, never `Math.random()`; this canvas draws no data):**
  ```js
  // Seeded Park-Miller LCG — deterministic, never Math.random()
  function lcg(seed) {
      var s = seed;
      return function () { s = (s * 16807) % 2147483647; return s / 2147483647; };
  }
  ```
- **Title (bold 13px `#1a5276`, top center):** "The Deployed Model Chooses Its Own Next Training Set — Schematic, Not Data".
- **Cycle nodes:** four rounded rects 148×42 (radius 6), fill `rgba(26,82,118,0.10)`, stroke `#1a5276` width 1.5, bold 10px labels, placed clockwise around centre (300,190): "model scores every row" (top, 300/86), "gate admits high scorers" (right, 502/176), "admitted rows get labels" (bottom, 300/272), "labels become training set" (left, 98/176).
- **Cycle arrows:** solid `#1a5276` width 2 with filled arrowheads between consecutive nodes, closing the loop back into the top node; the closing arrow labelled bold 10px `#e67e22` "retrain sharpens the gate".
- **Gate node emphasis:** the "gate admits high scorers" rect gets a second stroke offset 4px outward, `#e67e22` width 1, and a 9px `#e67e22` note "threshold, versioned or not".
- **Dead-end branch:** from the gate node, a dashed `#e74c3c` width 2 arrow (dash 6/4) pointing right-down to a rect 150×40 at (600,268), fill `rgba(231,76,60,0.08)`, stroke `#e74c3c` width 2, bold 10px `#e74c3c` "rejected rows", 9px `#e74c3c` "never labelled".
- **Terminator:** a red `#e74c3c` double bar (two vertical strokes width 2.5) drawn just below the rejected-rows rect with a bold 9px `#e74c3c` label "out of the loop forever — no retrain recovers them".
- **Legend strip (9px `#666`, centered above the caption):** "solid = rows that stay in the loop   ·   dashed = rows that leave it".
- **Bottom caption (bold 10px `#1a5276`, centered):** "Every turn of the loop, the training population is a narrower slice of the world the model actually meets".

## The Feature It Trusted Most Goes Flat

**Tags:** `restriction of range` (red), `counter-intuitive` (blue)

- **The gate uses feature A** — high-A rows are admitted, low-A rows are refused and never labelled
- **A's range is squeezed** — survivors all sit in a narrow high band of A, round after round
- **Correlation needs spread** — with A's variance cut, its measured association with the outcome shrinks
- **Feature B is untouched** — the gate ignores B, so B keeps its full range and its full apparent signal
- **The false reading** — the next retrain sees "A does not matter" and may drop the feature that worked
- **The correct name** — restriction of range on the gated feature, a variance effect, not a collider
- **Equal truth** — in the seeded data A and B have exactly equal influence on the outcome
- **A collapses** — r(A, outcome) falls +0.180 → +0.111 → +0.069 → +0.006 as the gate tightens
- **B holds** — r(B, outcome) stays in the +0.174 to +0.220 band across the same four rounds
- **B's wobble is noise** — n falls 6000 → 898, so B's estimate jitters; sampling error, not an effect

*Example:* With 6000 seeded rows where A and B matter equally, gating at A > 0.85 leaves 898 rows in which r(A, outcome) = +0.006 while r(B, outcome) = +0.174 — A looks useless precisely because the gate worked. (Illustrative Example.)

**Key idea:** A gated feature's importance is guaranteed to decay in the model's own logs, so falling importance after deployment is evidence about the gate, not evidence about the feature.

### Visualization (canvas `c2`, 720×340) — Illustrative Example

A two-line chart of measured correlation with the outcome across four successive gating rounds, computed at render time from the seeded generator.

- **Data (seeded, never `Math.random()`):** `lcg(42)` generates 6000 rows in order `a = rng()`, `b = rng()`, then `outcome = (rng() < 0.15 + 0.7 * (0.5 * a + 0.5 * b)) ? 1 : 0` — so `a` and `b` carry identical true weight.
- **Rounds (each an increasing gate on `a`, all values computed in JS, never hardcoded):**
  - round 1, everyone, no gate → n = 6000, r(a) = +0.180, r(b) = +0.201
  - round 2, `a > 0.40` → n = 3508, r(a) = +0.111, r(b) = +0.220
  - round 3, `a > 0.70` → n = 1748, r(a) = +0.069, r(b) = +0.199
  - round 4, `a > 0.85` → n = 898, r(a) = +0.006, r(b) = +0.174
- **Title (bold 13px `#1a5276`, top center):** "Equal True Influence, Unequal Measured Signal — Illustrative Example".
- **Axes:** plot box (78,58) size 566×204, stroke `#ccc`. y-axis = measured r from +0.26 (top) to -0.02 (bottom), ticks every 0.04 with 9px `#666` labels; x-axis = four round labels, bold 10px `#1a5276` "round 1 / no gate", "round 2 / a > 0.40", "round 3 / a > 0.70", "round 4 / a > 0.85".
- **Zero line:** solid `#999` width 1 at r = 0 with a 9px `#666` "r = 0" label at the left.
- **Series A (gated feature):** polyline `#e74c3c` width 2.5 through the four computed r(a) values, markers 4.5px filled `#e74c3c`; labelled once bold 10px `#e74c3c` "feature A — the gated one" near the first marker.
- **Series B (ignored feature):** polyline `#27ae60` width 2.5 through the four computed r(b) values, markers 4.5px filled `#27ae60`; labelled once bold 10px `#27ae60` "feature B — the gate ignores it".
- **Point labels:** each marker annotated bold 9px in its series colour with the computed r to 3 decimals, A's labels below the marker and B's above so they never collide.
- **Secondary n axis:** 9px `#666` "n = " + computed surviving count printed under each x category, plus a bold 9px `#666` axis note "surviving rows" at the left of that row of text.
- **Shrink marker:** a dashed `#e67e22` width 1.5 bracket (dash 5/3) under the n row spanning round 1 to round 4, labelled bold 9px `#e67e22` "the training population shrinks as the gate tightens".
- **Bottom caption (bold 10px `#e74c3c`, centered):** computed at render time — "A and B matter equally in truth, yet r(A) falls " + r(a) round 1 + " → " + r(a) round 4 + " while r(B) holds near " + r(b) round 4.

## Keeping the Loop Open

**Tags:** `fixes` (green), `costed remedy` (orange)

- **Reserve a random slice** — admit a small random share of rows every period, bypassing the model's gate
- **Log the score everywhere** — record the score and the gate decision on every row, admitted or not
- **Logging is not a fix** — without unfiltered rows, no amount of logging restores the missing range
- **Distrust falling importance** — a feature whose importance drops after deployment is suspect, not settled
- **Measure on the slice** — estimate feature signal on the random admissions, never on the selected majority
- **Bounded exploration** — epsilon-greedy or Thompson sampling makes the exploration budget explicit
- **The cost is real** — random admissions accept known bad rows, so the budget must be chosen deliberately
- **Version the threshold** — a moving gate silently changes the training population between retrains

*Example:* Holding 2% of applications back for random approval costs 2% of the refusals' expected loss, and buys the only rows in which the gated feature still has full range. (Illustrative Example.)

**Key idea:** Exploration is the only remedy that recovers the lost variance — everything else is bookkeeping around a hole, and the exploration budget is a real, quantifiable expense that has to be planned.

### Visualization (canvas `c3`, 720×340) — schematic, no data points, no numbers

Two side-by-side funnel schematics: a closed loop that narrows to a sliver, and an open loop held wide by a random slice.

- **Title (bold 13px `#1a5276`, top center):** "Closed Loop vs Open Loop — Schematic, Not Data".
- **Left funnel, header bold 11px `#e74c3c` "CLOSED LOOP — gate feeds only itself":**
  - Four stacked horizontal bands at x = 46, y = 74/118/162/206, height 30, widths 280 / 176 / 100 / 40, each fill `rgba(231,76,60,0.14)`, stroke `#e74c3c` width 1.5.
  - Round labels 9px `#666` to the left of each band: "round 1" … "round 4"; the bands are left-aligned so the narrowing is visible on the right edge.
  - A dashed `#e74c3c` width 1.5 outline (dash 5/3) traces the lost width from band 1's right edge down to band 4's right edge, labelled bold 9px `#e74c3c` "range lost, unrecoverable".
  - Under the last band, bold 10px `#e74c3c` "feature A's range: a sliver".
- **Right funnel, header bold 11px `#27ae60` "OPEN LOOP — small random slice each round":**
  - Four bands at x = 394, same y positions and height, main widths 280 / 176 / 100 / 40 filled `rgba(39,174,96,0.14)` stroke `#27ae60` width 1.5, representing the model-selected majority.
  - Appended to the right of bands 2, 3 and 4, a small block width 24 height 30 filled `rgba(230,126,34,0.30)` stroke `#e67e22` width 1.5 — the random-admission slice.
  - A dashed `#e67e22` width 1.5 vertical guide (dash 5/3) through the right edge of every random block, showing the full range stays represented in each round, labelled bold 9px `#e67e22` "random slice keeps the full range".
  - Under the last band, bold 10px `#27ae60` "feature A's range: still measurable".
- **Legend strip (9px `#666`, centered above the caption):** "solid band = rows the model chose   ·   orange block = rows admitted at random".
- **Bottom caption (bold 10px `#1a5276`, centered):** "The random slice is small and it costs something — it is also the only part of the log where a gated feature can still be evaluated".

## Regeneration instructions

- **Layout:** three `.card-section` blocks (The Model Becomes the Door / The Feature It Trusted Most Goes Flat / Keeping the Loop Open), each an h2 with blue bottom border followed by a `table.layout` with one row: left `td.text-col` (50%) holding `.tags` pills, a `ul` of labeled bullets, an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one canvas.
- **Column split is 50/50 and fixed.** Shrink a canvas with `max-width` / `max-height` on the canvas itself, never by narrowing the viz cell.
- **Bullets must not wrap:** each bullet is a bold `#1a5276` label plus a short phrase fitting one line at normal page width (~90–100 characters total), 5–8 bullets per section unless a fact forces one more. A fact that does not fit becomes another bullet; nothing is deleted.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `ul` 0.92rem; `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `strong` in `#1a5276`. `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×340 for `c1`, `c2` and `c3`, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Every printed statistic is computed at render time** by a shared `corr(a, b)` helper over the seeded arrays. No correlation or surviving count is hardcoded anywhere in the page.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links, no cross-references to other pages.
