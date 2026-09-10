# Pinterest Visual Search — The Query Is a Rectangle, Not a String

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + table per aspect, plus philosophy callouts and a summary table)
**HTML title tag:** Pinterest Visual Search — The Query Is a Rectangle, Not a String

**Subtitle:** A user drags a box around one striped chair in a room photo and expects the right products back. Treat the crop as the query and every comfortable assumption from text search breaks.

## Callout (philosophy box, top)

**The question:** In text search you can read the query, log it, cluster it, autocomplete it, and correct its spelling. What is left of that toolkit when the query is a rectangle of pixels?

**The answer:** Almost none of it. A crop is a point in a continuous embedding space, not a token from a finite vocabulary — so there is no query log to mine, no misspelling to fix, and no way to ask the user which of four plausible intents they meant. The system must guess intent, cannot measure whether it guessed right, and then trains tomorrow's model on the clicks its own guess produced.

## 1. A Query With No Vocabulary

**Obj-title:** Discrete Tokens vs a Continuous Point

Text queries repeat. Ten strings can cover a quarter of all traffic, which is why query logs, autocomplete, and spelling correction all work — they exploit the fact that the query space is discrete, finite, and re-visited. Crops almost never repeat exactly. Two users boxing the same chair produce two different rectangles, two different pixel sets, and two nearby-but-distinct vectors.

Math-box:

**Head concentration, same catalog, two query types (Illustrative Example):**

Top 10 text queries, out of `10,000` searches: `2,510` searches → `25.1%` of traffic
Top 10 crop clusters, out of `10,000` crops: `305` crops → `3.05%` of traffic

Same total volume, same catalog. The text head is `8.2x` heavier. With no head, there is nothing to precompute, nothing to hand-curate, and no popular-query safety net to fall back on.

- **No query log:** there is no table of "what words did users type" — a crop is a vector, and vectors do not group into inspectable strings
- **No spelling correction:** a bad text query has a nearby valid string; a bad crop has no notion of "invalid", so it silently returns wrong results
- **No autocomplete:** you cannot suggest a completion for a rectangle, so intent can never be refined before retrieval runs
- **No exact-match cache:** a text head lets you serve the top queries from a precomputed list; every crop is effectively a cold query
- **Debugging loses its handle:** "search quality for query X regressed" has no analogue when X is unique to one user, one photo, one drag
- **Curation does not scale:** editors can hand-tune 500 head strings; they cannot hand-tune a continuous space

### Visualization (canvas `canvas1`, 720x360)

Grouped bar chart: share of traffic held by rank-1..rank-10 queries, text vs crop, with both head totals computed at render time.

- **Layout:** origin at (70, 300), plot width 600, plot height 240. Axes `#1a5276`, width 2. No random data — both series are literal arrays.
- **Data (literal counts out of 10,000):** text = `[620, 410, 330, 260, 215, 180, 150, 130, 115, 100]`; crop = `[48, 41, 37, 33, 30, 28, 25, 23, 21, 19]`.
- **Scale:** value axis 0 to 7% of traffic (i.e. count/10000 up to 0.07), mapped to plot height. Gridlines at 0, 1, 2, 3, 4, 5, 6, 7% in `#eee`, labels `#666` 11px with a `%` suffix.
- **Bars:** 10 rank slots; per slot two bars, width 18, gap 4. Text bars fill `rgba(26,82,118,0.35)` with stroke `#1a5276`; crop bars fill `#e67e22`.
- **X ticks:** rank labels 1..10 in `#666` 11px. X axis label: "Query rank (most frequent first)"; y (rotated): "Share of 10,000 searches" — both `#1a5276`, 13px.
- **Computed labels (bold 12px):** sum each array in JS, divide by 10000, and print `"top-10 text queries = " + pct + "% of traffic"` in `#1a5276` and `"top-10 crop clusters = " + pct + "% of traffic"` in `#e67e22`, stacked in the upper-right of the plot. Also print the ratio `(textSum/cropSum).toFixed(1) + "x heavier head"` in `#e74c3c` 12px below them. All three strings are derived from the plotted arrays, never hardcoded.
- **Note (11px `#999`, bottom-left inside plot):** "Illustrative Example — same catalog, same volume".
- **Title (bold 14px `#1a5276`, top center):** "A Text Head Exists. A Crop Head Barely Does."

## 2. The Rectangle Is Ambiguous — and You Cannot Ask

**Obj-title:** Intent Underdetermined by Pixels

Alice boxes a striped chair. She might mean *this chair*, *striped upholstery fabric*, *mid-century furniture*, or *this blue-and-cream palette*. All four readings are faithful to the same pixels. A text searcher facing the same ambiguity types another word; a cropper has no second word to type, and the interface has no slot to put it in.

Math-box:

**Rater study on one crop set (Illustrative Example):** `40` crops, `5` raters each, question: "is this result visually similar?"

Votes-for-similar per crop: `0`→6 crops, `1`→7, `2`→9, `3`→8, `4`→6, `5`→4
Unanimous crops (all 5 agree either way): `10 of 40` = `25%`
Fleiss' kappa across the 40 crops: `0.23` — "fair" agreement, barely above chance

If five trained humans cannot agree on three quarters of the cases, the disagreement is not rater sloppiness. It is the intent being genuinely absent from the input.

- **Four faithful readings:** object identity, material or pattern, design era, and color palette all sit inside one rectangle
- **Scale is intent:** a tight crop hints at the object, a loose crop hints at the scene — but users crop imprecisely, so the hint is weak
- **No disambiguation turn:** text search can offer "did you mean"; there is no rectangle equivalent, so the system must commit
- **Context is discarded:** the surrounding room often reveals intent, yet the query pipeline usually sees only the box
- **The label is a coin flip:** with kappa near 0.23, a single rater's "relevant" carries little information about the next rater
- **Averaging hides the split:** 50% mean relevance can mean "half-good for everyone" or "perfect for half the users"

### Visualization (canvas `canvas2`, 720x360)

Histogram of rater agreement: 6 bars for 0..5 votes-for-similar, with Fleiss' kappa and the unanimous share computed from the plotted bar heights.

- **Layout:** origin at (80, 300), plot width 560, plot height 240. Axes `#1a5276`, width 2. Literal data, no PRNG.
- **Data (literal, crops per vote-count):** `[6, 7, 9, 8, 6, 4]` for vote counts 0,1,2,3,4,5. Total 40 crops.
- **Scale:** 0 to 10 crops, gridlines every 2 in `#eee`, labels `#666` 11px.
- **Bars:** width 56. The two unanimous bars (0 votes and 5 votes) fill `#27ae60`; the four split bars fill `rgba(26,82,118,0.35)` with stroke `#1a5276`. Bar value printed above each bar in `#333` bold 11px.
- **X ticks:** "0/5", "1/5", "2/5", "3/5", "4/5", "5/5" in `#666` 11px. X label: "Raters calling the result similar"; y (rotated): "Crops (of 40)" — `#1a5276`, 13px.
- **Computed statistics (bold 12px, upper right of plot):** in JS, from the plotted array only — total N, unanimous share `(bars[0]+bars[5])/N` printed as a percentage in `#27ae60`, and Fleiss' kappa computed with `p = sum(k*bars[k]) / (5*N)`, `Pbar = mean over crops of (k^2 + (5-k)^2 - 5) / (5*4)`, `Pe = p^2 + (1-p)^2`, `kappa = (Pbar-Pe)/(1-Pe)` printed to 2 decimals in `#e74c3c`. Expected values: unanimous `25%`, kappa `0.23`.
- **Bracket annotation:** gray `#999` horizontal bracket spanning the four middle bars just above them, labeled "no majority truth here" in `#999` 11px centered.
- **Note (11px `#999`, bottom-left inside plot):** "Illustrative Example".
- **Title (bold 14px `#1a5276`, top center):** "Five Raters, One Crop, No Ground Truth".

## 3. Two Embeddings, Both Correct, Completely Disagreeing

**Obj-title:** Style Space vs Object Space

Train one encoder to pull together images of the same product and it learns object identity. Train another on pattern and color statistics and it learns style. Query both with the same crop of a striped chair: the object space returns the same chair in plain and floral fabric; the style space returns a striped sofa, a striped lamp, and a striped table. Neither is wrong. Neither can be preferred without knowing intent.

Math-box:

**One query, two spaces, 10 catalog items (Illustrative Example):**

Object-space top 4: `chair/plain`, `chair/floral`, `chair/plain-2`, `chair/striped` — all chairs, 3 of 4 not striped
Style-space top 4: `sofa/striped`, `table/striped`, `chair/striped`, `lamp/striped` — all striped, 3 of 4 not chairs

Overlap between the two top-4 lists: `1 item` (`chair/striped`). The single item both spaces agree on is the only one that matches on *both* axes — and it is ranked 4th in one list and 3rd in the other, so a top-3 cutoff drops it from one side.

- **Disagreement is structural:** the two spaces optimize different invariances, so their neighbor sets can share almost nothing
- **A blend hides both:** averaging the two embeddings returns items that are mediocre on each axis rather than strong on one
- **Ranking cannot resolve it:** no reranker fixes a retrieval set that answered a different question than the user asked
- **Slot diversification is the honest fallback:** show results from both spaces and let the click reveal the intent
- **But diversification costs the top slot:** every hedged position is one fewer chance to nail the actual intent
- **Metric choice leaks in too:** cosine on a style vector is dominated by texture energy, which quietly favors busy patterns

### Visualization (canvas `canvas3`, 720x360)

Two side-by-side 2-D scatter panels — object space (left) and style space (right) — same 10 catalog items placed at different coordinates, query point marked, top-4 neighbors circled. All distances computed from the plotted coordinates.

- **Layout:** two panels, each 290 wide x 230 tall; left panel origin (60, 300), right panel origin (390, 300). Thin `#e0e0e0` frame per panel. Literal coordinate arrays, no PRNG.
- **Query point (both panels):** unit-square coordinate `(0.20, 0.50)`, drawn as a `#e74c3c` filled square of side 10 with label "crop" in `#e74c3c` bold 11px.
- **Data (unit-square coords per item; `[objX, objY, styleX, styleY]`):**
  - `chair/plain` — `[0.26, 0.44, 0.60, 0.46]`
  - `chair/floral` — `[0.24, 0.58, 0.88, 0.52]`
  - `chair/striped` — `[0.31, 0.52, 0.25, 0.44]`
  - `sofa/striped` — `[0.58, 0.47, 0.23, 0.56]`
  - `sofa/plain` — `[0.62, 0.55, 0.64, 0.52]`
  - `lamp/striped` — `[0.90, 0.50, 0.18, 0.60]`
  - `lamp/plain` — `[0.93, 0.42, 0.58, 0.58]`
  - `chair/plain-2` — `[0.18, 0.40, 0.66, 0.40]`
  - `sofa/floral` — `[0.55, 0.60, 0.92, 0.44]`
  - `table/striped` — `[0.75, 0.62, 0.16, 0.44]`
- **Points:** radius 5. Striped items fill `#e67e22`; non-striped fill `rgba(26,82,118,0.35)` with stroke `#1a5276`. Short 9px `#666` labels beside each point (abbreviate, e.g. "chair/str").
- **Top-4 rings:** in JS compute Euclidean distance from the query to every item in that panel's space, sort, and draw a `#27ae60` ring (radius 10, width 2) around the four nearest. Do not hardcode which four.
- **Panel captions (bold 12px):** left `"object space — top 4 are all chairs"` in `#1a5276`; right `"style space — top 4 are all striped"` in `#e67e22`.
- **Computed labels (11px, under each panel):** print `"mean top-4 distance = " + d.toFixed(3)` from the plotted coordinates. Expected: object `0.097`, style `0.080`.
- **Overlap label (bold 12px `#e74c3c`, centered between the panels near the bottom):** compute the set intersection of the two top-4 lists in JS and print `"lists overlap on " + n + " item"` — expected `1`.
- **Note (11px `#999`, bottom-left):** "Illustrative Example — 2-D stand-in for a high-dimensional space".
- **Title (bold 14px `#1a5276`, top center):** "The Same Crop, Two Honest Answers".

## 4. The Closed Loop: What Gets Shown Shapes What Gets Cropped

**Obj-title:** The Sampling Distribution Is Model Output

This is the statistical trap. In ordinary supervised learning the training distribution is fixed by the world and the model is fitted to it. Here the model's output *is* the mechanism that generates next week's inputs. Items the model retrieves well get impressions; impressions get clicks; clicks become positive pairs; the next model retrieves those items even better. Items the model retrieves poorly are never shown, so they never earn a click, so they never enter training — their absence looks like irrelevance rather than like censoring.

Math-box:

**A minimal closed loop (Illustrative Example).** 10 item classes, each starting with an equal `10%` share of impressions and a fixed *true* match quality `q` from `0.90` down to `0.22`. Two deterministic update rules, iterated 12 times:

`share_i <- share_i * (1 + (q_i - qbar))`, renormalized — better-matching classes win impressions
`q_i <- q_i + 0.8 * (share_i - 0.10)`, clipped to `[0.05, 0.98]` — training on your own impressions improves what you already show and lets the rest rot

Offline metric measured on logged traffic (impression-weighted): `0.562` → `0.972`
The same metric measured uniformly over all 10 classes: `0.562` → `0.395`
Impression concentration (Herfindahl index): `0.100` → `0.352`

Both series start at the identical value because impressions start uniform. Twelve generations later the dashboard reports a `+73%` improvement while true coverage has fallen `30%`. Nothing in the logs can distinguish these two worlds — the logs *are* the thing that narrowed.

- **The metric and the harm move oppositely:** logged similarity rises exactly because the denominator shrank to the easy cases
- **Censoring masquerades as irrelevance:** an item never retrieved has no clicks, which the next training round reads as a negative
- **Crops narrow too, not just clicks:** users crop what the interface rewards, so even the query distribution collapses toward the model
- **Concentration is the leading indicator:** the Herfindahl index rises before any quality metric turns, so watch it directly
- **Random exploration is the only real fix:** hold out a slice of slots for uniformly sampled candidates and log those separately
- **Propensity weighting needs the propensity:** you can only debias logged clicks if the serving policy's probabilities were recorded
- **A held-out set from logs is not held out:** it inherits the same censoring, so it certifies the loop instead of catching it

### Visualization (canvas `canvas4`, 720x360)

Line chart over 12 loop generations: logged (impression-weighted) similarity rising, true uniform similarity falling, and impression concentration rising on a right-hand axis. Every series is produced by running the deterministic recursion in JS — no PRNG, no hardcoded curve values.

- **Layout:** origin at (70, 300), plot width 570, plot height 240. Left axis and x axis `#1a5276`, width 2; right axis `#e67e22`, width 2 at x = ox + gw.
- **Simulation (in JS, deterministic):** `q = [0.90, 0.82, 0.75, 0.68, 0.60, 0.52, 0.45, 0.38, 0.30, 0.22]`, `share = [0.1 x 10]`. For generations 0..12, record `logged = sum(share_i * q_i)`, `uniform = mean(q)`, `hhi = sum(share_i^2)`, then apply the two update rules from the math-box in that order (share first, using the current `logged` as `qbar`; then q, using the *new* share).
- **Left scale:** 0.30 to 1.00 for the two similarity series. Gridlines at 0.3, 0.4, ... 1.0 in `#eee`, labels `#666` 11px.
- **Right scale:** 0.00 to 0.40 for the Herfindahl index, ticks at 0.0, 0.1, 0.2, 0.3, 0.4 in `#e67e22` 11px.
- **Series:** logged similarity `#e74c3c` width 2.5, with filled dots radius 3; uniform similarity `#27ae60` width 2.5 with dots radius 3; Herfindahl `#e67e22` width 2 dashed (6/4), plotted on the right scale.
- **X ticks:** 0, 2, 4, 6, 8, 10, 12 in `#666` 11px. X label: "Loop generations (retrain on your own impressions)"; left y (rotated): "Mean match quality" — `#1a5276`, 13px.
- **Divergence shading:** fill `rgba(231,76,60,0.10)` between the logged and uniform curves for every generation where they differ.
- **Computed end labels (bold 11px, at the right end of each series, from the simulated arrays):** `"logged: " + v.toFixed(3)` in `#e74c3c`, `"true (uniform): " + v.toFixed(3)` in `#27ae60`, `"HHI: " + v.toFixed(3)` in `#e67e22`. Expected `0.972`, `0.395`, `0.352`.
- **Computed callout (bold 12px, centered in the shaded region):** compute both percentage changes from generation 0 and print two lines — `"dashboard: +" + up + "%"` in `#e74c3c` and `"reality: -" + down + "%"` in `#27ae60`. Expected `+73%` and `-30%`.
- **Start marker:** dashed `#999` vertical line at generation 0 with `#666` 11px label "both metrics identical here" placed above the plot.
- **Note (11px `#999`, bottom-left inside plot):** "Illustrative Example — deterministic recursion, no random data".
- **Title (bold 14px `#1a5276`, top center):** "The Metric Improves Because the Test Set Narrowed".

## 5. Some Missing Results Are an Index Artifact, Not a Model Bug

**Obj-title:** Approximate Nearest Neighbor Is a Correctness Decision

Exact nearest-neighbor search over a large catalog is too slow to serve, so production uses an approximate index that probes only some partitions of the space. Whatever lives in an unprobed partition is invisible regardless of how good the embedding is. That makes the index a source of *ranking errors* — not just of latency — and the two failure modes look identical from the outside.

Math-box:

**Recall at 10 vs partitions probed (Illustrative Example).** `10` held-out crops; for each, the exact top-10 neighbors are known by brute force. Hits recovered by the approximate index, summed over all 10 crops (max `100`):

`1 probe`: `61` hits → recall@10 = `0.61`, p50 latency `3.1 ms`
`4 probes`: `81` hits → `0.81`, `6.4 ms`
`16 probes`: `95` hits → `0.95`, `18.0 ms`
`64 probes`: `99` hits → `0.99`, `61.0 ms`

At the 16-probe setting, `5 of 100` correct neighbors are absent from the response. A rater marking those queries "bad results" is measuring the index, not the embedding — and no amount of encoder retraining will move them.

- **Recall loss is not uniform:** it concentrates on items near partition boundaries, so specific product types silently under-retrieve
- **It feeds the closed loop above:** index-invisible items get no impressions, so the loop treats them as unpopular
- **Diminishing returns are steep:** the last 4 recall points cost `43 ms` more, ~`14x` the `3.1 ms` that bought the first 61
- **Rebuilds shift the boundaries:** re-clustering the index moves which items are missable, so quality jumps without a model change
- **Ablate the index before the model:** re-run a failing query set with brute-force search to see which failures survive
- **Report both numbers together:** a recall figure without its latency budget is not a comparable measurement

### Visualization (canvas `canvas5`, 720x360)

Bar chart of recall@10 by probe count with a latency line on a right-hand axis; recall bars are computed at render time by summing the literal per-query hit arrays.

- **Layout:** origin at (75, 295), plot width 545, plot height 235. Left/x axes `#1a5276`, width 2; right axis `#e67e22`, width 2.
- **Data (literal per-crop hit counts, out of 10 each, 10 crops per setting):**
  - 1 probe: `[7, 5, 6, 4, 8, 6, 5, 7, 6, 7]`
  - 4 probes: `[9, 8, 8, 7, 9, 8, 7, 9, 8, 8]`
  - 16 probes: `[10, 9, 10, 9, 10, 9, 9, 10, 10, 9]`
  - 64 probes: `[10, 10, 10, 10, 10, 9, 10, 10, 10, 10]`
  - p50 latency (ms), literal: `[3.1, 6.4, 18.0, 61.0]`
- **Recall:** computed in JS as `sum(hits) / 100` per setting. Expected `0.61`, `0.81`, `0.95`, `0.99` — these values are printed above the bars to 2 decimals from the computation, never hardcoded.
- **Bars:** 4 bars, width 62, fill `rgba(26,82,118,0.35)` with stroke `#1a5276`, on the left scale 0 to 1.0 (gridlines every 0.2 in `#eee`, labels `#666` 11px).
- **Latency line:** `#e67e22` width 2.5 with filled dots radius 4, on a right scale 0 to 70 ms (ticks 0, 20, 40, 60 in `#e67e22` 11px). Latency value printed beside each dot in `#e67e22` 11px as `"3.1 ms"` etc.
- **X ticks:** "1 probe", "4", "16", "64" in `#666` 11px. X label: "Index partitions probed per query"; left y (rotated): "Recall @ 10 vs brute force" — `#1a5276`, 13px.
- **Missing-results annotation:** at the 16-probe bar, draw a `#e74c3c` bracket from the bar top to the 1.0 gridline and label it in `#e74c3c` bold 11px with a computed string: `(100 - sum(hits16)) + " of 100 true neighbors invisible"` — expected `5 of 100`.
- **Note (11px `#999`, bottom-left inside plot):** "Illustrative Example — 10 crops, exact top-10 known by brute force".
- **Title (bold 14px `#1a5276`, top center):** "Recall Bought With Latency — Missing Results the Model Never Caused".

## 6. The Complete Picture

Summary table (`.summary-table`, header row + 7 rows):

| Search property | Text query | Crop query |
|---|---|---|
| **Query object** | A string from a finite vocabulary | A point in a continuous embedding space |
| **Repeat structure** | Heavy head — top 10 strings carry 25.1% here | Almost no head — top 10 clusters carry 3.05% |
| **Error correction** | Spelling correction, autocomplete, synonyms | None — a wrong crop is silently a valid query |
| **Intent disambiguation** | Add a word, pick a suggestion | Impossible — object, material, era, palette all fit |
| **Ground truth** | Clickable relevance judgments are teachable | Fleiss' kappa 0.23 across 5 raters on 40 crops |
| **Retrieval correctness** | Inverted index returns all matches | Approximate index hides 5 of 100 true neighbors at 16 probes |
| **Distribution shift** | Query mix drifts with the world | Query mix is a function of the model's own past output |

## Callout (philosophy box, bottom)

**One sentence:** A crop is a query with no vocabulary, no ground truth, and no way to ask a follow-up question — so the hard part of visual search is not the encoder but the closed loop, where the model's output becomes the sampling distribution of its next training set and every offline metric improves while coverage shrinks.

## Regeneration instructions

- **Layout:** detail page. h1 (no index number), `.subtitle`, opening `.philosophy` callout, then per aspect: `<h2>N. Title</h2>` (h2 1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table` — left `<td>` (50%) holds `.obj-title`, paragraph, `.math-box`, bullets; right `<td>` (50%, centered) holds the canvas. Section 6 is a `.summary-table`; page closes with a `.philosophy` callout.
- **Column split is fixed at 50/50.** Shrink a visualization via the canvas `style.maxWidth`, never by narrowing the viz `<td>`.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`. No nav bar, no back/home links, no cross-page links of any kind.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Math box:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `code` background `#eef2f7`, padding 2px 6px, radius 3px.
- **Summary table:** `.summary-table` — 0.9em, th background `#f0f4f8` `#1a5276` padding 10px 14px left-aligned, td padding 10px 14px, borders `1px solid #e0e0e0`.
- **Canvas:** intrinsic 720x360 each; a shared `setupCanvas(id, w, h)` sizes the backing store to the rendered width x `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **No randomness anywhere on this page.** Every series is either a literal array or a deterministic recursion, so `Math.random()` and even a seeded PRNG are unnecessary. Every statistic printed beside a chart is computed in JS from the plotted values at render time.
- **Naming:** no real company names in the body — use "a visual-discovery platform", "Vendor A", Alice/Bob. The page title keeps the established card name.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#999`, accent `#2980b9`.
