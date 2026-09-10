# Streaming & Recommendations — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, histogram canvas middle 31%, insight canvas right 31%, one table per section)
**HTML title tag:** Streaming & Recommendations — Distribution Patterns

## Play Counts — Most Content Gets Ignored (Power Law)

**Pitfall label:** WINNER-TAKE-ALL SHAPE (color `#795548`)

A tiny handful of hits hog almost all the attention while the rest of the catalog sits in a forgotten corner. The simulated 5,000-track catalog here is all left wall in the histogram, and its Lorenz curve bows far below the equality line.

- In the simulated catalog charted here, the top 1% of tracks captures 71% of all plays — and the top 10% captures 91%. The rest fight over scraps
- ~96% of the simulated tracks end up with fewer than 100 plays — effectively invisible
- A shape this concentrated is why "hidden gems will find their audience" is hard — the tail barely gets sampled
- One explanation: recommenders amplify whatever is already popular (rich-get-richer feedback) — though the histogram alone can't prove the mechanism

### Visualization (canvas `canvas1`, 420×340)

Histogram of play counts: steep power law.

- **Data:** 5000 samples from a Pareto/Zipf-like tail `1/u^(1/0.7)` (alpha ≈ 0.7), truncated at 10000, using the page-level seeded RNG mulberry32(42). With this seed the top 1% captures 71% of plays and the top 10% captures 91%.
- **Chart:** 40 bins over x range 1–500 (display capped), integer bin labels. Bars filled `rgba(22,160,133,0.35)` stroked `#1a5276` (0.5px). Gaussian-smoothed density line (sigma 1.5 bins) in `#0e6655` (2px) with 95% SE band filled `rgba(22,160,133,0.18)`. White background, `#333` axes, `#eee` horizontal gridlines with 5 y ticks (quarters of max count); margins top 40 / right 20 / bottom 50 / left 55.
- **Title (bold 14px `#1a5276`):** "Song/Episode Play Counts — Power Law"
- **X-axis label:** "Play Count (display capped at 500)"; **Y-axis label (rotated):** "Frequency".

### Visualization (canvas `canvas1b`, 400×340)

Lorenz curve of play concentration.

- **Title (bold 13px `#1a5276`):** "Lorenz Curve — Concentration Inequality".
- **Equality line:** dashed gray diagonal (`#95a5a6`, dash 4/3, width 1.5).
- **Lorenz curve:** cumulative play share vs cumulative content share (~50 sampled points), stroke `rgba(192,57,43,0.9)` width 2.5; area between equality line and Lorenz filled `rgba(231,76,60,0.2)`.
- **Gini gap annotation:** vertical double-headed red arrow (`#e74c3c`) at x = 50% between the equality line and the Lorenz curve, labeled bold 11px `#c0392b` "Gini Gap".
- **Annotation (bold 12px `#1a5276`, bottom right):** "Top 1% = 71% of plays" (computed from data).
- **Legend (top left):** "— Perfect equality" in `#95a5a6`, "— Actual distribution" in `rgba(192,57,43,0.9)`.
- **Axes:** x "0%", "50%", "100%" with title "% of Content (cumulative)"; y "0%" to "100%".

## Time to Abandon — The Opening Minutes Decide (Bathtub Shape)

**Pitfall label:** HIGH AT EDGES, DIP IN MIDDLE (color `#2980b9`)

Drop-offs aren't spread evenly. There's a cliff in the opening minutes — with a sharp spike around the 2-6 minute mark, right where intros sit — then a long low plateau, then a final bump at the episode's end. In this simulated cohort, roughly a quarter of viewers are gone within the first 6 minutes; the rest leak away at a slow, steady drip.

- Opening cliff: ~25% of the simulated cohort exits in the first ~6 minutes
- Spike at the intro timestamp — consistent with viewers bailing where the intro plays
- Long flat middle: a steady drip of ~1-1.5% per 2-minute bin, no sudden cliffs
- End-of-episode bump — plausibly credits and "one is enough" stops

### Visualization (canvas `canvas2`, 420×340)

Discrete-bin bar chart of dropout counts per 2-minute bin (bathtub + spikes).

- **Data:** 30 precomputed bins (labels 0, 2, 4, … 58 minutes). Base curve `80·exp(-8t) + 20 + 40·t⁴` (t normalized 0–1), +120 added to bins 1 and 2 (intro-skip spike), +50 to the second-to-last bin (end bump), plus Gaussian noise (sd 5), rounded, floored at 0.
- **Chart:** discreteBins mode. Bars `rgba(41,128,185,0.35)` stroked `#2980b9`. Density overlay disabled (`density: false`) — smoothing across hand-built per-minute counts would blur the cliff/spike structure.
- **Title:** "Time to Abandon — Bathtub + Spikes"
- **X-axis label:** "Minutes into Episode"; **Y-axis label:** "Dropout Rate".

### Visualization (canvas `canvas2b`, 400×340)

Viewer survival curve derived from the dropout bins.

- **Title:** "Viewer Survival Curve — Who Stays?".
- **Curve:** survival fraction starting at 100% with a cohort of (sum of dropout counts + 500), subtracting each bin's dropouts; stroke `rgba(41,128,185,0.85)` width 3, area under curve filled `rgba(41,128,185,0.15)`.
- **Hook window:** region from 0 to bin 3 (~6 min) tinted `rgba(231,76,60,0.15)` with vertical dashed red boundary line (`#e74c3c`, dash 5/3, width 2); labels bold 11px `#c0392b` "HOOK" / "WINDOW" and 10px "~6 min".
- **Committed zone label (center bottom):** bold 11px `rgba(39,174,96,0.8)` "COMMITTED VIEWERS" with 10px "(shallow slope = slow drip)".
- **50% marker:** if survival crosses 50%, orange dashed elbow guide (`#e67e22`, dash 3/2) from the y-axis to the crossing point down to the x-axis, labeled bold 10px "50% drop".
- **Axes:** y "0%", "50%", "100%"; x "0" and "60"; x-axis title "Minutes into Episode".

## Genre Diversity — Clusters at 3-5, Tapers Out by 8 (Left-Skewed)

**Pitfall label:** BUNCHED UP WITH A HARD WALL (color `#27ae60`)

Even with an unlimited catalog, the simulated users bunch tightly around 3-5 genres and the curve dies out before 9. One interpretation is a fixed attention budget — people actively follow only a handful of interest areas at a time. Another is recommender feedback: the system keeps serving the genres you already watch. The shape alone can't separate the two.

- Most users cluster around 3-5 genres; the right tail decays fast
- Almost nobody exceeds 8 — the ECDF hits 100% long before the catalog runs out of genres
- The ceiling could be human attention or algorithmic feedback — the histogram can't tell you which

### Visualization (canvas `canvas3`, 420×340)

Histogram of genres consumed per user, left-skewed with a ceiling.

- **Data:** 3000 samples of `4 + 1.2·N(0,1)`, capped at 9.5, floored at 1; with 15% probability the value is replaced by `2 + 2·u` (left-skew pull-down). Uses the page-level RNG.
- **Chart:** 18 bins over x range 1–10, integer labels. Bars `rgba(230,126,34,0.35)` stroked `#e67e22`. Smoothed density line `#0e6655` + SE band `rgba(22,160,133,0.18)` (shared helper).
- **Title:** "Genre Diversity per User — Left-Skewed"
- **X-axis label:** "Number of Genres Consumed"; **Y-axis label:** "Users".

### Visualization (canvas `canvas3b`, 400×340)

ECDF of genre diversity with sweet-spot and empty-zone annotations.

- **Title:** "ECDF — Diversity Ceiling".
- **Curve:** ECDF over x range 1–10, stroke `rgba(230,126,34,0.85)` width 2.5, closed to 100% at the max value.
- **Zones:** genres 3–5 tinted `rgba(39,174,96,0.12)` labeled bold 11px `#27ae60` "SWEET" / "SPOT" (top); genres 8–10 tinted `rgba(231,76,60,0.1)` with vertical dashed red boundary at 8 (`#e74c3c`, dash 6/3), labeled bold 11px `#c0392b` "EMPTY" / "ZONE"; a red right-pointing arrow at the wall labeled 10px "WALL".
- **Percentile markers:** 5px dots with bold 10px labels — P50 in green (`rgba(39,174,96,0.8)`/`#27ae60`) "P50={value}", P90 in orange (`rgba(230,126,34,0.8)`/`#e67e22`) "P90={value}".
- **Axes:** x ticks 1–10 with title "Genres Consumed"; y "0%", "50%", "100%".

## Binge Length — A Fatter Tail Than a Coin Flip (Geometric + Excess Tail)

**Pitfall label:** HALVING PATTERN WITH A SURPRISE TAIL (color `#e74c3c`)

If "watch another episode?" were a coin flip, each episode would keep half the viewers of the last — and for the first few episodes the simulated data tracks that geometric decay almost exactly. Deeper in, the observed bars sit above the coin-flip line: more long sessions than the model predicts. One interpretation: hooks and cliffhangers push some viewers into "one more" territory, and the excess over geometric is a way to quantify it.

- Early episodes: near-perfect halving, like a coin flip
- Episodes 6 and beyond: observed counts sit above the geometric prediction — the excess tail
- The gap between predicted and observed stops gives a measurable "can't stop" signal
- A show whose tail hugs the geometric line shows no such excess

### Visualization (canvas `canvas4`, 420×340)

Discrete-bin bar chart of observed binge lengths with a pure-geometric overlay line.

- **Data:** 12 bins (episodes 1–12), base 1000 sessions. Pure geometric: `1000·0.5^i`. Observed: same for episodes 1–4, then a fatter tail `1000·0.5⁴·0.7^(i-4)` for episodes 5+, plus Poisson-like noise `randn()·sqrt(count)`, rounded, floored at 0.
- **Chart:** discreteBins mode. Bars `rgba(142,68,173,0.35)` stroked `#8e44ad`. Overlay: dashed red line (`#e74c3c`, dash 5/3, width 2) through the pure-geometric values, with legend line + label "Pure Geometric" (top right). Density overlay disabled (`density: false`) — the point is the bar-vs-geometric comparison, not a smoothed curve.
- **Title:** "Binge Session Length — Geometric + Heavy Tail"
- **X-axis label:** "Episodes Watched in Session"; **Y-axis label:** "Sessions".

### Visualization (canvas `canvas4b`, 400×340)

Waterfall of the excess (observed − geometric) per episode with a cumulative excess line.

- **Title:** "Addictiveness Waterfall — Excess Over Geometric".
- **Bars:** one bar per episode 1–12 around a central zero line (`#333`, 1.5px, at mid-height); positive excess bars above the line in `rgba(142,68,173,0.7)` stroked `#8e44ad`, negative below in `rgba(41,128,185,0.6)` stroked `#2980b9`; scale ±1.2× the max absolute excess.
- **Cumulative excess line:** running total of positive excess from episode 5 onward, stroke `rgba(230,126,34,0.9)` width 2.5, scaled to 80% of the top half.
- **Annotations:** purple upward arrow near episode 8 labeled bold 10px "EXCESS" / "TAIL"; top right bold 12px `#1a5276` "Excess Tail Total: +{N}" with 10px `#e67e22` "— cumulative excess"; top left 10px labels "Above geometric" (purple) and "Below geometric" (blue).
- **Axes:** x ticks 1–12 with title "Episode Number in Session"; y "+{max}", "0", "-{max}".

## Rating Distribution — The Rating System Creates Its Own Shape (UI Artifact)

**Pitfall label:** SHAPE CREATED BY THE BUTTON DESIGN (color `#8e44ad`)

Here's a fun twist: the pattern of ratings has less to do with content quality than with the rating buttons themselves. In the simulated systems compared here, 5 stars produce a J-shape (piles at 1 and 5), thumbs up/down lands 92% positive, and a 10-point scale forms three humps at 1, 7, and 10. Same content, three different shapes — the UI is doing the shaping.

- 5-star systems show a J-shape — the middle sags while 1 and 5 spike
- Thumbs up/down: 92% positive in this simulation — going negative takes a more deliberate act
- 10-point scales form humps at 1, 7, and 10 — most mid scores go unused
- The shape tells you about the UI design, not about how good the content is — one reason a major platform famously swapped stars for thumbs

### Visualization (canvas `canvas5`, 420×340)

Discrete-bin bar chart of 5-star ratings forming a J-curve.

- **Data:** base counts `[280, 80, 120, 200, 520]` for stars 1–5, plus Gaussian noise (sd 15, rounded).
- **Chart:** discreteBins mode. Bars `rgba(231,76,60,0.35)` stroked `#c0392b`. Density overlay disabled (`density: false`) — a smoothed line would fill in the J-curve's empty middle.
- **Title:** "Content Ratings (5-Star) — J-Curve"
- **X-axis label:** "Star Rating"; **Y-axis label:** "Number of Ratings".

### Visualization (canvas `canvas5b`, 400×340)

Three stacked mini bar charts comparing rating-system shapes.

- **Title:** "Rating Shape = UI Artifact (3 Systems Compared)".
- **Rows (each one-third of plot height, separated by dashed `#ddd` lines):**
  - "5-Star Scale" — shares `[0.23, 0.07, 0.10, 0.17, 0.43]` for labels 1–5, bars `rgba(231,76,60,0.7)` stroked `#c0392b`; small arrow above the 5-star bar labeled bold 9px "J-peak".
  - "Binary Thumbs" — shares `[0.08, 0.92]` for thumbs-down/thumbs-up emoji labels, bars `rgba(39,174,96,0.7)` stroked `#27ae60`; bold 10px `#27ae60` annotation "92% positive!".
  - "10-Point Scale" — shares `[0.15, 0.03, 0.03, 0.04, 0.05, 0.06, 0.22, 0.12, 0.08, 0.22]` for labels 1–10, bars `rgba(41,128,185,0.7)` stroked `#2980b9`; small arrows above the peaks at 1, 7, 10 and bold 9px annotation "Trimodal (1,7,10)".
- System name in its stroke color at the left of each row; bars ≥15% show their percentage in white bold 10px inside the bar; 9px `#333` category labels below each bar.
- **Bottom annotation (bold 11px `#1a5276`, centered):** "Same content, different shapes = UI creates the distribution".

## Regeneration instructions

- **Layout:** one `.obj-table` (full-width, border-collapse) per section, single `<tr>` with three `<td>`: left 38% text (`.pitfall-label` span, `h3`, paragraph, `ul`), middle 31% centered canvas (420×340), right 31% centered insight canvas (400×340). Cell borders `1px solid #2980b9`, padding 12px. Includes viewport meta tag.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `h3` in cells `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase with 0.5px letter-spacing. Canvas CSS `width: 100%; height: auto`. No nav bar, no back/home links.
- **Pitfall label colors:** assigned by a small script cycling through `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` in document order.
- **Data generation:** single page-level seeded RNG `mulberry32(42)` shared across charts (consumed in document order), Box-Muller `randn()`, inverse-CDF `randExp(lambda)`.
- **Histogram helper:** shared `drawHistogram(canvasId, data, bins, title, color, options)` — supports continuous binning (min/max/labelDecimals) or `discreteBins: true` (data = precomputed counts, bins = labels array); options strokeColor/xLabel/yLabel/overlayLine/overlayLineColor/overlayLabel. White plot background; margins top 40 / right 20 / bottom 50 / left 55; title bold 14px `#1a5276` centered; `#333` axes with `#eee` gridlines at 5 y ticks (quarters); rotated y-axis label; plus a Gaussian-smoothed density line (`#0e6655`, sigma 1.5 bins) with a 95% SE band (`rgba(22,160,133,0.18)`, effective n clamped to [30, 200]); the density/SE overlay is skipped when `density: false` is passed (used on all discreteBins charts).
- **Canvas scaling:** all canvases set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`, purple `#8e44ad`, teal `#16a085`/`#0e6655`, dark red `#c0392b`, gray text `#555`/`#666`/`#333`.
- Note: regenerated HTML pages link nowhere (detail page); any grid page linking here uses the `.html` extension.
