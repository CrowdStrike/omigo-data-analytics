# LLM & GenAI — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, two canvases 31% each, one table per section)
**HTML title tag:** LLM & GenAI — Distribution Patterns

## Next-Token Probs (Temperature Reshapes Zipf)

**Pitfall label (uppercase, `#795548`):** TEMPERATURE-CONTROLLED ZIPF

At temperature=0, a delta spike (one token gets all mass). As temperature rises, distribution spreads toward uniform. "Creativity" = flattening the Zipf curve. In this simulated 50-token vocabulary, T=0.7 already leaves the top-5 tokens with only ~46% of the mass — temperature is just a reshaping knob, not magic.

- Temperature→0: delta spike (deterministic)
- Temperature→∞: approaches uniform (random)
- "Creativity" = mathematically flattening the Zipf curve
- T=0.7 (this sim): top-5 tokens hold ~46% of mass

### Visualization (canvas `canvas1`, 420×340)

Multi-line chart: softmax token probabilities at four temperatures.

- **Title (bold 13px, `#1a5276`, top center):** "Next-Token Probabilities at Different Temperatures".
- **Data:** 50-token vocabulary with Zipf-like logits `3.0 − 2.5·ln(i+1)/ln(50)` for rank i; probabilities via temperature-scaled softmax `softmax(logits / max(T, 0.01))`.
- **Lines (width 2.5):** T=0.1 `#e74c3c` "T=0.1 (near-deterministic)"; T=0.7 `#e67e22` "T=0.7 (typical)"; T=1.5 `#27ae60` "T=1.5 (creative)"; T=5.0 `#1a5276` "T=5.0 (near-uniform)".
- **Axes:** y normalized to the max probability across all curves, tick labels 2 decimals at 5 positions with `#eee` gridlines; x ticks "Token 1", "Token 11", … every 10 ranks; x label "Token Rank", rotated y label "Probability"; gray `#999` L-axes, margins top 40 / right 20 / bottom 50 / left 55.
- **Legend (top right, line samples):** the four temperature labels above.

### Visualization (canvas `canvas1b`, 400×340)

Cumulative token-mass ECDF per temperature.

- **Title (bold 12px):** "ECDF: Tokens Needed for 95% Mass".
- **Curves:** cumulative sums of descending-sorted softmax probabilities for the same four temperatures — colors `rgba(231,76,60,0.8)` (T=0.1), `rgba(230,126,34,0.8)` (T=0.7), `rgba(39,174,96,0.8)` (T=1.5), `rgba(26,82,118,0.8)` (T=5.0), width 2.5, each with area fill at 0.1 alpha.
- **Reference line:** dashed `#999` horizontal line at 95% (dash 4/3), labeled "95%" in `#666` 10px.
- **Crossing markers:** 4px dot at each curve's first crossing of 95% cumulative mass; labels are NOT drawn at the dots (the T=1.5 and T=5.0 crossings land within a few px of each other) but stacked bottom-right, right-aligned, one 13px row per curve in the curve's color: bold 9px "T=x.x: N tokens to reach 95%" (N computed per temperature).
- **Axes:** y ticks 0% to 100% by 25%; x ticks 0-50 by 10; x label "Token Rank (sorted by probability)"; margins top 40 / right 20 / bottom 50 / left 55.

## Prompt Length (Two Products in One API)

**Pitfall label (uppercase, `#2980b9`):** BIMODAL (TWO PRODUCTS)

Spike at 10-50 tokens (short chat question), dead zone, second mass at 500-5000 tokens (RAG/agent with pasted documents). Two completely different products in one API. Short = chatbot (latency-sensitive). Long = agent/RAG (throughput-sensitive). A single pricing or serving model is unlikely to fit both well.

- Spike at 10-50 tokens = chatbot usage
- Mass at 500-5000 = RAG/agent with context
- Two products needing different optimization
- Valley = decision boundary for when context matters

### Visualization (canvas `canvas2`, 420×340)

Histogram (shared `drawHistogram` utility): bimodal prompt token count.

- **Title:** "Prompt Token Count — Bimodal (Chat vs RAG)".
- **Data (local seeded RNG mulberry32(101)):** 3000 chat samples of `25 + 12·N(0,1)` kept if in (5, 100); 1500 RAG samples of log-normal `exp(7.5 + 0.6·N(0,1))` (~1800 median) kept if in (300, 6000).
- **Bins/range:** 50 bins, x 0-6000, integer tick format; x label "Token Count", y label "Frequency".
- **Bars:** fill `rgba(142,68,173,0.4)`, stroke `#8e44ad`.
- **Density line + SE band (standard for single-dataset histograms on this page):** Gaussian-smoothed counts (sigma 1.5 bins), line `#6c3483` width 2, 95% band `rgba(155,89,182,0.2)` using effective N clamped to [30, 200]. Multi-dataset charts draw one density line per dataset in its stroke color, without a band.

### Visualization (canvas `canvas2b`, 400×340)

Paired waterfall: token budget breakdown for chat vs RAG.

- **Title (bold 12px):** "Waterfall: Where Tokens Go (Chat vs RAG)".
- **Segments (side-by-side bars per category — chat bar blue `rgba(52,152,219,0.6)` stroke `#2980b9`, RAG bar in the category color; stacked cumulatively as a waterfall; "+N" value labels above each bar; TOTAL drawn as full bars from the baseline):**

| Label | Chat | RAG | RAG bar color |
|-------|------|-----|---------------|
| System Prompt | 20 | 200 | rgba(52,152,219,0.7) |
| User Query | 15 | 30 | rgba(46,204,113,0.7) |
| Retrieved Context | 0 | 2500 | rgba(155,89,182,0.7) |
| Few-shot Examples | 0 | 800 | rgba(230,126,34,0.7) |
| History | 10 | 400 | rgba(241,196,15,0.8) |
| TOTAL | 45 | 3930 | rgba(231,76,60,0.7) |

- **Y scale:** 0-4000 tokens with `#eee` gridlines; two-line 9px x category labels.
- **Legend (top left):** "Chat (45 tokens)" blue, "RAG (3930 tokens)" purple.
- **Annotation:** bold 11px red `#e74c3c` "87x more tokens!" with a downward red arrow near the right side.

## Human Eval Scores (Why RLHF Plateaus)

**Pitfall label (uppercase, `#27ae60`):** TRIMODAL (RLHF SATURATION)

Spike at "clearly wrong," broad middle at "fine/acceptable," smaller spike at "surprisingly good." One explanation for the broad middle: evaluators struggle to distinguish quality among "good enough" outputs. Consistent with RLHF plateauing — beyond a threshold, human raters may add noise, not signal.

- Spike at "clearly wrong" = easy to identify
- Broad middle = "acceptable" (raters can't differentiate)
- Spike at "impressive" = clearly exceptional
- Middle noise = one explanation for RLHF plateaus

### Visualization (canvas `canvas3`, 420×340)

Histogram: trimodal human evaluation scores.

- **Title:** "Human Evaluation Scores — Trimodal".
- **Data (local seeded RNG mulberry32(203)):** 800 "clearly wrong" samples of `1.5 + 0.4·N(0,1)` kept in [1, 3]; 2000 "acceptable" samples of `5.5 + 1.2·N(0,1)` kept in [3, 8]; 500 "impressive" samples of `9.2 + 0.35·N(0,1)` kept in [8, 10].
- **Bins/range:** 40 bins, x 1-10, 1-decimal tick format; x label "Score (1-10)", y label "Frequency".
- **Bars:** fill `rgba(41,128,185,0.35)`, stroke `#2980b9`. Standard density line + SE band.

### Visualization (canvas `canvas3b`, 400×340)

Grouped bar chart: quality-dimension profiles showing the indistinguishable middle (replaces an earlier radar whose overlapping polygons hid the point).

- **Title (bold 12px):** `Why Raters Can't Distinguish 'Good Enough'`; subtitle 9px `#888`: "Illustrative rater-score profiles (stylized, not simulated)".
- **Layout:** 6 dimension groups (Fluency, Accuracy, Relevance, Coherence, Creativity, Helpfulness; 8px `#333` labels below) × 4 bars per group; y axis 0-1 with `#eee` gridlines and 9px `#666` labels at 0.25 steps; margins top 44 / right 10 / bottom 74 / left 34.
- **Profiles (bar fills):**
  - Clearly Wrong — `rgba(231,76,60,0.7)` — values [0.3, 0.15, 0.2, 0.25, 0.4, 0.1]
  - Acceptable A — `rgba(230,126,34,0.8)` — values [0.72, 0.68, 0.7, 0.65, 0.55, 0.7]
  - Acceptable B — `rgba(241,196,15,0.8)` — values [0.7, 0.65, 0.72, 0.7, 0.6, 0.68]
  - Impressive — `rgba(39,174,96,0.8)` — values [0.92, 0.95, 0.88, 0.9, 0.85, 0.93]
- **Noise-zone band:** `rgba(230,126,34,0.08)` horizontal band from 0.55 to 0.75 with dashed `#e67e22` edges, labeled bold 9px right-aligned "A ≈ B on every dimension: the noise zone".
- **Legend (bottom row):** swatch + label for each of the four profiles.

## Hallucination Rate (Query Property, Not Model)

**Pitfall label (uppercase, `#e74c3c`):** ZERO-INFLATED (QUERY-DEPENDENT)

Factual queries = mostly ~0% hallucination. Creative/ambiguous queries = fat tail up to 40%+. In this simulation the aggregate rate works out to ~2-3% — a number that describes neither regime: near-zero for known facts, far worse for unknowns. The aggregate hides which regime a query is in.

- Factual queries: ~0% hallucination (retrieval/memory works)
- Ambiguous queries: fat tail to 40%+ hallucination
- Aggregate "~2-3%" (this sim) describes neither regime
- Must know which regime a query falls in

### Visualization (canvas `canvas4`, 420×340)

Histogram: zero-inflated hallucination rate with fat tail.

- **Title:** "Hallucination Rate — Zero-Inflated with Fat Tail".
- **Data (local seeded RNG mulberry32(307)):** 3500 factual samples of `|0.3·N(0,1)|` (values ≥2 replaced by `0.1·U`); 800 tail samples of exponential `−ln(1−u)/0.08` (mean ~12) kept in (3, 50).
- **Bins/range:** 50 bins, x 0-50, tick format "N%"; x label "Hallucination Rate (%)", y label "Frequency".
- **Bars:** fill `rgba(231,76,60,0.35)`, stroke `#e74c3c`. Standard density line + SE band.

### Visualization (canvas `canvas4b`, 400×340)

Scatter plot: model confidence vs hallucination percentage, two regimes.

- **Title (bold 12px):** "Scatter: Model Confidence vs Hallucination %".
- **Data (same local RNG):** 120 factual points — confidence `0.85 + 0.12·U`, hallucination `|1.5·N(0,1)|` (capped/remapped below 10%), green `rgba(39,174,96,0.7)`; 60 ambiguous points — confidence `0.25 + 0.35·U`, hallucination `10 + 25·U` (capped 45), red `rgba(231,76,60,0.7)`; 30 transition points — confidence `0.55 + 0.2·U`, hallucination `3 + 12·U`, orange `rgba(230,126,34,0.7)`. Dots 3.5px.
- **Decision boundary:** dashed `#666` vertical line at confidence 0.7 (dash 5/3), labeled bold 9px "Threshold = 0.7".
- **Zones:** left of boundary tinted `rgba(231,76,60,0.05)` labeled bold 10px "DANGER ZONE" in `rgba(231,76,60,0.8)`; right tinted `rgba(39,174,96,0.05)` labeled "SAFE ZONE" in `rgba(39,174,96,0.8)`.
- **Axes:** x confidence 0.2-1.0 with ticks every 0.2, label "Model Confidence Score"; y 0-48% with ticks every 12% and `#eee` gridlines, rotated label "Hallucination %"; margins top 40 / right 15 / bottom 50 / left 55.

## Inference Latency (P99 Explodes Under Load)

**Pitfall label (uppercase, `#8e44ad`):** LOG-NORMAL (LOAD-DEPENDENT)

Right-skewed log-normal that shifts right and spreads wider under high batch load. Quiet = tight around 100ms. Busy = median near 1s with a tail stretching to 2-5s. The P99/P50 ratio under load vs quiet reads as batching efficiency — a ratio that blows up suggests head-of-line blocking.

- Log-normal (multiplicative queuing delays)
- Quiet: tight near 100ms
- Busy: median ~1s, tail to 2-5 seconds
- P99 doesn't scale linearly — it explodes under load

### Visualization (canvas `canvas5`, 420×340)

Overlaid histogram: quiet vs under-load latency distributions.

- **Title:** "Inference Latency — Log-Normal (Quiet vs Under Load)".
- **Data (local seeded RNG mulberry32(411)):** quiet — 2000 samples of `exp(4.6 + 0.3·N(0,1))` (~100ms median); busy — 2000 samples of `exp(6.9 + 0.7·N(0,1))` (~1000ms median); both kept in (40, 5000).
- **Bins/range:** 50 bins, x 0-5000, tick format "Nms"; x label "Latency (ms)", y label "Frequency".
- **Series:** quiet fill `rgba(39,174,96,0.35)` stroke `#27ae60`; busy fill `rgba(231,76,60,0.30)` stroke `#e74c3c`. One smoothed density line per dataset, each in its dataset's stroke color (no SE band on multi-dataset charts).
- **Legend:** "Quiet (~100ms median)", "Under Load (~1000ms median)".

### Visualization (canvas `canvas5b`, 400×340)

Percentile-vs-load curves showing non-linear tail explosion.

- **Title (bold 12px):** "P50 / P90 / P99 vs Server Load — Non-Linear Explosion".
- **Model:** latency = `100 · (1 + (load/100)^3 · 8) · pMult` where pMult = 1 for P50, `1.8 + (load/100)^2·3` for P90, `3 + (load/100)^2·15` for P99; load sampled 0-100% in 5% steps; y clipped at 5000ms.
- **Curves (width 2.5, each with area fill):** P50 (median) `rgba(39,174,96,0.9)` fill `rgba(39,174,96,0.15)`; P90 `rgba(230,126,34,0.9)` fill `rgba(230,126,34,0.12)`; P99 (tail) `rgba(231,76,60,0.9)` fill `rgba(231,76,60,0.1)`.
- **Annotations:** dashed red vertical bracket (dash 3/2) between P99 and P50 at 70% load with bold 10px red label "<N>x gap!" (ratio computed from the model); small dark arrow at ~55% load on the P99 curve labeled bold 9px "knee".
- **Legend (top left, line samples):** "P50 (median)", "P90", "P99 (tail)".
- **Axes:** y ticks 0-5000ms by 1000 with `#eee` gridlines, rotated label "Latency (ms)"; x ticks 0-100% by 20%, label "Server Load (%)"; margins top 40 / right 15 / bottom 50 / left 55.

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per section, each with a single `<tr>` of three `<td>`s — left (38%) holds `.pitfall-label` span + `<h3>` + paragraph + `<ul>`; middle (31%, centered) holds the primary 420×340 canvas; right (31%, centered) holds the insight 400×340 canvas.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.obj-table` full width, collapsed borders, cells `1px solid #2980b9` with 12px padding; h3 `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase with 0.5px letter-spacing; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by a small script cycling `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` over all `.pitfall-label` elements in document order.
- **Data generation:** global seeded RNG `mulberry32(42)` plus per-chart local RNGs — `mulberry32(101)` (prompt length), `mulberry32(203)` (eval scores), `mulberry32(307)` (hallucination), `mulberry32(411)` (latency); Box-Muller for normals.
- **Shared histogram utility:** `drawHistogram(canvasId, data, options)` — accepts a single dataset or an array of datasets for overlays with per-dataset colors/strokes; white background, bold 13px `#1a5276` title, gray `#999` L-axes, `#eee` y-gridlines, y count ticks at 5 positions, x ticks at 6 positions with an optional `xTickFormat` callback, rotated y-label, optional legend swatches; every histogram also gets Gaussian-smoothed density lines (sigma 1.5 bins, width 2) — single dataset: one `#6c3483` line with a 95% SE band `rgba(155,89,182,0.2)`; multiple datasets: one line per dataset in its stroke color, no band; skipped entirely when `density: false` is passed. Note: this page's utility sizes canvases from `getBoundingClientRect()` (CSS width 100%) rather than the intrinsic attributes; the hand-drawn insight charts use the intrinsic 400×340 attributes.
- **Canvas scaling:** all canvases scale the backing store by `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; purples `#8e44ad`/`#6c3483`/`rgba(155,89,182,…)`, yellow `rgba(241,196,15,…)`, sky blue `rgba(52,152,219,…)`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
