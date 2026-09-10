# Robotics & Simulation — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, two canvases 31% each, one table per section)
**HTML title tag:** Robotics & Simulation — Distribution Patterns

## Grasp Success (Binary Phase Transition)

**Pitfall label (uppercase, `#795548`):** SIGMOID (PHASE TRANSITION)

Near-zero success below a pose threshold, then rapid S-curve to near-100% above it. Not a gradient — a cliff. Grasping behaves like a binary phase transition between graspable and ungraspable. Sigmoid steepness tracks gripper tolerance: squishy gripper = gentle slope, rigid jaw = near-vertical cliff.

- Not gradual — binary phase transition at threshold angle
- Sigmoid steepness = gripper mechanical tolerance
- Squishy gripper = gentle slope (forgiving)
- Rigid parallel jaw = near-vertical (one degree off = fail)

### Visualization (canvas `canvas1`, 420×340)

Line chart (shared utility in line mode): sigmoid success curve with filled area.

- **Title (bold 13px, `#1a5276`, top center):** "Grasp Success Rate vs Pose Deviation (degrees)".
- **Data:** sigmoid `y = 1 / (1 + exp(-2.5x))` sampled every 0.1 over x in [−5, 5]; y range 0-1.
- **Line:** `#1a5276` width 2.5; area under the curve filled `rgba(211,84,0,0.35)`.
- **Axes:** gray `#999` L-axes, light `#eee` horizontal gridlines at quarter heights; x ticks 0 decimals at 6 positions, y ticks 1 decimal at 5 positions; x-axis label "Pose Deviation from Ideal", rotated y-axis label "Success Rate"; padding left 55 / right 20 / top 35 / bottom 40.

### Visualization (canvas `canvas1b`, 400×340)

Overlaid sigmoid curves with shaded tolerance bands for three gripper types.

- **Title (bold 12px):** "Gripper Tolerance Bands: Rigid vs Squishy"; subtitle 10px `#666`: "Steepness reveals mechanical precision requirements".
- **Curves (sigmoid `1/(1+exp(-kx))` over x in [−5, 5]):**
  - Rigid Jaw (k=6) — line `rgba(231,76,60,0.7)`, band `rgba(231,76,60,0.12)`
  - Standard (k=2.5) — line `rgba(41,128,185,0.8)`, band `rgba(41,128,185,0.12)`
  - Squishy (k=0.8) — line `rgba(39,174,96,0.7)`, band `rgba(39,174,96,0.12)`
- **Tolerance bands:** vertical shaded region per gripper spanning x where success is 20%-80% (`x = ∓ln(1/p − 1)/k`).
- **Reference line:** dashed `#aaa` horizontal line at 50% success (dash 4/3), labeled "50% success" in `#888` 10px.
- **Annotations:** red two-line bold 10px "Narrow / tolerance" with small arrow near the rigid band; green bold 10px "Wide tolerance = forgiving" near the squishy band.
- **Legend (below plot):** swatches for "Rigid Jaw (k=6)", "Standard (k=2.5)", "Squishy (k=0.8)".
- **Axes:** x ticks −5° to 5° by 2 (degree suffix); y ticks 0.0-1.0 by 0.25; padding left 50 / right 20 / top 40 / bottom 45.

## Sim-to-Real Gap (Tail = Simulator Quality)

**Pitfall label (uppercase, `#2980b9`):** RIGHT-SKEWED (FIDELITY METRIC)

Most actions transfer well (sim ≈ real), but fat right tail where reality is dramatically worse. One explanation: the tail is everything simulation doesn't model (friction, cable snag, lighting). That makes tail thickness a natural fidelity read — the tail, not the average, is the informative benchmark.

- Body: sim error ≈ real error (good transfer)
- Right tail: reality dramatically worse than sim predicted
- Tail = unmodeled physics (friction, cables, lighting)
- Tail thickness = simulator fidelity benchmark

### Visualization (canvas `canvas2`, 420×340)

Histogram: right-skewed sim-to-real gap.

- **Title:** "Sim-to-Real Performance Gap Distribution".
- **Data (seeded RNG mulberry32(42)):** 3000 body samples of `Exp(3.0) * 0.5`; 400 fat-tail samples of `1.5 + Exp(0.8)`.
- **Bins/range:** 55 bins, x 0-6, 1 decimal; x-axis label "Gap Magnitude (real - sim error)", y-axis label "Frequency".
- **Bars:** fill `rgba(41,128,185,0.35)`, stroke `#1a5276`.
- **Density line + SE band (standard for all histograms on this page):** Gaussian-smoothed counts (sigma 1.5 bins), line `#a04000` width 2, 95% band `rgba(211,84,0,0.18)` using effective N clamped to [30, 200].

### Visualization (canvas `canvas2b`, 400×340)

ECDF with percentile annotations and tail zone.

- **Title (bold 12px):** "ECDF: Where Does the Tail Begin?"; subtitle 10px `#666`: "Percentile view reveals tail heaviness invisible in histogram".
- **Curve:** ECDF of the same gap data (sorted, ~300 plotted points), line `#1a5276` width 2.5; area under the curve filled with a horizontal gradient from `rgba(41,128,185,0.4)` (left) through `rgba(41,128,185,0.2)` (60%) to `rgba(231,76,60,0.4)` (right).
- **Percentile markers (dashed vertical drop line dash 3/3, 4px dot, bold 10px label "Pxx=value"):** P50 `#27ae60`; P90 `#e67e22`; P95 `#e74c3c`; P99 `#8e44ad` (values computed from the data).
- **Tail zone:** region beyond the P90 value shaded `rgba(231,76,60,0.08)`, labeled bold 11px "TAIL ZONE" with 9px sub-line "Unmodeled physics" in `rgba(231,76,60,0.8)`, plus a small red arrow at the zone edge.
- **Axes:** x ticks 0-6 integers, label "Gap Magnitude"; y ticks 0% / 25% / 50% / 75% / 100%; light `#eee` gridlines; padding left 50 / right 20 / top 40 / bottom 45.

## Joint Torque (Saturation = Hardware Bottleneck)

**Pitfall label (uppercase, `#27ae60`):** BOUNDED + SATURATION SPIKES

Gaussian body within operating range, then delta spikes at ±max torque (motor saturation). The spikes = moments the robot hit its physical limit. Spike mass / body mass ratio = how often hardware bottlenecks software. When spike mass is heavy (this sim: over 10%), the robot looks undersized for the task — no algorithm fix helps.

- Gaussian body = normal operation range
- Spikes at ±max = motor saturation (physical limit)
- Spike ratio = hardware bottleneck frequency
- Heavy spike mass (>10% here) = hardware limit, not a software problem

### Visualization (canvas `canvas3`, 420×340)

Histogram: bounded Gaussian with saturation spikes.

- **Title:** "Joint Torque Distribution with Saturation".
- **Data:** 3000 samples of `N(0,1)*3.0` clipped to ±10 Nm; plus 200 spike samples of `10 − U*0.3` and 200 of `−10 + U*0.3`.
- **Bins/range:** 60 bins, x −11 to 11, 0 decimals; x-axis label "Torque (Nm)", y-axis label "Frequency".
- **Bars:** fill `rgba(39,174,96,0.35)`, stroke `#1a5276`. Standard density line + SE band.

### Visualization (canvas `canvas3b`, 400×340)

Time-series control chart with saturation zones.

- **Title (bold 12px):** "Time-Series Control Chart: Torque Saturation Events"; subtitle 10px `#666`: "Red zones = motor hitting physical limits (no algorithm fix)".
- **Data:** 200 points sampled uniformly at random from the full torque dataset using an independent seeded RNG `mulberry32(7)` (so appended saturation samples are represented). Y range −12 to 12.
- **Zones:** above +10 and below −10 shaded `rgba(231,76,60,0.15)`; the band between shaded `rgba(39,174,96,0.06)`; dashed red `#e74c3c` limit lines (dash 6/3, width 2) at ±10.
- **Series:** connected line `rgba(41,128,185,0.7)` width 1.2; points with |torque| ≥ 9.7 overplotted as 3px red `rgba(231,76,60,0.9)` dots.
- **Zone labels (bold 10px):** "+MAX TORQUE" and "-MAX TORQUE" in `#e74c3c` at the limit lines; "NORMAL OPERATING RANGE" in `#27ae60` centered in the band.
- **Callout box (top right, white with `#e74c3c` border):** bold 11px "Saturation: <pct>%" with 9px `#666` verdict line — "Robot UNDERSIZED" if >5%, else "Within spec".
- **Axes:** y ticks −12 to 12 with "Nm" suffix at 5 positions; x-axis label "Time (sample index)"; padding left 50 / right 20 / top 40 / bottom 40.

## Path Planning Time (Familiar vs Novel)

**Pitfall label (uppercase, `#e74c3c`):** BIMODAL (TWO SYSTEMS)

Spike at <1ms (cached/known path, fast recall) and broad mode at 50-500ms (novel scene requiring full planning). Consistent with a two-system architecture: fast habitual execution vs slow deliberate planning. The ratio between modes reads as environment novelty — illustrative: factory (95% fast) vs home (60% slow).

- Spike at <1ms = cached path recall (System 1)
- Broad mode 50-500ms = full planning search (System 2)
- Ratio = environment novelty measure
- Illustrative: factory robot 95% fast, home robot 60% slow

### Visualization (canvas `canvas4`, 420×340)

Histogram: bimodal planning latency.

- **Title:** "Path Planning Latency (Bimodal: Cached vs Novel)".
- **Data:** 1800 fast-mode samples of `|N(0,1)|*0.3 + 0.1` ms; 1200 slow-mode samples of `200 + N(0,1)*80` (values <50 remapped to 50-80, values >500 clipped to 500).
- **Bins/range:** 60 bins, x 0-520 ms, 0 decimals; x-axis label "Planning Time (ms)", y-axis label "Frequency".
- **Bars:** fill `rgba(142,68,173,0.35)`, stroke `#1a5276`. Standard density line + SE band.

### Visualization (canvas `canvas4b`, 400×340)

Stacked horizontal bar comparison: fast vs slow planning share by environment.

- **Title (bold 12px):** "Two-System Architecture: Speed Breakdown"; subtitle 10px `#666`: "Factory robot vs Home robot environment novelty".
- **Bars (35px tall, 18px gaps, right-aligned 11px name labels):**

| Scenario | Fast % | Slow % |
|----------|--------|--------|
| Factory Robot | 95 | 5 |
| Warehouse | 80 | 20 |
| Home Robot | 40 | 60 |
| Outdoor/Unknown | 15 | 85 |

- **Segments:** fast (System 1) fill `rgba(39,174,96,0.7)` stroke `#27ae60`; slow (System 2) fill `rgba(142,68,173,0.7)` stroke `#8e44ad`; white bold 11px percentage labels on segments wider than 20%.
- **Speed annotations (9px `#666`):** "<1ms avg" beside the Factory bar; "~200ms avg" beside the Outdoor bar.
- **Arrow:** orange `#e67e22` vertical arrow down the right side with rotated 9px label "More novel".
- **Legend (bottom):** swatch "System 1: Cached recall (<1ms)" green; "System 2: Full planning (50-500ms)" purple.

## Simulation Reward (Why RL Needs Millions)

**Pitfall label (uppercase, `#8e44ad`):** BIMODAL (CLIFF DYNAMICS)

Cluster at high reward (task completed) and cluster at low/negative (catastrophic failure), thin middle (~3% of episodes here). The thin middle suggests cliff dynamics — you succeed completely or fail completely, so RL gets almost no gradient from the middle. One explanation for why robotic RL is so sample-hungry: most episodes return a near-binary signal.

- High-reward cluster = task completed successfully
- Low-reward cluster = catastrophic failure
- Thin middle = cliff dynamics (no partial success)
- Near-binary signal = one reason RL needs so many episodes

### Visualization (canvas `canvas5`, 420×340)

Histogram: bimodal RL episode reward.

- **Title:** "RL Episode Reward (Bimodal: Success vs Failure)".
- **Data:** 1400 failure samples of `−80 + N(0,1)*12`; 1000 success samples of `85 + N(0,1)*10`; 100 thin-middle samples of `N(0,1)*20`.
- **Bins/range:** 60 bins, x −120 to 120, 0 decimals; x-axis label "Cumulative Reward", y-axis label "Frequency".
- **Bars:** fill `rgba(231,76,60,0.35)`, stroke `#1a5276`. Standard density line + SE band.

### Visualization (canvas `canvas5b`, 400×340)

Episode scatter plot highlighting the "gradient desert" middle zone.

- **Title (bold 12px):** "Episode Scatter: The Gradient Desert"; subtitle 10px `#666`: "Middle zone gives RL almost zero learning signal".
- **Data:** all 2500 reward samples plotted as 1.8px dots, x = episode index, y = reward (−120 to 120).
- **Dot colors by zone:** reward >30 green `rgba(39,174,96,0.6)`; reward <−30 red `rgba(231,76,60,0.5)`; middle orange `rgba(243,156,18,0.8)`.
- **Desert zone:** band from −30 to +30 shaded `rgba(243,156,18,0.12)`, labeled bold 10px "GRADIENT DESERT" with 9px sub-line "< 4% of episodes land here" in `rgba(243,156,18,0.9)`.
- **Cluster labels (left side):** "SUCCESS" bold 10px `#27ae60` with 9px "Strong + signal" near y=85; "FAILURE" bold 10px `#e74c3c` with 9px "Strong - signal" near y=−80.
- **Callout box (bottom right, white with `#8e44ad` border):** three bold 9px lines: "Near-binary signal =" / "almost no gradient" / "per episode".
- **Axes:** y ticks −120 to 120 at 5 positions; x labeled "Episode Number" with "0" and total count at the ends; light `#f0f0f0` gridlines; padding left 50 / right 20 / top 40 / bottom 45.

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per section, each with a single `<tr>` of three `<td>`s — left (38%) holds `.pitfall-label` span + `<h3>` + paragraph + `<ul>`; middle (31%, centered) holds the primary 420×340 canvas; right (31%, centered) holds the insight 400×340 canvas. Head includes a responsive viewport meta tag.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.obj-table` full width, collapsed borders, cells `1px solid #2980b9` with 12px padding; h3 `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase with 0.5px letter-spacing; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by a small script cycling `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` over all `.pitfall-label` elements in document order.
- **Data generation:** seeded RNG `mulberry32(42)` shared across all charts in document order; Box-Muller for normals; `randExp(lambda)` helper for exponentials.
- **Shared chart utility:** `drawHistogram(canvasId, data, options)` supports two modes — histogram mode (bars normalized to max count, optional xMin/xMax, x ticks at 6 positions with configurable decimals, y count ticks at 5 positions, rotated y-label) and line mode (`drawLine: true` with `lineData` point pairs, filled area under the curve, `#eee` gridlines). Histograms also get a Gaussian-smoothed density line `#a04000` width 2 with a 95% SE band `rgba(211,84,0,0.18)`. White background, bold 13px `#1a5276` titles, gray `#999` L-axes.
- **Canvas scaling:** all canvases declare intrinsic width/height attributes and set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; purples `#8e44ad`/`rgba(142,68,173,…)`, burnt orange `#a04000`/`rgba(211,84,0,…)`, amber `rgba(243,156,18,…)`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
