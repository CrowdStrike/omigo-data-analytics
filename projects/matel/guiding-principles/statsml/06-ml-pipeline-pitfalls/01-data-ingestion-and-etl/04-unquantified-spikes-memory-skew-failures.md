# Pitfall: Unquantified Spikes (Memory / Skew Failures)

**Page type:** detail page (card-sections with h2 headers, two-column layout table per section: text left 45%, canvas right 55%)
**HTML title tag:** Unquantified Spikes (Memory / Skew Failures)

**Subtitle:** Data volume spikes or skew cause memory exhaustion and pipeline failures without profiling or warnings

## The Problem

Tags: `the trap` (red), `data skew` (blue)

- **Volume is a distribution** — data size has a long tail, yet pipelines treat it as a constant
- **No cardinality profiling** — rows-per-key are never measured, so memory needs are guesses
- **Skewed groupby keys** — even 100x the typical rows on one worker is enough to kill it
- **Seasonal spikes** — Black Friday or tax season exceeds capacity sized on quiet months
- **Unbounded windows** — window state grows with the largest partition, not the average one

*Example:* Illustrative Example: a pipeline groups by user_id; one bot key holds 10M events at ~10KB each, so about 100GB against an 8GB executor, and the job OOMs.

**Impact:** Pipelines fail unpredictably, retries replay the same skewed partition, and the outcome is data loss or SLA breaches.

### Visualization (canvas `c1`, 720×300)

Line chart of memory usage over time/partitions with a runaway spike hitting the limit and killing the job.

**Determinism:** all jitter comes from a seeded Park-Miller LCG (`lcg(20250759)`), never `Math.random()`, so the trace and every printed figure are identical on each load. Every annotated number is computed in the draw function from the plotted series.

- **Title (bold 14px, top center, `#1a5276`):** "Memory Spike from Skewed Partition".
- **Plot area:** left=60, right=660, top=70, bottom=240; gray (`#999`) axes with light gray (`#e0e0e0`) gridlines; x label "Time / Partition →", rotated y label "Memory Usage (GB)"; y ticks 0, 4, 8, 12, 16, 20 (axis ceiling `GBMAX` = 20 GB).
- **Memory limit line:** horizontal dashed orange (`#e67e22`, width 2, dash 6/4) positioned by value at `LIMIT` = 8 GB, labeled bold 10px orange "Memory Limit: 8GB".
- **Normal series:** green (`#27ae60`, width 2.5) line for partitions 0..60, GB = `3.0 + 1.0*sin(i*0.8) + 0.25*(rnd()-0.5)`. Computed from the plotted points: min 1.9, max 4.1, median 3.0 GB.
- **Spike series:** red (`#e74c3c`, width 3) line for partitions 61..100, GB = `min(3.0 + (i-60)*1.07, 20)` — a linear ramp clipped at the axis ceiling.
- **Limit-crossing marker:** small orange dot at the first plotted partition whose demand exceeds 8 GB, labeled bold 10px "crosses limit at partition 65 (8.4 GB)" — both figures computed, not asserted.
- **OOM marker:** filled red circle (radius 8) at the first partition reaching the 20 GB ceiling (partition 76), with bold 11px red "OOM" and "JOB KILLED".
- **Annotations:** 10px green lower-left "Normal partitions: 1.9-4.1 GB (median 3.0)"; 10px red "Skewed partition: 20+ GB — 6.6x the normal median"; 10px gray caveat "(trace clipped at the 20 GB axis ceiling; true demand runs far higher)".

## Why It Happens

Tags: `root cause` (orange), `capacity planning` (blue)

- **Average-case design** — pipelines are sized for the mean while failure is driven by the maximum
- **Uniformity assumption** — capacity plans treat all partitions as similar, which rarely holds
- **No pre-shuffle checks** — hot keys go undetected until they land on one worker mid-job
- **Average-based limits** — memory limits come from mean partition size, not the p99 or max
- **Long-tailed reality** — bots and power users sit orders of magnitude above the median
- **Crash as discovery** — with nothing profiling the data, the first evidence of skew is a crash

*Example:* Illustrative Example: the chart's seeded profile has a median of 200 rows per key but a maximum of 10,000,000 — a skew ratio of 50,000, invisible to any average.

**Root Cause:** Capacity is planned around averages while failure is triggered by the tail, which stays invisible without profiling.

### Visualization (canvas `c2`, 720×300)

Long-tail bar chart of rows per key (log-scale y-axis) with one extreme hot key.

**Determinism:** key counts come from a seeded Park-Miller LCG (`lcg(20250759)`), never `Math.random()`. Every statistic on the chart — median, p99, max, skew ratio, group shares, and the row ranges — is computed in the draw function from the plotted counts via `median()` / `percentile()` helpers.

- **Title (bold 14px, top center, `#1a5276`):** "Skewed Key Distribution (Long Tail)".
- **Plot area:** left=60, right=660, top=70, bottom=240; gray (`#999`) axes; x label "Keys (sorted by count) →", rotated y label "Row Count".
- **Log-scale mapping:** 1 to 10M spans 7 decades mapped onto 86% of the plot height; bar heights and the y tick labels 1, 10, 100, 1K, 10K, 100K, 1M, 10M all use that same mapping, so labels and bars cannot disagree.
- **Key counts (120 total = 99 + 20 + 1):** 99 typical keys drawn as `10^(1.0 + 2.0*rnd())` → 10-974 rows, fill `rgba(39,174,96,0.6)`; 20 heavy keys drawn as `10^(3.2 + 0.5*rnd())` → 1,713-4,999 rows, fill `rgba(230,126,34,0.6)`; 1 hot key fixed at 10,000,000 rows. Bars are sorted ascending, pitch = (600−30)/120, width = pitch−1.3.
- **Hot key:** single wide bar (12px) at the far right, height from its count via the log mapping, fill `rgba(231,76,60,0.8)` with `#e74c3c` stroke. Right-aligned annotations: bold 11px "Hot key", 10px "10,000,000 rows" and "(2,000x p99)".
- **Skew ratio badge:** filled red box (154×42) at top area, white text: bold 11px "Skew Ratio: 50,000" and 10px "max 10,000,000 / median 200".
- **Group labels (10px, computed shares that sum to 100.0%):** green "99 typical keys (82.5%): 10-974 rows"; orange "20 heavy keys (16.7%): 1,713-4,999 rows"; red "1 hot key (0.8%) — crashes the pipeline"; gray "p99 = 4,999 rows across all 120 keys".

## The Correct Approach

Tags: `the fix` (green), `profiling` (blue)

- **Measure first** — skew becomes manageable once profiling is a first-class pipeline stage
- **Profile keys** — count rows per key before every shuffle, groupby, or window operation
- **Budget for the tail** — set memory to p99 partition size × row size × a safety factor
- **Salt hot keys** — split a hot key by hour or hash bucket to spread its load across partitions
- **Emit skew metrics** — publish min, median, p99, and max partition sizes on every run
- **Alert on ratio** — flag when max/median crosses a per-workload threshold, e.g. 100

*Example:* Illustrative Example: the profile finds one key at 10,000,000 rows against a p99 of 4,999 — 2,000x — so it is repartitioned by (user_id, event_hour) into 24 slices.

**Fix:** Profile cardinality before expensive operations, publish skew metrics on every run, and salt hot keys — budgeting memory for p99, not the average.

### Visualization (canvas `c3`, 720×300)

Four-step workflow diagram with per-step details and a metrics dashboard strip.

**Determinism:** this chart is a static diagram — fixed geometry and literal text only, no generated data and no statistics to compute.

- **Title (bold 14px, top center, `#1a5276`):** "Profiling Workflow: Detect & Mitigate Skew".
- **Step boxes:** four 140×50 white boxes (stroke width 3) connected by gray (`#999`) arrows with filled arrowheads, centered at x = 80, 250, 420, 590, y=80:
  - "Profile / Cardinality" — stroke `#1a5276`
  - "Detect / Skew" — stroke `#e67e22`
  - "Repartition / Hot Keys" — stroke `#27ae60`
  - "Execute / Safely" — stroke `#27ae60`
- **Per-step details (below boxes):**
  - Step 1 (bold 10px monospace, `#1a5276`): "SELECT key," / "  COUNT(*) as cnt" / "GROUP BY key" / "ORDER BY cnt DESC"
  - Step 2 (orange `#e67e22`): bold "Skew ratio:" / "max / median", then "Alert if > 100"
  - Step 3 (green `#27ae60`): bold "If hot key:", then "Split by sub-key" / "(e.g., hour, hash)"
  - Step 4 (green `#27ae60`): bold "Memory budget:", then "p99 × row_size" / "× safety_factor"
- **Metrics dashboard strip (bottom):** box 680×50, fill `#f8f9fa`, stroke `#1a5276` width 2; bold 11px `#1a5276` "Emit Metrics: partition_count, min_size, median_size, p99_size, max_size, skew_ratio, memory_peak" then 10px "Alert if: skew_ratio > 100 OR memory_peak > 0.9 × limit OR partition failure rate > 1%".

## Regeneration instructions

- **Layout:** repeated `.card-section` blocks, one per section. Each has an `<h2>` (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) followed by a `table.layout` (full width, border-collapse) with a single `<tr>`: left `td.text-col` (45%) containing `.tags` pills, a `<ul>` of labeled bullets, a `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) containing one `<canvas width="720" height="300">`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `2px solid #2980b9` bottom border. `.subtitle` `#666` 0.95rem. `ul` 0.92rem with `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; intrinsic size 720×300, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Determinism:** no `Math.random()` anywhere. A shared seeded Park-Miller LCG helper `lcg(seed)` supplies all generated data; both data-bearing charts use seed 20250759. Shared `median(a)`, `percentile(a, p)`, and `grp(n)` (thousands separator) helpers compute every printed statistic from the plotted values at render time.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
