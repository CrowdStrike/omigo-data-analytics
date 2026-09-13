# Big-O Intuition

**Page type:** detail page (tutorial layout: h1 + subtitle, then four `.card-section` blocks each with an h2 and a `table.layout` — text column 50% left with tag pills / bullets / example / key-point, viz column 50% right with one 720×300 canvas)
**HTML title tag:** Big-O Intuition

**Subtitle:** Three ways to look up one customer among a million, plus one all-pairs pass — the number of steps each way needs is a shape, and the shape decides everything

## Finding One Customer Among a Million

Tags: `core idea` (blue), `running example` (green)

- **The task** — a table of 1,000,000 customers; find the one with a given email
- **Dictionary, O(1)** — the email tells you where to look: ~1 step, any table size
- **Sorted list, O(log n)** — halve the range each guess: 1M needs only ~20 halvings
- **Unsorted scan, O(n)** — check rows one by one: up to 1,000,000 checks
- **All pairs, O(n²)** — not a lookup: duplicate-check compares every row to every other, 10¹² checks

*Example:* Three lookups and one all-pairs pass: 1 step, 20 steps, a million steps, a trillion steps.

**Key point:** Big-O names the shape of "steps needed as data grows" — flat, barely-growing, straight line, or explosion.

### Visualization (canvas `c1`, 720×300)

Log-log line chart of the four complexity curves, endpoints marked at n = 1M.

- **Title (bold 15px, `#1a5276`, centered):** "Steps to Find One Customer (both axes log scale)".
- **Axes:** x = log10(n) from 0 to 6, tick labels "1", "10^2", "10^4", "10^6" (every 2 decades); y = log10(steps) from 0 to 12, gridlines (`#e5e9ef`) and labels "1", "10^3", "10^6", "10^9", "10^12" (every 3 decades). Axis lines `#999`; x-axis caption "n = number of customers" in `#444` 12px. Padding: top 46, bottom 46, left 66, right 150.
- **Curves (3px lines, drawn as functions of log10 n over 60 samples):** n² as `2·log n` in magenta `#d55181`; n as `log n` in orange `#d95926`; log n as `log10(log2 n)` in green `#008300`; O(1) as flat 0 in blue `#2a78d6`.
- **Endpoint dots (5px radius at x = 10^6) with bold 12px right-side labels:** magenta "all pairs: 10^12" at y=10^12; orange "scan: 10^6" at y=10^6; green "sorted: 20" at y=20; blue "dictionary: 1" at y=1.
- **Annotation (bold magenta 13px, top-left inside plot):** "at n = 1M: 1 vs 20 vs 10^6 vs 10^12 steps".

## The Same Lookup on a Wall Clock

Tags: `worked example` (green)

- **The machine** — assume a plain 10 million simple checks per second
- **O(1)** — 1 step ÷ 10⁷/s = 0.0000001 s: instant
- **O(log n)** — 20 steps = 0.000002 s: still instant
- **O(n)** — 1,000,000 steps = 0.1 s: you can feel it
- **O(n²)** — 10¹² steps = 100,000 s ≈ 28 hours: the job is dead

*Example:* From "instant" to "28 hours" with the same data, same machine — only the method changed.

**Key point:** Each jump in shape is not "a bit slower" — it is a different universe: microseconds, tenths of seconds, days.

### Visualization (canvas `c2`, 720×300)

Horizontal log-scale bar chart of wall-clock times.

- **Title (bold 15px, `#1a5276`, centered):** "Wall-Clock Time at 10 Million Checks per Second (log scale)".
- **Rows (bars start at x=185, scale width 380px over log10 seconds −7..5; bar height 24, alpha 0.75; bold 12px row labels at left, bold 12px colored value labels at bar ends):**
  - "dictionary O(1)" — "0.0000001 s", log −7, blue `#2a78d6`.
  - "sorted O(log n)" — "0.000002 s", log10(0.000002), green `#008300`.
  - "scan O(n)" — "0.1 s", log −1, orange `#d95926`.
  - "all pairs O(n²)" — "100,000 s ≈ 28 hours", log 5, magenta `#d55181`.
- **Vertical gridlines (`#e5e9ef`) with gray tick labels:** "1 µs" (−6), "1 ms" (−3), "1 s" (0), "17 min" (3).
- **Top line (bold magenta 13px, centered, y=44 under the title):** "instant → instant → feel it → dead: each shape is a different universe".

## Where a Data Scientist Meets These Shapes

Tags: `where it's used` (blue), `watch out` (orange)

- **The 10× test** — data grows 10×: O(1) work ×1, O(log n) ×1.2, O(n) ×10, O(n²) ×100
- **Dict vs list** — `x in my_set` is O(1); `x in my_list` is an O(n) scan
- **Lookups in a loop** — an O(n) lookup inside an O(n) loop is O(n²) in disguise
- **Joins** — a keyed merge is roughly O(n); matching rows by looping is O(n²)
- **Planning ahead** — the shape tells you today whether next year's data will still run

*Example:* A pipeline fine at 100k rows can be 100× slower at 1M rows — if the shape is n².

**Key point:** Ask "what happens when the data is 10× bigger?" — the shape answers before you run anything.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of work multipliers when data grows 10×.

- **Title (bold 15px, `#1a5276`, centered):** "Data Grows 10× (100k → 1M rows): How Much More Work?".
- **Bars (90px wide, evenly spaced over a 470px plot from x=110, baseline y=235 with thin `#999` line, height scale max 110, min visual bar height 4px, alpha 0.75; bold 15px colored multiplier labels above bars, bold 12px `#333` shape labels below):**
  - "O(1)": ×1, blue `#2a78d6`.
  - "O(log n)": ×1.2, green `#008300`.
  - "O(n)": ×10, orange `#d95926`.
  - "O(n²)": ×100, magenta `#d55181`.
- **Axis caption (gray 12px, centered below labels):** "work multiplier when rows go from 100,000 to 1,000,000".
- **Bottom line (bold magenta 13px, centered, y=288):** "n² pays 100× for 10× data — next year’s data breaks today’s code".

## Constants Matter Less Than Shape

Tags: `common mistake` (red), `rule of thumb` (blue)

- **The worry** — "but the O(n) method does 100 steps per row — isn't that slower?"
- **Race them** — 100·n steps vs n·n steps: at n = 50, the "heavy" 100·n loses (5,000 vs 2,500)
- **The crossover** — at n = 100 they tie; past it, n² pulls away and never looks back
- **At n = 1M** — 100·n = 10⁸ steps, n² = 10¹²: the constant is now a 10,000× loser
- **Why Big-O drops constants** — a constant scales the line; the exponent bends it

*Example:* A slow-per-step O(n) method beats a fast-per-step O(n²) method everywhere past n = 100.

**Common mistake:** Tuning constants on an n² method. At small n anything works; at scale, only the shape survives.

### Visualization (canvas `c4`, 720×300)

Linear-scale line chart: 100·n vs n² with crossover marker.

- **Title (bold 15px, `#1a5276`, centered):** "100·n (heavy steps) vs n² (light steps): Shape Wins".
- **Axes (lines `#999`; padding top 46, bottom 48, left 70, right 170):** x from 0 to 300 with ticks 0, 100, 200, 300 and caption "n = rows"; y from 0 to 90,000 with tick labels "30k", "60k", "90k" (gray 12px).
- **Lines (3px):** straight blue `#2a78d6` line for 100·n from (0,0) to (300, 30,000); magenta `#d55181` curve for n² sampled every 5 from 0 to 300 (n² at 300 = 90,000).
- **Crossover marker at n = 100 (both 10,000 steps):** dashed gray vertical guide (dash 5/4, 1.5px) from the x-axis up to the point, ink `#1a5276` filled dot radius 6, bold 13px label "tie at n = 100" to the right.
- **In-chart annotations (bold 13px):** magenta two-line "n²: light steps," / "loses anyway" near (220, 78,000); blue "100·n: heavy steps, wins at scale" near (120, 30,000) offset below.
- **Legend (top-right, 12px swatches):** blue square "100·n steps"; magenta square "n² steps".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` (full width, border-collapse) with `td.text-col` 50% and `td.viz-col` 50%, one canvas per section.
- **Text column structure:** `.tags` pill row (0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`), `<ul>` of one-line bullets with `<b>` lead terms in `#1a5276`, italic `.example` line (`#555`, 0.9rem), `.key-point` callout (background `#f8f9fa`, 3px `#e74c3c` left border, padding 8px 12px, 0.9rem). Inline `code` in bullets/example: ui-monospace, background `#f4f6f8`, padding 1px 4px, radius 3px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Page palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- No cross-page links; in regenerated HTML any card links elsewhere would use `.html` extensions.
