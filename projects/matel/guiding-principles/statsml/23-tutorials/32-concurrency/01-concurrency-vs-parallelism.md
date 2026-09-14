# Concurrency vs Parallelism

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Concurrency vs Parallelism

**Subtitle:** Concurrency is one worker juggling many tasks; parallelism is many workers each doing a task at the same instant — dealing with many vs doing many at once

## One Barista, Four Orders

**Tags:** `core idea` (blue), `juggling vs teamwork` (green), `coffee shop` (orange)

- **The rush** — four customers order lattes at once; the shop has one barista and a four-slot machine
- **Juggling** — the barista starts order 1, and while its shot brews she preps order 2, then 3, then 4
- **One pair of hands** — at any single instant she is doing exactly one thing; she just never stands idle
- **Teamwork** — hire three more baristas and four orders literally progress at the same instant
- **The names** — the juggling is concurrency (dealing with many); the teamwork is parallelism (doing many)

*Example (italic):* One barista interleaving four lattes is concurrent; four baristas pouring four lattes simultaneously is parallel.

**Key point:** Concurrency is a way of structuring work so many tasks make progress by taking turns; parallelism is executing several tasks at literally the same moment — you can have either one without the other.

### Visualization (canvas `c1`, 720×300)

Two-row timeline diagram: one interleaved track for the single barista (concurrency) vs four simultaneous short tracks for four baristas (parallelism).

- **Title (bold 15px, `#1a5276`, top center):** "Dealing With Many vs Doing Many at Once".
- **Row 1 label (12px `#444`, x=20, y=78):** "concurrency — 1 barista"; one bar at y=88, x=60 to 660, height 22, split into 12 equal 50px segments cycling order colors `[A,B,C,D,A,B,C,D,A,B,C,D]` where A=blue `#2a78d6`, B=green `#008300`, C=yellow `#c98500`, D=violet `#4a3aa7`; 11px white letters "A B C D..." centered in each segment.
- **Row 2 label (12px `#444`, x=20, y=158):** "parallelism — 4 baristas"; four bars at y = `[168, 194, 220, 246]`, each x=60, width 150, height 18, solid A/B/C/D colors with 11px white letter labels; dashed `#6b7280` vertical line (dash 4/3) at x=210 with 12px `#6b7280` label "all four finish here".
- **Annotation (bold 13px ink `#1a5276`, x≈300, y=210):** "one pair of hands takes turns; four pairs work at the same instant".
- **Caption (12px `#444`, bottom right):** "segment lengths schematic, illustrative".

## Timing the Coffee Rush

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **One order** — 20s hands-on prep, then 60s machine brew (barista free), then 20s hands-on serve
- **Naive sequential** — the barista stands watching each brew: 4 × (20+60+20) = 400s for four orders
- **Concurrent** — she preps orders 2–4 during brews; hands-on work is 4 × 40s = 160s total
- **Hand-check** — prep 1 at 0–20s, prep 2 at 20–40s, prep 3 at 40–60s, prep 4 at 60–80s, then serve 1–4 back to back at 80–160s
- **Parallel** — four baristas, four machines: every order runs 0–100s; the rush ends at 100s

*Example (italic):* The same four lattes take 400s watching the machine, 160s with one juggling barista, and 100s with four baristas.

**Key point:** Concurrency wins back the waiting time (400s → 160s) without adding staff; parallelism buys the last cut (160s → 100s) but only by adding hands.

### Visualization (canvas `c2`, 720×300)

Gantt chart of the single concurrent barista: four order lanes, each with a solid prep segment, a pale unattended brew segment, and a solid serve segment on a shared 0–160s axis.

- **Title (bold 15px, `#1a5276`, top center):** "One Barista, Four Lattes: 160 Seconds Instead of 400".
- **Axes:** origin x=60, baseline y=255, plot width 600 (scale 3.75 px per second); x ticks every 40s at `[0, 40, 80, 120, 160]` labeled "0s"–"160s" (12px `#444`); light gridlines `#e5e9ef` at each tick.
- **Lanes (height 22) at y = `[75, 120, 165, 210]`, left labels "order 1"–"order 4" (12px `#444`, x=8):**
  - order 1: prep `[0, 20]` solid blue `#2a78d6`, brew `[20, 80]` fill `rgba(42,120,214,0.15)` with 1px `#6b7280` border, serve `[80, 100]` solid green `#008300`
  - order 2: prep `[20, 40]`, brew `[40, 100]`, serve `[100, 120]` (same styles)
  - order 3: prep `[40, 60]`, brew `[60, 120]`, serve `[120, 140]`
  - order 4: prep `[60, 80]`, brew `[80, 140]`, serve `[140, 160]`
- **Legend (11px, top right under title):** blue swatch "hands-on prep", pale swatch "machine brews (barista free)", green swatch "hands-on serve".
- **Annotation (bold 13px green `#008300`, near x=430, y=62):** "brews overlap with prep — 400s of watching becomes 160s".
- **Caption (12px `#444`, bottom right):** "20s/60s/20s step times illustrative".

## Waiting Work vs Number-Crunching Work

**Tags:** `where it's used` (blue), `I/O vs CPU` (green), `data pipelines` (orange)

- **Waiting work** — API calls, database queries, file downloads: the task mostly waits, like the brew
- **Crunching work** — matrix math, model training, parsing: the task uses the CPU every moment
- **Downloads** — 8 files × 10s of waiting each: sequential 80s; concurrent on one core about 11s
- **Crunching** — 8 chunks × 10s of pure CPU: concurrency on one core still about 80s (82s with switching)
- **Cores help crunching** — the same 8 chunks on 4 cores in parallel finish in about 20s

*Example (italic):* A feature pipeline fetching 8 API pages drops from 80s to 11s with async concurrency, but its 80s of model scoring only drops (to ~20s) when spread across 4 cores.

**Key point:** Match the tool to the work — concurrency erases waiting time (I/O-bound), parallelism divides computing time (CPU-bound); using the wrong one changes nothing.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: total time for an I/O-bound job vs a CPU-bound job under three strategies (sequential, concurrent on 1 core, parallel on 4 cores).

- **Title (bold 15px, `#1a5276`, top center):** "Concurrency Erases Waiting; Parallelism Divides Crunching".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 175; y = seconds 0 to 90, gridlines `#e5e9ef` at 20/40/60/80 with 12px `#444` labels.
- **Group 1 (centered x≈210), label "I/O-bound: 8 downloads" (12px `#444` below baseline):** three 44px-wide bars with 12px gaps, heights from values `[80, 11, 20]` — sequential mute `#6b7280`, concurrent blue `#2a78d6`, parallel green `#008300`; 12px value labels "80s", "11s", "20s" above bars.
- **Group 2 (centered x≈480), label "CPU-bound: 8 chunks":** same bar styles, values `[80, 82, 20]`, labels "80s", "82s", "20s".
- **Legend (11px, top right):** mute swatch "sequential", blue swatch "concurrent, 1 core", green swatch "parallel, 4 cores".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=75):** "concurrency alone leaves CPU work at 82s".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Interleaving Is Not the Same as Extra Hands

**Tags:** `common mistake` (red), `threads` (orange)

- **The confusion** — "I added threads, so it runs in parallel now" — not if there is only one core
- **One core** — threads on a single core take turns, exactly like the lone barista; nothing overlaps
- **The overhead** — every switch costs a little, so more threads can make CPU work slower
- **Measured** — a 60s crunch job on one core: 1 thread 60s, 2 threads 61s, 4 threads 62s, 8 threads 64s
- **Real hands** — the same job on 4 cores with 4 threads drops to about 16s — cores, not threads, cut it

*Example (italic):* Splitting a 60s single-core computation across 8 threads yields 64s — four seconds slower — while 4 real cores yield 16s.

**Common mistake:** Treating threads as speed. Threads give you concurrency (turn-taking); only extra cores give you parallelism — for pure computation, turn-taking adds switching cost and zero overlap.

### Visualization (canvas `c4`, 720×300)

Bar chart: wall-clock time of a 60-second CPU-bound job as thread count grows on one core, plus one contrasting bar for 4 threads on 4 real cores.

- **Title (bold 15px, `#1a5276`, top center):** "More Threads on One Core: a 60s Job Gets Slower, Not Faster".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 175; y = seconds 0 to 70, gridlines `#e5e9ef` at 15/30/45/60 with 12px `#444` labels.
- **Bars (56px wide, centers at x = `[140, 240, 340, 440, 580]`), values `[60, 61, 62, 64, 16]`:** first four bars for "1 core" with 1 thread blue `#2a78d6`, 2 threads yellow `#c98500`, 4 threads orange `#d95926`, 8 threads red `#e74c3c`; fifth bar "4 cores, 4 threads" green `#008300`; 12px value labels "60s", "61s", "62s", "64s", "16s" above bars; x labels "1 thr", "2 thr", "4 thr", "8 thr", "4 cores" (12px `#444`).
- **Divider:** vertical dashed `#6b7280` line (dash 4/3) at x=515 separating the one-core group from the four-core bar; 12px `#6b7280` labels "one core" (left) and "four cores" (right) near y=60.
- **Annotation (bold 13px red `#e74c3c`, near x=250, y=80):** "switching adds cost, not hands".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness) and are invented, labeled illustrative — coffee step times 20s/60s/20s giving totals 400s sequential / 160s concurrent / 100s parallel; Gantt segments per order `[0,20]/[20,80]/[80,100]`, `[20,40]/[40,100]/[100,120]`, `[40,60]/[60,120]/[120,140]`, `[60,80]/[80,140]/[140,160]`; grouped bars I/O `[80, 11, 11]` and CPU `[80, 82, 20]` seconds; thread-count bars `[60, 61, 62, 64, 16]` seconds.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
