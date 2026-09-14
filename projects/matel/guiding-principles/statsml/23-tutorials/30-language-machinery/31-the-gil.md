# The GIL

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The GIL

**Subtitle:** Python's Global Interpreter Lock lets only one thread run Python code at a time — so threads speed up waiting, never computing

## One Knife in a Kitchen of Four Cooks

**Tags:** `core idea` (blue), `one at a time` (green), `the lock` (orange)

- **The kitchen** — a food truck hires four cooks to make salads faster, but the kitchen owns one knife
- **Chopping** — only the cook holding the knife can chop; the other three stand around and wait their turn
- **The GIL** — Python has the same rule: one global lock, and only the thread holding it runs Python code
- **Turn taking** — threads swap the lock every few milliseconds, so they take turns but never chop together
- **One core** — on an 8-core laptop, pure-Python threads still do about one core's worth of chopping

*Example (italic):* Four cooks each need 10 minutes of chopping; with one knife the meal still takes 40 minutes — exactly what one cook alone would need.

**Key point:** The GIL is a single lock inside the Python interpreter: many threads can exist, but only the one holding the lock executes Python instructions at any instant.

### Visualization (canvas `c1`, 720×300)

Gantt-style timeline: four cook rows on a shared 0–40 minute time axis; each cook's bar is solid only during their 10-minute knife turn and hollow grey while waiting, so the knife visibly hops from row to row and the total never shrinks.

- **Title (bold 15px, `#1a5276`, top center):** "Four Cooks, One Knife — Dinner Still Takes 40 Minutes".
- **Axis:** horizontal 2px `#999` line at y=250 from x=110 to x=690 (width 580), time 0 to 40 min; 12px `#444` tick labels "0", "10", "20", "30", "40 min" every 10 min; light `#e5e9ef` vertical gridlines at each tick.
- **Rows (y = 85, 128, 171, 214), each with a 12px `#444` label at x=20:** "cook A", "cook B", "cook C", "cook D".
- **Knife turns (solid 16px-tall bars, 3px rounded):** A holds 0–10 (blue `#2a78d6`), B holds 10–20 (green `#008300`), C holds 20–30 (orange `#d95926`), D holds 30–40 (violet `#4a3aa7`); hardcode the four `[start, end]` pairs `[[0,10],[10,20],[20,30],[30,40]]`.
- **Waiting stretches:** for each cook, the rest of their row is a 16px-tall bar outlined 1px `#6b7280` with fill `rgba(107,114,128,0.10)`; one 11px `#6b7280` label "waiting for the knife" inside cook D's 0–30 stretch.
- **Knife hand-offs:** small downward `#1a5276` arrows at 10, 20, 30 min just above the bars, 11px `#6b7280` label "knife passed" above the arrow at 10 min.
- **Annotation (bold 13px magenta `#d55181`, near x=430, y=60):** "one knife → 40 min total, same as one cook".
- **Caption (12px `#444`, bottom right):** "illustrative — each salad needs 10 min of chopping".

## Timing It: Counting vs Downloading

**Tags:** `worked example` (blue), `cpu vs waiting` (orange)

- **The CPU job** — a pure-Python loop counting to 40 million takes about 4.0 s on one thread
- **Split four ways** — four threads counting 10 million each still take 4.1 s: the lock made them queue
- **The waiting job** — fetching four files that each take 3 s takes 12.0 s when done one after another
- **Threads shine** — four threads fetch all four in 3.1 s: a thread waiting on the network drops the lock
- **The rule** — the GIL blocks parallel computing, not parallel waiting

*Example (italic):* Same four threads, opposite outcomes: counting stayed at 4.1 s vs 4.0 s (no gain), downloads fell from 12.0 s to 3.1 s (almost 4× faster).

**Key point:** Four threads cut download time from 12.0 s to 3.1 s but left the counting loop at ~4 s — the GIL is released while a thread waits, never while it computes.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: two task groups (counting to 40 million; downloading 4 files) on the x axis, each with a "1 thread" bar and a "4 threads" bar, y axis in seconds — the download pair collapses, the counting pair does not.

- **Title (bold 15px, `#1a5276`, top center):** "Four Threads Help Downloads, Not Counting".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y = seconds 0 to 13 with 12px `#444` labels "0s", "4s", "8s", "12s" and light `#e5e9ef` gridlines at 4, 8, 12; x = two group labels centered under the pairs (bold 12px `#2c3e50`): "count to 40 million (CPU)" and "download 4 files (waiting)".
- **Bars (each 70px wide, 30px gap inside a pair, ~120px between pairs):** hardcoded seconds `[4.0, 4.1, 12.0, 3.1]` — counting 1 thread 4.0 (mute `#6b7280`, fill `rgba(107,114,128,0.35)`), counting 4 threads 4.1 (blue `#2a78d6`, fill `rgba(42,120,214,0.35)`), download 1 thread 12.0 (mute), download 4 threads 3.1 (green `#008300`, fill `rgba(0,131,0,0.30)`); 2px solid border in each bar's color.
- **Value labels:** bold 13px in each bar's border color, centered above each bar: "4.0 s", "4.1 s", "12.0 s", "3.1 s".
- **Legend (12px, top right):** filled squares + "1 thread" (`#6b7280`) and "4 threads" (`#2a78d6`).
- **Annotation (bold 12px orange `#d95926`, above the counting pair near y=170):** two lines: "4 threads, zero speedup —" / "the lock made them queue".
- **Annotation (bold 12px green `#008300`, beside the 3.1 s bar):** "waiting releases the lock: ~4× faster".
- **Caption (12px `#444`, bottom right):** "illustrative timings on a 4-core laptop".

## Where a Data Scientist Hits the Lock

**Tags:** `where it's used` (blue), `right tool` (green)

- **The trap** — "I'll speed up my pure-Python feature loop with threads" is the classic wasted afternoon
- **Processes** — multiprocessing gives each worker its own interpreter and own lock: real multi-core gains
- **NumPy escape** — big NumPy array math runs in C and releases the GIL; pandas only sometimes
- **I/O work** — scraping, API calls, reading a folder of files: threads (or asyncio) are exactly right
- **Rule of thumb** — CPU-bound Python code wants processes; waiting-bound work wants threads

*Example (italic):* A team put a pure-Python cleaning loop on 4 threads and got 1.0× speedup; switching the same loop to 4 processes gave 3.8×.

**Key point:** Choose workers by what the job spends time on: 4 processes gave the Python loop 3.8×, and threads gave 3.7–3.9× only where C code or network waits released the lock.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: four job setups, each bar showing measured speedup with 4 workers against a dashed "perfect 4×" guide line — only the pure-Python-loop-plus-threads bar stays stuck at 1.

- **Title (bold 15px, `#1a5276`, top center):** "Speedup with 4 Workers — Same Laptop, Four Setups".
- **Axis:** horizontal 2px `#999` line at y=255 from x=250 to x=680 (width 430), speedup 0 to 4.5; 12px `#444` tick labels "0×", "1×", "2×", "3×", "4×" every 1.0; light `#e5e9ef` vertical gridlines at each tick.
- **Rows (bars 22px tall at y = 80, 125, 170, 215), each with a right-aligned 12px `#444` label ending at x=240:** hardcoded speedups `[1.0, 3.8, 3.7, 3.9]` —
  - "pure-Python loop + 4 threads": 1.0, red `#e74c3c`, fill `rgba(231,76,60,0.25)`
  - "pure-Python loop + 4 processes": 3.8, green `#008300`, fill `rgba(0,131,0,0.30)`
  - "NumPy matrix math + 4 threads": 3.7, blue `#2a78d6`, fill `rgba(42,120,214,0.30)`
  - "API downloads + 4 threads": 3.9, aqua `#199e70`, fill `rgba(25,158,112,0.30)`
- **Value labels:** bold 13px in each bar's color at the bar's right end: "1.0×", "3.8×", "3.7×", "3.9×".
- **Guide line:** vertical dashed `#6b7280` (dash 4/3) line at 4.0 from y=60 to the axis, 11px `#6b7280` label "perfect 4×" at its top.
- **Annotation (bold 12px red `#e74c3c`, right of the 1.0× bar near y=80):** "the GIL: threads queued, nothing gained".
- **Caption (12px `#444`, bottom right):** "illustrative — processes and C code sidestep the lock".

## "So Threads Are Useless in Python?"

**Tags:** `common mistake` (red), `released while waiting` (orange)

- **The myth** — the GIL makes people call Python threads pointless; that is only true for pure CPU loops
- **Held vs free** — a downloading thread holds the lock for tiny slivers, then waits with the lock released
- **Mostly free** — in the 3.1 s download run, each thread held the lock well under 1% of the time
- **Not thread safety** — the GIL does not make code safe: two threads can still corrupt a shared counter
- **Changing** — Python 3.13+ offers an optional "free-threaded" build with no GIL, still maturing

*Example (italic):* Zoom into the 3.1 s download run and each thread's lane is almost entirely "waiting, lock free", with only pin-thin stripes of "running Python, lock held".

**Common mistake:** Concluding either "threads never help" or "the GIL makes my code thread-safe" — threads help whenever the job is waiting, and shared data still needs its own locks.

### Visualization (canvas `c4`, 720×300)

Zoomed timeline of the 3.1 s download run: four thread lanes where long light bars mean "waiting — lock released" and thin dark stripes mean "running Python — lock held", showing the lock is free almost the whole time.

- **Title (bold 15px, `#1a5276`, top center):** "Inside the 3.1 s Download Run: the Lock Is Almost Always Free".
- **Axis:** horizontal 2px `#999` line at y=250 from x=110 to x=690 (width 580), time 0 to 3.1 s; 12px `#444` tick labels "0", "1s", "2s", "3s"; light `#e5e9ef` vertical gridlines at each labeled tick.
- **Lanes (16px-tall bars at y = 85, 128, 171, 214), each with a 12px `#444` label at x=20:** "thread 1" … "thread 4"; each lane's base bar spans 0.0–3.05 s in aqua fill `rgba(25,158,112,0.25)` with 1px `#199e70` border (the waiting stretch).
- **Lock-held stripes:** 4px-wide solid ink `#1a5276` vertical stripes on each lane at hardcoded times (seconds) — thread 1 `[0.02, 0.80, 1.60, 2.40, 3.00]`, thread 2 `[0.06, 0.95, 1.75, 2.55]`, thread 3 `[0.10, 1.10, 1.90, 2.70]`, thread 4 `[0.14, 1.25, 2.05, 2.85]` (staggered so no two stripes share a time).
- **Legend (12px, top left):** aqua swatch + "waiting — lock released", ink swatch + "running Python — lock held".
- **Annotation (bold 13px orange `#d95926`, centered near x=430, y=60):** "lock held < 1% of the time — that's why 4 threads finished in 3.1 s".
- **Caption (12px `#444`, bottom right):** "illustrative — stripe widths exaggerated to stay visible".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, timeline intervals, and stripe times are the hardcoded literal arrays above (no `Math.random()`); every timing and speedup is illustrative and captioned as such, and the numbers in the text bullets match the chart values exactly (4.0/4.1/12.0/3.1 s, 1.0/3.8/3.7/3.9×, 40 min, 3.1 s).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
