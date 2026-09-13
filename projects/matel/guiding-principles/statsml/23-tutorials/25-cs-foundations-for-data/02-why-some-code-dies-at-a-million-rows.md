# Why Some Code Dies at a Million Rows

**Page type:** detail page (tutorial layout: h1 + subtitle, then four `.card-section` blocks each with an h2 and a `table.layout` — text column 50% left with tag pills / bullets / example / key-point, viz column 50% right with one 720×300 canvas)
**HTML title tag:** Why Some Code Dies at a Million Rows

**Subtitle:** The same dedup written two ways: a nested loop that needs a trillion comparisons, and a set that needs a million — testing never showed the difference

## One Dedup, Written Two Ways

Tags: `core idea` (blue), `running example` (green)

- **The task** — a file of 1,000,000 emails; drop the duplicates
- **Nested loop** — compare each email against every other: 10⁶ × 10⁶ = 10¹² comparisons
- **Set** — walk the list once, ask "seen before?": 10⁶ checks, each ~1 step
- **Same output** — both produce the identical deduped list
- **Million-fold gap** — 10¹² vs 10⁶: one runs for days, the other in a blink

*Example:* Two loops nested = every row meets every row — a trillion meetings for a million rows.

**Key point:** The nested loop isn't "slower code" — it does a million times more work for the same answer.

### Visualization (canvas `c1`, 720×300)

Split panel: n×n comparison grid on the left, single pass into a set on the right, separated by a vertical dashed light-gray divider (`#bdc3c7`, dash 4/3) at x=360.

- **Title (bold 15px, `#1a5276`, centered):** "Every Row Meets Every Row vs One Pass With a Set".
- **Left panel:** a 10×10 grid of 16px cells starting at (90, 58); diagonal cells filled `#e5e9ef`, off-diagonal cells filled `rgba(213,81,129,0.35)`; magenta `#d55181` 1.5px outer border. Labels centered at x=185: bold magenta 13px "nested loop: n × n cells of work" (y=242); `#444` 12px "10 rows → 100 cells shown here" (y=262); bold magenta 13px "1M rows → 10^12 cells" (y=282).
- **Right panel:** a single row of ten 26×26 boxes starting at (400, 105), fill `rgba(0,131,0,0.30)` with green `#008300` 1px borders; a green 2px downward arrow from the row's center to a white box (140×32, green 2px border) labeled "set: \"seen before?\"" in bold green 12px. Labels centered at x=540: bold green 13px "one pass: n boxes of work" (y=242); `#444` 12px "each check ~1 step (a dictionary lookup)" (y=262); bold green 13px "1M rows → 10^6 checks" (y=282).

## Watching It Grow: 1k, 10k, 100k, 1M Rows

Tags: `worked example` (green)

- **The machine** — assume ~2 million pair-comparisons and ~5 million set checks per second
- **1,000 rows** — loop: ~500k pairs, 0.25 s; set: 0.0002 s — both feel instant
- **10,000 rows** — loop: 25 s (annoying); set: 0.002 s
- **100,000 rows** — loop: ~42 minutes; set: 0.02 s
- **1,000,000 rows** — loop: ~3 days; set: ~0.2 s

*Example:* 10× more rows made the set 10× slower — and made the loop 100× slower, every time.

**Key point:** Redo the math yourself: distinct pairs ≈ n²/2 — half the n×n grid. At n = 10⁶ that is 5×10¹¹ — no hardware upgrade covers a factor of a million.

### Visualization (canvas `c2`, 720×300)

Log-log line chart of dedup runtime vs row count, two series.

- **Title (bold 15px, `#1a5276`, centered):** "Dedup Runtime vs Rows (both axes log scale)".
- **Axes (padding top 46, bottom 48, left 78, right 165; axis lines `#999`):** x = log10(n) from 3 to 6, tick labels "1k", "10k", "100k", "1M", caption "rows"; y = log10(seconds) from −4 to 6 with `#e5e9ef` gridlines at labeled levels "1 ms" (−3), "1 s" (0), "~2 min" (2), "3 days" (log10 250,000).
- **Series (3px lines with 4px dots at each point):**
  - Nested loop, magenta `#d55181`: points at (1k, 0.25 s), (10k, 25 s), (100k, 2,500 s), (1M, 250,000 s).
  - Set, green `#008300`: points at (1k, 0.0002 s), (10k, 0.002 s), (100k, 0.02 s), (1M, 0.2 s).
- **Right-side endpoint labels (bold 13px):** magenta "nested loop: ~3 days at 1M"; green "set: ~0.2 s at 1M".
- **Annotation (bold orange `#d95926` 13px, top-left inside plot):** "every 10× in rows: set ×10, loop ×100".

## "But It Worked Fine in Testing"

Tags: `where it's used` (blue), `watch out` (orange)

- **The trap** — tests run on a 1,000-row sample: the loop takes 0.25 s and ships
- **Production** — real data is 1M rows: 1,000× more rows, 1,000,000× more work
- **The symptom** — "it's been running for six hours" on a job that tested in seconds
- **No warning** — n² code is silent: no error, no crash, just a clock that never stops
- **The fix is cheap** — a set, a dict, or a keyed merge usually removes the inner scan

*Example:* At test size both bars are invisible; at production size one bar is 3 days tall.

**Key point:** A fast test proves nothing about scale — estimate steps at real n before shipping, not after.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart (log-scale heights): test sample vs production, loop vs set.

- **Title (bold 15px, `#1a5276`, centered):** "Same Code, Two Worlds: Test Sample vs Production (log scale)".
- **Scale:** bar height maps log10(seconds) over range −4..6 to 160px; baseline y=230 with thin `#999` line from x=60 to x=660; minimum visual bar height 6px for the set bars; bars 70px wide, pair gap 20px, alpha 0.75.
- **Groups (bold 12px `#333` group labels below baseline):**
  - "testing: 1,000 rows" at x=150 — loop bar 0.25 s (label "0.25 s"), set bar 0.0002 s (label "0.2 ms").
  - "production: 1,000,000 rows" at x=450 — loop bar 250,000 s (label "~3 days"), set bar 0.2 s (label "0.2 s").
  - Loop bars magenta `#d55181`, set bars green `#008300`; bold 13px value labels in bar color above each bar.
- **Legend (top-right, 12px swatches):** magenta "nested loop"; green "set".
- **Bottom line (bold orange `#d95926` 13px, centered, y=285):** "1,000× more rows → 1,000,000× more loop work: the test never saw it coming".

## Finding the Hidden n² Before It Finds You

Tags: `common mistake` (red), `rule of thumb` (blue)

- **Loop in a loop** — the obvious form: `for a in rows: for b in rows:`
- **Disguised form** — `if x in big_list` inside a loop: the `in` is itself a full scan
- **Pandas form** — `df.apply` that searches another dataframe per row
- **Join form** — matching rows by looping instead of `merge` on a key
- **The tell** — "for each row, look through all the rows" said in plain English

*Example:* `x in my_list` and `x in my_set` read identically — one hides a 500,000-step scan.

**Common mistake:** Trusting how the code reads. Describe what it does per row — if the answer mentions "all the rows", it's n².

### Visualization (canvas `c4`, 720×300)

Two-row code-vs-cost comparison with proportional horizontal bars.

- **Title (bold 15px, `#1a5276`, centered):** "The Same \"in\" Keyword: 1M Checks Against 1M Items".
- **Rows (starting x=60, bar track width 540px, row height 92px from y=66; each row shows a bold 13px monospace code line, a gray 12px steps line, then a 22px-tall bar with alpha 0.75 and a bold 13px colored time label at its end):**
  - Code `for x in emails:  if x in seen_list` — steps "avg 500,000 steps per check → 5×10^11 total" — bar full width (fraction 1.0), magenta `#d55181` — time "~3 days".
  - Code `for x in emails:  if x in seen_set` — steps "~1 step per check → 10^6 total" — bar fraction 0.004 (minimum 6px), green `#008300` — time "~0.2 s".
- **Bottom line (bold orange `#d95926` 13px, centered, y=275):** "one word of difference in the code — a factor of a million in the work".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` (full width, border-collapse) with `td.text-col` 50% and `td.viz-col` 50%, one canvas per section.
- **Text column structure:** `.tags` pill row (0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`), `<ul>` of one-line bullets with `<b>` lead terms in `#1a5276`, italic `.example` line (`#555`, 0.9rem), `.key-point` callout (background `#f8f9fa`, 3px `#e74c3c` left border, padding 8px 12px, 0.9rem). Inline `code`: ui-monospace, background `#f4f6f8`, padding 1px 4px, radius 3px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Page palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- No cross-page links; in regenerated HTML any card links elsewhere would use `.html` extensions.
