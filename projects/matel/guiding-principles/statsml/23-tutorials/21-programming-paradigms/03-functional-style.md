# Functional Style

**Page type:** detail page (tutorial card-sections: h2 + two-column table.layout, text left 50%, canvas/code right 50%)
**HTML title tag:** Functional Style

**Subtitle:** Clean a list of prices as an assembly line of small functions — each takes data in and hands new data out, touching nothing else

## Cleaning Prices: One Mutating Loop vs Three Small Functions

**Tags:** `core idea` (blue pill), `running example` (green pill)

- **The mess** — a day of prices: [20, -5, 30, 0, 25, 999, 15] with junk mixed in
- **The job** — drop bad entries, apply the 10% discount, total the day's sales
- **Loop way** — one loop that deletes, overwrites, and adds to a running total
- **Functional way** — `filter` (keep good), `map` (transform each), `reduce` (combine all)
- **Assembly line** — each stage takes a list in, hands a NEW list out; nothing is edited in place

*Example:* filter is the quality inspector, map is the machine that stamps each item, reduce is the cashier.

**Key point:** Functional style splits "clean the prices" into three named, reusable steps — data flows through functions instead of being edited in place.

Code payload (`.payload` monospace block below the canvas, verbatim):

```
# Mutating loop — edits as it goes
total = 0
for p in prices:
    if 0 < p < 500:
        total += p * 0.9      # state changes each pass

# Functional pipeline — new data at each stage
valid      = filter(lambda p: 0 < p < 500, prices)
discounted = map(lambda p: p * 0.9, valid)
total      = reduce(add, discounted)   # 81.0
```

### Visualization (canvas `c1`, 720×300)

Vertical pipeline flow diagram with the real arrays inside each stage box.

- **Title (bold 15px, `#1a5276`, top center):** "The Assembly Line, With the Real Numbers on It".
- **Four centered stage boxes** (340×40, starting y=40, 22px gaps, connected by downward arrows colored like the next stage). Each box: bold 12px stage label, monospace 12px data line:
  1. "raw prices" (mute `#6b7280`, fill `#f6f8fa`) — `[20, -5, 30, 0, 25, 999, 15]`
  2. "filter: keep 0 < p < 500" (blue `#2a78d6`, fill `#eef4fb`) — `[20, 30, 25, 15]`
  3. "map: p × 0.9" (aqua `#199e70`, fill `rgba(25,158,112,0.08)`) — `[18, 27, 22.50, 13.50]`
  4. "reduce: sum" (green `#008300`, fill `rgba(0,131,0,0.08)`) — `81.0`
- **Right side annotation (bold magenta `#d55181` 12px, three lines):** "each arrow =" / "a NEW list," / "old one untouched".
- **Left side annotation (mute 12px, two lines, right-aligned):** "-5, 0, 999" / "dropped here".

## Trace the Seven Prices by Hand

**Tags:** `worked example` (green pill)

- **Start** — [20, -5, 30, 0, 25, 999, 15]: seven entries, three of them junk
- **filter** — keep 0 < p < 500: drops -5, 0, and the 999 typo, leaving [20, 30, 25, 15]
- **map** — apply the 10% discount: [18, 27, 22.50, 13.50]
- **reduce** — add them up: 18 + 27 + 22.50 + 13.50 = 81
- **Nothing lost** — the raw list [20, -5, 30, 0, 25, 999, 15] still exists, untouched

*Example:* Check by hand: 20 + 30 + 25 + 15 = 90, and 90 × 0.9 = 81 — the pipeline agrees.

**Key point:** Every intermediate list is real and inspectable — when the total looks wrong, you can print each stage and see exactly where it went wrong.

### Visualization (canvas `c2`, 720×300)

Grouped per-item bar chart: raw price bar next to discounted bar for each of the 7 items; dropped items marked with a red ✗.

- **Title (bold 15px, `#1a5276`):** "Every Item, Traced: Kept and Discounted, or Dropped".
- **Data:** raw `[20, -5, 30, 0, 25, 999, 15]`; kept flags `[true, false, true, false, true, false, true]`; mapped values `[18, —, 27, —, 22.5, —, 13.5]`.
- **Axes:** L-shaped gray `#999` axis; padding top 56, bottom 62, left 56, right 20; y display cap 35 (the 999 bar is drawn clipped to the cap with a white break stroke across it); 7 equal x slots labeled "#1"…"#7" in mute below the baseline.
- **Bars (24px wide, pairs per slot):** left bar = raw value, fill `rgba(42,120,214,0.45)` if kept, `rgba(107,114,128,0.30)` if dropped (only drawn when value > 0), raw value labeled above; right bar = mapped value in green `#008300` with bold green value label, or a bold red `#e74c3c` "✗" where the item was dropped.
- **Legend (top left):** blue swatch "raw price", green swatch "after ×0.9", bold red "✗ dropped by filter".
- **Bottom caption (bold green 13px, centered):** "green bars sum to 18 + 27 + 22.50 + 13.50 = 81".

## Pure Functions: Same Input, Same Output, Safe to Retry

**Tags:** `where it's used` (blue pill), `reliability` (orange pill)

- **Pure** — output depends only on input; no globals read, nothing outside changed
- **Retry-safe** — a pure stage run twice gives 81 twice; rerunning costs nothing
- **The impure trap** — a stage that adds into a global total gives 162 on retry
- **Pipelines retry constantly** — jobs crash mid-run; Spark and Airflow rerun failed steps
- **Parallel for free** — pure map on 4 items can run on 4 machines, no coordination

*Example:* A network blip makes the scheduler rerun the totals job — pure: still 81; impure: 162, silently.

**Key point:** Data pipelines love pure functions because failure is normal — a step that can be rerun without changing the answer makes retries free instead of dangerous.

### Visualization (canvas `c3`, 720×300)

Split panel comparing rerun behavior: pure vs impure, three run rows each.

- **Title (bold 15px, `#1a5276`):** "The Job Crashes and Reruns — What Happens to the Total?".
- **Divider:** vertical dashed `#bdc3c7` (4/3) at x=360.
- **Left panel (center x=185), header bold green `#008300`:** "Pure: total = sum(discounted)". Three run rows, each a gray label box → arrow → result box (fill `rgba(0,131,0,0.08)`, stroke green): "run 1" → "81", "retry (crash)" → "81", "retry again" → "81". Below: bold green "same input, same output — every time"; mute "reads nothing outside, writes nothing outside".
- **Right panel (center x=540), header bold red `#e74c3c`:** "Impure: global_total += batch". Rows (result boxes fill `rgba(231,76,60,0.08)`, stroke red): "run 1" → "81", "retry (crash)" → "162", "retry again" → "243". Below: bold red "each rerun adds again: 81, 162, 243"; mute "leftover state from the last run poisons the next".
- **Bottom caption (bold violet `#4a3aa7` 13px, centered, y=288):** "pipelines rerun failed steps all the time — pure steps make that free".

## The Common Confusion: Functional Does Not Mean "No Loops"

**Tags:** `common mistake` (red pill), `trade-off` (orange pill)

- **Loops still exist** — map and reduce loop internally; the loop is contained, not banned
- **The real rule** — no side effects: don't edit shared state while looping
- **Copies cost memory** — new lists at each stage; fine for 7 prices, a choice at 7 billion
- **You use it already** — `df[df.p > 0]` is filter, `df.p * 0.9` is map, `.sum()` is reduce
- **Chaining is the style** — pandas method chains and Spark jobs are functional pipelines

*Example:* `df[df.price > 0].price.mul(0.9).sum()` is the whole running example in one pandas chain.

**Key point:** The point is not avoiding loops — it is that each stage returns new data and touches nothing else, so stages compose, retry, and parallelize safely.

### Visualization (canvas `c4`, 720×300)

Split panel: mutating overwrite (one list, before/after) vs functional pipeline (all stages preserved).

- **Title (bold 15px, `#1a5276`):** "Edit in Place vs Keep the Original".
- **Divider:** vertical dashed `#bdc3c7` (4/3) at x=360.
- **Left panel (center x=185), header bold orange `#d95926`:** "Mutating loop: one list, overwritten". Two boxes 280×38 with left-aligned labels and monospace data: "before" (mute, fill `#f6f8fa`) `[20, -5, 30, 0, 25, 999, 15]`; "after" (orange, fill `#fdf0e6`) `[18, 27, 22.50, 13.50]`; a red `#e74c3c` downward arrow between them. Bold red caption: "raw data is gone — was 999 a typo for 99? can't check now"; mute caption: "rerunning the loop on this list double-discounts".
- **Right panel (center x=540), header bold green `#008300`:** "Pipeline: every stage still exists". Three boxes 280×38 (fill `#f6f8fa`): "raw" (mute) `[20, -5, 30, 0, 25, 999, 15]`; "valid" (blue `#2a78d6`) `[20, 30, 25, 15]`; "discounted" (green) `[18, 27, 22.50, 13.50]`. Bold green caption: "audit any stage, rerun any stage — the raw list is intact".
- **Bottom caption (bold violet 13px, centered, y=282):** "the trade: copies cost memory; the win: nothing is ever lost mid-pipeline".

## Regeneration instructions

- **Layout:** tutorial detail page — h1 + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%). Text column: `.tags` pill row, `<ul>` of one-line bullets each opening with `<b>` (bold term in `#1a5276`), an italic `.example` line, and a `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`). Viz column: one canvas per section; section 1 also has a `.payload` `<pre>` under the canvas (background `#f8f9fa`, left border 3px solid `#1a5276`, monospace 0.78em).
- **Tag pills:** 0.72rem bold, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`. Inline `code` in monospace on `#f4f6f8`.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` underline; `.subtitle` `#666` 0.95rem. Canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
