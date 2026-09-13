# Vectorized Thinking

**Page type:** detail page (tutorial card-sections: h2 + two-column table.layout, text left 50%, canvas/code right 50%)
**HTML title tag:** Vectorized Thinking

**Subtitle:** Adding tax to a million prices: say what should happen to the whole column, instead of looping over the rows yourself

## A Million Prices, Two Ways to Add Tax

**Tags:** `core idea` (blue pill), `running example` (green pill)

- **The job** — a shop has 1,000,000 prices and every one needs 8% tax added
- **The loop way** — visit each price, multiply by 1.08, one row per trip: ~8 s (illustrative)
- **The vectorized way** — `prices * 1.08`, one command for the whole column: ~0.02 s
- **Same answer** — both produce exactly the same million taxed prices
- **The shift** — stop thinking "for each row, do X" and start thinking "do X to the column"

*Example:* A cashier taxing receipts one by one vs a stamp that taxes the whole stack in one press.

**Key point:** Vectorized code states WHAT happens to the column; the library decides HOW to visit the rows.

### Visualization (canvas `c1`, 720×300)

Split panel: row-by-row loop (left) vs whole-column operation (right).

- **Title (bold 16px, `#1a5276`, top center):** "Same Job, Two Shapes of Code".
- **Divider:** vertical dashed grid-gray `#e5e9ef` (4/3) at x=360.
- **Left panel (center x=180), header bold orange `#d95926`:** "Loop: one row per trip". Six small stacked row boxes 70×22 (fill `#f4f6f8`, stroke mute `#6b7280`) with values "100", "250", "40", "80", "30", "..." — the second box highlighted `rgba(217,89,38,0.25)`. An orange arrow points from a circular orange loop symbol (labeled bold "Python" / "interpreter") back to the highlighted row. Text "x 1,000,000 trips" and bold orange "~8 s" below the loop. Bottom mute caption: "each trip: fetch row, check type," / "multiply, put it back".
- **Right panel (center x=545), header bold green `#008300`:** "Vectorized: whole column at once". Column box 100×130 (fill `rgba(42,120,214,0.15)`, stroke blue `#2a78d6`) labeled bold blue "prices" with "1,000,000" / "rows"; green arrow labeled bold "* 1.08" to a second column box (fill `rgba(0,131,0,0.12)`, stroke green) labeled "with_tax" with "1,000,000" / "rows". Bold green "~0.02 s" below.
- **Bottom captions (centered at x=545):** bold magenta `#d55181` 13px "same answer, ~400x faster (illustrative)"; mute 12px "one call crosses into fast compiled code".

## Five Prices You Can Check by Hand

**Tags:** `worked example` (green pill)

- **The column** — five prices: 100, 250, 40, 80, 30
- **The operation** — multiply the whole array by 1.08 in one line
- **The result** — 108.0, 270.0, 43.2, 86.4, 32.4 — check any row on paper
- **No index, no loop** — the code never mentions row 0, row 1, row 2
- **Scales silently** — the identical line works on 5 rows or 5 million

*Example:* Row 4 by hand: 80 × 1.08 = 86.4 — same as the array answer.

**Key point:** One operation, applied to every row at once — the row count appears nowhere in the code.

Code payload (`pre.code` monospace block below the canvas, verbatim):

```
import numpy as np

prices   = np.array([100, 250, 40, 80, 30])
with_tax = prices * 1.08
# array([108., 270.,  43.2,  86.4,  32.4])
```

### Visualization (canvas `c2`, 720×300)

Column-to-column mapping: five input value boxes with parallel arrows to five output boxes.

- **Title (bold 16px, `#1a5276`):** "prices * 1.08 — Every Row, One Command".
- **Left column (x=150, width 110), header bold blue `#2a78d6`:** "prices". Five boxes 110×26 (fill `rgba(42,120,214,0.15)`, stroke blue), values: 100, 250, 40, 80, 30.
- **Right column (x=470), header bold green `#008300`:** "with_tax". Five boxes (fill `rgba(0,131,0,0.12)`, stroke green), values: 108.0, 270.0, 43.2, 86.4, 32.4.
- **Arrows:** one violet `#4a3aa7` arrow per row from left box to right box; shared bold violet 14px operation label "x 1.08" centered above the arrows.
- **Bottom captions (centered):** bold magenta `#d55181` 13px "one operation, five rows — or a million; the code is identical"; mute 12px "check row 4 by hand: 80 x 1.08 = 86.4".

## Where a Data Scientist Meets This Every Day

**Tags:** `where it's used` (blue pill), `performance` (orange pill)

- **pandas** — `df.price * 1.08` is the column version; a `for` over rows is the slow one
- **Hidden loop** — `df.apply(lambda r: ...)` still visits rows one by one: ~6 s here
- **SQL and Spark** — `SELECT price * 1.08` is vectorized thinking with different spelling
- **Feature engineering** — a 40-feature build that loops can turn minutes into hours
- **Smell test** — if you wrote `for row in df.iterrows()`, look for the column version first

*Example:* The same tax job: plain loop ~8 s, df.apply ~6 s, numpy one-liner ~0.02 s (illustrative).

**Key point:** apply() looks vectorized but is a row-by-row loop in disguise — the shape of the code is not the speed of the code.

### Visualization (canvas `c3`, 720×300)

Horizontal timing bar chart: three approaches, seconds of wall-clock time.

- **Title (bold 16px, `#1a5276`):** "Add 8% Tax to 1,000,000 Prices (illustrative timings)".
- **Bars** (30px tall, from x=200, max width 430 scaled to 8 s, value labels bold to the right, row labels right-aligned to the left):
  - "Python for loop" — 8 s, orange `#d95926`
  - "df.apply(...)" — 6 s, yellow `#c98500`
  - "numpy: prices * 1.08" — 0.02 s (minimum 4px width), green `#008300`
- **Axis:** vertical mute `#6b7280` line at x=200.
- **Bottom captions (centered):** bold magenta `#d55181` 14px "apply() is a loop in disguise — only the last line is vectorized"; mute 12px "seconds of wall-clock time; shorter is better".

## The Loop Didn't Disappear — It Moved

**Tags:** `common mistake` (red pill), `core idea` (blue pill)

- **Not magic** — numpy still loops over all million rows, just inside fast compiled C
- **The real cost** — each Python loop step re-checks types and unwraps the number
- **Tiny arithmetic** — the multiply itself is nanoseconds; the interpreter overhead dwarfs it
- **Same work, faster worker** — vectorizing removes per-row overhead, not the per-row work
- **When loops are fine** — 1,000 items once? The loop takes milliseconds; write the clear version

*Example:* Per row: the multiply costs ~20 ns; Python's bookkeeping around it costs ~8 µs (illustrative).

**Common confusion (key-point callout):** "numpy skips the loop." It doesn't — it moves the loop into compiled code where each step is cheap.

### Visualization (canvas `c4`, 720×300)

Stacked horizontal bars: per-row cost breakdown, overhead vs arithmetic.

- **Title (bold 16px, `#1a5276`):** "Cost of ONE Row, Up Close (illustrative)".
- **Row 1 "Python loop"** (y=74, 30px tall, from x=190, scale 8,000 ns over 440px): orange `#d95926` segment ~7,980 ns wide with white bold in-bar label "interpreter bookkeeping ~8 µs (8,000 ns)", then a blue `#2a78d6` segment 20 ns wide (min 3px).
- **Row 2 "numpy (C loop)"** (y=148): blue segment 20 ns only, bold dark label to the right: "~20 ns: just the multiply".
- **Legend:** orange swatch "type checks, boxing, loop machinery"; blue swatch "the actual multiply".
- **Bottom captions (centered):** bold magenta `#d55181` 14px "the multiply was never the slow part — the wrapper was"; mute 12px "both versions loop over all rows; only one pays ~8 µs of overhead per row".

## Regeneration instructions

- **Layout:** tutorial detail page — h1 + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%). Text column: `.tags` pill row, `<ul>` of one-line bullets each opening with `<b>` (bold term in `#1a5276`), an italic `.example` line, and a `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`). Viz column: one canvas per section; section 2 also has a `pre.code` block under the canvas (background `#f8f9fa`, left border 3px solid `#1a5276`, monospace 0.78rem).
- **Tag pills:** 0.72rem bold, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`. Inline `code` in monospace on `#f4f6f8`.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` underline; `.subtitle` `#666` 0.95rem. Canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 (read from width/height attributes), scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
