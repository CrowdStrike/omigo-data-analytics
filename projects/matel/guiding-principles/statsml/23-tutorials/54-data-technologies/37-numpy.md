# NumPy

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** NumPy

**Subtitle:** NumPy stores a million numbers as one solid block of memory and runs compiled loops over it — the array under pandas, scikit-learn, and everything else

## Summing a Million Numbers Two Ways

**Tags:** `core idea` (blue), `vectorization` (green), `10-100× faster` (orange)

- **The readings** — a weather station logs 1,000,000 temperature readings and you want their sum
- **The loop** — `for x in data: total += x` runs Python bytecode a million times, one boxed float each
- **The array** — `np.array(data)` packs all million values into one contiguous block of raw 8-byte floats
- **One call** — `np.sum(a)` hands the whole block to a single compiled C loop; no bytecode per element
- **The payoff** — the loop takes ~80 ms, `np.sum` takes ~0.8 ms — about 100× (illustrative)

*Example (italic):* Same million numbers, same answer — 80 ms with the Python loop, 0.8 ms with one np.sum call (timings illustrative).

**Key point:** NumPy is fast because one Python call dispatches a whole array to a compiled loop over contiguous typed memory — the per-element interpreter overhead disappears.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: time to sum 1,000,000 floats three ways, log-feel via hardcoded pixel widths.

- **Title (bold 15px, `#1a5276`, top center):** "Summing 1,000,000 Floats: One Compiled Loop Beats a Million Bytecode Steps".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 90, 150, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "Python for loop — 80 ms": red `#e74c3c` bar width 440
  - "built-in sum(list) — 8 ms": orange `#d95926` bar width 200
  - "np.sum(array) — 0.8 ms": green `#008300` bar width 60
- **Bar style:** 18px tall, solid fills, 11px `#444` time labels just past each bar end.
- **Annotation (bold 13px green `#008300`, right side near y=250):** "100× faster — same numbers, same answer".
- **Caption (12px `#444`, bottom right):** "timings illustrative, bar widths schematic; the 10–100× ratio is typical".

## Centering a Matrix With No Loop at All

**Tags:** `worked example` (blue), `broadcasting` (green)

- **The matrix** — 3 sensors × 4 hours of readings: rows [10,20,30,40], [20,30,40,50], [30,40,50,60]
- **Column means** — `X.mean(axis=0)` gives one mean per hour: [20, 30, 40, 50], shape (4,)
- **The subtraction** — `X - X.mean(axis=0)` runs even though the shapes differ: (3,4) minus (4,)
- **The rule** — align shapes from the right; a missing or length-1 dimension is stretched, nothing copied
- **Hand-check** — row 1: [10,20,30,40] − [20,30,40,50] = [−10,−10,−10,−10] (exact)

*Example (italic):* The full centered matrix is rows [−10,−10,−10,−10], [0,0,0,0], [10,10,10,10] — one expression, zero Python loops (exact).

**Key point:** Broadcasting compares shapes right-to-left; dimensions are compatible if equal or one of them is 1 — the smaller array is virtually repeated, so one line replaces a nested loop.

### Visualization (canvas `c2`, 720×300)

Grid diagram of the broadcast subtraction: the 3×4 matrix, minus a 1×4 mean row shown virtually stretched, equals the 3×4 centered result.

- **Title (bold 15px, `#1a5276`, top center):** "Broadcasting: (3,4) − (4,) — the Mean Row Is Stretched, Not Copied".
- **Left grid (X):** 3×4 cells, each 52×38, top-left at (40, 90); values row-major `[10,20,30,40, 20,30,40,50, 30,40,50,60]`; fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` borders, 12px `#2c3e50` centered numbers; bold 12px `#2a78d6` label "X — shape (3, 4)" above at y=78.
- **Minus sign:** bold 20px `#2c3e50` "−" at (262, 152).
- **Mean row:** 1×4 cells, each 40×38, top-left at (285, 128); values `[20,30,40,50]`; fill `rgba(0,131,0,0.12)`, 1px `#008300` borders; bold 12px `#008300` label "mean — shape (4,)" above at y=78; dashed 1px `#008300` ghost rectangles (same size, no numbers) at y=90 and y=166 hinting the virtual repeat, 11px `#008300` label "virtually repeated 3×" below at y=222.
- **Equals sign:** bold 20px `#2c3e50` "=" at (458, 152).
- **Result grid:** 3×4 cells, each 52×38, top-left at (472, 90); values row-major `[-10,-10,-10,-10, 0,0,0,0, 10,10,10,10]`; fill `rgba(74,58,167,0.10)`, 1px `#4a3aa7` borders; bold 12px `#4a3aa7` label "centered — shape (3, 4)" above at y=78.
- **Annotation (bold 12px orange `#d95926`, centered near y=262):** "no loop written, no data copied".
- **Caption (12px `#444`, bottom right):** "values exact".

## The Array Under the Whole Stack

**Tags:** `where it's used` (blue), `ecosystem` (green)

- **pandas** — a DataFrame column is (by default) a NumPy array with labels wrapped around it
- **scikit-learn** — `fit(X, y)` expects X as a NumPy array (or something convertible to one)
- **SciPy** — stats, optimize, and linear algebra routines all operate on ndarrays
- **BLAS underneath** — matrix products call the same tuned C/Fortran libraries MATLAB and R use
- **The imitators** — PyTorch tensors and JAX arrays copy the NumPy API on purpose (`torch.sum`, `jnp.sum`)

*Example (italic):* When `df["price"].mean()` runs, pandas hands a NumPy array to a compiled loop — you were using NumPy all along.

**Key point:** An ndarray is one typed memory block plus shape and strides; that one data structure is the shared substrate of scientific Python — learn its shape/dtype/axis vocabulary once and pandas, scikit-learn, SciPy, PyTorch, and JAX all read the same.

### Visualization (canvas `c3`, 720×300)

Layer-stack diagram: three library boxes on top, the ndarray layer in the middle, compiled loops at the bottom, with a dashed side note for the API imitators.

- **Title (bold 15px, `#1a5276`, top center):** "One Array, One Ecosystem: What Sits on the ndarray".
- **Top row (y=60, boxes 160×40, 8px radius):** "pandas" at x=60, "scikit-learn" at x=280, "SciPy" at x=500; fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` borders, 13px `#2c3e50` centered text; 2px `#6b7280` arrows from each box bottom down to y=140.
- **Middle layer (y=140):** wide rounded box x=60, width 600, height 44; fill `rgba(0,131,0,0.12)`, 1px `#008300` border; bold 13px `#2c3e50` centered text "NumPy ndarray — one typed memory block + shape + strides"; 2px `#6b7280` arrow from its bottom center down to y=224.
- **Bottom layer (y=224):** wide rounded box x=60, width 600, height 44; fill `rgba(230,126,34,0.15)`, 1px `#e67e22` border; 13px `#2c3e50` centered text "compiled C / BLAS loops over contiguous memory".
- **Side note:** dashed 1px `#4a3aa7` rounded box 150×40 at (555, 110) — drawn to the upper right, overlapping no layer text — labeled "PyTorch / JAX" (12px `#4a3aa7`), with bold 12px `#4a3aa7` caption "same API, on purpose" just below it.
- **Caption (12px `#444`, bottom right):** "schematic".

## The Slice That Changed the Original

**Tags:** `common mistake` (red), `views vs copies` (orange)

- **The slice** — `b = a[:3]` makes no new data: b is a view — same block, new shape/strides/offset
- **The mutation** — `b[0] = 99` writes into the shared block, so `a[0]` is now 99 too
- **Why views exist** — slicing a 10-million-row array would be crushing if every slice copied
- **The escape** — `b = a[:3].copy()` allocates a fresh block; edits to b leave a untouched
- **The tell** — plain slices return views; fancy indexing (`a[[0,2]]`) and boolean masks return copies

*Example (italic):* a = [10, 20, 30, 40, 50]; b = a[:3]; b[0] = 99 — a is now [99, 20, 30, 40, 50] (exact).

**Common mistake:** Assuming a slice is an independent copy. A NumPy slice is a window onto the same memory — mutate it and you mutate the original; call `.copy()` when you mean a copy.

### Visualization (canvas `c4`, 720×300)

Two-row memory diagram: the default view sharing a's buffer (mutation leaks through) vs an explicit `.copy()` with its own buffer.

- **Title (bold 15px, `#1a5276`, top center):** "b = a[:3] Is a Window, Not a Copy".
- **Row 1 (buffer top edge y=85), label 12px `#444` "view (default)" at x=20:** one shared buffer drawn as 5 adjacent cells, each 60×36, starting at x=180; values `[99, 20, 30, 40, 50]`; cells 2–5 fill `rgba(42,120,214,0.15)` with 1px `#2a78d6` borders, cell 1 fill `rgba(231,76,60,0.12)` with 2px `#e74c3c` border; blue bracket line above cells 1–3 with 12px `#2a78d6` label "b (view)" at y=62; gray bracket line below all 5 cells with 12px `#444` label "a" at y=140; bold 12px red `#e74c3c` note "b[0] = 99 — ✗ a[0] changed too" at (500, 100).
- **Row 2 (buffer top edge y=195), label 12px `#444` "with .copy()" at x=20:** a's buffer as 5 cells (60×36) at x=180, values `[10, 20, 30, 40, 50]`, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` borders, 12px `#444` label "a" below at y=250; separate buffer of 3 cells (60×36) at x=510, values `[99, 20, 30]`, fill `rgba(0,131,0,0.12)`, 1px `#008300` borders, 12px `#008300` label "b = a[:3].copy()" below at y=250; bold 12px green `#008300` "✓ a unchanged" at (510, 188).
- **Annotation (bold 13px orange `#d95926`, centered near y=282):** "same block, two names — copies cost memory, views cost surprises".
- **Caption (12px `#444`, bottom right):** "values exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 80 ms / 8 ms / 0.8 ms timings are invented and labeled illustrative; the matrix values, column means, centered result, and the slice-mutation walkthrough ([99, 20, 30, 40, 50]) are exact arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
