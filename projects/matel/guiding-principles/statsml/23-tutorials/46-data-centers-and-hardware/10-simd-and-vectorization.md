# SIMD & Vectorization

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SIMD & Vectorization

**Subtitle:** Modern CPUs can add 8 numbers in one instruction — NumPy's array operations use this, a plain Python for-loop can't

## Adding a Million Sensor Readings

**Tags:** `core idea` (blue), `wide registers` (green), `AVX2` (orange)

- **The job** — a data scientist sums 1,000,000 float32 temperature readings from a sensor log
- **One at a time** — a plain loop loads one number, adds it, repeats: a million separate additions
- **The wide register** — a 256-bit AVX2 register holds 8 float32 values side by side in "lanes"
- **One instruction** — a single vector add (`vaddps`) adds all 8 lanes to 8 other lanes at once
- **The name** — this is SIMD: Single Instruction, Multiple Data — one operation, many values

*Example (italic):* The CPU adds `[1.5, 2.0, 3.5, 4.0, 0.5, 2.5, 1.0, 3.0]` to `[0.5, 1.0, 0.5, 1.0, 1.5, 0.5, 2.0, 1.0]` in one instruction, producing all 8 sums together.

**Key point:** SIMD packs several values into one wide register and applies one instruction to every lane simultaneously — the work per instruction goes up 8× (or 16× with 512-bit registers).

### Visualization (canvas `c1`, 720×300)

Lane diagram of a 256-bit register: two rows of 8 float32 lanes feeding one vector add, producing a third row of 8 results.

- **Title (bold 15px, `#1a5276`, top center):** "One Instruction, Eight Additions: Inside a 256-bit AVX2 Register".
- **Layout:** three rows of 8 boxes, each box 70px wide × 34px tall, 5px gap, first box at x=100 (row spans x=100 to x=695); rows at y=70 (register a), y=130 (register b), y=210 (result).
- **Row labels (12px `#444`, left-aligned at x=15, vertically centered on each row):** "a lanes", "b lanes", "a + b".
- **Register a boxes:** fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, 6px radius, 12px `#2c3e50` centered values `[1.5, 2.0, 3.5, 4.0, 0.5, 2.5, 1.0, 3.0]`.
- **Register b boxes:** fill `rgba(217,89,38,0.12)`, 1.5px `#d95926` border, values `[0.5, 1.0, 0.5, 1.0, 1.5, 0.5, 2.0, 1.0]`.
- **Result boxes:** fill `rgba(0,131,0,0.12)`, 1.5px `#008300` border, bold values `[2.0, 3.0, 4.0, 5.0, 2.0, 3.0, 3.0, 4.0]`.
- **Add marker:** bold 16px `#1a5276` "+" centered at x=57 between rows a and b (y≈117); one 3px `#008300` arrow from the middle of row b down to row 3 at x≈397, with bold 12px green label "one vaddps instruction" beside it.
- **Annotation (bold 13px violet `#4a3aa7`, bottom center, y≈280):** "8 float32 lanes × 32 bits = 256 bits — lane math exact".

## Counting the Instructions

**Tags:** `worked example` (blue), `lane math` (green)

- **Scalar loop** — summing 1,000,000 readings one at a time takes 1,000,000 add instructions
- **AVX2, 8 lanes** — 1,000,000 ÷ 8 = 125,000 vector adds sweep the whole array
- **AVX-512, 16 lanes** — 512 bits ÷ 32 bits = 16 float32 lanes, so 1,000,000 ÷ 16 = 62,500 adds
- **Hand-check** — 125,000 × 8 = 1,000,000: every reading is still added exactly once
- **The finish** — the 8 (or 16) running lane totals are folded into one number with a few final adds

*Example (italic):* The same million-element sum costs 1,000,000 scalar adds, 125,000 AVX2 vector adds, or 62,500 AVX-512 vector adds — exactly 8× and 16× fewer instructions.

**Key point:** The instruction count divides exactly by the lane count — this is arithmetic, not a benchmark: wider registers mean proportionally fewer instructions for the same array.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of add instructions needed to sum 1,000,000 float32s: scalar vs AVX2 vs AVX-512, widths exactly proportional.

- **Title (bold 15px, `#1a5276`, top center):** "Summing 1,000,000 float32s: Add Instructions Needed".
- **Axis:** vertical 2px `#999` baseline at x=190, bars extend right, max width 480.
- **Rows (bar tops at y = 75, 140, 205, bars 26px tall), each with a left-aligned 12px `#444` two-line label ending at x=180:**
  - "scalar / 1 value per add": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 480, bold 12px `#2c3e50` end label "1,000,000"
  - "AVX2 / 8 lanes": green `#008300` fill `rgba(0,131,0,0.30)`, width 60 (480 ÷ 8), end label "125,000"
  - "AVX-512 / 16 lanes": aqua `#199e70` fill `rgba(25,158,112,0.30)`, width 30 (480 ÷ 16), end label "62,500"
- **Annotation (bold 13px green `#008300`, near x=300, y=160):** "8 lanes → exactly 8× fewer instructions".
- **Caption (12px `#444`, bottom right):** "instruction counts exact — array length ÷ lane count".

## Why `a + b` Beats the For-Loop

**Tags:** `where it's used` (blue), `NumPy` (green), `interpreter tax` (orange)

- **The Python loop** — each iteration is interpreted: unbox the float, dispatch `+`, box the result
- **No lanes possible** — the interpreter handles one dynamically-typed object at a time, never 8
- **NumPy's inside** — `a + b` runs a compiled C loop over a contiguous float32 buffer
- **Two wins at once** — the C loop skips the interpreter AND compiles to SIMD vector instructions
- **The habit** — express work as whole-array operations (`a + b`, `a.sum()`, `a[a > 0]`), not element loops

*Example (italic):* Summing the million readings: a Python for-loop takes 95 ms, Python's built-in `sum()` 45 ms, and NumPy's `a.sum()` 1.2 ms — roughly 79× faster than the loop.

**Key point:** NumPy's speed is the interpreter tax removed plus SIMD applied — that combination is why whole-array operations beat element-by-element Python by 10–100×.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of wall-clock time to sum 1,000,000 float32s three ways: Python for-loop, built-in sum(), NumPy a.sum().

- **Title (bold 15px, `#1a5276`, top center):** "Same Sum, Three Ways: 95 ms vs 45 ms vs 1.2 ms".
- **Axis:** vertical 2px `#999` baseline at x=190, bars extend right, max width 480; widths proportional to time.
- **Rows (bar tops at y = 75, 140, 205, bars 26px tall), left-aligned 12px `#444` labels ending at x=180:**
  - "Python for-loop": red `#e74c3c` fill `rgba(231,76,60,0.30)`, width 480, bold 12px end label "95 ms"
  - "built-in sum()": orange `#d95926` fill `rgba(217,89,38,0.25)`, width 227, end label "45 ms"
  - "NumPy a.sum()": green `#008300` fill `rgba(0,131,0,0.30)`, width 6, end label "1.2 ms"
- **Annotation (bold 13px green `#008300`, near x=280, y=225):** "~79× faster — no interpreter, plus SIMD".
- **Caption (12px `#444`, bottom right):** "timings illustrative — speedups of 10–100× are typical, varying by machine".

## Not Every Loop Can Vectorize

**Tags:** `common mistake` (red), `auto-vectorization` (orange)

- **Three requirements** — contiguous memory, one uniform element type, no per-element data-dependent branch
- **Compilers help** — C compilers auto-vectorize simple loops that meet all three, silently
- **Where it fails** — pointer-chasing (linked lists), mixed types, or `if reading > limit: rare_fix()` per element
- **The trap** — `np.vectorize` and list comprehensions only hide the loop; the work is still scalar Python
- **The fix** — replace per-element branches with masks: `np.where(a > limit, fix, a)` keeps lanes full

*Example (italic):* A "vectorized" `np.vectorize(f)(a)` on the million readings still calls the Python function a million times — same speed as the loop it replaced.

**Common mistake:** Believing that removing the visible `for` keyword is vectorization. SIMD needs contiguous, uniform, branch-free data — an API that loops in Python underneath gains nothing.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a loop that meets the three requirements takes the SIMD path; a loop with a per-element branch falls back to scalar code.

- **Title (bold 15px, `#1a5276`, top center):** "The Compiler's Checklist: SIMD Path vs Scalar Fallback".
- **Row 1 (boxes centered at y=105), label 12px `#444` at x=15:** "clean loop"; blue `#2a78d6` rounded box at x=115 labeled "contiguous float32, no branches" (12px, two lines), 3px arrow to a green `#008300` box at x=370 labeled "auto-vectorized: 8 lanes/add", bold 12px green "✓ SIMD" at the arrow's end (x≈610).
- **Row 2 (boxes centered at y=215), label:** "branchy loop"; blue box at x=115 labeled "if x > limit per element", 3px arrow to a red `#e74c3c` box at x=370 labeled "scalar fallback: 1 value/add", bold 12px red "✗ no lanes" at x≈610.
- **Box style:** 200px wide, 46px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` centered text.
- **Checklist strip (12px `#444`, across the top under the title at y≈55):** "requirements: contiguous memory · uniform type · no data-dependent branching".
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "masks (np.where) turn branches back into lane-friendly math".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded arrays above (no randomness); register-width lane math (256÷32=8, 512÷32=16) and instruction counts (1,000,000 / 125,000 / 62,500) are exact; the 95 ms / 45 ms / 1.2 ms timings are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
