# Fast Exponentiation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Fast Exponentiation

**Subtitle:** Squaring what you already have doubles the exponent each time — so x⁶⁴ takes six multiplications instead of sixty-three, and any big power falls in a handful of steps

## Sixty-Four Years of Interest, Six Multiplications

**Tags:** `core idea` (blue), `repeated squaring` (green), `doubling` (orange)

- **The account** — money earning 2% a year multiplies by 1.02 each year; 64 years means 1.02⁶⁴
- **The slow way** — multiply by 1.02 once per year: 63 multiplications, one for every extra year
- **The trick** — square the number you already have: 1.02² = 1.0404 covers two years in one step
- **Doubling** — square again for 4 years, again for 8, then 16, 32, 64 — the exponent doubles each time
- **Six steps** — 1.02 → 1.0404 → 1.0824 → 1.1717 → 1.3728 → 1.8845 → 3.5515, and that last value is 1.02⁶⁴

*Example (italic):* $100 left in the account for 64 years grows to $100 × 3.5515 ≈ $355 — and the whole calculation was six squarings, not sixty-three year-by-year multiplies.

**Key point:** Each squaring doubles the exponent, so reaching x⁶⁴ takes log₂ 64 = 6 multiplications instead of 63.

### Visualization (canvas `c1`, 720×300)

Bar chart of the squaring chain: seven bars, one per stage, showing the value climbing from 1.02 to 3.5515 as the exponent doubles 1 → 2 → 4 → 8 → 16 → 32 → 64.

- **Title (bold 15px, `#1a5276`, top center):** "Six Squarings: 1.02 to 1.02⁶⁴".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = value 0 to 4 with 12px `#444` tick labels "0", "1", "2", "3", "4" and light `#e5e9ef` gridlines at 1, 2, 3; x = seven bar slots centered at x = `[103, 188, 273, 358, 443, 528, 613]`.
- **Bars:** width 56, heights from values `[1.02, 1.0404, 1.0824, 1.1717, 1.3728, 1.8845, 3.5515]`; first six bars fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, last bar fill `rgba(0,131,0,0.25)` with 2px `#008300` border.
- **Value labels:** bold 12px above each bar, `#2a78d6` for the first six ("1.02", "1.0404", "1.0824", "1.1717", "1.3728", "1.8845"), bold 13px `#008300` for the last ("3.5515").
- **Tick labels (12px `#444`, below baseline):** "1.02¹", "1.02²", "1.02⁴", "1.02⁸", "1.02¹⁶", "1.02³²", "1.02⁶⁴".
- **Squaring arrows:** small 2px `#6b7280` arrows between consecutive bar tops with an 11px `#6b7280` "²" label at each arrow midpoint.
- **Annotation (bold 13px orange `#d95926`, near x=140, y=70):** two lines: "6 squarings," / "not 63 multiplies".
- **Caption (12px `#444`, bottom right):** "illustrative savings scenario; powers of 1.02 rounded to 4 decimals".

## Computing 3¹³ by Reading Its Binary Digits

**Tags:** `worked example` (blue), `binary` (green)

- **The target** — 3¹³ by hand; the naive way is 12 multiplications, one per extra power of 3
- **Binary split** — 13 = 8 + 4 + 1, which is 1101 in binary, so 3¹³ = 3⁸ × 3⁴ × 3¹
- **Squaring chain** — 3 → 9 → 81 → 6561 gives 3¹, 3², 3⁴, 3⁸ in just three squarings
- **Pick the 1-bits** — keep 3⁸, 3⁴, 3¹ (the 1s of 1101) and skip 3² (its bit is 0)
- **Combine** — 6561 × 81 = 531,441, then × 3 = 1,594,323 — five multiplications in total

*Example (italic):* Check it on paper: 3¹³ = 1,594,323, reached with 3 squarings plus 2 combining multiplies instead of 12 naive ones.

**Key point:** Any exponent, not just powers of two: square your way up, then multiply together the powers matching the exponent's binary 1s.

### Visualization (canvas `c2`, 720×300)

Flow diagram: the squaring chain as four boxes across the top, the binary digits of 13 beneath each box selecting which powers to keep, and the combining product along the bottom.

- **Title (bold 15px, `#1a5276`, top center):** "3¹³ = 3⁸ × 3⁴ × 3¹ — because 13 = 1101 in binary".
- **Chain boxes (top row, y=70, each 110×44, 6px corner radius, centers at x = `[120, 280, 440, 600]`):** "3¹ = 3", "3² = 9", "3⁴ = 81", "3⁸ = 6561" in bold 13px; kept boxes (3¹, 3⁴, 3⁸) fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` border and `#1a5276` text; skipped box (3² = 9) fill `#f4f5f7` with 2px dashed `#6b7280` border and `#6b7280` text.
- **Squaring arrows:** 2px `#6b7280` horizontal arrows between consecutive boxes, 11px `#6b7280` label "square" above each.
- **Bit row (y=145, centered under each box):** bold 14px digits of 1101 read low bit first — "1" (`#008300`) under 3¹, "0" (`#6b7280`) under 3², "1" (`#008300`) under 3⁴, "1" (`#008300`) under 3⁸; 12px `#444` label "binary of 13:" at x=20, y=145.
- **Pick lines:** 2px `#008300` vertical lines from each "1" bit down to y=200; none from the "0".
- **Combine line (bold 14px `#008300`, centered at y=220):** "6561 × 81 × 3 = 1,594,323".
- **Annotation (bold 13px orange `#d95926`, centered at y=260):** "5 multiplications instead of 12".

## Why Big Powers Are Everywhere

**Tags:** `where it's used` (blue), `log speed` (green), `cryptography` (orange)

- **The gap grows** — x⁶⁴ costs 63 vs 6; x¹⁰²⁴ costs 1,023 vs 10; doubling the exponent adds one multiply
- **Cryptography** — RSA raises numbers to exponents hundreds of digits long; only squaring makes that finish
- **Matrix powers** — the same trick raises matrices, giving the millionth Fibonacci number in ~20 steps
- **Under the hood** — integer and modular power routines in math and crypto libraries square their way up
- **Log speed** — cost tracks log₂ of the exponent, the classic sign of a divide-and-conquer algorithm

*Example (italic):* At x⁶⁴ the naive method has done 63 multiplications while squaring has done 6 — and the naive count doubles at every step to the right while the fast count just ticks up by one.

**Key point:** Multiplications grow like log₂ n instead of n — the difference between "instant" and "never finishes" once exponents get cryptography-sized.

### Visualization (canvas `c3`, 720×300)

Two-line chart: multiplications needed vs exponent for the naive method and repeated squaring, six exponent stops from 2 to 64, the naive line exploding while the fast line stays flat.

- **Title (bold 15px, `#1a5276`, top center):** "Multiplications Needed: Naive vs Repeated Squaring".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = multiplications 0 to 70 with 12px `#444` tick labels "0", "10", ..., "70" and light `#e5e9ef` gridlines every 10; x = six category stops at x = `[110, 210, 310, 410, 510, 610]` with 12px `#444` tick labels "x²", "x⁴", "x⁸", "x¹⁶", "x³²", "x⁶⁴".
- **Naive line:** orange `#d95926` 3px line with 6px dots through multiplications `[1, 3, 7, 15, 31, 63]`; bold 12px orange label "naive: n−1 multiplies" near x=430, y=110.
- **Fast line:** green `#008300` 3px line with 6px dots through multiplications `[1, 2, 3, 4, 5, 6]`; bold 12px green label "squaring: log₂ n" near x=430, y=215.
- **End labels:** bold 13px orange "63" just above the last naive dot, bold 13px green "6" just above the last fast dot.
- **Annotation (bold 13px orange `#d95926`, near x=120, y=75):** two lines: "at x⁶⁴:" / "63 vs 6".

## Neighbor Exponents, Different Costs

**Tags:** `common mistake` (red), `binary 1s` (orange)

- **The worry** — "13 isn't a power of two, squaring will overshoot it" — the binary 1-bits fix that
- **The cost rule** — squarings = ⌊log₂ n⌋, plus one extra multiply for each binary 1 beyond the first
- **Neighbors differ** — binary method: x¹⁵ (1111₂) costs 3 + 3 = 6 multiplies; x¹⁶ (10000₂) only 4
- **Bigger can be cheaper** — a larger exponent with a cleaner binary form beats a smaller messy one
- **The mistake** — assuming cost climbs smoothly with the exponent; it jumps with the count of 1-bits

*Example (italic):* Raising to the 15th power takes 6 multiplications while raising to the 16th takes 4 — the extra 1-bits in 1111 cost more than the bigger exponent 10000.

**Common mistake:** Thinking fast exponentiation only works for power-of-two exponents, or that a bigger exponent always costs more. The binary digits decide: squarings for the length, extra multiplies for the 1s.

### Visualization (canvas `c4`, 720×300)

Bar chart of multiplication cost for every exponent from 8 to 16, each bar labeled with the exponent's binary form, showing the jagged cost pattern and the x¹⁵ vs x¹⁶ surprise.

- **Title (bold 15px, `#1a5276`, top center):** "Cost Is Jagged: Multiplications for x⁸ through x¹⁶".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = multiplications 0 to 7 with 12px `#444` tick labels "0"–"7" and light `#e5e9ef` gridlines at 1–6; x = nine bar slots centered at x = `[95, 161, 227, 293, 359, 425, 491, 557, 623]`.
- **Bars:** width 44, heights from costs `[3, 4, 4, 5, 4, 5, 5, 6, 4]` for exponents 8–16; default fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; the x¹⁵ bar fill `rgba(217,89,38,0.30)` with 2px `#d95926` border; the x¹⁶ bar fill `rgba(0,131,0,0.25)` with 2px `#008300` border.
- **Cost labels:** bold 12px above each bar in the bar's border color: "3", "4", "4", "5", "4", "5", "5", "6", "4".
- **Tick labels:** 12px `#444` exponent labels "x⁸"–"x¹⁶" below the baseline, with the binary form in 11px `#6b7280` on a second line: "1000", "1001", "1010", "1011", "1100", "1101", "1110", "1111", "10000".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=70):** two lines: "x¹⁵ costs 6," / "x¹⁶ only 4".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, line points, box contents, and cost arrays are the hardcoded literals above (no randomness); powers of 1.02 are true values rounded to 4 decimals; costs equal ⌊log₂ n⌋ plus popcount(n) − 1; unicode superscripts (¹ ² ⁴ ⁸ ¹⁶ ³² ⁶⁴) render directly in canvas `fillText`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
