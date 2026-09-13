# Fixed Point vs Floating Point

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Fixed Point vs Floating Point

**Subtitle:** Two ways to store $3.14 — pin the decimal point in one agreed spot, or store the digits plus where the point goes; one is exact inside a narrow window, the other spans a huge range with limited digits

## One Coffee Price, Two Ways to Write It Down

**Tags:** `core idea` (blue), `fixed point` (green), `floating point` (orange)

- **The price** — a register must store $3.14, but a computer's memory cells hold only whole numbers
- **Fixed point** — store the integer 314 and agree once, for every value, the point sits 2 from the right
- **Floating point** — store the digits 314 plus an exponent −2, meaning 314 × 10^−2 = 3.14
- **The point floats** — the same digits with exponent 0 mean 314; with exponent −4 they mean 0.0314
- **Binary detail** — real hardware uses base 2 instead of 10, but both schemes work exactly this way

*Example (italic):* $3.14 is stored as the plain integer 314 in fixed point, and as the pair (314, −2) in floating point — one number, two encodings.

**Key point:** Fixed point pins the decimal point at one agreed spot for all values; floating point stores where the point goes along with the digits, so the point can slide anywhere.

### Visualization (canvas `c1`, 720×300)

Dual-panel diagram: fixed-point digit boxes with a pinned decimal marker (left) vs floating-point rows where the same digits slide with the exponent (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Storing $3.14: Pinned Point vs Sliding Point".
- **Left panel (fixed point):** heading bold 13px `#1a5276` "fixed point — the point is pinned" at x=60, y=60; three rows of digit boxes starting y=85, row spacing 55; each row is monospace 16px digits in 30×34 boxes (1px `#e5e9ef` border, 4px gap) starting x=80: row 1 digits "0 0 0 3 1 4" labeled "$3.14" (12px `#444`, right of boxes), row 2 "0 0 0 0 0 5" labeled "$0.05", row 3 "0 0 1 2 0 0" labeled "$12.00"; a magenta `#d55181` bold 16px "." drawn between the 4th and 5th box of every row (same x on all rows) with a magenta 11px caption "point always here, 2 from the right" under row 3.
- **Right panel (floating point):** heading bold 13px `#1a5276` "floating point — the point slides" at x=400, y=60; three monospace 15px rows at x=410, y=100/145/190: "314 × 10^−2 = 3.14", "314 × 10^0  = 314", "314 × 10^−4 = 0.0314"; the digits "314" in blue `#2a78d6`, the exponent part ("10^−2" etc.) in orange `#d95926`, the result in `#2c3e50`; orange bold 13px annotation "same digits, sliding point" at x=410, y=235.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## A Six-Digit Toy Computer

**Tags:** `worked example` (blue), `range vs precision` (orange)

- **Toy machine** — give each scheme six digits: fixed keeps 2 after the point; float keeps 4 digits + exponent
- **Easy case** — $3.14 fits both: fixed stores 0003.14 and float stores 3.140 × 10^0, both exact
- **Tiny value** — half a cent ($0.005) breaks fixed (rounds to $0.01); float stores 5.000 × 10^−3 exactly
- **Huge value** — $1,234,567.89 overflows fixed; float keeps 1.235 × 10^6, which is $1,235,000
- **The loss** — float saved the huge number by dropping digits: it lands $432.11 away from the truth

*Example (italic):* With the same six digits, fixed point covers $0.01 to $9,999.99 exactly, while float reaches from 10^−9 to 10^9 but promises only 4 sure digits.

**Key point:** Same storage budget, different spending — fixed point buys exactness inside a narrow window; floating point buys enormous range at a fixed number of significant digits.

### Visualization (canvas `c2`, 720×300)

Three-row scoreboard: each of the three test values shown with its fixed-point result and float result side by side, with green check / red cross verdicts.

- **Title (bold 15px, `#1a5276`, top center):** "Three Values, Six Digits Each: Who Stores What?".
- **Column headers (bold 12px `#444`, y=62):** "value to store" at x=70, "fixed: dddd.dd" at x=290, "float: d.ddd × 10^e" at x=500.
- **Rows (monospace 14px, y=105/165/225, light `#e5e9ef` separator lines between rows):**
  - Row 1: "$3.14" at x=70; "0003.14" at x=290 with green `#008300` bold "exact" tag; "3.140 × 10^0" at x=500 with green bold "exact" tag.
  - Row 2: "$0.005" at x=70; "0000.01" at x=290 in red `#e74c3c` with red bold "rounds to $0.01 (2× the truth)" tag; "5.000 × 10^−3" at x=500 with green bold "exact" tag.
  - Row 3: "$1,234,567.89" at x=70; "———" at x=290 in red with red bold "overflow" tag; "1.235 × 10^6" at x=500 in orange `#d95926` with orange bold "off by $432.11" tag.
- **Tags:** 11px bold text drawn 16px below each stored value.
- **Takeaway (bold 13px `#1a5276`, bottom center, y=282):** "fixed: perfect inside its window — float: vastly wider range, only ~4 sure digits".
- **Caption (11px `#6b7280`, under title, y=42):** "toy decimal formats, illustrative of real binary ones".

## Where Each One Breaks

**Tags:** `failure mode` (red), `float32` (blue), `32-bit cents` (green)

- **Float gaps grow** — float32 gaps double each power of 2: 0.00000012 near 1, 0.000122 near 1,024
- **The cliff** — near 16,777,216 the float32 gap is 2, so 16,777,216 + 1 returns 16,777,216 unchanged
- **Fixed stays even** — a 32-bit cents field is exact to $0.01 at every value it can reach
- **Fixed hits walls** — that cents field tops out at $21,474,836.47; one more dollar overflows
- **Fixed floor** — $0.005 does not exist in a cents field; it must round to $0.00 or $0.01

*Example (italic):* A play counter kept as float32 froze forever at 16,777,216 because adding 1 fell below the gap to the next representable float.

**Key point:** Fixed point fails loudly at the edges of its window; floating point fails quietly in the middle, wherever the gap between neighbors outgrows the change you are adding.

### Visualization (canvas `c3`, 720×300)

Dual panel: bar chart of float32 gap size vs magnitude (left) and a fixed-point cents number line with uniform steps and a hard overflow wall (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "How Each Scheme Degrades: Growing Gaps vs Hard Walls".
- **Left panel (float32 gaps):** axis origin x=55, baseline y=240, chart height 175; five bars 40px wide at x=65/120/175/230/285 with heights `[20, 58, 96, 128, 160]` px (log-scale heights, illustrative spacing), fill `rgba(42,120,214,0.45)`; magnitude labels below each bar (12px `#444`): "1", "1k", "1M", "16.8M", "1B"; gap-value labels above each bar (bold 11px `#2a78d6`): "1.2e-7", "1.2e-4", "0.125", "2", "64"; red `#e74c3c` bold 12px annotation over the 4th bar, two lines: "gap = 2 here:" / "+1 does nothing"; caption 11px `#444` at y=262 "gap between neighboring float32 values".
- **Right panel (32-bit cents):** horizontal line 2px `#999` at y=150 from x=400 to x=660; nine green `#008300` 5px tick dots evenly spaced from x=410 to x=570 (20px apart) with green bold 12px label "exact $0.01 steps, everywhere" above at y=120; red `#e74c3c` 3px vertical wall at x=610 from y=110 to y=190 with red bold 12px two-line label "$21,474,836.47" / "overflow wall" to its right/above; magenta `#d55181` bold 11px annotation at x=410, y=195: "below $0.01: nothing exists ($0.005 impossible)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Which One for Money?

**Tags:** `common mistake` (red), `rule of thumb` (green), `where it's used` (blue)

- **The trap** — floats print $3.14 perfectly, so people store money in them; pennies then leak in sums
- **Symptom** — in floats 0.1 + 0.2 gives 0.30000000000000004, so a check `== 0.3` is false
- **Money rule** — store currency as integer smallest units (cents); banks and ledgers do exactly this
- **Measurement rule** — sensor readings and model weights span huge scales and belong in floats
- **Counter rule** — things you count (rows, clicks, users) belong in plain integers, never floats

*Example (italic):* An accountant chased a one-cent mismatch for days — the ledger had been summing float dollars all year instead of integer cents.

**Common mistake:** Assuming a decimal point on screen means decimal arithmetic inside. Floats are binary and round almost every decimal fraction, so exact-money work belongs in fixed point.

### Visualization (canvas `c4`, 720×300)

Coverage map: a log10 number line from 10^−38 to 10^38 with two horizontal bands — the fixed-point cents window (a narrow green sliver) and the float32 range (nearly the whole line) — with $3.14 marked inside both.

- **Title (bold 15px, `#1a5276`, top center):** "One Number Line, Two Coverage Maps (log scale)".
- **Axis:** horizontal 2px `#999` line at y=235 from x=70 to x=650; x maps log10(value) linearly from −38 (x=70) to +38 (x=650), i.e. ~7.63px per decade; ticks with 12px `#444` labels at "10^−38" (x=70), "10^−19" (x=215), "1" (x=360), "10^19" (x=505), "10^38" (x=650).
- **Float32 band:** rectangle fill `rgba(42,120,214,0.35)`, 26px tall at y=110, from x=71 (10^−38) to x=649 (3.4 × 10^38); blue `#2a78d6` bold 13px label above at y=100: "float32: almost everything, but only ~7 sure digits".
- **Fixed band:** rectangle fill `rgba(0,131,0,0.5)`, 26px tall at y=170, from x=345 (log10 0.01 = −2) to x=416 (log10 21,474,836.47 ≈ 7.33); green `#008300` bold 13px label at x=430, y=187: "32-bit cents: a narrow window, exact to $0.01".
- **Marker:** magenta `#d55181` 6px dot at x=364 (log10 3.14 ≈ 0.5) on each band, with magenta bold 12px label "$3.14 lives in both" at x=380, y=75 and thin magenta 1px leader lines to both dots.
- **Takeaway (bold 13px `#1a5276`, bottom center, y=285):** "money and counts: fixed/integers — measurements and science: floats".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
