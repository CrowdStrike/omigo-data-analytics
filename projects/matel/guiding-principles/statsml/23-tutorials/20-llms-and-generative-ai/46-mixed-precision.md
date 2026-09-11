# Mixed Precision

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Mixed Precision

**Subtitle:** Storing each number with fewer digits — fp16, bf16, or int8 instead of fp32 — halves or quarters the bytes, so the same chip can move and multiply numbers roughly twice as fast for almost no loss in the answer

## Copying the Day's Sales with Fewer Digits

**Tags:** `core idea` (blue), `digit budget` (green), `half the bytes` (orange)

- **The ledger** — a coffee shop owner copies every sale of the day into a paper ledger by hand
- **Full digits** — writing each price as "4.7500000" is exact but slow and fills pages fast
- **Fewer digits** — writing "4.75" keeps what matters; writing "5" is coarser but even quicker
- **The formats** — fp32 holds ~7 digits in 4 bytes, fp16 ~3 (bf16 ~2) in 2, int8 whole numbers in 1
- **The trade** — half the bytes means half the ink, half the pages, and twice the copying speed
- **Mixed** — "mixed precision" keeps a few totals in full digits while the bulk uses the short form

*Example (italic):* The owner writes the running grand total with all its cents but jots each individual sale to the nearest dollar — the important number stays exact, the bulk goes fast.

**Key point:** A number format is a digit budget: fp32 spends 4 bytes for ~7 digits, fp16 spends 2 bytes for ~3 and bf16 for ~2, int8 spends 1 byte for whole numbers — half the bytes, double the speed.

### Visualization (canvas `c1`, 720×300)

Byte-block chart: four rows, one per format, each drawn as a strip of byte squares so the halving in storage is visible at a glance, with digit-budget labels at the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Number, Four Sizes: Bytes per Stored Number".
- **Rows (y = 80, 130, 180, 230), each with a left-aligned 13px `#2c3e50` format label at x=30:** "fp32", "fp16", "bf16", "int8".
- **Byte squares:** 44px wide, 30px tall, 6px gap, starting at x=110 — fp32 gets 4 squares filled blue `#2a78d6`, fp16 gets 2 filled aqua `#199e70`, bf16 gets 2 filled green `#008300`, int8 gets 1 filled orange `#d95926`; all squares 1px `#1a5276` border; ghost outlines (1px dashed `#e5e9ef`) where fp32's remaining squares would sit, so shorter rows read as "half".
- **Right labels (12px `#444`, x=330):** "4 bytes — ~7 digits", "2 bytes — ~3 digits", "2 bytes — ~2 digits, huge range", "1 byte — whole numbers −128..127".
- **Annotation (bold 13px violet `#4a3aa7`, near x=430, y=55, two lines):** "half the bytes →" / "twice as many numbers per trip".
- **Caption (12px `#444`, bottom right):** "digit counts are approximate — the byte counts are exact".

## Rounding Eight Coffees to Whole Dollars

**Tags:** `worked example` (blue), `rounding error` (green)

- **Eight sales** — the exact prices: 4.75, 3.20, 5.15, 2.60, 4.75, 3.85, 5.15, 4.30
- **Exact total** — added with all their cents, the eight sales come to $33.75
- **Whole dollars** — rounded like int8 would: 5, 3, 5, 3, 5, 4, 5, 4 — total $34.00
- **The damage** — the coarse total is off by $0.25, an error of only 0.7%
- **Why so small** — round-ups and round-downs mostly cancel when many numbers are summed
- **Same story in a model** — millions of tiny multiply-adds tolerate short numbers the same way

*Example (italic):* Redo it by hand: 4.75 + 3.20 + 5.15 + 2.60 + 4.75 + 3.85 + 5.15 + 4.30 = 33.75, while 5 + 3 + 5 + 3 + 5 + 4 + 5 + 4 = 34 — a whole day of speed for a quarter.

**Key point:** Rounding every price to whole dollars changed the total from $33.75 to $34.00 — 0.7% off — because individual rounding errors largely cancel in a big sum.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: eight coffee sales, each shown as a pair of bars (exact price in blue, whole-dollar version in orange), with the two totals compared in an annotation box.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Sales, Exact vs Rounded to Whole Dollars".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; y axis = price $0 to $6 with 12px `#444` tick labels "$0".."$6" every $1 and light `#e5e9ef` gridlines; x axis = eight groups labeled "sale 1".."sale 8" (12px `#444`).
- **Bars:** per group, exact bar (blue `#2a78d6`, 22px wide) at values `[4.75, 3.20, 5.15, 2.60, 4.75, 3.85, 5.15, 4.30]`, rounded bar (orange `#d95926`, 22px wide, 4px gap) at values `[5, 3, 5, 3, 5, 4, 5, 4]`.
- **Legend (12px, top left inside plot at x=75, y=75):** blue swatch "exact cents", orange swatch "whole dollars (int8-style)".
- **Annotation box (near x=430, y=28, 1px `#1a5276` border, white fill):** bold 13px ink `#1a5276` lines: "exact total $33.75" / "rounded total $34.00" / bold green `#008300` third line "off by $0.25 — 0.7%".
- **Caption (12px `#444`, bottom right):** "illustrative prices from one morning's till".

## Why GPUs Love Small Numbers

**Tags:** `where it's used` (blue), `memory & speed` (green), `training trick` (orange)

- **The wall** — a GPU spends most of its time moving numbers, not multiplying them
- **Model size** — a 7-billion-weight model needs 28 GB at fp32, 14 GB at fp16/bf16, 7 GB at int8
- **The fit** — on a 16 GB card the fp32 copy does not fit; the fp16 copy fits; int8 fits twice over
- **The speedup** — half the bytes per number roughly doubles throughput on the same hardware
- **The "mixed" part** — training multiplies in fp16/bf16 but keeps a master fp32 copy of the weights
- **Loss scaling** — fp16 training multiplies the loss up so tiny gradients don't round down to zero

*Example (italic):* The same chatbot that needs a 28 GB fp32 home squeezes into a 14 GB fp16 one and answers roughly twice as fast — the words it produces are nearly indistinguishable.

**Key point:** Precision is the cheapest speed knob there is: 7 billion weights cost 28 GB at fp32 but 14 GB at fp16 and 7 GB at int8, and every halving of bytes buys roughly a doubling of speed.

### Visualization (canvas `c3`, 720×300)

Bar chart: memory needed to hold a 7-billion-weight model at each precision, with a dashed horizontal line marking a 16 GB GPU so the reader sees which versions fit.

- **Title (bold 15px, `#1a5276`, top center):** "One 7B Model, Three Sizes — Which Fits on a 16 GB GPU?".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; y axis = memory 0 to 30 GB, 12px `#444` tick labels "0", "10", "20", "30 GB" with light `#e5e9ef` gridlines every 5.
- **Bars (90px wide, centered at x = 170, 360, 550):** fp32 at 28 filled `rgba(217,89,38,0.35)` with 2px orange `#d95926` border, fp16/bf16 at 14 filled `rgba(42,120,214,0.35)` with 2px blue `#2a78d6` border, int8 at 7 filled `rgba(0,131,0,0.35)` with 2px green `#008300` border; bold 13px value labels above each bar: "28 GB", "14 GB", "7 GB"; 12px `#444` x labels below: "fp32 (4 B each)", "fp16 / bf16 (2 B)", "int8 (1 B)".
- **GPU line:** horizontal dashed `#6b7280` (dash 6/4) 2px line at 16 GB across the plot; 12px `#6b7280` label at its right end: "16 GB GPU card".
- **Annotation (bold 13px, two lines):** orange `#d95926` "doesn't fit" above the fp32 bar tip; green `#008300` "fits twice over" beside the int8 bar near y=150.
- **Caption (12px `#444`, bottom right):** "weights only — activations and optimizer state cost extra; illustrative".

## fp16 and bf16 Are Not the Same Half

**Tags:** `common mistake` (red), `range vs digits` (orange)

- **Same 2 bytes** — fp16 and bf16 are both half-size, but they spend their 16 bits differently
- **fp16** — more digits of detail, but its biggest storable number is only 65,504
- **bf16** — fewer digits, but the same enormous range as fp32, up to about 3.4×10³⁸
- **The failure** — a gradient of 70,000 overflows to infinity in fp16; bf16 shrugs it off
- **The mistake** — treating the two as interchangeable because "both are 16-bit"
- **Rule of thumb** — bf16 for training stability, fp16 where supported detail matters, int8 for serving

*Example (italic):* A team's fp16 training run kept exploding into NaNs until they realized a loss spike of 70,000 simply cannot be written in fp16 — switching to bf16 fixed it without any tuning.

**Common mistake:** Assuming fp16 and bf16 are interchangeable because both use 2 bytes. fp16 buys extra digits by giving up range and overflows past 65,504; bf16 keeps fp32's range by giving up digits.

### Visualization (canvas `c4`, 720×300)

Range-ruler chart: three horizontal bars on a shared log-scale axis of "biggest and smallest positive numbers each format can write", making fp16's short reach obvious against fp32 and bf16.

- **Title (bold 15px, `#1a5276`, top center):** "Same 2 Bytes, Very Different Reach".
- **Axis:** horizontal 2px `#999` line at y=250 from x=140 to x=690 (width 550) mapping log10 of the value from −38 to +38; 12px `#444` tick labels "10⁻³⁸", "10⁻²⁰", "1", "10²⁰", "10³⁸" at log10 = −38, −20, 0, 20, 38.
- **Rows (bars 16px tall, rounded, at y = 90, 145, 200), each with a left-aligned 13px `#2c3e50` label at x=20:**
  - "fp32 — 4 bytes": bar log10 −38 to +38, fill `rgba(42,120,214,0.35)`, 2px blue `#2a78d6` border; 12px blue label at bar right: "~7 digits".
  - "bf16 — 2 bytes": bar log10 −38 to +38, fill `rgba(0,131,0,0.35)`, 2px green `#008300` border; 12px green label: "~2 digits, full range".
  - "fp16 — 2 bytes": bar log10 −4.2 to +4.8 only, fill `rgba(217,89,38,0.35)`, 2px orange `#d95926` border; bold 12px orange label at bar right end: "tops out at 65,504".
- **Overflow marker:** vertical dashed `#6b7280` (dash 4/3) line at log10 = 4.85 from y=70 to the axis; 11px `#6b7280` label at its top: "a 70,000 gradient lands here".
- **Annotation (bold 13px magenta `#d55181`, near x=400, y=60):** "bf16 spends its bits on range, fp16 on digits — pick for the job".
- **Caption (12px `#444`, bottom right):** "normal-number ranges, log scale — endpoints rounded for readability".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, prices, totals, byte counts, and range endpoints are the hardcoded literals above (no randomness); the c2 totals must equal the sums of the plotted arrays ($33.75 and $34.00); the 28/14/7 GB bars are 7×10⁹ weights times 4/2/1 bytes.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
