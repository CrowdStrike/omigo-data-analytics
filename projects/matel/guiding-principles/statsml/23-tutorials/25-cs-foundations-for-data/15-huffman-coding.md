# Huffman Coding

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Huffman Coding

**Subtitle:** Give the most common things the shortest codes and the rare things longer ones — Huffman's greedy tree finds the best such code, and it is the trick inside every ZIP file

## A Coffee Shop's Order Log

**Tags:** `core idea` (blue), `variable-length codes` (green), `prefix rule` (orange)

- **The ticket printer** — a coffee shop logs every order as a string of bits; fewer bits, faster prints
- **The tally** — out of 100 orders: latte 45, espresso 25, cappuccino 15, mocha 10, tea 5
- **The flat way** — 5 drinks need 3 bits each if all codes have equal length: 300 bits per 100 orders
- **The trick** — let the latte pay just 1 bit and let rare tea carry 4; the average bill drops
- **Prefix rule** — no code may be the start of another code, so a bit stream reads one way only

*Example (italic):* With latte = 0, the stream "000" can only mean latte, latte, latte — no commas or spaces needed between codes.

**Key point:** When some symbols are far more common than others, variable-length codes beat fixed-length ones — Huffman coding finds the best possible set.

### Visualization (canvas `c1`, 720×300)

Single-panel vertical bar chart: the five drinks' daily counts, with each bar topped by its Huffman code so short codes visibly sit on tall bars.

- **Title (bold 15px, `#1a5276`, top center):** "100 Orders a Day — Common Drinks Should Get Short Codes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; y = orders 0 to 50 with light `#e5e9ef` gridlines and 12px `#444` labels at 10, 20, 30, 40, 50.
- **Bars:** five bars, width 82, left edges at x = `[95, 210, 325, 440, 555]`; counts `[45, 25, 15, 10, 5]`; fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` outline; 12px `#444` drink labels below the baseline: "latte", "espresso", "cappuccino", "mocha", "tea".
- **Count labels:** bold 13px `#2a78d6` count centered just above each bar top: "45", "25", "15", "10", "5".
- **Code labels:** bold 13px green `#008300` monospace code above each count: `0`, `10`, `110`, `1110`, `1111`.
- **Annotation (bold 13px orange `#d95926`, near x=430, y=90):** two lines: "tall bar, short code —" / "latte pays 1 bit, tea pays 4".
- **Caption (12px `#444`, bottom right):** "illustrative — one day's order tally".

## Pair the Two Rarest, Repeat

**Tags:** `worked example` (blue), `greedy tree` (green)

- **The greedy step** — take the two smallest counts on the shelf, glue them into one bundle, put it back
- **Round 1** — tea 5 + mocha 10 → bundle 15; the shelf now holds 45, 25, 15, 15
- **Round 2** — cappuccino 15 + bundle 15 → 30; then 25 + 30 → 55; then 45 + 55 → 100, done
- **Reading codes** — walk down from the top, left branch = 0, right branch = 1, stop at a drink
- **The bill** — 45×1 + 25×2 + 15×3 + 10×4 + 5×4 = 200 bits per 100 orders, vs 300 fixed

*Example (italic):* Tea pays 4 bits but only 5 times a day; latte pays 1 bit 45 times — the total drops from 300 to 200 bits.

**Key point:** "Merge the two rarest, repeat" builds a tree whose leaf depths are the code lengths — and this greedy tree is provably the shortest-average prefix code.

### Visualization (canvas `c2`, 720×300)

Single-panel tree diagram: the Huffman tree built from the five counts, drawn top-down with 0/1 edge labels, merge order visible in the internal-node totals, and the final code under each leaf.

- **Title (bold 15px, `#1a5276`, top center):** "The Huffman Tree: Rarest Drinks Sink Deepest".
- **Nodes:** circles radius 17, 2px outline, bold 12px count centered inside; internal nodes outlined `#1a5276` on white; leaf nodes filled `rgba(42,120,214,0.15)`, outlined `#2a78d6`.
- **Node centers (x, y):** root 100 at (360, 55); leaf latte 45 at (185, 120); internal 55 at (480, 120); leaf espresso 25 at (390, 180); internal 30 at (565, 180); leaf cappuccino 15 at (495, 240); internal 15 at (635, 240); leaf mocha 10 at (585, 285); leaf tea 5 at (685, 285) — mocha and tea drawn as radius-14 circles to fit.
- **Edges:** 2px `#6b7280` lines between parent and child circles; bold 12px edge labels beside each edge midpoint, "0" in blue `#2a78d6` on every left edge, "1" in orange `#d95926` on every right edge.
- **Leaf labels:** 12px `#444` drink name under each leaf, bold 12px green `#008300` monospace code under the name: latte `0`, espresso `10`, cappuccino `110`, mocha `1110`, tea `1111`.
- **Merge-order tags:** 11px `#6b7280` labels "merge 1" beside internal 15, "merge 2" beside internal 30, "merge 3" beside internal 55, "merge 4" beside the root.
- **Annotation (bold 13px violet `#4a3aa7`, near x=95, y=210):** two lines: "greedy rule:" / "always glue the two rarest".

## The Trick Inside Every ZIP File

**Tags:** `where it's used` (blue), `compression` (green), `entropy floor` (orange)

- **In your files** — ZIP, GZIP, PNG, JPEG and MP3 all finish their compression with a Huffman pass
- **The floor** — the order mix has entropy ≈ 1.98 bits/order; Huffman's 2.00 average nearly touches it
- **When it shines** — skewed tallies compress well; five equally common drinks would give no saving
- **A rare gem** — one of the few famous problems where the simple greedy choice is provably optimal
- **Data work** — columnar stores and categorical encoders shrink skewed columns the same way

*Example (italic):* Zipping a log of the shop's 100 daily orders is really just building this tree from the tally and rewriting each order with its code.

**Key point:** Huffman coding turns a frequency table into a near-entropy-optimal code — 200 bits instead of 300 here, a third off for free, with exact reversibility.

### Visualization (canvas `c3`, 720×300)

Single-panel horizontal bar chart: bits needed per 100 orders under the fixed 3-bit code vs the Huffman code, with the entropy floor drawn as a dashed limit line.

- **Title (bold 15px, `#1a5276`, top center):** "Same 100 Orders, One-Third Fewer Bits".
- **Axis:** horizontal 2px `#999` line at y=235 from x=180 to x=680 (width 500), scale 0 to 350 bits; 12px `#444` tick labels at 0, 100, 200, 300; 12px `#444` axis caption "bits per 100 orders" centered below.
- **Rows (bars 34px tall, left edge x=180), each with a left-aligned 12px `#444` label at x=20:**
  - "fixed 3-bit code" at y=95: bar to 300 bits, fill `rgba(107,114,128,0.35)`, 2px `#6b7280` outline; bold 13px `#6b7280` value "300 bits" at the bar end
  - "Huffman code" at y=165: bar to 200 bits, fill `rgba(0,131,0,0.30)`, 2px `#008300` outline; bold 13px green `#008300` value "200 bits" at the bar end
- **Entropy floor:** vertical dashed orange `#d95926` (dash 4/3) line at 198 bits from y=60 to the axis; bold 12px orange label at its top: "entropy floor ≈ 198".
- **Annotation (bold 13px green `#008300`, near x=430, y=140):** "33% smaller — and perfectly reversible".
- **Caption (12px `#444`, bottom right):** "illustrative — counts from the day's tally above".

## Why "Just Use Short Codes" Breaks

**Tags:** `common mistake` (red), `prefix-free` (orange)

- **Tempting shortcut** — latte 0, espresso 1, cappuccino 01: even shorter, so what could go wrong?
- **Ambiguity** — the bits "01" now read two ways: latte then espresso, or one cappuccino
- **Prefix-free** — Huffman codes sit at tree leaves, so no code can continue into another one
- **Not unique** — swapping 0/1 on any branch gives different codes with the same average length
- **Whole bits only** — Huffman cannot charge 1.98 bits; fancier coders squeeze out the last 1%

*Example (italic):* The Huffman stream "0101110" splits only one way — 0, 10, 1110 — latte, espresso, mocha; the shortcut code above can't promise that.

**Common mistake:** Judging a code only by its lengths. Codes must also be prefix-free to decode without separators — Huffman gives the shortest lengths that still keep that promise.

### Visualization (canvas `c4`, 720×300)

Two-row bit-strip diagram: the same idea of streaming bits, shown once with a non-prefix code that splits two ways (top, red) and once with the Huffman code that splits exactly one way (bottom, green).

- **Title (bold 15px, `#1a5276`, top center):** "One Bit Stream Must Mean One Thing".
- **Bit cells:** 44×44 squares, 2px outline, bold 16px monospace bit centered inside, drawn left-to-right from x=60.
- **Row 1 (cells at y=70), label 12px `#444` above at x=60:** "shortcut code (latte=0, espresso=1, cappuccino=01) — bits: 0 1"; two cells `0`, `1` outlined red `#e74c3c` on white; below them two bracket readings: a red bracket under both cells labeled bold 12px red "cappuccino?", and a second red bracket pair under each single cell labeled bold 12px red "latte + espresso?"; 12px red note at the right: "two readings — broken".
- **Row 2 (cells at y=190), label 12px `#444` above at x=60:** "Huffman code — bits: 0 1 0 1 1 1 0"; seven cells `0,1,0,1,1,1,0` outlined green `#008300`, fill `rgba(0,131,0,0.10)`; single green brackets underneath grouping cells 1 | 2–3 | 4–7, labeled bold 12px green "latte", "espresso", "mocha".
- **Annotation (bold 13px magenta `#d55181`, near x=430, y=155):** "no code starts another code — that's the prefix rule".
- **Caption (12px `#444`, bottom right):** "codes from the tree above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all counts, codes, node positions and bar values are the hardcoded literals above (no randomness); the tally is `[45, 25, 15, 10, 5]` for latte/espresso/cappuccino/mocha/tea everywhere, average length 2.00 bits, fixed baseline 300 bits, entropy ≈ 1.98 bits/order; text numbers and chart numbers must stay identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
