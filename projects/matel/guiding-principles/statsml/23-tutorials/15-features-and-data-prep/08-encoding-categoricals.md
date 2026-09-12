# Encoding Categoricals

**Page type:** detail page (tutorial page: h1 + subtitle, 4 `.card-section` blocks each with an h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Encoding Categoricals

**Subtitle:** Turning labels like "red / green / blue" into numbers a model can use without inventing a fake order

## Red = 1, Green = 2, Blue = 3 Invents an Order

**Tags:** `core idea` (blue), `ordinal` (orange)

- **The table** — a t-shirt orders table with a color column (red / green / blue) and a size column
- **Models eat numbers** — "blue" means nothing to a formula, so labels must become numbers
- **The naive move** — red=1, green=2, blue=3 quietly claims blue > green > red
- **Fake distances** — it also claims blue is twice as far from red as from green — pure fiction
- **Real order exists too** — sizes S < M < L < XL genuinely order, so S=1..XL=4 is honest

*Example:* Nothing about a blue shirt is "two more" than a red one — but 1, 2, 3 tells the model exactly that.

**Key point:** First ask "do these labels truly have an order?" — sizes do (ordinal encoding is fine), colors do not (they need another trick).

### Visualization (canvas `c1`, 720×300)

Two horizontal number-line rulers contrasting a fake order (colors) with a real one (sizes).

- **Title (bold 16px, `#1a5276`, top center):** "Two Label Columns, Only One Has a Real Order".
- **Top ruler (y=100, from x=90 to width−90):** thin gray (`#6b7280`) horizontal line with three 11px-radius dots at 15%, 50%, 85% of the ruler width — red `#e74c3c`, green `#008300`, blue `#2a78d6` — labeled below in bold 13px: "red = 1", "green = 2", "blue = 3". Left label above the ruler in bold 13px `#2c3e50`: "color column:".
- **Fake-distance bracket:** dashed orange (`#d95926`, dash 5/4, width 2) horizontal line 24px above the ruler spanning from the red dot to the blue dot, with bold orange 13px caption above it: `"blue − red = 2"? meaningless — the order is invented`.
- **Bottom ruler (y=220, same span):** four 11px violet (`#4a3aa7`) dots at 10%, 36.6%, 63.2%, 89.8% of ruler width, each with a white bold 10px letter inside (S, M, L, XL) and a bold 13px violet label below: "S = 1", "M = 2", "L = 3", "XL = 4". Left label above in bold 13px `#2c3e50`: "size column:".
- **Green annotation (bold 13px `#008300`, centered, 24px above bottom ruler):** "sizes really do grow left to right — ordinal encoding is honest here".

## One-Hot: Five Orders Become a 0/1 Grid, by Hand

**Tags:** `worked example` (green), `one-hot` (blue)

- **The recipe** — one new column per color: is_red, is_green, is_blue
- **Fill it in** — each order gets a 1 in its own color's column and 0 in the other two
- **Order 1 (red)** — becomes the row 1, 0, 0; order 2 (blue) becomes 0, 0, 1
- **No fake order** — every color is now the same "distance" from every other color
- **Row sum is 1** — exactly one hot column per row, which is where the name comes from

*Example:* Five orders and three colors turn into a 5×3 grid of fifteen 0/1 cells — checkable by eye.

**Key point:** One-hot trades one column for one-column-per-label — perfectly honest about order, at the price of width.

### Visualization (canvas `c2`, 720×300)

Table-transformation diagram: a small orders table, an arrow labeled "one-hot", and the resulting 5×3 0/1 grid.

- **Title (bold 16px, `#1a5276`, top center):** "Five Orders, One Color Column → Three 0/1 Columns".
- **Left mini-table (x=90, top=70, row height 38):** two columns headed "order" and "color" (bold 13px `#1a5276`); 5 rows: #1 red, #2 blue, #3 green, #4 red, #5 blue. Cell borders `#e5e9ef`; order numbers in 13px `#2c3e50`; color words in bold 13px in their own color (red `#e74c3c`, green `#008300`, blue `#2a78d6`).
- **Arrow:** horizontal `#1a5276` line (width 3) from x=255 to x=320 at the table's vertical middle, filled triangular head, bold 12px label "one-hot" above it.
- **Right one-hot grid (x=370, cell width 84, row height 38):** column headers "is_red" (`#e74c3c`), "is_green" (`#008300`), "is_blue" (`#2a78d6`) in bold 13px. Data matrix rows: [1,0,0], [0,0,1], [0,1,0], [1,0,0], [0,0,1]. Hot cells filled with the header color at 0.85 alpha and a white bold 14px "1"; cold cells `#f8f9fa` with a muted (`#6b7280`) "0". All cells outlined in `#e5e9ef`.
- **Row-sum notes:** muted 12px "sum 1" to the right of each grid row.
- **Caption (bold 13px violet `#4a3aa7`, centered below the grid):** `exactly one "hot" cell per row — no order, no fake distances`.

## Why It Matters: The City Column with 5,000 Labels

**Tags:** `what goes wrong` (red), `target encoding` (blue), `where it's used` (blue)

- **Everywhere** — color, size, city, device, plan type: most business columns are labels, not numbers
- **Width explosion** — one-hot on 3 colors adds 3 columns; on 5,000 cities it adds 5,000
- **Sparse and slow** — 5,000 near-empty columns starve models of signal and memory
- **Target encoding** — replace each city with its average outcome, e.g. its return rate: one column
- **Frequency encoding** — or replace each city with how often it appears; also one column

*Example:* Chicago's 400 orders had a 12% return rate, so every "Chicago" cell becomes 0.12 — one number, one column.

**Key point:** One-hot for a handful of labels, target or frequency encoding for thousands — the label count picks the tool.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of columns created by one-hot encoding, on a log scale.

- **Title (bold 16px, `#1a5276`, top center):** "Columns Created by One-Hot Encoding Each Column".
- **Bars (start x=180, height 34, gap 18, first bar at y=62):** right-aligned bold 13px `#2c3e50` labels left of each bar; bar width proportional to log(v)/log(5000) of the max width (width − 180 − 160), minimum 14px:
  - "color (3 labels)" — 3 — blue `#2a78d6`
  - "size (4 labels)" — 4 — violet `#4a3aa7`
  - "device (12 labels)" — 12 — aqua `#199e70`
  - "city (5,000 labels)" — 5000 — orange `#d95926`
- **Value labels (bold 13px, bar color, right of each bar):** "3 new columns", "4 new columns", "12 new columns", "5,000 new columns".
- **Footnote (12px muted `#6b7280`, left-aligned under bars):** "bar widths on a log scale — the real city bar would be ≈1,700x the color bar".
- **Takeaway (bold 14px orange `#d95926`, centered):** "5,000 near-empty columns: use target encoding (city → its 1-column return rate) instead".

## The One Thing People Get Wrong: Target Encoding Leaks

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The trap** — a city with 1 order gets encoded as that order's own outcome: the answer leaks in
- **Looks brilliant** — training accuracy soars because rare cities memorize their own labels
- **Fails live** — new orders from those cities carry a memorized answer, not a pattern
- **The fix** — encode from other rows only (fold-wise), shrink rare cities toward the global mean
- **Also plan for** — a brand-new city at serving time: give it the global average, not an error

*Example:* Springfield had one returned order, so its encoding became 1.00 — the model "learned" Springfield means return.

**Key point:** If a category's encoding was computed using the very row being predicted, the model is reading the answer key — always encode from other rows.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart of target encodings per city, with the rare-city leak highlighted.

- **Title (bold 16px, `#1a5276`, top center):** "Target Encoding per City (computed on ALL data — the leak)".
- **Axes:** L-shaped gray (`#6b7280`) axes; padding top 60, bottom 66, left 80, right 40. Y ticks every 0.25 from 0.00 to 1.00 (12px muted labels, light `#e5e9ef` gridlines).
- **Bars (width 90, evenly gapped):** Chicago 0.12, Dallas 0.18, Boise 0.24 in blue `#2a78d6` at 0.75 alpha; Springfield 1.00 in red `#e74c3c` at full alpha. Value labels bold 13px `#2c3e50` above each bar; city names below the baseline; muted 12px order counts beneath: "400 orders", "120 orders", "25 orders", "1 order".
- **Global-mean line:** dashed green (`#008300`, dash 6/4, width 2) horizontal line at 0.15, labeled in bold 12px green: "global return rate 0.15 — shrink rare cities toward this".
- **Leak annotation (bold 13px red `#e74c3c`, two lines near the Springfield bar top):** "encoding = its own" / "answer: pure leak".
- **Caption (12px muted, bottom center):** "illustrative return rates".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle` paragraph, then four `.card-section` divs, each an `<h2>` (bottom border `2px solid #2980b9`) followed by a `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bold-term bullets, an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas width="720" height="300">`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276`. `ul` 0.92rem; `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 8px 12px, 0.9rem. Canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; `.tag.blue` background rgba(26,82,118,0.12) color `#1a5276`; `.tag.green` rgba(39,174,96,0.15) `#27ae60`; `.tag.red` rgba(231,76,60,0.12) `#e74c3c`; `.tag.orange` rgba(230,126,34,0.15) `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all charts 720×300 logical, drawn via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
