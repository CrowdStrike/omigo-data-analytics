# Iterator & Generator

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Iterator & Generator

**Subtitle:** Hand out one item per ask instead of the whole pile at once — a design pattern so useful that languages turned it into the `yield` keyword

## One Receipt at a Time from the Box

**Tags:** `core idea` (blue), `lazy` (green), `design pattern` (orange)

- **The box** — a coffee shop keeps all 50,000 receipts from the year in one big box
- **The dump** — photocopying every receipt onto the accountant's desk buries her before work starts
- **The clerk** — instead, a clerk hands over one receipt per request and remembers his place in the box
- **The protocol** — the accountant only ever asks two things: "next one?" and "any left?"
- **The names** — that clerk is an iterator; a generator is the easy way to write one — a function with yield
- **The keyword** — the pattern proved so useful that languages baked it in as `yield` and the `for` loop

*Example (italic):* The accountant totals the whole year by saying "next" 50,000 times; her desk never holds more than one receipt.

**Key point:** An iterator hands out items one at a time on demand — the caller sees a simple "next" and never needs the whole collection in memory at once.

### Visualization (canvas `c1`, 720×300)

Line chart comparing desk load while totaling the box: photocopy-everything (desk holds 50,000 from the start) vs iterator (desk holds 1), over receipts processed.

- **Title (bold 15px, `#1a5276`, top center):** "Totaling 50,000 Receipts: What Sits on the Desk at Any Moment".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = receipts processed 0 to 50,000 with 12px `#444` tick labels "0", "10k", "20k", "30k", "40k", "50k"; y = receipts on the desk 0 to 50,000, gridlines `#e5e9ef` at 12,500/25,000/37,500.
- **Photocopy line:** orange `#d95926` 3px line through processed `[0, 10000, 20000, 30000, 40000, 50000]`, desk load `[50000, 50000, 50000, 50000, 50000, 50000]` — flat at the top; bold 12px orange label "photocopy everything: 50,000 on the desk" above it near x=25,000.
- **Iterator line:** green `#008300` 3px line through the same processed grid, desk load `[1, 1, 1, 1, 1, 1]` — drawn 3px above the baseline so it stays visible; bold 12px green label "iterator" at its right end.
- **Annotation (bold 13px green `#008300`, near x=25,000, y=200):** "the desk never holds more than 1 receipt".
- **Caption (12px `#444`, bottom right):** "receipt counts illustrative".

## Five Asks, Five Receipts: Tracing the Clerk by Hand

**Tags:** `worked example` (blue), `yield` (green)

- **Five receipts** — a slow Monday leaves just five receipts in the box: $3, $5, $2, $7, $4
- **Ask one** — the first "next" yields $3; the running total on the accountant's pad reads $3
- **Ask two** — the second ask yields $5; the pad now reads $3 + $5 = $8
- **The pause** — between asks the clerk does nothing; receipt #3 has not even been touched yet
- **Asks three to five** — $2, $7, $4 arrive in turn; the pad reads $10, then $17, then $21
- **The end** — a sixth ask answers "no more", and the loop stops itself

*Example (italic):* After the fifth ask the pad shows $21 — exactly the hand sum 3 + 5 + 2 + 7 + 4.

**Key point:** A generator runs only between "next" calls — it pauses at each yield, remembers exactly where it was, and resumes from that spot on the following ask.

### Visualization (canvas `c2`, 720×300)

Combo chart of the five asks: blue bars for the amount yielded at each ask, a green step line for the running total climbing to $21.

- **Title (bold 15px, `#1a5276`, top center):** "Five next() Calls: What Each Yield Hands Over, What the Pad Reads".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = ask number 1 to 5 centered at pixel x `[110, 230, 350, 470, 590]` with 12px `#444` labels "ask 1"–"ask 5"; y = dollars 0 to 25, gridlines `#e5e9ef` at 5/10/15/20.
- **Yield bars:** blue fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, width 44, centered on each ask, heights for amounts `[3, 5, 2, 7, 4]`; bold 12px `#2a78d6` value labels "$3", "$5", "$2", "$7", "$4" above each bar.
- **Running-total line:** green `#008300` 3px step line through the ask centers at totals `[3, 8, 10, 17, 21]`, 4px-radius green dots at each point; bold 12px `#008300` labels "$3", "$8", "$10", "$17", "$21" above the dots.
- **Annotation (bold 13px violet `#4a3aa7`, near x=350, y=70):** "nothing runs between asks — the clerk sleeps at yield".
- **Caption (12px `#444`, bottom right):** "receipt amounts illustrative".

## Files Bigger Than Memory, Pipelines Longer Than One Step

**Tags:** `where it's used` (blue), `big data` (green), `pipelines` (orange)

- **Big files** — a 40 GB orders log cannot fit in 16 GB of RAM; a generator reads one line at a time
- **Batching** — model-training loaders yield one batch per step instead of materializing every batch
- **Pipelines** — read → clean → total chains as three generators; each row flows through all three stages
- **Infinite streams** — a generator can yield forever (sensor readings); a list of "all readings" cannot exist
- **The history** — the Gang of Four wrote Iterator as a multi-class recipe in 1994; `yield` made it one line

*Example (italic):* Totaling the 40 GB log through a generator holds about one 200-byte line at a time instead of the whole file.

**Key point:** Iterators turn "load it all, then work" into "work as it arrives" — the only way to process data larger than memory, or streams that never end.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: peak memory for totaling a 40 GB log by loading it all vs streaming it through a generator, with the 16 GB RAM limit marked.

- **Title (bold 15px, `#1a5276`, top center):** "Totaling a 40 GB Orders Log on a 16 GB Machine".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, linear scale 40 GB = 440px; left-aligned 12px `#444` row labels at x=20.
- **Row 1 (y=100):** "read every line into a list — 40 GB": orange `#d95926` bar width 440, bold 12px red `#e74c3c` label "2.5× the RAM — crashes" at the bar end.
- **Row 2 (y=180):** "generator, one line at a time — 200 bytes": green `#008300` bar width 2, bold 12px green label "peak: one 200-byte line" beside it.
- **RAM marker:** vertical dashed red `#e74c3c` (dash 4/3) line at x=406 (16 GB on the 440px scale) from y=60 to y=230, 12px red label "16 GB RAM" at its top.
- **Bar style:** 18px tall, list bar fill `rgba(217,89,38,0.30)` with 2px `#d95926` border, generator bar solid green.
- **Annotation (bold 13px magenta `#d55181`, centered near y=260):** "peak memory is one line, not the whole file".
- **Caption (12px `#444`, bottom right):** "linear scale; file and line sizes illustrative".

## The Stream You Can Only Drink Once

**Tags:** `common mistake` (red), `single-use` (orange)

- **One pass** — a generator is a place in a stream, not a container; once finished it stays finished
- **The trap** — summing the receipts, then looping over the same generator again to count them
- **Silent zero** — no error is raised; the second loop simply sees nothing and reports a count of 0
- **The fix** — re-create the generator for each pass, or store the items in a list if two passes are needed
- **The check** — looping over the same variable twice? Ask whether it is a list or a spent generator

*Example (italic):* The sum pass reads all 5 receipts and gets $21; the count loop right after reports 0 receipts — the clerk's box is already empty.

**Common mistake:** Treating a generator like a list. A list can be walked many times; a generator is consumed by its first walk, and a second loop silently produces nothing.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: pass 1 (sum works) vs pass 2 (same generator, silent zero), shown as boxes flowing left to right.

- **Title (bold 15px, `#1a5276`, top center):** "Second Loop, Empty Stream: Why the Count Comes Back 0".
- **Row 1 (y=95), label 12px `#444` at x=20:** "pass 1 — sum"; blue `#2a78d6` rounded box at x=180 labeled "generator: 5 receipts" (12px), 3px arrow to a green `#008300` box at x=440 labeled "sum = $21" with bold 12px green "✓ 5 items yielded".
- **Row 2 (y=205), label:** "pass 2 — count"; gray `#6b7280` rounded box at x=180 labeled "same generator: spent", 3px arrow to a red `#e74c3c` box at x=440 labeled "count = 0" with bold 12px red "✗ silently wrong".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(107,114,128,0.12)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "re-create the generator for each pass — or make a list if you truly need two".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 50,000-receipt box, the Monday amounts `[3, 5, 2, 7, 4]` with running totals `[3, 8, 10, 17, 21]`, and the 40 GB / 200-byte / 16 GB memory figures are invented and labeled illustrative; 40 GB against 16 GB of RAM being 2.5× over is exact arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
