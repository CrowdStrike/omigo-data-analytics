# Two Sum & the Hash-Map Trade

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Two Sum & the Hash-Map Trade

**Subtitle:** Instead of comparing every item with every other item, remember what you have already seen and ask for the missing half — spending a little memory to make each question instant

## One Gift Card, Two Items

**Tags:** `core idea` (blue), `memory for time` (green), `running example` (orange)

- **The gift card** — a coffee-shop card has exactly $17 left, and you want two items that use it all
- **The menu** — six prices on the board: $8, $2, $12, $5, $14, $6
- **The slow way** — try every pairing of two items; six prices already means 15 pairs to check
- **The flip** — for each price, ask one question instead: "have I already seen 17 minus this?"
- **The notebook** — jot down every price you pass; the question then takes one glance, not a re-scan
- **The trade** — the notebook costs a little memory, and in return every check becomes instant

*Example (italic):* Standing at the $5 tag, the missing half is 17 − 5 = $12 — and $12 is already in the notebook, so the search is over.

**Key point:** Two Sum swaps "compare everything with everything" for "remember what you saw, ask for the missing half" — that swap is memory buying time.

### Visualization (canvas `c1`, 720×300)

Single-panel shelf diagram: six price tags in a row with the one matching pair highlighted and joined by an arc, and a few faint brute-force comparison arcs shown for contrast.

- **Title (bold 15px, `#1a5276`, top center):** "Six Menu Prices, One $17 Card — Which Two Fit Exactly?".
- **Price tags:** six rounded rectangles 80×56 centered at x = `[110, 210, 310, 410, 510, 610]`, y=165; labels bold 15px centered: "$8", "$2", "$12", "$5", "$14", "$6"; default style fill `#f8f9fa`, 2px `#6b7280` border, label `#2c3e50`.
- **Winning pair:** the "$12" and "$5" tags get 3px green `#008300` borders and fill `rgba(0,131,0,0.12)`; a 3px green arc from the top of "$12" (x=310) to the top of "$5" (x=410) peaking at y=95, with a bold 13px green label above the arc: "$12 + $5 = $17".
- **Failed checks:** three faint 1.5px `#e5e9ef`-stroke arcs under the tags (from x=110 to 210, 110 to 310, and 210 to 510, dipping to y=245) with one 11px `#6b7280` label below the middle arc: "15 possible pairs the slow way".
- **Annotation (bold 12px violet `#4a3aa7`, top left near x=70, y=70):** two lines: "one notebook question per price" / "beats checking every pair".
- **Caption (12px `#444`, bottom right):** "illustrative menu prices".

## Walking the Menu with a Notebook

**Tags:** `worked example` (blue), `hash map` (green)

- **Step 1** — see $8, need 17 − 8 = $9; the notebook is empty, so write down 8 and move on
- **Step 2** — see $2, need $15; the notebook holds {8}, no 15 there, so add the 2
- **Step 3** — see $12, need $5; the notebook holds {8, 2}, no 5 yet, so add the 12
- **Step 4** — see $5, need $12; the notebook holds {8, 2, 12} — 12 is there: answer $12 + $5
- **Four looks** — the pair turned up in 4 steps; pair-by-pair checking could take up to 15

*Example (italic):* The notebook never re-reads the menu — each price gets one glance, one question, one jot, and then the walk moves on.

**Key point:** At every price the move is identical: compute the missing half, check the notebook, then add yourself to it — the hit at step 4 needed only 4 looks, not 15 pairs.

### Visualization (canvas `c2`, 720×300)

Four-column step diagram: each column shows the current price tag, the "need" it computes, and the notebook contents below, with the fourth column lighting up green on the hit.

- **Title (bold 15px, `#1a5276`, top center):** "Four Steps to $17: price → need → check the notebook".
- **Columns:** centered at x = `[130, 290, 450, 610]`; bold 12px `#6b7280` step labels "step 1"–"step 4" at y=58.
- **Price tags (top of each column):** rounded rectangles 90×44 at y=70 (top edge), bold 15px labels "$8", "$2", "$12", "$5"; steps 1–3 fill `#f8f9fa` with 2px `#2a78d6` border and blue labels, step 4 fill `rgba(0,131,0,0.12)` with 3px `#008300` border and green label.
- **Need line:** bold 13px under each tag at y=140 — "need $9", "need $15", "need $12" in orange `#d95926`; step 4's "need $12" in bold green `#008300`.
- **Notebook boxes:** rounded rectangles 120×72 at y=165 (top edge), fill `#f8f9fa`, 2px `#6b7280` border, 11px `#6b7280` header "notebook" inside the top of each; contents 13px `#2c3e50` centered: "(empty)", "{ 8 }", "{ 8, 2 }", "{ 8, 2, 12 }" — in step 4 the "12" drawn bold green.
- **Flow arrows:** 2px `#6b7280` horizontal arrows between consecutive columns at y=115.
- **Annotation (bold 13px green `#008300`, centered under step 4 near x=610, y=272):** "12 is in the notebook — done: $12 + $5".
- **Caption (12px `#444`, bottom left):** "illustrative walk, target $17".

## Why the Trade Shows Up Everywhere

**Tags:** `where it's used` (blue), `speed vs memory` (orange)

- **The blow-up** — pair-checking grows fast: 100 items means 4,950 pairs, 1,000 items means 499,500
- **The line** — with a notebook the work is one look per item: 100 items, 100 looks
- **The name** — the notebook is a hash map, a structure answering "have I seen X?" in about one step
- **The pattern** — caches, database indexes, and lookup tables all buy speed with memory this way
- **Interviews** — Two Sum opens interviews because this single trade powers so many later answers

*Example (italic):* A database index is the same notebook grown up — extra storage kept on the side so each lookup is one step instead of a full scan.

**Key point:** Memory-for-time is the deal in its purest form: keep a little extra state so every question costs one step instead of one pass over all the data.

### Visualization (canvas `c3`, 720×300)

Single-panel line chart: comparisons needed versus list size, the pair-checking curve bending upward while the hash-map line stays flat and low.

- **Title (bold 15px, `#1a5276`, top center):** "Checks Needed as the Menu Grows".
- **Axes:** origin x=70, baseline y=250, plot width 590, plot height 185; x = number of items 0 to 100 with 12px `#444` tick labels "10", "20", ..., "100" every 10; y = checks 0 to 5,000 with 12px `#444` labels "1,000"–"5,000" on light `#e5e9ef` gridlines every 1,000.
- **Pair-checking curve:** orange `#d95926` 3px line through hardcoded points at items = `[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]`, checks = `[45, 190, 435, 780, 1225, 1770, 2415, 3160, 4005, 4950]`; 12px orange label "check every pair" beside the curve near items≈70.
- **Hash-map line:** blue `#2a78d6` 3px line through the same items grid, checks = `[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]`; 12px blue label "one notebook look per item" below the line near items≈55.
- **End markers:** 6px dots at (100, 4950) orange and (100, 100) blue with bold 13px value labels "4,950" and "100".
- **Annotation (bold 13px violet `#4a3aa7`, near items≈30, y=95):** two lines: "100 items: 4,950 pair checks" / "vs 100 notebook looks".
- **Caption (12px `#444`, bottom right):** "checks = n(n−1)/2 vs n; counts exact, menu illustrative".

## Don't Pair an Item with Itself

**Tags:** `common mistake` (red), `duplicates` (orange)

- **New target** — same menu, a $16 card: at the $8 item the missing half is 16 − 8 = $8
- **The trap** — "is 8 anywhere in the list? yes!" — but that yes is the item matching itself
- **The fix** — check only the notebook of prices seen BEFORE you, then add yourself after checking
- **Look behind** — at step 1 the notebook is empty, so the lone $8 correctly finds no partner
- **Real pair** — the walk continues and $14 finds the earlier $2 in the notebook: 2 + 14 = 16

*Example (italic):* One $8 item can never make $16 alone, but two separate $8 items could — the notebook of earlier prices is what tells those cases apart.

**Common mistake:** Searching the whole list for the missing half lets an item match itself. Only search what you have already walked past — check first, then jot yourself down.

### Visualization (canvas `c4`, 720×300)

Two-row diagram on the same six-tag menu: the top row shows the self-match trap crossed out, the bottom row shows the look-behind walk finding the true pair.

- **Title (bold 15px, `#1a5276`, top center):** "Target $16: the Self-Match Trap".
- **Row 1 (wrong, tags at y=95):** six rounded tags 64×40 centered at x = `[150, 240, 330, 420, 510, 600]` labeled bold 13px "$8", "$2", "$12", "$5", "$14", "$6", fill `#f8f9fa`, 2px `#6b7280` border; a 2.5px red `#e74c3c` loop arrow from the "$8" tag curling back to itself above it (peak y=58), a bold 14px red "×" on the loop, and a bold 12px red label to its right: "8 + itself = 16 — not allowed"; 12px `#444` row label "whole-list search" at x=20, y=88.
- **Row 2 (right, tags at y=205):** the same six tags at the same x positions; "$2" and "$14" get 3px green `#008300` borders and fill `rgba(0,131,0,0.12)`; a 3px green arc from "$14" (x=510) back to "$2" (x=240) dipping to y=262, with a bold 13px green label below the arc: "$2 + $14 = $16"; 12px `#444` row label "look-behind notebook" at x=20, y=198.
- **Direction cue:** an 11px `#6b7280` arrow-and-label above row 2 near x=600, y=170: "walk →, notebook holds earlier prices".
- **Annotation (bold 12px violet `#4a3aa7`, between the rows near x=150, y=152):** "check the notebook first, add yourself after".
- **Caption (12px `#444`, bottom right):** "illustrative, same menu as above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red appears only in c4's self-match trap (a genuine error state).
- **Data:** all prices, steps, and curve points are the hardcoded arrays above (no randomness); the c3 counts are exact values of n(n−1)/2 and n; the menu prices and gift-card targets are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
