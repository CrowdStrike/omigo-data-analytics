# Count-Min Sketch

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Count-Min Sketch

**Subtitle:** When a stream is too big to keep a counter per item, a count-min sketch counts everything on a tiny fixed grid of shared boxes — and reads an item's count as the smallest box it touches, which can only overshoot, never undershoot

## A Tally Card Smaller Than the Menu

**Tags:** `core idea` (blue), `shared counters` (green), `fixed memory` (orange)

- **The cafe** — a busy cafe's register logs thousands of orders a day across a menu of 200 drinks
- **Too many counters** — keeping one tally per drink means 200 counters; a search engine would need billions
- **The card** — instead, use one small card: 2 rows of 4 boxes, sized before the day starts
- **The rules** — each row has its own rule (a hash) that sends every drink name to exactly one of its boxes
- **Shared boxes** — with 200 drinks and 4 boxes per row, strangers share a box; their counts pile up together
- **Reading it** — to count a drink, look at its one box in each row and take the smallest number

*Example (italic):* An order of "cola" bumps row A's box 2 and row B's box 1 by one each — two pen strokes, no matter how big the menu is.

**Key point:** A count-min sketch is a small fixed grid of shared counters: every arrival increments one box per row, and an item's count is read as the minimum across its boxes.

### Visualization (canvas `c1`, 720×300)

Routing diagram: one incoming order on the left, arrows into a 2×4 grid of boxes on the right, showing that each row's rule sends the same order to a different box.

- **Title (bold 15px, `#1a5276`, top center):** "One Order, Two Rules: 'cola' Lands in One Box per Row".
- **Order token:** rounded rectangle from (30, 125) to (160, 180), 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.12)`; bold 14px blue text "order: cola" centered inside; 12px `#6b7280` label "from the day's stream of orders" below it at y=200.
- **Grid:** two rows of 4 cells, each cell 85×52, 2px `#1a5276` borders, white fill; row A cells start at x=350, y=85; row B cells at x=350, y=175; 11px `#6b7280` labels "box 0", "box 1", "box 2", "box 3" centered above the row A cells at y=75.
- **Row labels (12px `#2c3e50`, right-aligned at x=340):** "row A — rule 1" beside row A (y=115), "row B — rule 2" beside row B (y=205).
- **Arrows:** two 3px blue `#2a78d6` arrows with filled arrowheads: from (160, 140) to the left edge of row A box 2 (x=520, y=111), and from (160, 165) to the left edge of row B box 1 (x=435, y=201); each target cell gets a "+1" in bold 15px green `#008300` centered inside.
- **Annotation (bold 12px orange `#d95926`, two lines, near x=350, y=45):** "200 drinks share 8 boxes —" / "each row splits them differently".
- **Caption (12px `#444`, bottom right):** "illustrative — a 2×4 card standing in for a much wider real sketch".

## Eight Orders on a Two-Row Card

**Tags:** `worked example` (blue), `take the min` (green)

- **The stream** — 8 lunch orders arrive: cola, chai, cola, soup, cola, cake, chai, cola
- **True counts** — cola 4, chai 2, soup 1, cake 1; the card never stores these, only its 8 boxes
- **Rule collisions** — row A sends cola AND chai to box 2; row B sends cola AND soup to box 1
- **Card after lunch** — row A reads 1, 0, 6, 1 and row B reads 1, 5, 0, 2 across its four boxes
- **Reading cola** — its boxes hold 6 (row A) and 5 (row B); take the min: estimate 5, truth 4
- **Reading chai** — its boxes hold 6 and 2; the min is 2, which happens to be exactly right

*Example (italic):* Row A's box 2 says 6 because cola's 4 and chai's 2 piled into the same box — the min across rows is what strips away most of that pile-up.

**Key point:** Estimate = min over rows of the item's box: cola gets min(6, 5) = 5 against a true 4 — one stray order of overcount, never any undercount.

### Visualization (canvas `c2`, 720×300)

The filled 2×4 card after all 8 orders, with cola's two boxes highlighted and the min readout spelled out beneath.

- **Title (bold 15px, `#1a5276`, top center):** "After 8 Orders: Read 'cola' as the Smallest of Its Boxes".
- **Grid:** two rows of 4 cells, each cell 100×54, 2px `#1a5276` borders, white fill; row A cells start at x=230, y=75; row B cells at x=230, y=165; 11px `#6b7280` labels "box 0"–"box 3" centered above row A at y=65.
- **Cell values (bold 18px `#2c3e50`, centered):** row A: `[1, 0, 6, 1]`; row B: `[1, 5, 0, 2]`.
- **Row labels (12px `#2c3e50`, right-aligned at x=220):** "row A" at y=106, "row B" at y=196.
- **Cola highlight:** row A box 2 and row B box 1 get a 3px orange `#d95926` border and fill `rgba(217,89,38,0.12)`; bold 11px orange tag "cola's box" just below each highlighted cell.
- **Collision notes (11px `#6b7280`):** "cola + chai share it" under row A box 2 at y=145 (below the orange tag), "cola + soup share it" under row B box 1 at y=235.
- **Annotation (bold 13px green `#008300`, bottom left at x=60, y=272):** "cola estimate = min(6, 5) = 5 — true count is 4".
- **Caption (12px `#444`, bottom right):** "illustrative 8-order lunch stream".

## Where the Small Card Beats the Big Ledger

**Tags:** `where it's used` (blue), `streams` (green), `heavy hitters` (orange)

- **Fixed size** — the card is sized before the stream starts and never grows, however long the day runs
- **Real streams** — trending search terms, top network flows, most-played songs: too many keys to tally exactly
- **Heavy hitters** — overcounts are tiny next to popular items' counts, so the big sellers rank correctly
- **Tunable error** — more boxes per row shrinks the overcount; more rows makes a lucky low box more likely
- **One pass** — each arrival costs one bump per row and is then forgotten; nothing is ever revisited

*Example (italic):* By closing time the cafe has logged 8,000 orders, and an exact ledger would be carrying 186 drink counters — the card is still the same 8 boxes it opened with.

**Key point:** Memory is chosen up front and stays flat: the stream can grow all day while the sketch keeps answering from the same few boxes.

### Visualization (canvas `c3`, 720×300)

Two-line chart over the day's stream: counters an exact ledger must keep (growing) versus the sketch's boxes (flat).

- **Title (bold 15px, `#1a5276`, top center):** "A Day of Orders: the Exact Ledger Grows, the Card Stays at 8 Boxes".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; x = orders seen 0 to 8,000 with 12px `#444` tick labels "0", "2,000", "4,000", "6,000", "8,000"; y = counters kept 0 to 200 with 12px `#444` tick labels "0", "50", "100", "150", "200" and light `#e5e9ef` gridlines at 50, 100, 150, 200; 12px `#6b7280` axis captions "orders seen" (below center) and "counters kept" (rotated, left).
- **Exact ledger line:** blue `#2a78d6` 3px line through orders = `[0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000]`, counters = `[0, 60, 95, 120, 140, 155, 168, 178, 186]`; 12px blue label "exact: one counter per distinct drink" above the curve near x=4,500.
- **Sketch line:** green `#008300` 3px line, flat at 8 counters across the full x range; bold 12px green label "sketch: 8 boxes, fixed" just above it near x=1,800.
- **End markers:** 6px dots at the right end of each line with bold 13px value labels "186" (blue) and "8" (green).
- **Annotation (bold 12px green `#008300`, two lines, near x=5,200, y=175):** "the card's size is chosen" / "before the day starts".
- **Caption (12px `#444`, bottom right):** "illustrative — counter growth invented".

## It Never Undercounts — and It Can't Name Items

**Tags:** `common mistake` (red), `one-sided error` (orange)

- **One-sided error** — boxes only ever gain from collisions, so every estimate is truth plus junk, never less
- **Not an average** — averaging cola's boxes gives (6+5)/2 = 5.5; the min, 5, is the tighter and safer read
- **Can't enumerate** — the card answers "how many cola?" but cannot list which drinks were ordered
- **Not a Bloom filter** — a Bloom filter answers "seen at all?"; the count-min sketch answers "roughly how many?"
- **Rare items suffer** — a 1-count drink sharing a box with a 4-count seller can look five times too popular

*Example (italic):* Asking the lunch card about "latte" — never ordered — returns min(6, 5) = 5, because latte's boxes happen to be cola's boxes; zero is never guaranteed, only "at most this".

**Common mistake:** Treating the estimate as exact or two-sided. It is an upper bound: fine for spotting big sellers, misleading for items whose true counts are near zero.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart of true count vs card estimate for the four lunch drinks plus never-ordered latte, making the one-sided overcount visible.

- **Title (bold 15px, `#1a5276`, top center):** "True Count vs Card Estimate — the Error Only Points Up".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = orders 0 to 6 with 12px `#444` tick labels "0", "2", "4", "6" and light `#e5e9ef` gridlines at 2, 4, 6.
- **Groups (five, centered at x = 130, 250, 370, 490, 610), 12px `#2c3e50` item labels below the baseline:** "cola", "chai", "soup", "cake", "latte".
- **Bars:** per group two 40px-wide bars 4px apart — true count in blue `#2a78d6` fill `rgba(42,120,214,0.75)`, estimate in orange `#d95926` fill `rgba(217,89,38,0.75)`; true = `[4, 2, 1, 1, 0]`, estimate = `[5, 2, 1, 1, 5]`; bold 12px value label in the bar's color above each bar (show "0" above latte's empty true slot).
- **Legend (12px, top left at x=80, y=55):** blue swatch "true count", orange swatch "card estimate".
- **Annotation (bold 12px orange `#d95926`, two lines, near x=500, y=95, with a thin orange pointer line to latte's estimate bar):** "never ordered, still reads 5 —" / "its boxes belong to cola".
- **Caption (12px `#444`, bottom right):** "illustrative — same 8-order lunch as above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all grid values, bar heights, and line points are the hardcoded arrays above (no randomness); the toy card is 2 rows × 4 boxes throughout, the 8-order stream is cola, chai, cola, soup, cola, cake, chai, cola, and every chart number matches the text (row A `[1, 0, 6, 1]`, row B `[1, 5, 0, 2]`, cola estimate 5 vs true 4, latte phantom 5, ledger endpoint 186 vs sketch 8).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
