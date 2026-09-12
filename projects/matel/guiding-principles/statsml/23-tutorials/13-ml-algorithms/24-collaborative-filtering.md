# Collaborative Filtering

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Collaborative Filtering

**Subtitle:** "People like you also bought" works by finding customers whose past ratings match yours, then recommending what they loved and you haven't tried — no product descriptions needed

## Five Readers, One Missing Rating

**Tags:** `core idea` (blue), `ratings matrix` (green), `taste neighbors` (orange)

- **The bookshop** — an online bookshop stores star ratings (1–5) from five readers across five books
- **Your row** — you rated Starfall 5, Dragonkeep 4, Ghost Manor 1, City Lights 2, and skipped Quiet Garden
- **The lookalike** — Ana's row (5, 5, 1, 2 on the same four books) is almost a copy of yours
- **The bet** — Ana gave Quiet Garden 5 stars, so a reader with your taste will probably love it too
- **No content needed** — the system never reads the books; matching rating rows is the whole trick

*Example (italic):* The shop shows you "Readers like you also bought Quiet Garden" purely because Ana's ratings mirror yours.

**Key point:** Collaborative filtering predicts your missing ratings from people whose known ratings agree with yours — taste twins, not product features, drive the recommendation.

### Visualization (canvas `c1`, 720×300)

Ratings-matrix heatmap: 5 readers × 5 books, blue cells shaded by star rating, with your unknown Quiet Garden cell highlighted in yellow.

- **Title (bold 15px, `#1a5276`, top center):** "The Ratings Matrix: Five Readers × Five Books".
- **Data (rows = readers, cols = books Starfall, Dragonkeep, Ghost Manor, Quiet Garden, City Lights):** You `[5, 4, 1, null, 2]`; Ana `[5, 5, 1, 5, 2]`; Ben `[2, 1, 5, 2, 4]`; Cara `[4, 4, 2, 4, 3]`; Dev `[1, 2, 4, 1, 5]`.
- **Grid:** origin x=150, y=68; cell width 90, height 36; column labels 11px `#444` centered above each column at y=58 (two-word names on one line); row labels bold 12px `#2c3e50` right-aligned at x=142.
- **Cells:** fill `rgba(42,120,214, 0.10 + rating*0.14)`, 1px white gaps; rating number bold 13px, white when rating ≥ 4, `#1a5276` otherwise, centered in cell.
- **Missing cell (You × Quiet Garden):** fill `rgba(201,133,0,0.25)`, 2px `#c98500` border, bold 16px `#c98500` "?" centered.
- **Row highlights:** 2px `#1a5276` rounded border around the You row; 2px `#008300` border around the Ana row.
- **Annotation (bold 12px `#d55181`, right of grid at x=610, two lines):** "Ana's row" / "matches yours".
- **Caption (12px `#444`, bottom center y=288):** "stars 1–5, darker = higher; the ? is what we want to predict (illustrative)".

## Finding Your Nearest Neighbor by Hand

**Tags:** `worked example` (blue), `similarity score` (green)

- **The score** — for each reader, average the star gaps to you over the four books you both rated
- **Ana** — gaps 0, 1, 0, 0 average to 0.25, the smallest score on the board, so the closest taste
- **Cara** — gaps 1, 0, 1, 1 average to 0.75, a decent second neighbor
- **Ben and Dev** — both average 3.0, near-opposite taste, so their opinions get ignored
- **The prediction** — average the two neighbors on Quiet Garden: (Ana 5 + Cara 4) / 2 = 4.5 stars

*Example (italic):* Ana vs you on Dragonkeep is |5 − 4| = 1 star apart; summing her four gaps gives 1, and 1 / 4 = 0.25.

**Key point:** Score every customer by how little they disagree with you, keep the closest few, and predict the blank as their average — the whole algorithm fits on a napkin.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of each reader's average star gap to You (smaller = more similar), with a prediction callout box on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Average Star Gap to You (smaller = closer taste)".
- **Data (sorted):** Ana 0.25, Cara 0.75, Ben 3.0, Dev 3.0.
- **Bars:** start x=130, max width 340 at scale 0–3.5; four bars 30px tall, tops at y=60, 112, 164, 216; Ana fill `#008300`, Cara fill `#199e70`, Ben and Dev fill `rgba(107,114,128,0.45)`; reader names bold 12px `#2c3e50` right-aligned at x=122; value labels bold 13px in the bar color, 8px right of each bar end.
- **Axis:** baseline 2px `#1a5276` vertical line at x=130 from y=50 to y=256; ticks 0, 1, 2, 3 as 11px `#6b7280` labels along y=268.
- **Neighbor bracket:** dashed `#008300` (dash 4/3) rounded rectangle enclosing the Ana and Cara bars, bold 12px `#008300` label "your 2 nearest neighbors" above it at y=48.
- **Callout box (right side, x=520 to 700, y=90 to 190):** 1px `#e5e9ef` border, fill `#f8f9fa`; bold 12px `#1a5276` heading "predict Quiet Garden:"; lines 13px `#2c3e50` "Ana 5 ★, Cara 4 ★" then bold 15px `#008300` "(5 + 4) / 2 = 4.5 ★".
- **Caption (12px `#444`, bottom center y=290):** "Ben and Dev disagree with you by 3 stars on average — their votes are dropped".

## How "People Also Bought" Runs at Scale

**Tags:** `where it's used` (blue), `item-item` (green), `co-purchase` (orange)

- **The flip** — big shops compare items to items: two books are similar if the same people buy both
- **Why flip** — millions of customers change taste daily, but a book's buyer crowd is stable
- **The counts** — of 100 Starfall buyers, 84 also bought Dragonkeep and 61 bought Quiet Garden
- **The shelf** — the "people also bought" row under Starfall is just those counts, sorted
- **Everywhere** — the same trick drives video queues, playlist radio, and grocery coupons

*Example (italic):* Only 7 of 100 Starfall buyers also bought Ghost Manor, so it never appears on Starfall's shelf.

**Key point:** At scale the question flips from "which customers match you?" to "which items share buyers?" — precompute the co-purchase counts once, then serve the top of the sorted list.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of co-purchase counts: of 100 Starfall buyers, how many also bought each other book, top two highlighted as the shelf.

- **Title (bold 15px, `#1a5276`, top center):** "Of 100 Starfall Buyers, How Many Also Bought... (illustrative)".
- **Data:** Dragonkeep 84, Quiet Garden 61, City Lights 12, Ghost Manor 7.
- **Bars:** axis origin x=90, baseline y=240, chart height 175, scale 0–100; four bars 100px wide with 30px gaps starting at x=110; Dragonkeep and Quiet Garden fill `rgba(0,131,0,0.55)`, City Lights and Ghost Manor fill `rgba(107,114,128,0.35)`; count labels bold 14px above each bar (green `#008300` for the first two, `#6b7280` for the rest); book names 12px `#444` below the baseline.
- **Y-axis:** 2px `#1a5276` line at x=90; gridlines `#e5e9ef` at 25, 50, 75, 100 with 11px `#6b7280` labels.
- **Shelf bracket:** dashed `#008300` (dash 4/3) horizontal bracket over the first two bars, bold 13px `#008300` label "these two go on the shelf" centered above at y=42.
- **Annotation (bold 12px `#d55181`, over the last bar):** "7/100 — never shown".
- **Caption (12px `#444`, bottom center y=292):** "item-item counts are precomputed nightly, then served as a sorted list".

## Cold Start and the Popularity Trap

**Tags:** `common mistake` (red), `cold start` (orange), `feedback loop` (red)

- **Cold start** — a brand-new book has zero ratings, so no taste rows mention it and it is never shown
- **New customers too** — a first-time visitor has an empty row, so there is nobody "like" them yet
- **Rich get richer** — heavily rated books win more shelf slots, which earns them even more ratings
- **The numbers** — the top two books hold 980 and 640 ratings and grab 77% of all shelf slots
- **The fix sketch** — shops blend in content features or show new items to a small test slice

*Example (italic):* A new release with 0 ratings gets 0% of shelf slots while the 980-rating blockbuster takes 46%.

**Common mistake:** Assuming collaborative filtering treats all items fairly — it can only recommend what already has ratings, so unaided it buries new items and amplifies whatever is already popular.

### Visualization (canvas `c4`, 720×300)

Paired horizontal bar chart: for five books, ratings count (blue, left scale) next to share of shelf slots (orange), exposing the popularity loop and the cold-start zero.

- **Title (bold 15px, `#1a5276`, top center):** "Ratings Owned vs Shelf Slots Won (illustrative)".
- **Data:** Blockbuster 980 ratings / 46% of slots; Old Favorite 640 / 31%; Steady Seller 310 / 19%; Niche Gem 45 / 4%; New Release 0 / 0%.
- **Layout:** five row groups, group tops at y=56, 100, 144, 188, 232; book names bold 12px `#2c3e50` left-aligned at x=15; each group has two 14px-tall bars starting at x=140 (ratings bar on top, slots bar 17px below).
- **Ratings bars:** fill `rgba(42,120,214,0.55)`, width scaled 0–1000 over 300px max; value labels 12px `#2a78d6` right of each bar ("980", "640", "310", "45", "0").
- **Slot bars:** fill `rgba(217,89,38,0.6)`, width scaled 0–50% over 300px max; labels 12px `#d95926` ("46%", "31%", "19%", "4%", "0%").
- **Legend (top right, y=40):** two 12px swatch+label pairs, blue "ratings count", orange "share of shelf slots".
- **Cold-start annotation (bold 12px `#e74c3c`, right of the New Release group, two lines):** "0 ratings → never shown" / "(cold start)".
- **Loop annotation (bold 12px `#d55181`, right-aligned at x=705 beside the top group):** "top 2 books take 77% of slots".
- **Caption (12px `#444`, bottom center y=292):** "more slots → more ratings → more slots: the loop runs until someone breaks it".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data rule:** all arrays above are hardcoded literals — no `Math.random()`; invented numbers keep their "(illustrative)" labels.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
