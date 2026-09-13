# B-Trees

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** B-Trees

**Subtitle:** A B-tree stores keys in wide sorted blocks stacked only a few levels deep, so finding one record among billions takes a handful of steps — it is the structure behind almost every database index

## A Filing Room with One Card at Reception

**Tags:** `core idea` (blue), `wide nodes` (green), `sorted ranges` (orange)

- **The filing room** — a clinic keeps 1,000 patient folders, numbered 000 to 999, in one big room
- **One card** — reception holds a single card: folders 0xx in cabinet 1, 1xx in cabinet 2, and so on
- **Inside a cabinet** — each cabinet's own card splits its hundred folders across 10 labeled drawers
- **Three steps** — reception card, cabinet card, drawer: any folder found without scanning the room
- **That's a B-tree** — a tree where every node is a fat block of sorted ranges, not one single value
- **Wide and shallow** — 10 choices per step means 3 levels cover 10 × 10 × 10 = 1,000 folders

*Example (italic):* Asked for folder 647, the receptionist glances at one card, walks to cabinet 7, opens the drawer labeled 640–649, and pulls the folder — three looks, zero searching.

**Key point:** A B-tree is this filing room as a data structure: each node holds many sorted keys, so a few wide steps replace a long scan.

### Visualization (canvas `c1`, 720×300)

Three-level diagram of the filing room: one reception card on top, a row of 10 cabinets, a row of 10 drawers, with the path to folder 647 highlighted in green.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Folders: One Card → Ten Cabinets → Ten Drawers".
- **Level labels (12px `#6b7280`, left-aligned at x=8):** "reception" at y=70, "cabinets" at y=160, "drawers of cab. 7" at y=246.
- **Reception card:** rounded rect x=270–450, y=48–84, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; bold 12px `#1a5276` line "reception card" and 11px `#2c3e50` line "0xx … 9xx → cabinets 1–10" centered inside.
- **Cabinet row:** 10 boxes 56×34 at y=138–172, left edges x = `[65, 127, 189, 251, 313, 375, 437, 499, 561, 623]`, 11px centered labels `["0xx", "1xx", "2xx", "3xx", "4xx", "5xx", "6xx", "7xx", "8xx", "9xx"]`; default fill `#f8f9fa`, 1px `#e5e9ef` border, `#6b7280` text; the "6xx" box gets fill `rgba(0,131,0,0.18)`, 2px `#008300` border, bold `#008300` text.
- **Drawer row:** 10 boxes same widths/x at y=224–258, 11px labels `["600–609", "610–619", "620–629", "630–639", "640–649", "650–659", "660–669", "670–679", "680–689", "690–699"]`; the "640–649" box highlighted green like the cabinet.
- **Arrows:** thin 1px `#ccc` lines from the reception card's bottom edge to the top of every cabinet box; from the "6xx" box bottom to the top of every drawer box; then a bold 3px `#008300` arrow (with arrowhead) reception → "6xx" and another "6xx" → "640–649" drawn on top.
- **Annotation (bold 12px orange `#d95926`, near x=490, y=110):** "3 steps to any folder — never scan all 1,000".
- **Caption (11px `#444`, bottom right at y=292):** "illustrative filing room — 10 × 10 × 10 = 1,000 folders".

## Finding Folder 647 in Three Reads

**Tags:** `worked example` (blue), `lookup` (green)

- **Read 1** — the reception card says 6xx → cabinet 7, so 647 must be there; nine cabinets skipped
- **Read 2** — cabinet 7's card says 640–649 → drawer 5; the other 90 folders in it are ruled out
- **Read 3** — drawer 5 holds ten folders 640 to 649 in order; slide to 647 and stop
- **The count** — 3 reads found 1 folder among 1,000; a straight scan averages 500 looks
- **Misses too** — if 647 were gone, the same 3 reads prove it: drawer 5 is the only place it fits

*Example (italic):* Every folder from 000 to 999 costs the same 3 reads — the filing room has no slow corners.

**Key point:** Each read cuts the search to a tenth — 1,000 → 100 → 10 → the folder — so 3 reads replace a 500-look average scan.

### Visualization (canvas `c2`, 720×300)

Three "page" cards left to right showing exactly what each read looks at, with the matching line highlighted and green arrows carrying 647 from card to card.

- **Title (bold 15px, `#1a5276`, top center):** "Reading Just 3 Pages to Find Folder 647".
- **Card headers (bold 12px `#1a5276`, centered above each card at y=62):** "read 1 — reception card", "read 2 — cabinet 7 card", "read 3 — drawer 5".
- **Cards:** three rounded rects 190×170 at x = `[40, 265, 490]`, y=70–240, fill `#f8f9fa`, 1px `#e0e0e0` border.
- **Card 1 lines (12px `#2c3e50`, left-padded 12px, 24px line spacing from y=95):** `["0xx → cabinet 1", "1xx → cabinet 2", "…", "6xx → cabinet 7", "…", "9xx → cabinet 10"]`; the "6xx → cabinet 7" line bold `#008300` on a full-width `rgba(0,131,0,0.12)` strip.
- **Card 2 lines:** `["600–609 → drawer 1", "610–619 → drawer 2", "…", "640–649 → drawer 5", "…", "690–699 → drawer 10"]`; "640–649 → drawer 5" highlighted the same way.
- **Card 3 lines:** `["640", "641", "…", "646", "647  ✓", "648", "649"]`; "647  ✓" highlighted bold green.
- **Arrows:** bold 3px `#008300` arrows with arrowheads across the gaps, card 1 → card 2 and card 2 → card 3, at mid-height y=155.
- **Annotation (bold 12px orange `#d95926`, centered at y=268):** "3 reads instead of a scan that averages 500 looks".
- **Caption (11px `#444`, bottom right at y=292):** "illustrative — keys are folder numbers 000–999".

## Why Every Database Index Works This Way

**Tags:** `where it's used` (blue), `disk pages` (orange), `scaling` (green)

- **Disk pages** — a database reads data one fixed-size page at a time, and a page holds ~100 sorted keys
- **Node = page** — a B-tree node is sized to exactly one page, so each tree level costs one page read
- **The payoff** — 100 choices per read: 2 reads cover 10 thousand rows, 3 cover 1 million, 5 cover 10 billion
- **Your queries** — a lookup like `WHERE id = 647` is instant because the index walks a few pages, not the table
- **Self-balancing** — a full node splits in two and pushes one key up, so the tree stays shallow as data grows
- **Everywhere** — the standard index in relational databases and many filesystems is a B-tree

*Example (italic):* On a 10-billion-row orders table, a key lookup touches about 5 pages, while a full scan would touch millions.

**Key point:** Because each node fills a whole disk page, one slow disk read eliminates 99% of the remaining data — that is why indexes make billion-row lookups feel free.

### Visualization (canvas `c3`, 720×300)

Bar chart of page reads needed per lookup as the table grows from 10 thousand to 10 billion rows, with fanout 100 per page.

- **Title (bold 15px, `#1a5276`, top center):** "With 100 Keys per Page, Reads Barely Grow".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = page reads 0 to 6 with light `#e5e9ef` gridlines and 12px `#444` labels at 1–6; x = four category labels (12px `#444`, centered under bars): `["10 thousand", "1 million", "100 million", "10 billion"]` with an 11px `#6b7280` sub-label "rows in the table" centered at y=285.
- **Bars:** reads = `[2, 3, 4, 5]`; width 80, centers at x = `[145, 295, 445, 595]`; fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; bold 13px `#1a5276` value labels "2", "3", "4", "5" above each bar.
- **Annotation (bold 12px green `#008300`, near x=470, y=90):** "10 billion rows → just 5 page reads".
- **Caption (11px `#444`, bottom right):** "reads = tree levels touched; fanout 100, illustrative".

## B Is Not for Binary

**Tags:** `common mistake` (red), `binary vs B-tree` (orange)

- **The mix-up** — people hear "tree" and picture a binary tree: one key per node, two children
- **Depth bill** — a binary tree on 1,000,000 keys needs about 20 levels, since 2^20 ≈ 1 million
- **Wide wins** — a B-tree node holding 100 keys reaches the same 1,000,000 in 3 levels (100^3)
- **Why depth hurts** — each level is a page read, so 20 reads vs 3 is the entire performance gap
- **RAM vs disk** — binary trees are fine in memory; B-trees exist because disk reads are the cost

*Example (italic):* Two engineers index the same million keys; the binary tree pays 20 reads per lookup, the B-tree pays 3.

**Common mistake:** Treating a B-tree as "a binary tree, but balanced". The defining feature is width — many keys per node, so each read eliminates 99% of the data instead of half.

### Visualization (canvas `c4`, 720×300)

Two horizontal bars comparing tree depth for the same 1,000,000 keys: a binary tree at 20 levels versus a B-tree at 3 levels.

- **Title (bold 15px, `#1a5276`, top center):** "Same 1,000,000 Keys: Binary Tree vs B-Tree".
- **Axis:** horizontal 2px `#999` line at y=250 from x=230 to x=680 (width 450), scale 0 to 22 levels; ticks at `[0, 5, 10, 15, 20]` with 12px `#444` labels below and light `#e5e9ef` vertical gridlines up to y=70; 11px `#6b7280` axis label "levels (one page read each)" centered at y=285.
- **Row 1 (bar center y=115), label 12px `#444` at x=20:** "binary tree — 2 children per node"; bar from 0 to 20, 26px tall, fill `rgba(217,89,38,0.35)`, 2px `#d95926` border; bold 13px `#d95926` label "20 levels" just right of the bar end.
- **Row 2 (bar center y=185), label 12px `#444` at x=20:** "B-tree — 100 keys per node (one page)"; bar from 0 to 3, 26px tall, fill `rgba(0,131,0,0.30)`, 2px `#008300` border; bold 13px `#008300` label "3 levels" just right of the bar end.
- **Annotation (bold 13px violet `#4a3aa7`, near x=380, y=185):** "wide nodes = one disk read each — that's the whole trick".
- **Caption (11px `#444`, bottom right):** "2^20 ≈ 1 million; 100^3 = 1 million".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box labels, card lines, bar values, and coordinates are the hardcoded literal arrays above (no randomness); the read counts `[2, 3, 4, 5]` assume fanout 100 and match the bullets exactly; arrowheads are small filled triangles drawn with `ctx.fill()`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
