# The DOM

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The DOM

**Subtitle:** The browser turns a page's HTML into a live tree of nodes it keeps in memory — every script, scraper, and devtools panel works on that tree, not on the file

## A Cafe Menu Becomes a Tree

**Tags:** `core idea` (blue), `live tree` (green), `nodes` (orange)

- **The page** — a cafe's menu page is 9 lines of HTML: a heading and a 3-item list
- **The parse** — the browser reads the file once and builds one node per tag as it goes
- **The tree** — `<html>` holds `<body>`, which holds `<h1>` and the `<ul>` with its 3 `<li>` items
- **The count** — html, body, h1, ul, and three li: 7 element nodes (ignoring the auto-inserted head)
- **Live, not text** — after parsing, the browser renders and edits the tree; the file is never reread

*Example (italic):* The line `<li>Latte — $4</li>` in the file becomes one `li` node in memory, hanging off the `ul` node, holding the text "Latte — $4".

**Key point:** The DOM (Document Object Model) is the browser's in-memory tree of nodes built from the HTML — the page you see is drawn from the tree, so changing a node changes the page instantly.

### Visualization (canvas `c1`, 720×300)

Tree diagram of the menu page: 7 element-node boxes connected by parent-child lines, the raw HTML's role shown by a small annotation on the left.

- **Title (bold 15px, `#1a5276`, top center):** "9 Lines of HTML Parse into a Tree of 7 Nodes".
- **Layout:** rounded boxes 96px wide, 30px tall, 8px radius, 12px `#2c3e50` labels centered; 2px `#6b7280` connector lines from each parent's bottom center to each child's top center.
- **Level 1 (y=55):** blue `#2a78d6` box at x=312 labeled "html", fill `rgba(42,120,214,0.15)`.
- **Level 2 (y=115):** blue box at x=312 labeled "body", same fill.
- **Level 3 (y=175):** aqua `#199e70` box at x=170 labeled "h1 'Bean There'", fill `rgba(25,158,112,0.12)`; violet `#4a3aa7` box at x=430 labeled "ul #menu", fill `rgba(74,58,167,0.12)`.
- **Level 4 (y=240):** three yellow `#c98500` boxes at x=310, 445, 580, fill `rgba(201,133,0,0.12)`, labeled "li Espresso $3", "li Latte $4", "li Mocha $5" — all children of the ul box.
- **Annotation (bold 13px blue `#2a78d6`, left side at x=25, y=150):** "one node per tag — 7 nodes total".
- **Caption (12px `#444`, bottom right):** "menu items and prices illustrative".

## Finding a Node, Changing It, Adding One

**Tags:** `worked example` (blue), `querySelector` (green), `before/after` (orange)

- **Find** — `document.querySelector('#menu')` walks the tree and returns the `ul` node
- **Change** — setting the second li's text to "Latte — $4.50" swaps the text inside that one node
- **Add** — `document.createElement('li')` makes a "Chai — $4" node; `appendChild` hangs it on the ul
- **The result** — the list grows from 3 items to 4 and the node count from 7 to 8
- **Instant repaint** — the screen updates the moment the tree changes; no reload, no new HTML

*Example (italic):* Three lines of script raise the latte to $4.50 and add chai for $4 — the customer's screen shows 4 items while the HTML file on the server still says 3.

**Key point:** Every DOM operation is a tree operation — find a node, edit a node, attach a node — and the rendered page follows the tree, not the original file.

### Visualization (canvas `c2`, 720×300)

Before/after pair of subtrees: the `ul` branch with 3 li children on the left, the same branch after the edits with 4 li children on the right, changes highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "The ul Branch Before and After the Script Runs".
- **Layout:** two panels split at x=360; 12px bold `#6b7280` panel labels "before (3 items)" at x=90 y=55 and "after (4 items)" at x=460 y=55; a bold 3px `#2a78d6` arrow with 12px label "script runs" crossing the split at y=150.
- **Left panel:** violet `#4a3aa7` box (96×28, 8px radius, fill `rgba(74,58,167,0.12)`) at x=130 y=80 labeled "ul #menu"; 2px `#6b7280` lines down to three yellow `#c98500` boxes (fill `rgba(201,133,0,0.12)`) at y=150, 195, 240 labeled "li Espresso $3", "li Latte $4", "li Mocha $5".
- **Right panel:** same ul box at x=470 y=80; four child boxes at y=140, 180, 220, 260 — "li Espresso $3" (yellow, unchanged), "li Latte $4.50" (green `#008300` border, fill `rgba(0,131,0,0.12)`), "li Mocha $5" (yellow, unchanged), "li Chai $4" (green dashed 2px border, fill `rgba(0,131,0,0.12)`, 11px green tag "new node" at its right).
- **Annotation (bold 13px green `#008300`, at x=470, y=292):** "one text edit + one new node: 7 → 8 nodes".
- **Caption (12px `#444`, bottom left):** "prices illustrative".

## Who Works on the Tree All Day

**Tags:** `where it's used` (blue), `scraping` (green), `tag managers` (orange)

- **Scrapers** — a price-tracking script parses the page into a tree, then reads every `li` under `ul#menu`
- **Analytics tags** — a tag manager is a script that appends new script nodes to the tree at load time
- **Devtools** — the Elements panel is a viewer for the live tree; editing a node there edits the page
- **Frameworks** — React and friends compute which nodes changed and touch only those
- **Tests** — browser tests find nodes ("the button labeled Pay") and fire events on them

*Example (italic):* A coffee-price tracker selects the 3 `li` nodes, reads $3, $4, $5 from their text, and stores the numbers — it never looks at the raw HTML string again.

**Key point:** The DOM is the shared workbench of the web — scrapers read it, tag managers and frameworks write it, and devtools lets a human do both by hand.

### Visualization (canvas `c3`, 720×300)

Hub diagram: the menu tree as a central box, with three labeled actors around it — a scraper reading, a tag manager writing, devtools doing both.

- **Title (bold 15px, `#1a5276`, top center):** "Three Actors, One Tree".
- **Hub:** ink-bordered `#1a5276` rounded box (170×70, 8px radius, fill `rgba(26,82,118,0.08)`) centered at x=360 y=160, 13px bold `#1a5276` label "the live DOM tree" with 11px `#6b7280` sub-label "7 nodes".
- **Actor boxes (140×46, 8px radius, 12px `#2c3e50` text):**
  - blue `#2a78d6` box at x=80 y=90 "scraper" with 11px sub-label "reads li prices $3 $4 $5", fill `rgba(42,120,214,0.15)`; 3px blue arrow pointing hub → scraper (a read).
  - green `#008300` box at x=80 y=220 "tag manager" with sub-label "appends 1 script node", fill `rgba(0,131,0,0.12)`; 3px green arrow tag manager → hub (a write).
  - violet `#4a3aa7` box at x=560 y=155 "devtools" with sub-label "views + edits nodes", fill `rgba(74,58,167,0.12)`; 3px violet double-headed arrow devtools ↔ hub.
- **Arrow labels:** 11px in each arrow's color — "read" on the blue arrow, "write" on the green, "read/write" on the violet.
- **Annotation (bold 13px orange `#d95926`, bottom center y=285):** "none of them touch the HTML file — only the tree".
- **Caption (12px `#444`, bottom right):** "actors schematic, node count from the menu example".

## View Source Shows the File, Not the Tree

**Tags:** `common mistake` (red), `DOM ≠ HTML` (orange)

- **The confusion** — "View Source" shows the HTML file the server sent; devtools shows the tree right now
- **They drift** — the moment any script edits a node, the tree no longer matches the source file
- **The menu** — after our script, the source still lists 3 items while the live tree holds 4
- **The scraper trap** — fetching raw HTML with a plain HTTP request sees 3 items and misses Chai
- **The fix** — to see what users see, run the page in a real browser (or headless one) and read the tree

*Example (italic):* An analyst downloads the menu page's HTML, counts 3 drinks, and reports chai isn't sold — devtools on the same page plainly shows the 4th li node.

**Common mistake:** Treating the HTML source as the page. The page is the DOM after scripts run; on script-heavy sites the source file can be a nearly empty shell while the tree holds everything.

### Visualization (canvas `c4`, 720×300)

Side-by-side panel comparison: "View Source" showing the 3-item file versus "DevTools Elements" showing the 4-item live tree, the missing item flagged.

- **Title (bold 15px, `#1a5276`, top center):** "Same Page, Two Answers: the File Says 3, the Tree Says 4".
- **Panels:** two rounded rectangles 300×200 at x=40 and x=390, y=60, 1.5px `#6b7280` border, fill `#fafbfc`; 13px bold panel headers "View Source (file sent by server)" in `#2c3e50` and "DevTools Elements (live tree)" in `#1a5276`.
- **Left panel rows (12px monospace `#2c3e50`, at y=115, 145, 175):** "<li>Espresso — $3</li>", "<li>Latte — $4</li>", "<li>Mocha — $5</li>"; below them at y=215, bold 12px red `#e74c3c` "3 items — no chai anywhere".
- **Right panel rows (12px monospace, y=105, 133, 161, 189):** "li Espresso — $3", "li Latte — $4.50", "li Mocha — $5" in `#2c3e50`, and "li Chai — $4" in bold green `#008300` with an 11px green tag "added by script"; the $4.50 shown in green as the edited text.
- **Divider:** vertical dashed `#6b7280` (dash 4/3) line at x=360 from y=60 to y=260.
- **Annotation (bold 13px red `#e74c3c`, centered at y=285):** "scrape the file and you miss what scripts added".
- **Caption (12px `#444`, bottom right):** "menu contents illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the menu (Espresso $3, Latte $4→$4.50, Mocha $5, added Chai $4), the 7→8 node count, and the 3-vs-4 item comparison are invented and labeled illustrative; the same numbers must appear identically in text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
