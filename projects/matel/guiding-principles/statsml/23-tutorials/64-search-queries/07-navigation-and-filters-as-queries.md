# Navigation & Filters as Queries

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Navigation & Filters as Queries

**Subtitle:** Clicking through menus and checking filter boxes states a complete query without typing a word

## A Query Made of Three Clicks

**Tags:** `core idea` (blue), `clicks as queries` (green)

- **No keyboard** — clicking Menswear, then Shoes, then Running states "men's running shoes"
- **Menus are queries** — every category page is the answer to a question a click asked
- **Breadcrumbs** — the trail Menswear → Shoes → Running reads exactly like a query written out
- **Same want** — the click path and the typed phrase point at the identical shelf of shoes
- **Stated, not typed** — a query is any stated want; typing is only one way to state it

*Example (italic):* A shopper who never touches the search box has still asked for men's running shoes — the store heard it in three clicks.

**Key point:** Finding things by clicking is also querying — menus, categories, and breadcrumbs are queries made of clicks.

### Visualization (canvas `c1`, 720×300)

A three-chip breadcrumb trail (the clicks) flowing down into a search-box mockup showing the typed equivalent, with a words-typed / clicks-made pair of stat chips below.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "Three Clicks Spell the Same Query".
- **Click labels (11px `#6b7280`, y=54, centered over each chip):** "click 1", "click 2", "click 3".
- **Breadcrumb chips (y=62, h=30, w=140, white fill, 2px blue `#2a78d6` border):** "Menswear" (x=80), "Shoes" (x=290), "Running" (x=500); bold 12px `#2c3e50` centered text. Blue right-arrows in the gaps (x 228→282 and x 438→492, at chip mid-height y=77).
- **Down arrow (ink, 2px, arrowhead):** from (360, 100) to (360, 130).
- **Search-box mockup (x=185, y=134, w=350, h=36, white fill, 2px ink border):** magnifier icon (circle r=6 at (208,151) + handle stroke, `#6b7280`), query text bold 13px green `#008300` "men's running shoes" left-aligned at x=228, baseline y=157.
- **Sub-label (bold 12px green, centered, y=194):** "the same query, written out".
- **Stat chips (y=216, h=26):** left rect x=130 w=200, fill `rgba(42,120,214,0.12)`, bold 12px blue "words typed: 0" centered at x=230; right rect x=390 w=200, fill `rgba(25,158,112,0.12)`, bold 12px aqua `#199e70` "clicks made: 3" centered at x=490.
- **Caption (11px `#6b7280`, centered, y=286):** "a browse path is a query made of clicks".

## Cutting 12,400 Results Down to 42

**Tags:** `worked example` (blue), `narrowing funnel` (orange)

- **Start typed** — "running shoes" returns 12,400 results: far too many to scan
- **Check a brand box** — 12,400 drops to 3,100; like adding the brand word to the query
- **Pick size 10** — 3,100 drops to 640; like adding "size 10" exactly
- **Price under $100** — 640 drops to 42: a shelf a human can actually browse
- **Exact words** — each filter acts like an added query word with one fixed, unambiguous meaning
- **No typos** — a checkbox cannot be misspelled and never means two different things

*Example (illustrative, italic):* Four small clicks did the work of typing a five-word query — with zero spelling risk.

**Key point:** Each filter behaves like adding an exact word to the query — the counts 12,400 → 3,100 → 640 → 42 are illustrative, but the narrowing shape is universal.

### Visualization (canvas `c2`, 720×300)

A four-step funnel of centered horizontal bars: the result count shrinks at each applied filter, with keep-fractions labeled on the connecting arrows.

- **Title (bold 15px ink, top center, y=22):** "Four Steps From 12,400 Results to 42 (illustrative)".
- **Rows (bar top y = 52, 110, 168, 226; bar h=30; bars centered on x=430):** widths 480, 300, 170, 90.
- **Step colors + tints:** blue `#2a78d6` / `rgba(42,120,214,0.15)`; aqua `#199e70` / `rgba(25,158,112,0.15)`; orange `#d95926` / `rgba(217,89,38,0.15)`; violet `#4a3aa7` / `rgba(74,58,167,0.15)`. Tint fill, 2px step-color border.
- **Left step labels (bold 12px in step color, left-aligned at x=40, baseline = bar top + 20):** `typed: "running shoes"`, `+ brand box checked`, `+ size 10`, `+ price under $100`.
- **Counts inside bars (bold 13px step color, centered at x=430):** "12,400 results", "3,100", "640", "42 results".
- **Connecting arrows (2px, next step's color, x=430):** from bar bottom +4 to next bar top −4; keep-fraction labels 11px `#6b7280` left-aligned at x=448, mid-gap (y=99, 157, 215): "keep 1 in 4", "keep 1 in 5", "keep 1 in 15".
- **Caption (11px `#6b7280`, centered, y=290):** "counts illustrative · bar widths not to scale".

## Clicks Feed the Ranker Too

**Tags:** `where it's used` (blue), `hidden signal` (green)

- **Same signal** — filter clicks and browse paths feed ranking and personalization like typed queries
- **Price sensitivity** — sorting by lowest price tells the store you shop on price, without a word
- **Division of labor** — typed words find the pile; filters cut the pile down to a browsable shelf
- **Most searches mix** — a typical store search is a short phrase plus two or three filter clicks
- **Ignoring clicks** — a ranker fed only typed text throws away half of what the shopper said

*Example (italic):* Two shoppers type the same "running shoes"; one sorts by lowest price, one filters to one brand — the store can now tell them apart.

**Key point:** If it changes what results you see, it is query input — whether it arrived by keyboard or by mouse.

### Visualization (canvas `c3`, 720×300)

Two input boxes (typed words, filter clicks) converging with arrows into one ranking-and-personalization box, which flows on to the next results page; a magenta annotation calls out what sort-by-lowest-price reveals.

- **Title (bold 15px ink, top center, y=22):** "Typed Words and Filter Clicks Feed the Same Ranker".
- **Input box A (x=35, y=55, w=230, h=52, fill `rgba(42,120,214,0.12)`, 2px blue border):** bold 12px blue "typed words" centered at (150, 75); 12px `#2c3e50` `"running shoes"` at (150, 95).
- **Input box B (x=35, y=150, w=230, h=68, fill `rgba(217,89,38,0.12)`, 2px orange border):** bold 12px orange "filter clicks" centered at (150, 170); 11px `#2c3e50` "brand ✓ · size 10" at (150, 188) and "sort: lowest price" at (150, 204).
- **Converging arrows (2px, box color, with heads):** (265, 81) → (328, 120) in blue; (265, 184) → (328, 152) in orange.
- **Ranker box (x=330, y=105, w=170, h=64, fill `rgba(26,82,118,0.12)`, 2px ink border):** bold 12px ink, two centered lines at (415, 131) "ranking &" and (415, 149) "personalization".
- **Output arrow (ink, 2px):** (502, 137) → (556, 137); **result box (x=558, y=110, w=140, h=54, fill `rgba(0,131,0,0.08)`, 2px green border):** bold 12px green, two centered lines at (628, 132) "your next" and (628, 148) "results page".
- **Annotation (bold 12px magenta `#d55181`, centered, y=252):** `"sort: lowest price" quietly says: this shopper shops on price`.
- **Caption (11px `#6b7280`, centered, y=286):** "mouse input and keyboard input land in the same models".

## Browsing and Searching Are Not Rivals

**Tags:** `common mistake` (red), `rule of thumb` (orange)

- **Not rivals** — browsing and searching are two ways of asking, not two kinds of people
- **Knows the words** — the shopper types "waterproof trail shoes" when the vocabulary is there
- **Doesn't know them** — the same shopper clicks Menswear → Shoes → Running when it isn't
- **Same log** — both trails land in the same interaction log and describe the same shopper
- **Often both** — a single visit can type one phrase and then click four filters on the results

*Example (italic):* The shopper who types "waterproof trail shoes" on Monday clicks Menswear → Shoes → Running on Saturday — same person, one log.

**Key point:** Type when you know the words, browse when you don't — both trails are queries, and both land in the same log.

### Visualization (canvas `c4`, 720×300)

The same shopper on two days: a typed-query chip on the left, a breadcrumb-trail chip on the right, both arrows converging into one search-log box.

- **Title (bold 15px ink, top center, y=22):** "Two Ways of Asking, One Log".
- **Day labels (11px `#6b7280`, y=50, centered):** "Monday — knows the words" at x=180; "Saturday — doesn't know the words" at x=540.
- **Typed chip (x=85, y=58, w=190, h=28, white fill, 2px blue border):** bold 12px blue `"waterproof trail shoes"` centered at (180, 76).
- **Browse chip (x=425, y=58, w=230, h=28, white fill, 2px orange border):** bold 12px orange "Menswear → Shoes → Running" centered at (540, 76).
- **Converging arrows (2px, chip color, with heads):** (180, 94) → (322, 168) in blue; (540, 94) → (398, 168) in orange.
- **Log box (x=270, y=172, w=180, h=56, fill `rgba(26,82,118,0.12)`, 2px ink border):** bold 13px ink "one search log" centered at (360, 196); 11px `#6b7280` "both trails recorded" at (360, 214).
- **Annotation (bold 12px aqua `#199e70`, centered, y=258):** "same shopper, two ways of asking".
- **Caption (11px `#6b7280`, centered, y=288):** "browsing and searching are two inputs, not two kinds of people".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` (no index number); subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers: `arrowDown`, `arrowRight`, and `arrowTo` (line with a rotated arrowhead for diagonal converging arrows in c3/c4).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Typed input is blue, click/filter input is orange throughout; red is unused (no error state on this page).
- **Data:** everything is hardcoded (no randomness): the funnel counts 12,400 / 3,100 / 640 / 42 with bar widths 480 / 300 / 170 / 90 and keep-fractions "1 in 4", "1 in 5", "1 in 15" — all invented and labeled "illustrative". Text numbers match chart numbers (the same four counts appear in section 2's bullets, key point, and chart).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
