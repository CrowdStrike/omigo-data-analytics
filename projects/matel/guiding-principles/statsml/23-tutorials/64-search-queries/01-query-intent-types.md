# Query Intent Types

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Query Intent Types

**Subtitle:** Every search wants one of three things — to reach a place, to learn something, or to get something done — and the results page is built around that guess

## Three Queries, One Search Box

**Tags:** `core idea` (blue), `three intents` (green)

- **Same box** — three queries typed: "gmail login", "how to boil eggs", "running shoes size 10"
- **Different wants** — a door to walk through, an answer to read, a shelf of products to browse
- **Navigational** — reach a place: the searcher already knows the site; the query is a typed shortcut
- **Informational** — learn something: the searcher wants text that answers a question
- **Transactional** — get something done: buy, book, download, or find one nearby
- **The guess** — the engine guesses the want first, then builds the whole page around that guess

*Example (italic):* Answering "gmail login" with an essay on the history of email would be useless — that searcher wants exactly one link.

**Key point:** One search box serves three different jobs. Deciding which job a query is doing comes before any ranking of results.

### Visualization (canvas `c1`, 720×300)

Three columns, one per query: the typed query in a chip at the top, an arrow down, and a differently-shaped mini result-page mockup below, each labeled with its intent class.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "Three Queries, Three Page Shapes".
- **Columns centered at x = 125, 362, 599.** Each column: query chip (rounded feel via plain rect, w=200 h=26, y=40), 2px border in the class color, query text bold 12px `#2c3e50` centered; short arrow down (class color, 2px, arrowhead) from y=70 to y=88.
- **Mockup panels (w=190 h=145, y=92, white fill, 1px `#ccc` border):**
  - *Navigational (blue `#2a78d6`):* one dominant result — big bar (x+12, y+14, 166×30) fill `rgba(42,120,214,0.18)`, 2px blue border, bold 11px blue text "gmail.com — Sign in"; below it three faded thin gray bars (`#d5d9df`, h=7, widths 150/130/140) = ignored results.
  - *Informational (green `#008300`):* answer box (166×52) fill `rgba(0,131,0,0.08)`, 2px green border, three thin dark-green text lines inside; below it two gray two-line article stubs.
  - *Transactional (orange `#d95926`):* 2×2 product cards (each 78×56, 2px orange border), each with a small gray image square and a bold 11px orange price: "$79", "$95", "$64", "$88".
- **Class labels under each panel:** bold 12px in the class color at y=258 ("navigational", "informational", "transactional"); 11px `#6b7280` sub-label at y=276 ("reach a place · one dominant link", "learn something · answer first", "get something done · shelf + prices").

## Classifying Eight Queries by Hand

**Tags:** `worked example` (blue), `word cues` (orange)

- **Word cues** — a site name, a question word, or a shopping word is usually enough to call the class
- **Site names** — "gmail login", "amazon": a name you could type into an address bar → navigational
- **Question words** — "how", "why", "what" open informational queries: "how to boil eggs"
- **Shopping words** — "buy", a size, a price cap, "near me" all mark transactional queries
- **The tricky pair** — "amazon" is navigational; "amazon rainforest facts" is informational
- **Two extra words** — a short added phrase can flip the class completely

*Example (italic):* "pizza near me" has no question word and no site name — "near me" alone makes it transactional.

**Key point:** You can classify most everyday queries by hand from one cue word — but the cue is read off the whole query, as the "amazon" pair shows.

### Visualization (canvas `c2`, 720×300)

Table-like chart: eight query rows, the cue word rendered bold in its class color inside the query text, the cue reason in a middle column, and a colored class pill on the right; the last two rows (the "amazon" pair) sit on a highlighted band.

- **Title (bold 15px ink, top center, y=22):** "Eight Queries, One Cue Each".
- **Header (11px `#6b7280`, y=46):** "cue word shown in its class color" at x=40; "cue" at x=340; "class" at x=630 (centered).
- **Rows at y = 68, 95, 122, 149, 176, 203, 230, 257.** Query text starts at x=40, 13px system-ui; prefix/suffix in `#2c3e50`, cue segment bold in class color (segment widths via `measureText`). Cue reason 11px `#6b7280` at x=340. Class pill: rect x=565 w=130 h=18 centered on the row, tint fill + bold 11px class-color label.
- **Row data (prefix | cue | suffix | reason | class):** "" | gmail | " login" | site name | navigational; "" | how to | " boil eggs" | question words | informational; "running shoes " | size 10 | "" | a size | transactional; "" | why | " is the sky blue" | question word | informational; "pizza " | near me | "" | "near me" | transactional; "" | buy | " winter jacket" | "buy" | transactional; "" | amazon | "" | brand name | navigational; "amazon " | rainforest facts | "" | topic words | informational.
- **Class colors:** navigational blue `#2a78d6` (tint `rgba(42,120,214,0.12)`), informational green `#008300` (tint `rgba(0,131,0,0.10)`), transactional orange `#d95926` (tint `rgba(217,89,38,0.12)`).
- **Highlight band:** `rgba(201,133,0,0.10)` rect (x=25, y=213, w=670, h=54) behind the two "amazon" rows.
- **Annotation (bold 12px yellow `#c98500`, centered, y=288):** "two extra words flip 'amazon' from a website to a topic".

## The First Guess Every Query Gets

**Tags:** `where it's used` (blue), `common mistake` (red)

- **First model** — intent classification is the first model a query touches, before any ranking runs
- **Layout decider** — the class picks the page shape: one big link, an answer box, or a shelf
- **Ranker feature** — the predicted class then feeds every downstream ranker as an input feature
- **Wrong class, wasted ranking** — a perfectly ranked page of the wrong type still fails the searcher
- **Illustrative cost** — right class ~90% satisfied vs wrong class ~25%; numbers illustrative

*Example (illustrative, italic):* A shelf of egg cookers for "how to boil eggs" can rank its products perfectly and still help no one.

**Key point:** Getting the intent wrong makes every later step irrelevant — which is exactly why this guess runs first.

### Visualization (canvas `c3`, 720×300)

Side-by-side before/after: the same query classified correctly on the left and incorrectly on the right, each flowing to its result page mockup and a satisfaction bar (90% vs 25%, labeled illustrative).

- **Title (bold 15px ink, top center, y=22):** "Same Query, Right vs Wrong Class (illustrative)".
- **Divider:** dashed `#bdc3c7` vertical line at x=360, y=35 to y=280 (dash 4/3). Halves centered at x=180 and x=540.
- **Query chips (both halves, y=42, w=170 h=24):** 2px ink border, bold 12px "how to boil eggs"; arrow down to the classifier pill.
- **Classifier pills (y=86, w=190 h=22):** left fill green `#008300`, bold 11px white "classified: informational"; right fill red `#e74c3c`, bold 11px white "classified: transactional".
- **Mockups (y=122, w=190 h=92, white fill, 1px `#ccc` border):** left = answer box (green border, `rgba(0,131,0,0.08)` fill, three dark-green text lines) over one gray article stub; right = two orange-bordered product cards side by side, bold 11px orange "egg cooker $39" and "egg timer $12".
- **Satisfaction bars (y=236, h=18, track w=190 in `#eef1f4`):** left green fill 171px (=90%), right red fill 48px (=25%); bold 13px value labels "90% satisfied" (green) and "25% satisfied" (red) above each bar at y=230.
- **Caption (11px `#6b7280`, centered, y=292):** "satisfaction numbers are illustrative".

## Intent Lives in the Whole Query, Not One Word

**Tags:** `common mistake` (red), `rule of thumb` (orange)

- **Not per word** — intent is a property of the whole query, never of any single word in it
- **Context flips it** — "amazon" → a site; "+ rainforest facts" → a topic; "+ returns label" → a task
- **Rough split** — about half of web queries are informational; the rest split between the other two
- **Blurry edges** — "running shoes review" sits between learning and buying; the classes overlap
- **Session clues** — the queries typed before and after often reveal what a lone query hides

*Example (italic):* The same six letters "amazon" carry three different intents depending on nothing but what follows them.

**Key point:** Treat intent as a per-query prediction with uncertainty, not a dictionary lookup — and treat any published share numbers as rough (the 50 / 20 / 30 split here is illustrative).

### Visualization (canvas `c4`, 720×300)

Left panel: one base word fanning out into three completed queries, each with its class pill. Right panel: a three-bar chart of the illustrative intent share of web queries.

- **Title (bold 15px ink, top center, y=22):** "One Word, Three Intents — and the Rough Traffic Split".
- **Divider:** dashed `#bdc3c7` vertical line at x=380, y=35 to y=280.
- **Left rows at y = 70, 150, 230.** Query box (x=30, w=210, h=26, 1px `#ccc` border): text 12px, base word "amazon" in `#2c3e50`, added words bold in the class color ("", " rainforest facts", " returns label"). Arrow (class color) to class pill (x=262, w=112, h=20, tint fill, bold 11px class-color text): navigational / informational / transactional.
- **Left caption (11px `#6b7280`, centered at x=200, y=286):** "same first word — the extra words decide".
- **Right bars:** subtitle bold 12px ink centered at x=550, y=52: "share of web queries (illustrative)". Baseline y=240 (1px `#999`, x=430 to x=690). Three bars w=64: informational 50% (green, height 160px), navigational 20% (blue, 64px), transactional 30% (orange, 96px); tint fills with 2px class-color borders; bold 13px class-color "%" labels above the bars; 11px `#444` class names below the baseline.
- **Right caption (11px `#6b7280`, centered at x=550, y=290):** "labels move with wording — shares are rough".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` (no index number); subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Class color mapping used on every chart: navigational = blue, informational = green, transactional = orange; red `#e74c3c` only for the misclassification state in c3.
- **Data:** everything is hardcoded (no randomness): the eight c2 rows and their cue segments, the c3 satisfaction pair 90% / 25%, and the c4 split 50 / 20 / 30 — all invented and labeled "illustrative" where numeric. Text numbers match chart numbers (90/25 in section 3, 50/20/30 in section 4).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
