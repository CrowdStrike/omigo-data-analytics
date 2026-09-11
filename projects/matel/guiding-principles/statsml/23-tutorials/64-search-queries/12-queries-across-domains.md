# Queries Across Domains

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Queries Across Domains

**Subtitle:** The same person types differently in web search, a store, a map, and a video app — length, vocabulary, and intent shift with the search box

## One Saturday, Five Search Boxes

**Tags:** `core idea` (blue), `five dialects` (green)

- **One person** — same Saturday, same phone, five different search boxes
- **Web box** — "weather today": short and general, a topic to read about
- **Store box** — "running shoes men size 10 under 100": product attributes typed as plain words
- **Map box** — "coffee near me": a place type plus proximity; location does half the work
- **Video box** — "lofi study mix 1 hour": mood, genre, and duration packed into one phrase
- **Docs box** — "reset api key": bare task words aimed at exactly one help page

*Example (italic):* The person did not change between boxes — the box did; each search box teaches its users a local dialect.

**Key point:** Query style is a property of the search box, not of the person typing into it — the same searcher speaks five dialects in one day.

### Visualization (canvas `c1`, 720×300)

Five rows, one per search box: a colored domain pill on the left, an arrow, the typed query in a chip, and a short gray note on what the words are doing.

- **Title (bold 15px ink `#1a5276`, top center, y=24):** "One Person, Five Dialects".
- **Rows at y = 66, 112, 158, 204, 250** (row center). Each row:
  - Domain pill: rect x=30 w=110 h=22 centered on the row, tint fill + 2px border in the domain color, bold 11px domain-color label centered.
  - Arrow right (domain color, 2px, arrowhead) from x=146 to x=166.
  - Query chip: rect x=170 w=300 h=24 centered on the row, white fill, 1px `#ccc` border, bold 12px `#2c3e50` query text centered at x=320.
  - Note: 11px `#6b7280` left-aligned at x=486.
- **Row data (pill | query | note):** web search | "weather today" | a topic to read about; store search | "running shoes men size 10 under 100" | attributes as words; map search | "coffee near me" | a place + proximity; video search | "lofi study mix 1 hour" | mood + genre + duration; docs search | "reset api key" | task words, one page.
- **Store query text at 11px** (bold) so the long query fits the 300px chip; all other queries bold 12px.
- **Domain colors:** web blue `#2a78d6` (tint `rgba(42,120,214,0.12)`), store orange `#d95926` (tint `rgba(217,89,38,0.12)`), map green `#008300` (tint `rgba(0,131,0,0.10)`), video magenta `#d55181` (tint `rgba(213,81,129,0.12)`), docs violet `#4a3aa7` (tint `rgba(74,58,167,0.12)`).
- **Caption (11px `#6b7280`, centered, y=288):** "same person, same day — five boxes, five ways of typing".

## Measuring the Five Dialects

**Tags:** `worked example` (blue), `word counts` (orange)

- **Redo it** — count the words in each Saturday query, then average over a day of queries per box
- **Web ~2.4 words** — questions and topic names: "weather today", "how to poach eggs"
- **Store ~4.8 words** — brand + attributes + a price cap stacked into one line
- **Map ~2.6 words** — a place type plus a typed or implied "near me"
- **Video ~3.9 words** — title-ish phrases: genre, mood, a duration
- **Docs ~2.1 words** — task verb + object ("reset api key"); app-store boxes similar ("photo editor")

*Example (illustrative, italic):* "running shoes men size 10 under 100" runs to seven words because the shopper is filling in filters with text.

**Key point:** The averages are illustrative — the ordering is the lesson: boxes that must pin down one item get long queries; boxes that already have context get short ones.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: five bars, one per domain in its domain color, bar length = illustrative average words per query; a gray "made of" note under each domain label.

- **Title (bold 15px ink, top center, y=24):** "Average Words per Query (illustrative)".
- **Bars at row centers y = 70, 114, 158, 202, 246.** Bar track starts at x=180, scale 90px per word, height 20 centered on the row; tint fill + 2px border in the domain color; bold 13px domain-color value label ("2.4 words" etc.) just right of the bar end (+10px).
- **Left labels:** domain name bold 12px domain color, right-aligned at x=170, 5px above row center; "made of" note 11px `#6b7280`, right-aligned at x=170, 11px below row center.
- **Bar data (domain | value | made of):** web | 2.4 | questions + topics; store | 4.8 | brand + attributes + price; map | 2.6 | place + "near me"; video | 3.9 | title-ish phrases; docs | 2.1 | task words.
- **Baseline:** 1px `#999` vertical line at x=180 from y=52 to y=262; light gridlines (`#e5e9ef`, 1px) at 1..4 words (x = 180+90k) over the same span, 10px `#6b7280`... use 11px `#6b7280` tick labels "1", "2", "3", "4" at y=276 under the gridlines.
- **Annotation (bold 12px yellow `#c98500`, centered, y=292):** "the store box gets queries twice as long as the web box".

## Why One Ranker Does Not Travel

**Tags:** `where it's used` (blue), `common mistake` (red)

- **No transfer** — a ranker tuned for web pages fails when dropped into a store, map, or video box
- **Store needs** — parse size, gender, and a price cap out of plain words, then match structured fields
- **Map needs** — distance from the phone matters more than any word match
- **Video needs** — queries chase catalog titles, so title and duration fields dominate ranking
- **Synonym gap** — the catalog says "hooded sweatshirt", shoppers type "hoodie": zero word overlap
- **Local fixes** — each box needs its own synonym list, its own parser, its own signals

*Example (italic):* A web engine happily returns articles about hoodies; the store engine returns nothing until someone teaches it hoodie = hooded sweatshirt.

**Key point:** Even the vocabulary problem is domain-specific — the store's synonym gap lives between shopper words and catalog words, and no web ranker ships with that list.

### Visualization (canvas `c3`, 720×300)

Four rows: a query chip on the left, an arrow, and a box naming the machinery that domain's ranking needs; the last row is the "hoodie" synonym-gap failure in red.

- **Title (bold 15px ink, top center, y=24):** "Same Job — Ranking — Four Different Machines".
- **Rows at y = 72, 124, 176, 228** (row center). Each row:
  - Query chip: rect x=30 w=250 h=26 centered on the row, white fill, 2px border in the row color, bold 12px `#2c3e50` text centered at x=155 (the hoodie row's query at bold 12px, red border).
  - Arrow right (row color) from x=286 to x=306.
  - Machinery box: rect x=310 w=380 h=32 centered on the row, tint fill + 2px border in the row color; bold 12px row-color machinery name at x=322 (left-aligned), 11px `#6b7280` detail text right after it on the same baseline... put machinery name bold 12px row color at x=322 on the row center minus 6, and detail 11px `#6b7280` at x=322 on row center plus 9 (two lines inside the box).
- **Row data (query | color | machinery | detail):** "size 10 under 100" | orange store | attribute matching | parse size=10, price &lt; $100, match fields; "coffee near me" | green map | location math | distance from the phone, open now; "lofi study mix 1 hour" | magenta video | catalog-title match | title text + a duration field; "hoodie" | red `#e74c3c` | synonym gap | catalog says "hooded sweatshirt" — 0 word matches.
- **Red tint** for the failure row: `rgba(231,76,60,0.10)`.
- **Annotation (bold 12px red `#e74c3c`, centered, y=284):** 'the fix — hoodie = "hooded sweatshirt" — exists only inside this one store'.

## Short Queries Are Not Lazy Queries

**Tags:** `common mistake` (red), `rule of thumb` (orange)

- **Not laziness** — the same person types two words in one box and seven in another
- **Context fills gaps** — the map box already knows location and time, so two words suffice
- **The store knows nothing** — only typed attributes narrow a million products to one shoe
- **Read length as need** — query length tracks what the box must disambiguate, not user effort
- **Cross-domain traps** — comparing raw query lengths across boxes says nothing about engagement

*Example (italic):* The map searcher typing "coffee near me" is not lazier than the shopper typing seven words — the map already knows where they stand.

**Key point:** Query length measures the context the box is missing, not the effort of the person typing.

### Visualization (canvas `c4`, 720×300)

Two stacked horizontal bars showing the same total disambiguation job split differently: words the user types (colored) vs context the box already has (gray) — map vs store, labeled illustrative.

- **Title (bold 15px ink, top center, y=24):** "Who Supplies the Missing Pieces (illustrative)".
- **Two bar groups, row centers y = 92 and y = 192.** Each group:
  - Label above the bar (bold 12px domain color, left-aligned at x=40, 22px above row center): 'map box — "coffee near me"' (green) and 'store box — "running shoes men size 10 under 100"' (orange).
  - Bar: x=40, w=640, h=30 centered on the row; split into two segments summing to 640px representing 8 disambiguation pieces (80px per piece).
  - Map: user segment 2 pieces (160px) green tint `rgba(0,131,0,0.10)` + 2px green border, bold 12px green label "2 typed words" centered in it; box segment 6 pieces (480px) `#eef1f4` fill, 1px `#ccc` border, 11px `#6b7280` text centered: "box already knows: your location · the time · nearby shops".
  - Store: user segment 7 pieces (560px) orange tint `rgba(217,89,38,0.12)` + 2px orange border, bold 12px orange label "7 typed words: brand · gender · size · price cap" centered in it; box segment 1 piece (80px) `#eef1f4` fill, 1px `#ccc` border, 11px `#6b7280` centered "little context".
  - Sub-caption under each bar (11px `#6b7280`, left-aligned at x=40, 26px below row center): "short query, big context" and "long query, no context".
- **Annotation (bold 12px ink `#1a5276`, centered, y=258):** "the total job is the same — only who supplies the pieces changes".
- **Caption (11px `#6b7280`, centered, y=288):** "piece counts are illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` (no index number); subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Domain color mapping used on every chart: web = blue, store = orange, map = green, video = magenta, docs = violet; red `#e74c3c` only for the synonym-gap failure row in c3.
- **Data:** everything is hardcoded (no randomness): the five c1 queries, the c2 averages 2.4 / 4.8 / 2.6 / 3.9 / 2.1 (invented, labeled "illustrative"), the four c3 machinery rows, and the c4 2-vs-6 and 7-vs-1 piece splits (labeled "illustrative"). Text numbers match chart numbers (2.4/4.8/2.6/3.9/2.1 in section 2, 2 and 7 typed words in section 4).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
