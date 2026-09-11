# Intent Filters & Clarification Chips

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Intent Filters & Clarification Chips

**Subtitle:** The results page grows filters and intent cards out of your query — and every chip you click is really more query

**Grid-card description:** Type "red shoes" and size chips appear; type something broad and the page asks which aspect you meant — clicked chips are query words in disguise.

## The Query Decides Which Filters Appear

**Tags:** `core idea` (blue), `generated filters` (green)

- **Two searches** — "red shoes" gets size, brand, and price chips; "laptop" gets RAM, screen, and brand
- **Not a fixed menu** — the filter row is generated per query, from the intent the engine detected
- **Intent first** — the engine classifies the query as shoes-shopping, then picks shoe-shaped filters
- **Attributes follow** — each detected product type carries its own set of refinable attributes
- **A quiet hint** — the filter row itself tells you what the engine thinks you meant

*Example (italic):* Search "running shoes" and a size chip appears; search "shoe storage rack" and it doesn't — same word, different detected intent.

**Key point:** Filters are not furniture — they are generated from the query's detected intent, so the filter row changes when the query does.

### Visualization (canvas `c1`, 720×300)

Two stacked mini results-pages: the same store, two different queries, each search box followed by its own auto-generated chip row — showing the filter set changing with the query.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "Same Store, Two Queries, Two Filter Rows".
- **Row 1 search box (x=40, y=48, w=250, h=30, white fill, 2px ink border):** bold 12px `#2c3e50` left-aligned at x=52, mid-height: "red shoes".
- **Row 1 chips (y=92, h=24):** four chips left-aligned starting x=40, gap 12px, widths ~78/70/78/92: "size", "color", "brand", "price range". Chip style: fill `rgba(0,131,0,0.10)`, 1px green `#008300` border, bold 11px green centered text.
- **Row 1 caption (11px `#6b7280`, left-aligned x=40, y=136):** "detected intent: shoe shopping → shoe attributes".
- **Divider:** dashed `#bdc3c7` horizontal line at y=155, x=30 to x=690 (dash 4/3).
- **Row 2 search box (x=40, y=172, w=250, h=30, white fill, 2px ink border):** bold 12px `#2c3e50` left-aligned at x=52, mid-height: "laptop".
- **Row 2 chips (y=216, h=24):** four chips left-aligned starting x=40, gap 12px, widths ~66/96/70/92: "RAM", "screen size", "brand", "price range". Chip style: fill `rgba(42,120,214,0.10)`, 1px blue `#2a78d6` border, bold 11px blue centered text.
- **Row 2 caption (11px `#6b7280`, left-aligned x=40, y=260):** "detected intent: laptop shopping → laptop attributes".
- **Annotation (bold 12px violet `#4a3aa7`, right-aligned x=690, y=290):** "the filter row is generated from the query".

## A Chip Click Is Query Words in Disguise

**Tags:** `worked example` (blue), `equivalence` (green)

- **Path one** — type "red shoes", then click the size chip and pick 11
- **Path two** — type "red shoes size 11" as one query and press enter
- **Same request** — both paths land on the same structured query: text "red shoes", size = 11
- **One difference** — the chip travels as an exact constraint; the typed "size 11" is soft text match
- **Same log lesson** — either way, the system learns this shopper wants red shoes in size 11

*Example (italic):* "red shoes" + a size-11 filter click and the typed query "red shoes size 11" ask the store for the same thing.

**Key point:** Conceptually a clicked filter appends words to the query — the click just arrives pre-parsed, as a hard field constraint instead of loose text.

### Visualization (canvas `c2`, 720×300)

Two paths converging: the typed-then-clicked path on top and the all-typed path below, both arriving at the same structured query box on the right.

- **Title (bold 15px ink, top center, y=22):** "Two Roads to the Same Query".
- **Path 1 label (bold 12px green `#008300`, left-aligned x=30, y=58):** "type, then click a chip".
- **Path 1 boxes:** search box (x=30, y=70, w=160, h=28, white fill, 2px ink border) bold 12px `#2c3e50` centered "red shoes"; plus sign bold 14px `#6b7280` at x=205, y=88; chip (x=222, y=72, w=96, h=24, fill `rgba(0,131,0,0.10)`, 1px green border) bold 11px green centered "size: 11".
- **Path 1 arrow (2px green, small filled triangle head):** from (325, 84) to (455, 128).
- **Path 2 label (bold 12px blue `#2a78d6`, left-aligned x=30, y=196):** "type it all as one query".
- **Path 2 box:** search box (x=30, y=208, w=230, h=28, white fill, 2px ink border) bold 12px `#2c3e50` centered "red shoes size 11".
- **Path 2 arrow (2px blue, triangle head):** from (267, 222) to (455, 172).
- **Converged box (x=460, y=110, w=230, h=84, fill `rgba(74,58,167,0.10)`, 2px violet border):** header bold 12px violet centered at x=575, y=132: "structured query"; two 11.5px `#2c3e50` lines left-aligned at x=478, y=156/176: "text: red shoes", "size = 11 (exact)".
- **Footnote (11px `#6b7280`, left-aligned x=30, y=262):** "chip → exact field constraint · typed words → soft text match".
- **Annotation (bold 12px violet, centered, y=290):** "the chip is pre-parsed query words".

## When the Page Asks What You Meant

**Tags:** `clarification cards` (blue), `broad queries` (orange)

- **A broad query** — "90s music" on a video site could mean pop, rock, live shows, or what's hot now
- **The card** — instead of guessing silently, the page shows chips: pop hits, rock, live, trending today
- **One click** — picking "trending today" selects an intent branch, not just an attribute value
- **Filters vs cards** — filters narrow attributes of one intent; clarification cards choose between intents
- **Kin to related searches** — the machine proposes refinements; here it asks before ranking, not after

*Example (italic):* Search "90s music" and the video site answers with a question — four chips asking which 90s you meant.

**Key point:** Clarification cards are the engine admitting the query is too broad to rank — it asks you to pick the intent, and your click completes the query.

### Visualization (canvas `c3`, 720×300)

A broad query fanning into a clarification card of four aspect chips, one chip highlighted as clicked, leading to the refined result set for that branch.

- **Title (bold 15px ink, top center, y=22):** "A Broad Query Gets a Clarifying Question".
- **Query box (x=25, y=124, w=170, h=36, white fill, 2px ink border):** bold 13px `#2c3e50` centered "90s music".
- **Arrow (2px ink, triangle head):** from (200, 142) to (238, 142).
- **Clarification card (x=242, y=52, w=210, h=196, white fill, 1px `#ccc` border, subtle fill `#fafbfc`):** header bold 12px ink centered at x=347, y=76: "which 90s music?"; four chips (x=258, w=178, h=28) at y=94, 132, 170, 208: "90s pop hits", "90s rock", "live performances", "trending today". First three chips: fill `rgba(42,120,214,0.10)`, 1px blue border, bold 11px blue centered text. Fourth chip ("trending today"): fill `rgba(0,131,0,0.18)`, 2px green border, bold 11px green centered text — the clicked one.
- **Arrow from clicked chip (2px green, triangle head):** from (436, 222) to (490, 190).
- **Results box (x=494, y=120, w=200, h=110, white fill, 2px green border):** header bold 12px green centered at x=594, y=142: "90s music — trending"; three light-grey result bars (`#e5e9ef`, x=508, w=172, h=8) at y=158, 178, 198; caption 11px `#6b7280` centered at x=594, y=220: "ranked for that intent only".
- **Annotation (bold 12px orange `#d95926`, centered, y=284):** "the click picks an intent branch — and completes the query".

## Chips Write Cleaner Labels Than Typing

**Tags:** `why it matters` (blue), `training labels` (green), `common mistake` (red)

- **A typed rewrite** — "90s music trending" must be parsed; did "trending" mean the aspect or a song?
- **A chip click** — logs as query "90s music" + aspect "trending today": intent labeled by the user
- **Free training data** — every clicked card teaches the intent classifier what broad queries meant
- **Steering returns** — like autocomplete, users mostly pick from what's shown; unshown intents vanish
- **The mistake** — reading chip-built and hand-typed refinements as one population of "typed queries"

*Example (illustrative, italic):* A log where refinements arrive pre-labeled by chip clicks trains an intent classifier without a single human annotator.

**Key point:** Chips turn messy rewrites into labeled choices — cleaner training data, but only for the intents the card offered.

### Visualization (canvas `c4`, 720×300)

Left panel: two log rows comparing the typed rewrite (raw string, needs parsing) with the chip click (pre-labeled fields). Right panel: the steering caveat — four offered chips absorb nearly all clicks while unshown intents get almost none (illustrative bars).

- **Title (bold 15px ink, top center, y=22):** "What the Log Sees, and What Gets Chosen".
- **Divider:** dashed `#bdc3c7` vertical line at x=360, y=40 to y=272 (dash 4/3).
- **Left header (bold 12px ink, centered at x=185, y=50):** "two refinements in the log".
- **Typed row box (x=30, y=68, w=310, h=58, white fill, 1px `#ccc` border):** chip (x=40, y=76, w=54, h=17, fill `rgba(42,120,214,0.12)`, 1px blue border, bold 10.5px blue centered) "typed"; 11.5px `#2c3e50` at x=40, y=112: `"90s music trending"`; 11px `#6b7280` right-aligned at x=330, y=112: "raw string — parse it".
- **Chip row box (x=30, y=142, w=310, h=74, fill `rgba(0,131,0,0.07)`, 1px green border):** chip (x=40, y=150, w=54, h=17, fill `rgba(0,131,0,0.12)`, 1px green border, bold 10.5px green centered) "chip"; two 11.5px `#2c3e50` lines at x=40, y=186/204: `query: "90s music"`, `aspect: trending today`; 11px `#6b7280` right-aligned at x=330, y=195: "already labeled".
- **Left caption (11px `#6b7280`, centered x=185, y=244):** "the chip click arrives pre-parsed".
- **Right header (bold 12px ink, centered at x=540, y=50):** "clicks per intent (illustrative)".
- **Right bars:** baseline 1px `#999` at y=232 from x=400 to x=690. Five bars (w=44, gap 14) starting x=408: offered chips "pop" 62, "rock" 48, "live" 30, "trending" 74 (heights scaled ~1.6px per unit: 99/77/48/118) in green tint fill `rgba(0,131,0,0.35)` with 2px green border; unshown intent "9. others" 6 (height 10) in grey `#e5e9ef` with 1px `#999` border. Bold 12px labels above bars (green for offered, `#6b7280` for unshown): "62", "48", "30", "74", "6". 11px `#444` labels below baseline at y=250, centered per bar: "pop", "rock", "live", "trending", "unshown".
- **Annotation (bold 12px yellow `#c98500`, centered at x=540, y=288):** "users pick from what's offered".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` (no index number); subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared helper `arrow(x1,y1,x2,y2,color)` draws a 2px line with a small filled triangle head.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. No red in charts.
- **Data:** everything hardcoded (no randomness). The c4 click counts (62/48/30/74 offered, 6 unshown) are invented and labeled "illustrative". The chip sets in c1 (size/color/brand/price vs RAM/screen size/brand/price range) match section 1's bullets; the two c2 paths match section 2's path-one/path-two bullets; the four c3 aspect chips match section 3's card bullet.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
