# The Query Log as a Dataset

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Query Log as a Dataset

**Subtitle:** Every query lands with a time and a place — a demand stream you can forecast and act on before the searches arrive

**Grid-card description:** Every query carries a time and a place — a demand stream with forecastable surges, local pockets, and searches for things you don't have yet.

## A Log Row Is a Demand Record

**Tags:** `core idea` (blue), `demand stream` (green)

- **The row** — every search logs what was asked, when, from roughly where, and on what device
- **Not just input** — beyond serving results, the accumulated log is a dataset in its own right
- **What** — the query text says what was wanted, in the searcher's own words
- **When** — the timestamp turns the log into a time series of demand, minute by minute
- **Where** — coarse location splits that demand by city, region, and language

*Example (italic):* One log row — "umbrella", 7:42 am, Rain City, phone — is a tiny measurement of demand; a billion rows are a demand survey nobody had to run.

**Key point:** The query log is a continuous measurement of what people want, when, and where — a dataset that exists as a side effect of search.

### Visualization (canvas `c1`, 720×300)

A mini log table on the left, with three colored arrows pulling out its three analytical dimensions — what, when, where — each landing on a labeled dimension card.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "One Log, Three Dimensions".
- **Log table (x=25, y=52, w=330, h=196, white fill, 1px `#ccc` border):** header row (fill `#f0f3f7`, h=24) with bold 11px ink column labels at y=68: "query" x=40, "time" x=170, "city" x=255; header underline 1px `#ccc` at y=76. Four data rows, 11px `#2c3e50`, baselines y=98/128/158/188: ("umbrella", "7:42", "Rain City"), ("snow chains", "8:05", "Hill Town"), ("cup final score", "8:06", "Metro City"), ("umbrella", "8:11", "Rain City"). Caption 11px `#6b7280` centered at x=190, y=224: "…a billion rows a day (illustrative)".
- **Three dimension cards (x=470, w=225, h=54) at y=58, 124, 190:** each with tint fill, 2px colored border, bold 12px colored header (left-aligned x=482, first line) and 11px `#2c3e50` second line at x=482. Card 1 blue `#2a78d6` / `rgba(42,120,214,0.10)`: "WHAT — the words" / "demand in searchers' own terms". Card 2 green `#008300` / `rgba(0,131,0,0.08)`: "WHEN — the timestamp" / "a minute-by-minute time series". Card 3 orange `#d95926` / `rgba(217,89,38,0.10)`: "WHERE — the place" / "demand split by city and language".
- **Arrows (2px, colored per card, small filled triangle heads):** from the table's right edge (355) at y=100/140/180 to each card's left edge (470) at card mid-height.
- **Annotation (bold 12px violet `#4a3aa7`, centered, y=284):** "a demand survey that runs itself".

## Surges You Can See Coming

**Tags:** `worked example` (blue), `prediction` (green)

- **Datable events** — a phone launch or a cup final has a known date; its queries do too
- **The ramp** — weeks before launch, guesses at the new model's names start climbing together
- **Every variant** — "phone 12", "phone 12 pro", "phone 12 release date" all surge as one family
- **The spike** — on launch day the family jumps roughly tenfold, then decays over the next weeks
- **The final whistle** — "cup final score" goes near-zero to peak within minutes of full time

*Example (illustrative, italic):* Two weeks out, the name-variant family is at index 20; launch day hits 100 — a forecast any analyst could draw by hand.

**Key point:** Query demand around scheduled events is a forecasting problem with the answer half-known — the date is public, only the height is uncertain.

### Visualization (canvas `c2`, 720×300)

A weekly time-series chart: three name-variant lines ramping toward a launch-day spike and decaying after, with the launch week marked by a dashed vertical line.

- **Title (bold 15px ink, top center, y=22):** "The Launch Surge, Week by Week (illustrative)".
- **Axes:** 1px `#999` — y-axis at x=70 from y=45 to y=245; x-axis at y=245 from x=70 to x=690. Y-label 11px `#6b7280` rotated or horizontal at top-left (x=25, y=52): "volume index". Y-ticks 11px `#6b7280` right-aligned at x=62 for 0, 50, 100 (y=245, 145, 45); light grid lines `#e5e9ef` at those heights from x=70 to x=690.
- **X-ticks (11px `#444`, centered under axis at y=264):** 8 weekly points evenly spaced x=110..650 (step 77): "-5w", "-4w", "-3w", "-2w", "-1w", "launch", "+1w", "+2w".
- **Launch marker:** dashed 1.5px `#c98500` vertical line at the "launch" x (x=495) from y=45 to y=245 (dash 4/3).
- **Three lines (2.5px, round joins), values as index 0–100 mapped to the y-scale:**
  - blue `#2a78d6` "phone 12": 8, 12, 18, 26, 42, 100, 70, 45
  - green `#008300` "phone 12 pro": 4, 6, 10, 16, 30, 80, 60, 38
  - violet `#4a3aa7` "phone 12 release date": 12, 18, 26, 34, 48, 60, 12, 6
- **Line labels (bold 11px, matching colors):** near each line's right end: "phone 12" at (655, y of 45), "phone 12 pro" at (655, y of 38, offset to avoid overlap), "release date" at (655, y of 6).
- **Callout (bold 12px yellow `#c98500`, left-aligned near the marker, x=505, y=60):** "date known in advance".
- **Annotation (bold 12px green, centered, y=292):** "the whole name family surges together — forecast it as one".

## Queries Have a Home Address

**Tags:** `locality` (blue), `demographics` (orange)

- **Local demand** — "snow chains" lives in hill towns; "monsoon umbrella" lives on the wet coast
- **Local meaning** — the same word can point at different things in different places
- **Segments** — age, language, and profession shape which queries a group ever types
- **Serving locally** — the same query can deserve different results in different cities
- **Weighting warning** — a "national top queries" list is mostly a census of the biggest cities

*Example (italic):* "football" means one sport to a searcher in Rio and another in Dallas — the log row's location disambiguates it.

**Key point:** Location and demographics are first-class columns of the query dataset — demand is not one national curve but a stack of local ones.

### Visualization (canvas `c3`, 720×300)

A grouped bar chart: three queries on the x-axis, each with three city bars, showing how sharply each query concentrates in one place.

- **Title (bold 15px ink, top center, y=22):** "Where Each Query Lives (illustrative volume index)".
- **Legend (y=46, centered row starting x=190):** three swatches (12×12) with 11px `#2c3e50` labels, gap ~60px: "Hill Town" blue `#2a78d6`, "Rain City" aqua `#199e70`, "Metro City" orange `#d95926`.
- **Axes:** 1px `#999` x-axis at y=240 from x=60 to x=690.
- **Three groups centered at x=170, 375, 580; bars w=42, gap 8 within a group; scale 1.7px per unit.** Values (Hill Town, Rain City, Metro City):
  - "snow chains": 90, 6, 10
  - "monsoon umbrella": 8, 84, 22
  - "subway delays": 3, 14, 88
- **Bar style:** tint fills (`rgba(42,120,214,0.55)`, `rgba(25,158,112,0.55)`, `rgba(217,89,38,0.55)`) with 2px solid borders in the full colors.
- **Value labels:** bold 11px in each bar's full color, centered above each bar.
- **Group labels (bold 12px `#2c3e50`, centered under each group at y=260):** "snow chains", "monsoon umbrella", "subway delays".
- **Annotation (bold 12px orange `#d95926`, centered, y=290):** "each query concentrates where its life happens".

## Acting Before the Searches Arrive

**Tags:** `where it's used` (blue), `operations` (green), `missing inventory` (red)

- **Serve** — precompute and cache the predicted surge family so launch-day results are instant
- **Freshness** — during a live event, ranking flips from evergreen pages to minutes-old ones
- **Collect** — crawl, stock, or license inventory ahead of demand the forecast says is coming
- **The gap** — zero-result and low-click queries are demand for things not in the inventory
- **Closing the loop** — mined gaps become next quarter's catalog additions and new content

*Example (illustrative, italic):* A store sees 4,000 searches for a gadget it doesn't carry — the query log just wrote its purchasing memo.

**Key point:** Forecasting the query stream tells you what to cache, what to rank fresh, what to stock — and the searches you can't answer are the sharpest signal of all.

### Visualization (canvas `c4`, 720×300)

Three action lanes fed by a forecast box: serve (cache ahead), collect (stock ahead), and the gap lane where zero-result queries are mined into an inventory wishlist.

- **Title (bold 15px ink, top center, y=22):** "Three Things to Do With a Forecast".
- **Forecast box (x=25, y=112, w=140, h=76, fill `rgba(74,58,167,0.10)`, 2px violet `#4a3aa7` border):** bold 12px violet centered at x=95, two lines: "predicted" (y=142), "query demand" (y=160).
- **Three lanes (x=250, w=445, h=72) at y=44, 118, 192:** each a rounded-feel rect (plain rect fine) with tint fill and 2px colored border; lane header bold 12px colored at (x=264, y = lane top + 22); one 11.5px `#2c3e50` line at (x=264, y = lane top + 44); one 11px `#6b7280` line at (x=264, y = lane top + 60).
  - Lane 1 green `#008300` / `rgba(0,131,0,0.08)`: "SERVE — cache ahead" / "precompute results for the surge family" / "launch-day answers come from cache".
  - Lane 2 blue `#2a78d6` / `rgba(42,120,214,0.08)`: "COLLECT — stock ahead" / "crawl, stock, license before demand lands" / "inventory arrives before the searches".
  - Lane 3 magenta `#d55181` / `rgba(213,81,129,0.08)`: "MINE THE GAP — missing inventory" / "zero-result queries = unmet demand" / "4,000 searches, nothing to show (illustrative)".
- **Arrows (2px, colored per lane, small filled triangle heads):** from the forecast box's right edge (165) at y=132/150/168 fanning to each lane's left edge (250) at lane mid-height (y=80/154/228).
- **Annotation (bold 12px magenta `#d55181`, centered, y=288):** "what people search and don't find is a shopping list".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` (no index number); subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared helper `arrow(x1,y1,x2,y2,color)` draws a 2px line with a small filled triangle head.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. No red in charts.
- **Data:** everything hardcoded (no randomness), all invented numbers labeled "illustrative". The c2 weekly series (three arrays of 8 values listed above) matches section 2's index-20-to-100 framing; the c3 grouped-bar values (90/6/10, 8/84/22, 3/14/88) match section 3's local-concentration bullets; the c4 "4,000 searches" figure matches section 4's example line. Generic names only — "phone 12" as a deliberately generic model family, fictional city names (Hill Town, Rain City, Metro City).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
