# Data Warehouse vs Data Lake

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks, each h2 + two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Data Warehouse vs Data Lake

**Subtitle:** The warehouse is the library — clean, labeled, shelved. The lake is the attic — everything you ever kept, in the box it arrived in

## The Library and the Attic

**Tags:** `core idea` (blue), `running example` (green)

- **The warehouse** — curated monthly sales tables: typed columns, named, modeled, documented
- **The lake** — the landing zone: raw JSON events, CSV exports, app logs, dumped as-is
- **Library** — a librarian checked every book in; you find "March revenue" in one minute
- **Attic** — nothing was thrown away; the answer is up there, in some box, probably
- **Both exist on purpose** — curation costs effort; keeping everything costs searching

*Example (italic):* Finance asks "March revenue by region" — the librarian hands over one shelf; the attic hands over forty boxes.

**Key point:** The warehouse stores answers you prepared in advance. The lake stores raw material for questions nobody has asked yet.

### Visualization (canvas `c1`, 720×300)

Side-by-side split-panel diagram: warehouse shelves (left) vs lake attic boxes (right).

- **Title (bold 15px, `#1a5276`, top center):** "Curated Shelves vs Boxes As They Arrived"
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3, width 1) at x=360 from y=38 to y=288.
- **Left panel** — heading "WAREHOUSE — the library" in bold 13px blue `#2a78d6` centered at (180, 54). Four stacked shelf boxes at x=55, width 250, height 30, starting y=70 with 40px spacing; fill `rgba(42,120,214,0.12)`, stroke `#2a78d6` width 1.5. Monospace 12px labels inside each: `sales_2026_01   (typed, modeled)`, `sales_2026_02   (typed, modeled)`, `sales_2026_03   (typed, modeled)`, `customers        (typed, modeled)`. Below in bold 12px blue, two centered lines: "every table named, columns typed," / "a librarian checked it in".
- **Right panel** — heading "LAKE — the attic" in bold 13px orange `#d95926` centered at (545, 54). Six scattered file boxes (height 28, fill `rgba(217,89,38,0.10)`, stroke `#d95926`) with monospace 11px labels: `orders_raw.json` (400,76,w128), `clicks_v2.csv` (560,90,w118), `app_2026.log` (425,128,w110), `export_final2.csv` (565,146,w120), `events_backup.json` (405,182,w140), `misc_dump/` (572,200,w100). Below in bold 12px orange, two centered lines: "nothing thrown away, nothing labeled —" / "stored in the box it arrived in".
- **Bottom captions** in bold 13px violet `#4a3aa7`: "find it in a minute" at (180, 288); "it's up here somewhere" at (545, 288).

## One Sale, Two Homes: 40 Raw Fields Become 8 Clean Columns

**Tags:** `worked example` (green), `core idea` (blue)

- **The event** — the shop app fires a JSON blob per sale: ~40 fields, device info, debug junk
- **Into the lake** — 2M events land daily, untouched: about 6 GB of raw JSON per day
- **The nightly job** — picks 8 fields, checks types, fixes dates, drops the junk
- **Into the warehouse** — 2M tidy rows/day in monthly_sales: about 0.3 GB, 20x smaller
- **Typed for real** — amount is a decimal, order_date is a date — not strings that look like them

*Example (italic):* The raw event says "amount": "49.99", "currency": "EUR" plus 38 other fields; the warehouse row keeps amount_usd = 54.10 and 7 friends.

**Key point:** The same sale exists twice: once as evidence (lake), once as an answer (warehouse). The nightly job is the librarian.

### Visualization (canvas `c2`, 720×300)

Left-to-right flow diagram: app → lake (raw JSON) → nightly job → warehouse (typed row).

- **Title (bold 15px, `#1a5276`, top center):** "The Daily Journey of 2M Sales: 6 GB Raw In, 0.3 GB Clean Out"
- **App box** at (30,110), 100×60, fill `#f8f9fa`, stroke `#6b7280`; bold 12px "shop app", 11px muted "2M sales/day".
- **Lake box** at (180,70), 200×150, fill `rgba(217,89,38,0.08)`, stroke `#d95926`; heading "LAKE — raw JSON" bold 13px orange; monospace 11px sample lines: `{ "amount": "49.99",` / `  "currency": "EUR",` / `  "device": "iOS 19",` / `  "dbg_flag": true,` / `  ...36 more fields }`; below, bold 12px orange "~6 GB / day".
- **Nightly job box** at (420,105), 110×70, fill `#f3f0fb`, stroke `#4a3aa7`; bold 12px violet "nightly job", 11px text "pick 8 fields," / "check types".
- **Warehouse box** at (570,70), 130×150, fill `rgba(42,120,214,0.08)`, stroke `#2a78d6`; heading "WAREHOUSE" bold 13px blue; monospace 11px column lines: `order_id  int`, `order_date date`, `amount_usd dec`, `region    text`, `+ 4 more columns`; below, bold 12px blue "~0.3 GB / day".
- **Arrows** (width 2, filled arrowheads): app→lake in orange, lake→job in violet, job→warehouse in blue, all at y=140.
- **Labels:** 11px muted "as-is" at (153,128); bold 12px violet "40 fields → 8 columns" at (475,250).
- **Takeaway (bold 13px green `#008300`, centered at y=282):** "same sale, stored twice: raw evidence in the lake, a 20x smaller typed answer in the warehouse"

## Schema-on-Write vs Schema-on-Read: When the Bad Row Explodes

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **Schema-on-write** — the warehouse checks every row at load time; wrong shape = rejected
- **Schema-on-read** — the lake accepts any bytes; the shape is only checked when queried
- **Bad row, warehouse** — "amount": "N/A" fails the decimal check on day 1, loudly
- **Bad row, lake** — stored without complaint; your query crashes on it on day 90
- **Why you care** — features built from the lake inherit every quirk the lake never checked

*Example (italic):* The model training job that "randomly" fails each March is choking on last March's malformed rows — stored 11 months ago, checked today.

**Key point:** Both check the schema — the only question is when. Write-time errors are cheap and early; read-time errors are expensive and land on whoever queries.

### Visualization (canvas `c3`, 720×300)

Two-lane timeline (day 0 to day ~100) showing the same bad row hitting each system.

- **Title (bold 15px, `#1a5276`, top center):** 'The Row  "amount": "N/A"  Meets Both Systems'
- **Lanes:** horizontal grid lines (`#e5e9ef`) from x=175 to x=660; WAREHOUSE lane at y=100, LAKE lane at y=195. Lane labels right-aligned at x=160: "WAREHOUSE" bold 13px blue with 11px muted "schema-on-write" beneath; "LAKE" bold 13px orange with 11px muted "schema-on-read" beneath.
- **Time axis:** vertical gridlines and 12px muted labels "day 0", "day 30", "day 60", "day 90" at y=240; x mapped linearly day 0–100 across 175–660.
- **Warehouse lane:** red `#e74c3c` filled circle (r=8) at day 1 with white "✗" inside; bold 12px red two-line annotation: "day 1: load fails the decimal check —" / "loud, early, cheap to fix"; 12px green `#008300` line: "table stays clean for the next 99 days".
- **Lake lane:** orange filled circle (r=7) at day 1, bold 12px orange "day 1: stored, no complaint"; dormant band `rgba(201,133,0,0.12)` from day 2 to day 88 (12px tall) with 12px yellow `#c98500` label "sits quietly for 89 days"; red circle (r=8) with white "✗" at day 90, bold 12px red right-aligned: "day 90: an analyst's query crashes on it".
- **Takeaway (bold 14px violet `#4a3aa7`, centered at y=282):** "both check the schema — the lake just bills the reader instead of the writer"

## Each One Fails When Used as the Other

**Tags:** `common mistake` (red), `trade-off` (orange)

- **Lake as warehouse** — daily finance report off raw JSON: ~4 min per query, numbers drift
- **Warehouse as lake** — landing a brand-new feed needs modeling first: ~15 workdays of waiting
- **The right tool** — the same revenue query runs in ~3 s on the curated table
- **The right tool, reversed** — the lake takes a never-seen-before feed the same day
- **Rule of thumb** — repeated question, warehouse; brand-new data or one-off dig, lake

*Example (italic):* "Why does the daily dashboard take 40 minutes and disagree with itself?" — it was pointed at the attic, not the library.

**Key point (labeled "Common mistake:"):** Treating them as rivals and picking one. They fail in opposite directions — most teams need the attic feeding the library.

### Visualization (canvas `c4`, 720×300)

Two-panel bar comparison split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Opposite Strengths: Answering Fast vs Accepting Fast"
- **Left panel** — subtitle bold 13px ink '"monthly revenue by region"' at (180,56), 12px muted "query time (log scale)" at (180,74). Two bars 70px wide on a log10 scale (1s–1000s mapped over 140px, baseline y=240, axis line `#999` from x=50 to x=320): warehouse bar at x=100, value 3 s, fill blue `#2a78d6`, bold 13px value label "3 s" above, 12px label "warehouse" below; lake bar at x=240, value 240 s, fill orange `#d95926`, label "240 s", "lake (raw JSON)". Bold 12px blue caption at (180,280): "80x faster on the curated table".
- **Right panel** — subtitle bold 13px ink "landing a brand-new data feed" at (540,56), 12px muted "time until the data is stored (workdays)" at (540,74). Two bars, linear scale max 16 days over 140px (baseline y=240, axis from x=410 to x=690): lake bar at x=465, value 1, orange, label "same day"; warehouse bar at x=615, value 15, blue, label "15 days". Bold 12px orange caption at (540,280): "modeling first vs dump now, model later".
- **Both panels:** 11px muted note "illustrative timings" at y=96 (x=180 and x=540).

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference style). h1 + `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (`width:100%`, border-collapse collapse) with one row: `td.text-col` (50%) and `td.viz-col` (50%), both `vertical-align: top`, padding 12px.
- **Text column structure:** `.tags` row of pill spans first (`.tag` — inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.tag.blue` bg `rgba(26,82,118,0.12)` color `#1a5276`; `.tag.green` bg `rgba(39,174,96,0.15)` color `#27ae60`; `.tag.red` bg `rgba(231,76,60,0.12)` color `#e74c3c`; `.tag.orange` bg `rgba(230,126,34,0.15)` color `#e67e22`), then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`, one italic `.example` paragraph (`#555`, 0.9rem), and one `.key-point` callout (bg `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>` lead-in.
- **Page CSS:** global reset `* { margin:0; padding:0; box-sizing:border-box }`; body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases styled `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each canvas declares `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data is hardcoded literal arrays (no `Math.random()`); invented numbers carry an "illustrative" label. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none — it is a leaf tutorial page).
