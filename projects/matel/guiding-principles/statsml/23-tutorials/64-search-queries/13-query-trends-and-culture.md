# Query Trends & Culture

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Query Trends & Culture

**Subtitle:** Rising and falling query volume traces seasons, events, and places — what a city searches most says what it cares about

## Counting a Query Instead of Reading It

**Tags:** `core idea` (blue), `volume over time` (green)

- **One at a time** — a single "flu symptoms" query tells you one person felt ill one morning
- **Counted together** — add up how often it was typed each month and a shape appears
- **Winter mountain** — "flu symptoms" volume climbs every autumn and peaks in January
- **Summer mountain** — "sunscreen" draws the opposite hill, peaking in July
- **Population thermometer** — summed queries trace what a population is thinking about, and when

*Example (italic):* The same log that looks like noise row by row draws a clean yearly mountain once it is counted by month.

**Key point:** Stop reading queries one at a time and start counting them — volume over time is a signal no single query contains.

### Visualization (canvas `c1`, 720×300)

Left panel: a column of near-identical raw query-log rows (each row says almost nothing). Right panel: the same query counted per month as 12 bars forming the winter shape — high at both ends of the year, quiet in summer.

- **Title (bold 15px ink `#1a5276`, top center, y=22):** "From Reading Queries to Counting Them".
- **Divider:** dashed `#bdc3c7` vertical line at x=300, y=35 to y=280 (dash 4/3).
- **Left panel (centered x=160):** subtitle bold 12px ink at y=48: "one query at a time". Seven log rows, 12px `#6b7280`, left-aligned at x=38, y = 72 to y=216 step 24: `Jan 03 09:14 · "flu symptoms"`, then 09:15, 09:17, 09:22, 09:31, 09:38, 09:44. Ellipsis row bold 12px `#6b7280` at y=242: "⋮  millions more rows". Caption 11px `#6b7280` centered at x=160, y=272: "each row says almost nothing".
- **Right panel:** subtitle bold 12px ink centered at x=510, y=48: "\"flu symptoms\" · monthly count (illustrative)". Baseline y=250 (1px `#999`, x=335 to x=705). Twelve bars w=26, gap 4 (x = 340 + i*30), heights = value × 1.7 px; fill `rgba(42,120,214,0.25)`, 1.5px blue `#2a78d6` border. Values (Jan–Dec): 70, 55, 35, 20, 12, 8, 7, 8, 14, 28, 45, 65. Peak label bold 11px blue "70" above the Jan bar. Month initials 11px `#6b7280` under each bar at y=266 (J F M A M J J A S O N D).
- **Annotation (bold 12px blue, centered x=520, y=80):** "high in winter, quiet in summer".

## Twelve Months, Two Mountains, One Festival Spike

**Tags:** `worked example` (blue), `two seasons` (green), `festival spike` (orange)

- **Two queries, one scale** — monthly volume indexed 0–100 for a year (numbers illustrative)
- **Flu row (Jan–Dec)** — 70 55 35 20 12 8 7 8 14 28 45 65
- **Sunscreen row (Jan–Dec)** — 8 10 15 25 45 68 75 70 40 20 10 8
- **Peaks** — flu tops in January at 70; sunscreen in July at 75
- **Crossings** — the lines swap leaders between Mar–Apr (≈23) and Sep–Oct (≈25)
- **Festival spike** — a dish query climbs 14 → 38 → 85 in the 3 weeks before a local festival

*Example (italic):* Cover the labels and you can still tell the two queries apart — one mountain is winter, the other summer.

**Key point:** Seasons can be read off raw counts alone: peaks, valleys, and the two months where the lines cross.

### Visualization (canvas `c2`, 720×300)

Left panel: dual line chart of the two monthly series with peak labels and the two crossing points circled. Right panel: eight weekly bars of a dish-name query in one region, peaking the week before a local festival.

- **Title (bold 15px ink, top center, y=22):** "Two Mountains and a Festival Spike (illustrative)".
- **Divider:** dashed `#bdc3c7` vertical line at x=470, y=35 to y=285.
- **Left panel axes:** baseline y=250 (1px `#999`, x=50 to x=445); gridlines at 25/50/75/100 in `#e5e9ef` with 11px `#6b7280` labels right-aligned at x=44. y = 250 − value × 1.8. Month points at x = 62 + i*34; month initials 11px `#6b7280` at y=266.
- **Legend (y=48):** 10×10 swatches + 11px labels at x=60: blue `#2a78d6` "flu symptoms", at x=170: yellow `#c98500` "sunscreen".
- **Lines:** flu blue 2.5px with r=3 dots, values 70 55 35 20 12 8 7 8 14 28 45 65; sunscreen yellow 2.5px with r=3 dots, values 8 10 15 25 45 68 75 70 40 20 10 8.
- **Peak labels:** bold 12px blue "Jan 70" near the first flu point; bold 12px yellow "Jul 75" above the July sunscreen point.
- **Crossing markers:** 5px-radius ink circles (2px stroke, no fill) at (x≈157, value≈23) between Mar–Apr and (x≈360, value≈25) between Sep–Oct; 11px ink labels "≈23" and "≈25" beside them.
- **Left caption (11px `#6b7280`, centered x=250, y=290):** "lines swap leaders in spring and autumn".
- **Right panel:** subtitle bold 12px ink centered at x=595, y=48: "dish query, one region (weekly)". Baseline y=250 (1px `#999`, x=485 to x=708). Eight bars w=22, gap 5 (x = 490 + i*27), heights = value × 1.9; values 5, 6, 5, 9, 14, 38, 85, 30 for weeks 7,6,5,4,3,2,1 before the festival plus the festival week; fill `rgba(213,81,129,0.25)`, 1.5px magenta `#d55181` border; festival-week bar gets a violet `#4a3aa7` border instead. Week labels 11px `#6b7280` at y=266: 7 6 5 4 3 2 1 F, with "F = festival wk" 11px `#6b7280` centered at x=595, y=280.
- **Peak annotation (bold 12px magenta, centered x=590, y=76):** "peak 1 wk before". Caption 11px `#6b7280` centered x=595, y=294: "people search before they cook".

## What a City Searches Is What It Cares About

**Tags:** `where it's used` (blue), `rule of thumb` (orange)

- **Cultural signature** — the top queries of a place list its dishes, sports, teams, and festivals
- **City A vs City B** — hockey and snow lead one list; cricket and monsoon lead the other
- **Interest first** — people search before they buy or visit; volume moves ahead of official numbers
- **Early demand read** — analysts watch query trends for a lead on sales and clinic-visit counts
- **Own history only** — compare a query to its own past, not raw counts across different-size cities

*Example (illustrative, italic):* Ten thousand searches means "big city" or "obsessed small town" — you cannot tell which without that city's own baseline.

**Key point:** Query trends are a cultural map and an early indicator — but only read relative to each place's own history.

### Visualization (canvas `c3`, 720×300)

Left panel: two city cards with their top-query lists side by side (illustrative cultural signatures). Right panel: a two-line lead-lag chart where search volume peaks about two weeks before official store sales.

- **Title (bold 15px ink, top center, y=22):** "A City's Signature — and Interest Before the Numbers (illustrative)".
- **Divider:** dashed `#bdc3c7` vertical line at x=350, y=35 to y=285.
- **City cards (y=55, w=145, h=168, white fill, 1px `#ccc` border):** City A at x=25, City B at x=185. Header band (h=26) tint `rgba(42,120,214,0.12)` for A / `rgba(25,158,112,0.12)` for B, bold 12px header text ("City A top queries" blue `#2a78d6`, "City B top queries" aqua `#199e70`). Four rows each, 12px `#2c3e50`, y offsets 48/78/108/138 inside the card: City A: "1. hockey scores", "2. poutine recipe", "3. snow forecast", "4. ice fishing spots"; City B: "1. cricket scores", "2. biryani recipe", "3. monsoon forecast", "4. kite festival dates".
- **Left caption (11px `#6b7280`, centered x=177, y=272):** "top queries form a cultural signature".
- **Right panel:** legend at y=48 (10×10 swatches + 11px labels): green `#008300` "searches" at x=390, violet `#4a3aa7` "store sales (official)" at x=490. Baseline y=250 (1px `#999`, x=375 to x=700); 10 weekly points at x = 382 + i*34; y = 250 − value × 1.9. Searches green 2.5px line with dots: 10, 12, 18, 30, 55, 80, 70, 50, 35, 25. Sales violet 2.5px line with dots: 8, 8, 10, 14, 24, 45, 70, 78, 60, 40. Week numbers 1–10, 11px `#6b7280`, under every other point at y=266.
- **Lead annotation:** horizontal ink arrow at y=86 from the search peak (x≈552) to the sales peak (x≈620); bold 12px ink "≈2 wks ahead" centered above it at y=76.
- **Right caption (11px `#6b7280`, centered x=537, y=290):** "interest first, purchases later".

## A Spike in Searches Is Not a Spike in Events

**Tags:** `common mistake` (red), `ground truth` (green)

- **Attention, not events** — a rising query counts people thinking about a thing, not the thing itself
- **News can do it alone** — one national story can multiply a query while nothing changes on the ground
- **Illustrative pair** — searches jump 12 → 88 the week a story airs; clinic visits stay near 10
- **Ground-truth check** — before reading a trend as a measurement, compare it with a real-world count

*Example (illustrative, italic):* A famous person's diagnosis can spike searches for a disease whose case counts never moved.

**Key point:** A search trend measures curiosity; treat it as a measurement of the world only after a ground-truth check.

### Visualization (canvas `c4`, 720×300)

Full-width two-line chart over 12 weeks: searches for a disease spike sevenfold the week a news story airs while weekly clinic visits stay flat; a dashed marker pins the story's air date.

- **Title (bold 15px ink, top center, y=22):** "A Spike in Attention, a Flat Line on the Ground (illustrative)".
- **Axes:** baseline y=250 (1px `#999`, x=60 to x=690); y = 250 − value × 1.8; 12 weekly points at x = 75 + i*54; week numbers 1–12, 11px `#6b7280`, at y=266.
- **Legend (y=48, swatches + 11px labels):** orange `#d95926` "searches for the disease" at x=75, aqua `#199e70` "weekly clinic visits" at x=250.
- **Lines:** searches orange 2.5px with r=3 dots: 10, 11, 10, 12, 11, 12, 88, 60, 30, 18, 13, 12. Clinic visits aqua 2.5px with r=3 dots: 10, 10, 11, 10, 11, 10, 11, 10, 10, 11, 10, 10.
- **Story marker:** dashed violet `#4a3aa7` vertical line at x=399 (week 7), y=70 to y=250; bold 12px violet "news story airs" left-aligned at x=407, y=84.
- **Flat-line annotation (bold 12px aqua, centered):** "clinic visits: no change" at x=600, y=190 (clear of both lines).
- **Caption (11px `#6b7280`, centered, y=292):** "searches ×7, events unchanged — attention is not the event".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` (no index number); subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). A shared `line(ctx, pts)` helper strokes a polyline and draws r=3 dots.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Series colors: flu = blue, sunscreen = yellow, festival dish = magenta (festival week bordered violet), searches-lead = green, official sales = violet, news-spike searches = orange, clinic visits = aqua. No red anywhere (no error state drawn).
- **Data:** everything is hardcoded (no randomness) and labeled "illustrative" where numeric: the flu series 70 55 35 20 12 8 7 8 14 28 45 65 (shared by c1 bars and c2 line), the sunscreen series 8 10 15 25 45 68 75 70 40 20 10 8, crossing values ≈23 and ≈25, the festival bars 5 6 5 9 14 38 85 30, the lead-lag pair (searches 10 12 18 30 55 80 70 50 35 25 / sales 8 8 10 14 24 45 70 78 60 40), and the news-spike pair (searches 10 11 10 12 11 12 88 60 30 18 13 12 / visits flat near 10). Text numbers match chart numbers (rows in section 2; 14→38→85; 12→88 vs ~10 in section 4).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
