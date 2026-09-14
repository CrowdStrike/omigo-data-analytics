# Data Pipeline Patterns

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Data Pipeline Patterns

**Subtitle:** A pipeline is the nightly route from cash register to report — the patterns decide where to clean (ETL vs ELT), how to layer the data (bronze/silver/gold), and how to rerun a job without doubling the numbers

## Three Registers, One Warehouse: ETL vs ELT

**Tags:** `core idea` (blue), `ETL vs ELT` (green), `warehouse` (orange)

- **The chain** — Beanline runs 3 coffee shops; every night each register uploads its day of receipts
- **Monday's haul** — shop A sends 412 rows, shop B 305, shop C 283: 1,000 raw receipt rows in total
- **ETL** — clean the rows first (drop the double-scans), then load only the 962 good ones
- **ELT** — load all 1,000 raw rows first, then clean them with queries inside the warehouse
- **The trade** — ETL stores less; ELT keeps the raw originals so you can re-clean them later

*Example (italic):* On Monday the ELT route lands all 1,000 raw rows at 11pm; a query inside the warehouse at 11:10pm produces the 962 clean ones.

**Key point:** A data pipeline is just the nightly route from registers to report; ETL transforms before loading, ELT loads raw and transforms inside the warehouse — the pattern names say where the cleaning happens.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same 1,000 Monday rows travelling the ETL route (clean, then load) vs the ELT route (load raw, then clean in place), drawn as labeled boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Same 1,000 Rows, Two Routes: ETL Cleans First, ELT Loads First".
- **Row 1 (y=95), label 12px `#444` at x=20:** "ETL"; blue `#2a78d6` rounded box at x=90 labeled "Extract — 1,000 rows", 3px arrow to a green `#008300` box at x=300 labeled "Transform — 962 clean", arrow to a blue box at x=520 labeled "Load — 962 stored".
- **Row 2 (y=205), label:** "ELT"; blue box at x=90 "Extract — 1,000 rows", arrow to a violet `#4a3aa7` box at x=300 labeled "Load — 1,000 raw stored", arrow to a green box at x=520 labeled "Transform inside — 962".
- **Box style:** 160px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(74,58,167,0.12)`, 12px `#2c3e50` text, 2px borders in the box color.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "ELT keeps the raw 1,000 on hand — re-cleaning later is a query, not a crisis".
- **Caption (12px `#444`, bottom right):** "row counts from Monday's example, illustrative".

## Bronze, Silver, Gold: Monday's 1,000 Receipts

**Tags:** `worked example` (blue), `medallion` (green), `bronze/silver/gold` (orange)

- **Bronze** — the raw landing zone: all 1,000 Monday rows exactly as the registers sent them
- **Silver** — cleaned and deduplicated: shop A 412→397, B 305→294, C 283→271
- **Hand-check** — 15 + 11 + 12 = 38 double-scans out; 1,000 − 38 = 962 clean rows land
- **Gold** — business-ready summary: exactly 3 rows, one per shop, with order count and revenue
- **The rule** — each layer is built only from the layer before it; nothing writes straight to gold

*Example (italic):* Gold's Monday table reads: shop A 397 orders / $1,867, shop B 294 / $1,378, shop C 271 / $1,267.

**Key point:** The medallion pattern is progressive refinement — bronze keeps raw data forever, silver holds one clean version, and gold holds the small tables the dashboards actually read.

### Visualization (canvas `c2`, 720×300)

Horizontal funnel of the three layers: bar length proportional to Monday row count, shrinking from 1,000 bronze rows to 962 silver rows to 3 gold rows.

- **Title (bold 15px, `#1a5276`, top center):** "The Medallion Funnel: 1,000 Bronze → 962 Silver → 3 Gold Rows".
- **Rows (top to bottom at y = 75, 145, 215), each with a left-aligned 12px `#444` label at x=20:** "bronze — raw as sent", "silver — dupes out", "gold — one row per shop". Bars start at x=230, max width 460.
- **Bronze bar:** orange `#d95926`, fill `rgba(217,89,38,0.30)` with 2px solid edge, width 460, 12px label "1,000 rows" at bar end.
- **Silver bar:** blue `#2a78d6`, fill `rgba(42,120,214,0.30)` with 2px solid edge, width 442 (962/1,000 of 460), label "962 rows".
- **Gold bar:** yellow `#c98500`, solid, width 10 (true share under 2px — drawn at minimum width), label "3 rows".
- **Side notes (11px `#6b7280`, under each bar):** bronze "kept forever, never edited"; silver "412→397, 305→294, 283→271"; gold "orders + revenue per shop".
- **Bar style:** 22px tall, 3px corner radius.
- **Annotation (bold 13px green `#008300`, right side near y=260):** "each layer is built only from the one above it".
- **Caption (12px `#444`, bottom right):** "Monday's counts; revenue figures illustrative".

## When the Cleaning Rule Changes

**Tags:** `where it's used` (blue), `replay` (green)

- **The change** — on Wednesday the ops lead asks that voided orders be dropped from counts too
- **Before the fix** — silver only dropped double-scans, so Monday's clean count stood at 962
- **No re-asking** — bronze still holds every raw row, so nobody phones the shops for old files
- **The replay** — one job rebuilds silver and gold for the whole week straight from stored bronze
- **Monday redone** — 962 counted orders becomes 940 once Monday's 22 voids drop out
- **Where you meet it** — every "restate last quarter's metric" request is exactly this replay

*Example (italic):* The full-week rebuild is one pass over 7 bronze files; each gold day shifts down by its void count, e.g. Saturday 1,210 → 1,183.

**Key point:** Keeping raw bronze turns a rule change from a data-collection crisis into a cheap recompute — this is the main reason ELT and the medallion layers travel together.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart of the week's daily clean order counts, before the void rule (orange) vs after the bronze replay (green), one pair per day.

- **Title (bold 15px, `#1a5276`, top center):** "One New Rule, Whole Week Rebuilt from Bronze".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = 7 day groups labeled "Mon"–"Sun" (12px `#444`); y = clean orders 850 to 1,250 (axis starts at 850), gridlines `#e5e9ef` at 900/1000/1100/1200 with 12px `#444` labels.
- **Before bars (orange `#d95926`):** days Mon–Sun, counts `[962, 918, 1004, 955, 990, 1210, 1185]`, bars 30px wide.
- **After bars (green `#008300`):** counts `[940, 897, 981, 930, 968, 1183, 1158]`, 30px wide, 6px gap from the orange bar in each pair.
- **Legend (12px, top left inside plot):** orange swatch "before fix (voids counted)", green swatch "after replay".
- **Annotation (bold 13px green `#008300`, above the Saturday pair):** "Sat: 1,210 → 1,183 — 27 voids out".
- **Caption (12px `#444`, bottom right):** "y-axis starts at 850 to make the shift visible; counts illustrative".

## Run the Job Twice, Get the Numbers Once

**Tags:** `common mistake` (red), `idempotent` (orange), `backfill` (green)

- **The failure** — Monday's load dies halfway at 3am; the on-call engineer reruns the job at 7am
- **The naive job** — an append-only INSERT adds Monday's 940 rows again: gold now shows 1,880
- **Run three times** — 940, 1,880, 2,820: every rerun inflates Monday by another full copy
- **Idempotent** — the job deletes Monday's partition first, then rewrites it: 940 on every rerun
- **The test** — run any job twice on the same day; if the numbers move, it is not idempotent

*Example (italic):* After the 3am failure the idempotent job is rerun twice more; Monday's gold row reads 940 orders all three times.

**Common mistake:** Writing backfill jobs as pure appends. A safe job overwrites its date partition (or merges on a key), so "just run it again" is always harmless — reruns and backfills stop being scary.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart of Monday's gold order count after 1, 2, and 3 runs of the same job: append pipeline grows, idempotent pipeline stays flat.

- **Title (bold 15px, `#1a5276`, top center):** "Rerun the Backfill: Append Multiplies, Idempotent Doesn't".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = three groups labeled "run 1", "run 2", "run 3" (12px `#444`); y = Monday gold orders 0 to 3,000, gridlines `#e5e9ef` at 940/1880/2820 with 12px `#444` labels.
- **Append bars (magenta `#d55181`):** counts `[940, 1880, 2820]`, bars 55px wide, 12px value labels above each bar.
- **Idempotent bars (green `#008300`):** counts `[940, 940, 940]`, 55px wide, 6px gap from the magenta bar in each pair, value labels above.
- **Legend (12px, top left inside plot):** magenta swatch "append-only job", green swatch "overwrite-partition job".
- **Annotation (bold 13px magenta `#d55181`, near run 3 at y=70):** "three runs, triple-counted Monday".
- **Second label (bold 12px green `#008300`, above the run-3 green bar):** "940 every time".
- **Caption (12px `#444`, bottom right):** "append growth schematic; 940 is Monday's true count".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); Beanline, its shop row counts (412/305/283 → 397/294/271), the weekly before/after counts, revenue dollars, and the rerun sequence 940/1,880/2,820 are invented and labeled illustrative; the arithmetic (1,000 − 38 = 962; 962 − 22 = 940; 940 × 3 = 2,820) is exact and must hand-check.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
