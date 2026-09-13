# Metric Definition & the Single Source of Truth

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Metric Definition & the Single Source of Truth

**Subtitle:** Five teams query the same orders table and report five different "March revenue" numbers — because "revenue" was never defined once, in one place, that everyone reads from

## Five Teams, Five Revenue Numbers

**Tags:** `core idea` (blue), `metric definition` (green), `same table` (orange)

- **The question** — the CEO asks "what was March revenue?" and five teams answer from the same orders table
- **Sales says $1.25M** — every order placed in March, UTC clock, nothing filtered out
- **Marketing says $1.23M** — same, but internal test-account orders removed
- **Product says $1.15M** — test and cancelled orders removed, refunded orders still counted
- **Data says $1.10M** — refunds subtracted too; **Finance says $1.07M** — all that, plus the local-time month cutoff
- **The trap** — every number is "correct" for its own filters; the metric name is what's ambiguous

*Example (italic):* Same table, same month, five defensible SQL queries — and a $180K spread between the highest and lowest "March revenue".

**Key point:** A metric is not a name, it is a name plus a full filter list — until the filters are pinned down in one shared place, "revenue" means five different things.

### Visualization (canvas `c1`, 720×300)

Bar chart of the five teams' "March revenue" numbers side by side, all drawn from the same orders table.

- **Title (bold 15px, `#1a5276`, top center):** "Same Orders Table, Five 'March Revenue' Numbers".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = revenue $1.00M to $1.30M, gridlines `#e5e9ef` at $1.10M and $1.20M with 12px `#444` tick labels; 11px `#6b7280` note "y-axis starts at $1.0M" just above the origin.
- **Bars (70px wide, centered at x = 130, 245, 360, 475, 590), heights above baseline in px:** Sales `$1.25M` → 150, Marketing `$1.23M` → 138, Product `$1.15M` → 90, Data `$1.10M` → 60, Finance `$1.07M` → 42.
- **Bar colors:** Sales blue `#2a78d6`, Marketing aqua `#199e70`, Product violet `#4a3aa7`, Data yellow `#c98500`, Finance magenta `#d55181`; fills at 0.35 alpha with 2px solid top edge in the same hue.
- **Labels:** team name 12px `#444` below each bar; dollar value bold 12px `#2c3e50` above each bar top.
- **Annotation (bold 13px red `#e74c3c`, right side near y=70):** "$180K spread — same table, same month".
- **Caption (12px `#444`, bottom right):** "all numbers illustrative".

## Walking One Number Down to Another

**Tags:** `worked example` (blue), `filters` (green)

- **Start at $1.25M** — all orders with a March timestamp on the UTC clock (Sales' number)
- **Drop test accounts** — internal QA orders total $20K, leaving $1.23M (Marketing's number)
- **Drop cancelled orders** — $80K of orders were cancelled before shipping, leaving $1.15M (Product's number)
- **Subtract refunds** — $50K was refunded after delivery, leaving $1.10M (Data's number)
- **Shift the clock** — in local time, $30K of late-night March 31 orders belong to April, leaving $1.07M (Finance's number)
- **Hand-check** — 1,250 − 20 − 80 − 50 − 30 = 1,070 (in $K); every gap is one explicit filter

*Example (italic):* The five numbers differ by exactly four filter choices — test accounts ($20K), cancellations ($80K), refunds ($50K), and timezone ($30K).

**Key point:** Each team's number is one path through four yes/no filter decisions — write the path down and any two numbers reconcile in arithmetic, not in an argument.

### Visualization (canvas `c2`, 720×300)

Waterfall chart stepping from Sales' $1.25M down to Finance's $1.07M, one filter per step.

- **Title (bold 15px, `#1a5276`, top center):** "From $1.25M to $1.07M: Four Filters, One Step Each".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = revenue $1.00M to $1.30M, gridlines `#e5e9ef` at $1.10M and $1.20M with 12px `#444` tick labels; 11px `#6b7280` note "y-axis starts at $1.0M" above the origin.
- **Columns (80px wide, centered at x = 115, 215, 315, 415, 515, 615):** start bar "gross $1.25M" full height 150px in blue `rgba(42,120,214,0.35)` with 2px `#2a78d6` top; end bar "net $1.07M" height 42px in green `rgba(0,131,0,0.30)` with 2px `#008300` top.
- **Floating drop segments (orange fill `rgba(217,89,38,0.35)`, 2px `#d95926` edges), spanning the running-total levels in px above baseline:** "− test $20K" from 150 down to 138, "− cancelled $80K" from 138 down to 90, "− refunds $50K" from 90 down to 60, "− timezone $30K" from 60 down to 42.
- **Connectors:** dashed `#6b7280` (dash 4/3) horizontal lines linking each segment's bottom edge to the next segment's top edge.
- **Labels:** step name 12px `#444` below each column; step dollar amount bold 12px `#d95926` beside each drop segment; bold 12px labels `$1.25M` (blue) and `$1.07M` (green) above the end bars.
- **Annotation (bold 13px violet `#4a3aa7`, near x=430, y=75):** "every gap is a named filter, not an error".
- **Caption (12px `#444`, bottom right):** "amounts illustrative; steps sum exactly".

## What Metric Disputes Cost, and the Fix

**Tags:** `where it's used` (blue), `semantic layer` (green)

- **The meeting tax** — leadership reviews stall while teams argue whose number is right instead of what to do
- **Eroded trust** — after two dashboards disagree, every number gets re-checked by hand before anyone acts
- **Silent drift** — five copies of the revenue SQL mean a filter fixed in one dashboard stays wrong in four
- **The fix** — one canonical definition (`net_revenue`: exclude test, exclude cancelled, subtract refunds, local-time month) stored in a shared semantic layer / metrics store
- **One read path** — every dashboard, report, and export calls the metrics store; nobody re-implements the SQL
- **Variants get names** — Sales' number survives as `gross_bookings`, a different metric, not a different "revenue"

*Example (italic):* After the metrics store ships, all five dashboards show $1.07M for `net_revenue`, and Sales' $1.25M lives on under its own name, `gross_bookings`.

**Key point:** The fix is not agreeing in a meeting — it is moving the definition into code that every consumer reads from, so a filter change happens once and lands everywhere.

### Visualization (canvas `c3`, 720×300)

Two-panel flow diagram: five dashboards each with their own SQL against the orders table (left, numbers disagree) vs five dashboards reading one metrics store (right, one number).

- **Title (bold 15px, `#1a5276`, top center):** "Five Private Queries vs One Shared Definition".
- **Layout:** vertical dashed `#6b7280` (dash 4/3) divider at x=360; 12px bold `#444` panel labels "before" at x=40 y=52 and "after" at x=390 y=52.
- **Left panel:** blue `rgba(42,120,214,0.15)` rounded box (120×34, 8px radius) at x=30 y=130 labeled "orders table" (12px `#2c3e50`); five 3px arrows fanning out to five small boxes (90×26) stacked at x=210, y = 65/110/155/200/245, each labeled with its team and number in 11px: "Sales $1.25M", "Mktg $1.23M", "Prod $1.15M", "Data $1.10M", "Fin $1.07M"; each small box fill `rgba(231,76,60,0.12)` with 11px red `#e74c3c` "own SQL" tag on its arrow.
- **Right panel:** blue box "orders table" (120×34) at x=390 y=130, single 3px arrow to a green `rgba(0,131,0,0.12)` box (150×40, 2px `#008300` edge) at x=520 y=127 labeled "metrics store: net_revenue"; five thin 2px `#008300` arrows from it to five small boxes (86×24) at x=630, y = 65/110/155/200/245 labeled "dash 1"…"dash 5", each showing bold 11px green "$1.07M".
- **Annotation (bold 13px green `#008300`, centered under right panel near y=285):** "one definition, read everywhere".
- **Annotation (bold 13px red `#e74c3c`, centered under left panel near y=285):** "five copies, five answers".
- **Caption (12px `#444`, bottom right):** "dollar values illustrative".

## A Wiki Page Is Not a Source of Truth

**Tags:** `common mistake` (red), `enforcement` (orange)

- **The confusion** — teams write a definitions wiki page and declare the problem solved
- **Nothing reads it** — dashboards still run their own SQL; the document and the queries drift apart within months
- **The test** — a source of truth is something the queries *execute*, not something the analysts *promise to follow*
- **The sixth number** — a definitions doc that nobody's code calls often just adds one more variant to argue about
- **The mistake** — fixing the words (documentation) while leaving five separate implementations in place

*Example (italic):* Six months after the wiki page ships, a refund-handling change lands in two dashboards out of five — the March numbers split again, now with a document claiming they can't.

**Common mistake:** Treating documentation as enforcement. If each dashboard still owns its SQL, the definition lives in five places no matter what the wiki says — the store must be the only query path.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the wiki-page path (definitions drift back apart) vs the metrics-store path (one executable definition), shown as boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Documented vs Executable: Where the Definition Actually Lives".
- **Row 1 (y=95), label 12px `#444` at x=20:** "wiki page"; blue `#2a78d6` rounded box at x=140 labeled "definition written down" (12px), 3px arrow to an orange `#d95926` box at x=350 labeled "5 teams still own 5 queries", 3px arrow to a red `#e74c3c` box at x=560 labeled "numbers drift apart" with bold 12px red "✗ dispute returns".
- **Row 2 (y=205), label:** "metrics store"; blue box at x=140 labeled "definition written in code", 3px arrow to a green `#008300` box at x=350 labeled "all dashboards call it", arrow to a green box at x=560 labeled "one number everywhere" with bold 12px green "✓".
- **Box style:** 160–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "a definition only counts if the queries execute it".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); the five team totals ($1.25M / $1.23M / $1.15M / $1.10M / $1.07M) and the four filter steps ($20K test, $80K cancelled, $50K refunds, $30K timezone) are invented and labeled illustrative; the waterfall steps must sum exactly (1,250 − 20 − 80 − 50 − 30 = 1,070 in $K) and match the bar chart in c1.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
