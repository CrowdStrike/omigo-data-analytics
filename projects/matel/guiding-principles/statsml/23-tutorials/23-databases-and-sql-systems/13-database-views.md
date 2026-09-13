# Database Views

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Database Views

**Subtitle:** A view is a saved query wearing a table costume — you query the name, and the database quietly runs the stored query underneath

## Five Reports, One Messy Three-Table Join

**Tags:** `core idea` (blue), `saved query` (green), `SQL` (orange)

- **The definition** — an online store calls a customer "active" if they have a paid order in the last 90 days
- **The join** — answering that takes three tables: customers joined to orders joined to payments
- **The mess** — two join keys, a date filter, a payment-status filter — 14 lines of SQL, every time
- **Five copies** — the email list, churn dashboard, revenue report, loyalty picks and exec summary each paste it
- **The view** — `CREATE VIEW active_customers AS <that query>` saves the query under a table-like name
- **The costume** — every report now writes `FROM active_customers` as if that table really existed

*Example (italic):* The Tuesday email report shrinks from a 14-line three-table join to one line: `SELECT email FROM active_customers`.

**Key point:** A view is a SELECT statement saved under a name. The database swaps the full query in wherever the name appears — no rows are copied, no new table is stored.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three raw tables feed one view box, and five report boxes all read from the view.

- **Title (bold 15px, `#1a5276`, top center):** "One Saved Join, Five Reports Share It".
- **Table boxes (left column):** three rounded boxes at x=40, width 140, height 36, y = 62 / 127 / 192; fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, 12px `#2c3e50` labels "customers — 12,000 rows", "orders — 48,000 rows", "payments — 46,500 rows".
- **View box (center):** rounded box at x=270, y=112, width 180, height 66; fill `rgba(0,131,0,0.12)`, 2px `#008300` border; bold 12px `#008300` line "VIEW active_customers", 11px `#6b7280` line "3,400 rows when queried".
- **Report boxes (right column):** five rounded boxes at x=540, width 150, height 30, y = 48 / 98 / 148 / 198 / 248; fill `rgba(74,58,167,0.10)`, 1.5px `#4a3aa7` border, 12px `#2c3e50` labels "email list", "churn dashboard", "revenue report", "loyalty picks", "exec summary".
- **Arrows:** 2px `#6b7280` arrows from each table box's right edge (x=180) to the view box's left edge, and from the view box's right edge (x=450) fanning out to each report box.
- **Annotation (bold 13px green `#008300`, near x=200, y=280):** "the join is written once — the name is reused five times".
- **Caption (12px `#444`, bottom right):** "row counts illustrative".

## What the Database Actually Runs

**Tags:** `worked example` (blue), `query expansion` (green), `permissions` (orange)

- **The analyst types** — `SELECT name, last_order_date FROM active_customers WHERE total_paid > 200`
- **The rewrite** — before anything runs, the name expands into the stored three-table join plus the filter
- **Hand-check in** — customer 4127 has 3 paid orders totaling $260 in the window, so she appears
- **Hand-check out** — customer 5580's last paid order was 140 days ago, so the view never shows him
- **The lock** — analysts get SELECT on the view only; the payments table (with card_last4) grants them nothing
- **The trick** — the view reads tables with its owner's rights, so readers see results without touching raw data

*Example (italic):* Customer 4127 (3 paid orders, $260 in 90 days) is in the result and 5580 (last paid order 140 days ago) is not — identically by hand or through the view.

**Key point:** Querying the view and querying its stored SELECT are the same operation — and because it is a separate object, you can grant people the view while denying them the raw tables underneath.

### Visualization (canvas `c2`, 720×300)

Side-by-side code panels: the short query the analyst types on the left, the expanded three-table join the database runs on the right, joined by an arrow.

- **Title (bold 15px, `#1a5276`, top center):** "You Type Three Lines — the Database Runs Fourteen".
- **Left panel:** rounded box x=40, y=70, width 270, height 150; fill `rgba(42,120,214,0.08)`, 2px `#2a78d6` border; bold 12px `#1a5276` header "what the analyst types" at y=60; inside, 12px monospace `#2c3e50` lines: "SELECT name, last_order_date", "FROM active_customers", "WHERE total_paid > 200;".
- **Arrow:** 2.5px `#6b7280` arrow from x=318 to x=396 at y=145, 12px `#6b7280` label "the name expands" above it.
- **Right panel:** rounded box x=400, y=52, width 290, height 186; fill `rgba(0,131,0,0.07)`, 2px `#008300` border; bold 12px `#008300` header "what the database runs" at y=44; inside, 11px monospace `#2c3e50` lines: "SELECT c.name, MAX(o.order_date), SUM(p.amount)", "FROM customers c", "JOIN orders o ON o.customer_id = c.id", "JOIN payments p ON p.order_id = o.id", "WHERE p.status = 'paid'", "  AND o.order_date >= today - 90 days", "GROUP BY c.name", "HAVING SUM(p.amount) > 200;".
- **Caption (12px `#6b7280`, right-aligned at x=690, y=252):** "expansion abridged — 14 lines in full".
- **Permission note (bold 12px orange `#d95926`, x=40, y=252):** "analysts: SELECT on the view only — direct reads of payments denied".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "same rows either way — the view adds a name, not data".

## One Definition of Active, Not Five

**Tags:** `why it matters` (blue), `single source` (green), `column security` (orange)

- **The drift** — the five pasted copies quietly diverged: 90-day, 60-day, 30-day and 75-day windows
- **The worst copy** — the exec summary forgot the paid filter and counted unpaid orders too
- **Five answers** — the same word "active" reported as 3,400, 2,600, 1,900, 3,150 and 3,650 customers
- **One edit** — after the view, changing the window is one `CREATE OR REPLACE VIEW`; all five reports follow
- **Column security** — the view exposes 5 safe columns; card_last4 and margin never leave the raw tables
- **A stable name** — tables behind the view can be renamed or split; reports keep querying the same name

*Example (italic):* In the Monday meeting the exec asks why churn says 2,600 actives while the email list says 3,400 — nobody can answer, because both copies are "the" definition.

**Key point:** A view turns a business definition into one shared object — one place to fix the logic, one place to attach permissions, instead of five drifting copies.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: "active customers" as counted by each report before the view (five pasted copies, five numbers) vs after (all read the shared view).

- **Title (bold 15px, `#1a5276`, top center):** "Five Pasted Copies, Five Answers — One View, One Answer".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 180; y = customers 0 to 4,000, gridlines `#e5e9ef` at 1,000 / 2,000 / 3,000 with 12px `#444` labels; group labels 12px `#444` below the baseline: "email", "churn", "revenue", "loyalty", "exec", groups centered at x = 120 / 240 / 360 / 480 / 600.
- **Before bars:** orange `#d95926`, width 34, left of each group center; values `[3400, 2600, 1900, 3150, 3650]`, 11px `#444` value labels on top.
- **After bars:** green `#008300`, width 34, right of each group center; values `[3400, 3400, 3400, 3400, 3400]`, 11px value label on the first bar only.
- **Reference line:** dashed `#1a5276` (dash 4/3) horizontal line at 3,400 with 12px `#1a5276` label "one definition: 3,400" at its right end.
- **Legend (top right, 12px):** orange swatch "pasted copy (before)", green swatch "shared view (after)".
- **Annotation (bold 13px orange `#d95926`, near x=90, y=60):** "same word 'active', five different numbers".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## A View Is Not a Faster Table

**Tags:** `common mistake` (red), `computed at query time` (orange)

- **No speedup** — the view stores the query text, not the answer; the join runs fresh on every query
- **Same 1.8s** — the raw three-table join takes 1.8s; the same SELECT through the view takes the same 1.8s
- **Stacking** — views built on views rerun the whole stack each time, so slowness quietly compounds
- **The fast cousin** — a materialized view or nightly summary table stores the rows (0.05s) but goes stale
- **The tell** — if a "table" is always perfectly up to date and cost nothing to create, it is a plain view

*Example (italic):* The team wraps the slow churn join in a view hoping the dashboard gets snappy — it stays at 1.8s, because the view reruns that exact join.

**Common mistake:** Expecting a view to speed up a slow query. A plain view is expanded and recomputed at query time; only storing the results — a summary table or materialized view — buys speed, at the price of staleness.

### Visualization (canvas `c4`, 720×300)

Bar chart of query time for the same question asked three ways: the raw join, the join through a view, and a precomputed nightly summary table.

- **Title (bold 15px, `#1a5276`, top center):** "Same Question Three Ways: the View Is Exactly as Slow as the Join".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 180; y = query time 0 to 2.0s, gridlines `#e5e9ef` at 0.5 / 1.0 / 1.5 with 12px `#444` labels "0.5s" / "1.0s" / "1.5s".
- **Bars (width 120, centered at x = 170 / 370 / 570):** blue `#2a78d6` "raw 3-table join" at 1.8s; orange `#d95926` "through the view" at 1.8s; aqua `#199e70` "nightly summary table" at 0.05s; bold 12px `#2c3e50` time labels "1.8s" / "1.8s" / "0.05s" above each bar, 12px `#444` bar names below the baseline.
- **Equality marker:** dashed `#6b7280` (dash 4/3) horizontal line joining the tops of the first two bars.
- **Staleness note (11px `#6b7280`, under the third bar's time label):** "stale until tonight's refresh".
- **Annotation (bold 13px orange `#d95926`, centered between the first two bars near y=60):** "same 1.8s — the view saved typing, not time".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); table row counts (12,000 / 48,000 / 46,500), the view's 3,400 rows, the drifted report counts `[3400, 2600, 1900, 3150, 3650]`, customer hand-checks (4127 with $260 in / 5580 at 140 days out) and query timings (1.8s / 1.8s / 0.05s) are all invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
