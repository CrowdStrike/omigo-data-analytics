# Oracle & SQL Server

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Oracle & SQL Server

**Subtitle:** The two enterprise database incumbents sell certainty — a licensed product, a support hotline, and certified DBAs — and the per-core price of that certainty is what launched the open-source migration wave

## The Database Behind the Bank

**Tags:** `core idea` (blue), `enterprise model` (green), `incumbents` (orange)

- **The incumbents** — Oracle Database (first release 1979) and Microsoft SQL Server (1989) run most large enterprises
- **The model** — buy a license up front, then pay a yearly support contract for patches, updates, and help
- **Oracle's edge** — RAC clustering lets several servers serve one database for failover and scale-out
- **SQL Server's edge** — T-SQL plus tight integration with Windows, .NET, and Microsoft's tooling (SSMS)
- **The promise** — certified reliability: a vendor contractually on the hook when the payroll database goes down

*Example (italic):* A bank's core ledger has run on the same Oracle installation for twenty years; what the CFO signs each year is not the software — it's the support contract.

**Key point:** The enterprise-database model sells certainty — a battle-tested product, a support hotline, and an ecosystem of certified DBAs — and prices that certainty into the license.

### Visualization (canvas `c1`, 720×300)

Two-column comparison diagram: an Oracle stack on the left, a SQL Server stack on the right, and one shared box beneath both showing the common business model.

- **Title (bold 15px, `#1a5276`, top center):** "Two Incumbents, One Business Model".
- **Oracle column (x=70, width 270):** header box at y=52 (height 32, fill `rgba(42,120,214,0.25)`, bold 13px `#1a5276` text "Oracle Database — since 1979"); three rows beneath at y=94, 130, 166 (height 28, fill `rgba(42,120,214,0.12)`, 12px `#2c3e50` text): "RAC clustering — many servers, one DB", "PL/SQL stored procedures", "per-core licensing".
- **SQL Server column (x=380, width 270):** header box at y=52 (fill `rgba(25,158,112,0.25)`, bold 13px `#1a5276` text "SQL Server — since 1989"); three rows at y=94, 130, 166 (fill `rgba(25,158,112,0.12)`): "Windows / .NET ecosystem", "T-SQL stored procedures", "per-core licensing (since 2012)".
- **Shared box:** rounded box at x=70, y=222, width 580, height 40, fill `rgba(230,126,34,0.12)`, 2px `#d95926` border, bold 12px `#d95926` centered text "same model: license + yearly support contract + certified DBA ecosystem".
- **Connectors:** 2px `#6b7280` vertical lines from the bottom center of each column (y=194) down to the shared box (y=222).
- **Box style:** 6px corner radius, 12px `#2c3e50` row text left-padded 10px.
- **Caption (12px `#444`, bottom right):** "release years as publicly documented".

## Pricing One 32-Core Server

**Tags:** `worked example` (blue), `per-core licensing` (green)

- **The server** — one machine with 2 CPUs of 16 cores each = 32 cores runs the orders database
- **The core factor** — x86 cores count at 0.5 each, so 32 × 0.5 = 16 processor licenses (exact math)
- **The license** — at an illustrative $47,500 list per license, 16 × $47,500 = $760,000 up front
- **The support** — 22% of the license per year: $167,200 annually for patches and the hotline
- **Five years** — $760,000 + 5 × $167,200 = $1,596,000 for this one server (totals illustrative)

*Example (italic):* Doubling the cores to speed up quarter-end reports doubles the license bill before a single query runs faster.

**Key point:** Per-core licensing ties the database bill directly to hardware size — every scale-up decision becomes a procurement decision, not just an engineering one.

### Visualization (canvas `c2`, 720×300)

Stacked bar chart of cumulative 5-year cost for the 32-core server: license portion (constant) plus support portion (growing year by year).

- **Title (bold 15px, `#1a5276`, top center):** "Five-Year Cost of One 32-Core Server (illustrative list prices)".
- **Axes:** origin x=80, baseline y=245, plot width 580, plot height 180; x = years 1–5, one bar per year, 12px `#444` labels "yr 1"–"yr 5" under each bar; y = cumulative $ 0 to $1.6M, gridlines `#e5e9ef` at $0.4M / $0.8M / $1.2M with 12px `#444` labels.
- **Bars (width 64, evenly spaced across the plot):** each bar stacks a blue license block `rgba(42,120,214,0.35)` with 2px `#2a78d6` border representing the constant $760,000, topped by a green support block `rgba(0,131,0,0.30)` with 2px `#008300` border for cumulative support `[167.2, 334.4, 501.6, 668.8, 836.0]` (thousands); cumulative totals `[927.2, 1094.4, 1261.6, 1428.8, 1596.0]` thousand, 11px `#444` total label above each bar ("$0.93M" … "$1.60M").
- **Legend (12px, top left inside plot):** blue swatch "license $760k (one-time)", green swatch "support 22%/yr".
- **Annotation (bold 13px green `#008300`, near the year-5 bar, y=75):** "by year 5, support ($836k) has cost more than the license ($760k)".
- **Caption (12px `#444`, bottom right):** "core-factor math exact; prices illustrative — contracts are negotiated".

## Why the Migration Wave Happened

**Tags:** `where it's used` (blue), `open source` (green)

- **Where you meet it** — the warehouses that feed a data scientist's queries often started life in Oracle or SQL Server
- **The cost driver** — a bill that scales with cores collided with data and compute needs that exploded
- **The alternatives** — PostgreSQL and MySQL matured through the 2000s with no per-core license at all
- **The cloud push** — managed open-source services made running without a dedicated DBA team realistic
- **What stayed** — core ledgers and ERP systems often remain: migration risk outweighs license savings

*Example (italic):* A startup picking PostgreSQL by default was unusual in 2005 and unremarkable by 2020 — the license line item simply never appears in its budget.

**Key point:** The migration wave was economics, not features — once open-source engines were good enough, the per-core bill became the argument that won budget meetings.

### Visualization (canvas `c3`, 720×300)

Two-line crossover chart: share of new projects choosing a commercial engine vs an open-source engine, 2000 to 2025, with the crossover marked.

- **Title (bold 15px, `#1a5276`, top center):** "New Projects: Commercial vs Open-Source Engine Choice (illustrative)".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = years 2000 to 2025, 12px `#444` tick labels every 5 years; y = share of new projects 0–100%, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels.
- **Commercial line:** blue `#2a78d6` 3px line through years `[2000, 2005, 2010, 2015, 2020, 2025]`, shares `[85, 75, 60, 45, 32, 25]`, 12px bold blue label "commercial (Oracle, SQL Server)" near its left end.
- **Open-source line:** green `#008300` 3px line through the same years, shares `[15, 25, 40, 55, 68, 75]`, 12px bold green label "open source (Postgres, MySQL)" near its right end.
- **Crossover marker:** vertical dashed `#6b7280` (dash 4/3) line at year ≈2013 where the lines cross at 50%, 12px `#6b7280` label "crossover" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near year 2020, y=70):** "existing enterprise installs did not vanish — new projects changed".
- **Caption (12px `#444`, bottom right):** "shares illustrative — direction, not measurement".

## Open Source Is Not Free Either

**Tags:** `common mistake` (red), `total cost` (orange)

- **The mistake** — reading "$0 license" as "$0 cost" when planning a migration off Oracle or SQL Server
- **The dialect wall** — years of PL/SQL or T-SQL stored procedures do not run on PostgreSQL unchanged
- **The people cost** — the support contract bought labor: backups, tuning, patching now come from your team
- **The dual run** — old and new systems run in parallel for months while every report is reconciled
- **The honest math** — the savings are real, but they arrive after a migration project that costs real money

*Example (italic):* An illustrative migration saves $200k/yr in license and support but costs $800k as a one-time project — break-even lands in year 4, not on day one.

**Common mistake:** Comparing the license line to zero. The fair comparison is license + support vs migration project + new operating costs — often still a win, but never a free one.

### Visualization (canvas `c4`, 720×300)

Payback-curve line chart: cumulative net savings of a migration ($200k/yr savings against an $800k one-time project cost) crossing zero at year 4.

- **Title (bold 15px, `#1a5276`, top center):** "The Migration Payback Curve (illustrative)".
- **Axes:** origin x=80, plot width 580, plot height 190 (top y=45, bottom y=235); x = years 0 to 5, 12px `#444` tick labels each year; y = cumulative net $ from −$800k to +$400k, gridlines `#e5e9ef` at −$400k and +$200k, and a solid 2px `#6b7280` zero line at y=172 labeled "$0" (12px `#444`).
- **Net line:** blue `#2a78d6` 3px line through years `[0, 1, 2, 3, 4, 5]`, cumulative net `[-800, -600, -400, -200, 0, 200]` (thousands, exact arithmetic from the two illustrative inputs), 5px `#2a78d6` dots at each point.
- **Region shading:** below the zero line, fill under the curve `rgba(231,76,60,0.10)` with 12px bold `#e74c3c` label "project cost not yet recovered" near year 1.5; above the zero line, fill `rgba(0,131,0,0.12)` with 12px bold `#008300` label "net savings" near year 4.7.
- **Break-even marker:** vertical dashed `#6b7280` (dash 4/3) line at year 4, bold 13px orange `#d95926` annotation "break-even in year 4" beside it at y=90.
- **Caption (12px `#444`, bottom right):** "$200k/yr savings and $800k project cost illustrative; the curve's arithmetic is exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the core-factor arithmetic (32 × 0.5 = 16) and the payback-curve arithmetic (−800 + 200 × year) are exact, but the dollar prices, adoption shares, and migration costs are invented and labeled illustrative; product release years (Oracle 1979, SQL Server 1989, SQL Server per-core licensing 2012) are publicly documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
