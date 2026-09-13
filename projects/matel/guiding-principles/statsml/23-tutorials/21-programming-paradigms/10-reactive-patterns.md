# Reactive Patterns

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table.layout with text left 50% / canvas right 50%)
**HTML title tag:** Reactive Patterns

**Subtitle:** Like a spreadsheet: you declare how values depend on each other once, and every change ripples through on its own

## Change One Cell, the Sheet Updates Itself

Tags: `core idea` (blue pill), `running example` (green pill)

- **The sheet** — B1 price $40, B2 units 12, B3 revenue = B1×B2, B4 tax = B3×10%, B5 total = B3+B4
- **The edit** — you type 50 into B1 and press Enter; that is all you do
- **The ripple** — revenue 480 → 600, tax 48 → 60, total 528 → 660, by itself
- **Untouched** — B2 (units) doesn't depend on price, so it never recomputes
- **You declared, it propagates** — formulas state relationships; the sheet decides what to redo

*Example (italic):* Nobody writes "when price changes, recalculate B3 then B4 then B5" — the formulas already say who depends on whom.

**Key point:** Reactive means declaring how outputs relate to inputs, then letting the system push every change through the dependency chain automatically.

### Visualization (canvas `c1`, 720×300)

Dependency graph with before → after values.

- **Title (bold 15px ink `#1a5276`, top center):** "Edit B1 Once — Three Cells Recompute Themselves"
- **Five cell boxes** (148×62 each, drawn by a `cellBox()` helper): changed cells get pink fill `rgba(213,81,129,0.10)` and magenta `#d55181` stroke with bold magenta "before → after" value line; unchanged cells get fill `rgba(26,82,118,0.05)`, the given stroke color, and bold dark "value  (unchanged)". Each box shows bold ink name, gray formula line, then value line.

| position | name | formula | before | after | changed | base stroke |
|----------|------|---------|--------|-------|---------|-------------|
| (24,60) | B1  price | input | $40 | $50 | yes | blue `#2a78d6` |
| (24,180) | B2  units | input | 12 | — | no | blue `#2a78d6` |
| (258,120) | B3  revenue | = B1 × B2 | 480 | 600 | yes | aqua `#199e70` |
| (500,55) | B4  tax | = B3 × 10% | 48 | 60 | yes | violet `#4a3aa7` |
| (500,195) | B5  total | = B3 + B4 | 528 | 660 | yes | orange `#d95926` |

- **Arrows:** blue from B1 and B2 into B3; aqua from B3 to B4 and to B5; violet vertical from B4 down to B5.
- **Annotations:** bold magenta 13px centered at (350,288): "you typed one number — pink cells updated on their own"; bold green `#008300` 12px two lines at (98, 262/278): "B2 not downstream:" / "never recomputed"

## Tracing the Update Wave by Hand

Tags: `worked example` (green pill), `step by step` (blue pill)

- **Step 0** — edit lands: B1 becomes 50; the sheet marks B1's dependents dirty
- **Step 1** — B3 recomputes: revenue = 50 × 12 = 600 (was 480)
- **Step 2** — B4 recomputes: tax = 600 × 0.10 = 60 (was 48)
- **Step 3** — B5 recomputes: total = 600 + 60 = 660 (was 528)
- **Order matters** — B5 waits for both B3 and B4, so it always sees fresh inputs
- **Work done** — exactly 3 recomputes, no matter how big the rest of the sheet is

*Example (italic):* One keystroke, three recomputes, done — a sheet with 10,000 other cells does zero extra work.

**Key point:** The system walks the dependency graph in order — each cell recomputes once, after everything it reads is already fresh.

### Visualization (canvas `c2`, 720×300)

Ordered step boxes: update wave as ordered steps.

- **Title (bold 15px ink, top center):** "The Update Wave: One Edit, Three Recomputes, In Order"
- **Four step boxes** in a row (152×74, gap 24, starting x=22, y=92; fill `rgba(26,82,118,0.05)`, colored 2px stroke; bold colored step label, bold dark 13px cell line, gray 12px sub line; gray arrows between boxes):

| step | cell line | sub line | stroke color |
|------|-----------|----------|--------------|
| step 0 | B1 = 50 | your edit lands | blue `#2a78d6` |
| step 1 | B3 = 50 × 12 = 600 | revenue (was 480) | aqua `#199e70` |
| step 2 | B4 = 600 × 0.10 = 60 | tax (was 48) | violet `#4a3aa7` |
| step 3 | B5 = 600 + 60 = 660 | total (was 528) | orange `#d95926` |

- **Annotations (centered):** gray 12px at y=215: "B5 runs last: it reads B3 and B4, so it waits until both are fresh"; bold green 13px at y=252: "total work = 3 recomputes — a 10,000-cell sheet does nothing extra"

## Dashboards Are Spreadsheets Over Live Data

Tags: `why it matters` (blue pill), `where it's used` (green pill)

- **Same idea, live inputs** — declare "today's revenue = sum of order totals" once
- **Each order is an edit** — $120 at 09:04, $75 at 09:12, $210 at 09:25, $60 at 09:41
- **The metric ripples** — running total: 120 → 195 → 405 → 465, no refresh job
- **Streaming aggregates** — Kafka Streams, Flink, materialized views all work this way
- **Front ends too** — React, notebooks with widgets: change an input, the view recomputes

*Example (italic):* A "live" dashboard is a spreadsheet whose input cells are being edited by the order stream.

**Key point:** Once you see metrics as formulas over event streams, "keeping the dashboard fresh" stops being a cron job and becomes a dependency graph.

### Visualization (canvas `c3`, 720×300)

Step-line chart: running revenue stepping up as orders arrive.

- **Title (bold 15px ink, top center):** "A Live Metric Is a Formula Being Edited by Events"
- **Axes:** x = time 09:00 to 09:50 (ticks every 10 min labeled "09:00" … "09:50"); y = $0 to $500 (gridlines and labels at $0, $100, $200, $300, $400, $500 in light gray `#e5e9ef` grid, gray labels). Padding: top 52, bottom 56, left 66, right 30.
- **Step line** (blue `#2a78d6`, width 3) starting at $0, stepping up at each order; **data:**

| minute | order amount | cumulative |
|--------|-------------|------------|
| 4 | +$120 | $120 |
| 12 | +$75 | $195 |
| 25 | +$210 | $405 |
| 41 | +$60 | $465 |

- **Order markers:** orange `#d95926` 5px dots at each step top, with bold orange "+$amt" label above and bold blue "$cum" label just below it.
- **Annotations:** bold green 13px left-aligned near (X(14), Y(430)): "no refresh job — each order nudges the total"; gray 12px centered at bottom: "today's revenue = sum(order totals), declared once"

## The Common Confusion: Reactive Is Not a Refresh Loop

Tags: `common mistake` (red pill), `trade-off` (orange pill)

- **Refresh loop** — "recompute all 500 dashboard tiles every minute", changed or not
- **Reactive** — one price change recomputes the 3 tiles downstream of it, now
- **Cost gap** — 500 recomputes per tick vs 3 per actual change
- **Freshness gap** — the loop is up to a minute stale; reactive updates immediately
- **The tell** — if nothing changed and work still happened, that's a loop, not reactive

*Example (italic):* A page that re-runs every query on a 60-second timer feels live but is a polling loop wearing a dashboard costume.

**Key point:** Reactive systems do work proportional to what changed; refresh loops do work proportional to how much you have — that difference is the whole pattern.

### Visualization (canvas `c4`, 720×300)

Two-bar comparison: recomputes per change — refresh loop vs reactive.

- **Title (bold 15px ink, top center):** "Work Done When One Input Changes (500-tile dashboard)"
- **Bars:** width 160, baseline at y=226 (thin gray line from x=70 to x=650), chart height 158, scale max 520; minimum bar height 4px.
  - Left bar at x=130: value 500, orange `#d95926`; bold orange value label "500 recomputes" above; below: bold dark "refresh loop" and gray sub "recompute every tile, every tick".
  - Right bar at x=430: value 3, green `#008300`; label "3 recomputes"; below: "reactive" and "recompute only what changed".
- **Bottom annotation (bold magenta 13px, centered, y=284):** "167x less work — and fresher, too (instant vs up to 60 s stale)"

## Regeneration instructions

- **Template/layout:** tutorials detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks each with `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, td padding 12px, vertical-align top; `td.text-col` 50% / `td.viz-col` 50%).
- **Text column structure:** `.tags` pill row first (pills 0.72rem bold, 2px 10px padding, 10px radius: blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (0.9rem `#555`); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases have `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 intrinsic; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; shared `arrow()` and `cellBox()` (name/formula/before/after/changed-highlight) drawing helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card/page links use `.html` extensions.
