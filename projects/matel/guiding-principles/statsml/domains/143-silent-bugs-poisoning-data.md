# Silent Bugs Poisoning Data

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 143. Silent Bugs Poisoning Data

**Subtitle:** Pipeline bugs that corrupt values without triggering alerts — row counts match, schemas validate, but the numbers are wrong. Models train on poison and dashboards report fiction.

## Callout (philosophy box)

**The core problem:** Most validation checks structural correctness (schema, nulls, types, counts), not semantic correctness. A bug producing wrong-but-plausible values passes every check and poisons everything downstream silently.

## JOIN Fanout — Silent Row Multiplication

**One-to-Many JOIN Silently Duplicates Revenue**

- **The bug:** JOINing orders to items repeats each order's total once per item row.
- **The math:** SUM(order_total) then counts every order N times, N = items in that order.
- **Why it's silent:** Row counts grow and no nulls appear, so no validation alert ever fires.
- **Plausible upside:** Inflated revenue looks like good news, so nobody goes looking for a bug.
- **Discovery timeline:** Weeks to months — a human doubts a 4× quarter, or finance reconciliation disagrees.
- **Blast radius:** Every downstream model, forecast, budget, and headcount plan built on it.

### Visualization (canvas `c1`, 720×300)

Table-join diagram: 1 order row joined to 3 item rows producing 3 duplicated result rows. Light gray `#f9f9f9` canvas background.

- **Title (bold 14px sans-serif `#1a5276`, centered):** "JOIN Fanout: 1 order × 3 items = revenue counted 3×".
- **Left ("orders (1 row)"):** blue-stroked (`#1a5276`, 1.5px) 180×35 rect containing "order_id=101  total=$100".
- **Right ("items (3 rows)"):** three orange-stroked (`#e67e22`) 200×25 rects containing "order_id=101  item=1/2/3".
- **JOIN arrow:** red 2px line between the tables with bold red label "JOIN".
- **Result block ("Result after JOIN:"):** three 500×24 rows filled `rgba(231,76,60,0.1-0.15)` with red 1px stroke, each reading "order_id=101  total=$100  item=N    ← $100 counted again".
- **Sum line (bold red):** "SUM(total) = $300    Real revenue: $100    Inflated 3×".
- **Caption (italic `#555`, bottom center):** "No error. No null. Row count \"grew.\" Revenue \"up.\" All silent."

## Timezone/Encoding Bugs — Correct Structure, Wrong Values

**Every Row Looks Valid. Every Value Is Shifted.**

- **Timezone shift:** A server migration flips PST to UTC, shifting every timestamp +8 hours.
- **Still valid:** Every value parses as a legal timestamp, but time-of-day features are poisoned.
- **Encoding flip:** A Latin-1 to UTF-8 change turns "José" into "JosÃ©" at the byte level.
- **Match breakage:** That byte difference silently breaks string matching and deduplication.
- **Null representation:** Upstream sends empty strings for NULL, so IS NOT NULL filters admit garbage.
- **Diluted averages:** Those empty-string rows stay in the denominator and pull averages toward zero.
- **Silent type coercion:** An INT-to-FLOAT change makes join keys "100" ≠ "100.0", dropping rows.
- **Read as noise:** The resulting 30% revenue shrink gets written off as "normal variance."

### Visualization (canvas `c2`, 720×300)

Two overlaid 24-hour purchase-pattern curves: real vs timezone-shifted. Light gray `#f9f9f9` background; black axes.

- **Title (bold 14px `#1a5276`):** "Hourly purchase pattern: before and after silent timezone bug".
- **Real curve (solid green `#27ae60`, width 2.5):** hourly values v(i) = 10 + 40·exp(−(i−10)²/8) + 30·exp(−(i−14)²/6) + 25·exp(−(i−20)²/10) for i = 0…23 (peaks at 10am, 2pm, 8pm PST); scale max 80.
- **Shifted curve (dashed 5/5 red `#e74c3c`, width 2.5):** the same series rotated by +8 hours: shifted[i] = real[(i+16) mod 24].
- **X labels:** "0h", "3h", … every 3 hours.
- **Legend:** "Real pattern (PST)" (green solid) and "After bug: shifted +8h (UTC). No error. Features poisoned." (red dashed).

## Stale Cache / Snapshot — Data Stops Updating but Looks Current

**The Pipeline Ran Successfully. It Just Read Yesterday's Data.**

- **The bug:** A materialized view refresh fails silently, so the ETL "succeeds" on yesterday's data.
- **What users see:** The dashboard renders normally and reports stale numbers as if they were current.
- **The cache variant:** An expired cache falls back to last-known values, freezing user_last_login.
- **Model effect:** With that feature frozen, the model scores everyone in the base as inactive.
- **The snapshot variant:** A hardcoded path re-reads last month's export on every training run.
- **False stability:** The model "retrains" on identical data and looks stable while actually frozen.
- **Discovery:** Usually a human noticing "this number hasn't changed in 3 days," not an alert.
- **Why checks pass:** Data exists with the right shape, right types, and the right row count.

### Visualization (canvas `c3`, 720×300)

Time-series chart over 30 days: growing reality vs dashboard value frozen after a silent refresh failure. Light gray `#f9f9f9` background; black axes.

- **Title (bold 14px `#1a5276`):** "Dashboard value over time — stale data goes unnoticed".
- **Real line (solid green `#27ae60`, width 2):** value ≈ 100 + day×3 plus small random noise (0-10), for 30 days; scale max 200.
- **Stale line (dashed 4/4 red `#e74c3c`, width 2):** tracks the real line for days 0-14, then freezes at ≈145 (plus tiny noise) for days 15-29.
- **Bug marker:** vertical red 2px line at day 15 spanning the plot, labeled above in bold red: "View refresh" / "fails silently".
- **Legend:** "Reality (growing)" (green solid) and "What dashboard shows (frozen). Pipeline \"succeeded.\" Data is stale." (red dashed).

## Upstream Schema Change — Silent Semantic Shift

**Column Name Didn't Change. Meaning Did.**

- **The classic:** A new enum value like 'add_to_cart' falls outside your event_type filter.
- **What it costs:** A funnel step silently vanishes, so the report shows a conversion drop that isn't real.
- **The reuse:** Upstream reuses deprecated status=7 for enterprise leads instead of expired trials.
- **Inverted filter:** Your "exclude expired trials" clause now quietly excludes enterprise deals.
- **The backfill:** An upstream backfill leaves two formula versions inside your processed history.
- **Learned artifact:** Models treat the discontinuity between the versions as a real "pattern."
- **The default change:** A column default moves from 0 to NULL, shrinking the denominator of AVG.
- **Phantom win:** The metric "improves 40%" with nothing real happening in the business.

### Visualization (canvas `c4`, 720×300)

Before/after event-type boxes against a fixed SQL filter, new types marked DROPPED. Light gray `#f9f9f9` background.

- **Title (bold 14px `#1a5276`):** "Upstream adds event type — your filter silently drops it".
- **Filter line (12px monospace `#333`):** `WHERE event_type IN ('click', 'view', 'purchase')`.
- **Before row ("Before (all captured):"):** three 120×35 boxes — click, view, purchase — green fill `rgba(39,174,96,0.2)` with `#27ae60` stroke.
- **After row ("After upstream change (2 types silently lost):"):** five boxes — click, view, purchase (green) plus add_to_cart and wishlist in red fill `rgba(231,76,60,0.2)` with `#e74c3c` stroke and bold red "DROPPED" beneath each.
- **Caption (italic red, bottom center):** "Funnel analysis now misses a step. Conversion rate drops. Bug looks like a product problem."

## Partial Failure — Some Rows Poisoned, Most Fine

**99% of Data Is Correct. The 1% Corrupts Downstream.**

- **Race condition:** Reading a table mid-write leaves ~2% of rows with mixed-vintage columns.
- **Types still valid:** Nothing fails validation, yet revenue is simply wrong for those orders.
- **Sparse corruption:** A -999 sensor sentinel is treated as a real reading rather than a missing one.
- **What it teaches:** Averages get dragged down and the model learns an impossible temperature.
- **Silent truncation:** VARCHAR(50) clips longer values without error in roughly 1% of rows.
- **What gets mangled:** URLs, product names, and addresses come out shortened but still well-formed.

### Visualization (canvas `c5`, 720×280)

Grid of row cells, mostly green with a few red poisoned cells. Light gray `#f9f9f9` background.

- **Title (bold 14px `#1a5276`):** "99% correct, 1% poisoned — enough to corrupt downstream".
- **Grid:** 25 columns × 4 rows of cells (40px tall, width filling the margins) starting at y=50; valid cells filled `rgba(39,174,96,0.15)` with `#ddd` border; poisoned cells at flat indices 3, 27, 48, 61, 79 filled `rgba(231,76,60,0.4)` with `#e74c3c` border.
- **Legend (below grid):** "■ Valid row" in green and "■ Silently corrupted (wrong value, passes all checks)" in red.
- **Caption (italic `#555`, bottom center):** "AVG pulled by outliers. Model trains on poison. Alert threshold: not breached (only 1% bad)."

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle` + one `.philosophy` callout, then one `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` div + `<ul>` of labeled bullets, right `<td>` (60%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links.
- **Canvas:** charts are 720×300 except `c5` at 720×280; every chart first fills the canvas with a `#f9f9f9` background; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart text uses 9-14px sans-serif (plus monospace for SQL/code strings). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#555`/`#333`, light grid border `#ddd`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
