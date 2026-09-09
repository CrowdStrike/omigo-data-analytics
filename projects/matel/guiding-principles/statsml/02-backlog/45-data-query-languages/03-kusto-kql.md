# 3. Kusto KQL

**Page type:** detail page (backlog kusto-style 2-col text/viz layout: text left 45%, viz right 55%, one layout table per section)
**HTML title tag:** 3. Kusto KQL

**Subtitle:** Azure's pipe-forward language for telemetry at scale

**Intro callout:** Tabular operators chained left-to-right. Like SPL but with stronger typing and a more formal grammar. Designed for massive telemetry ingestion where schema-on-write enables faster queries.

## 1. How It Works

Kusto Query Language — a read-only, pipe-forward query language used in Azure Data Explorer, Azure Monitor, Microsoft Sentinel, and Application Insights.

- **Model:** Pipe-forward tabular operators: source | where | summarize | extend | render
- **Table in, table out:** Each operator takes a table in and produces a table out
- **Schema-on-write:** Strong typing with schema fixed at ingestion

**Trade-off:** Schema-on-write pays the cost at ingestion so queries stay fast at massive telemetry scale — the opposite bet from schema-on-read systems.

### Visualization (canvas `c1`, 720×300)

Horizontal narrowing-pipeline diagram: five typed-operator boxes with shrinking row counts.

- **Title (bold 14px, centered, `#1a5276`):** "KQL Pipeline: Typed Tabular Operators, Table In → Table Out"
- **Subcaption (13px `#444`, centered):** "typed table flows left to right — each operator narrows it further"
- **Stages (5 boxes, 116×56px, 30px gaps, starting x=10, y=110), each with a bold first line, plain second line, and a bold red count below the box:**
  1. "requests" / "typed table" — count "8.1M rows"
  2. "where" / "ago(1h)" — count "420K rows"
  3. "where" / "success==false" — count "9,300 rows"
  4. "summarize" / "bin 5m, op name" — count "144 rows"
  5. "where, order" / "count > 50" — count "7 rows"
- **Box fills:** first box `rgba(26,82,118,0.35)`; last box solid green `#27ae60` with white text; middle boxes white. All stroked `#1a5276` width 1.5. In-box text `#222` (13px, first line bold). Counts in bold 13px red `#e74c3c`.
- **Pipe arrows between boxes:** orange `#e67e22`, line width 2, filled triangle heads.
- **Bottom annotation (13px `#444`, centered, y≈265):** "Schema-on-write: columns typed at ingestion — every operator is strongly typed, table in → table out"

## 2. Where It Fits

- **Strength:** Readable left-to-right flow (no nested subqueries); rich rendering hints for visualization
- **Strength:** Built-in time-series functions (make-series, series_decompose); scales to petabytes; integrated across the Azure ecosystem
- **Weakness:** Azure-locked — not available outside the Microsoft ecosystem; smaller community than SQL
- **Weakness:** Learning curve for SQL users (different keyword ordering); limited join semantics (innerunique default surprises people); no write/update operations
- **Use case:** Azure Monitor logs, Application Insights telemetry, Microsoft Sentinel (cloud SIEM), IoT and game telemetry, large-scale operational monitoring

*Example: find operations with more than 50 failed requests per 5-minute bin in the last hour.*

Code block (`pre`, in the viz column above the canvas):

```
requests
| where timestamp > ago(1h)
| where success == false
| summarize failed_count = count() by bin(timestamp, 5m), operation_name
| where failed_count > 50
| order by failed_count desc
```

### Visualization (canvas `c2`, 720×300)

Time-binned bar chart with a dashed threshold line.

- **Title (bold 14px, centered, `#1a5276`):** "Query Result: failed_count per 5-min Bin (kept only where > 50)"
- **Plot area:** left=60, right=690, top=50, bottom=250; L-shaped axes in `#999` width 1.
- **Data:** 12 five-minute bins over the last hour, one operation_name: `[12, 18, 25, 40, 62, 95, 140, 110, 78, 54, 30, 20]`; y-scale max 160.
- **Bars:** equal-width slots across the plot with 4px inset each side; bins above threshold 50 filled red `#e74c3c`, bins at/below filled `rgba(26,82,118,0.35)`. Bin value printed above each bar in 13px `#222`.
- **Threshold line:** horizontal dashed orange `#e67e22` (dash 6/4, width 2) at y=50, labeled "threshold: 50" in bold 13px orange near the right end.
- **X labels (13px `#444`):** "-60m" at left, "-30m" at center, "now" at right, below the baseline.
- **Legend note (13px `#444`, centered, y≈288):** "red bins pass the filter and appear in the result; blue bins are dropped"

## Takeaway callout

**Takeaway:** Table in, table out — a formal, strongly-typed pipeline where schema-on-write pays the cost at ingestion so queries stay fast at petabyte scale.

## Regeneration instructions

- **Layout:** backlog kusto-style detail page; the h1 carries the index number "3. Kusto KQL" matching the file index. Structure: h1, `.subtitle`, `.intro` callout, then one `.lang-section` per section (h2 with `2px solid #2980b9` bottom border), each containing a `table.layout` with one `<tr>`: left `td.text-col` (45%) with paragraph/bullets/key-point/example, right `td.viz-col` (55%) with optional `pre` and a canvas. A final standalone `.key-point` takeaway after the sections.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 0.95rem; `.intro` background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `pre` background `#f4f4f4`, padding 12px 16px, radius 4px, 0.85rem. Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, bar fill `rgba(26,82,118,0.35)`, gray text `#444`/`#222`/`#999`.
- In regenerated HTML, any card/page links use .html extensions (this page has none).
