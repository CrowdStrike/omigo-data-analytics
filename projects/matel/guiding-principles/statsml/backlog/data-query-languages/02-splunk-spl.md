# 2. Splunk SPL

**Page type:** detail page (backlog kusto-style 2-col text/viz layout: text left 45%, viz right 55%, one layout table per section)
**HTML title tag:** 2. Splunk SPL

**Subtitle:** Pipe-based, time-ordered — narrow a stream stage by stage

**Intro callout:** SQL assumes you are combining tables. Log analysis assumes you are narrowing a stream. Each stage takes previous output and reduces it further — search, filter, transform, present.

## 1. How It Works

Search Processing Language — Splunk's proprietary query language for searching, filtering, transforming, and visualizing machine-generated data (logs, events, metrics).

- **Model:** Pipeline of commands chained with pipes: search | where | stats | eval | sort
- **Time:** First-class citizen — time-bucketing built in (earliest, latest, span)
- **Schema-on-read:** Fields are extracted at query time, not ingestion time — no upfront schema

**Trade-off:** Search-time extraction gives flexibility (no schema management at ingestion), but performance degrades on non-indexed fields.

### Visualization (canvas `c1`, 720×300)

Horizontal narrowing-pipeline diagram: five stage boxes with shrinking event counts.

- **Title (bold 14px, centered, `#1a5276`):** "SPL Pipeline: Each Stage Narrows the Event Stream"
- **Subcaption (13px `#444`, centered):** "event stream flows left to right — each pipe reduces it further"
- **Stages (5 boxes, 116×56px, 30px gaps, starting x=10, y=110), each with a bold first line, plain second line, and a bold red count below the box:**
  1. "index=web" / "raw events" — count "12.4M events"
  2. "status=5*" / "search filter" — count "18,200 events"
  3. "stats count" / "by status, uri" — count "312 rows"
  4. "where" / "count > 100" — count "24 rows"
  5. "sort, head" / "top results" — count "20 rows"
- **Box fills:** first box `rgba(26,82,118,0.35)`; last box solid green `#27ae60` with white text; middle boxes white. All stroked `#1a5276` width 1.5. In-box text `#222` (13px, first line bold). Counts in bold 13px red `#e74c3c`.
- **Pipe arrows between boxes:** orange `#e67e22`, line width 2, filled triangle heads.
- **Bottom annotation (13px `#444`, centered, y≈265):** "Schema-on-read: fields (status, uri_path) extracted at search time — no schema at ingestion"

## 2. Where It Fits

- **Strength:** Intuitive log exploration — each pipe stage narrows results visibly, with interactive drill-down in the UI
- **Strength:** Search-time field extraction plus a strong ecosystem of apps and add-ons
- **Weakness:** No real joins — only lookup tables (static enrichment); limited aggregation flexibility vs SQL
- **Weakness:** Expensive at scale (license by daily ingestion volume); proprietary, no portability
- **Use case:** Security operations (SIEM — threat detection, alert correlation), IT ops monitoring, app log analysis, compliance auditing, incident root-cause investigation

*Example: find URI paths with more than 100 server errors, top 20 by count.*

Code block (`pre`, in the viz column above the canvas):

```
index=web sourcetype=access_combined status=5*
| stats count by status, uri_path
| where count > 100
| sort -count
| head 20
```

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of the example query result.

- **Title (bold 14px, centered, `#1a5276`):** "Query Result: Top URI Paths Returning 5xx (count > 100, sorted desc)"
- **Data:** URI paths `['/checkout', '/api/cart', '/search', '/api/login', '/api/orders', '/static/js']`, status codes `['503', '500', '502', '500', '500', '503']`, counts `[1840, 1210, 890, 640, 410, 285]`.
- **Bars:** 6 bars, width 74px, gap 40px, centered horizontally; baseline y=248, max bar height 190px scaled to max 2000. First two bars filled red `#e74c3c`; the rest `rgba(26,82,118,0.35)`.
- **Labels:** count above each bar in bold 13px `#222`; URI path below baseline in 13px `#222`; status code below that in `#444`.

## Takeaway callout

**Takeaway:** Log analysis is narrowing a stream, not combining tables — each pipe stage takes the previous output and reduces it further.

## Regeneration instructions

- **Layout:** backlog kusto-style detail page; the h1 carries the index number "2. Splunk SPL" matching the file index. Structure: h1, `.subtitle`, `.intro` callout, then one `.lang-section` per section (h2 with `2px solid #2980b9` bottom border), each containing a `table.layout` with one `<tr>`: left `td.text-col` (45%) with paragraph/bullets/key-point/example, right `td.viz-col` (55%) with optional `pre` and a canvas. A final standalone `.key-point` takeaway after the sections.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 0.95rem; `.intro` background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `pre` background `#f4f4f4`, padding 12px 16px, radius 4px, 0.85rem. Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, bar fill `rgba(26,82,118,0.35)`, gray text `#444`/`#222`.
- In regenerated HTML, any card/page links use .html extensions (this page has none).
