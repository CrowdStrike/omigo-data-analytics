# Asana & monday.com APIs

**Page type:** detail page (two-column obj-table layout with bordered cells: text left 45%, payload + canvas right 55%, one row; no "Overview" h2 above the table)
**HTML title tag:** Asana & monday.com APIs — Platform APIs

**Subtitle:** Pull tasks, projects, and boards — including their custom columns — out of the two most common work-management tools.

**Verified badge:** Last verified: August 2026

## Main table

### Left column

**What you can get**

- Tasks and projects with due dates, assignees, and completion times (Asana)
- Boards and items with all their columns (monday.com)
- Per-task activity history — comments and change events
- The customer-defined custom columns, where the real business data usually lives

**Key-point callout:** These feeds record **declared state, not performed work**. "Done" means somebody clicked Done — often hours or days after the work actually finished. Duration metrics built on these timestamps mix real work with bookkeeping delay, and the two cannot be separated afterwards.

**Watch out for**

- Asana returns almost nothing by default — you must ask for each field explicitly, and there is no error when you forget
- monday.com values arrive as JSON hidden inside a text string; the human-readable label next to it is not safe to compute on
- The "same" column (say, Priority) has a different id on every board or project — map ids explicitly, never join on display names
- History windows are limited and plan-dependent, and deleted or archived items vanish from later reads

### Right column

**Payload note (italic, #666):** monday.com item (abbreviated) — each column value is JSON inside a string, requiring a second parse.

Payload block (`pre.payload`, monospace, blue left border):

```
{
  "id": "7654399001",
  "name": "Migrate ingestion job",
  "column_values": [
    { "id": "status", "type": "status",
      "text": "Working on it",
      "value": "{\"index\":0}" },
    { "id": "date4", "type": "date",
      "text": "2026-08-29",
      "value": "{\"date\":\"2026-08-29\"}" }
  ]
}
```

### Visualization (canvas `c1`, responsive width × 380)

Side-by-side dual-panel horizontal bar chart comparing the two APIs' cost models — relative cost (0–100 scale) of adding one more thing to a read.

- **Title (bold 14px, #1a5276, centered):** "Two cost models for the same extract".
- **Subtitle (italic 11px, #666, centered):** "relative cost of adding one more thing to the read — illustrative, not published figures".
- **Left panel — title (bold 11px, #1a5276):** "Asana — cost is REQUEST COUNT". Four horizontal bars (height 22, 40px gap), each with a monospace 10.5px #2c3e50 label above and an italic 10px #777 note below; light track behind each bar in rgba(26,82,118,0.10):
  - "GET /projects" — cost 1, note "1 request"
  - "GET /tasks?project=…" — cost 1, note "1 request per page"
  - "+ opt_fields (wide response)" — cost 1, note "same count, larger payload"
  - "GET /tasks/{gid}/stories" — cost 100, note "1 request per task — the real cost"
- **Right panel — title (bold 11px, #27ae60):** "monday.com — cost is QUERY COMPLEXITY". Four bars in the same style:
  - "boards { name }" — cost 1, note "trivial complexity"
  - "+ items_page(limit:100)" — cost 30, note "charged per item requested"
  - "+ column_values" — cost 55, note "multiplies by items"
  - "+ updates { body }" — cost 100, note "nesting compounds"
- **Bar colors:** cost ≥ 100 → red #e74c3c; cost ≥ 50 → orange #e67e22; otherwise the panel color (#1a5276 left, #27ae60 right); bars drawn at 85% alpha; bar width = (cost/100) × panel width, min 3px.
- **Divider:** vertical #e5e5e5 line at mid-width between the panels.
- **Caption (italic 11px, #e74c3c, centered at bottom):** "Red bars are where a naive extract stalls: per-task fan-out on one side, nested fields on the other."

## Official API References

- [Asana Developer Docs](https://developers.asana.com/docs) — REST API guides, authentication, pagination and webhooks
- [monday.com API Reference](https://developer.monday.com/api-reference/docs) — GraphQL API docs: boards, items, column values, activity logs

## Regeneration instructions

- **Layout:** single-page detail doc: h1, `.subtitle` paragraph, `.verified` badge span, then directly one `.obj-table` (no h2 above it; one `<tr>`: left `<td>` 45% with `.obj-title` headings + bullet lists + one `.key-point` callout; right `<td>` 55%, text-align center, with a `.payload-note`, a `pre.payload` block, and the canvas), then `h2` "Official API References" with a link list. No nav bar, no back/home links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text #2c3e50, padding 30px 40px, white background. h1 1.8rem #1a5276; `.subtitle` #666 1.05em; `.verified` badge — plain variant: color #888, border 1px solid #e0e0e0, padding 2px 10px, radius 4px, 0.8em; h2 1.3em #1a5276 with 2px solid #2980b9 bottom border; `.obj-title` bold #1a5276 1.1em; `.obj-table td` border 1px solid #e0e0e0, padding 16px, vertical-align top; `.payload` background #f8f9fa, left border 3px solid #1a5276, ui-monospace 0.78em, left-aligned; `.payload-note` 0.82em #666 italic left-aligned; `.key-point` background #f8f9fa, left border 3px solid #1a5276, padding 10px 14px, 0.93em; li 0.93em; links #1a5276.
- **Canvas:** `<canvas id="c1" height="380">`, CSS `width: 100%`; draw() sizes backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redraw on window resize; panels each take (width − 36)/2 with 12px outer margins.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, rgba(26,82,118,0.10) bar track, grays #666/#777/#e5e5e5.
