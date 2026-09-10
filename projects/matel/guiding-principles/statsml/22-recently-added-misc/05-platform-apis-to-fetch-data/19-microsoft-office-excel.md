# Microsoft Office / Excel

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, single row under an Overview h2)
**HTML title tag:** Microsoft Office / Excel API — Platform APIs

**Subtitle:** Read and write live Excel workbooks stored in the cloud — Excel itself does the recalculating on the server.

**Verified badge:** Last verified: August 2026

## Overview

## What You Can Get

- Read and write cell ranges, tables, and named ranges in a stored workbook
- Write a formula and read back the freshly calculated result
- Call built-in Excel functions (VLOOKUP, NPV, …) as a remote calculator
- Rendered chart images for reports and emails

**Key-point callout:** **Only Excel gets a live API.** Word and PowerPoint files can be stored, listed, versioned, and downloaded, but their content cannot be edited in place through this API. Changing a document or slide deck server-side means downloading the file and editing it yourself.

## Watch Out For

- A "session" decides whether your edits are saved to the file or thrown away in a scratch sandbox — you choose at the start, cannot change your mind later, and idle sessions expire
- Writing a plain value over a formula erases the formula permanently
- Macros never run through the API, and some features (pivot refresh, very large workbooks) are out of reach
- A person editing the same workbook at the same time can collide with your writes

## Excel as a remote calculator

Code block (`pre`, HTTP request/response):

```
POST .../workbook/functions/vlookup
{
  "lookupValue": "Q2",
  "tableArray": { "address": "Forecast!A2:C3" },
  "colIndexNum": 2,
  "rangeLookup": false
}

Response:
{
  "error": null,
  "value": 511900
}
```

## Session Mode — What Persists Where

### Visualization (canvas `sessionChart`, width 100% responsive × 380)

Three-lane flow diagram: each lane shows a request-mode client box, an arrow (or blocked marker) toward a store box, and a fate description.

- **Title (bold 13px, `#1a5276`, at 12,8):** "Three request modes, three durability outcomes"
- **Lanes (top to bottom), each a rounded client box (radius 6, 52px tall, fill lane color + `1f` hex alpha, stroke lane color 1.5px) containing bold 11.5px title in lane color, 10px monospace `#666` subtitle, italic 9.5px `#888` calc note:**
  1. Title "Persistent session", subtitle `persistChanges: true`, color `#27ae60`, calc note "shared calculation state" — solid arrow (lane color, 2px, triangular head) to a "stored .xlsx" box; fate text: "Writes land in the stored .xlsx and appear in version history"
  2. Title "Non-persistent session", subtitle `persistChanges: false`, color `#e67e22`, calc note "shared calculation state" — dashed gray `#ccc` arrow (dash 4/3) ending in a red `#e74c3c` X (blocked marker) before a "nothing written" box; fate text: "Sandbox only — discarded when the session ends or expires"
  3. Title "No session header", subtitle "stateless per-request", color `#8e44ad`, calc note "no shared state, highest per-call cost" — solid arrow to a "stored .xlsx" box; fate text: "Writes persist, but every call reloads the workbook"
- **Store boxes:** rounded rect (radius 5, 40px tall); when stored — fill `rgba(26,82,118,0.35)`, stroke `#1a5276`, bold 10.5px `#1a3a52` label "stored .xlsx"; when not stored — fill `#f4f4f4`, stroke `#ddd`, label "nothing written" in `#aaa`.
- **Fate text:** 10.5px `#2c3e50`, word-wrapped to the right of the store box. Lanes separated by thin `#eee` rules.
- **Footnotes (italic 10px, bottom left):** red `#e74c3c`: "Sessions expire on idle. Cache the id, but always be prepared to re-create it mid-run."; gray `#888`: "A non-persistent session cannot be promoted to persistent — its results must be read out before it ends."

## Official API References

- [Excel workbook API in Microsoft Graph](https://learn.microsoft.com/en-us/graph/api/resources/excel?view=graph-rest-1.0) — worksheets, ranges, tables, charts, sessions, functions
- [Open XML SDK documentation](https://learn.microsoft.com/en-us/office/open-xml/open-xml-sdk) — editing downloaded .docx / .xlsx / .pptx files directly

## Regeneration instructions

- **Layout:** platform-apis-fetch detail page. h1, `.subtitle` paragraph, `.verified` badge span, h2 "Overview", then one `.obj-table` (full width) with a single `<tr>`: left `<td>` (45%) holds `.section-head` headings ("What You Can Get", "Watch Out For") + bullets + one `.key-point` callout; right `<td>` (55%) holds a `.section-head` + `<pre>` request/response sample, another `.section-head` + the canvas. After the table, h2 "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, padding 30px 40px, white background; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.section-head` bold 0.95em `#1a5276`; `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `pre` — background `#f4f4f4`, padding 14px, radius 6px, 0.8em, ui-monospace; `li`/`p` 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="sessionChart" height="380">`, CSS `display:block; width:100%`; JS resizes on window resize, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes CSS height to 380px, and applies `ctx.setTransform(dpr,0,0,dpr,0,0)` before drawing. Rounded rects drawn with a quadratic-curve helper.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, box fill `rgba(26,82,118,0.35)`, gray text `#888`/`#666`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions.
