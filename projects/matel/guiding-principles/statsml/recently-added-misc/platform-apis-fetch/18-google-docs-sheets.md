# Google Docs & Sheets

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, single row under an Overview h2)
**HTML title tag:** Google Docs & Sheets API — Platform APIs

**Subtitle:** Read and edit the text in Google Docs and the cells in Google Sheets from a program.

**Verified badge:** Last verified: August 2026

## Overview

## What You Can Get

- The full text and structure of a document, including tracked-change suggestions
- Cell values, formulas, and formatting from a spreadsheet
- Edits from a program — insert or replace text, write cells, add sheets and charts
- Many edits bundled into one call, applied all-or-nothing

**Key-point callout:** **How you ask determines what you get back.** The same spreadsheet cell can answer as a display string ("$482,300"), a plain number, or the formula behind it — and the default read mode returns the display string, which loses the most information. Pick the read mode deliberately.

## Watch Out For

- There is no "give me the plain text" call for a document — you reassemble the text yourself, and naive code drops tables and footnotes
- Unaccepted suggestions can hold text no reader ever sees — scanning only the visible document misses real content
- Empty trailing cells are trimmed from results, so a "rectangular" range can come back ragged
- When rate-limited, slow down and retry with increasing delays — and batch many small edits into one call instead of looping

## One cell, three stored layers

Code block (`pre`, JSON):

```
{
  "userEnteredValue": { "formulaValue": "=SUM(Detail!B2:B40)" },
  "effectiveValue":   { "numberValue": 482300 },
  "formattedValue":   "$482,300"
}
```

## Same Cell, Four Different Answers

### Visualization (canvas `renderChart`, width 100% responsive × 360)

Fan-out diagram: one source cell box connected to four read-mode chips, each with an arrow to its output text.

- **Title (bold 13px, `#1a5276`, at 12,8):** "One cell, four read modes — Sheet1!B2"
- **Source cell box:** rounded rect (radius 6) at 12,32, up to 300px wide × 34 tall, fill `rgba(26,82,118,0.35)`, stroke `#1a5276` 1.5px; monospace bold 11.5px text in `#1a3a52`: "B2  =SUM(Detail!B2:B40)  →  482300".
- **Connectors:** light gray `#ccc` 1px elbow lines from below the source box down to each mode chip.
- **Four rows** (rounded mode chip on the left — fill mode color + `22` hex alpha, stroke mode color 1.5px, bold 10.5px label; colored arrow with triangular head to the output; output value in 11.5px monospace `#2c3e50` with an italic 10px gray `#888` line "type — note" beneath):
  1. Mode "FORMATTED_VALUE (default)", color `#e67e22` → output `"$482,300"`; type "string, locale-dependent"; note "Display string. Breaks numeric parsing."
  2. Mode "UNFORMATTED_VALUE", color `#27ae60` → output `482300`; type "number"; note "Computed result, typed. Usually what you want."
  3. Mode "FORMULA", color `#1a5276` → output `"=SUM(Detail!B2:B40)"`; type "string, the formula"; note "What was typed. Needed for formula audits."
  4. Mode "spreadsheets.get (grid)", color `#8e44ad` → output `userEnteredValue + effectiveValue + format`; type "full cell object"; note "Everything, at much higher quota cost."
- **Footnote (italic 10px, red `#e74c3c`, bottom left):** "The default mode is the one that loses the most information."

## Official API References

- [Google Docs API](https://developers.google.com/docs/api) — document structure, batch edits, suggestions
- [Google Sheets API](https://developers.google.com/sheets/api) — spreadsheets, values, and read modes

## Regeneration instructions

- **Layout:** platform-apis-fetch detail page. h1, `.subtitle` paragraph, `.verified` badge span, h2 "Overview", then one `.obj-table` (full width) with a single `<tr>`: left `<td>` (45%) holds `.section-head` headings ("What You Can Get", "Watch Out For") + bullets + one `.key-point` callout; right `<td>` (55%) holds a `.section-head` + `<pre>` JSON sample, another `.section-head` + the canvas. After the table, h2 "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, padding 30px 40px, white background; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.section-head` bold 0.95em `#1a5276`; `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `pre` — background `#f4f4f4`, padding 14px, radius 6px, 0.8em, ui-monospace; `li`/`p` 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="renderChart" height="360">`, CSS `display:block; width:100%`; JS resizes on window resize, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes CSS height to 360px, and applies `ctx.setTransform(dpr,0,0,dpr,0,0)` before drawing. Rounded rects drawn with a quadratic-curve helper.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, box fill `rgba(26,82,118,0.35)`, gray text `#888`/`#666`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions.
