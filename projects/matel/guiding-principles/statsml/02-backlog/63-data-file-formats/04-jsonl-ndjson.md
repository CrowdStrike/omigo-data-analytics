# JSONL / NDJSON

**Page type:** detail page (single card-section with two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** JSONL / NDJSON

**Subtitle:** One JSON object per line.

## Callout (intro)

**Core trade-off:** Appendable, splittable, and streamable — but there is no root document structure, no way to express file-level metadata.

## How It Works

Each line is a complete, self-contained JSON object. No wrapping array, no commas between records — the newline is the only separator. JSONL and NDJSON are the same format with different names.

- **Schema:** None — fields can vary line to line
- **Layout:** Row-oriented, newline-delimited
- **Property:** Appendable, splittable, tail-able while being written
- **Best for:** Logs, event streams, ML training data

**How it's parsed:** Read a line, parse it as JSON, repeat. Each line is independently valid, so a stream processor never holds more than one record in memory, a distributed job can split the file at any newline boundary and hand each chunk to a different worker, and `cat` concatenates two files into a valid third. You can `grep` for errors, `wc -l` for count, `tail -f` while another process writes. A corrupt line loses one record, not the file.

**Trade-off:** No root structure means no place for file-level metadata, no way to express relationships between records, and no multi-line pretty-printing. The format is exactly as smart as a stack of independent records. *(styled as key-point callout, red left border)*

*Example: Structured log shipping, click-streams, one-training-example-per-line datasets, Kafka consumer dumps, incremental ETL.*

### Visualization (canvas `c1`, 720×300)

Diagram of four JSONL record lines rendered as boxes, with split-point and append annotations.

- **Title (bold 14px, top center, `#1a5276`):** "JSONL — Every Line Stands Alone".
- **Record rows:** starting at x=40, y=55, one 480px-wide box per line, 30px row height (22px box height), `#ddd` 1px borders. Lines (12px monospace):
  1. `{"event":"click","user":12345,"page":"/pricing"}`
  2. `{"event":"purchase","user":12345,"amount":49.99}`
  3. `{"event":"click","user":67890,"page":"/docs"}`
  4. `{"event":"error","user":null,"code":500}`
  - Rows 1–3: fill `rgba(26,82,118,0.06)`, text `#333`. Row 4 (error record): fill `rgba(231,76,60,0.08)`, text `#e74c3c`.
  - Each row followed to the right of the box by a bold 11px monospace `\n` marker in `#e67e22`.
- **Split annotation:** green (`#27ae60`) dashed horizontal line (dash 4/3, width 1.5) between rows 2 and 3, spanning slightly past the boxes; 11px green label to its right: "← split here: worker 1 above, worker 2 below". Green labels "worker 1" (beside rows 1–2) and "worker 2" (beside rows 3–4).
- **Append slot:** dashed `#1a5276` outline box (same 480px width) below row 4, containing italic 12px `#1a5276` text: "next record appends here — no rewrite, tail -f while writing".
- **Bottom note (centered, `#e74c3c`, 11px):** "a corrupt line loses one record, not the file — unlike a JSON array, where one bad byte kills everything after it".

## Regeneration instructions

- **Layout:** single-page detail doc: h1 with `2px solid #2980b9` bottom border, `.subtitle` paragraph, `.intro-callout` div, then one `.card-section` containing an h2 (also `2px solid #2980b9` bottom border) and a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) holding intro paragraph, `<ul>` bullets, "How it's parsed" paragraph, `.key-point` div, `.example` paragraph; right `<td class="viz-col">` (55%) holding the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `.intro-callout` background `#f8f9fa`, left border `3px solid #2980b9`, padding 10px 14px, 0.93rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `code` background `#f0f4f8`, 2px 6px padding, 3px radius. Canvas `width: 100%`, `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; bar fill `rgba(26,82,118,0.06)` / `rgba(231,76,60,0.08)`.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
