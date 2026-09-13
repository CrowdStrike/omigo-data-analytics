# Compression Basics

**Page type:** detail page (tutorial card-sections, two-column layout: text left 50%, canvas right 50%, one table row per section)
**HTML title tag:** Compression Basics

**Subtitle:** A 10GB CSV of orders shrinks to 800MB with gzip and 400MB as Parquet — because repeated values are wasted space, and city names repeat a lot

## The 10GB File That Was Mostly Repetition

**Tags:** core idea (blue), running example (green)

- **The file** — a 10GB orders CSV; the city column says "San Francisco" on millions of rows
- **gzip** — spots repeated byte patterns and stores "repeat what you saw" → 800MB
- **Parquet** — stores each column together and dictionary-codes it → 400MB
- **The principle** — only genuinely new information costs bytes; repetition is nearly free
- **Nothing lost** — decompress either one and every original row comes back exactly

*Example:* The 25× shrink measures how repetitive the data was — not compression magic (illustrative sizes).

**Key point:** Compressed size roughly equals the amount of genuinely new information in the file — repetition costs a compressor almost nothing.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart comparing three file sizes.

- **Title (bold 15px, `#1a5276`, top center):** "Same rows, three sizes (illustrative)"
- **Bars** (120px wide, evenly gapped, value scale max 10.5 GB, min bar height 8px, baseline axis in gray `#6b7280`):
  - "CSV (raw)" — 10 GB, gray `#6b7280`, value label "10 GB"
  - "CSV + gzip" — 0.8 GB, blue `#2a78d6`, value label "800 MB"
  - "Parquet" — 0.4 GB, green `#008300`, value label "400 MB"
- **Bar style:** fill is the bar color at 0.35 alpha, stroke is the solid color width 2; bold 14px value label above each bar in bar color; 12px bar name below the baseline in `#2c3e50`.
- **Annotations:** bottom center in bold 13px green `#008300`: "same rows, same numbers — 25× smaller"; top right in 12px orange `#d95926`: "the shrink = how repetitive the data was".
- **Padding:** top 60, bottom 56, left 90, right 40.

## Dictionary Encoding by Hand

**Tags:** worked example (green)

- **The column** — 1,000,000 city values, ~11 bytes each → 11,000,000 bytes as raw text
- **The dictionary** — only 50 distinct cities; list each once ≈ 550 bytes
- **The codes** — replace every value with a 1-byte number pointing into the dictionary
- **New size** — 1,000,000 code bytes + 550 dictionary bytes ≈ 1MB: 11× smaller, lossless
- **Bonus** — sort the column and runs appear: "code 7 × 214,000" stores in a few bytes

*Example:* "San Francisco" (13 bytes) becomes the single byte 7, one million times over.

**Key point:** You can redo this with a pen — each distinct value once, plus one small code per row. That is most of Parquet's trick.

### Visualization (canvas `c2`, 720×300)

Two-panel before/after diagram of dictionary encoding, split by a vertical dashed gray divider (`#bdc3c7`, dash 4/3) at x=330.

- **Title (bold 15px, `#1a5276`, top center):** "Pay for \"San Francisco\" once, then point at it"
- **Left panel** — label (bold 12px gray `#6b7280`): "raw column: 1,000,000 strings"; six stacked value boxes (200×20px, 11px monospace text) with values: San Francisco, San Francisco, Austin, San Francisco, Boston, San Francisco. "San Francisco" boxes highlighted orange `#d95926` (fill at 0.12 alpha, orange stroke and text); others `#f8f9fa` fill, `#c8ced6` stroke, `#2c3e50` text. Below: gray 12px "… ×1,000,000 rows, ~11 bytes each" and bold 13px `#2c3e50` "= 11,000,000 bytes".
- **Right panel top** — label: "dictionary (50 entries ≈ 550 B)"; four stacked boxes (180×18px, 11px monospace): "0: Austin", "1: Boston", "2: Chicago", "7: San Francisco" — the last highlighted green `#008300` (fill 0.12 alpha); below the list, gray 11px "(… 46 more)".
- **Right panel bottom** — label (bold 12px gray): "code stream: 1 byte per row"; seven small boxes (28×22px, bold 12px monospace) with codes: 7, 7, 0, 7, 1, 7, … — "7" boxes highlighted green (fill 0.15 alpha, green stroke/text). Below: bold 13px `#2c3e50` "= 1,000,550 bytes".
- **Takeaway (bottom center, bold 14px green `#008300`):** "11 MB → 1 MB, losslessly: 11× smaller"

## Why Columnar Files Compress Better

**Tags:** where it's used (blue), rule of thumb (green)

- **Row storage** — CSV interleaves id, city, price, date: neighbors are dissimilar
- **Column storage** — Parquet groups all cities together, all prices together
- **Similar neighbors** — a run of city names is far more repetitive than mixed rows
- **Read less** — a query touching 2 of 40 columns reads ~5% of a columnar file
- **CPU vs IO** — decompression costs CPU, but disks and networks are slower: compressed wins

*Example:* Scanning the 400MB Parquet from cloud storage beats the 10GB CSV even after paying to decompress (illustrative).

**Key point:** Compression trades cheap CPU for expensive IO — on big files that trade is almost always worth taking.

### Visualization (canvas `c3`, 720×300)

Two-panel diagram (vertical dashed gray divider `#bdc3c7`, dash 4/3, at x=430): row-vs-column storage layout on the left, bytes-read bar comparison on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Row files mix values; column files group them"
- **Cell colors** (fill at 0.30 alpha, solid stroke): id gray `#6b7280`, city green `#008300`, price blue `#2a78d6`, date violet `#4a3aa7`.
- **Left top** — label (bold 12px gray): "row storage (CSV): dissimilar neighbors"; a 4×4 grid of wide cells (82×18px) with the column kinds interleaved per row (id, city, price, date across each row).
- **Left bottom** — label: "column storage (Parquet): similar neighbors"; a single horizontal strip of 16 narrow cells (18×18px) grouped by kind — 4 id, then 4 city, then 4 price, then 4 date.
- **Legend** — small swatches (11×11px, fill 0.5 alpha) with 11px labels: id, city, price, date.
- **Right** — label (bold 12px gray): "bytes read: SELECT city, price"; sublabel (11px gray): "(2 of 40 columns, illustrative)". Two horizontal bars (26px tall, length by log10 of value scaled to 200px):
  - "CSV" — 10000 MB, gray `#6b7280`, value label "10 GB (all of it)"
  - "Parquet" — 20 MB, green `#008300`, value label "~20 MB"
  - Note below in 11px gray: "(bar length on log scale)".
- **Takeaway (bottom center, bold 13px green):** "columnar reads only the columns the query names — CSV must read every byte"

## Lossless vs Lossy, and What Won't Compress

**Tags:** rule of thumb (green), common mistake (orange)

- **Lossless** — gzip, zip, Parquet, PNG: decompress and every original byte comes back
- **Lossy** — JPEG, MP3: detail is thrown away forever to shrink further
- **The rule** — data files are always lossless; lossy is only for eyes and ears
- **No double dip** — gzipping a gzip saves ~nothing; the repetition is already gone
- **Varies by content** — repeated categories ~25×, text ~3×, random or encrypted ~1×

*Example:* A pipeline stage that "compressed twice" was pure CPU cost — the second pass saved under 1% (illustrative).

**Key point:** If a decompressed file could differ from the original by even one byte, that format has no place in a data pipeline.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of gzip compression ratio by content type.

- **Title (bold 15px, `#1a5276`, top center):** "How much gzip shrinks different content (illustrative)"
- **Bars** (30px tall, left-aligned at x=190 against a vertical gray axis, scale max 26×, min bar width 6px; fill at 0.35 alpha, solid stroke width 1.5; row name right-aligned left of axis in 12px `#2c3e50`; ratio value in bold 13px bar color right of bar):
  - "repeated categories" — 25×, green `#008300`
  - "server logs" — 10×, aqua `#199e70`
  - "English text" — 3×, blue `#2a78d6`
  - "JPEG photos" — 1.05×, yellow `#c98500`
  - "encrypted / random" — 1×, orange `#d95926`
- **Annotation (bottom right, bold 13px orange `#d95926`):** "no repetition left → nothing to remove"
- **Padding:** top 56, bottom 42, left 190, right 110.

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference style): `<h1>` (no index number), `.subtitle`, then four `.card-section` blocks each with an `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` line, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Bullets 0.92rem, `li b` in `#1a5276`; inline `code` in ui-monospace on `#f4f6f8`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). A `tintOf(hex, alpha)` helper produces rgba fills from hex strokes.
- **Chart palette (`P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
