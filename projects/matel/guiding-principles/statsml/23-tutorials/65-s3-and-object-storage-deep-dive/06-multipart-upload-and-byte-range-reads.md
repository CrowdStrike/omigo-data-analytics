# Multipart Upload &amp; Byte-Range Reads

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Multipart Upload &amp; Byte-Range Reads

**Subtitle:** A large file goes up as many numbered pieces, and the same idea in reverse lets a reader take a few bytes out of the middle instead of downloading the whole thing

## Why a Big File Cannot Go Up in One Write

**Tags:** `core idea` (blue), `three steps` (green), `all or nothing` (orange)

- **The file** — one nightly export of 200 GiB, stored in the bucket under a single name
- **One write is capped** — a single write request can carry at most 5 GB of content
- **So you split it** — a big file goes up as many separately numbered pieces instead
- **Step 1, start** — you open an upload and get back an ID that names this batch
- **Step 2, send pieces** — the numbered pieces go up in any order, and in parallel
- **Step 3, finish** — you send the list of pieces and the whole file appears at once
- **Nothing partial shows** — before that final step a read of the name finds nothing
- **Or cancel** — cancelling the upload throws away every piece that was already sent

*Example (illustrative):* Alice's export goes up as 6,400 pieces over half an hour, and the file does not exist until the last step returns.

**Key point:** This is not a gradual write. The pieces are staged first, then one final step publishes the whole file at once.

### Visualization (canvas `c1`, 720×300)

Three-stage flow: start on the left, a fan of pieces sent in parallel in the middle, finish on the right, with a strip underneath that flips from "no such file" to "file visible" only at the finish.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "The File Exists Only After the Final Step".
- **Geometry constants:** `startX=36, startW=134` (right edge 170); `pillX=210, pillW=250` (right edge 460, centre 335); `endX=536, endW=148` (right edge 684). Both stage boxes `y=64, h=54`, so their centre line is y=91. All fan-line endpoints are derived from these constants, never hardcoded.
- **Stage boxes (rounded 8px, 2px border, h=54):**
  - Start: fill `rgba(42,120,214,0.14)`, border `#2a78d6`; bold 12.5px `#2a78d6` two lines "start the" / "upload" on baselines y=85 and y=103; 12px `#6b7280` caption centred under the box at y=136 "you get an upload ID".
  - Finish: fill `rgba(0,131,0,0.16)`, border `#008300`; bold 12.5px `#008300` two lines "finish the" / "upload"; caption "you send the piece list".
- **Piece pills (five, tops y = 58, 88, 118, 148, 178, h=22, 5px radius):** fill `rgba(201,133,0,0.14)`, 1.5px `#c98500` border, 11.5px monospace `#2c3e50` centred at x=335 — "piece 1 · 32 MiB", "piece 2 · 32 MiB", "piece 3 · 32 MiB", "…", "piece 6,400 · 32 MiB".
- **Fan lines (1.5px `#6b7280`):** from (170, 91) to each pill's left edge centre, and from each pill's right edge centre to (536, 91); arrowheads only on the first and last line of each fan.
- **Parallel label (bold 12px `#c98500`, centered at (335, 206)):** "6,400 pieces, sent at the same time, each retried alone".
- **Visibility strip (y=228, h=30, 6px radius), split at `endX`:** left segment x=36 to 536, fill `rgba(107,114,128,0.12)`, 1.5px `#6b7280`, bold 12px `#6b7280` centered at x=286 "a read of this name finds no such file"; right segment x=536 to 684, fill `rgba(0,131,0,0.18)`, 2px `#008300`, bold 11.5px `#008300` centered at x=610 "file visible".
- **Annotation (bold 13px `#4a3aa7`, centered at y=272):** "one moment of publication — nothing is ever half-written".
- **Caption (12px `#444`, right-aligned at (708, 294)):** "sizes illustrative; the pieces become one file only at the final step".

## Choosing the Piece Size: 204,800 MiB Divided by 10,000

**Tags:** `worked example` (blue), `arithmetic` (green), `hard limit` (orange)

- **Two limits** — an upload holds at most 10,000 pieces, each of them 5 MiB or more
- **The file in MiB** — 200 GiB × 1,024 = 204,800 MiB of content to divide into pieces
- **The rule** — the smallest piece allowed here is 204,800 ÷ 10,000 = 20.48 MiB
- **A copied default fails** — 5 MiB pieces would need 40,960 of them, far too many
- **How far over** — 40,960 − 10,000 = 30,960 pieces more than the limit allows
- **A working choice** — 32 MiB pieces give 204,800 ÷ 32 = 6,400, inside the limit
- **Room to grow** — 32 MiB × 10,000 pieces covers any file up to 312.5 GiB in size
- **Not a habit** — piece size follows the file size, so it has to scale as files grow

*Example (illustrative):* A 5 MiB piece size works for a 1 GiB file (205 pieces) and fails at 200 GiB (40,960 pieces).

**Key point:** Piece size is arithmetic, not habit. Divide the file size by 10,000, round up to something convenient, and the limit stops being a surprise.

### Visualization (canvas `c2`, 720×300)

Bar chart of piece count for five candidate piece sizes on a log axis, with the 10,000-piece limit drawn as a red line and the 20.48 MiB minimum annotated.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "Piece Count for a 204,800 MiB File".
- **Plot box:** baseline y=246, top y=58, gridline span x=62 to x=690; log10 scale over two decades, `y(n) = 246 - (log10(n / 1000) / 2) × 188`, so y(1,000)=246, y(10,000)=152, y(100,000)=58.
- **Gridlines (1px `#e5e9ef`) with 12px `#6b7280` right-aligned labels at x=56:** 1,000 / 10,000 / 100,000. Baseline axis line 1.5px `#1a5276`.
- **Bars (five, width 62, left edge `barX(i) = 90 + i × 127`, centre `barX(i) + 31`):** piece count is computed in JS as `Math.round(204800 / size)`, never hardcoded; bar fill drawn at 0.85 alpha.
  - 5 MiB → 40,960 — fill `#d95926` (over the limit)
  - 10 MiB → 20,480 — fill `#d95926` (over the limit)
  - 20.48 MiB → 10,000 — fill `#c98500` (exactly at the limit)
  - 32 MiB → 6,400 — fill `#008300`, 2.5px `#1a5276` outline (the chosen setting)
  - 64 MiB → 3,200 — fill `#2a78d6`
- **Value labels (bold 12px in the bar colour, centered 8px above each bar):** the same computed counts, comma-formatted.
- **X labels (12px `#2c3e50`, centered at y=264):** "5 MiB", "10 MiB", "20.48 MiB", "32 MiB", "64 MiB"; axis title 12px `#6b7280` centered at (376, 280) "piece size".
- **Limit line:** 2.5px `#e74c3c` dashed (6/4) horizontal line at y(10,000)=152 across the gridline span; bold 12.5px `#e74c3c` "10,000-piece limit" right-aligned at (686, 144).
- **Minimum annotation (bold 12px `#c98500`, left-aligned at (330, 80) and (330, 97)):** "204,800 ÷ 10,000 = 20.48 MiB" and "smaller pieces are not allowed here".
- **Chosen marker (bold 11.5px white, centered inside the 32 MiB bar at y=196):** "chosen".
- **Caption (12px `#444`, right-aligned at (708, 294)):** "200 GiB illustrative; piece counts computed, the 10,000-piece limit documented".

## Reading Back Only the Bytes You Need

**Tags:** `range read` (blue), `columnar files` (green), `common mistake` (red)

- **Range read** — you can ask for one byte range of a file instead of the whole thing
- **You pay for the slice** — one request charge plus only the bytes you actually read
- **Two reads, not one** — read the small index first, then only the blocks you need
- **The index** — a 64 KiB read at the end of the file locates every block inside it
- **A real query** — 8 MiB from each of four blocks, plus the index, is 32.06 MiB read
- **The share read** — 32.06 of 204,800 MiB works out at 0.0157% of the stored file
- **The cost gap** — pulling the whole file costs $18.00; the slice costs about $0.003
- **Many readers** — separate workers can each read a different range at the same time

*Example (illustrative):* Bob's query over the 200 GiB export reads 32.06 MiB — a 64 KiB index plus four 8 MiB blocks — and leaves the other 1,596 blocks untouched.

**Common mistake:** Downloading a whole file to look at a few columns. If the format keeps an index, two small reads answer the question and the rest of the file never moves.

### Visualization (canvas `c3`, 720×300)

The file drawn as one long bar of 1,600 blocks, with four read blocks and the index highlighted, then the share-read figure and a two-box cost comparison.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "One Range Read Takes 32.06 MiB From a 204,800 MiB File" — the 32.06 is computed as `4 × 8 + 64/1024`.
- **File bar (x=44, y=78, w=632, h=46, 6px radius):** fill `rgba(42,120,214,0.10)`, 1.5px `#2a78d6`; byte offsets map with `fx(mib) = 44 + (mib / 204800) × 632`.
- **Bar label (bold 12px `#2a78d6`, centered at (360, 68)):** "200 GiB file · 1,600 blocks of 128 MiB".
- **Block ticks:** 1px `#e5e9ef` vertical lines at 40 equal divisions inside the bar (632/40 = 15.8 px, each division standing in for 40 blocks); 12px `#6b7280` note left-aligned at (44, 142) "each tick = 40 blocks".
- **Four read blocks (fill `#008300` at 0.9 alpha, w=8, full bar height):** block numbers 180, 520, 940 and 1,410, drawn at `fx(n × 128)` — 115.1, 249.4, 415.3, 601.0, computed in JS from the block numbers.
- **Read label (bold 12px `#008300`, centered at (360, 142)):** "4 blocks × 8 MiB = 32 MiB".
- **Index block (fill `#d55181`, w=8, at the bar's right end, x = 44 + 632 − 8 = 668):** bold 12px `#d55181` "64 KiB index" right-aligned at (676, 68).
- **Untouched note (12px `#6b7280`, centered at (360, 164)):** "the rest of the file is never transferred".
- **Share annotation (bold 14px `#4a3aa7`, centered at (360, 192)):** "32.06 of 204,800 MiB = 0.0157% of the file" — the percentage computed at render time from 32.0625 / 204,800.
- **Cost boxes (y=214, h=32, 6px radius):** left x=52 w=300, fill `rgba(217,89,38,0.12)`, 2px `#d95926`, bold 12px `#d95926` centered at x=202 "whole file: 200 GiB × $0.09 = $18.00"; right x=372 w=300, fill `rgba(0,131,0,0.14)`, 2px `#008300`, bold 12px `#008300` centered at x=522 "range read: 0.031 GiB × $0.09 ≈ $0.003". Both dollar figures computed in JS.
- **Side note (12px `#6b7280`, left-aligned at (52, 272)):** "one small read finds the blocks, one read fetches them".
- **Caption (12px `#444`, right-aligned at (708, 292)):** "sizes and $0.09/GiB illustrative; the share of the file read is computed".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `01-buckets-keys-and-the-flat-namespace.html` and `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section three's callout uses the label "Common mistake:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead` and `rgba` as in page 11.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine limits and risk: only the 10,000-piece limit line in `c2`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly **75–90 characters** including the bold lead term, so each holds one line at the 50/50 split without wrapping while still carrying a full clause. Two guards, both learned the hard way:
  - An earlier pass clipped them to ~55–70 characters and it was **rejected for compromising quality**. Do not shorten them to stubs again.
  - Equally, do **not** pad them back out to the folder default of ~90–100 characters. This page sits deliberately below that.
- **Register and vocabulary.** Plain simple technical English for a first-time reader, with far less API surface than the earlier draft: say "start / send pieces / finish the upload", not CreateMultipartUpload / UploadPart / CompleteMultipartUpload; say "cancel", not AbortMultipartUpload; say "a read of the name finds nothing", not "a GET returns 404"; say "piece", not "part … with its ETag"; say "you can ask for one byte range", not "a `Range` header returns 206 Partial Content"; say "index" and "block", not Parquet footer and row group. **No analogies, no invented scenes, no story framing.** Exact call names, header names and product feature names belong on the reference pages, not here. Fewer details overall — this is a tutorial, not a reference course — but shorten the wording, never drop a real fact.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No parallelism-and-orphaned-parts section.** A fourth section covered upload wall clock at 1, 4, 16 and 32 streams, the 1,600 MiB/s link-saturation ceiling, per-piece retry cost, and abandoned uploads billing 360 GiB at $0.023/GiB-month (a `c4` timeline-bar chart plus a red cost strip). It was cut for making a tutorial page read like a tuning guide. Throughput and concurrency belong on `09-throughput-and-request-rate`; abandoned-upload storage and the cleanup rule belong on `13-lifecycle-rules-and-storage-classes` and `19-cost-egress-and-data-gravity`.
  - **No ETag discussion.** The `-N` suffix on a multipart file's ETag and its "not an MD5" warning is integrity-and-metadata reference detail, not part of this concept.
  - **No maximum-object-size or maximum-part-size bullets.** 5 TB and 5 GiB were trimmed as ceilings the reader never touches in this example.
  - **No sixth bar in `c2`.** The 128 MiB → 1,600 candidate was dropped; five bars carry the point.
  - **Three sections, exactly 8 bullets each, one canvas per section (`c1`, `c2`, `c3`).**
- **Data:** all values are hardcoded literals, no randomness anywhere — never `Math.random()`.
  - **Documented behaviour the page stands on:** a single write carries at most 5 GB of body; a multipart upload uses 1 to 10,000 numbered pieces; every piece except the last must be at least 5 MiB; pieces may be sent in any order and in parallel; the file becomes visible only when the upload is finished, and a read before that finds nothing; cancelling discards the uploaded pieces; a range read transfers only the requested bytes and is billed as such.
  - **Illustrative and labelled as such:** the 200 GiB nightly export, the 128 MiB block size, the 8 MiB blocks, the 64 KiB index, and the $0.09/GiB egress price.
  - **Computed and verifiable:** 200 GiB × 1,024 = 204,800 MiB; 204,800 ÷ 10,000 = 20.48 MiB minimum piece size; 204,800 ÷ 5 = 40,960 pieces and 40,960 − 10,000 = 30,960 excess; 204,800 ÷ 10 = 20,480; 204,800 ÷ 20.48 = 10,000; 204,800 ÷ 32 = 6,400; 204,800 ÷ 64 = 3,200; 10,000 × 32 MiB = 320,000 MiB = 312.5 GiB; 1 GiB = 1,024 MiB and 1,024 ÷ 5 = 204.8, rounded up to 205 pieces; 204,800 ÷ 128 = 1,600 blocks and 1,600 − 4 = 1,596 untouched; 4 × 8 MiB + 64 KiB = 32.0625 MiB, shown as 32.06; 32.0625 ÷ 204,800 = 0.00015655 = 0.0157%; 200 × $0.09 = $18.00; 32.0625 MiB = 0.0313 GiB, × $0.09 = $0.0028 ≈ $0.003.
  - **Computed geometry:** `c1` derives every fan-line endpoint from the three box constants (`startX`+`startW`, `pillX`, `pillX`+`pillW`, `endX`) and the stage centre line y=91. `c2` places bars with `barX(i) = 90 + i × 127`, heights with the log formula above, and divides 204,800 by each piece size at render time (rounded, since 20.48 is not exact in binary floating point). `c3` maps byte offsets with `fx(mib)` and derives the four highlighted block positions from their block numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
