# Listing, Pagination & Prefix Scans

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Listing, Pagination &amp; Prefix Scans

**Subtitle:** Asking which objects exist gives you at most 1,000 keys at a time, in sorted order, so a million-key prefix is a thousand requests taken one after another

## Running example (carried through every section)

A clickstream bucket holds **1,200,000 objects** under one dated prefix, written as
**24 hourly sub-prefixes of 50,000 objects** each (1,200,000 ÷ 24 = 50,000 exactly).
At most **1,000 keys** come back per request, so listing the whole prefix is
**1,200 requests**. Illustrative round trip: **40 ms** per request, so 48.0 seconds.

## Listing Returns One Page at a Time

**Tags:** `core idea` (blue), `1,000 keys per reply` (green), `bookmark` (orange)

- **The bucket** — 1,200,000 clickstream objects written under a single dated prefix
- **The limit** — one request hands back at most 1,000 keys, which is also the maximum
- **More to come** — the reply tells you whether more keys remain behind this page
- **The bookmark** — it also returns a marker recording where the next page starts
- **The loop** — send that marker back with your next request to get the next page
- **Strictly in order** — page 7 cannot be asked for until page 6 has already landed
- **No shortcut** — one prefix is a single chain, walked front to back page by page
- **The only speedup** — run several different prefixes at the same time, side by side

*Example (italic):* Listing 1,200,000 keys takes 1,200 requests: the first 1,199 replies say more keys remain, the last one does not.

**Key point:** Listing is a walk through pages, not a query. The number of requests is set by the key count, and nothing you write makes it smaller.

### Visualization (canvas `c1`, 720×300)

Chain diagram: request/reply pairs threaded by the bookmark, showing requests 1, 2, 3, a gap, and the last one.

- **Title (bold 15px `#1a5276`, centered at y=22):** built in JS as "One Page at a Time: 1,200,000 Keys ÷ 1,000 = 1,200 Requests", with the key count, page size and computed request count formatted from the literals `KEYS = 1200000` and `PAGE = 1000`.
- **Row geometry (computed, not hardcoded):** `PITCH = 50`, `rowY(i) = 40 + i × PITCH` for the first three rows (40, 90, 140); the final row is pushed below the elision gap at `rowY(2) + PITCH + 16 = 206`. Box height 30, so text baselines are `rowY + 19` and row centres `rowY + 15`.
- **Request boxes (x=45, w=175, h=30, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`, 12px `#2c3e50` centered at x=132):** "request 1", "request 2 + bookmark", "request 3 + bookmark", "request 1,200 + bookmark".
- **Reply boxes (x=350, w=235, h=30, 8px radius, 2px border, 12px `#2c3e50` centered at x=467):** the first three read "1,000 keys · more to come" with fill green `#008300` at 0.12 and a green border; the last reads "last 1,000 keys · no bookmark" with fill violet `#4a3aa7` at 0.12 and a violet border.
- **Forward arrows:** 2px `#6b7280` from x=224 to x=344 at each row centre, with an arrowhead at x=350.
- **Bookmark threads (the first two gaps only):** 2px dashed (5/4) magenta `#d55181` path (585, rowY+15) → (610, rowY+15) → (610, railY) → (35, railY) → (35, rowY+PITCH+15) → arrowhead into (45, rowY+PITCH+15), where `railY = rowY + 40` (80 and 130, inside the gap between boxes). Bold 11px magenta "bookmark" centered at (320, railY − 4).
- **Elision rule:** dashed (4/3) `#e5e9ef` rule at y=190 from x=45 to x=585; 12px italic `#6b7280` "… 1,196 more requests, each waiting for the one before …" centered at (315, 185). The 1,196 is computed as 1,200 minus the 4 rows drawn.
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=264):** "each request needs the bookmark from the reply before it".
- **Caption (12px `#444`, bottom right at y=290):** "1,000 keys per reply is the documented limit; object count illustrative".

## Counting the Round Trips

**Tags:** `worked example` (blue), `1,200 requests` (green), `48 seconds` (orange)

- **The division** — 1,200,000 keys ÷ 1,000 keys per page = exactly 1,200 requests
- **The wait** — at 40 ms per round trip (illustrative), that adds up to 48.0 seconds
- **One after another** — the waits cannot overlap, so they simply add end to end
- **A straight line** — 300,000 keys is 300 requests and 12.0 seconds of waiting
- **Half the keys** — 600,000 keys is 600 requests and 24.0 seconds, exactly half
- **No data yet** — those 48 seconds move no object contents, only lists of names
- **Before any work** — the job waits the full 48.0 s before reading its first row
- **Every run pays** — a job that lists this prefix each hour waits again each time

*Example (italic):* Before the job reads a single row, it spends 48.0 seconds just finding out which 1,200,000 files exist.

**Key point:** Listing time is a straight line in key count: requests = keys ÷ 1,000, wait = requests × round trip. Nothing else moves it.

### Visualization (canvas `c2`, 720×300)

Single-series line chart: seconds of listing against the number of objects under the prefix, with the two end points labelled.

- **Title (bold 15px `#1a5276`, centered at y=24):** "More Keys, More Round Trips — the Wait Is a Straight Line".
- **Plot box:** x0=90, x1=640, baseline y=240, top y=62; x scale linear 0 → 1.3 million objects, y scale 0 → 52 seconds.
- **Data (literals):** object counts in millions `[0.1, 0.3, 0.6, 0.9, 1.2]`, requests `[100, 300, 600, 900, 1200]`; **seconds are computed at render time** as requests × 0.040 → 4.0, 12.0, 24.0, 36.0, 48.0.
- **Y axis:** gridlines `#e5e9ef` and 12px `#444` labels at 0, 16, 32, 48 seconds; 1px `#999` axis lines.
- **X ticks (12px `#444`, y = baseline+20):** "100k", "300k", "600k", "900k", "1.2M" at their mapped positions.
- **Series (orange `#d95926`, 3px line, 5px filled dots)** through the five points.
- **End labels (bold 12px orange, both strings built from the computed seconds):** "100 requests · 4.0 s" left-aligned 10px right of and 8px above the first dot; "1,200 requests · 48.0 s" right-aligned 14px left of the last dot.
- **Annotation (bold 13px green `#008300`, centered at (300, 108) — in the empty space above the rising line):** "double the keys, double the requests, double the wait".
- **Axis titles (12px `#444`):** "objects under the prefix" centered below the x ticks; "seconds of listing" rotated at x=26.
- **Caption (12px `#444`, bottom right at y=294):** "40 ms per request illustrative; request counts exact".

## Sorted Keys, So Split the Work Across Prefixes

**Tags:** `fan out` (blue), `range scan` (green), `common mistake` (red)

- **Sorted order** — keys always come back sorted, so a prefix is a contiguous range
- **A prefix is a slice** — one hour's folder can be listed entirely on its own
- **Split the day** — 24 hourly prefixes hold 50,000 keys each, evenly divided
- **Each one is short** — 50,000 keys is 50 requests, about 2.0 seconds of waiting
- **Run them together** — all 24 chains at once finish in 2.0 s rather than 48.0 s
- **Same total work** — still 24 × 50 = 1,200 requests; only the waiting shrinks
- **No other index** — nothing lets you ask for "the objects changed yesterday"
- **A catalog instead** — one catalog file names the same paths in a single read

*Example (italic):* Splitting the day into 24 hourly prefixes uses the same 1,200 requests but finishes in 2.0 seconds instead of 48.0.

**Common mistake:** Treating slow listing as something to tune at read time. It is a layout decision — the prefixes you choose when writing decide how fast the listing can ever be.

### Visualization (canvas `c3`, 720×300)

Two tracks on one shared time axis: a single chain of 1,200 requests above, 24 short chains running together below.

- **Title (bold 15px `#1a5276`, centered at y=24):** built in JS from the computed seconds — "One Prefix Takes 48.0 s; 24 Prefixes Take 2.0 s".
- **Computed values:** `KEYS = 1200000`, `PAGE = 1000`, `MS = 0.040`, `PREFIXES = 24`; calls = 1,200, sequential = 48.0 s, keys per prefix = 50,000, calls per prefix = 50, parallel = 2.0 s, speedup = 24×. Every number printed on the chart comes from these.
- **Shared time axis:** t = 0 → 50 s mapped to x = 90 → 660, so `tx(t) = 90 + (t / 50) × 570`; 1px `#999` baseline at y=258; vertical `#e5e9ef` gridlines at 0/10/20/30/40/50 s from y=46 to y=258 with 12px `#444` labels "0 s" … "50 s" at y=276.
- **Top track labels (12px `#444`, right-aligned at x=82):** "1 prefix" at y=74, "1.2M keys" at y=90.
- **Top bar:** x = tx(0) to tx(48.0), y=66, h=26, fill `rgba(42,120,214,0.55)`, 2px `#2a78d6`; bold 12px white centered "1,200 requests, one after another"; bold 13px `#2a78d6` "48.0 s" right-aligned at (660, 58).
- **Bottom track labels (12px `#444`, right-aligned at x=82):** "24 prefixes" at y=178, "50,000 keys" at y=194.
- **Bottom bars:** 24 bars from tx(0) to tx(2.0), height 4, at y = 130 + i × 5 for i = 0…23, fill `rgba(25,158,112,0.75)`, no border; 1px `#199e70` bracket from y=130 to y=249 just right of the bars.
- **Bottom labels (left-aligned right of the bracket):** 12px aqua "24 chains running at the same time" at y=168; bold 13px aqua "2.0 s instead of 48.0 s — 24× less waiting" at y=192; 12px `#6b7280` "same 1,200 requests either way, just not one after another" at y=214.
- **Caption (12px `#444`, bottom right at y=294):** "40 ms per request illustrative; 1,200,000 ÷ 24 = 50,000 exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section three's callout uses the label "Common mistake:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead` and `rgba` as on page 11.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. No red inside any canvas on this page — nothing drawn here is an error state; red appears only on section three's `common mistake` tag pill and the `.key-point` left border.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, long enough to hold one line at the 50/50 split while still carrying a full clause rather than a clipped stub. An earlier pass cut them to ~55–70 characters and was rejected for compromising quality — the facts were there but the phrasing read as shorthand. Do not shorten them further, and do not pad them back out to the folder's ~90–100 default either.
- **Register and vocabulary.** Plain technical English for a first-time reader, with much less API surface than the earlier draft: say "one request returns at most 1,000 keys", not `ListObjectsV2` / `MaxKeys`; say "the reply tells you whether more keys remain", not `IsTruncated`; say "a marker saying where to resume" / "bookmark", not `NextContinuationToken` / `ContinuationToken`; say "you can start listing from a chosen key onward", not `StartAfter`; say "a catalog file", not S3 Inventory or Iceberg; say "requests" and "replies", not LIST calls and responses. No analogies and no invented scenes — the bucket is just a bucket of clickstream objects. The exact parameter names live on the sibling reference pages.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No small-file / "listing looks free" section.** A fourth section held the same 1,200 GB fixed while shrinking object size (1 GB → 1 MB objects, 1,200 → 1,228,800 keys, 0.08 s → 49.16 s), a flat 30.0 s byte-scan line, and a 1.6384 MB crossover ring in `c4`. It was cut for turning a tutorial into a sizing exercise. The small-file problem belongs on the object-layout/compaction page, and per-request pricing belongs on `19-cost-egress-and-data-gravity`.
  - **No pricing.** The earlier draft priced every scan ($0.005 per 1,000 requests, $0.0060 a scan, $3.00 a day, $90.00 a month). Dollars are a cost-page concern; this page measures the wait in requests and seconds only.
  - **No bucket or prefix name strings, no manifest/table-format naming.** They added vocabulary without adding the idea.
  - **Three sections, 8 bullets each at 75–90 characters, one canvas per section.**
- **Data:** all values are hardcoded literals, no randomness anywhere.
  - **Documented behaviour the page stands on:** a listing request returns at most 1,000 keys (the default and the maximum); the reply states whether more keys remain and carries an opaque marker to resume from; that marker must be sent back to get the next page, so one prefix is walked strictly in order; keys are returned in sorted byte order, which makes a prefix a contiguous range; listing can be resumed from a chosen key; there is no secondary index over object metadata, so "changed yesterday" is not a question you can ask.
  - **Illustrative and labelled as such:** the 1,200,000-object bucket, its split into 24 hourly prefixes, and the 40 ms round trip.
  - **Exact arithmetic to preserve:** 1,200,000 ÷ 1,000 = 1,200 requests; 1,200 × 0.040 = 48.0 s; 300,000 → 300 requests → 12.0 s; 600,000 → 600 requests → 24.0 s; 1,200,000 ÷ 24 = 50,000 keys per prefix; 50,000 ÷ 1,000 = 50 requests; 50 × 0.040 = 2.0 s; 24 × 50 = 1,200 requests; 48.0 ÷ 2.0 = 24×; 1,200 − 4 rows drawn = 1,196 elided requests.
  - **Computed geometry:** `c1` derives its four row tops from `PITCH = 50` rather than hardcoding them, and its title and elided-request count from `KEYS`/`PAGE`; the bookmark threads route along a rail at `rowY + 40`, which falls in the gap between boxes for any row pitch. `c2` computes every second value as requests × `MS` and both end labels from those computed values. `c3` maps time with `tx(t) = 90 + (t / 50) × 570` and derives both bar widths, the title, and all three bottom labels from `KEYS`, `PAGE`, `MS` and `PREFIXES`; the 24× figure is `Math.round(SEQ / PAR)` and the "40 ms" captions are `Math.round(MS × 1000)`, so no timing is written twice.
