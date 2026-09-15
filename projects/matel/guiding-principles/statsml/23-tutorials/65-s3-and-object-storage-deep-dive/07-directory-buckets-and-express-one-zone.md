# Directory Buckets &amp; Express One Zone

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Directory Buckets &amp; Express One Zone

**Subtitle:** A storage class that keeps one copy in one zone, and stores real folders, so that reads of small objects come back very fast

## The Wait Before the First Byte

**Tags:** `core idea` (blue), `latency not bandwidth` (green), `one zone` (orange)

- **The workload** — a training job rereads the same 500 GB working set on every pass
- **Small objects** — 2,000,000 shard files of about 250 KB each, not a few big ones
- **The bottleneck** — the job spends far more time waiting for reads than streaming bytes
- **What Express is** — a storage class built for very fast reads of small objects
- **A new bucket type** — you reach the class through a bucket type called a directory bucket
- **One zone** — all of the data sits in a single Availability Zone that you pick
- **Compute nearby** — run the job on machines inside that same zone, not across zones
- **The gain** — the first byte arrives in single-digit milliseconds, about 10x faster

*Example (illustrative):* At 30 ms per read, 1,000 reads one after another take 30 seconds; at 3 ms the same 1,000 reads take 3 seconds.

**Key point:** The bytes per second did not change. What shrank is the wait before the first byte arrives.

### Visualization (canvas `c1`, 720×300)

Two first-byte-latency bars on the left with the documented single-digit-millisecond line marked, and a right-hand panel showing that the waiting shrank while the bytes did not.

- **Title (bold 15px, `#1a5276`, centered at y=24):** "First-Byte Latency Is What Changed, Not Bandwidth".
- **Bar geometry:** baseline y=232, scale 5 px per ms, bar width 62; bar i sits at `x = 104 + i × 110`, so Standard at x=104 and Express at x=214.
- **Bar 1 (Standard, `#2a78d6` at 0.85 alpha):** 30 ms → 150 px tall; bold 12.5px blue value label "30 ms" centred 8 px above the bar.
- **Bar 2 (Express, `#008300` at 0.85 alpha):** 3 ms → 15 px tall; bold 12.5px green value label "3 ms" centred 8 px above the bar.
- **Axis:** 1.5px `#1a5276` baseline from x=88 to x=300 at y=232; category labels 12px `#2c3e50` centred at y=250 — "S3 Standard" and "Express One Zone"; y tick labels 12px `#6b7280` right-aligned at x=82 for 0, 10 ms, 20 ms, 30 ms, with 1px `#e5e9ef` gridlines from x=88 to x=300 for the non-zero ticks.
- **Documented line (dashed 4/3, 1.5px aqua `#199e70`) at 9 ms (y=187) from x=88 to x=300:** bold 11.5px aqua label "single-digit ms" **right-aligned at (298, 181)**. It is right-aligned deliberately: left-aligned at x=92 the label ran straight across the 30 ms bar, and the longer wording "(documented)" made the collision worse. The word "documented" lives in the caption instead.
- **Right panel (x=330, y=64, width 360, height 168, 8px radius):** fill `#f4f6f9`, 1.5px `#e5e9ef` border.
  - Bold 13px `#1a5276` heading at (346, 90): "same bytes, less waiting".
  - Three 12.5px `#2c3e50` lines at (346, 118 / 142 / 166), each printed from values computed in JS: "2,000,000 objects × 250 KB = 500 GB", "1,000 reads in a row @ 30 ms = 30.0 s", "1,000 reads in a row @ 3 ms = 3.0 s".
  - Bold 12.5px orange `#d95926` at (346, 196): "bandwidth per read: unchanged".
  - 12px `#6b7280` at (346, 218): "the object is the same size either way".
- **Annotation (bold 13px violet `#4a3aa7`, centred at y=272):** "the wait shrinks about 10x; the bytes per second do not move".
- **Caption (12px `#444`, bottom right at y=294):** "30 ms and 3 ms illustrative; single-digit ms and about 10x are documented".

## Directory Buckets Really Have Directories

**Tags:** `the exception` (blue), `real folders` (green), `naming` (orange)

- **Usually a fiction** — in an ordinary bucket the folders you see are not stored
- **Here they are real** — a directory bucket stores an entry for every folder level
- **On write** — a key with slashes in it creates a folder for each level of the path
- **Listing** — reading a folder walks down the tree instead of scanning sorted keys
- **Order changes** — results come back in tree order, not in alphabetical key order
- **Scaling** — the tree spreads the load, so you do not need prefix tricks any more
- **The name is fixed** — a directory bucket's name ends with the zone id and `--x-s3`
- **Zone is visible** — the bucket name itself tells you which zone holds the data

*Example (illustrative):* The key `shards/epoch=01/part-000001.arrow` creates a `shards` folder and an `epoch=01` folder inside it; an ordinary bucket stores only the one long key.

**Key point:** This is the one place where "S3 has no folders" stops being true, and that is exactly why listing and scaling behave differently here.

### Visualization (canvas `c2`, 720×300)

Two panels side by side over the same four objects: on the left a flat sorted key list with its folder view marked as worked out per request, on the right the stored tree with real folder rows.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "Same Four Objects: Folders Worked Out vs Folders Stored".
- **Panels:** left x=28 width 320, right x=372 width 320, both y=44 height 216, 8px radius, fill `#fbfcfd`, 1.5px border — left `#2a78d6`, right `#008300`.
- **Panel headers (bold 12.5px, baseline y=64):** left "ordinary bucket — flat keys" in `#2a78d6` at x=40; right "directory bucket — stored tree" in `#008300` at x=384.
- **Left note (12px `#6b7280` at (40, 80)):** "four keys, sorted order".
- **Left key list (11.5px monospace `#2c3e50`, x=40, baselines 102, 122, 142, 162):**
  - `index/manifest.json`
  - `shards/epoch=01/part-000001.arrow`
  - `shards/epoch=01/part-000002.arrow`
  - `shards/epoch=02/part-000001.arrow`
- **Left "worked out" box (x=40, y=168, width 296, height 68, 6px radius):** dashed 4/3 1.5px `#6b7280` border, fill `rgba(107,114,128,0.07)`; bold 12px `#6b7280` at (52, 188) "the folder view you see"; 11.5px monospace `#2c3e50` at (52, 208) `shards/    index/`; bold 12px magenta `#d55181` at (52, 228) "worked out per request, stored nowhere".
- **Right tree (rows 20px apart from baseline y=88):** indent 16px per level from x=388; folder rows bold 11.5px monospace `#008300`, file rows 11.5px monospace `#2c3e50`; 1px `#e5e9ef` elbow connector from each indented row back to its parent column.
  - `shards/` (folder, level 0)
  - `epoch=01/` (folder, level 1)
  - `part-000001.arrow` (file, level 2)
  - `part-000002.arrow` (file, level 2)
  - `epoch=02/` (folder, level 1)
  - `part-000001.arrow` (file, level 2)
  - `index/` (folder, level 0)
  - `manifest.json` (file, level 1)
  - Bold 12px `#008300` at (388, 250): "4 folders stored on write" — the count is derived in JS by tallying the folder rows in the tree array, not typed in. (An earlier draft printed 3 here, which contradicted its own tree: `shards/`, `epoch=01/`, `epoch=02/` and `index/` are four folders.)
- **Annotation (bold 13px violet `#4a3aa7`, centred at y=276):** "left: folders inferred from the key text · right: folders are stored rows".
- **Caption (12px `#444`, bottom right at y=294):** "keys illustrative; the stored folder tree is documented behaviour".

## What You Give Up for the Speed

**Tags:** `the trade` (blue), `not a system of record` (red), `session token` (green)

- **One copy only** — one zone holds it, so losing that zone loses the data outright
- **Not for records** — use it only for data you can rebuild from somewhere durable
- **No second region** — copying the bucket to another region is not available here
- **No versions** — an overwrite is final, and there is no earlier copy to roll back to
- **No colder tiers** — you cannot move an object into a cheaper storage class later
- **Storage costs more** — per GB stored it costs several times what Standard charges
- **Reads cost less** — per request it is roughly half of what Standard charges you
- **A short-lived token** — you sign in once per session instead of once for every read

*Example (illustrative):* A shuffle file lost with its zone costs a job restart; a ledger lost with its zone costs the record.

**Key point:** The trade is honest: you hand back durability and cheap bytes, and you get low latency, cheap reads and real folders.

### Visualization (canvas `c3`, 720×300)

Four paired horizontal bars, Standard against Express, each row scaled to its own maximum so the direction of the trade is readable; green where Express wins, red where it loses, plus a red one-zone banner.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "Row by Row: Where Express Wins and Where It Loses".
- **Legend line (12px `#6b7280`, centred at y=42):** "upper bar = S3 Standard · lower bar = Express (green when it wins, red when it loses)".
- **Rows (top y = 62, 106, 150, 194), bar track x=250 width 350, bar height 15, 3px gap:** upper bar Standard in `#2a78d6`, lower bar Express; bar length = `value / row max × 350`, both at 0.85 alpha. The track is 350 px, not 390 px, so that a value label past a full-length bar still has room — see the label rule below.
- **Row labels (12px `#2c3e50`, right-aligned at x=240, baseline at `y + bh + gap` = 18 px below the row top, so the label sits vertically centred on the bar pair):**
  - "features kept (of 4)" — Standard 4, Express 0 — Express loses (red `#e74c3c`)
  - "first-byte latency (ms)" — Standard 30, Express 3 — Express wins (green `#008300`), lower is better
  - "read cost ($ / million)" — Standard 0.40, Express 0.20 — Express wins (green), lower is better
  - "storage ($ / GB-month)" — Standard 0.023, Express 0.16 — Express loses (red), lower is better
- **Value labels (bold 12px):** Standard labels in `#2a78d6`; Express labels in that row's win/lose colour. Default position is left-aligned 8 px past the bar end. **Overflow rule:** measure the label with `measureText`; if `barEnd + 8 + width` would pass `w − 12`, draw it right-aligned *inside* the bar at `barEnd − 8` in white instead. This is what keeps the storage row's long Express label on the canvas — a full-length bar plus "$0.16 (6.96x more)" ran off the right edge in an earlier draft. The zero-length Express features bar draws a 3px stub so its label still clears the track.
- **Label text:** Standard "4 of 4", "30 ms", "$0.40", "$0.023"; Express "0 of 4 — none of them", then three ratio labels built in JS from the same numbers — "3 ms (10x lower)", "$0.20 (2x lower)", "$0.16 (6.96x more)".
- **Counted features (11.5px `#6b7280` at (40, 243)):** "counted: copies in more than one zone · copy to another region · versions · colder classes".
- **Red banner (x=40, y=250, width 640, height 30, 6px radius):** fill `rgba(231,76,60,0.10)`, 2px `#e74c3c` border, bold 12.5px `#e74c3c` centred at (360, 270): "one zone: lose the zone, lose the data — a cache, not a system of record".
- **Caption (12px `#444`, bottom right at y=294):** "prices and latencies illustrative; the missing features and the single zone are documented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `01-buckets-keys-and-the-flat-namespace.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead` and `rgba` as in page 01 — keep all three defined even when a chart does not use `arrowHead`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine risk: the two losing rows and the one-zone banner in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly **75–90 characters** including the bold lead term, so each holds one line at the 50/50 split while still carrying a full clause. Two guard rails, both learned the hard way:
  - **Do not clip to ~55–70 characters.** An earlier pass cut them to stubs and it was rejected for compromising quality — the wording lost the clause and read as notes.
  - **Do not pad back to the folder default of ~90–100 either.** That default is for the denser pitfall-style pages; this page is deliberately shorter.
  - Keep every section at **8 bullets**. Shorten wording, never substance: if a bullet is over length, rewrite the phrasing, do not drop the fact.
- **Register and vocabulary.** Plain technical English for a first reading, and fewer product and API names than the earlier draft: say "read" and "write", not GET / PUT / LIST / HEAD; say "folders", not hierarchical namespace or `CommonPrefixes` / `delimiter=/`; say "a short-lived token, one sign-in per session", not `CreateSession` or per-request IAM signing; say "you cannot move an object into a cheaper storage class", not lifecycle transitions to Standard-IA or Glacier; say "it ends with the zone id and `--x-s3`", without spelling out a full example bucket name; say "an ordinary bucket", not "a general-purpose bucket". The storage-class name is fair to state once in the opening section and to use as a chart series label; do not sprinkle product names through the prose. **No analogies, no invented scenes, no story framing.** This is a tutorial, not a reference course — the exact API and configuration names belong on the reference pages.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No cost-crossover section.** A fourth section derived the monthly bill from illustrative rates (0.023S + 1.0368R against 0.16S + 0.5184R), the 500 GB / 4,000 reads-per-second case, the 200 TB low-ratio case, the break-even ratio of 0.2643 reads per second per GB, and `c4`'s two cost lines with a computed crossing point. It was cut for turning a tutorial into a pricing exercise. Cost arithmetic belongs on `19-cost-egress-and-data-gravity`.
  - **No re:Invent-2023 announcement date, no "6.96x" or "48.2%" figures in the prose,** and no cross-AZ transfer-tax bullets — they travelled with the cost section. The one surviving ratio label, "6.96x more", is computed in `c3` from the two rates it plots.
  - **No 4,000-reads-per-second / 1 GB per second throughput arithmetic.** It came in with the cost section and turned the opening into a capacity calculation; the opening only needs "waiting, not streaming".
  - **Three sections, 8 bullets each, one canvas per section (`c1`, `c2`, `c3`).**
- **Data:** all chart values are hardcoded literals, no randomness anywhere.
  - **Documented behaviour the page stands on:** S3 Express One Zone is a storage class reached through a new bucket type called a directory bucket; the data is held in one Availability Zone chosen by the customer; single-digit-millisecond first-byte latency and up to roughly 10x faster access than S3 Standard; request charges are materially lower than Standard while per-GB storage is materially higher; a directory bucket stores a real folder tree, so slash-separated keys create folder entries, listing is a walk down that tree, result order follows the tree rather than key text, and the per-prefix request-rate ceiling of ordinary buckets does not apply the same way; the bucket name must end with the zone id and `--x-s3`; authentication uses a short-lived session token instead of signing every request individually; replication to another region, versioning and storage-class transitions are unsupported.
  - **Illustrative and labelled as such:** the 2,000,000-object, 250 KB, 500 GB working set; the 30 ms and 3 ms first-byte latencies; and the four rates — Standard $0.023 per GB-month and $0.40 per million reads, Express $0.16 and $0.20. These are stated as illustrative rather than quoted list prices, and the page's arithmetic is self-consistent with them.
  - **Computed at render time, not typed in:** 2,000,000 × 250 KB = 500,000,000 KB = 500 GB and both "1,000 reads in a row" totals in `c1`'s panel (1,000 × 30 ms = 30.0 s, 1,000 × 3 ms = 3.0 s); the folder count in `c2`, tallied from the tree array (4, not the 3 an earlier draft asserted); and all three ratio labels in `c3` (30 / 3 = 10x lower, 0.40 / 0.20 = 2x lower, 0.16 / 0.023 = 6.96x more).
  - **Computed geometry:** `c1` places bar *i* at `104 + i × 110` and derives bar height as `ms × 5` from the baseline y=232, so the 9 ms documented line lands at y=187. `c2` uses a 20 px row pitch from y=88 and a 16 px indent per tree level. `c3` uses a 44 px row pitch from y=62, scales each bar to its own row maximum over a 350 px track, and decides each value label's side with `measureText` rather than by eye.
