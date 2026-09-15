# S3 & Object Storage

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** S3 & Object Storage

**Subtitle:** Amazon S3 (2006) stores whole objects in buckets behind plain HTTP — cheap, 11-nines-durable bytes that quietly became the disk under the entire modern data stack

## The Clickstream That Lands in a Bucket

**Tags:** `core idea` (blue), `key → bytes` (green), `HTTP` (orange)

- **The stream** — a retail site logs every click; each hour lands as one Parquet file of events
- **The bucket** — each hourly file is PUT over HTTP into a bucket named `shop-clicks`
- **The key** — an object's full name is one string: `clicks/date=2026-08-26/hour=14.parquet`
- **No folders** — the slashes are just characters in the key; a bucket is a flat key → bytes map
- **Born 2006** — S3 launched in 2006 as AWS's first widely used service: PUT, GET, LIST, DELETE

*Example (italic):* The 2pm file goes up with one HTTP PUT and any engine reads it back with one GET on the same key — no mount, no filesystem, no server to log into.

**Key point:** An object store is a giant key → bytes dictionary behind HTTP: whole objects in, whole or byte-range reads out, no in-place edits — addressed by key, not a disk you mount.

### Visualization (canvas `c1`, 720×300)

Side-by-side diagram: a filesystem tree of nested folders (left) vs the flat bucket holding full-string keys (right), showing the same three hourly files both ways.

- **Title (bold 15px, `#1a5276`, top center):** "Same Three Files: Filesystem Tree vs Flat Bucket of Keys".
- **Left half, 12px `#444` label "filesystem (nested folders)" at (60, 65):** folder boxes 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` text — "clicks/" at (60, 80, 110×30), "date=2026-08-26/" at (95, 125, 150×30), then three file boxes fill `rgba(0,131,0,0.12)` at (130, 170), (130, 210), (130, 250), each 170×30: "hour=13.parquet", "hour=14.parquet", "hour=15.parquet"; 2px `#6b7280` connector lines between parent and child boxes.
- **Right half, 12px `#444` label "bucket shop-clicks (flat keys)" at (400, 65):** three key boxes 300×34 at x=400, y = 90, 140, 190, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 11px monospace `#2c3e50` text "clicks/date=2026-08-26/hour=13.parquet" / "...hour=14.parquet" / "...hour=15.parquet" (full keys, one per box).
- **Divider:** vertical dashed `#e5e9ef` (dash 4/3) line at x=375 from y=60 to y=280.
- **Annotation (bold 13px violet `#4a3aa7`, right half near y=250):** "the slashes are just characters in the key".
- **Caption (12px `#444`, bottom right):** "file names illustrative; the flat-key model is documented S3 behavior".

## Pricing a Year of Clicks

**Tags:** `worked example` (blue), `GB-month` (green), `lifecycle tiers` (orange)

- **The pile** — after a year the site holds 2,000 GB of hourly Parquet files (illustrative)
- **Storage bill** — standard at $0.023/GB-mo: 2,000 × 0.023 = $46.00 a month (rate illustrative)
- **Request bill** — requests bill too: $5.00 per million PUTs, $0.40 per million GETs (illustrative)
- **Lifecycle rule** — after 90 days objects slide to infrequent access: $0.0125/GB-mo → $25.00
- **Cold tiers** — archive at $0.004/GB-mo → $8.00; deep archive at $0.00099/GB-mo → $1.98
- **Hand-check** — 2,000 × 0.0125 = 25.00 and 2,000 × 0.00099 = 1.98 (multiplications exact)

*Example (italic):* The same 2,000 GB costs $46.00 a month hot, $25.00 in infrequent access, $8.00 archived, and $1.98 in deep archive — about 23× cheaper cold than hot.

**Key point:** The pricing model is pay per GB-month plus pay per request — that part is exact; lifecycle rules slide aging objects down storage classes, so the same bytes get roughly 23× cheaper as they cool.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: monthly cost of the same 2,000 GB in four storage classes, falling from $46.00 to $1.98.

- **Title (bold 15px, `#1a5276`, top center):** "The Same 2,000 GB, Four Storage Classes: $46.00 Down to $1.98 a Month".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = $0 to $50, gridlines `#e5e9ef` at $10/$20/$30/$40 with 12px `#444` labels; scale 3.6 px per $.
- **Bars (90px wide, at x = 110, 260, 410, 560), heights from values `[46.00, 25.00, 8.00, 1.98]` → pixel heights `[166, 90, 29, 7]`:** standard blue `#2a78d6`, infrequent access aqua `#199e70`, archive orange `#d95926`, deep archive violet `#4a3aa7`; fills at 0.85 alpha with solid 2px borders.
- **Value labels (bold 13px, bar color, centered above each bar):** "$46.00", "$25.00", "$8.00", "$1.98".
- **Tier labels (12px `#444`, centered under baseline):** "standard", "infrequent access", "archive", "deep archive".
- **Annotation (bold 13px green `#008300`, near x=430, y=95):** "lifecycle rules move cooling data down automatically".
- **Caption (12px `#444`, bottom right):** "per-GB rates illustrative; the multiplications are exact".

## The Disk That Ate the Data Center

**Tags:** `where it's used` (blue), `11 nines` (green), `data lake` (orange)

- **Durability** — S3 documents 99.999999999% (11 nines), kept by copies across availability zones
- **What it means** — store 10 million objects and expect one loss per ~10,000 years (documented)
- **Data lake** — the dated Parquet objects in the bucket ARE the data lake; no loading step exists
- **Split stack** — modern warehouses keep tables on object storage and rent compute separately
- **Read in place** — warehouse, Spark, and ML jobs all read the same 2,000 GB; no per-tool copies
- **2020 upgrade** — eventually consistent for 14 years; strong read-after-write since Dec 2020

*Example (italic):* An analyst points a SQL engine straight at the bucket and queries a year of clicks — the "warehouse disk" is the bucket itself.

**Key point:** Once bytes were cheap, 11-nines durable, and reachable over HTTP by anything, storage stopped living inside databases — the bucket became the shared bottom layer of the data stack.

### Visualization (canvas `c3`, 720×300)

Layered stack diagram: one wide object-storage substrate at the bottom, format and table layers in the middle, three independent engines on top, all reading the same bytes.

- **Title (bold 15px, `#1a5276`, top center):** "One Bucket, Many Engines: Object Storage as the Bottom Layer".
- **Bottom layer (x=60, y=225, 600×45, 8px radius):** fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, bold 13px `#1a5276` text "object storage — bucket shop-clicks (2,000 GB, 11-nines durability)".
- **Middle layer (y=160, two boxes 290×42):** at x=60 "open file format — Parquet files", at x=370 "table format — Iceberg / Delta metadata"; fill `rgba(25,158,112,0.15)`, 2px `#199e70` border, 12px `#2c3e50` text.
- **Top layer (y=85, three boxes 185×42 at x = 60, 268, 476):** "SQL warehouse", "Spark jobs", "ML training"; fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, 12px `#2c3e50` text.
- **Arrows:** 2px `#6b7280` vertical arrows from each top box down to the middle layer and from the middle layer down to the substrate (arrowheads pointing down).
- **Annotation (bold 13px green `#008300`, centered near y=60):** "one copy of the data — every engine reads it in place".
- **Side note (12px `#6b7280`, under the bottom layer near y=283, left):** "strong read-after-write consistency since Dec 2020 — a landed file is instantly visible".
- **Caption (12px `#444`, bottom right):** "11-nines durability is S3's documented figure; sizes illustrative".

## It Looks Like a Filesystem — It Isn't

**Tags:** `common mistake` (red), `whole-object writes` (orange)

- **The trap** — the keys look like paths, so a new engineer treats the bucket like a mounted disk
- **No appends** — adding one row means GET the whole object, edit it, and PUT it all back
- **No rename** — "moving" a key is a full copy to the new key plus a delete of the old one
- **No real dirs** — listing a "folder" is a prefix scan over keys; an empty folder cannot exist
- **Design around it** — write immutable dated files; to add data, add new objects, never edit

*Example (italic):* Appending one 200-byte click to a 1 GB object re-uploads the whole 1 GB (sizes illustrative); a local file write touches just the 200 bytes.

**Common mistake:** Treating an object store as a filesystem. The unit of change is the whole object — the tools that thrive on S3 (Parquet, Iceberg, Delta) are built around immutable files plus new-file appends, never in-place edits.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: appending one click on a local file (tiny in-place write) vs on an object store (whole-object round trip), shown as boxes and arrows with data volumes.

- **Title (bold 15px, `#1a5276`, top center):** "Add One 200-Byte Click to a 1 GB File: Local Disk vs Object Store".
- **Row 1 (y=95), 12px `#444` label "local disk" at x=20:** blue `#2a78d6` rounded box at x=150 labeled "1 GB file on disk" (12px), 3px green arrow to a green `#008300` box at x=430 labeled "seek + write 200 bytes" with bold 12px green "✓ 200 bytes touched".
- **Row 2 (y=205), label "object store" at x=20:** blue box at x=150 "1 GB object in bucket", 3px arrow to an orange `#d95926` box at x=330 labeled "GET 1 GB down", then arrow to a red `#e74c3c` box at x=520 labeled "edit, PUT 1 GB back" with bold 12px red "✗ 2 GB moved for 200 bytes".
- **Box style:** 150–175px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the object is the unit of change — so design for immutable files".
- **Caption (12px `#444`, bottom right):** "sizes illustrative; whole-object writes are documented S3 behavior".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the retail site, its 2,000 GB, per-GB rates, request prices, and the 1 GB / 200-byte append are invented and labeled illustrative; the tier costs `[46.00, 25.00, 8.00, 1.98]` are exact products of 2,000 GB × the stated rates; the 2006 launch, the 99.999999999% (11-nines) durability, the "10 million objects, one loss per ~10,000 years" expectation, whole-object writes, copy+delete rename, the flat-key model, and strong read-after-write consistency since December 2020 are Amazon's documented S3 facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
