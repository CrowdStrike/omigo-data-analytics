# GFS & Colossus

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** GFS &amp; Colossus

**Subtitle:** Google's 2003 file system built reliable petabyte storage out of cheap machines that constantly die — the paper that begat HDFS

## A 1TB Log File on a Sea of Cheap Machines

**Tags:** `core idea` (blue), `2003 paper` (green), `replication` (orange)

- **The file** — one day of server logs lands as a single 1TB file, too big for any one disk
- **The split** — GFS cuts every file into fixed 64MB chunks: 1TB / 64MB = 16,384 chunks (exact)
- **The copies** — each chunk is written to 3 different cheap machines: 49,152 replicas in total
- **The master** — a single master maps file names to chunk locations, holding it all in memory
- **The bet** — with thousands of commodity boxes, failure is the norm, not the exception

*Example (italic):* A client asking for byte 700,000,000,000 of the log is pointed at chunk 10,430 and reads it from any of its 3 replicas.

**Key point:** GFS's founding idea — don't buy reliable machines; assume every machine will fail and let the file system repair itself with cheap copies.

### Visualization (canvas `c1`, 720×300)

Block diagram: the 1TB file splitting into 64MB chunks, one chunk fanning out to 3 chunkservers, the master off to the side holding only metadata.

- **Title (bold 15px, `#1a5276`, top center):** "1TB File → 16,384 Chunks of 64MB → 3 Replicas Each".
- **File bar:** rounded rect x=60, y=45, width 600, height 26, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, centered 12px `#2c3e50` label "server-logs.txt — 1TB".
- **Chunk row (y=100, height 26):** six 60px-wide rounded boxes at x = 60, 130, 200, 270, 340, 410, fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, 11px labels "c0", "c1", "c2", "…", "c16382", "c16383"; thin 1px `#e5e9ef` lines from the file bar bottom edge down to the chunk row.
- **Master box:** rounded rect x=530, y=95, width 160, height 44, fill `rgba(217,89,38,0.12)`, 2px `#d95926` border, 12px `#2c3e50` two-line label "master — name → chunk locations"; dashed `#6b7280` (dash 4/3) line from its left edge to the chunk row.
- **Replica fan-out:** three 2px `#008300` arrows from chunk "c2" (x=200) down to three green boxes at y=190 (width 130, height 34, fill `rgba(0,131,0,0.12)`, 2px `#008300` border) at x = 90, 280, 470, labeled 12px "chunkserver 4", "chunkserver 17", "chunkserver 251".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=255):** "16,384 × 3 = 49,152 replicas; the master stores locations, never data".
- **Caption (12px `#444`, bottom right):** "chunk count exact: 1TB / 64MB; server ids illustrative".

## A Machine Dies Mid-Read and Nothing Is Lost

**Tags:** `worked example` (blue), `failover` (green)

- **The read** — an analytics job streams the 1TB log chunk by chunk at roughly 96 MB/s (illustrative)
- **The death** — at second 24 the chunkserver holding the current replica loses power
- **The retry** — the client already knows two other replica locations and asks the next one
- **The stall** — the failover costs about 2 seconds (illustrative); not one byte is lost
- **The repair** — the master misses the heartbeat and re-copies that machine's chunks back to 3

*Example (italic):* At second 24 chunkserver 17 dies mid-chunk; by second 26 the job is reading the same chunk from chunkserver 251.

**Key point:** Replication turns a machine death into a 2-second detour — the client fails over on its own, and the master restores the third copy in the background.

### Visualization (canvas `c2`, 720×300)

Timeline chart of read throughput over 60 seconds: steady ~96 MB/s, a cliff to 0 when the chunkserver dies at second 24, full recovery 2 seconds later from a replica.

- **Title (bold 15px, `#1a5276`, top center):** "Second 24: a Chunkserver Dies, the Read Barely Notices".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 60 with 12px `#444` tick labels every 10s; y = read MB/s 0 to 120, gridlines `#e5e9ef` at 30/60/90.
- **Throughput line:** blue `#2a78d6` 3px line through seconds `[0, 6, 12, 18, 23, 24, 25, 26, 32, 38, 44, 50, 56, 60]`, MB/s `[95, 97, 96, 98, 97, 0, 0, 94, 97, 96, 95, 98, 97, 96]` — vertical cliff to 0 at second 24, vertical recovery at second 26.
- **Death marker:** vertical dashed red `#e74c3c` (dash 4/3) line at second 24, bold 12px red label "chunkserver 17 dies" at its top.
- **Recovery label:** 12px green `#008300` label "now reading chunkserver 251" near second 27, y=210, with a short green arrow to the recovery point.
- **Annotation (bold 13px green `#008300`, near second 42, y=80):** "2-second stall, zero bytes lost".
- **Caption (12px `#444`, bottom right):** "throughput and stall length illustrative".

## The Paper That Begat HDFS

**Tags:** `where it's used` (blue), `open source` (green), `Hadoop` (orange)

- **The paper** — Google published the GFS design at SOSP 2003 instead of keeping it a secret
- **The clone** — Doug Cutting's team rebuilt it in open source; it shipped as HDFS inside Hadoop (2006)
- **The echo** — HDFS kept the shape: big blocks, 3× replication, one NameNode playing the master
- **The reach** — Hive, HBase, and early Spark all sat on HDFS files
- **The habit** — data lakes still assume files are huge, append-only, and scanned in bulk

*Example (italic):* HDFS's NameNode is GFS's single master with a new name — same design, same scaling ceiling.

**Key point:** The 2003 GFS paper directly begat HDFS, which carried the design — 64MB-class chunks, 3× replication, one master — to every company that could download Hadoop.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline of the lineage: GFS paper (2003) and MapReduce paper (2004) flowing into open-source Hadoop (2006), with Colossus replacing GFS inside Google (~2010).

- **Title (bold 15px, `#1a5276`, top center):** "From a 2003 Paper to Everyone's Cluster".
- **Timeline:** 2px `#999` horizontal line at y=160 from x=60 to x=660; short ticks and 12px `#444` year labels below at x = 100 ("2003"), 220 ("2004"), 380 ("2006"), 570 ("~2010").
- **Boxes (8px radius, 12px `#2c3e50` text):**
  - blue `rgba(42,120,214,0.15)` / 2px `#2a78d6` box, x=45, y=95, 130×40, "GFS paper" — above the 2003 tick
  - blue box, x=165, y=190, 130×40, "MapReduce paper" — below the 2004 tick
  - green `rgba(0,131,0,0.12)` / 2px `#008300` box, x=305, y=88, 170×52, "Hadoop: HDFS + MapReduce (open source)" — above the 2006 tick
  - orange `rgba(217,89,38,0.12)` / 2px `#d95926` box, x=500, y=190, 170×46, "Colossus replaces GFS inside Google" — below the ~2010 tick
- **Arrows:** 2px `#6b7280` arrows from the GFS-paper box and MapReduce-paper box to the Hadoop box, and from the GFS-paper box along the line toward the Colossus box.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "one public paper seeded HDFS, Hive, HBase, and the data-lake era".
- **Caption (12px `#444`, bottom right):** "paper and release years exact; Colossus rollout year approximate".

## The Single Master That Colossus Retired

**Tags:** `common mistake` (red), `Colossus` (orange)

- **The ceiling** — the master keeps under 64 bytes per chunk (exact), but RAM still caps total chunks
- **The small-file trap** — a billion 1KB files still cost a billion chunks of master metadata
- **The mistake** — treating the single metadata brain as a detail rather than the hard limit
- **The fix** — Colossus shards metadata across many servers (stored in Bigtable) so it scales out
- **The bonus** — Reed-Solomon erasure coding replaces 3× copies: ~1.5× raw disk for similar safety

*Example (italic):* Storing 1PB costs 3PB of raw disk under 3× copies but about 1.5PB under a 6+3 Reed-Solomon code.

**Common mistake:** Thinking the storage bill was GFS's big flaw. The real ceiling was the single master — Colossus's core change is distributed metadata; erasure coding is the savings on top.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: GFS's one master as a red bottleneck vs Colossus's sharded metadata, with the erasure-coding storage saving as a closing annotation.

- **Title (bold 15px, `#1a5276`, top center):** "The Lesson Colossus Fixed: One Metadata Brain Doesn't Scale".
- **Row 1 (y=95), label 12px `#444` at x=20:** "GFS"; blue `rgba(42,120,214,0.15)` / 2px `#2a78d6` rounded box at x=110, 160×40, labeled "billions of files"; 3px arrow to a red `rgba(231,76,60,0.12)` / 2px `#e74c3c` box at x=360, 200×40, labeled "one master, all metadata in RAM", with bold 12px red `#e74c3c` "✗ hard ceiling" at its right (x≈575).
- **Row 2 (label at y=205):** "Colossus"; blue box at x=110, y=185, 160×40, "billions of files"; 3px arrow fanning to three stacked green `rgba(0,131,0,0.12)` / 2px `#008300` boxes at x=360, 200×24 each, at y = 170, 200, 230, labeled "metadata shard 1", "metadata shard 2", "metadata shard 3 (in Bigtable)", with bold 12px green `#008300` "✓ scales out" at x≈575, y=205.
- **Box style:** 8px radius, 12px `#2c3e50` text, arrows `#6b7280` with filled triangular heads.
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "bonus fix: Reed-Solomon stores 1PB in ~1.5PB raw, not 3PB".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness). Exact facts from the 2003 GFS paper: 64MB chunks, 3× replication, single master with under 64 bytes of metadata per chunk; derived math exact: 1TB / 64MB = 16,384 chunks, × 3 = 49,152 replicas, byte 700,000,000,000 → chunk 10,430; years exact: GFS paper 2003, MapReduce paper 2004, Hadoop 2006 (Colossus rollout ~2010 approximate). Invented and labeled illustrative: read throughput ~96 MB/s, 2-second stall, chunkserver ids 4/17/251. The 1.5× overhead is exact for a 6+3 Reed-Solomon code; Google's actual encodings vary.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
