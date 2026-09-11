# P2P & BitTorrent

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** P2P & BitTorrent

**Subtitle:** Every downloader becomes an uploader — files split into hash-checked pieces so a swarm can move what no single server could serve

## The Download That Gets Faster as the Crowd Grows

**Tags:** `core idea` (blue), `swarms` (green), `seeding` (orange)

- **One server** — a 100 Mbps pipe split 50 ways gives each downloader just 2 Mbps
- **The flip** — in a torrent every downloader uploads too, so each arrival adds capacity
- **The swarm** — everyone trading pieces of one file forms a single group of peers
- **Seeders** — peers holding the complete file; they only upload, asking nothing back
- **Leechers** — peers still downloading; they upload the pieces they already hold
- **Illustrative math** — each peer adds 20 Mbps of upload: 50 peers share ~22 Mbps apiece

*Example (italic):* Alice's OS image crawls at 2 Mbps from a busy mirror but streams at 22 Mbps from its swarm.

**Key point:** A server divides a fixed pipe among downloaders; a swarm grows the pipe with every downloader who joins.

### Visualization (canvas `c1`, 720×300)

Two-line chart of speed per downloader as the crowd grows — the server line collapses, the swarm line levels off high.

- **Title (bold 15px, `#1a5276`, top center):** "Speed per Downloader as the Crowd Grows (illustrative)".
- **Caption under title (11px `#6b7280`, centered, y=40):** "assumes a 100 Mbps server pipe; each peer uploads 20 Mbps".
- **Axes:** x = 6 evenly spaced points for downloader counts 1 / 2 / 5 / 10 / 20 / 50 mapped px 60–560; y = Mbps 0–100 mapped baseline y=250 up to y=50; 1px `#999` axis lines; x tick labels 12px `#444` ("1", "2", "5", "10", "20", "50"); x caption "simultaneous downloaders" 12px `#444` at y=284; y labels "0", "50", "100" 12px `#444` at left.
- **Server line (hardcoded):** (1,100), (2,50), (5,20), (10,10), (20,5), (50,2) — 3px orange `#d95926`, 4px dots.
- **Swarm line (hardcoded):** (1,100), (2,60), (5,36), (10,28), (20,24), (50,22) — 3px green `#008300`, 4px dots.
- **Legend (x=580, squares 12px + 12px `#222` text):** green "swarm (P2P)" at y=60, orange "single server" at y=80.
- **Annotations:** bold 13px green `#008300` "swarm holds ~22 Mbps each" above the swarm line near x=430, y=180; bold 13px orange `#d95926` "server: 2 Mbps each" below the server line near x=450, y=240.

## Alice Assembles 400 MB From Five Peers at Once

**Tags:** `worked example` (blue), `chunks` (orange)

- **Split first** — the 400 MB image becomes 100 pieces of 4 MB each, numbered 1 to 100
- **Hash check** — the torrent file lists a fingerprint per piece; corrupt pieces get refetched
- **Any order** — Alice pulls piece 41 from Bob while piece 7 arrives from Carol
- **Rarest first** — she asks for pieces fewest peers hold, keeping every piece easy to find
- **Do the math** — 5 peers at 2 MB/s each is 10 MB/s: 400 MB lands in 40 s, not 200 s
- **No full copy needed** — 100 peers each holding a different 1% still add up to the whole file

*Example (italic):* The moment a verified piece lands, Alice starts uploading it — she is a source before she finishes.

**Key point:** Chunking plus per-piece hashes let a file arrive from many strangers at once and still be bit-perfect.

### Visualization (canvas `c2`, 720×300)

Left: a 10×10 grid of Alice's 100 pieces colored by which of five peers supplied each. Right: the five peers with their contribution, then the speed arithmetic.

- **Title (bold 15px, `#1a5276`, top center):** "One File, 100 Pieces, Five Sources at Once".
- **Divider:** dashed 1px `#bdc3c7` vertical line at x=250 from y=40 to y=285.
- **Piece grid (left):** 10×10 cells, 16px each with 1px gap, origin x=40, y=70; piece i (0–99) filled with source-peer color `SRC[(i*3)%5]` where `SRC = [P.blue, P.green, P.violet, P.orange, P.aqua]` — the (i*3)%5 rule interleaves colors and gives each peer exactly 20 pieces; each cell stroked 1px white.
- **Grid captions (centered x=120):** 11px `#444` "100 pieces × 4 MB, colored by source peer" at y=252; bold 11px ink `#1a5276` "every piece hash-checked on arrival" at y=270.
- **Peer rows (right, starting x=285):** five rows at y=70+i*30 — 12px color swatch in `SRC[i]` + 12px `#2c3e50` label: "Bob — 20 pieces (80 MB) @ 2 MB/s", "Carol — 20 pieces (80 MB) @ 2 MB/s", "Dave — 20 pieces (80 MB) @ 2 MB/s", "Erin — 20 pieces (80 MB) @ 2 MB/s", "Frank — 20 pieces (80 MB) @ 2 MB/s".
- **Annotations (left-aligned x=285):** bold 13px green `#008300` "5 peers × 2 MB/s = 10 MB/s" at y=240; bold 13px orange `#d95926` "400 MB ÷ 10 MB/s = 40 s (one peer: 200 s)" at y=262.
- **Caption (11px `#6b7280`, x=285, y=282):** "speeds illustrative".

## Where the Same Machinery Ships Legitimate Bytes

**Tags:** `where it's used` (blue), `scale` (green)

- **OS images** — Linux distributions publish torrents so release-day crowds feed themselves
- **Game updates** — a 20 GB patch to 1 million players is 20 PB; no single pipe serves that
- **Fleet rollouts** — data centers push builds peer-to-peer so one artifact store isn't crushed
- **Datasets** — research groups seed large public datasets; every lab that grabs one is a mirror
- **Resilience** — no single point of failure: the swarm survives any one machine going away

*Example (italic):* A data scientist grabbing a 300 GB public web crawl by torrent is re-serving it while it downloads.

**Key point:** When the audience is huge and the file is huge, letting the crowd carry the bytes is often the only design that scales.

### Visualization (canvas `c3`, 720×300)

Two stacked bars comparing who carries the bytes of a 20 PB release: central-only vs peer-assisted.

- **Title (bold 15px, `#1a5276`, top center):** "Delivering a 20 GB Update to 1 Million Players (illustrative)".
- **Axes:** y = petabytes 0–20 mapped baseline y=240 up to y=70; 1px `#999` y axis at x=90 and baseline; y labels "0", "10", "20 PB" 12px `#444` at left.
- **Bar 1 (x=160, width 130):** single orange segment, fill `rgba(217,89,38,0.35)`, 2px `#d95926` border, full height 20 PB; bold 12px `#d95926` in-bar label "20 PB from origin servers"; 12px `#444` label "central servers only" under the bar at y=262.
- **Bar 2 (x=430, width 130):** bottom orange segment 1 PB (fill `rgba(217,89,38,0.35)`, 2px `#d95926` border) labeled bold 12px `#d95926` "origin: 1 PB" just above the bar top area; upper green segment 19 PB (fill `rgba(0,131,0,0.22)`, 2px `#008300` border) with bold 12px `#008300` in-bar label "peers: 19 PB"; 12px `#444` label "peer-assisted swarm" under the bar at y=262.
- **Annotation (bold 13px green `#008300`, centered x=495, y=60):** "peers carry 95% of the bytes".
- **Caption (11px `#6b7280`, bottom right):** "volumes illustrative: 20 GB × 1,000,000 = 20 PB".

## An Architecture, Not a Legality Verdict

**Tags:** `common mistake` (red), `same rules as HTTP` (blue)

- **The confusion** — "torrent" is often heard as "piracy"; it is only a way to move bytes
- **What decides legality** — the content's license and your right to copy it, not the pipe
- **Same file, two pipes** — a freely licensed OS image is fine over HTTP and fine over BitTorrent
- **Why the reputation** — swarms made unlicensed sharing easy at scale, so the tool took the blame
- **Peers are visible** — everyone in a swarm can see fellow peers' network addresses

*Example (italic):* Alice's torrent of a free OS image and Bob's torrent of a paid film use identical machinery — one is fine, one is not.

**Key point:** Judge the content and the license, not the protocol — P2P describes how bytes move, never whether they may.

### Visualization (canvas `c4`, 720×300)

A 2×2 grid: transfer method (rows) × content rights (columns) — the verdict tracks the column, never the row.

- **Title (bold 15px, `#1a5276`, top center):** "Method × Content: Where Legality Lives".
- **Column headers (bold 12px `#1a5276`, centered over each column, y=68):** "openly licensed content" at x=282, "no right to copy" at x=522.
- **Row labels (12px `#444`, right-aligned at x=162, vertically centered per row):** "direct download (HTTP)" and "torrent (P2P swarm)".
- **Cells (225×70, col1 x=170, col2 x=410, row1 y=80, row2 y=160):** both left cells fill `rgba(39,174,96,0.15)` with 2px `#27ae60` border and bold 14px `#27ae60` centered text "fine"; both right cells fill `rgba(231,76,60,0.12)` with 2px `#e74c3c` border and bold 14px `#e74c3c` centered text "infringement" (red marks a genuine violation state).
- **Annotation (bold 13px ink `#1a5276`, centered x=360, y=260):** "the verdict follows the column (content), never the row (method)".
- **Caption (11px `#6b7280`, centered x=360, y=282):** "the pipe is identical in both rows".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" or "Common mistake:" for the last section).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` appears only in tag pills, key-point borders, and the c4 "infringement" cells (a genuine violation state).
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Everything numeric is illustrative and labeled so in charts. Arithmetic that must stay consistent between text and charts: 100 Mbps ÷ 50 = 2 Mbps; swarm per-peer speed = 20 + 80/n Mbps → (1,100), (2,60), (5,36), (10,28), (20,24), (50,22); 400 MB = 100 pieces × 4 MB; 5 × 2 MB/s = 10 MB/s; 400 ÷ 10 = 40 s vs 400 ÷ 2 = 200 s from one peer; 20 GB × 1,000,000 = 20 PB; 19/20 = 95% carried by peers.
- This page has no links.
