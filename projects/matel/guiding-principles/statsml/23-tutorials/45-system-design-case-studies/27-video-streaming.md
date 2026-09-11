# Video Streaming

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Video Streaming

**Subtitle:** Video at planetary scale — the website is ordinary, the video bytes are not, and the CDN is the product (a generic design exercise using well-known streaming concepts)

## One Upload Becomes 750 Chunks

**Tags:** `core idea` (blue), `upload pipeline` (green), `transcoding` (orange)

- **The upload** — a creator posts a 10-minute video; one 4K master file arrives at the service
- **The ladder** — servers transcode it into 5 renditions: 240p, 480p, 720p, 1080p, and 4K
- **The chunks** — each rendition is cut into 4-second pieces: 600s / 4s = 150 chunks per rendition
- **The manifest** — a small index file lists every chunk at every quality, so a player can fetch any piece
- **The payoff** — one upload becomes 5 × 150 = 750 addressable files that fit any device and network

*Example (italic):* A phone on weak coffee-shop wifi and a living-room 4K TV both play the same 10-minute video — they just fetch different rungs of the same 750-chunk ladder.

**Key point:** Transcoding into a bitrate ladder and chunking turn one big video into many small interchangeable pieces — that is what makes quality switching, seeking, and caching possible at all.

### Visualization (canvas `c1`, 720×300)

Fan-out flow diagram: one source box on the left feeding five rendition rows, each drawn as a strip of chunk segments.

- **Title (bold 15px, `#1a5276`, top center):** "One 4K Upload → 5 Renditions × 150 Chunks = 750 Files".
- **Source box:** blue `#2a78d6` rounded box (8px radius, fill `rgba(42,120,214,0.15)`) at x=25, y=122, 135×56, two-line 12px `#2c3e50` label "4K master / 10-min upload".
- **Fan-out arrows:** five 2px `#6b7280` lines from the box's right edge (x=160, y=150) to each row's label start (x=225) at row centers.
- **Rendition rows (top to bottom at y = 62, 107, 152, 197, 242):** left-aligned 12px `#444` label at x=230 — "240p — 0.3 Mbps", "480p — 1 Mbps", "720p — 3 Mbps", "1080p — 5 Mbps", "4K — 16 Mbps"; then a chunk strip from x=395 to x=665: 15 segments each 16px wide, 18px tall, 2px gaps, one color per row: blue `#2a78d6`, aqua `#199e70`, green `#008300`, orange `#d95926`, violet `#4a3aa7`, all at 0.55 alpha with a solid 1px border in the same hue.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=282):** "any device can start at any chunk, at any quality".
- **Caption (11px `#444`, bottom right):** "15 boxes drawn per row — each stands for 10 chunks; ladder bitrates illustrative".

## Picking a Quality Every Four Seconds

**Tags:** `worked example` (blue), `adaptive bitrate` (green)

- **The player** — before fetching each 4-second chunk, the client measures its recent download speed
- **The rule** — pick the highest rendition whose bitrate fits under measured bandwidth, with a safety margin
- **The dip** — at t=20s wifi drops to 1.5 Mbps; the next 720p chunk (3 Mbps) would drain the buffer and stall
- **The switch** — the client fetches the next chunk at 480p (1 Mbps) instead; playback never pauses
- **The recovery** — by t=40s bandwidth is back to 6 Mbps and the player steps up to 1080p (5 Mbps)

*Example (italic):* Over this 60-second stretch the player serves chunks at three different qualities — 720p, 480p, then 1080p — and the viewer never sees a buffering wheel.

**Key point:** Adaptive bitrate streaming is a client-side loop: measure bandwidth, pick the quality of the next chunk, repeat — the server just hosts the ladder and stays dumb.

### Visualization (canvas `c2`, 720×300)

Two-line chart over 60 seconds of playback: measured bandwidth (smooth line) vs the bitrate of the chunk the player chose (step line).

- **Title (bold 15px, `#1a5276`, top center):** "One Wifi Dip, Handled Chunk by Chunk".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = playback seconds 0 to 60 with 12px `#444` tick labels every 10s; y = Mbps 0 to 8, gridlines `#e5e9ef` at 2/4/6 with 12px `#444` labels.
- **Bandwidth line:** blue `#2a78d6` 3px line through seconds `[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]`, Mbps `[8, 7.5, 6, 3, 1.5, 1.4, 2.5, 4, 6, 7, 8, 8, 7.5]`; 12px blue label "measured bandwidth" near (8s, y of 8 Mbps).
- **Chosen-bitrate step line:** green `#008300` 3px horizontal-step line on the same second grid, Mbps `[3, 3, 3, 1, 1, 1, 1, 3, 5, 5, 5, 5, 5]` (720p → 480p → 1080p); 12px green label "chunk quality picked" near (48s, y of 5 Mbps + 14px).
- **Dip marker:** vertical dashed `#6b7280` (dash 4/3) line at t=20s, 12px `#6b7280` label "wifi dip" at its top.
- **Annotation (bold 13px green `#008300`, near t=25s, y=200):** "drops to 480p — buffer never empties".
- **Annotation (bold 13px orange `#d95926`, near t=42s, y=95):** "back up to 1080p at t=40s".
- **Caption (12px `#444`, bottom right):** "bandwidth trace illustrative".

## Why the CDN Is the Product

**Tags:** `where it's used` (blue), `edge caching` (green), `popularity skew` (orange)

- **The bytes** — one hour of 1080p at 5 Mbps is ~2 GB; the browse page that launched it is ~2 MB — a 1000× gap
- **The skew** — popularity is heavily skewed: the top 10% of videos draw ~90% of watch traffic (illustrative)
- **The edge** — CDN servers near users cache that hot set, so most chunks arrive from a few milliseconds away
- **The origin** — the full catalog sits in cheap origin storage; only cache misses travel back to it
- **The split** — metadata, search, and recommendations are a normal web app; only the video bytes are special

*Example (italic):* A clip watched a million times is uploaded once and transcoded once — after that, edge caches serve almost every view and origin sees only a sliver of the traffic.

**Key point:** When one asset type dominates the bytes and its popularity is heavily skewed, the winning architecture is "put the hot bytes at the edge" — the CDN stops being plumbing and becomes the product.

### Visualization (canvas `c3`, 720×300)

Cumulative-share curve: percent of videos (ranked by popularity) on x vs cumulative percent of watch traffic on y, with the top-10% point called out.

- **Title (bold 15px, `#1a5276`, top center):** "Popularity Skew: Top 10% of Videos = 90% of Watch Traffic".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = "% of videos, most popular first" 0 to 100, 12px `#444` ticks every 25; y = "cumulative % of traffic" 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Skew curve:** blue `#2a78d6` 3px line through video-percent `[0, 1, 5, 10, 25, 50, 100]`, cumulative traffic `[0, 60, 82, 90, 96, 99, 100]`, with fill `rgba(42,120,214,0.25)` down to the baseline.
- **Equal-share reference:** dashed `#6b7280` (dash 4/3) diagonal from (0,0) to (100,100), 11px `#6b7280` label "if every video were equally popular" along it.
- **Guide lines:** dashed `#6b7280` vertical at x=10% up to the curve and horizontal from that point to the y-axis, marking (10, 90); 3px green `#008300` dot at the point.
- **Annotation (bold 13px green `#008300`, near x=35%, y=95):** "cache the top 10% at the edge → 90% of bytes never touch origin".
- **Caption (12px `#444`, bottom right):** "skew curve illustrative".

## Don't Push Video Bytes Through the App Servers

**Tags:** `common mistake` (red), `origin vs edge` (orange)

- **The confusion** — treating video like any other API response and streaming bytes through the app servers
- **The blow-up** — every viewer pulling 5 Mbps through origin melts its network long before any CPU is busy
- **The split** — the app server returns a small manifest with chunk URLs; the CDN serves the chunks themselves
- **The other trap** — one whole file per quality, not chunks, makes mid-stream quality switches impossible
- **The hand-check** — 100,000 concurrent 1080p viewers × 5 Mbps = 500 Gbps; 90% edge hits leave origin 50 Gbps

*Example (italic):* Same 100,000 viewers: everything through origin needs 500 Gbps of egress; with edge caches at a 90% hit rate, origin carries 50 Gbps and the CDN absorbs the rest.

**Common mistake:** Designing the video path and the web path as one system. The manifest is an ordinary web request; the chunks are a byte-delivery problem that belongs to the edge — mixing them makes origin bandwidth the first thing to fall over.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the naive all-through-origin design vs the split design, with origin egress numbers on each.

- **Title (bold 15px, `#1a5276`, top center):** "Same 100,000 Viewers, Two Designs".
- **Row 1 (y=95), label 12px `#444` at x=20:** "all through origin"; blue `#2a78d6` rounded box at x=165 labeled "100k players" (12px), 3px arrow to a red `#e74c3c` box at x=400 labeled "app servers + origin" with bold 12px red "✗ 500 Gbps egress" beneath it.
- **Row 2 (y=205), label:** "split design"; blue box "100k players" at x=165, 3px arrow to a green `#008300` box at x=350 labeled "edge CDN — 90% hits", then a thinner 2px arrow to a blue box at x=555 labeled "origin (misses only)" with bold 12px green "✓ 50 Gbps" beneath it.
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "the manifest is a web request; the chunks are a CDN problem".
- **Caption (11px `#444`, bottom right):** "5 Mbps per 1080p viewer; rates illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); ladder bitrates, the bandwidth trace, the popularity-skew curve, and the 90% hit rate are invented and labeled illustrative; the arithmetic is exact given those inputs (600s / 4s = 150 chunks, 5 × 150 = 750 files, 100,000 × 5 Mbps = 500 Gbps, 10% residual = 50 Gbps).
- **Framing:** this is a generic design exercise built from publicly well-known streaming concepts (bitrate ladders, chunked adaptive streaming, edge CDNs); it makes no claims about any real company's current internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
