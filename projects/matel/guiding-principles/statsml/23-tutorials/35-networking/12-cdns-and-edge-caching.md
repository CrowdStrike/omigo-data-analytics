# CDNs & Edge Caching

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** CDNs & Edge Caching

**Subtitle:** A CDN keeps copies of your files on servers near the reader — so the photo loads from 20 miles away, not from 5,000

## The Lasagna Photo Next Door

**Tags:** `core idea` (blue), `edge servers` (green), `distance` (orange)

- **The blog** — a recipe blog runs on one server in Denver; its lasagna photo weighs 2 MB
- **The reader** — someone in Lisbon opens the recipe; Denver is roughly 5,000 miles of fiber away
- **The CDN** — a network of edge servers scattered near readers, one about 20 miles from Lisbon
- **First visit** — the Lisbon edge doesn't have the photo yet, fetches it once from Denver, keeps a copy
- **Every visit after** — served from the edge's copy; Denver isn't contacted until the copy expires

*Example (italic):* The second Lisbon reader gets the exact same lasagna photo, but it now travels 20 miles instead of 5,000.

**Key point:** A CDN doesn't make the network faster — it moves a copy of the file closer, so the distance the bytes travel shrinks by a factor of hundreds.

### Visualization (canvas `c1`, 720×300)

Flow diagram: a reader in Lisbon, an edge server 20 miles away, and the origin server 5,000 miles away, with the cache-miss path dashed and the cache-hit path solid.

- **Title (bold 15px, `#1a5276`, top center):** "One Origin Far Away, One Cached Copy Next Door".
- **Layout:** reader circle (radius 18, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) at (100, 160) labeled "reader, Lisbon" (12px `#2c3e50` below); edge rounded box 150×44 (8px radius, fill `rgba(0,131,0,0.12)`, 2px `#008300` border) at x=230 y=138 labeled "edge server — 20 mi"; origin rounded box 160×44 (fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) at x=530 y=138 labeled "origin, Denver — 5,000 mi".
- **Cache-miss path (dashed `#6b7280`, dash 5/4, 2px):** arrow from edge box right edge to origin box, 12px `#6b7280` label above it "first request only (miss)".
- **Cache-hit path (solid `#008300`, 3px):** double-headed arrow between reader and edge, bold 12px `#008300` label above "every request after (hit)".
- **Distance ruler:** thin `#e5e9ef` line at y=250 from x=100 to x=690 with 11px `#6b7280` ticks "20 mi" under the edge box and "5,000 mi" under the origin box.
- **Annotation (bold 13px blue `#2a78d6`, near x=380, y=70):** "the photo moves once; readers hit the copy".
- **Caption (12px `#444`, bottom right):** "cities and distances illustrative".

## Counting the Milliseconds: 5,000 Miles vs 20

**Tags:** `worked example` (blue), `speed of light` (green), `round trips` (orange)

- **Fiber rule** — light in glass covers about 200 km per ms one way, so a round trip costs ~1 ms per 100 km
- **Origin trip** — 5,000 miles ≈ 8,000 km, so one round trip to Denver ≈ 8,000 / 100 = 80 ms
- **Handshakes** — DNS, TCP, TLS, then the GET itself: about 4 round trips before the first pixel
- **Origin total** — 4 × 80 ms = 320 ms of pure distance before the photo even starts arriving
- **Edge total** — 20 miles ≈ 32 km, round trip ≈ 0.32 ms, so 4 × 0.32 ≈ 1.3 ms — about 250× less waiting

*Example (italic):* Same reader, same photo, same 4 handshakes — 320 ms of travel time from Denver versus 1.3 ms from the edge 20 miles away.

**Key point:** Latency is bought in round trips, and each round trip is priced by distance — the CDN wins by shrinking the per-trip price from 80 ms to a third of a millisecond.

### Visualization (canvas `c2`, 720×300)

Horizontal stacked bar chart: the 4 round trips to fetch the photo, origin row vs edge row, each round trip a labeled segment.

- **Title (bold 15px, `#1a5276`, top center):** "Four Round Trips Before the First Pixel: 320 ms vs 1.3 ms".
- **Axes:** baseline x=170, bars extend right, max width 480; x scale 0–320 ms with gridlines `#e5e9ef` at 80/160/240/320 and 11px `#6b7280` tick labels "80ms"–"320ms" below y=240.
- **Origin row (y=105, 34px tall), left label 12px `#444` at x=20 "origin — 5,000 mi":** four segments each 120px wide (80 ms each) at cumulative ms `[80, 160, 240, 320]`, fills blue `rgba(42,120,214,0.30)` / aqua `rgba(25,158,112,0.30)` / violet `rgba(74,58,167,0.25)` / yellow `rgba(201,133,0,0.30)`, 2px `#2a78d6` outer border, 11px `#2c3e50` segment labels "DNS", "TCP", "TLS", "GET" centered in each.
- **Edge row (y=185, 34px tall), left label "edge — 20 mi":** one solid green `#008300` bar 4px wide (1.3 ms is sub-pixel at this scale, drawn at minimum 4px), bold 12px `#008300` label to its right "1.3 ms total (0.32 ms per trip)".
- **Annotation (bold 13px green `#008300`, near x=420, y=215):** "~250× less waiting, purely from distance".
- **Caption (12px `#444`, bottom right):** "1 ms per 100 km round trip; handshake count simplified, times illustrative".

## One Origin, a Million Requests

**Tags:** `where it's used` (blue), `hit ratio` (green), `origin offload` (orange)

- **The metric** — cache hit ratio: the share of requests answered by an edge copy instead of the origin
- **The scale** — the blog's photos draw 1,000,000 requests a day; the Denver server alone would melt
- **At 95% hits** — only 1 request in 20 reaches Denver: 50,000 a day, a load one machine can handle
- **In the logs** — a data scientist analyzing traffic sees edge logs, not origin logs; counting origin hits undercounts readers 20×
- **The dashboard** — page-speed and bounce-rate numbers move with hit ratio; a cold cache shows up as a latency spike

*Example (italic):* Raising the hit ratio from 90% to 99% cuts origin traffic from 100,000 to 10,000 requests a day — a 10× drop from 9 points of hits.

**Key point:** The hit ratio runs the show: it decides both how fast readers feel the site is and how much traffic (and cost) the origin actually carries.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: origin requests per day at increasing cache hit ratios, out of 1,000,000 total daily requests.

- **Title (bold 15px, `#1a5276`, top center):** "1,000,000 Daily Requests: What Still Reaches the Origin".
- **Axes:** origin x=80, baseline y=245, plot width 580, plot height 180; y = origin requests 0 to 1,000,000, gridlines `#e5e9ef` at 250k/500k/750k with 11px `#6b7280` labels "250k"–"1M"; x = five bars with 12px `#444` labels under each.
- **Bars (80px wide, centered at x = 140, 255, 370, 485, 600):** hit ratios `["0%", "50%", "90%", "95%", "99%"]`, origin requests `[1000000, 500000, 100000, 50000, 10000]`; fills blue `rgba(42,120,214,0.35)` for 0%/50%, aqua `rgba(25,158,112,0.35)` for 90%, green `rgba(0,131,0,0.35)` for 95%/99%; 2px matching solid borders; bold 12px `#2c3e50` value labels above each bar ("1M", "500k", "100k", "50k", "10k").
- **Annotation (bold 13px green `#008300`, near x=430, y=90):** "at 95% hits, the origin sees 1 request in 20".
- **Caption (12px `#444`, bottom right):** "request volumes illustrative; arithmetic exact".

## Fast Is Not the Same as Fresh

**Tags:** `common mistake` (red), `stale cache` (orange)

- **The trap** — the edge serves whatever copy it holds; if the origin's file changed, readers get the old one
- **TTL** — each cached copy carries a time-to-live; the blog's photos use 24 hours before an edge re-fetches
- **The stagger** — edges cached the photo at different times, so their copies expire at different hours
- **Hand-check** — 12 hours after the origin swaps the photo, about half the edges still serve the old lasagna
- **The fixes** — purge the file from all edges explicitly, or publish under a new versioned name (`lasagna-v2.jpg`)
- **Never cache** — personalized pages (a user's cart, their account) must not sit on a shared edge copy

*Example (italic):* The blogger replaces a mislabeled photo at 9am; without a purge, some readers still see the wrong dish at 8am the next day.

**Common mistake:** Assuming an update to the origin updates the world. A CDN trades freshness for speed — until the TTL expires or you purge, the edge happily serves yesterday's file.

### Visualization (canvas `c4`, 720×300)

Line chart: share of readers seeing the new photo in the 24 hours after the origin update — TTL-only expiry (slow ramp) vs an explicit purge (instant step).

- **Title (bold 15px, `#1a5276`, top center):** "Photo Replaced at Hour 0: Who Actually Sees the New One?".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hours after update 0 to 24, 12px `#444` tick labels "0h", "6h", "12h", "18h", "24h"; y = readers seeing new photo 0–100%, gridlines `#e5e9ef` at 25/50/75 with 11px `#6b7280` labels.
- **TTL-only line:** orange `#d95926` 3px line through hours `[0, 6, 12, 18, 24]`, percent new `[0, 25, 50, 75, 100]` — a straight ramp as staggered 24h TTLs expire.
- **Purge line:** green `#008300` 3px line through hours `[0, 0.5, 24]`, percent new `[0, 100, 100]` — a near-vertical step at hour 0.5, then flat.
- **Stale zone label (bold 12px orange `#d95926`, near x=13h, y=170 under the ramp):** "still serving the old photo".
- **Update marker:** vertical dashed `#6b7280` (dash 4/3) line at hour 0, 12px `#6b7280` label "origin updated" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near x=15h, y=70):** "at hour 12, half the edges are still stale".
- **Caption (12px `#444`, bottom right):** "uniformly staggered 24h TTLs, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); distances (20 mi / 5,000 mi), request volumes (1M daily; origin loads 1,000,000 / 500,000 / 100,000 / 50,000 / 10,000 at hit ratios 0/50/90/95/99%), and TTL-ramp percents (0/25/50/75/100 at hours 0/6/12/18/24) are invented and labeled illustrative; the latency arithmetic (~1 ms round trip per 100 km, 80 ms × 4 = 320 ms vs 0.32 ms × 4 ≈ 1.3 ms) follows from the speed of light in fiber and is hand-checkable.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
