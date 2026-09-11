# Web Crawler

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Web Crawler

**Subtitle:** A crawler is BFS over the internet, done politely — fetch a page, collect its links, queue the ones you haven't seen, and never hammer any single site

## BFS, but the Graph Is the Internet

**Tags:** `core idea` (blue), `BFS` (green), `frontier` (orange)

- **The seeds** — the crawl starts from 3 hand-picked pages: a news homepage, a wiki portal, a directory
- **The fetch** — download each page, parse the HTML, and pull out every outgoing link
- **The enqueue** — links never seen before go to the back of a queue called the frontier
- **The loop** — pop a URL, fetch, extract, enqueue unseen; repeat until the queue empties (it never does)
- **The name** — this is breadth-first search: the frontier is the BFS queue, the seen-set is the visited set

*Example (italic):* From 3 seeds averaging 10 new links per page, depth 1 holds 30 URLs, depth 2 holds 300, depth 3 holds 3,000 — the frontier grows tenfold per hop.

**Key point:** A web crawler is BFS where the graph is discovered as you walk it — the frontier queue and the seen-set are the whole algorithm; everything else is scale and manners.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart of frontier size by crawl depth: 3 seeds exploding tenfold per hop, bar heights on a log-feel schematic scale.

- **Title (bold 15px, `#1a5276`, top center):** "BFS Over the Web: 3 Seeds Become 3,000 URLs in Three Hops".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = four bars centered at x = 150, 300, 450, 600 with 12px `#444` labels "depth 0 (seeds)", "depth 1", "depth 2", "depth 3"; no y gridlines (schematic scale).
- **Bars:** 70px wide, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border; heights 25, 75, 125, 175 px (log-feel, hardcoded); bold 13px `#1a5276` value labels "3", "30", "300", "3,000" centered above each bar.
- **Arrows:** 2px `#6b7280` curved arrows between consecutive bar tops, each with 12px `#6b7280` label "×10".
- **Annotation (bold 13px green `#008300`, upper left near x=90, y=60):** "each hop multiplies the frontier ×10".
- **Caption (12px `#444`, bottom right):** "10 new links per page illustrative; bar heights schematic (log)".

## One Request Per Second, Per Host

**Tags:** `worked example` (blue), `politeness` (green), `robots.txt` (orange)

- **The rule** — before fetching, read the site's robots.txt: it lists paths the owner asks crawlers to skip
- **The cap** — the frontier keeps one sub-queue per host and releases at most 1 request per second per host
- **The math** — a 10,000-page site at 1 req/s takes 10,000 seconds ≈ 2.8 hours, however many machines you own
- **The breadth** — 1,000 polite hosts in parallel deliver 1,000 pages/s; a million hosts, a million pages/s
- **The failure** — 1,000 req/s at one host looks like an attack: 403 errors, IP bans, an angry site owner

*Example (italic):* Crawling one 10,000-page site politely takes ~2.8 hours; crawling a million sites at the same polite rate yields a million pages every second.

**Key point:** Politeness caps the crawl rate per host, so throughput comes from breadth across millions of hosts — you scale sideways, never by hitting one site harder.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: crawl throughput at a fixed polite per-host rate as breadth grows, plus one red bar for the impolite alternative.

- **Title (bold 15px, `#1a5276`, top center):** "Same 1 req/s Per Host — Throughput Comes From Breadth".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "1 host × 1 req/s = 1 page/s": blue `#2a78d6` bar width 40
  - "1,000 hosts × 1 req/s = 1,000 pages/s": blue bar width 230
  - "1,000,000 hosts × 1 req/s = 1M pages/s": green `#008300` bar width 420
  - "1 host × 1,000 req/s": red `#e74c3c` bar width 120, bold 12px red label at bar end "banned — politeness violated"
- **Bar style:** 16px tall, blue bars fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, green bar fill `rgba(0,131,0,0.25)` with 2px `#008300` border, red bar solid; 11px `#444` width labels at bar ends for the first three rows.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "the per-host cap is fixed; only breadth scales".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic (log); rates exact from the arithmetic shown".

## Have I Seen This URL Before? Ten Billion Times

**Tags:** `dedupe at scale` (blue), `Bloom filter` (green), `hashing` (orange)

- **The check** — every extracted link is tested against the seen-set before it may join the frontier
- **The naive set** — 10 billion URLs at ~80 bytes each is 800 GB of raw strings; too big for RAM
- **The hash** — store an 8-byte fingerprint per URL instead: 80 GB, better but still heavy
- **The Bloom filter** — a bit array plus a few hash functions: 10 bits per URL is 12.5 GB total
- **The trade** — a Bloom "seen" is probabilistic (~1% false positives); a Bloom "new" is certain
- **The refresh** — dedupe is per crawl cycle: a news homepage is re-queued hourly, an old archive page monthly

*Example (italic):* The 12.5 GB Bloom filter wrongly skips about 1 genuinely new URL in 100 — the price of shrinking an 800 GB seen-set 64-fold to fit in memory.

**Key point:** At billions of URLs, exact dedupe is a memory problem — a Bloom filter answers "definitely new" or "probably seen" in 10 bits per URL, and skipping ~1% of new pages is a cheap price.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing memory for a 10-billion-URL seen-set under three representations.

- **Title (bold 15px, `#1a5276`, top center):** "One Seen-Set, Three Sizes: 10 Billion URLs in Memory".
- **Axis:** vertical 2px `#999` baseline at x=240, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths.
- **Rows (top to bottom at y = 80, 145, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "raw strings — 80 B/URL = 800 GB": red `#e74c3c` bar width 440, fill `rgba(231,76,60,0.25)`, 2px red border
  - "8-byte hashes — 80 GB": orange `#d95926` bar width 220, fill `rgba(217,89,38,0.25)`, 2px orange border
  - "Bloom filter, 10 bits/URL — 12.5 GB": green `#008300` bar width 90, fill `rgba(0,131,0,0.25)`, 2px green border
- **Bar style:** 22px tall; bold 12px matching-color size labels ("800 GB", "80 GB", "12.5 GB") at bar ends.
- **Annotation (bold 13px green `#008300`, below the green bar near y=250):** "64× smaller than raw strings — cost: ~1 in 100 false 'seen'".
- **Caption (12px `#444`, bottom right):** "10 billion URLs; sizes exact from the arithmetic shown, widths schematic (log)".

## The Calendar That Never Ends

**Tags:** `common mistake` (red), `crawler traps` (orange)

- **The trap** — an events page's "next month" link mints ?m=2027-01, 2027-02, ... forever, every URL brand new
- **Session IDs** — the same page served as ?sid=a91f, ?sid=c04b, ... looks like a fresh URL on every visit
- **Content dedupe** — hash the page body (simhash for near-duplicates) so one page at many URLs counts once
- **Depth and priority** — cap link depth (say 15 hops) and rank the frontier so junk waits behind good URLs
- **The mistake** — trusting URL dedupe alone: traps pass it perfectly, because every trap URL really is unseen

*Example (italic):* In a 1,000-fetch crawl of a trapped site, unique content plateaus at 120 pages — the other 880 fetches pull the same calendar in new disguises.

**Common mistake:** New URL ≠ new content. URL dedupe happily waves traps through — bounding a crawl also needs content hashing, depth caps, and frontier priorities.

### Visualization (canvas `c4`, 720×300)

Line chart of a crawl entering a trap: fetches keep climbing while unique content flat-lines.

- **Title (bold 15px, `#1a5276`, top center):** "The Calendar Trap: 1,000 Fetches, 120 Unique Pages".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = fetches 0 to 1,000 with 12px `#444` tick labels every 250; y = unique content pages 0 to 1,000, gridlines `#e5e9ef` at 250/500/750.
- **Reference line:** dashed `#6b7280` (dash 4/3) 2px diagonal from (0,0) to (1000,1000) in data space, 12px `#6b7280` label "every fetch new (ideal)" along it near x=700.
- **Unique-content line:** blue `#2a78d6` 3px line through fetches `[0, 50, 100, 120, 250, 500, 750, 1000]`, unique pages `[0, 50, 100, 118, 120, 120, 120, 120]` — climbs, then plateaus at 120.
- **Trap marker:** vertical dashed red `#e74c3c` (dash 4/3) line at fetch 120, bold 12px red label "crawler enters ?m= loop" at its top.
- **Annotation (bold 13px red `#e74c3c`, near x=600, y=170 in data space y≈250 pages):** "880 fetches, 0 new pages".
- **Caption (12px `#444`, bottom right):** "fetch counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); frontier growth (×10 per hop), trap fetch counts (1,000 fetches / 120 unique), and host counts are invented and labeled illustrative; seen-set sizes (800 GB / 80 GB / 12.5 GB) and throughput rates (1 / 1,000 / 1,000,000 pages/s) follow exactly from the arithmetic stated in the bullets.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
