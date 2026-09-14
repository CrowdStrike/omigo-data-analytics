# HTTP Caching

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** HTTP Caching

**Subtitle:** A browser can keep a copy of a response and reuse it — Cache-Control says how long to trust the copy, and ETag lets it be re-checked for 0.3 KB instead of re-downloaded

## The Same Menu Photo, Three Visits

**Tags:** `core idea` (blue), `browser cache` (green), `HTTP headers` (orange)

- **The photo** — a coffee shop's homepage shows latte.jpg, a 240 KB picture that rarely changes
- **First visit, 9:00** — the browser downloads all 240 KB; the reply says Cache-Control: max-age=3600
- **The fingerprint** — the reply also carries ETag: "v7", a short tag identifying the file's current bytes
- **Second visit, 9:40** — still inside the hour, the browser reuses its saved copy; zero network traffic
- **Third visit, 10:15** — past the hour, the browser asks "still v7?" and the server answers 304 Not Modified
- **The win** — that 304 reply is about 0.3 KB of headers, not the 240 KB photo again

*Example (italic):* Three visits, one download — 240 KB at 9:00, 0 KB at 9:40, 0.3 KB at 10:15; the photo crosses the network exactly once.

**Key point:** HTTP caching lets the browser reuse a saved response: Cache-Control sets how long the copy is trusted, and ETag makes an expired copy cheap to re-check instead of re-fetch.

### Visualization (canvas `c1`, 720×300)

Three-row flow diagram of the same request made three times: full download, cache hit, revalidation — browser boxes on the left, server boxes on the right, bytes labeled on each row.

- **Title (bold 15px, `#1a5276`, top center):** "One Photo, Three Visits: 240 KB, then 0 KB, then 0.3 KB".
- **Rows (centered at y = 95, 165, 235), each with a left-aligned 12px `#444` time label at x=20:**
  - "9:00 first visit": blue `#2a78d6` rounded box at x=110 labeled "GET latte.jpg" (12px), 3px blue arrow to a green `#008300` box at x=430 labeled "200 OK — 240 KB body", 11px `#6b7280` note under the arrow "max-age=3600, ETag: \"v7\"".
  - "9:40 cache hit": single aqua `#199e70` rounded box at x=110, width 300, labeled "served from cache — 0 KB on the network"; no arrow reaches the server side, 12px `#6b7280` label "server never asked" at x=470.
  - "10:15 revalidate": blue box at x=110 labeled "GET + If-None-Match: \"v7\"", 3px arrow to a violet `#4a3aa7` box at x=430 labeled "304 Not Modified — 0.3 KB".
- **Box style:** 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(25,158,112,0.12)` / `rgba(74,58,167,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, right side near y=265):** "the 240 KB photo travels only once".
- **Caption (12px `#444`, bottom right):** "sizes illustrative".

## The max-age Clock and the ETag Handshake

**Tags:** `worked example` (blue), `freshness math` (green), `304` (orange)

- **The rule** — max-age=3600 means the copy is fresh for 3,600 s (one hour) counted from 9:00:00
- **9:40 check** — age is 2,400 s, under the 3,600 s budget, so the cache answers alone: 0 KB moved
- **10:15 check** — age is 4,500 s, over budget by 900 s: stale, so the browser sends If-None-Match: "v7"
- **Server side** — the file's current ETag is still "v7", so the server replies 304 with no body at all
- **Bytes saved** — 0.3 KB instead of 240 KB on that visit, a 99.9% saving for an unchanged photo
- **If it had changed** — the new file gets ETag "v8" and a full 200 with 240 KB; nothing stale is shown

*Example (italic):* 10:15 minus 9:00 is 4,500 s of age against a 3,600 s budget — stale by 900 s, so one cheap "still v7?" round trip replaces a full download.

**Key point:** Freshness is plain arithmetic — age versus max-age; the ETag handshake only runs once the copy is stale, and it costs a 304 (0.3 KB) when the bytes haven't changed.

### Visualization (canvas `c2`, 720×300)

Timeline from 9:00 to 10:30 with the one-hour fresh window shaded, and a vertical bytes-transferred bar at each of the three visits.

- **Title (bold 15px, `#1a5276`, top center):** "Fresh Until 10:00: Age vs max-age Decides What Goes Over the Wire".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = clock time 9:00 to 10:30 with 12px `#444` tick labels every 30 min ("9:00", "9:30", "10:00", "10:30"); y = KB transferred 0 to 240, gridlines `#e5e9ef` at 60/120/180.
- **Fresh window:** green fill `rgba(0,131,0,0.10)` from x(9:00) to x(10:00) over the full plot height, 12px green `#008300` label "fresh — max-age=3600" at its top; stale zone right of 10:00 gets a 12px `#c98500` label "stale — must revalidate".
- **Expiry marker:** vertical dashed `#6b7280` (dash 4/3) line at 10:00, 11px `#6b7280` label "age = 3,600 s" beside it.
- **Bars (30px wide, 11px `#444` value labels on top):** at 9:00 a blue `#2a78d6` bar height 180 (full scale) labeled "200 — 240 KB"; at 9:40 no bar, bold 12px aqua `#199e70` label "cache hit — 0 KB" just above the baseline; at 10:15 a violet `#4a3aa7` bar 2px tall labeled "304 — 0.3 KB".
- **Annotation (bold 13px violet `#4a3aa7`, near x(9:50), y=90):** "stale ≠ re-download: ask 'still v7?' first".
- **Caption (12px `#444`, bottom right):** "sizes illustrative; times exact for max-age=3600".

## Where the Saved Bytes Show Up

**Tags:** `where it's used` (blue), `page speed` (green), `CDN` (orange)

- **Repeat visits** — a page with 30 cached assets skips 30 downloads; only the fresh HTML is fetched
- **The numbers** — first visit moves 1,800 KB and takes 2.4 s; the repeat moves 60 KB in 0.4 s
- **CDNs** — edge servers obey the same headers, serving one cached origin response to thousands of users
- **Server load** — answering a 304 is a fingerprint compare, not a file read plus a 240 KB transfer
- **Stale-data bugs** — a long max-age on changing data shows old prices until the clock runs out
- **The pattern** — long max-age for fingerprinted files (app.9f3c.css), short or none for the HTML itself

*Example (italic):* After a price change published with max-age=86400, some customers can see yesterday's price for up to 24 hours — the cache is doing exactly what it was told.

**Key point:** Caching is the cheapest speed win on the web, but max-age is a promise the server cannot take back — pick durations by how bad it is for a user to see the old copy.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing a first visit against a repeat visit on the same page: bytes transferred and load time, two rows per metric.

- **Title (bold 15px, `#1a5276`, top center):** "Same Page, Second Visit: 30× Fewer Bytes, 6× Faster".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; two metric groups with 12px bold `#1a5276` group labels at x=20: "bytes moved" above rows 1–2, "load time" above rows 3–4.
- **Rows (bars 18px tall at y = 85, 120, 195, 230), each with a left-aligned 12px `#444` label at x=20 and an 11px value label at the bar end:**
  - "first visit — 1,800 KB": blue `#2a78d6` bar width 440
  - "repeat visit — 60 KB": aqua `#199e70` bar width 15
  - "first visit — 2.4 s": blue bar width 440
  - "repeat visit — 0.4 s": aqua bar width 73
- **Bar style:** first-visit bars fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge; repeat-visit bars solid.
- **Annotation (bold 13px green `#008300`, right side near y=160):** "only the HTML re-crossed the network".
- **Caption (12px `#444`, bottom right):** "page weights illustrative; bar widths proportional per group".

## no-cache Does Not Mean "Don't Cache"

**Tags:** `common mistake` (red), `Cache-Control` (orange)

- **The trap** — Cache-Control: no-cache still stores the reply; it only forces a check before every reuse
- **The real off switch** — no-store is the directive that keeps a response out of the cache entirely
- **What no-cache buys** — each reuse costs one revalidation round trip, but a 304 keeps that at 0.3 KB
- **max-age=0** — a close cousin: store the copy, but it is stale immediately, so always revalidate
- **The symptom** — a team picks no-cache to keep a private reply off disk, and it gets stored anyway
- **The cost** — with no-store, three visits move 240 + 240 + 240 KB; with no-cache, 240 + 0.3 + 0.3 KB

*Example (italic):* Serving latte.jpg with no-cache instead of no-store cuts three visits from 720 KB to 240.6 KB — and the copy is still verified as "v7" before every reuse.

**Common mistake:** Reading no-cache as "never cache". It means "cache it, but revalidate before each use" — if the response must never be stored at all, the directive is no-store.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: KB transferred on each of the three visits (9:00, 9:40, 10:15) under three directives — no-store, no-cache, and max-age=3600.

- **Title (bold 15px, `#1a5276`, top center):** "Three Directives, Three Visits: KB Moved Each Time".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = three groups centered at x=170/370/570 with 12px `#444` labels "visit 1 (9:00)", "visit 2 (9:40)", "visit 3 (10:15)"; y = KB 0 to 240, gridlines `#e5e9ef` at 60/120/180.
- **Bars (26px wide, 3px gaps within a group, 11px value labels on top), per group left to right:**
  - no-store, orange `#d95926`: heights for `[240, 240, 240]` KB — pixel heights `[180, 180, 180]`
  - no-cache, blue `#2a78d6`: `[240, 0.3, 0.3]` KB — pixel heights `[180, 2, 2]`
  - max-age=3600, green `#008300`: `[240, 0, 0]` KB — pixel heights `[180, 0, 0]`, "0" as a 11px label at the baseline
- **Legend (12px, top right, y≈55):** orange swatch "no-store", blue swatch "no-cache", green swatch "max-age=3600".
- **Annotation (bold 13px magenta `#d55181`, centered near y=80):** "no-cache still caches — it just checks first".
- **Caption (12px `#444`, bottom right):** "sizes illustrative; 0.3 KB is the 304 header cost".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded arrays above (no randomness); the photo size (240 KB), 304 cost (0.3 KB), page weights (1,800 KB / 60 KB), and load times (2.4 s / 0.4 s) are invented and labeled illustrative; the freshness arithmetic (max-age=3600, ages 2,400 s and 4,500 s, stale by 900 s) and the directive semantics (no-cache stores + revalidates, no-store never stores) are exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
