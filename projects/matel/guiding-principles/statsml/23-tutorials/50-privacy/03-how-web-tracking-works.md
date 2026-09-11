# How Web Tracking Works

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** How Web Tracking Works

**Subtitle:** Third-party cookies, invisible pixels, and redirect chains — the plain mechanics that turn ordinary page loads into cross-site behavioral profiles

## Three Sites, One Cookie ID

**Tags:** `core idea` (blue), `third-party cookies` (green), `cross-site` (orange)

- **The visits** — one morning a reader opens a news site, a recipe site, and a shoe store
- **First party** — each site sets its own cookie for logins and carts; that cookie stays on that site
- **Third party** — all three pages also embed an ad script served from the same ad domain
- **One ID** — the ad domain set cookie `id=USER-0001` once; the browser sends it back on all three sites
- **The profile** — no hacking involved: plain set-and-send cookie rules link the three visits into one record

*Example (italic):* By 9am the ad domain's log shows `USER-0001` read election news, saved a pasta recipe, and browsed running shoes — three sites the reader never thought of as connected.

**Key point:** A third party embedded on thousands of sites sees you on all of them under one cookie ID — cross-site profiles fall out of ordinary cookie mechanics, not exotic technology.

### Visualization (canvas `c1`, 720×300)

Convergence diagram: three first-party site boxes on the left, each embedding the same ad domain, with arrows converging on one third-party box holding a single cookie ID and the merged profile.

- **Title (bold 15px, `#1a5276`, top center):** "Three Separate Visits, One Third-Party Cookie ID".
- **Site boxes (left column, x=30, 190px wide, 44px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) at y = 60, 130, 200:** "news.example", "recipes.example", "shoes.example"; under each name an 11px `#6b7280` line "embeds adnet.example".
- **Arrows:** three 2.5px `#6b7280` arrows from each box's right edge converging to the tracker box's left edge, each with an 11px `#6b7280` mid-label "cookie: id=USER-0001".
- **Tracker box (x=430, y=100, 250px wide, 100px tall, 8px radius, fill `rgba(217,89,38,0.12)`, 2px `#d95926` border):** bold 13px `#d95926` header "adnet.example (third party)", then 12px `#2c3e50` lines "USER-0001: election news", "USER-0001: pasta recipe", "USER-0001: running shoes".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "three first parties, one cross-site profile".
- **Caption (12px `#444`, bottom right):** "domains and tracking id are made-up placeholders".

## Reading a Tracking Pixel by Hand

**Tags:** `worked example` (blue), `pixels` (green)

- **The pixel** — the shoe store's checkout page includes a 1×1 transparent image from the ad domain
- **The URL** — `p.gif?id=USER-0001&page=/checkout&item=trail-runner&price=79` — the data rides in the URL
- **The trick** — loading the image IS the transmission; no form, no click, no visible element
- **Hand-check** — one request tells the tracker who (`USER-0001`), where (`/checkout`), what (79 dollars)
- **Email too** — a pixel in a newsletter fires when opened, logging the open time and reader ID

*Example (italic):* The reader lingers on checkout without buying; the pixel already fired, so `USER-0001` is tagged as a 79-dollar cart abandoner and retargeted for weeks.

**Key point:** A tracking pixel is a data channel disguised as an image — the browser's ordinary "fetch this image" step delivers the parameters, so rendering the page is itself the report.

### Visualization (canvas `c2`, 720×300)

Anatomy diagram of one pixel request: the URL rendered large in monospace, with bracket callouts decoding each segment, and a log-row box showing what the tracker's server records.

- **Title (bold 15px, `#1a5276`, top center):** "One Invisible Image Load = One Log Row".
- **URL (13px monospace `#2c3e50`, centered at y=80):** "adnet.example/p.gif?id=USER-0001&page=/checkout&item=trail-runner&price=79"; the four query segments drawn in distinct colors: id in `#2a78d6`, page in `#008300`, item in `#c98500`, price in `#d55181`.
- **Callouts (bold 12px, each a short 1.5px line from its segment down to a label at y=135, matching segment colors):** "who: cookie ID" (blue), "where: checkout page" (green), "what: the product" (yellow `#c98500`), "how much: $79" (magenta).
- **Pixel note (11px `#6b7280`, left side near y=80):** "1×1 gif, invisible" beside a 6px gray square marker.
- **Log box (x=110, y=185, 500px wide, 56px tall, 8px radius, fill `rgba(0,131,0,0.10)`, 2px `#008300` border):** bold 12px `#008300` header "tracker's server log", 12px monospace `#2c3e50` row "09:14:07 | USER-0001 | /checkout | trail-runner | 79".
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "no click needed — rendering the page sent the data".
- **Caption (12px `#444`, bottom right):** "URL and log row illustrative; tracking id is a made-up placeholder".

## The Click That Bounces, and the Profile Economy

**Tags:** `why it matters` (blue), `redirect chains` (orange), `data source` (green)

- **The bounce** — clicking the shoe ad doesn't go straight to the store; it hops through tracker domains
- **Each hop logs** — three redirects add 40 + 65 + 55 = 160 ms, and each domain records `USER-0001` in passing
- **Ad auctions** — every page load auctions the ad slot, sharing the bid request with dozens of buyers
- **Identity graphs** — a login on any site lets brokers join cookie IDs into one persistent person record
- **The data scientist's stake** — much "behavioral data" is exhaust from this pipeline, caveats included

*Example (italic):* A clickstream dataset lands on an analyst's desk; every row was logged by a hop like these, with consent buried in a banner and bot traffic mixed in — quality caveats are structural.

**Key point:** Cookies, pixels, and redirects feed a profile economy of auctions and identity graphs — a data scientist should know the pipeline because it is where much behavioral data, and its consent and quality problems, comes from.

### Visualization (canvas `c3`, 720×300)

Horizontal hop diagram of one ad click: five boxes left to right from the click to the destination, each intermediate hop stamped with its added latency and a "logs USER-0001" marker.

- **Title (bold 15px, `#1a5276`, top center):** "One Click, Three Logging Hops Before the Store".
- **Boxes (five, 120px wide, 46px tall, 8px radius, centered on y=140, at x = 20, 160, 300, 440, 580):** "ad click" (fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`), "clicks.adnet.example" / "sync.dsp.example" / "match.idgraph.example" (fill `rgba(217,89,38,0.12)`, 2px `#d95926`), "shoes.example" (fill `rgba(0,131,0,0.12)`, 2px `#008300`); box text 11px `#2c3e50`.
- **Arrows:** 2.5px `#6b7280` arrows between boxes with bold 12px `#d95926` latency labels above: "+40 ms", "+65 ms", "+55 ms", and a plain 12px `#6b7280` "arrives" on the last arrow.
- **Log stamps (bold 11px `#d55181`, under each of the three tracker boxes at y≈200):** "logs USER-0001".
- **Total bar (2px `#4a3aa7` bracket under the three tracker boxes at y=230):** bold 12px `#4a3aa7` centered label "160 ms detour — three more records of you".
- **Annotation (bold 13px green `#008300`, top right near y=60):** "the reader only sees a brief pause".
- **Caption (12px `#444`, bottom right):** "hop latencies illustrative".

## Blocking Cookies Doesn't End Tracking

**Tags:** `common mistake` (red), `countermeasures` (orange)

- **The squeeze** — some major browsers block third-party cookies; blocklists strip known trackers
- **The pressure** — privacy regulation adds consent requirements and real fines for silent tracking
- **The response** — the industry's documented shift: server-side tracking routed through the site's own domain
- **Fingerprinting** — browser traits (fonts, canvas, timezone) combine into an ID with no cookie at all
- **The mistake** — reading "cookies deprecated" as "tracking over", or trusting old datasets as consented

*Example (italic):* An analyst assumes a 2026 clickstream is cookie-based and consent-flagged like the 2019 one; it is actually server-side events under the first-party domain, with different coverage and bias.

**Common mistake:** Treating countermeasures as the end of the story. When one channel closes, tracking demonstrably migrates to server-side and fingerprinting — so the meaning, consent status, and coverage of behavioral data keep changing underneath your models.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: illustrative reach index (0–100) of three tracking channels in 2019 vs 2026 — third-party cookies collapsing while server-side tracking and fingerprinting grow.

- **Title (bold 15px, `#1a5276`, top center):** "Channels Shift, Tracking Persists (illustrative reach index)".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 175; y = index 0 to 100, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels; x = three groups centered at x = 180, 380, 580 with 12px `#444` labels "3rd-party cookies", "server-side tracking", "fingerprinting".
- **Bars (per group: two 52px-wide bars, 8px apart):** 2019 bar fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, 2026 bar solid `#d95926`; heights from values 2019 = `[95, 10, 15]`, 2026 = `[30, 55, 45]`; bold 12px value labels above each bar in the bar's color.
- **Legend (12px, top right at y=55):** blue swatch "2019", orange swatch "2026".
- **Annotation (bold 13px magenta `#d55181`, above the cookies group near y=75):** "blocked here, rerouted there".
- **Caption (12px `#444`, bottom right):** "index values illustrative — direction reflects documented industry shifts".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); domains, cookie ID `USER-0001`, the $79 pixel parameters, hop latencies 40/65/55 ms (total 160), and the reach-index arrays `[95, 10, 15]` vs `[30, 55, 45]` are invented and labeled illustrative; keep text numbers identical to chart numbers.
- Do not name real companies for undocumented behavior — all tracker and site domains stay generic `.example` placeholders.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: session ids, cookie values, tracking ids, token parts, and example passwords on this page are made-up placeholders — for illustration only, and to avoid false positives from secret scanners."
