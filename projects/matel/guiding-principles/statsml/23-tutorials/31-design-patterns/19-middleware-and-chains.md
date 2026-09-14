# Middleware & Chains

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Middleware & Chains

**Subtitle:** Every request walks through the same pipeline of wrappers — log it, check it, limit it — before the real work happens, and back out again on the way home

## One Coffee Order, Four Doors

**Tags:** `core idea` (blue), `pipeline` (green), `wrappers` (orange)

- **The shop** — a coffee shop's online ordering app receives an order request for a latte
- **The doors** — before the barista code runs, the request passes a logger, an auth check, and a rate limiter
- **The chain** — each wrapper does one small job, then hands the request to the next in line
- **The core** — only requests that survive every door reach the handler that actually makes the order
- **The way back** — the response walks the same chain in reverse, so the logger also sees the reply
- **The shape** — like an onion: the handler sits in the middle, wrappers form the layers around it

*Example (italic):* A latte order enters at 8:01am, gets logged, passes the auth check, passes the rate limiter, and only then does the order handler charge the card.

**Key point:** Middleware is a chain of wrappers around one handler — each layer sees the request on the way in and the response on the way out, and any layer can stop the chain early.

### Visualization (canvas `c1`, 720×300)

Onion/pipeline flow diagram: request boxes flowing left-to-right through three middleware layers into the handler, and the response flowing right-to-left back out through the same layers.

- **Title (bold 15px, `#1a5276`, top center):** "One Order, Four Doors: In Through the Chain, Back Out in Reverse".
- **Layout:** four rounded boxes on a middle band, left to right at x = 80, 230, 380, 530, each 120px wide, 44px tall, 8px radius, centered vertically at y=150; 12px `#2c3e50` labels "logger", "auth check", "rate limiter", "order handler".
- **Box fills:** logger blue `rgba(42,120,214,0.15)` with 2px `#2a78d6` border; auth aqua `rgba(25,158,112,0.15)` with 2px `#199e70` border; rate limiter yellow `rgba(201,133,0,0.15)` with 2px `#c98500` border; handler green `rgba(0,131,0,0.12)` with 2px `#008300` border.
- **Request arrows (top lane, y=115):** 3px `#2a78d6` arrows left-to-right between consecutive boxes, plus an entry arrow from x=15 labeled bold 12px `#2a78d6` "order request" above it.
- **Response arrows (bottom lane, y=185):** 3px `#008300` arrows right-to-left between the same boxes, exit arrow to x=15 labeled bold 12px `#008300` "response" below it.
- **Early-exit mark:** short dashed `#d95926` (dash 4/3) arrow dropping down from the rate limiter box to a 12px `#d95926` label "can reject here — chain stops" at y=235.
- **Annotation (bold 13px violet `#4a3aa7`, top right near x=560, y=70):** "the handler never sees a bad request".
- **Caption (12px `#444`, bottom right):** "flow schematic, illustrative".

## Following 200 Morning Orders Down the Chain

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **The rush** — between 8:00 and 8:15am, 200 order requests hit the shop's app
- **Door one** — the logger writes one line per request and passes all 200 through
- **Door two** — the auth check rejects 20 requests with bad tokens; 200 − 20 = 180 continue
- **Door three** — the rate limiter blocks 30 requests from one scripted buyer; 180 − 30 = 150 continue
- **The core** — the order handler makes exactly 150 drinks; the other 50 never touched it
- **Hand-check** — rejected 20 + 30 = 50, and 150 served + 50 rejected = 200 logged

*Example (italic):* Of 200 logged requests, 20 fail auth and 30 hit the rate limit, so the handler serves exactly 150 — every request is accounted for at some door.

**Key point:** Each layer's output count is the next layer's input count — 200 → 200 → 180 → 150 — so the chain is a funnel you can audit stage by stage.

### Visualization (canvas `c2`, 720×300)

Funnel bar chart: one vertical bar per stage showing how many of the 200 requests are still alive after each door, with rejected counts called out at the doors that drop them.

- **Title (bold 15px, `#1a5276`, top center):** "200 Requests In, 150 Reach the Handler".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = requests 0 to 200, gridlines `#e5e9ef` at 50/100/150 with 12px `#444` tick labels.
- **Bars (centered at x = 135, 285, 435, 585, width 80):** stages `["logged", "after auth", "after rate limit", "handled"]` with counts `[200, 200, 180, 150]`; fills blue `rgba(42,120,214,0.30)`, aqua `rgba(25,158,112,0.30)`, yellow `rgba(201,133,0,0.30)`, green `rgba(0,131,0,0.30)`; 2px borders in the matching solid colors `#2a78d6`, `#199e70`, `#c98500`, `#008300`.
- **Value labels:** bold 13px `#2c3e50` count on top of each bar; 12px `#444` stage labels under the baseline.
- **Rejection callouts:** bold 12px `#d95926` "−20 bad tokens" between bars 2 and 3 at y=95; bold 12px `#d95926` "−30 rate limited" between bars 3 and 4 at y=115.
- **Annotation (bold 13px green `#008300`, above bar 4 at y=55):** "150 + 20 + 30 = 200 — every request accounted for".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Write the Check Once, Not Forty-Eight Times

**Tags:** `where it's used` (blue), `cross-cutting` (green), `frameworks` (orange)

- **The problem** — the shop's app has 12 endpoints (order, refund, menu, ...) and 4 shared concerns
- **Copy-paste** — without a chain, 12 endpoints × 4 concerns = 48 pasted blocks of the same code
- **With a chain** — 4 middleware written once + 12 clean handlers = 16 pieces of code total
- **Web servers** — Express, Django, and Rails all run every request through this kind of chain
- **Data work** — an sklearn `Pipeline` chains scaler → encoder → model the same way, fit once, reuse everywhere
- **The fix cost** — a bug in the auth check is one edit in a chain, or 12 hunts in the copy-paste version

*Example (italic):* When the shop changes its token format, the chained app edits one auth middleware; the copy-paste app edits 12 endpoints and misses one.

**Key point:** Middleware exists for cross-cutting concerns — logging, auth, limits touch every request, so the chain lets you write each once instead of once per endpoint.

### Visualization (canvas `c3`, 720×300)

Grouped horizontal bar chart: pieces of code to write and maintain, copy-paste style vs middleware chain, split into concern code and handler code.

- **Title (bold 15px, `#1a5276`, top center):** "12 Endpoints, 4 Shared Concerns: 48 Copies vs 16 Pieces".
- **Axis:** vertical 2px `#999` baseline at x=200, bars extend right, max width 440 for 48 pieces (scale ~9.2 px per piece); light `#e5e9ef` gridlines at 12/24/36/48 pieces with 12px `#444` labels at y=260.
- **Row 1 (y=95), left label 12px `#444` at x=20:** "copy-paste (no chain)"; orange `#d95926` bar for concern copies, width 440 (48 pieces = 12 × 4), fill `rgba(217,89,38,0.30)` with 2px solid border, bold 12px `#d95926` end label "48 pasted blocks".
- **Row 2 (y=185), left label:** "middleware chain"; blue `#2a78d6` segment width 37 (4 middleware) fill `rgba(42,120,214,0.35)`, then green `#008300` segment width 110 (12 handlers) fill `rgba(0,131,0,0.30)`, bold 12px `#008300` end label "4 + 12 = 16 pieces".
- **Bar style:** 26px tall, 11px in-bar segment labels "middleware ×4" and "handlers ×12" where they fit.
- **Annotation (bold 13px magenta `#d55181`, near x=380, y=160):** "one bug fix: 1 edit instead of 12".
- **Caption (12px `#444`, bottom right):** "piece counts exact for 12 endpoints × 4 concerns; bar pixels to scale".

## The Chain's Order Is Part of the Program

**Tags:** `common mistake` (red), `ordering` (orange)

- **The confusion** — treating the chain as a bag of features when it is really a sequence with meaning
- **Lost logs** — put the logger after the auth check and every rejected request vanishes unlogged
- **Wasted work** — put the cheap rate limiter after the expensive auth check and you pay auth for doomed requests
- **The costs** — logging takes 2ms, the auth check 8ms, the rate limiter 1ms per request
- **Hand-check** — a rate-limited request costs 2+8+1 = 11ms with auth first, only 2+1 = 3ms with the limiter first
- **At scale** — the 30 rate-limited rush requests waste 30 × 8 = 240ms of auth work in the bad order

*Example (italic):* Reordering the chain to logger → rate limiter → auth cuts a doomed request from 11ms to 3ms and still logs every rejection.

**Common mistake:** Assuming middleware order doesn't matter. The chain runs top to bottom — put cheap, high-rejection layers early and the logger first, or you burn work and lose evidence.

### Visualization (canvas `c4`, 720×300)

Two-row cost breakdown: a rate-limited request's per-layer milliseconds under the bad ordering (auth before limiter) vs the good ordering (limiter before auth), drawn as stacked horizontal ms segments.

- **Title (bold 15px, `#1a5276`, top center):** "Cost of One Doomed Request: 11ms vs 3ms".
- **Axis:** vertical 2px `#999` baseline at x=190, bars extend right, scale 36px per ms (max 11ms = 396px); light `#e5e9ef` gridlines every 2ms with 12px `#444` ms labels at y=255.
- **Row 1 (y=90), left label 12px `#444` at x=20:** "auth first (bad)"; segments left to right: blue `rgba(42,120,214,0.35)` width 72 labeled "log 2ms", orange `rgba(217,89,38,0.35)` width 288 labeled "auth 8ms", yellow `rgba(201,133,0,0.35)` width 36 labeled "limit 1ms"; bold 12px `#d95926` end label "11ms — 8ms wasted".
- **Row 2 (y=180), left label:** "limiter first (good)"; segments: blue width 72 "log 2ms", yellow width 36 "limit 1ms"; bold 12px `#008300` end label "3ms — rejected before auth".
- **Bar style:** 30px tall, segment borders 2px in solid segment colors `#2a78d6` / `#d95926` / `#c98500`, 11px `#2c3e50` in-segment labels.
- **Reject marker:** dashed `#6b7280` (dash 4/3) vertical tick at each bar's end with 12px `#6b7280` label "rejected here".
- **Annotation (bold 13px red `#e74c3c`, near x=420, y=240):** "30 doomed requests × 8ms = 240ms of auth burned per rush".
- **Caption (12px `#444`, bottom right):** "per-layer timings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the funnel counts `[200, 200, 180, 150]` with rejections 20 and 30, the per-layer timings 2/8/1 ms with totals 11ms vs 3ms, and the 240ms wasted-auth figure (30 × 8) are invented and labeled illustrative; the piece counts 48 (12 × 4) vs 16 (4 + 12) are exact arithmetic for the stated 12-endpoint, 4-concern setup.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
