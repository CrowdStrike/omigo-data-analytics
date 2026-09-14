# HTTP Semantics

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** HTTP Semantics

**Subtitle:** Every HTTP verb is a promise about what happens if the request runs twice — GET swears "nothing changes", PUT and DELETE swear "twice is the same as once", POST promises nothing

## Four Buttons on the Coffee Shop's Website

**Tags:** `core idea` (blue), `verbs as contracts` (green), `orders table` (orange)

- **The shop** — a coffee shop's website talks to one orders table on its server
- **GET /menu** — asks to read the menu; the promise is it changes nothing on the server
- **PUT /orders/12** — sets order #12 to an exact new content; repeating just sets it again
- **DELETE /orders/12** — removes order #12; deleting it a second time leaves it just as gone
- **POST /orders** — appends a brand-new order row; every send adds another one
- **The contract** — the verb name itself tells caches, proxies, and clients what repeats will do

*Example (italic):* Maya's browser can re-issue GET /menu freely after a glitch, but it warns her before resubmitting the POST that placed her latte order.

**Key point:** HTTP verbs are not just names — each carries a documented contract about side effects, and the whole web's plumbing is built on trusting it.

### Visualization (canvas `c1`, 720×300)

Flow diagram: four verb boxes on the left, each with a labeled arrow into one orders-table box on the right, arrow labels stating each verb's promise.

- **Title (bold 15px, `#1a5276`, top center):** "Four Verbs, Four Promises to the Orders Table".
- **Verb boxes (left, x=40, 132px wide, 34px tall, 8px radius, bold 13px labels), rows at y = 75, 128, 181, 234:** "GET /menu" blue `#2a78d6` fill `rgba(42,120,214,0.15)`; "PUT /orders/12" aqua `#199e70` fill `rgba(25,158,112,0.14)`; "DELETE /orders/12" orange `#d95926` fill `rgba(217,89,38,0.13)`; "POST /orders" magenta `#d55181` fill `rgba(213,81,129,0.13)`.
- **Orders table box (right):** rounded box at x=520, y=75 to y=268, 160px wide, ink `#1a5276` 2px border, fill `rgba(26,82,118,0.08)`, bold 13px ink label "orders table" at its top, five thin `#e5e9ef` row lines inside suggesting 5 rows.
- **Arrows:** 2px lines from each verb box to the table's left edge, each with a 12px label in the verb's color above it: "reads — changes nothing", "replaces #12 — twice = once", "removes #12 — gone stays gone", "appends a row — every single time".
- **Annotation (bold 13px ink `#1a5276`, bottom left near y=285):** "the verb is a promise about what a repeat will do".
- **Caption (12px `#444`, bottom right):** "shop and orders illustrative".

## Sending Each Request Three Times

**Tags:** `worked example` (blue), `safe vs idempotent` (green), `truth table` (orange)

- **Start** — the orders table holds exactly 5 rows before each experiment
- **GET ×3** — three reads of /menu leave 5 rows untouched: GET (and HEAD) are safe
- **PUT ×3** — three sends of "order #12 = large latte" rewrite row 12 three times: still 5 rows
- **DELETE ×3** — the first send drops the table to 4 rows; the next two change nothing
- **POST ×3** — each send appends a new order: 6, then 7, then 8 rows — neither safe nor idempotent
- **The ladder** — safe implies idempotent; PUT/DELETE are idempotent but not safe; POST is neither

*Example (italic):* After 3 sends the table holds 5 rows (GET), 5 rows with row #12 rewritten (PUT), 4 rows (DELETE), and 8 rows (POST).

**Key point:** Safe = the server's state is untouched (GET, HEAD); idempotent = N identical sends end in the same state as 1 (PUT, DELETE); POST guarantees neither.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: rows in the orders table after 1 send vs after 3 sends, one group per verb, starting from 5 rows.

- **Title (bold 15px, `#1a5276`, top center):** "Rows in the Orders Table After 1 Send vs 3 Sends (start: 5 rows)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = rows 0 to 10, gridlines `#e5e9ef` at 2/4/6/8, 12px `#444` y tick labels; four groups centered at x = 135, 285, 435, 585 with bold 13px verb labels beneath the baseline.
- **Bars (two per group, 44px wide, 10px gap, 12px value labels on top):** GET blue `#2a78d6` heights for rows `[5, 5]`; PUT aqua `#199e70` `[5, 5]`; DELETE orange `#d95926` `[4, 4]`; POST magenta `#d55181` `[6, 8]`. "After 1" bar solid, "after 3" bar same color at fill alpha 0.45 with a 2px solid border.
- **Dashed start line:** 1px dashed `#6b7280` horizontal line at the 5-row level, 11px `#6b7280` label "start: 5 rows" at its left.
- **Small notes (11px `#6b7280`, under the verb labels):** GET "untouched (safe)"; PUT "row #12 rewritten (idempotent)"; DELETE "idempotent"; POST "grows every send".
- **Annotation (bold 13px magenta `#d55181`, near the POST group, y=70):** "only POST keeps moving: 6, 7, 8".
- **Caption (12px `#444`, bottom right):** "row counts illustrative".

## The Plumbing That Trusts the Promise

**Tags:** `where it's used` (blue), `caches & retries` (green)

- **Caches** — proxies and CDNs store GET responses and replay them without asking the server
- **Retries** — browsers and load balancers auto-retry safe and idempotent requests after a timeout
- **Crawlers** — search bots follow every link on the site, firing GETs they assume are harmless
- **The day's traffic** — 10,000 menu views arrive as GETs; the cache absorbs 9,500 of them
- **No shortcut for POST** — all 1,200 order submissions must reach the origin server, every time

*Example (italic):* On a busy day the origin answers only 500 of 10,000 GET /menu requests but every one of the 1,200 POST /orders.

**Key point:** Caching, automatic retries, prefetching, and crawling all work only because infrastructure can act on the verb's contract without reading your application code.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: for one day's traffic, total requests vs requests that actually reach the origin server, for GET /menu and POST /orders.

- **Title (bold 15px, `#1a5276`, top center):** "One Day of Traffic: What the Origin Server Actually Sees".
- **Axis:** vertical 2px `#999` baseline at x=210, bars extend right, max width 460 representing 10,000 requests; left-aligned 12px `#444` row labels at x=20.
- **Rows (label / bar pairs at y = 75, 115, 185, 225):**
  - "GET /menu — sent: 10,000": blue `#2a78d6` bar width 460, 11px value label "10,000" at its end
  - "GET /menu — hits origin: 500": aqua `#199e70` bar width 23, 11px label "500 — cache serves 9,500"
  - "POST /orders — sent: 1,200": magenta `#d55181` bar width 55, 11px label "1,200"
  - "POST /orders — hits origin: 1,200": orange `#d95926` bar width 55, 11px label "1,200 — no cache allowed"
- **Bar style:** 16px tall, "sent" bars fill alpha 0.35 of their color, "hits origin" bars solid.
- **Annotation (bold 13px green `#008300`, right side near y=150):** "safe verbs let the cache absorb 95% of reads".
- **Caption (12px `#444`, bottom right):** "request counts illustrative; widths proportional".

## When the Promise Gets Broken

**Tags:** `common mistake` (red), `mutating GET` (orange), `POST retry` (red)

- **Mutating GET** — putting a cancel action behind a GET link invites anything that follows links to fire it
- **The crawler** — a search bot fetching GET /orders/12/cancel silently cancels a real order
- **The prefetch** — a browser preloading links on hover can do the same before any click
- **Blind POST retry** — a timeout hides whether the POST landed; resending it can charge twice
- **The fix** — put mutations behind POST/PUT/DELETE, and retry POST only with an idempotency key

*Example (italic):* Maya's POST /orders times out after the charge succeeded; her retry creates a second order — two large lattes billed for one click.

**Common mistake:** Treating verbs as interchangeable spellings of "send a request". A GET that mutates and a POST retried blindly both break contracts the rest of the web is silently relying on.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a crawler triggering a mutating GET, and a client blindly retrying a timed-out POST, both ending in red failure boxes.

- **Title (bold 15px, `#1a5276`, top center):** "Two Broken Promises: Mutating GET, Blind POST Retry".
- **Row 1 (y=95), label 12px `#444` at x=20:** "mutating GET"; blue `#2a78d6` rounded box at x=150 labeled "crawler follows link" (12px), 3px arrow to a blue box at x=340 labeled "GET /orders/12/cancel", 3px arrow to a red `#e74c3c` box at x=545 labeled "order #12 cancelled" with bold 12px red "✗ a bot cancelled a real order".
- **Row 2 (y=205), label:** "blind POST retry"; blue box at x=150 labeled "POST /orders — timeout", 3px arrow to a blue box at x=340 labeled "client resends POST", 3px arrow to a red box at x=545 labeled "2 orders, 2 charges" with bold 12px red "✗ one latte, billed twice".
- **Box style:** 150–175px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=272):** "keep GET harmless; make retried writes idempotent (PUT or an idempotency key)".
- **Caption (12px `#444`, bottom right):** "scenario illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the orders-table row counts (start 5; GET 5/5, PUT 5/5, DELETE 4/4, POST 6/8 after 1 and 3 sends), the traffic counts (10,000 GETs with 500 reaching origin, 1,200 POSTs all reaching origin), and the two failure scenarios are invented and labeled illustrative; the safe/idempotent classification (GET/HEAD safe, PUT/DELETE idempotent, POST neither) follows the HTTP semantics standard.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
