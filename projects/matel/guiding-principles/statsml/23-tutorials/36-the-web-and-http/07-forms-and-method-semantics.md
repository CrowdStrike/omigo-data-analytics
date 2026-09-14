# Forms & Method Semantics

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Forms & Method Semantics

**Subtitle:** A form that only looks something up uses GET, a form that changes something uses POST — the method is a promise about what happens if the request is sent twice

## The Search Box and the Buy Button

**Tags:** `core idea` (blue), `GET vs POST` (green), `HTML forms` (orange)

- **The shop** — an online coffee shop has exactly two forms: a search box and a checkout button
- **The search** — typing "latte" submits `GET /search?q=latte`; the query rides inside the URL itself
- **The checkout** — clicking Buy submits `POST /orders`; the cart travels hidden in the request body
- **The difference** — the search only reads the menu; the checkout writes a new row into the orders table
- **The promise** — GET says "just looking, repeat me freely"; POST warns "each send does something new"

*Example (italic):* Searching "latte" ten times shows the same 6 drinks ten times; clicking Buy ten times creates ten separate $4.50 orders.

**Key point:** A form's method declares intent, not packaging: GET is for requests you can repeat without consequence, POST is for requests where every submission changes the world.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the search form's GET flowing to a read-only result, the checkout form's POST flowing to a new database row.

- **Title (bold 15px, `#1a5276`, top center):** "Two Forms, Two Promises: GET Reads, POST Writes".
- **Row 1 (y=100), label 12px `#444` at x=20:** "search form"; blue `#2a78d6` rounded box at x=140 labeled "GET /search?q=latte" (12px), 3px arrow to an aqua `#199e70` box at x=350 labeled "server reads menu", 3px arrow to an aqua box at x=555 labeled "6 results, nothing changed".
- **Row 2 (y=200), label:** "checkout form"; blue box at x=140 labeled "POST /orders (body: 1 latte)", 3px arrow to an orange `#d95926` box at x=350 labeled "server inserts row", 3px arrow to an orange box at x=555 labeled "order #1042 — $4.50".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(25,158,112,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=265):** "the method tells the browser which form is safe to replay".
- **Caption (12px `#444`, bottom right):** "order number and price illustrative".

## What Refresh, Back, and Double-Click Do to Each Form

**Tags:** `worked example` (blue), `idempotency` (green)

- **Idempotent** — a request is idempotent when sending it 5 times leaves the world as if sent once
- **The search** — `GET /search?q=latte` sent 5 times: five identical pages, 0 new rows in orders
- **The checkout** — `POST /orders` sent 5 times: five rows in orders, the customer owes $22.50
- **Refresh** — the browser silently re-runs a GET; before re-sending a POST it asks "resubmit form?"
- **Back button** — returning to search results is harmless; returning past a POST risks a resend
- **Bookmark** — the GET lives in a sharable URL; the POST body is gone the moment the page loads

*Example (italic):* One nervous double-click on Buy plus three refreshes turns a $4.50 latte into five orders totaling $22.50.

**Key point:** The same request repeated: GET adds 0 rows every time, POST adds one order per send — that asymmetry is idempotency, and browser refresh/back warnings are built around it.

### Visualization (canvas `c2`, 720×300)

Idempotency table drawn as a canvas grid: four everyday actions down the side, GET and POST outcomes side by side.

- **Title (bold 15px, `#1a5276`, top center):** "Send the Same Request Five Times: What Lands in the Database".
- **Grid:** 3 columns with left edges at x=30, x=250, x=470; header baseline y=70; data rows at y=110, 150, 190, 230; 1px `#e5e9ef` horizontal rules under the header and each row, full width 30–690.
- **Header (bold 13px `#1a5276`):** "you do", "GET /search?q=latte", "POST /orders".
- **Rows (12px), action label in `#2c3e50`, GET cell, POST cell:**
  - "submit once" | green `#008300` "0 new rows" | blue `#2a78d6` "+1 order — $4.50"
  - "double-click (×2)" | green "0 new rows" | orange `#d95926` "+2 orders — $9.00"
  - "refresh 3 more times" | green "0 new rows" | red `#e74c3c` "warns; if resent, 5 total — $22.50"
  - "bookmark & revisit" | green "same page, sharable URL" | mute `#6b7280` "nothing to replay — body not in URL"
- **Annotation (bold 13px red `#e74c3c`, centered near y=268):** "5 sends: GET leaves 0 rows, POST leaves 5 orders ($22.50)".
- **Caption (12px `#444`, bottom right):** "prices illustrative".

## Duplicate Orders, Safe Retries, and Free Caching

**Tags:** `where it's used` (blue), `retries` (green), `caching` (orange)

- **Lost responses** — on a flaky network a reply vanishes; someone must decide if re-sending is safe
- **Auto-retry** — browsers and proxies re-send GETs on their own, because idempotent requests can't hurt
- **Blind POST retry** — re-sending 30 unanswered checkouts creates 30 duplicate orders, $135 of them
- **Caching** — 740 of the shop's 1,000 daily searches repeat a recent query and are answered from cache
- **POST passes through** — all 200 daily checkouts must reach the server; caches never answer a POST

*Example (italic):* One flaky afternoon 30 checkout responses are lost; blind retries would create 30 duplicate orders — $135 of lattes nobody wanted.

**Key point:** Method semantics let machines make safety decisions without reading your code: anything GET can be retried and cached automatically, anything POST must reach the server exactly as sent.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: the cost of blindly retrying 30 lost responses per method, plus how much daily traffic caching absorbs per method.

- **Title (bold 15px, `#1a5276`, top center):** "A Flaky Afternoon: 30 Lost Responses, Retried Blindly".
- **Axis:** vertical 2px `#999` baseline at x=280, bars extend right, max width 400; row labels right-aligned 12px `#444` ending at x=270.
- **Rows (bars 16px tall, centered at y = 90, 135, 195, 240), hardcoded pixel widths:**
  - "GET /search retried ×30 — duplicates": no bar (width 0), bold 12px green `#008300` label "0 duplicates — $0" at x=290
  - "POST /orders retried ×30 — duplicates": red `#e74c3c` bar width 300, 12px red label "30 duplicate orders — $135"
  - "searches served from cache (of 1,000/day)": aqua `#199e70` bar width 296, 12px `#444` label "740"
  - "checkouts served from cache (of 200/day)": no bar, 12px mute `#6b7280` label "0 — every POST hits the server"
- **Group rules:** thin 1px `#e5e9ef` separator line at y=165 between the retry pair and the cache pair.
- **Annotation (bold 13px green `#008300`, right side near y=60):** "idempotent requests are free to retry and free to cache".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic; counts illustrative".

## The Delete Link That a Robot Clicked

**Tags:** `common mistake` (red), `unsafe GET` (orange)

- **The shortcut** — the shop's admin page removes menu items with plain links: `GET /remove?item=7`
- **The robot** — a link-prefetching crawler follows every link it sees, trusting that GET is safe
- **The morning after** — all 12 menu items are gone by 6am; each "removal" was a robot fetching a URL
- **The fix** — state-changing actions become POST forms with buttons, which no crawler submits
- **The mirror mistake** — POST for search: results can't be bookmarked or shared, and Back nags to resubmit

*Example (italic):* Overnight a crawler fetches the admin page's 12 remove links and the menu is empty by 6am — no human clicked anything.

**Common mistake:** Choosing the method by convenience — links are easier to write than forms — instead of by semantics. Anything that changes data must not be reachable through a bare GET.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: remove-as-GET-link letting a crawler empty the menu vs remove-as-POST-form leaving it untouched.

- **Title (bold 15px, `#1a5276`, top center):** "State-Changing GET: The Night a Crawler Emptied the Menu".
- **Row 1 (y=100), label 12px `#444` at x=20:** "remove via GET"; blue `#2a78d6` rounded box at x=155 labeled "12 links: GET /remove?item=N" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "crawler fetches all 12" with bold 12px red "✗ menu 12 → 0 by 6am" beside it.
- **Row 2 (y=205), label:** "remove via POST"; blue box at x=155 labeled "12 buttons: POST /remove", 3px arrow to a green `#008300` box at x=420 labeled "crawler indexes, submits nothing" with bold 12px green "✓ all 12 items intact".
- **Box style:** 160–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "robots assume GET is safe — your job is to make that true".
- **Caption (12px `#444`, bottom right):** "item counts and timing illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the running numbers are one $4.50 latte, 5 repeated sends → 0 GET rows vs 5 POST orders ($9.00 at ×2, $22.50 at ×5), 30 blind POST retries → 30 duplicates ($135 = 30 × $4.50), cache hits 740 of 1,000 searches vs 0 of 200 checkouts, and 12 menu items deleted by the crawler — all invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
