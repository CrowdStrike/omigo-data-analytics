# REST vs RPC vs GraphQL

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** REST vs RPC vs GraphQL

**Subtitle:** Three ways to ask a server for the same data — name the thing (REST), name the action (RPC), or name the exact fields you want (GraphQL)

## Maya's Order History, Asked Three Ways

**Tags:** `core idea` (blue), `three styles` (green), `same data` (orange)

- **The screen** — a coffee shop app shows Maya (customer 42) her 3 past orders with each order's items
- **REST** — ask for things by address: `GET /customers/42/orders`, then one `GET /orders/N/items` per order
- **RPC** — ask for an action by name: one call to `getOrderHistory(customerId: 42)` built just for this screen
- **GraphQL** — ask for fields by shape: one query listing exactly `date`, `total`, and item `name`
- **Same wire** — all three travel as ordinary HTTP requests; only the shape of the ask differs

*Example (italic):* To draw one screen, REST sends 4 requests, RPC sends 1 custom call, and GraphQL sends 1 query naming its fields.

**Key point:** REST, RPC, and GraphQL are not different networks or databases — they are three grammars for the same question: "server, give me Maya's orders with items."

### Visualization (canvas `c1`, 720×300)

Three-row diagram: each row shows the requests one style sends to fetch the same order-history screen, drawn as labeled boxes on a left-to-right lane.

- **Title (bold 15px, `#1a5276`, top center):** "One Screen, Three Ways to Ask the Server".
- **Rows (lane baselines at y = 95, 170, 245), each with a bold 13px `#2c3e50` style label at x=20:** "REST", "RPC", "GraphQL".
- **REST row:** four blue `#2a78d6` rounded boxes (fill `rgba(42,120,214,0.15)`, 1.5px border, 8px radius, 34px tall) at x = 90, 250, 400, 550, widths 150/140/140/140, 11px `#2c3e50` labels "GET /customers/42/orders", "GET /orders/101/items", "GET /orders/102/items", "GET /orders/103/items"; 2px `#6b7280` arrows between boxes.
- **RPC row:** one green `#008300` box (fill `rgba(0,131,0,0.12)`) at x=90, width 300, label "POST getOrderHistory(customerId: 42)".
- **GraphQL row:** one violet `#4a3aa7` box (fill `rgba(74,58,167,0.12)`) at x=90, width 340, label "POST /graphql { orders { date total items { name } } }".
- **Annotation (bold 13px violet `#4a3aa7`, right side near x=470, y=210):** "same data need — 4 asks vs 1 vs 1".
- **Caption (12px `#444`, bottom right):** "request shapes schematic; 3 orders illustrative".

## Counting the Requests and the Bytes

**Tags:** `worked example` (blue), `over-fetching` (orange), `under-fetching` (red)

- **REST count** — 1 orders request + 3 items requests = 4 round trips for Maya's 3 orders
- **REST payload** — full order objects (status, tax, store id, payment ref…) total 11.2 KB sent
- **What's used** — the screen needs only date, total, and item names: about 1.4 KB of that 11.2 KB
- **RPC** — 1 request, 1.5 KB: the server hand-picks the screen's fields inside one custom procedure
- **GraphQL** — 1 request, 1.4 KB: the client's query names the fields, so nothing extra is sent
- **The jargon** — REST here over-fetches (extra fields) and under-fetches (items need extra calls)

*Example (italic):* REST ships 11.2 KB across 4 requests to paint a 1.4 KB screen; RPC and GraphQL each ship ~1.5 KB in a single request.

**Key point:** Over-fetching means paying for fields you don't use; under-fetching means paying extra round trips for fields you do — REST's fixed resources risk both, RPC and GraphQL trade them away differently.

### Visualization (canvas `c2`, 720×300)

Two side-by-side bar panels on one canvas: left panel counts HTTP requests per style, right panel shows payload KB per style, same three colors in both.

- **Title (bold 15px, `#1a5276`, top center):** "4 Requests and 11.2 KB vs 1 Request and ~1.4 KB".
- **Left panel:** baseline y=245 from x=60 to x=330, bold 13px `#2c3e50` panel label "HTTP requests" at x=120, y=60; three bars 60px wide at x = 75, 165, 255 for REST/RPC/GraphQL, heights scaled to max 4 → 160px: REST 160px blue `#2a78d6`, RPC 40px green `#008300`, GraphQL 40px violet `#4a3aa7`; bold 13px value labels "4", "1", "1" above bars; 12px `#444` style names below baseline.
- **Right panel:** baseline y=245 from x=390 to x=660, panel label "payload sent (KB)" at x=440, y=60; three bars 60px wide at x = 405, 495, 585, heights scaled to max 11.2 → 160px: REST 160px blue, RPC 21px green, GraphQL 20px violet; value labels "11.2", "1.5", "1.4"; inside the REST bar a dashed `#6b7280` line at the 1.4 KB level (20px) with 11px `#6b7280` label "used: 1.4".
- **Gridlines:** `#e5e9ef` horizontal lines in each panel at 1/4, 1/2, 3/4 of bar height range.
- **Annotation (bold 13px orange `#d95926`, centered near x=360, y=285):** "REST ships 8× the bytes and 4× the trips for this screen".
- **Caption (12px `#444`, bottom right):** "counts and KB illustrative".

## Why Mobile Apps and Internal Services Choose Differently

**Tags:** `where it's used` (blue), `latency` (green), `API design` (orange)

- **Mobile pain** — a phone round trip costs ~300 ms, so REST's 4 sequential requests take ~1200 ms
- **Single-trip win** — RPC and GraphQL fetch the screen in one trip: ~300 ms on the same network
- **Internal calm** — between servers a round trip is ~1 ms, so 4 trips (4 ms) vs 1 trip is invisible
- **So in practice** — internal service-to-service calls lean RPC; simple public resources lean REST
- **Many screens** — GraphQL earns its setup cost when many client screens each need different fields

*Example (italic):* The same 4-request REST flow costs ~1200 ms on a phone but only ~4 ms between two servers in one datacenter.

**Key point:** The style choice is mostly a round-trip budget question — where round trips are expensive (mobile, varied screens), single-request styles win; where they are cheap (internal), simplicity wins.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart in two groups: time to fetch the screen on a mobile network (300 ms/round trip) vs an internal network (1 ms/round trip), three bars per group.

- **Title (bold 15px, `#1a5276`, top center):** "Round-Trip Budget: 300 ms on Mobile vs 1 ms Inside the Datacenter".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 430 for the largest value (1200 ms); left-aligned 12px `#444` row labels at x=20.
- **Mobile group (bold 13px `#2c3e50` group label "mobile app" at x=20, y=60), rows at y = 80, 112, 144:**
  - "REST — 4 trips": blue `#2a78d6` bar width 430, 11px label "1200 ms" at bar end
  - "RPC — 1 trip": green `#008300` bar width 108, label "300 ms"
  - "GraphQL — 1 trip": violet `#4a3aa7` bar width 108, label "300 ms"
- **Internal group (group label "internal service" at x=20, y=190), rows at y = 210, 242, 274:**
  - "REST — 4 trips": blue bar width 8, label "4 ms"
  - "RPC — 1 trip": green bar width 2, label "1 ms"
  - "GraphQL — 1 trip": violet bar width 2, label "1 ms"
- **Bar style:** 16px tall, fills at 0.85 alpha, widths proportional to milliseconds (430px = 1200 ms).
- **Annotation (bold 13px green `#008300`, near x=380, y=225):** "inside the datacenter, all three are effectively instant".
- **Caption (12px `#444`, bottom right):** "sequential requests, times illustrative; scales differ per group".

## One Request Is Not One Query

**Tags:** `common mistake` (red), `N+1 problem` (orange)

- **The confusion** — seeing GraphQL's single HTTP request and assuming the server does less work
- **Behind the curtain** — a naive GraphQL resolver runs 1 orders query + 3 item queries: the same N+1
- **Moved, not removed** — the join work leaves the phone's network and lands on the server's database
- **The fix** — batching (a dataloader) collapses the 3 item lookups into 1 query: 2 queries total
- **RPC's version** — the hand-written procedure does the same thing manually with one tuned join

*Example (italic):* Maya's single GraphQL request still triggers 4 database queries until batching cuts it to 2 — the client just never sees them.

**Common mistake:** Treating "one request" as "one query." GraphQL fixes over- and under-fetching on the wire, but the joins still run somewhere — an unbatched server does exactly the work REST's 4 requests did.

### Visualization (canvas `c4`, 720×300)

Paired horizontal bar chart: for each setup, one bar for HTTP requests the client sees and one for database queries the server runs, showing work moving rather than disappearing.

- **Title (bold 15px, `#1a5276`, top center):** "The Client Sees 1 Request; the Database Still Sees the Queries".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, scale 90px per unit (max 4 units = 360px); left-aligned 12px `#444` row labels at x=20.
- **Rows (top to bottom at y = 70, 122, 174, 226), each with two stacked-thin bars (12px tall, 4px gap):**
  - "REST": blue `#2a78d6` HTTP bar width 360 ("4"), mute `#6b7280` DB bar width 360 ("4 queries")
  - "RPC (tuned join)": green `#008300` HTTP bar width 90 ("1"), mute DB bar width 90 ("1 query")
  - "GraphQL (naive)": violet `#4a3aa7` HTTP bar width 90 ("1"), orange `#d95926` DB bar width 360 ("4 queries")
  - "GraphQL (batched)": violet HTTP bar width 90 ("1"), mute DB bar width 180 ("2 queries")
- **Legend (12px, x=480, y=55):** colored swatch + "HTTP requests (client)", mute swatch + "DB queries (server)".
- **Value labels:** 11px `#444` counts at each bar end.
- **Annotation (bold 13px magenta `#d55181`, near x=400, y=185):** "naive GraphQL: N+1 hides behind one request".
- **Caption (12px `#444`, bottom right):** "query counts for 3 orders, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); request counts (4/1/1), payload sizes (11.2/1.5/1.4 KB with 1.4 KB used), round-trip times (300 ms mobile, 1 ms internal → 1200/300/300 ms and 4/1/1 ms), and database query counts (4/2/4/2 for REST, RPC, naive GraphQL, batched GraphQL) are invented for a 3-order example and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
