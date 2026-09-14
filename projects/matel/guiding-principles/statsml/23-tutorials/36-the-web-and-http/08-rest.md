# REST

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** REST

**Subtitle:** A REST API gives every piece of data its own URL and reuses the same few HTTP verbs on all of them — the order is the noun, the verb says what to do

## Every Coffee Order Gets Its Own Address

**Tags:** `core idea` (blue), `resources` (green), `HTTP` (orange)

- **The shop** — a coffee shop's app talks to a server that tracks every order placed today
- **The resource** — each order is a "thing" with its own URL: order 17 lives at `/orders/17`
- **The collection** — `/orders` is the list of all orders, the shelf that individual orders sit on
- **Nouns only** — URLs name things (`/orders/17`), never actions (`/deleteOrder` does not exist)
- **Verbs do the work** — GET reads, POST creates, PUT/PATCH change, DELETE removes — on any URL
- **The definition** — this style is called REST: resources with addresses, acted on by standard verbs

*Example (italic):* Order 17 (a large latte) is `/orders/17`; to see it you GET that URL, to cancel it you DELETE the very same URL.

**Key point:** REST models an API as a set of resources, each with a stable URL, manipulated by the same small set of HTTP verbs — the URL says what, the verb says how.

### Visualization (canvas `c1`, 720×300)

Resource-map diagram: the `/orders` collection box on top, three order boxes below it, and a row of verb pills showing the actions that apply to every box.

- **Title (bold 15px, `#1a5276`, top center):** "A Coffee Shop's Orders API: Nouns Get URLs, Verbs Do the Work".
- **Collection box:** rounded box at x=280, y=60, 180×40, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` label "/orders  (the collection)".
- **Order boxes (y=155, each 140×40, 8px radius):** "/orders/16" at x=110, "/orders/17" at x=310, "/orders/18" at x=510; fills `rgba(42,120,214,0.15)` except `/orders/17` in `rgba(0,131,0,0.12)` with 2px `#008300` border and 11px `#008300` sublabel "large latte, $4.50".
- **Connectors:** 2px `#6b7280` lines from the collection box bottom center to each order box top center.
- **Verb pills (y=235, left to right starting x=140, 12px bold white text, 6px radius, 54–70px wide):** "GET" `#2a78d6`, "POST" `#008300`, "PUT" `#4a3aa7`, "PATCH" `#c98500`, "DELETE" `#d95926`.
- **Annotation (bold 13px ink `#1a5276`, centered near y=280):** "the URL names the thing; the verb names the action".
- **Caption (12px `#444`, bottom right):** "order numbers illustrative".

## Six Requests in the Life of Order 17

**Tags:** `worked example` (blue), `verbs` (green), `status codes` (orange)

- **POST creates** — `POST /orders` with the latte details makes order 17; the server answers 201 Created
- **GET reads** — `GET /orders/17` returns the order's current state (brewing), answer 200
- **PATCH edits a field** — `PATCH /orders/17` changes just the size to large, answer 200
- **PUT replaces it all** — `PUT /orders/17` sends the whole record again, answer 200
- **DELETE removes** — `DELETE /orders/17` cancels the order, answer 204 No Content
- **404 after** — a second `GET /orders/17` now answers 404 Not Found: the resource is gone

*Example (italic):* All six requests hit the same idea of "order 17" — only the verb changes, and the status code (201, 200, 200, 200, 204, 404) tells the app what happened.

**Key point:** One URL supports the full life cycle — create, read, change, replace, delete — because the verb, not the path, carries the action, and status codes report the outcome.

### Visualization (canvas `c2`, 720×300)

Request ledger: six rows, each showing a colored verb pill, the URL, a short note, and the response status code — same resource all the way down.

- **Title (bold 15px, `#1a5276`, top center):** "Six Requests, One Resource: /orders/17".
- **Rows (y = 70, 105, 140, 175, 210, 245):** verb pill at x=50 (60px wide, 20px tall, 6px radius, bold 12px white text), URL in 12px `#2c3e50` mono at x=125, note in 12px `#6b7280` at x=250, 2px `#6b7280` arrow from x=470 to x=530, status pill at x=545 (52px wide, bold 12px white text).
- **Data:** verbs `["POST", "GET", "PATCH", "PUT", "DELETE", "GET"]`; urls `["/orders", "/orders/17", "/orders/17", "/orders/17", "/orders/17", "/orders/17"]`; notes `["create latte, $4.50", "read: brewing", "size → large", "replace whole record", "cancel the order", "order is gone"]`; codes `[201, 200, 200, 200, 204, 404]`.
- **Verb pill colors:** GET `#2a78d6`, POST `#008300`, PATCH `#c98500`, PUT `#4a3aa7`, DELETE `#d95926`.
- **Status pill colors:** 201/200/204 green `#008300`; 404 red `#e74c3c`.
- **Annotation (bold 13px violet `#4a3aa7`, right side near x=470, y=45):** "the URL never changes — only the verb does".
- **Caption (12px `#444`, bottom right):** "order details illustrative; status codes exact".

## Stateless Requests Let Any Server Answer

**Tags:** `where it's used` (blue), `statelessness` (green), `scaling` (orange)

- **The rule** — each request carries everything needed (URL, verb, credentials); servers remember nothing
- **Scaling** — the shop's 3 servers are interchangeable: any one can answer any request
- **Caching** — a GET is safe to cache, because reading `/orders/17` never changes anything
- **Retries** — a timed-out GET or DELETE can be safely resent; the request is self-contained
- **Everywhere** — internal microservices, data APIs, and cloud storage all speak this same grammar

*Example (italic):* Three GETs for `/orders/17` from the same phone land on servers A, B, and C — all three answer 200 identically, because no server had to remember the phone.

**Key point:** Statelessness is what makes REST scale — since no request depends on server memory of a previous one, you add servers freely and cache or retry reads without fear.

### Visualization (canvas `c3`, 720×300)

Fan-out diagram: one client, a load balancer, and 3 interchangeable servers, with the same self-contained request landing on a different server each time.

- **Title (bold 15px, `#1a5276`, top center):** "Stateless: Any of 3 Servers Can Answer Any Request".
- **Client box:** rounded box at x=30, y=130, 110×44, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` label "phone app".
- **Load balancer box:** at x=290, y=130, 130×44, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, 12px label "load balancer".
- **Server boxes (x=550, 140×40 each, 8px radius, fill `rgba(0,131,0,0.12)`, 2px `#008300` border):** "server A" at y=58, "server B" at y=132, "server C" at y=206.
- **Arrows:** 2px `#6b7280` arrow client → load balancer with 12px `#2c3e50` label "GET /orders/17" above it; three 2px `#6b7280` arrows load balancer → each server, each with 12px `#008300` label "→ 200" at its midpoint.
- **Annotation (bold 13px aqua `#199e70`, centered near y=275):** "each request carries everything — no server memory needed".
- **Caption (12px `#444`, bottom right):** "servers and routing illustrative".

## Verbs Belong in the Method, Not the URL

**Tags:** `common mistake` (red), `URL design` (orange), `sessions` (blue)

- **The smell** — paths like `/getOrder?id=17`, `/orders/17/delete`, `/updateOrder17` bake the action into the noun
- **Why it hurts** — every new action needs a new endpoint name, and the verb and path can disagree
- **GET with side effects** — a `GET /orders/17/delete` link means a prefetching browser can cancel orders
- **Sticky sessions** — storing the cart in server memory pins a client to one server; that server dies, the cart dies
- **The fix** — keep the URL a noun (`/orders/17`) and let the standard verb say delete, read, or change

*Example (italic):* A crawler follows every link on an admin page; because delete was a GET URL, it cancels all of today's orders — DELETE requests would never have been followed.

**Common mistake:** Putting the action in the path. Once `/orders/17/delete` exists, caches, retries, and link-following tools can no longer trust that a GET is safe to repeat.

### Visualization (canvas `c4`, 720×300)

Two-row comparison: verb-in-URL endpoints (red, one endpoint per action) vs RESTful endpoints (green, one URL plus standard verbs).

- **Title (bold 15px, `#1a5276`, top center):** "Verbs in the URL Recreate the Problem REST Solves".
- **Row 1 (y=90), label 12px `#444` at x=20:** "verb-in-URL"; three rounded boxes (150×40, 8px radius, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, 12px `#2c3e50` mono text) at x=140, x=320, x=500: "/getOrder?id=17", "/orders/17/delete", "/updateOrder17"; bold 12px red `#e74c3c` "✗ three names for one thing" beneath at y=145.
- **Row 2 (y=195), label:** "RESTful"; three rounded boxes (150×40, fill `rgba(0,131,0,0.12)`, 2px `#008300` border) at x=140, x=320, x=500: "GET /orders/17", "DELETE /orders/17", "PUT /orders/17"; bold 12px green `#008300` "✓ one URL, standard verbs" beneath at y=250.
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "if the URL contains a verb, the method and the path now disagree".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all content is the hardcoded arrays above (no randomness); order 17, the $4.50 large latte, the 3 servers, and the six-request ledger (verbs POST/GET/PATCH/PUT/DELETE/GET with codes 201/200/200/200/204/404) are invented and labeled illustrative; the HTTP verb semantics and status-code meanings (201 Created, 204 No Content, 404 Not Found) are exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
