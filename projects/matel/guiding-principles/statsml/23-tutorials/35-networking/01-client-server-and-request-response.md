# Client-Server & Request-Response

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Client-Server & Request-Response

**Subtitle:** Almost everything on the internet is one machine asking and another answering — the client sends a request, the server sends back a response

## The Coffee Counter: You Ask, the Barista Answers

**Tags:** `core idea` (blue), `request` (green), `response` (orange)

- **The counter** — a coffee shop has one barista at the counter and a line of customers
- **The request** — a customer states exactly what they want: "one medium latte"
- **The response** — the barista makes the drink and hands back exactly one answer per ask
- **Nothing unasked** — the barista never runs drinks out into the street; no request, no response
- **The mapping** — your phone is the customer (client), the shop's machine is the barista (server)

*Example (italic):* Three customers order in turn and the barista answers each ticket with one drink — three requests, three responses, always in that pairing.

**Key point:** A client sends a request and waits; a server listens, does the work, and sends back one response — that pairing is the basic unit of nearly all network communication.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three customer (client) boxes on the left, one barista (server) box on the right, paired request/response arrows between them.

- **Title (bold 15px, `#1a5276`, top center):** "One Counter, Many Customers: Every Exchange Starts With an Ask".
- **Client boxes:** three blue `#2a78d6` rounded boxes (140×40, 8px radius, fill `rgba(42,120,214,0.15)`) at x=50, y = 60 / 140 / 220, 12px `#2c3e50` labels "Customer 1 (client)", "Customer 2 (client)", "Customer 3 (client)".
- **Server box:** one green `#008300` rounded box (190×64, 8px radius, fill `rgba(0,131,0,0.12)`) at x=480, y=128, 13px bold `#2c3e50` label "Barista (server)".
- **Request arrows:** solid blue `#2a78d6` 2.5px arrows from each client box's right edge to the server box's left edge, 12px blue labels above each: "request: 1 latte", "request: 2 mochas", "request: 1 tea".
- **Response arrows:** dashed green `#008300` 2px arrows (dash 5/4) running back from server to each client, one 12px green label "response: the drink" below the arrow fan near the server side.
- **Annotation (bold 13px ink `#1a5276`, centered near y=280):** "no response ever leaves the counter without a request".
- **Caption (12px `#444`, bottom right):** "orders illustrative".

## Where 180 Milliseconds Go: One Request, Step by Step

**Tags:** `worked example` (blue), `latency` (green)

- **Same four steps** — a web request is the counter routine: find the shop, get in line, order, receive
- **Find the server** — DNS lookup turns the shop's name into a network address: 20 ms
- **Get in line** — opening the connection takes one round trip of hellos: 30 ms
- **The barista works** — the server builds the answer (looks up data, renders the page): 80 ms
- **Carry it back** — the finished response travels across the network to your phone: 50 ms
- **Hand-check** — 20 + 30 + 80 + 50 = 180 ms from tap to answer on screen

*Example (italic):* You tap at 0 ms and see the answer at 180 ms — and only 80 of those milliseconds were the server actually working.

**Key point:** A request's total time is a sum of steps you can budget line by line — and the network steps often cost more than the server's own work.

### Visualization (canvas `c2`, 720×300)

Waterfall chart of one request: four horizontal segments on a shared 0–180 ms axis, each starting where the previous one ends.

- **Title (bold 15px, `#1a5276`, top center):** "Anatomy of One Request: 180 ms From Tap to Answer".
- **Axes:** time axis from x=200 (0 ms) to x=680 (180 ms), so 1 ms = 480/180 px; 2px `#999` baseline at y=245; 12px `#444` tick labels at 0 / 60 / 120 / 180 ms; vertical gridlines `#e5e9ef` at those ticks.
- **Rows (bars 26px tall at y = 70, 115, 160, 205), each with a right-aligned 12px `#444` label at x=190:**
  - "DNS lookup — 20 ms": blue `#2a78d6` bar from 0 to 20 ms (x 200→253)
  - "connect — 30 ms": aqua `#199e70` bar from 20 to 50 ms (x 253→333)
  - "server works — 80 ms": orange `#d95926` bar from 50 to 130 ms (x 333→547)
  - "response travels — 50 ms": green `#008300` bar from 130 to 180 ms (x 547→680)
- **Bar style:** solid fills at 85% opacity, 11px `#444` duration labels at each bar's right end.
- **Annotation (bold 13px orange `#d95926`, near x=340, y=50):** "the server's 80 ms is less than half the wait — the network eats the rest".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Why One Big Ask Beats a Thousand Small Ones

**Tags:** `where it's used` (blue), `round trips` (orange)

- **Everywhere** — a notebook calling an API, a dashboard querying a database, a model behind an endpoint
- **The tax** — every request pays the fixed find/connect/travel cost (here 100 ms) before any data moves
- **Chatty code** — fetching 1,000 rows one per fresh connection pays that tax 1,000 times: 180 s
- **Batched ask** — one request for all 1,000 rows pays the tax once: about 0.4 s total
- **The habit** — count round trips before counting bytes; trips, not data size, dominate small fetches

*Example (italic):* A training script that calls the feature API once per row runs 3 minutes; the same script asking once for all 1,000 rows finishes in under half a second.

**Key point:** Request-response makes every remote ask expensive compared to a local one — good data code asks few, big questions instead of many small ones.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing total time to fetch 1,000 rows: one batched request vs 1,000 single-row requests.

- **Title (bold 15px, `#1a5276`, top center):** "Fetching 1,000 Rows: One Ask vs 1,000 Asks".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440; pixel widths schematic (log-feel by hardcoded widths, not a real log axis).
- **Rows (bars 28px tall), each with a right-aligned 12px `#444` label at x=240:**
  - y=100 "one batched request — 0.4 s": green `#008300` bar width 10, 11px green label "0.4 s" at its end
  - y=180 "1,000 single-row requests — 180 s": red `#e74c3c` bar width 440, bold 12px red label "180 s — 450× slower" at its end (inside the bar, right-aligned)
- **Detail strip (12px `#6b7280`, under the red bar at y=225):** "each of the 1,000 asks repeats the 100 ms round-trip tax + 80 ms of work".
- **Annotation (bold 13px magenta `#d55181`, centered near y=265):** "the round trip, not the data, is the cost".
- **Caption (12px `#444`, bottom right):** "timings illustrative; bar widths schematic, not to scale".

## Client and Server Are Roles, Not Machines

**Tags:** `common mistake` (red), `roles` (orange)

- **The label rule** — client and server name who asks and who answers in one exchange, not which box is which
- **Chains** — the web server answering your phone turns around and asks the database: now it is the client
- **Any machine** — your laptop becomes a server the moment another program sends it a request
- **No pushing** — in plain request-response the server cannot start a conversation; clients must ask (or poll)
- **The mistake** — hard-coding "server = the big machine" hides who is waiting on whom when things slow down

*Example (italic):* When the shop's order page is slow, the "server" may be innocently waiting as a client on a slower database behind it.

**Common mistake:** Treating client and server as fixed kinds of machines. They are roles per exchange — the asker is the client, the answerer is the server, and most machines play both in the same second.

### Visualization (canvas `c4`, 720×300)

Chain diagram: phone asks the app server, which in turn asks the database — the middle box wears both labels at once.

- **Title (bold 15px, `#1a5276`, top center):** "One Machine, Two Roles: Server to the Phone, Client to the Database".
- **Boxes (rounded, 8px radius, 46px tall, 13px `#2c3e50` labels, centered on y=130):** blue `#2a78d6` box "Phone" at x=40 width 130 (fill `rgba(42,120,214,0.15)`); aqua `#199e70` box "App server" at x=290 width 150 (fill `rgba(25,158,112,0.12)`); violet `#4a3aa7` box "Database" at x=550 width 130 (fill `rgba(74,58,167,0.10)`).
- **Request arrows (solid blue `#2a78d6` 2.5px, above the boxes at y=110):** Phone→App server labeled 12px blue "request: menu page"; App server→Database labeled "request: today's menu".
- **Response arrows (dashed green `#008300` 2px, dash 5/4, below the boxes at y=160):** Database→App server labeled 12px green "response: menu rows"; App server→Phone labeled "response: the page".
- **Role tags (bold 12px `#c98500`, under the App server box at y=210):** "server (to the phone) · client (to the database)".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=265):** "the middle box plays both roles in the same request".
- **Caption (12px `#444`, bottom right):** "labels illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); the step timings (20 / 30 / 80 / 50 ms) and fetch totals (0.4 s vs 180 s) are invented and labeled illustrative; the 180 ms total, the 100 ms per-request tax, and the 450× ratio follow from those numbers by plain arithmetic (20+30+80+50 = 180; 1,000 × 180 ms = 180 s; 180 / 0.4 = 450).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
