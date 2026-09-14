# Node.js

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Node.js

**Subtitle:** Node.js takes the JavaScript engine out of the browser so the same language that draws a web page can also run the server behind it

## The Coffee Shop's Price Function Leaves the Browser

**Tags:** `core idea` (blue), `one language` (green), `V8 engine` (orange)

- **The page** — a coffee shop's online order page runs JavaScript in the visitor's browser to total the cart
- **The function** — `priceOrder()` adds the items, applies the loyalty discount, and returns the total
- **The escape** — in 2009 Node.js wrapped Chrome's V8 engine so JavaScript could run with no browser at all
- **The server** — the shop's server now runs the very same `priceOrder()` file to verify each total before charging
- **No rewrite** — browser and server share one function, so the two totals can never disagree

*Example (italic):* The customer's tab shows $9.40 for two lattes, and the server independently computes $9.40 — from the same file, not a Python re-implementation.

**Key point:** Node.js is JavaScript running outside the browser — the same language and the same V8 engine, now able to read files, open sockets, and serve web requests.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one shared code file at the top feeding two runtime boxes below — a browser tab on the left, a Node server on the right — each producing the same $9.40.

- **Title (bold 15px, `#1a5276`, top center):** "One JavaScript File, Two Places It Runs".
- **Code box:** rounded rectangle 170×42 centered at x=360, y=75, fill `rgba(26,82,118,0.10)`, 1px `#1a5276` border, bold 13px `#1a5276` text "priceOrder.js".
- **Arrows:** two 3px `#6b7280` arrows from the code box's bottom corners down to the two runtime boxes, each with a 12px `#6b7280` midpoint label "same file".
- **Browser box (left):** rounded rectangle 240×64 at x=70, y=165, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border; bold 13px `#2a78d6` line "Browser tab", 12px `#2c3e50` line "customer sees $9.40 instantly"; 12px `#6b7280` caption below the box: "V8 engine inside the browser".
- **Server box (right):** rounded rectangle 240×64 at x=410, y=165, fill `rgba(0,131,0,0.12)`, 1px `#008300` border; bold 13px `#008300` line "Node server", 12px `#2c3e50` line "verifies $9.40 before charging"; 12px `#6b7280` caption below the box: "same V8 engine, no browser".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "one function on both sides — the totals can never drift".
- **Caption (12px `#444`, bottom right):** "prices illustrative".

## Five Slow Orders on a Single Thread

**Tags:** `worked example` (blue), `event loop` (green), `concurrency` (orange)

- **The setup** — a tiny Node HTTP server takes orders; each needs 2ms of JavaScript and a 200ms database wait
- **Blocking math** — a thread that waits with each request serves 5 orders in 5 × 202ms = 1,010ms
- **The event loop** — Node starts the database call, parks that request, and immediately picks up the next one
- **Overlap math** — the five 200ms waits overlap; total JavaScript work is only 5 × 2ms = 10ms
- **Hand-check** — order 5 starts its 2ms of JS at 8ms, waits 200ms, and answers at ~210ms — not 1,010ms

*Example (italic):* While order 1's database lookup sleeps for 200ms, the single thread has already started orders 2, 3, 4, and 5.

**Key point:** Node serves many requests at once not with many threads but by never letting its one thread wait — all waiting is handed to the operating system while the loop moves on.

### Visualization (canvas `c2`, 720×300)

Two stacked Gantt panels on a shared time axis: the blocking model runs 5 orders end to end (done at 1,010ms); the event-loop model overlaps all five waits (done at ~210ms).

- **Title (bold 15px, `#1a5276`, top center):** "5 Orders, One Thread: 1,010ms Blocking vs 210ms Event Loop".
- **Axes:** time 0–1,050ms mapped to x=140..680 (plot width 540); baseline 2px `#999` at y=255; tick labels "0", "200", "400", "600", "800", "1,000ms" (12px `#444`); panel labels 12px `#444` at x=20: "thread that waits" at y=85, "event loop" at y=195.
- **Blocking panel (bars 11px tall at y = 55, 70, 85, 100, 115):** five orange `#d95926` fill `rgba(217,89,38,0.25)` bars starting at ms `[0, 202, 404, 606, 808]`, each 202ms wide; bold 12px orange label "done at 1,010ms" just right of the last bar.
- **Event-loop panel (bars 11px tall at y = 165, 180, 195, 210, 225):** five aqua `#199e70` fill `rgba(25,158,112,0.20)` bars starting at ms `[0, 2, 4, 6, 8]`, each 202ms wide, with a solid green `#008300` sliver at each bar's start marking the 2ms of JS (drawn 6px wide for visibility); bold 12px green label "all done by ~210ms" just right of the bars.
- **Legend (12px `#2c3e50`, top right):** green square "JS work (2ms)", aqua square "database wait (200ms)".
- **Annotation (bold 13px green `#008300`, near x=400, y=150):** "same one thread — the waits overlap, the JS never does".
- **Caption (12px `#444`, bottom right):** "timings illustrative; JS slivers exaggerated for visibility".

## Why Data Dashboards Ship With Node

**Tags:** `where it's used` (blue), `npm` (green), `build tooling` (orange)

- **One language** — the team writing the browser chart code writes the dashboard API in the same language
- **Shared logic** — validation, date formatting, and the price function run identically on both sides
- **npm** — Node's package registry is where front-end libraries live; installing them means running Node
- **Build tooling** — bundlers, TypeScript, and linters are themselves Node programs, even for browser-only sites
- **Dashboards** — a data dashboard is charts (browser JS) plus a small API (Node JS): one runtime covers both

*Example (italic):* A data scientist who runs `npm install` to build a chart page is already running Node, whether or not a server is ever written.

**Key point:** Even teams that never deploy a Node server run Node every day — modern front-end tooling is JavaScript that needs a home outside the browser.

### Visualization (canvas `c3`, 720×300)

Stack diagram of a typical dashboard project: three runtime layers on the left (chart page, API, database) and the Node-powered build tools on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Runtime Behind a Typical Data Dashboard".
- **Left stack (rounded boxes 300×46 at x=60):** at y=70 blue box (fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border) "Chart page — JavaScript in the browser"; at y=140 green box (fill `rgba(0,131,0,0.12)`, 1px `#008300` border) "Dashboard API — JavaScript on Node"; at y=210 mute box (fill `#f8f9fa`, 1px `#6b7280` border) "Database — not JavaScript"; 3px `#6b7280` arrows connecting the boxes vertically; all box text 12px `#2c3e50`.
- **Right stack (rounded boxes 230×46 at x=430):** at y=95 aqua box (fill `rgba(25,158,112,0.15)`, 1px `#199e70` border) "npm packages"; at y=175 violet box (fill `rgba(74,58,167,0.12)`, 1px `#4a3aa7` border) "bundler + linter — Node programs"; bold 12px `#6b7280` bracket label "build time" at x=545, y=70.
- **Dashed link:** dashed `#6b7280` (dash 4/3) line from the violet box to the blue chart-page box, 11px `#6b7280` label "produces the page's JS".
- **Annotation (bold 13px green `#008300`, centered near y=278):** "one language from the chart to the API".
- **Caption (12px `#444`, bottom right):** "layout schematic".

## One Thread for JS Is Not One Thread for Everything

**Tags:** `common mistake` (red), `CPU-bound` (orange), `blocking` (blue)

- **The myth** — "Node is single-threaded" is half true: one thread runs your JS, but I/O waits happen off it
- **The flip side** — a long stretch of pure computation has nowhere to hide; nothing else runs until it ends
- **The freeze** — a 300ms report calculation starting at t=100ms blocks every request until t=400ms
- **The math** — an order arriving at t=150ms waits 400 − 150 + 5 = 255ms instead of its usual 5ms
- **The fix** — push CPU-heavy work to a worker thread or a separate service; keep the loop free to loop

*Example (italic):* One user requesting a big CSV export makes every other customer's $9.40 order hang for up to 305ms.

**Common mistake:** Assuming Node's concurrency covers all slowness. The event loop only hides waiting (I/O); it cannot hide computing — CPU-bound JavaScript blocks every request on the box.

### Visualization (canvas `c4`, 720×300)

Line chart of response latency by arrival time: flat at 5ms, spiking to 305ms for requests that arrive while a 300ms pure-JS report calculation holds the thread.

- **Title (bold 15px, `#1a5276`, top center):** "A 300ms Calculation Freezes Every Other Request".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 175; x = arrival time 0–500ms with 12px `#444` tick labels every 100ms; y = response latency 0–320ms, gridlines `#e5e9ef` at 100/200/300 with 11px `#6b7280` labels.
- **Block band:** light red fill `rgba(231,76,60,0.08)` from x=100ms to x=400ms, full plot height, 12px `#e74c3c` label "report running (pure JS)" at its top.
- **Latency line:** blue `#2a78d6` 3px line with 4px dots through arrivals `[0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500]`, latencies `[5, 5, 305, 255, 205, 155, 105, 55, 5, 5, 5]` — a cliff up at 100ms, a straight slide back to 5ms at 400ms.
- **Annotation (bold 13px red `#e74c3c`, near x=170, y=60):** "arrive at 100ms, wait 305ms".
- **Annotation 2 (bold 12px green `#008300`, near x=440, y=210):** "loop free again — back to 5ms".
- **Caption (12px `#444`, bottom right):** "latencies illustrative; 300ms block exact by construction".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the $9.40 order, the per-request timings (2ms JS + 200ms database wait), and the latency curve are invented and labeled illustrative; the derived totals (5 × 202 = 1,010ms; last event-loop reply at ~210ms; 400 − 150 + 5 = 255ms) follow from those inputs by arithmetic; Node's 2009 release, its use of Chrome's V8 engine, and its event-driven single-JS-thread model are documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
