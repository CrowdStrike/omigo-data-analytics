# Tomcat

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Tomcat

**Subtitle:** Apache Tomcat has run enterprise Java since 1999 — your code sees a request and a response object, and Tomcat handles the sockets, threads, and HTTP underneath

## The Orders Servlet That Never Touches a Socket

**Tags:** `core idea` (blue), `servlet contract` (green), `since 1999` (orange)

- **The servlet** — an orders servlet's `doGet` reads an order id from the request and writes JSON back
- **The contract** — the code receives an `HttpServletRequest` and fills an `HttpServletResponse`; nothing else
- **Tomcat's half** — accepting sockets, parsing HTTP, keep-alive, threads, and writing bytes back out
- **The split** — the servlet is maybe 30 lines; the machinery it never sees is what Tomcat provides
- **The lineage** — Tomcat was the original reference implementation of the servlet spec (1999) and is still the default choice

*Example (italic):* A browser opens a TCP socket, sends raw HTTP text, and the servlet author never sees any of it — just `request.getParameter("orderId")`.

**Key point:** The servlet contract is the whole deal: request object in, response object out. Tomcat owns everything between the network card and that method call.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: a browser request passing through Tomcat's machinery boxes into the one box the developer writes, then back out.

- **Title (bold 15px, `#1a5276`, top center):** "The Servlet Contract: Tomcat Owns Everything Outside One Method".
- **Browser box:** rounded rect at x=20, y=120, 90×50, fill `rgba(107,114,128,0.12)`, 12px `#2c3e50` label "browser".
- **Tomcat zone:** dashed `#6b7280` (dash 4/3) rounded rect from x=130 to x=470, y=60 to y=250, bold 12px `#6b7280` label "Tomcat" at its top-left inside corner; contains three blue `#2a78d6` boxes (each 95×44, fill `rgba(42,120,214,0.15)`, 12px text) at y=123: "accept socket" (x=145), "parse HTTP + keep-alive" (x=255, two-line label), "thread pool" (x=365).
- **App box:** green `#008300` rounded rect at x=500, y=110, 190×70, fill `rgba(0,131,0,0.12)`, bold 12px label "OrdersServlet.doGet(", second line "  request, response)".
- **Arrows:** 3px `#2c3e50` arrows connecting browser → each Tomcat box → app box left-to-right at y=145; one 2px `#6b7280` return arrow curving back along y=225 from the app box to the browser labeled 12px `#6b7280` "response bytes".
- **Annotation (bold 13px green `#008300`, centered near y=280):** "the developer writes only the green box".
- **Caption (12px `#444`, bottom right):** "schematic".

## 200 Open Connections, 8 Busy Threads

**Tags:** `worked example` (blue), `thread pool` (green)

- **The pool** — Tomcat's default `maxThreads` is 200: at most 200 requests execute at once
- **The load** — the orders servlet gets 200 requests/sec, each taking 40ms (illustrative)
- **The math** — 200 req/s × 0.04s = 8 threads busy on average (exact arithmetic, Little's law)
- **Keep-alive** — with the NIO connector, 200 browsers hold sockets open between requests without holding any thread
- **Hand-check** — a 40ms request means one thread serves 25 req/s, so 8 threads cover 200 req/s

*Example (italic):* At 2pm the connector shows 200 open keep-alive connections but only ~8 of the 200 pool threads mid-request — open sockets are cheap, busy threads are the scarce thing.

**Key point:** Connections and threads are different resources: Tomcat parks idle keep-alive sockets for free and spends a pool thread only for the milliseconds a request is actually executing.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart contrasting three quantities at the same 2pm instant: open connections, thread pool size, and threads actually busy.

- **Title (bold 15px, `#1a5276`, top center):** "Same Instant: 200 Sockets Open, 8 Threads Working".
- **Axis:** left edge of bars at x=230, 2px `#999` vertical baseline; bars extend right, scale 2px per unit (value 200 → width 400); 12px `#444` value labels at bar ends.
- **Rows (14px-tall bars at y = 90, 150, 210), each with right-aligned 12px `#444` label ending at x=220:**
  - "open keep-alive connections": blue `#2a78d6` bar, fill `rgba(42,120,214,0.30)`, width 400, end label "200"
  - "thread pool (maxThreads)": violet `#4a3aa7` bar, fill `rgba(74,58,167,0.25)`, width 400, end label "200 (Tomcat default)"
  - "threads busy right now": green `#008300` solid bar, width 16, end label "8"
- **Annotation (bold 13px green `#008300`, near x=300, y=250):** "200 req/s × 40 ms = 8 busy threads".
- **Caption (12px `#444`, bottom right):** "load illustrative; 8 = 200 × 0.04 exact; maxThreads default 200 documented".

## From WAR Files to Boot Jars

**Tags:** `where it's used` (blue), `Spring Boot` (green), `deployment` (orange)

- **The old way** — package the app as a WAR, drop it into a shared Tomcat's `webapps/` directory
- **Shared container** — one long-running Tomcat could host several teams' WARs side by side
- **The inversion** — Spring Boot embeds Tomcat inside the app jar: `java -jar app.jar` starts its own
- **What flipped** — the app used to live inside the container; now the container lives inside the app
- **Still everywhere** — embedded or standalone, Tomcat quietly serves a huge share of enterprise Java

*Example (italic):* The 2005 orders app was a WAR handed to the ops team's Tomcat; the 2020 rewrite is a self-contained jar that boots its own embedded Tomcat on port 8080.

**Key point:** Spring Boot didn't replace Tomcat — it repackaged it. The servlet contract and thread pool are the same; only who owns the process flipped.

### Visualization (canvas `c3`, 720×300)

Two side-by-side containment diagrams: apps inside a shared container (left) vs the container embedded inside each app jar (right).

- **Title (bold 15px, `#1a5276`, top center):** "The Inversion: App in Container vs Container in App".
- **Left panel — outer box:** blue `#2a78d6` rounded rect at x=40, y=70, 280×190, fill `rgba(42,120,214,0.10)`, bold 13px `#2a78d6` label "shared Tomcat" at its top; inside, three green `#008300` boxes (240×38, fill `rgba(0,131,0,0.12)`, 12px text) stacked at y=110/155/200: "orders.war", "billing.war", "reports.war".
- **Right panel — outer box:** green `#008300` rounded rect at x=400, y=70, 280×190, fill `rgba(0,131,0,0.10)`, bold 13px `#008300` label "orders app.jar" at its top; inside, one 12px text line "orders code (Spring Boot)" at y=125 and one blue `#2a78d6` box (240×50, fill `rgba(42,120,214,0.15)`, 12px text) at y=165: "embedded Tomcat".
- **Panel captions (12px `#444`, centered under each panel at y=285):** "classic: WARs deployed into it" / "modern: it ships inside the jar".
- **Arrow:** bold 3px `#d95926` arrow from x=330 to x=390 at y=165, bold 12px `#d95926` label "inverted" above it.
- **Annotation (bold 12px violet `#4a3aa7`, top right near y=55):** "same servlet contract in both".

## When the Payment Call Slows to Two Seconds

**Tags:** `common mistake` (red), `pool exhaustion` (orange)

- **The dependency** — the orders servlet calls a payment service that normally answers in 40ms
- **The slowdown** — one afternoon the payment service degrades to 2,000ms per call (illustrative)
- **The math** — 200 threads ÷ 2s each = 100 req/s capacity, against 300 req/s arriving (exact arithmetic)
- **The cliff** — within seconds all 200 threads sit blocked waiting; the accept queue fills, then connections are refused
- **The trap** — the servlet code is fine and the CPU is idle; the pool drowned waiting on someone else
- **The guardrails** — client timeouts and separate pools for slow dependencies stop one dependency taking all 200

*Example (italic):* At 40ms the pool needs 8 busy threads for 200 req/s; at 2,000ms the 300 req/s arriving would need 600 — three times the whole pool — so exhaustion is arithmetic, not bad luck.

**Common mistake:** Sizing the thread pool for your own handler time and forgetting that a blocked thread is a spent thread — a slow downstream converts a 4%-busy pool into a hard 100 req/s ceiling.

### Visualization (canvas `c4`, 720×300)

Timeline of the incident: busy threads (of 200) and refused requests per second, before and after the payment service slows at t=10s.

- **Title (bold 15px, `#1a5276`, top center):** "Downstream Slows at t=10s: 8 Busy Threads Become 200 in Seconds".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 60, 12px `#444` tick labels every 10s; y = 0 to 300, gridlines `#e5e9ef` at 100/200; dashed `#6b7280` (dash 4/3) horizontal line at y-value 200 labeled 12px `#6b7280` "maxThreads = 200".
- **Busy threads line:** blue `#2a78d6` 3px line through seconds `[0, 5, 10, 11, 12, 15, 20, 30, 45, 60]`, values `[8, 9, 8, 120, 200, 200, 200, 200, 200, 200]` — flat at 8, cliff to the 200 ceiling just after t=10.
- **Refused req/s line:** red `#e74c3c` 3px line through the same seconds, values `[0, 0, 0, 0, 0, 150, 190, 200, 200, 200]` — zero until the pool and accept queue saturate, then climbing to 200 refusals/s (300 arriving − 100 served).
- **Slowdown marker:** vertical dashed `#6b7280` line at t=10, 12px `#6b7280` label "payment: 40ms → 2,000ms" at its top.
- **Annotation (bold 13px red `#e74c3c`, near t=35, y=95):** "all 200 threads blocked waiting — CPU idle".
- **Caption (12px `#444`, bottom right):** "timings illustrative; 200 ÷ 2s = 100 req/s capacity exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); request rates, service times, and refusal counts are invented and labeled illustrative; the derived values 8 = 200 req/s × 0.04s, 600 = 300 req/s × 2s, and 100 req/s = 200 threads ÷ 2s are exact arithmetic on those illustrative inputs; `maxThreads` default 200 and the 1999 reference-implementation lineage are documented Tomcat facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
