# API Gateways & Service Mesh

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** API Gateways & Service Mesh

**Subtitle:** The traffic layer of microservices — the gateway guards the front door for clients coming in (north-south), the mesh manages the chatter between services inside (east-west)

## One Front Door for the Food-Delivery App

**Tags:** `core idea` (blue), `north-south` (green), `one entry point` (orange)

- **The app** — a food-delivery backend runs four services: orders, menu, payments, delivery
- **The door** — every client request hits one gateway first; no client ever calls a service directly
- **Its jobs** — terminate TLS, check the auth token, rate-limit, route `/orders` to the orders service
- **The shield** — clients never learn there are four services; the gateway can also merge two responses into one
- **Without it** — all four services reimplement auth, and every client hardcodes your internal topology

*Example (italic):* The mobile app calls GET /orders/981; the gateway checks the token, then forwards it to the orders service — the app never knows which service answered.

**Key point:** An API gateway is the single entry point for north-south traffic (clients → your services): it authenticates, rate-limits, routes, and transforms once, so no service repeats that work and no client sees your internals.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three client boxes on the left funnel into one tall gateway box in the middle, which fans out to four service boxes on the right, with route labels on the fan-out arrows.

- **Title (bold 15px, `#1a5276`, top center):** "North-South: Every Client Enters Through One Door".
- **Client boxes (left column, x=30, width 130, height 34, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text, centered at y = 85, 150, 215):** "mobile app", "web app", "partner API".
- **Gateway box (x=250, y=70, width 160, height 170, 8px radius, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border):** bold 13px `#1a5276` label "API gateway" at its top, then 12px `#444` lines "TLS", "auth", "rate limit", "route" stacked below.
- **Client arrows:** 3px `#2a78d6` arrows from each client box's right edge to the gateway's left edge.
- **Service boxes (right column, x=560, width 130, height 30, 8px radius, fill `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, centered at y = 78, 130, 182, 234):** "orders svc", "menu svc", "payments svc", "delivery svc".
- **Fan-out arrows:** 3px `#008300` arrows from the gateway's right edge to each service box, each carrying an 11px `#6b7280` route label above it: "/orders", "/menu", "/pay", "/track".
- **Annotation (bold 13px violet `#4a3aa7`, bottom center near y=285):** "clients see one address; services stay hidden behind it".

## The Retry Nobody Had to Write

**Tags:** `worked example` (blue), `east-west` (green), `sidecar proxy` (orange)

- **The chatter** — placing one order fans out inside: orders calls payments, menu, delivery, and notify — 4 hops
- **The old way** — each service wrote its own retry and timeout code, in Go, Java, Python, and Node, inconsistently
- **The sidecar** — a small proxy runs beside each service instance and intercepts all traffic in and out of it
- **Hand-check** — each hop fails 2% of the time; one sidecar retry cuts that to 2% × 2% = 0.04% per hop
- **Across 4 hops** — end-to-end success climbs from 0.98⁴ ≈ 92.2% to 0.9996⁴ ≈ 99.8%, with zero app code changed
- **Control plane** — the mesh is the fleet of sidecars plus a control plane that pushes each one its config

*Example (italic):* Order #981 crosses 4 internal hops; with sidecar retries it succeeds 99.8% of the time instead of 92.2% — and no service's source code mentions a retry.

**Key point:** A service mesh handles east-west traffic (service → service): sidecar proxies apply retries, timeouts, mTLS, and metrics uniformly at the network layer, so the policy lives in one config instead of five codebases.

### Visualization (canvas `c2`, 720×300)

Line chart of end-to-end request success rate vs number of internal hops, comparing no retries (falling) against one sidecar retry per hop (nearly flat).

- **Title (bold 15px, `#1a5276`, top center):** "One Sidecar Retry per Hop: 92.2% → 99.8% at 4 Hops".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hops 1 to 8 with 12px `#444` tick labels at every hop; y = success % from 84 to 100, gridlines `#e5e9ef` at 88/92/96, 12px `#444` labels.
- **No-retry line:** red `#e74c3c` 3px line through hops `[1, 2, 3, 4, 5, 6, 7, 8]`, success % `[98.0, 96.0, 94.1, 92.2, 90.4, 88.6, 86.8, 85.1]` (0.98^n).
- **Retry line:** green `#008300` 3px line through the same hops, success % `[99.96, 99.92, 99.88, 99.84, 99.80, 99.76, 99.72, 99.68]` (0.9996^n).
- **Markers:** 4px filled circles at hop 4 on both lines, with bold 12px labels: red "92.2%" below the red point, green "99.8%" above the green point.
- **Legend (12px, top right inside plot):** red swatch "no retries (2% per hop)", green swatch "1 sidecar retry per hop".
- **Annotation (bold 13px green `#008300`, near hop 6, y=110):** "same services, zero code changed — the sidecar retries".
- **Caption (12px `#444`, bottom right):** "2% per-hop failure rate illustrative".

## What the Mesh Buys — and What It Charges

**Tags:** `where it's used` (blue), `trade-off` (orange), `rule of thumb` (green)

- **Buys: mTLS** — every service-to-service call encrypted and mutually authenticated with zero app code
- **Buys: telemetry** — every hop emits the same golden-signal metrics and traces, in every language
- **Buys: traffic control** — a canary taking 5% of traffic is one line of config, not a code release
- **Charges: latency** — each hop crosses two proxies, adding roughly 2ms per hop (illustrative)
- **Charges: ops** — a fleet of sidecars to run and upgrade, plus a control plane that can itself fail
- **The rule** — adopt at mesh-scale problems: dozens of services, polyglot teams, security or compliance mandates

*Example (italic):* A 5-hop checkout with 120ms of app time pays about 10ms of proxy tax — 8.3% overhead in exchange for fleet-wide mTLS and canary rollouts by config.

**Key point:** The mesh trades a proxy hop's latency and real operational complexity for uniform security, reliability, and observability — a good trade only once your service count makes per-service solutions unmanageable.

### Visualization (canvas `c3`, 720×300)

Horizontal stacked bar chart: for requests with 1, 3, 5, and 8 internal hops, app processing time (blue) plus mesh proxy overhead (orange), with the overhead percentage labeled at each bar's end.

- **Title (bold 15px, `#1a5276`, top center):** "The Proxy Tax: ~2ms per Hop on Top of App Time".
- **Layout:** bars start at x=180, max total width 448; scale = 196ms full width; rows centered at y = 75, 125, 175, 225, bar height 22px; left-aligned 12px `#444` row labels at x=20: "1 hop", "3 hops", "5 hops", "8 hops".
- **App-time segments (fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` edge):** widths for app ms `[40, 90, 120, 180]`.
- **Overhead segments (solid orange `#d95926`, stacked to the right of each blue segment):** widths for overhead ms `[2, 6, 10, 16]` (2ms per hop).
- **End labels (11px `#444` just past each bar):** "42ms · +5.0%", "96ms · +6.7%", "130ms · +8.3%", "196ms · +8.9%".
- **Legend (12px, top right):** blue swatch "app time", orange swatch "mesh overhead".
- **Annotation (bold 13px orange `#d95926`, bottom center near y=270):** "single-digit % latency buys mTLS, retries, metrics, canaries".
- **Caption (12px `#444`, bottom right):** "all timings illustrative".

## Not Two Names for the Same Box

**Tags:** `common mistake` (red), `direction` (orange)

- **The confusion** — gateway and mesh both proxy traffic, so teams assume one replaces the other
- **The compass** — the gateway faces out (north-south, clients in); the mesh faces in (east-west, service to service)
- **They compose** — gateway at the edge, mesh inside; the gateway's own calls to services ride the mesh too
- **Too early** — installing a mesh for 3 services buys the operational cost before the problems exist
- **Sidecar-less** — newer "ambient" mesh variants move the proxy off the pod to cut the per-instance cost

*Example (italic):* A team with 3 services adopts a mesh "for the future" and spends more time upgrading sidecars than they ever spent writing retries by hand.

**Common mistake:** Treating the gateway and the mesh as competing products. They differ by traffic direction — north-south vs east-west — and mature systems run both: one door at the edge, uniform plumbing inside.

### Visualization (canvas `c4`, 720×300)

Compass diagram: a cluster boundary box with the gateway sitting on its top edge; a vertical north-south arrow enters from clients above, and horizontal east-west arrows connect three sidecar-equipped services inside.

- **Title (bold 15px, `#1a5276`, top center):** "Two Directions, Two Layers: Gateway Outside, Mesh Inside".
- **Cluster boundary:** rounded rect x=110, y=95, width 560, height 185, 10px radius, 2px dashed `#6b7280` border, 11px `#6b7280` label "your cluster" just inside its top-left corner.
- **Clients label:** 12px `#2c3e50` text "clients" centered at (x=390, y=48).
- **Gateway box:** x=320, y=80, width 140, height 32, 8px radius, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border, bold 12px `#1a5276` label "API gateway", straddling the cluster's top edge.
- **North-south arrow:** 3px `#2a78d6` vertical arrow from (390, 58) down into the gateway, bold 12px blue `#2a78d6` label "north-south" to its right at y≈68.
- **Service boxes (inside, centered at y=210, width 110, height 40, 8px radius, fill `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, x = 150, 335, 520):** "orders", "payments", "delivery"; each with a small 14×14 solid violet `#4a3aa7` square attached to its top-right corner (the sidecar), one shared 11px violet label "sidecar proxies" pointing at the squares from y≈165.
- **East-west arrows:** 3px `#199e70` double-headed horizontal arrows between adjacent service boxes, bold 12px aqua `#199e70` label "east-west (mesh)" centered below them at y≈250.
- **Gateway-to-service arrow:** 2px `#6b7280` arrow from the gateway's bottom edge down to the orders box, 11px `#6b7280` label "rides the mesh too".
- **Annotation (bold 13px magenta `#d55181`, bottom center near y=290):** "one door for outsiders, uniform plumbing between insiders".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 2% per-hop failure rate and all latency timings are invented and labeled illustrative; the success-rate arrays are exact powers of 0.98 and 0.9996 rounded to the shown precision, and the overhead percentages (5.0 / 6.7 / 8.3 / 8.9) follow exactly from the hardcoded app/overhead millisecond arrays.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
