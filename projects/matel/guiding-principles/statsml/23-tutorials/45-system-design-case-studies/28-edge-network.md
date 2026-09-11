# Edge Network

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Edge Network

**Subtitle:** How a global edge network absorbs attacks by being everywhere — one IP announced from hundreds of cities, so no single building is ever the target (publicly documented architecture concepts)

## One Address, Announced From Everywhere

**Tags:** `core idea` (blue), `anycast` (green), `BGP routing` (orange)

- **The address** — a site behind the network resolves to one IP, say 203.0.113.1, everywhere on Earth
- **The trick** — hundreds of edge locations all announce that same IP into BGP, the internet's routing system
- **The landing** — each client's packets follow the shortest advertised path to the nearest location
- **No dispatcher** — no central load balancer picks the destination; internet routing itself decides
- **The name** — this is anycast: one address, many places, the nearest announcement wins

*Example (italic):* A Mumbai user and a Paris user type the same URL, hit the same IP, and are served by datacenters 7,000 km apart.

**Key point:** Anycast means the same IP is announced from every edge location, so "which datacenter serves you" is decided by the internet's routing tables, not by any single coordinating machine.

### Visualization (canvas `c1`, 720×300)

Row-flow diagram: four clients on the left, each connected straight across to its nearest edge location on the right — every edge box carries the identical IP.

- **Title (bold 15px, `#1a5276`, top center):** "Four Clients, One IP: Routing Sends Each to the Nearest Location".
- **Left column (client boxes):** rounded boxes 150px wide, 34px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text, at x=40, y = `[58, 112, 166, 220]`, labeled top to bottom "Mumbai client", "Tokyo client", "Paris client", "São Paulo client".
- **Right column (edge boxes):** same style but fill `rgba(0,131,0,0.12)`, 200px wide, at x=470, same y values, labeled "Mumbai edge · 203.0.113.1", "Tokyo edge · 203.0.113.1", "Paris edge · 203.0.113.1", "São Paulo edge · 203.0.113.1".
- **Arrows:** 3px `#2a78d6` horizontal arrow per row from x=190 to x=470, with a 12px `#444` round-trip label centered above each: `["6 ms", "4 ms", "8 ms", "12 ms"]`.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=278):** "same IP announced from hundreds of cities — the network picks the closest".
- **Caption (12px `#444`, bottom right):** "RTTs illustrative".

## Splitting a 3,000 Gbps Attack Into 10 Gbps Slices

**Tags:** `worked example` (blue), `DDoS` (red), `divide and absorb` (green)

- **The attack** — a botnet floods the target IP with 3,000 Gbps of junk traffic (illustrative sizes)
- **One target** — against a single datacenter with 200 Gbps capacity, that is 15× overload: instant outage
- **The split** — anycast scatters the flood: each bot's traffic lands at its own nearest edge location
- **The slice** — 3,000 Gbps ÷ 300 locations = 10 Gbps per site, just 5% of one location's 200 Gbps
- **Hand-check** — even if traffic skews 3× toward a few sites, 30 Gbps is still far under 200 Gbps

*Example (italic):* The flood that would flatten any single building arrives as a 10 Gbps nuisance at each of 300 doors — and every door was built for 200.

**Key point:** The attacker cannot choose where their packets land — anycast divides the attack across the whole fleet automatically, so each location absorbs only its local slice.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: total attack size vs one datacenter's capacity vs the per-location slice after the anycast split.

- **Title (bold 15px, `#1a5276`, top center):** "3,000 Gbps ÷ 300 Locations = 10 Gbps Each".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 430; widths hardcoded pixels (schematic scale, not linear).
- **Rows (top to bottom at y = 80, 145, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "attack total — 3,000 Gbps": red `#e74c3c` bar width 430
  - "one datacenter absorbs — 200 Gbps": blue `#2a78d6` bar width 140, bold 12px red label at bar end "attack is 15× this — site down"
  - "per-location slice (÷300) — 10 Gbps": green `#008300` bar width 28, bold 12px green label at bar end "5% of local capacity — absorbed"
- **Bar style:** 22px tall, fills red `rgba(231,76,60,0.75)`, blue `rgba(42,120,214,0.55)`, green solid `#008300`; 11px `#444` Gbps value labels inside or at bar ends.
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "the fleet is the shield — no single building is the target".
- **Caption (12px `#444`, bottom right):** "sizes illustrative, pixel widths schematic".

## Every Server Runs Everything

**Tags:** `where it's used` (blue), `fungible capacity` (green), `edge compute` (orange)

- **Same stack** — every server in every location runs the full software: CDN cache, WAF, DNS resolver
- **Fungible capacity** — any machine can absorb any workload, so no box idles waiting for "its" job
- **Latency win** — serving from the nearest edge cuts Mumbai's round trip from 240 ms to 15 ms (illustrative)
- **Edge compute** — the same everywhere-fleet can run customer code next to users, not in one far region
- **One fleet** — capacity added for caching also soaks attacks; there is no separate scrubbing tier

*Example (italic):* An edge server in São Paulo spends Tuesday serving cached video and Wednesday absorbing a DNS flood — same box, same software.

**Key point:** Homogeneous servers turn the whole fleet into one pool — hardware bought for content delivery doubles as attack-absorbing armor, and code deployed once runs where the users are.

### Visualization (canvas `c3`, 720×300)

Grouped vertical bar chart: round-trip time from five cities to a single Virginia datacenter vs to each city's nearest edge location.

- **Title (bold 15px, `#1a5276`, top center):** "Round Trip to One Virginia Datacenter vs the Nearest Edge".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = milliseconds 0 to 250, gridlines `#e5e9ef` at 50/100/150/200 with 12px `#444` tick labels; x = five city groups with 12px `#444` labels "New York", "Paris", "São Paulo", "Mumbai", "Sydney".
- **Single-origin bars (left of each pair):** magenta `#d55181`, values `[12, 90, 140, 240, 210]` ms.
- **Nearest-edge bars (right of each pair):** green `#008300`, values `[6, 8, 18, 15, 12]` ms.
- **Bar style:** 34px wide, 8px gap within a pair, 11px `#444` value labels above each bar.
- **Legend (12px, top right inside plot):** magenta swatch "to Virginia", green swatch "to nearest edge".
- **Annotation (bold 13px green `#008300`, above the Mumbai pair):** "Mumbai: 240 ms → 15 ms".
- **Caption (12px `#444`, bottom right):** "RTTs illustrative".

## Not the Same as DNS Load Balancing

**Tags:** `common mistake` (red), `geo-DNS` (orange)

- **The confusion** — "isn't this just geo-DNS?" — no: geo-DNS hands different IPs to different regions
- **Stale answers** — DNS replies get cached; when a region dies, cached IPs keep sending users there
- **Anycast heals** — one IP everywhere: a failed location withdraws its route, BGP reroutes in seconds
- **The catch** — a mid-connection route change lands packets at a different site, resetting long TCP flows
- **The mitigation** — keep edge work short and stateless, so a reroute costs one retried request

*Example (italic):* With a 5-minute DNS TTL, users keep hitting a dead regional IP for up to 5 minutes; with anycast, routing shifts them to the next-nearest site in seconds.

**Common mistake:** Treating anycast and DNS load balancing as interchangeable — DNS steers by handing out different addresses (and inherits cache staleness), while anycast steers packets to one address at the routing layer, below DNS entirely.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a regional failure under geo-DNS (stale cached IP, failed requests) vs under anycast (route withdrawn, traffic reflows).

- **Title (bold 15px, `#1a5276`, top center):** "Region Fails: Geo-DNS Serves Stale IPs, Anycast Reroutes One IP".
- **Row 1 (y=95), label 12px `#444` at x=20:** "geo-DNS"; blue `#2a78d6` rounded box at x=130 labeled "DNS returns 198.51.100.7 (EU IP)" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "EU site down — cached answer still points there", bold 12px red beneath "✗ up to TTL minutes of failures".
- **Row 2 (y=205), label:** "anycast"; blue box at x=130 labeled "everyone dials 203.0.113.1", 3px arrow to a green `#008300` box at x=420 labeled "EU route withdrawn — BGP shifts to next-nearest", bold 12px green beneath "✓ reroute in seconds".
- **Box style:** 240–260px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "caveat: a route flap mid-connection resets long-lived TCP sessions".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); attack sizes, capacities, location count, and RTTs are invented and labeled illustrative; the derived figures (3,000 ÷ 300 = 10 Gbps, 10/200 = 5%, 15× overload) must stay arithmetically consistent between text and charts. Frame everything as publicly documented/blogged architecture concepts — no claims beyond what the company has published (anycast fleet, homogeneous full-stack servers, edge compute).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
