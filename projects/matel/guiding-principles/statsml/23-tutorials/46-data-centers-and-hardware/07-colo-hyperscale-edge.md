# Colo, Hyperscale, Edge

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Colo, Hyperscale, Edge

**Subtitle:** Three ways to have a data center — rent a cage in someone's building, rent slices of a giant's campus, or scatter tiny sites near your users

## Three Buildings, Three Deals

**Tags:** `core idea` (blue), `deployment models` (green)

- **The startup closet** — a delivery app begins with six servers on a shelf in the office storage room
- **Colo** — the servers move to a rented cage: their building, power, cooling, guards; still your machines
- **Hyperscale** — "the cloud" is someone's purpose-built 100 MW campus; you rent slices and own nothing
- **Edge** — hundreds of small sites near users — CDN PoPs, cell-tower closets — trading size for closeness
- **The trade** — each step swaps ownership and control for someone else's scale and expertise

*Example (italic):* The delivery app's checkout runs in a hyperscale region, its menu photos come off edge PoPs, and its old accounting box still hums in a colo cage.

**Key point:** Colocation rents you the building around your own servers; hyperscale rents you the servers too; edge rents you proximity — many tiny sites instead of one big one.

### Visualization (canvas `c1`, 720×300)

Three-column ownership diagram: the same four stacked layers (building, power/cooling, servers, your software) under each deployment model, colored by who owns each layer.

- **Title (bold 15px, `#1a5276`, top center):** "Who Owns What: the Same Four Layers, Three Different Deals".
- **Columns:** three columns of boxes at x = 80, 300, 520, each box 160px wide; bold 13px `#1a5276` column headers centered above at y=58: "Colo", "Hyperscale", "Edge (×200 sites)".
- **Layer stacks:** four rounded boxes per column (6px radius, 160×38) at y = 70, 114, 158, 202, labeled top to bottom in 12px `#2c3e50`: "your software", "servers", "power / cooling", "building".
- **Ownership fills:** blue `rgba(42,120,214,0.25)` with 2px `#2a78d6` border = you own it; green `rgba(0,131,0,0.16)` with 2px `#008300` border = the operator owns it. Colo column: top two boxes blue, bottom two green. Hyperscale column: only "your software" blue, other three green. Edge column: only "your software" blue, other three green.
- **Legend (12px, y=262):** blue swatch "yours" at x=80, green swatch "the operator's" at x=180.
- **Annotation (bold 12px violet `#4a3aa7`, right-aligned near x=690, y=268):** "colo splits the stack; the other two rent you nearly all of it".

## One Tap, Three Data Centers

**Tags:** `worked example` (blue), `request path` (green)

- **The tap** — a customer in Denver taps "order"; the request must reach the app and come back
- **Edge first** — a Denver PoP a few km away terminates TLS and checks its cache: 8 ms round trip
- **Hyperscale next** — the app itself runs in a Virginia region: 42 ms there and back over fiber
- **Colo last** — stock levels live in the company's legacy system in a Chicago colo: 18 ms more
- **Hand-check** — 8 + 42 + 18 ms of network plus 24 ms of app work = 92 ms tap-to-confirmation

*Example (italic):* One order touches all three models before the "confirmed" screen appears: edge PoP, hyperscale region, colo cage — 92 ms end to end.

**Key point:** The three models are not rivals on one request — a single order often crosses all three, and each hop's latency is set by distance, not by software.

### Visualization (canvas `c2`, 720×300)

Left-to-right request flow diagram with labeled round-trip latencies, plus a stacked total-time bar underneath.

- **Title (bold 15px, `#1a5276`, top center):** "One Tap in Denver: 92 ms Across Edge, Hyperscale, and Colo".
- **Flow boxes (y=110, 46px tall, 8px radius, 12px `#2c3e50` two-line labels):** "phone / Denver" at x=25 width 110 fill `rgba(107,114,128,0.15)`; "edge PoP / Denver" at x=175 width 130 fill `rgba(0,131,0,0.14)` border 2px `#008300`; "hyperscale region / Virginia" at x=370 width 150 fill `rgba(42,120,214,0.18)` border 2px `#2a78d6`; "colo cage / Chicago" at x=575 width 120 fill `rgba(217,89,38,0.14)` border 2px `#d95926`.
- **Arrows:** 3px `#6b7280` double-headed arrows between consecutive boxes with bold 12px labels above: "8 ms" (green `#008300`), "42 ms" (blue `#2a78d6`), "18 ms" (orange `#d95926`); 12px `#444` note "+24 ms app work" centered under the hyperscale box at y=175.
- **Total bar (y=225, 26px tall, starting x=80, scale 6 px/ms):** four solid segments left to right — green 48px "edge 8", blue 252px "hyperscale 42", orange 108px "colo 18", violet `#4a3aa7` 144px "compute 24" — 11px white labels inside; bold 13px `#1a5276` "= 92 ms" at the bar's right end.
- **Caption (12px `#444`, bottom right):** "latencies illustrative; distance sets the floor".

## Rent or Build: the Four Deciding Axes

**Tags:** `rule of thumb` (blue), `cost` (orange), `latency` (green)

- **Capital vs rent** — colo means buying servers up front; cloud turns the same fleet into a monthly bill
- **Control vs convenience** — colo lets you touch your hardware; hyperscale hands you an API instead
- **Latency** — edge exists because latency is distance: no software trick moves Denver closer to Virginia
- **Scale** — spiky or unknown demand favors renting; a steady, huge, predictable load favors owning
- **The math** — at $220/server cloud vs $18k/mo + $95/server colo, colo wins past 144 steady servers
- **The return trip** — that's why most startups left colo for cloud, and a few steady giants move back

*Example (italic):* A video company with 200 always-busy encoding servers repatriates from cloud to colo and cuts the bill from $44k to $37k a month.

**Key point:** Cloud rent scales with zero commitment; colo has a fixed floor but a cheaper slope — the crossover only pays off when the workload is large and steady enough to sit past it.

### Visualization (canvas `c3`, 720×300)

Line chart of monthly cost vs steady fleet size: linear cloud-rent line vs colo line with a fixed floor and a gentler slope, crossing at 144 servers.

- **Title (bold 15px, `#1a5276`, top center):** "Steady Fleet Cost: Cloud Rent vs Colo, Crossover at 144 Servers".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = steady servers 0 to 300 with 12px `#444` tick labels every 100; y = $k per month 0 to 70, gridlines `#e5e9ef` at 20/40/60 with 12px `#444` labels.
- **Cloud line:** blue `#2a78d6` 3px line through servers `[0, 100, 200, 300]`, $k `[0, 22, 44, 66]` — pure rent, $220 per server.
- **Colo line:** orange `#d95926` 3px line through servers `[0, 100, 200, 300]`, $k `[18, 27.5, 37, 46.5]` — $18k fixed floor plus $95 per server.
- **Crossover marker:** vertical dashed `#6b7280` (dash 4/3) line at 144 servers with a 6px `#1a5276` dot at the crossing (~$31.7k); 12px `#6b7280` label "144" on the x-axis.
- **Annotations:** bold 12px blue `#2a78d6` near (x≈60 servers, upper left of colo floor) "small or spiky: rent wins, nothing to build"; bold 13px green `#008300` near (x≈230 servers, below the blue line) "past 144 steady servers, colo is cheaper".
- **Caption (12px `#444`, bottom right):** "prices illustrative; slopes are the point".

## Edge Is Not Automatically Faster

**Tags:** `common mistake` (red), `latency` (orange)

- **The hope** — "put it at the edge and everything gets fast" treats edge as a magic speed layer
- **What edge serves well** — cached images, TLS handshakes, tiny lookups that fit inside the PoP
- **What it can't** — a fresh order still needs the region's database; the PoP can only forward it
- **The backfire** — an edge hop that calls home pays both legs: 8 ms to the PoP plus 42 ms onward
- **The test** — before moving work to the edge, ask: can the whole answer live in 200 small sites?

*Example (italic):* A cached menu photo returns from the PoP in 8 ms; a fresh order routed through that same PoP to the region takes 50 ms — barely different from the 46 ms direct path.

**Common mistake:** Treating "edge" as a place you move an app to. Edge sites are small and stateless by design — they speed up only what they can fully answer, and add a hop to everything else.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart comparing three paths for the same user: fully answered at the edge, straight to the region, and an edge hop that forwards to the region anyway.

- **Title (bold 15px, `#1a5276`, top center):** "Edge Only Helps If the Answer Lives There".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 8 px/ms; 12px `#444` left-aligned row labels at x=20.
- **Rows (bar tops at y = 80, 140, 200, bars 26px tall, 12px bold width labels at bar ends):**
  - "answered at the edge (cached photo)": green `#008300` bar width 64 — "8 ms"
  - "straight to the region (no PoP)": blue `#2a78d6` bar width 368 — "46 ms"
  - "edge hop, then region anyway": orange `#d95926` bar width 400, drawn as a 64px green-tinted `rgba(0,131,0,0.35)` segment ("8") followed by a 336px solid orange segment ("42") — "50 ms" total label
- **Annotation (bold 13px magenta `#d55181`, centered near y=265):** "a PoP that has to call home adds a hop, not speed".
- **Caption (12px `#444`, bottom right):** "latencies illustrative, consistent with the 92 ms request above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and pixel widths above (no randomness); latencies (8 / 42 / 18 / 24 / 46 / 50 ms), prices ($220 cloud, $18k + $95 colo), and the 144-server crossover are invented and labeled illustrative — the crossover follows exactly from the stated prices (18000 / (220 − 95) = 144), and the c4 latencies reuse the c2 hop numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
