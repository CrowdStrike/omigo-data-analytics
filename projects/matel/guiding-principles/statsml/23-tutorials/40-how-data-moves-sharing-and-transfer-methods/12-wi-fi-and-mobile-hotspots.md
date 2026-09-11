# Wi-Fi &amp; Mobile Hotspots

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Wi-Fi &amp; Mobile Hotspots

**Subtitle:** The local radio network — one router serving a home, a phone becoming the router, and devices talking directly without one

## One Router on the Bookshelf: Every Conversation Passes Through It

**Tags:** `core idea` (blue), `802.11` (orange), `radio` (green)

- **The running example** — Alice sends a 5 GB video to Bob's laptop three feet away, in the same room
- **Wi-Fi = ethernet, unwired** — the same job as the office cable, with radio waves carrying the bits
- **802.11** — the standard's family name; the letters after it (b, g, n, ac, ax) name generations
- **The hub** — every device talks to the router; the router relays everything, like a mail room
- **Two hops** — Alice's file goes up to the router, then back down to Bob — never laptop to laptop

*Example (italic):* Alice and Bob share one table, yet the video travels via the router on the bookshelf — twice over the air.

**Key point:** A Wi-Fi network is hub-and-spoke: one router in the middle, and even neighbors at the same desk speak through it.

### Visualization (canvas `c1`, 720×300)

Room schematic: two laptops at the bottom, the router at the top center; the file's path is two radio arcs through the router while the direct laptop-to-laptop line is dashed and unused.

- **Title (bold 15px, `#1a5276`, top center):** "Same Room, Two Radio Hops — the Router Relays Everything".
- **Router (rounded box 120×46 at x=300, y=50):** violet fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border; line 1 bold 13px `#4a3aa7` "router", line 2 11px `#6b7280` "on the bookshelf". Two small concentric arc marks (1.5px `#4a3aa7`) above the box suggesting radio.
- **Alice's laptop (rounded box 155×52 at x=60, y=205):** blue fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border; line 1 bold 13px `#2a78d6` "Alice's laptop", line 2 12px `#2c3e50` "sending 5 GB video".
- **Bob's laptop (rounded box 155×52 at x=505, y=205):** aqua fill `rgba(25,158,112,0.12)`, 2px `#199e70` border; line 1 bold 13px `#199e70` "Bob's laptop", line 2 12px `#2c3e50` "receiving".
- **Hop 1 arc:** quadratic curve from Alice's top edge (137, 205) to router's left edge (300, 85), control point (170, 105), 2.5px `#2a78d6`, arrowhead at the router end; label bold 12px `#2a78d6` "hop 1: up" near (165, 135).
- **Hop 2 arc:** quadratic curve from router's right edge (420, 85) to Bob's top edge (582, 205), control point (550, 105), 2.5px `#199e70`, arrowhead at Bob's end; label bold 12px `#199e70` "hop 2: down" near (555, 135).
- **Unused direct path:** dashed 1.5px `#6b7280` straight line from (215, 240) to (505, 240); label 12px `#6b7280` centered at (360, 230): "no direct path — 3 feet apart".
- **Annotation (bold 13px orange `#d95926`, centered near y=285):** "the file crosses the air twice — even across one table".

## Copying the 5 GB Video, Generation by Generation

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The formula** — time = size ÷ speed; Wi-Fi quotes megabits, so divide the Mbps by 8 first
- **802.11b (1999)** — 11 Mbps ≈ 1.375 MB/s; 5,000 MB ÷ 1.375 ≈ 3,636 s ≈ 1 hour
- **802.11g (2003)** — 54 Mbps = 6.75 MB/s; 5,000 ÷ 6.75 ≈ 741 s ≈ 12 min
- **802.11n (2009)** — up to 600 Mbps = 75 MB/s; 5,000 ÷ 75 ≈ 67 s
- **802.11ac/ax** — gigabit-class: 1,300 Mbps → ≈ 31 s; 2,400 Mbps → ≈ 17 s
- **Label vs life** — these are the box's ideal rates; real copies land near half (last section)

*Example (italic):* The video that once ate Alice's whole lunch hour on 802.11b arrives before Bob unlocks his screen on 802.11ax.

**Key point:** The whole ladder is one recipe — Mbps ÷ 8 = MB/s, then file size ÷ MB/s — redo it for any file and any generation.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of time to send 5 GB (5,000 MB) at each Wi-Fi generation's ideal rate, on a log-scale seconds axis so the 200× spread stays visible.

- **Title (bold 15px, `#1a5276`, top center):** "Sending a 5 GB Video at Each Wi-Fi Generation".
- **Axis:** log10(seconds) 0–3.7 mapped to px x=215 (1 s) through x=670; baseline 1px `#999` at y=262; vertical gridlines `#e5e9ef` with 12px `#444` tick labels at 1 s / 10 s / 100 s / 1,000 s; axis caption "seconds — log scale" 12px `#444` centered under the baseline.
- **Rows (bars 26px tall from x=215, left-aligned 12px `#444` labels at x=20; tinted fill + 2px solid border + bold 13px value label in the bar color just right of the bar end, except the first row's label drawn inside the bar's right end in white):**
  - y=58: "802.11b — 1.375 MB/s", blue `rgba(42,120,214,0.30)` / `#2a78d6`, length log10(3636)=3.561, label "3,636 s ≈ 1 hour" (inside bar, white)
  - y=100: "802.11g — 6.75 MB/s", aqua `rgba(25,158,112,0.30)` / `#199e70`, length log10(741)=2.870, label "741 s ≈ 12 min"
  - y=142: "802.11n — 75 MB/s", violet `rgba(74,58,167,0.30)` / `#4a3aa7`, length log10(67)=1.826, label "67 s"
  - y=184: "802.11ac — 162.5 MB/s", orange `rgba(217,89,38,0.30)` / `#d95926`, length log10(31)=1.491, label "31 s"
  - y=226: "802.11ax — 300 MB/s", green `rgba(0,131,0,0.30)` / `#008300`, length log10(17)=1.230, label "17 s"
- **Annotation (bold 13px orange `#d95926`, near x=460, y=44):** "1999 → today: an hour shrinks to 17 seconds".
- **Caption (12px `#6b7280`, bottom right near y=294):** "time = 5,000 MB ÷ (Mbps ÷ 8) — ideal label rates; real copies run near half".

## The Phone Becomes the Router: Tethering &amp; Wi-Fi Direct

**Tags:** `tethering` (orange), `wifi direct` (blue), `where it's used` (green)

- **Tethering** — turn on "hotspot" and the phone becomes the router; the laptop joins it like any Wi-Fi
- **The uplink swap** — a home router's uplink is the broadband line; a hotspot's is the cell network
- **The bottleneck** — laptop-to-phone ~300 Mbps; the cell uplink ~30 Mbps sets the pace (illustrative)
- **Wi-Fi Direct** — devices pair radio-to-radio, no router; the machinery under phone file-sharing
- **Field work** — a 5 GB upload over a hotspot takes ~22 min; the cell hop, not Wi-Fi, sets it

*Example (italic):* Alice's dashboard "on Wi-Fi" crawls on the train — the Wi-Fi hop is gigabit-class, but the tower link behind it is not.

**Key point:** A hotspot is a real Wi-Fi network with a cellular back door — the chain moves at the speed of its slowest link.

### Visualization (canvas `c3`, 720×300)

Two-panel schematic split by a dashed vertical divider at x=365: tethering chain on the left, Wi-Fi Direct pairing on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways Around the Home Router".
- **Divider:** dashed 1px `#bdc3c7` vertical line from y=38 to y=262 at x=365.
- **Left panel header (bold 12px `#1a5276`, centered x=182, y=52):** "Tethering: the phone is the router".
- **Left vertical chain (rounded boxes, centered column at x≈160):**
  - tower box 80×36 at x=120, y=62: violet fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border, bold 12px `#4a3aa7` "cell tower"
  - phone box 100×40 at x=110, y=130: green fill `rgba(0,131,0,0.10)`, 2px `#008300` border, bold 12px `#008300` "phone (hotspot)"
  - laptop box 120×40 at x=100, y=204: blue fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border, bold 12px `#2a78d6` "laptop"
  - link laptop→phone: 2.5px `#008300` vertical line x=160 from y=204 to y=170; label bold 12px `#008300` left-aligned at (175, 192): "Wi-Fi ~300 Mbps"
  - link phone→tower: 2.5px `#d95926` vertical line x=160 from y=130 to y=98; label bold 12px `#d95926` left-aligned at (175, 118): "cell uplink ~30 Mbps"
  - panel caption 11px `#6b7280` centered x=182, y=268: "the 30 Mbps hop sets the pace"
- **Right panel header (bold 12px `#1a5276`, centered x=540, y=52):** "Wi-Fi Direct: no router at all".
- **Right panel:**
  - Alice's phone box 90×44 at x=410, y=150: blue fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border, bold 12px `#2a78d6` "Alice's phone"
  - Bob's phone box 90×44 at x=580, y=150: aqua fill `rgba(25,158,112,0.12)`, 2px `#199e70` border, bold 12px `#199e70` "Bob's phone"
  - direct link: 3px `#008300` quadratic arc from (500, 160) to (580, 160), control (540, 112); label bold 12px `#008300` centered (540, 104): "direct radio link"
  - skipped router: rounded box 100×36 at x=490, y=222, dashed 1.5px `#6b7280` border, no fill, 11px `#6b7280` centered label "router (skipped)"; dashed 1px `#6b7280` lines from (445, 194) to (505, 224) and (635, 194) to (575, 224)
- **Annotation (bold 13px orange `#d95926`, centered w/2, y=290):** "hotspot: the slowest link wins — Wi-Fi Direct: skip the middleman".

## The Confusion: The Number on the Box Is Not Your Speed

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The box number** — the quoted rate assumes perfect air: close range, no walls, no neighbors
- **Distance** — radio fades with every foot; the far corner sees a fraction of the same-room rate
- **Walls** — each wall or floor roughly halves what gets through (illustrative rule of thumb)
- **Shared air** — devices on a channel take turns; roommates and neighbors split the airtime
- **Rule of thumb** — plan on about half the label in the same room, then keep halving per wall

*Example (italic):* Bob's speed test reads 650 Mbps beside the router and 70 Mbps in the far corner — same router, same laptop, same evening.

**Key point:** Wi-Fi speed is a place, not a number — measure where you actually sit, not where the router lives.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of measured throughput at five spots served by one 802.11ac router quoted at 1,300 Mbps, halving step by step.

- **Title (bold 15px, `#1a5276`, top center):** "One Router, Five Places to Sit (illustrative Mbps)".
- **Axis:** Mbps 0–1,400 mapped to px x=200 (0) through x=680 (1,400); baseline 1px `#999` at y=262; vertical gridlines `#e5e9ef` with 12px `#444` tick labels at 0 / 500 / 1,000; axis caption "measured throughput, Mbps" 12px `#444` centered under the baseline.
- **Rows (bars 28px tall from x=200, left-aligned 12px `#444` labels at x=20; tinted fill + 2px solid border + bold 13px value label in the bar color just right of the bar end):**
  - y=58: "quoted on the box", blue `rgba(42,120,214,0.30)` / `#2a78d6`, value 1,300, label "1,300"
  - y=100: "same room", green `rgba(0,131,0,0.30)` / `#008300`, value 650, label "650"
  - y=142: "one wall away", aqua `rgba(25,158,112,0.30)` / `#199e70`, value 330, label "330"
  - y=184: "two walls away", orange `rgba(217,89,38,0.30)` / `#d95926`, value 160, label "160"
  - y=226: "far corner", magenta `rgba(213,81,129,0.30)` / `#d55181`, value 70, label "70"
- **Annotation (bold 13px orange `#d95926`, near x=480, y=130):** "each wall or step of distance roughly halves it".
- **Caption (12px `#6b7280`, bottom right near y=294):** "illustrative — measured where the laptop sits, not where the router lives".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:").
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` appears only in the key-point border CSS, not in any chart.
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Documented facts: 802.11 generation label rates 11 / 54 / 600 / 1,300 / 2,400 Mbps; 8 bits per byte; tethering and Wi-Fi Direct are documented standards. Invented/typical figures (hotspot link speeds ~300 and ~30 Mbps, the per-wall halving, the five measured spots 650/330/160/70) are labeled illustrative in each chart's title, caption, or bullet. Arithmetic that must stay consistent between text and charts: 11 ÷ 8 = 1.375; 5,000 ÷ 1.375 ≈ 3,636 s ≈ 1 hour; 54 ÷ 8 = 6.75; 5,000 ÷ 6.75 ≈ 741 s ≈ 12 min; 600 ÷ 8 = 75; 5,000 ÷ 75 ≈ 67 s; 1,300 ÷ 8 = 162.5; 5,000 ÷ 162.5 ≈ 31 s; 2,400 ÷ 8 = 300; 5,000 ÷ 300 ≈ 17 s; 30 ÷ 8 = 3.75; 5,000 ÷ 3.75 ≈ 1,333 s ≈ 22 min.
- This page has no links.
