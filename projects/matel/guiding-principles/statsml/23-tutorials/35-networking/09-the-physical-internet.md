# The Physical Internet

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Physical Internet

**Subtitle:** Every tap on your phone travels real glass — through your city, along a backbone, and across the seafloor — and the speed of light in fiber writes the bill

## One Tap, Sixteen Thousand Kilometers

**Tags:** `core idea` (blue), `submarine cables` (green), `backbone` (orange)

- **The tap** — a customer in Sydney taps "order" in a coffee-shop app whose server sits in Virginia
- **The last mile** — the request rides wifi, then ~15 km of city fiber to the ISP's exchange
- **The landing** — another ~30 km brings it to a beach building where a submarine cable comes ashore
- **The ocean** — one seafloor fiber carries it ~12,000 km across the Pacific to a US landing station
- **The backbone** — long-haul terrestrial fiber hauls it the last ~4,000 km inland to the data center

*Example (italic):* One coffee order covers ≈16,000 km of physical glass — and about 12,000 km of that, roughly 75%, is a single cable lying on the seafloor.

**Key point:** The internet is not abstract: it is a mesh of physical fiber — city loops, terrestrial backbones, and submarine cables — and every request follows one concrete path through it.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the order's path (five boxes with per-leg distances) above a proportional strip showing each leg's share of the total distance.

- **Title (bold 15px, `#1a5276`, top center):** "One Tap in Sydney: the Physical Path to a Virginia Server".
- **Boxes (y=110, 40px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, 12px `#2c3e50` text):** "phone" at x=20 (width 80), "ISP exchange" at x=140 (width 110), "landing station" at x=290 (width 120), "US landing" at x=450 (width 100), "Virginia DC" at x=590 (width 110).
- **Arrows:** 2px `#6b7280` arrows between boxes with 12px `#444` distance labels above: "15 km", "30 km", "12,000 km (submarine)", "4,000 km (backbone)"; the submarine arrow is drawn 3px in blue `#2a78d6`.
- **Proportion strip (x=60, y=225, width 600, height 18):** segments left to right — mute `#6b7280` sliver width 6 (the two local legs, 45 km), blue `#2a78d6` width 449 (submarine 12,000 km), aqua `#199e70` width 145 (backbone 4,000 km); 11px labels under each segment.
- **Annotation (bold 13px blue `#2a78d6`, above the strip at y=205):** "75% of the trip is one cable on the seafloor".
- **Caption (12px `#444`, bottom right):** "leg distances illustrative of a typical Sydney→Virginia route".

## Racing Light Across the Pacific

**Tags:** `worked example` (blue), `latency math` (green)

- **The speed** — light in fiber moves at about 200,000 km/s, roughly 2/3 of its speed in vacuum
- **One way** — 16,000 km ÷ 200,000 km/s = 0.08 s, so the request needs 80 ms just to arrive
- **Round trip** — the reply crosses back, so the physics floor is 2 × 80 = 160 ms
- **The vacuum floor** — even at full c (300,000 km/s), 32,000 km round trip would take ~107 ms
- **The real ping** — measured pings run ~210 ms: route detours and equipment add ~50 ms on top

*Example (italic):* Hand-check the ocean leg alone: 12,000 km ÷ 200,000 km/s = 60 ms each way — the Pacific crossing is 120 of the 160 ms floor.

**Key point:** No server upgrade can beat 160 ms round trip from Sydney to Virginia over fiber — distance divided by the speed of light in glass is a hard floor.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart comparing three round-trip times for the same 16,000 km path: vacuum-light floor, fiber floor, and a measured ping.

- **Title (bold 15px, `#1a5276`, top center):** "Sydney→Virginia Round Trip: Physics Floor vs Measured Ping".
- **Axis:** vertical 2px `#999` baseline at x=170, bars extend right, scale 2.4 px per ms; left-aligned 12px `#444` row labels at x=20.
- **Rows (14px-tall bars at y = 90, 150, 210):**
  - "light in vacuum (32,000 km)": aqua `#199e70` bar width 257 (107 ms)
  - "light in fiber — the floor": blue `#2a78d6` bar width 384 (160 ms)
  - "measured ping": orange `#d95926` bar width 504 (210 ms)
- **Bar labels:** 12px `#444` at each bar end: "107 ms", "160 ms", "210 ms".
- **Marker:** vertical dashed `#6b7280` (dash 4/3) line at x=554 (the 160 ms fiber floor), 11px `#6b7280` label "fiber floor" at its top.
- **Annotation (bold 13px blue `#2a78d6`, near y=250):** "the ocean costs 160 ms before any server does any work".
- **Caption (12px `#444`, bottom right):** "measured ping illustrative; floors computed from distance ÷ speed".

## Two Humps in the Latency Histogram

**Tags:** `where it's used` (blue), `peering` (green), `edge caches` (orange)

- **Peering** — networks hand traffic to each other at internet exchange points (IXPs), often for free
- **Staying local** — because ISPs peer at a Sydney IXP, Sydney-to-Sydney traffic never leaves the city
- **Edge caches** — the app copies its menu images to a Sydney cache, cutting the ocean out of the trip
- **The split** — cached requests answer in ~30 ms; payments must still reach Virginia in ~215 ms
- **The data view** — a response-time histogram grows two humps, one per side of the Pacific

*Example (italic):* An analyst sees p50 latency of 32 ms but p90 of 214 ms and suspects a bug — it is just the payment calls crossing the ocean.

**Key point:** Geography shows up in your data: multimodal latency, timeout tuning, and regional A/B differences often trace back to which requests crossed a submarine cable.

### Visualization (canvas `c3`, 720×300)

Histogram of 400 illustrative response times for the coffee app, bimodal: an edge-cache hump near 30 ms and a cross-ocean hump near 215 ms.

- **Title (bold 15px, `#1a5276`, top center):** "One App, Two Humps: Response Times Split by Geography".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = response time 0 to 300 ms, 12px `#444` tick labels every 50 ms; y = requests 0 to 120, gridlines `#e5e9ef` at 30/60/90.
- **Bars:** 12 bins of 25 ms (width 50px each, 46px drawn), counts `[5, 120, 90, 12, 3, 2, 2, 3, 85, 40, 30, 8]`, fill `rgba(42,120,214,0.35)`, 1.5px `#2a78d6` border.
- **Hump labels:** bold 12px green `#008300` "served from Sydney edge (~30 ms)" above the left hump; bold 12px magenta `#d55181` "crossed to Virginia (~215 ms)" above the right hump.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=60):** "two humps = one ocean, not a slow server".
- **Caption (12px `#444`, bottom right):** "request counts illustrative".

## The Cloud Is Not in the Sky

**Tags:** `common mistake` (red), `satellite myth` (orange)

- **The picture** — people imagine data beaming up to a satellite and down into "the cloud"
- **The reality** — the vast majority of intercontinental traffic travels submarine fiber, not satellite
- **The scale** — roughly 550 active submarine cables span about 1.4 million km of seafloor
- **The fragility** — cables suffer on the order of 150 faults a year, mostly ship anchors and fishing gear
- **The repair** — a specialized ship grapples the cable off the seabed and splices it by hand

*Example (italic):* When an anchor drags through a cable, a country's traffic reroutes onto neighboring cables and everyone's pings jump — nothing in "the cloud" changed.

**Common mistake:** Treating the internet as wireless and placeless. It is ships, beach landings, and glass under the sea — physical routes with owners, chokepoints, and repair times.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the imagined path (phone → satellite → cloud) vs the real path (phone → city fiber → seafloor cable → data center).

- **Title (bold 15px, `#1a5276`, top center):** "The Mental Picture vs the Actual Route".
- **Row 1 (y=95), label 12px `#444` at x=20:** "imagined"; blue `#2a78d6` rounded box at x=150 labeled "phone" (12px), 2px arrow up-and-over to a mute `#6b7280` box at x=330 labeled "satellite", arrow to a mute box at x=520 labeled "the cloud", with bold 12px red `#e74c3c` "✗ carries under 1% of intercontinental traffic" beneath the row.
- **Row 2 (y=205), label:** "reality"; blue box "phone" at x=150, arrow to a green `#008300` box at x=320 labeled "city + backbone fiber", arrow to a green box at x=520 labeled "seafloor cable → DC", with bold 12px green "✓ ~99% goes by fiber".
- **Box style:** 110–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(107,114,128,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "≈550 cables, ~1.4 million km — enough glass to circle Earth 35 times".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the route legs (15 / 30 / 12,000 / 4,000 km), the 210 ms measured ping, and the histogram counts `[5, 120, 90, 12, 3, 2, 2, 3, 85, 40, 30, 8]` are invented and labeled illustrative; the physics numbers (fiber ≈ 200,000 km/s ≈ 2/3 c, 160 ms fiber floor and 107 ms vacuum floor for a 16,000 km path) are computed, and the submarine-cable facts (≈550 active cables, ~1.4 million km, ~150 faults/yr, ~99% of intercontinental traffic) are documented approximations.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
