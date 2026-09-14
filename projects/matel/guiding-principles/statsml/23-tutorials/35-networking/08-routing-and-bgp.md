# Routing & BGP

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Routing & BGP

**Subtitle:** The internet has no central map — each network just tells its neighbors what it can reach, and packets find their way one hop at a time

## A Photo's Trip Across Three Networks

**Tags:** `core idea` (blue), `autonomous systems` (green), `no central map` (orange)

- **The upload** — a laptop in Denver sends a photo to a hosting server across the internet
- **The neighborhoods** — the internet is 70,000+ independent networks called autonomous systems (ASes)
- **No boss** — no single authority knows the whole map; each AS only talks to its direct neighbors
- **The gossip** — via BGP, each AS announces "I can reach these addresses" and passes it along
- **Path vector** — every announcement carries the list of ASes it crossed, so loops are spotted
- **The trip** — the photo crosses home ISP AS 100, then exchange AS 400, then hosting AS 700

*Example (italic):* The photo never sees a master map — AS 100 heard from AS 400, which heard from AS 700, that the server's addresses live there.

**Key point:** BGP (Border Gateway Protocol) is how independent networks share reachability: each announcement lists the AS path it traveled, and packets follow those learned paths hop by hop.

### Visualization (canvas `c1`, 720×300)

Node-and-edge diagram of six points — laptop, four ASes, server — with the announced path highlighted in blue and unused links in gray.

- **Title (bold 15px, `#1a5276`, top center):** "No Central Map: the Photo Follows Announcements, AS by AS".
- **Nodes (rounded boxes 110×44, 8px radius, 12px `#2c3e50` labels, centers at):** laptop (90, 160) fill `rgba(107,114,128,0.12)` labeled "laptop (Denver)"; "AS 100 home ISP" (240, 160) fill `rgba(42,120,214,0.15)`; "AS 400 exchange" (400, 95) fill `rgba(42,120,214,0.15)`; "AS 200 carrier" (400, 225) fill `rgba(107,114,128,0.10)`; "AS 700 hosting" (555, 160) fill `rgba(0,131,0,0.12)`; server (665, 160) fill `rgba(0,131,0,0.12)` labeled "server".
- **Chosen path edges (3px blue `#2a78d6`, arrowheads):** laptop→AS 100, AS 100→AS 400, AS 400→AS 700, AS 700→server.
- **Unused edges (2px `#e5e9ef`):** AS 100→AS 200, AS 200→AS 700.
- **Edge labels (11px `#6b7280`):** "announce: 203.0.113.0/24" along AS 700→AS 400 and AS 400→AS 100 back-direction, drawn as small dashed `#199e70` arrows above the path edges.
- **Annotation (bold 13px blue `#2a78d6`, centered near y=52):** "each AS only knows what its neighbors announced".
- **Caption (12px `#444`, bottom right):** "AS numbers and topology illustrative".

## The Router's Choice: Longest Prefix Wins

**Tags:** `worked example` (blue), `longest-prefix match` (green)

- **The packet** — a packet arrives at AS 100's router addressed to 203.0.113.7
- **Route 1** — 203.0.0.0/16 via AS path `200 700`: matches the first 16 bits, path is 2 ASes
- **Route 2** — 203.0.113.0/24 via AS path `400 500 700`: matches 24 bits, path is 3 ASes
- **Route 3** — 0.0.0.0/0 (the default route) via AS path `200`: matches everything, 0 specific bits
- **The rule** — the most specific prefix wins first; the /24 beats the /16 despite its longer AS path
- **Hand-check** — 203.0.113.7 starts with 203.0.113, so all 24 bits of route 2's prefix match

*Example (italic):* The router forwards 203.0.113.7 toward AS 400 on the 3-AS path, because the /24 route matches 24 leading bits versus the /16's 16.

**Key point:** Forwarding uses longest-prefix match — prefix length is compared before anything else; AS-path length only breaks ties between routes to the same prefix.

### Visualization (canvas `c2`, 720×300)

Routing-table diagram: the destination address at top, three candidate route rows below, with matched bits highlighted and the winning row marked.

- **Title (bold 15px, `#1a5276`, top center):** "Destination 203.0.113.7: Three Routes Match, the /24 Wins".
- **Destination box:** rounded box centered at (360, 65), 240×34, fill `rgba(26,82,118,0.10)`, bold 13px `#1a5276` text "packet to 203.0.113.7".
- **Route rows (y = 125, 175, 225; columns: prefix at x=70, AS path at x=300, matched bits at x=470, verdict at x=590; 12px `#2c3e50` text):**
  - "203.0.0.0/16" | "path: 200 700" | "16 bits match" | 12px `#6b7280` "runner-up"
  - "203.0.113.0/24" | "path: 400 500 700" | "24 bits match" | bold 12px green `#008300` "WINNER"
  - "0.0.0.0/0" | "path: 200" | "0 bits match" | 12px `#6b7280` "last resort"
- **Winner highlight:** row 2 gets a full-width rounded band (x=50 to 690, 36px tall) filled `rgba(0,131,0,0.10)` with a 2px `#008300` border.
- **Match bars:** small horizontal bars at x=470 under each "bits match" label, width = bits × 5 px (80 / 120 / 0), 8px tall, fill blue `#2a78d6` for rows 1–2, `#e5e9ef` outline only for row 3.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "24 matched bits beat a shorter AS path — specificity first, path length second".
- **Caption (12px `#444`, bottom right):** "prefixes from documentation ranges; paths illustrative".

## When the Map Changes, Your Metrics Move

**Tags:** `where it's used` (blue), `latency` (green), `data quality` (orange)

- **Routes shift** — BGP announcements are withdrawn and re-announced constantly, all day, worldwide
- **The symptom** — a regional latency dashboard spikes with zero code deploys and zero server load
- **The office** — median ping from the Denver office to the app server sits near 120 ms for days
- **The reroute** — on day 6 the usual path is withdrawn and traffic detours over a longer AS path
- **The jump** — median latency rises from 120 ms to about 190 ms for three days, then recovers
- **The lesson** — geo-latency anomalies in your data are often the network's map changing, not your app

*Example (italic):* Days 6–8 read 191, 188, and 190 ms instead of the usual ~120 ms — the "regression" was a route change, and it fixed itself on day 9.

**Key point:** Anyone who monitors latency, availability, or regional user metrics inherits BGP's churn — rule out a route change before blaming the application.

### Visualization (canvas `c3`, 720×300)

Line chart of daily median latency over ten days with a three-day plateau while traffic detours over a longer path.

- **Title (bold 15px, `#1a5276`, top center):** "Median Ping, Denver Office → App Server: a Route Change, Not a Bug".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = day 1 to 10, 12px `#444` tick labels each day; y = 0 to 250 ms, gridlines `#e5e9ef` at 50/100/150/200 with 12px `#444` labels.
- **Latency line:** blue `#2a78d6` 3px line with 4px dots through days `[1,2,3,4,5,6,7,8,9,10]`, latency ms `[118, 121, 119, 122, 120, 191, 188, 190, 124, 122]`.
- **Detour band:** vertical band from day 5.5 to day 8.5, fill `rgba(217,89,38,0.10)`, dashed `#d95926` (dash 4/3) edges, 12px `#d95926` label "detour path (5 ASes)" at its top.
- **Baseline label:** 12px `#6b7280` "usual path (3 ASes)" near day 2.5 above the 120 ms level.
- **Annotation (bold 13px orange `#d95926`, near day 7, y=75):** "+70 ms with no deploy — the map changed".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## BGP Believes Whatever It's Told

**Tags:** `common mistake` (red), `trust` (orange), `route hijack` (blue)

- **The assumption** — people assume announced routes are verified and that BGP picks the fastest path
- **The truth** — classic BGP has no built-in proof of ownership; routers accept announcements on trust
- **Not fastest** — route choice follows prefix length and policy, never a live latency measurement
- **The hijack** — a rogue AS announces someone else's addresses, often as a more specific prefix
- **Why it works** — a bogus /25 beats the owner's /24 by longest-prefix match, where filters allow it
- **The patch** — RPKI lets networks cryptographically validate origins, but adoption is still partial

*Example (italic):* The owner announces 203.0.113.0/24; a rogue AS announces 203.0.113.0/25, and networks that accept such small prefixes silently follow the more specific lie.

**Common mistake:** Treating a BGP route as verified or optimal. It is neither — it is the most specific announcement anyone claimed, and hijacks exploit exactly that trust.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: normal announcement steering traffic to the owner, versus a more-specific rogue announcement stealing it.

- **Title (bold 15px, `#1a5276`, top center):** "The More Specific Lie Wins: Anatomy of a Route Hijack".
- **Row 1 (y=100), label 12px `#444` at x=20:** "normal"; green `#008300` rounded box at x=150 labeled "owner announces 203.0.113.0/24" (12px), 3px green arrow to a blue `#2a78d6` box at x=420 labeled "routers install /24", 3px arrow to bold 12px green "✓ traffic reaches owner" at x=600.
- **Row 2 (y=210), label:** "hijack"; red `#e74c3c` rounded box at x=150 labeled "rogue AS announces 203.0.113.0/25", 3px red arrow to a blue box at x=420 labeled "/25 beats /24 — longest prefix", 3px arrow to bold 12px red "✗ traffic diverted" at x=600.
- **Box style:** 180–200px wide, 44px tall, 8px radius, fills `rgba(0,131,0,0.12)` / `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "no ownership check — the routers did exactly what BGP told them to".
- **Caption (12px `#444`, bottom right):** "scenario schematic; many networks filter prefixes longer than /24".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); AS numbers, topology, and the latency series `[118, 121, 119, 122, 120, 191, 188, 190, 124, 122]` are invented and labeled illustrative; prefixes use the 203.0.113.0/24 documentation range; longest-prefix match, path-vector announcements, trust-based announcements/hijack risk, and RPKI are documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
