# Max-Flow / Min-Cut

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Max-Flow / Min-Cut

**Subtitle:** The most you can push through a network equals the capacity of its tightest cut — find one shipping plan and one cut that match, and both are proven optimal

## Five Roads, One Bottleneck Question

**Tags:** `core idea` (blue), `network flow` (green), `capacity` (orange)

- **The roastery** — a coffee roastery ships crates to a downtown cafe over five one-way roads
- **Capacities** — each road carries a fixed max per hour: 10, 8, 3, 6, and 9 crates
- **A flow** — a shipping plan that never exceeds any road's cap and loses nothing at junctions
- **Conservation** — whatever arrives at junction A or B each hour must leave it the same hour
- **The question** — what is the largest hourly rate the cafe can possibly receive?

*Example (italic):* Sending 10 down S→A fails — junction A's exits (6 to the cafe, 3 to B) can only pass 9 onward.

**Key point:** A flow is any plan obeying road capacities and junction conservation; max-flow asks for the best such plan, not a guess.

### Visualization (canvas `c1`, 720×300)

Node-and-arrow network diagram of the delivery network with capacity labels on every road.

- **Title (bold 15px, `#1a5276`, top center):** "The Delivery Network: Five Roads, Crates per Hour".
- **Nodes:** circles radius 22 at S(90,160), A(310,85), B(310,235), T(580,160); S and T filled ink `#1a5276` with white bold 14px letters; A and B white fill with 2.5px ink outline and ink bold 14px letters; 12px `#444` captions "roastery" left of S and "cafe" right of T.
- **Edges (3px lines, filled arrowheads stopping at node rims):** S→A, S→B, A→T, B→T in blue `#2a78d6`; cross road A→B (vertical, x=310) in violet `#4a3aa7`.
- **Capacity labels (bold 13px, matching edge color, at edge midpoints, offset 14px off the line):** "cap 10" (S→A), "cap 8" (S→B), "cap 3" (A→B), "cap 6" (A→T), "cap 9" (B→T).
- **Annotation (bold 13px magenta `#d55181`, centered at y=288):** "how many crates per hour can reach the cafe?".
- **Caption (12px `#444`, top right at y=40):** "capacities are crates/hour (illustrative)".

## Pushing Flow Path by Path

**Tags:** `worked example` (blue), `augmenting paths` (green)

- **Path 1** — send 6 along S→A→T; road A→T (cap 6) is now completely full
- **Path 2** — send 8 along S→B→T; road S→B (cap 8) is now completely full
- **Path 3** — send 1 along S→A→B→T, squeezing through the cross road A→B
- **Stuck** — every route from S to T now hits at least one full road, so we stop at 15
- **Final loads** — S→A 7/10, S→B 8/8, A→B 1/3, A→T 6/6, B→T 9/9

*Example (italic):* After the three pushes the cafe receives 6 + 8 + 1 = 15 crates per hour.

**Key point:** Push flow along one open path at a time until none remains; real solvers can also undo earlier flow (residual edges) when a greedy choice blocks.

### Visualization (canvas `c2`, 720×300)

Dual panel: the three augmenting-path pushes as horizontal bars (left) and the cumulative total climbing to 15 (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Three Pushes Reach the Maximum of 15".
- **Left panel (path pushes):** heading bold 12px `#444` "push along one open path at a time"; three horizontal bars 26px tall at y=90, 150, 210, starting x=115, length scaled over 230px with max 9; path labels 12px `#444` at x=15: "S→A→T", "S→B→T", "S→A→B→T"; fills blue `rgba(42,120,214,0.55)` (6), green `rgba(0,131,0,0.5)` (8), orange `rgba(217,89,38,0.55)` (1); bold 12px value labels "+6", "+8", "+1" at bar ends in matching solid colors.
- **Right panel (cumulative total):** axis origin x=400, baseline y=245, chart height 185, scale 0–16; three vertical bars width 55 centered at x=460, 545, 630 with heights for totals `[6, 14, 15]`; first two bars fill `rgba(42,120,214,0.45)`, final bar `rgba(0,131,0,0.5)`; bold 13px value labels "6", "14", "15" above bars; x labels 12px `#444` "after 1", "after 2", "after 3"; dashed magenta `#d55181` horizontal line at value 15 with bold 12px label "max flow = 15".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Cut That Proves You Are Done

**Tags:** `duality` (blue), `min cut` (orange), `proof of optimality` (green)

- **A cut** — any line separating the roastery side from the cafe side; add up the crossing caps
- **Four cuts** — this network has exactly four cuts, with capacities 18, 19, 17, and 15
- **Ceiling** — every crate must cross every cut, so no flow can beat any cut's capacity
- **Tightest one** — the cut through A→T and B→T carries 6 + 9 = 15, exactly our flow
- **Duality** — max-flow = min-cut always; when the two numbers meet, both are optimal

*Example (italic):* Our 15 crates/hour plan and the 15-capacity cut certify each other — no better plan can exist.

**Key point:** A flow that equals some cut's capacity is provably maximum, and that cut is provably minimum — the min cut is the network's true bottleneck.

### Visualization (canvas `c3`, 720×300)

Dual panel: the network with the minimum cut drawn through it (left) and all four cut capacities as bars (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Every Cut Is a Ceiling; the Smallest One Is Exact".
- **Left panel (network + cut):** nodes radius 16 at S(70,150), A(200,85), B(200,220), T(320,150), same node styling as c1 with bold 12px letters; edges 2.5px blue `#2a78d6` (A→B violet `#4a3aa7`); bold 12px capacity labels "10", "8", "3" in mute `#6b7280` and "6", "9" in magenta `#d55181` on the two cut edges; dashed magenta `#d55181` (dash 6/4) 2.5px vertical cut line from (255,45) to (255,275) crossing only A→T and B→T; bold 13px magenta label "cut = 6 + 9 = 15" centered below at y=292.
- **Right panel (cut capacities):** axis origin x=395, width 290, baseline y=240, chart height 175, scale 0–20; four vertical bars width 52 for cuts `[18, 19, 17, 15]` with x labels 11px `#444` "{S}", "{S,B}", "{S,A}", "{S,A,B}"; first three bars fill `rgba(107,114,128,0.35)`, last bar `rgba(0,131,0,0.5)`; bold 12px value labels above each bar; green `#008300` bold 13px annotation above the last bar: "smallest cut = 15 = max flow".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Upgrading the Wrong Road

**Tags:** `common mistake` (red), `where it's used` (blue)

- **The fallacy** — the smallest road (A→B, cap 3) looks like the bottleneck, but it is not
- **Wasted money** — doubling S→A from cap 10 to cap 20 leaves max flow stuck at 15
- **Real fix** — raising A→T from cap 6 to cap 9 lifts max flow from 15 to 18
- **Why** — extra capacity only helps on a road that crosses the minimum cut
- **Where it's used** — matching, image segmentation, and airline scheduling reduce to max-flow

*Example (italic):* A city widened its biggest highway (S→A) and delivery rates did not move at all.

**Common mistake:** Fixing the single smallest road, or the single busiest one — the bottleneck of a network is a cut (a set of roads acting together), never one road in isolation.

### Visualization (canvas `c4`, 720×300)

Three-bar comparison of max flow today versus after two possible road upgrades, with the useless upgrade called out.

- **Title (bold 15px, `#1a5276`, top center):** "Which Road Upgrade Actually Raises Max Flow?".
- **Data:** scenarios `["today", "widen S→A: 10→20", "widen A→T: 6→9"]` with max flows `[15, 15, 18]`.
- **Bars:** baseline y=235, chart height 170, scale 0–20; three vertical bars width 90 centered at x=170, 360, 550; fills blue `rgba(42,120,214,0.5)`, orange `rgba(217,89,38,0.5)`, green `rgba(0,131,0,0.5)`; bold 14px value labels "15", "15", "18" above bars in matching solid colors; scenario labels 12px `#444` below the baseline (two lines for the upgrade bars).
- **Reference line:** dashed `#bdc3c7` horizontal line at value 15 across the plot with 11px mute `#6b7280` label "today's max" at the left end.
- **Annotations (bold 12px):** orange `#d95926` above the middle bar "+10 capacity, +0 flow"; green `#008300` above the right bar "+3 on the min cut, +3 flow".
- **Takeaway (bold 13px magenta `#d55181`, centered at y=290):** "only widening a min-cut road moves the number".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Arrowheads:** draw filled triangles (length ~10px, half-width ~5px) at edge endpoints, oriented along the edge direction, stopping at the target node's rim.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
