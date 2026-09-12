# DBSCAN

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** DBSCAN

**Subtitle:** DBSCAN grows clusters by chaining nearby points, so it can trace a curved riverside path, keep a round plaza separate, and call the stray pins noise — no cluster count needed up front

## Check-In Pins Along the River

**Tags:** `core idea` (blue), `running example` (green), `density` (orange)

- **The map** — a coffee app drops a pin for every check-in: 13 along a curved riverside path, 9 on a plaza, 5 strays
- **The shape problem** — the riverside group is a long bent arc, not a round blob; the plaza is a tight ball
- **Your eye** — you see two groups instantly because pins in a group sit near each other, gap or no gap
- **The rule** — DBSCAN copies your eye: a cluster is any set of pins connected by short hops
- **Noise is a label** — the 5 strays join nothing; DBSCAN reports them as noise instead of forcing a group

*Example (italic):* The riverside arc bends around the plaza, yet no riverside pin is near a plaza pin — hopping pin to pin never crosses between them.

**Key point:** **DBSCAN** (density-based spatial clustering) says: a cluster is a region where points are packed densely, whatever its shape. It discovers how many clusters exist — you never pick k.

### Visualization (canvas `c1`, 720×300)

Single scatter plot of all 27 raw check-in pins (uncolored), with annotations pointing at the arc, the plaza, and the strays.

- **Title (bold 15px, `#1a5276`, top center):** "27 Check-In Pins: a Bent Path, a Round Plaza, 5 Strays".
- **Data (meters east, meters north), hardcoded:** RIVER (13 pins) `[[78,29],[75,36],[71,41],[67,45],[61,48],[55,51],[49,50],[43,49],[37,46],[31,44],[27,39],[24,33],[21,28]]`; PLAZA (9 pins) `[[46,14],[50,12],[54,14],[47,18],[51,16],[55,18],[49,20],[53,21],[45,17]]`; STRAY (5 pins) `[[8,52],[90,8],[12,10],[88,48],[68,8]]`.
- **Mapping:** px = 60 + mx×6.2 (x range 0–100 m), py = 250 − my×3.4 (y range 0–60 m); gray `#999` L axes at x=60 / y=250; x tick labels "0 m", "50 m", "100 m" and y labels "0", "30", "60 m" (12px `#6b7280`).
- **Points:** all pins ink `#1a5276` filled 5px dots at 0.7 alpha (deliberately one color — no labels yet).
- **Annotations (bold 13px):** green `#008300` "a curved path — not a blob" near px of (60,55) pointing at the arc top; blue `#2a78d6` "a tight plaza" near (62,12) beside the plaza; magenta `#d55181` "strays" near (10,57) with a thin line to pin (8,52).
- **Caption (12px `#444`, bottom right):** "illustrative check-in coordinates".

## Growing a Cluster With an 8-Meter Tape

**Tags:** `worked example` (green), `eps & minPts` (blue)

- **Two dials** — pick a radius eps = 8 m and a head-count minPts = 3 (a point counts itself)
- **Core point** — pin B has A and C within 8 m, so 3 pins inside its circle: B is a core point
- **Chaining** — cores B, C, D, E, F sit ~5 m apart, so their circles overlap into one cluster
- **Border point** — A has only B within 8 m (count 2), but sits inside core B's circle: A joins as border
- **Noise point** — N is 23 m from its nearest pin, inside nobody's circle: N stays noise

*Example (italic):* Seven pins about 5 m apart along a path — plus lone pin N — become one 7-pin cluster and one noise label with eps = 8, minPts = 3.

**Key point:** DBSCAN grows a cluster outward from any core point, swallowing every point reachable through overlapping eps-circles. Cores expand the cluster; borders ride along; noise touches no circle.

### Visualization (canvas `c2`, 720×300)

Zoomed scatter of the 8-pin worked example with three eps = 8 m circles drawn, colored by role (core / border / noise).

- **Title (bold 15px, `#1a5276`, top center):** "eps = 8 m, minPts = 3: Core, Border, Noise".
- **Data (meters), hardcoded:** chain A(10,20), B(15,21), C(20,20), D(25,21), E(30,20), F(35,21), G(40,20); lone pin N(60,32).
- **Mapping (equal scale so circles stay round):** px = 80 + mx×8, py = 260 − (my−12)×8; no axes — this is a zoomed map; letter labels A–G and N bold 12px `#2c3e50` above each pin.
- **Roles/colors:** cores B, C, D, E, F green `#008300` 6px filled dots; borders A, G yellow `#c98500` 6px filled dots; noise N drawn as magenta `#d55181` ✕ (2.5px stroke, 6px arms).
- **eps circles (radius 64px = 8 m):** around B in green stroke `rgba(0,131,0,0.5)` fill `rgba(0,131,0,0.07)`; around A in yellow stroke `rgba(201,133,0,0.55)` no fill; around N in magenta stroke `rgba(213,81,129,0.5)` dashed (dash 5/4), no fill.
- **Annotations (bold 12px, colored to match):** green "B: 3 pins inside → core" near B's circle top; yellow "A: only 2, but inside B's circle → border" below A; magenta "N: circle is empty → noise" right of N.
- **Chain hint:** thin green `rgba(0,131,0,0.35)` 2px line linking A→B→C→D→E→F→G under the dots.

## Why k-Means Draws the Wrong Map

**Tags:** `where it's used` (blue), `comparison` (orange), `failure mode` (red)

- **k-means assumption** — nearest-center membership carves space into round-ish cells; arcs get cut
- **Forced k = 2** — k-means slices the 13-pin riverside arc in half and glues each half to plaza pins
- **No noise concept** — k-means assigns all 5 strays to a cluster; DBSCAN reports them as noise
- **DBSCAN result** — chaining with eps = 8 recovers the 13-pin arc, the 9-pin plaza, and 5 noise pins
- **Where it's used** — GPS hotspots, crime and outbreak maps, and outlier flagging via the noise label

*Example (italic):* On the same 27 pins, k-means returns two mixed half-arc-plus-plaza blobs; DBSCAN returns the path, the plaza, and the strays.

**Key point:** Use DBSCAN when clusters may be elongated, curved, or ring-shaped, when stragglers should be flagged rather than absorbed, and when you cannot guess k. Use k-means when blobs really are round and every point must belong.

### Visualization (canvas `c3`, 720×300)

Dual-panel scatter: the same 27 pins clustered by k-means k = 2 (left, wrong) and by DBSCAN (right, correct), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same 27 Pins: k-Means (k = 2) vs DBSCAN (eps = 8, minPts = 3)".
- **Data:** the exact RIVER / PLAZA / STRAY arrays from canvas `c1`.
- **Left panel (k-means):** mapping px = 45 + mx×2.9, py = 245 − my×3.2; membership rule (illustrative nearest-centroid split): pins with mx < 48 blue `#2a78d6` 4.5px dots, pins with mx ≥ 48 orange `#d95926` 4.5px dots; centroid ✕ marks dark `#2c3e50` (3px stroke, 7px arms) at (30,32) and (64,32) labeled "✕ = centroid" 11px `#6b7280`; panel heading bold 13px `#2c3e50` "k-means: arc cut in half" at top left; magenta `#d55181` bold 12px annotation near the strays: "strays forced into clusters".
- **Right panel (DBSCAN):** mapping px = 395 + mx×2.9, same py; RIVER pins green `#008300`, PLAZA pins blue `#2a78d6` (4.5px dots); STRAY pins gray `#6b7280` ✕ marks (2px stroke, 5px arms); panel heading bold 13px `#2c3e50` "DBSCAN: path + plaza + noise"; green bold 12px annotation "the whole arc, one cluster" above the arc; gray 11px label "✕ = noise" near a stray.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## One eps Can't Fit Two Densities

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **One global tape** — DBSCAN uses a single eps everywhere; it defines "dense" once for the whole map
- **Two neighborhoods** — a downtown block has 8 shops ~3 m apart; a suburban strip has 5 lots 12 m apart
- **eps = 8** — downtown clusters fine, but every suburban lot is beyond 8 m of its neighbor: all noise
- **eps = 13** — the strip now chains, but the 12 m gap to downtown also chains: one merged blob
- **The fixes** — read eps off a k-distance elbow plot, or use HDBSCAN/OPTICS, which vary density

*Example (italic):* With 12 m spacing inside the strip and a 12 m gap to downtown, any eps big enough to hold the strip together also welds the two neighborhoods into one.

**Common mistake:** Blaming your tuning when no single eps works. If cluster densities differ, that is a limit of plain DBSCAN itself — switch to a variable-density method instead of turning the dial forever.

### Visualization (canvas `c4`, 720×300)

Dual-panel scatter: the same 13 shop pins under eps = 8 (left: sparse strip all noise) and eps = 13 (right: everything merges), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Dense Block + Sparse Strip: No Single eps Works".
- **Data (meters), hardcoded:** DENSE (8 downtown shops) `[[10,25],[13,24],[16,26],[12,28],[15,29],[18,24],[11,22],[17,28]]`; SPARSE (5 strip lots) `[[30,25],[42,26],[54,24],[66,26],[78,25]]`.
- **Left panel (eps = 8):** mapping px = 50 + mx×3.1, py = 165 − (my−25)×3.1; DENSE pins green `#008300` 5px dots; SPARSE pins magenta `#d55181` ✕ marks (2.5px stroke, 6px arms); panel heading bold 13px `#2c3e50` "eps = 8 m: strip = all noise" top left; magenta bold 12px annotation "12 m apart > 8 m — nobody chains" under the strip; thin gray dimension bracket between strip pins (30,25) and (42,26) labeled "12 m" 11px `#6b7280`.
- **Right panel (eps = 13):** mapping px = 400 + mx×3.1, same py; ALL 13 pins orange `#d95926` 5px dots joined by a thin orange `rgba(217,89,38,0.35)` 2px chain line from (18,24) through each strip pin; panel heading bold 13px `#2c3e50` "eps = 13 m: one merged blob"; orange bold 12px annotation "the 12 m gap chains too" over the link from (18,24) to (30,25); thin gray bracket between (18,24) and (30,25) labeled "12 m gap" 11px `#6b7280`.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Takeaway (bold 13px magenta `#d55181`, bottom center):** "eps that keeps the strip alive also bridges the gap — plain DBSCAN assumes one density".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All scatter data are the fixed literal arrays above — no `Math.random()`, no seeded jitter; reuse the same RIVER / PLAZA / STRAY arrays in canvases `c1` and `c3` so the panels depict identical data.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
