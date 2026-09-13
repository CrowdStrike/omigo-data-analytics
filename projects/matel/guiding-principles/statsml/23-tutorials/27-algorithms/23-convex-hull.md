# Convex Hull

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Convex Hull

**Subtitle:** Stretch a rubber band around a set of pins and let go — the polygon it snaps into is the convex hull, the tightest fence that still encloses every point

## A Rubber Band Around Ten Sprinklers

**Tags:** `core idea` (blue), `rubber band` (green), `tightest fence` (orange)

- **The lawn** — a gardener maps 10 sprinkler heads on a 10 m × 10 m lawn and wants one fence around them all
- **The trick** — push a pin into each sprinkler spot, stretch a rubber band around the whole cluster, let go
- **The snap** — the band snaps tight against the outermost pins and forms a polygon: the convex hull
- **Who touches** — only 5 of the 10 pins touch the band; the other 5 sit inside and never matter
- **Convex** — the band only ever turns one way (always left walking around); it has no dents pointing inward

*Example (italic):* The band snaps onto pins (6,1), (9,4), (7,9), (2,8), (1,2) and ignores the five inner sprinklers — about 25.9 m of fence encloses all ten.

**Key point:** The convex hull is the shape a rubber band takes around your points: the smallest convex polygon containing all of them, built only from the outermost points.

### Visualization (canvas `c1`, 720×300)

Single-panel map of the lawn: all ten sprinkler pins as dots, with the rubber-band hull drawn as a closed polygon through the five outer pins and a light fill inside.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Sprinklers, One Rubber Band — 5 Pins Touch, 5 Sit Inside".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = 0 to 10 m with 12px `#444` tick labels "0 m", "2 m", ..., "10 m" every 2; y = 0 to 10 m, tick labels "0"–"10" every 2 (12px `#444`); light `#e5e9ef` gridlines. Map a point (x,y) to pixels as px = 60 + x/10·600, py = 245 − y/10·190.
- **Pins (all ten, hardcoded):** `[[1,2],[6,1],[9,4],[7,9],[2,8],[4,4],[5,6],[6,4],[3,5],[7,5]]`.
- **Hull polygon:** closed 3px green `#008300` path through hull pins in order `[[6,1],[9,4],[7,9],[2,8],[1,2]]`, fill `rgba(0,131,0,0.10)`.
- **Dot style:** hull pins 7px green `#008300` dots; interior pins 6px mute `#6b7280` dots; every pin gets an 11px `#444` coordinate label like "(6,1)" offset to avoid the band.
- **Annotation (bold 13px green `#008300`, near pixel x=430, y=75):** "the band touches only the outer 5 pins".
- **Caption (12px `#444`, bottom right):** "illustrative — sprinkler positions invented for the example".

## Wrapping the Band, One Pin at a Time

**Tags:** `worked example` (blue), `gift wrapping` (green)

- **Start low** — begin at the lowest pin, (6,1); it must be on the band, nothing is below it
- **Swing a ruler** — anchor the ruler at (6,1) pointing east, rotate it counterclockwise until it first hits a pin
- **First hit** — (9,4) at 45° beats (7,5) at 76° and (7,9) at 83°, so the band's first edge is (6,1)→(9,4)
- **Repeat** — anchor at (9,4), keep rotating the same way: next hit (7,9), then (2,8), then (1,2)
- **Stop** — from (1,2) the ruler hits the start pin (6,1) again; the band is closed after 5 edges
- **Cost** — each of the 5 wraps checks all 10 pins: about h·n work (Jarvis march), fine for small n

*Example (italic):* Anchored at (6,1), the ruler sweeps up from east and touches (9,4) at 45° before any other pin — that one sweep picks the first fence post.

**Key point:** Gift wrapping is the rubber band in slow motion: from each hull pin, the next hull pin is simply the first one a rotating ruler touches — repeat until you return to the start.

### Visualization (canvas `c2`, 720×300)

Single-panel step view of the first wrap: all ten pins, the anchor at (6,1), three candidate rays with their angles, the winning edge drawn solid, and the remaining hull shown faint.

- **Title (bold 15px, `#1a5276`, top center):** "One Wrap: Swing from (6,1), First Pin Hit Wins".
- **Axes and pixel mapping:** identical to c1 (origin x=60, baseline y=245, plot 600×190, x and y 0 to 10 m, 12px `#444` ticks, `#e5e9ef` gridlines).
- **Pins:** same ten hardcoded points as c1; anchor (6,1) drawn as an 8px blue `#2a78d6` dot with bold 12px blue label "start: lowest pin (6,1)" below the axis area.
- **Candidate rays (from (6,1)):** dashed (dash 4/3) 2px lines — mute `#6b7280` to (7,5) with 11px label "76°", mute `#6b7280` to (7,9) with 11px label "83°", orange `#d95926` to (9,4) with bold 12px orange label "45° — first hit".
- **Winning edge:** solid 3px green `#008300` segment (6,1)→(9,4) with a small arrowhead at (9,4).
- **Rest of the hull:** faint 2px `rgba(0,131,0,0.25)` path (9,4)→(7,9)→(2,8)→(1,2)→(6,1) showing where later wraps will go.
- **Sweep arc:** thin orange `#d95926` arc of radius 40px around the anchor from 0° to 45° with a tiny arrowhead, showing the rotation direction.
- **Annotation (bold 12px green `#008300`, near pixel x=150, y=70):** two lines: "repeat 5 times:" / "(6,1)→(9,4)→(7,9)→(2,8)→(1,2)".

## Fences, Game Physics, and the Edge of Your Data

**Tags:** `where it's used` (blue), `real fences` (green), `extrapolation` (orange)

- **Real fences** — shortest fence around sprinklers, GPS collar points, or delivery drop-offs is a hull problem
- **Tighter than a box** — the hull encloses 45.5 m²; the box fences 64 m², ~41% more — the hull is ~29% tighter
- **Game physics** — engines wrap complex 3D models in convex hulls because hull collisions are cheap to test
- **Edge of the data** — a model asked to predict outside the hull of its training points is extrapolating
- **Outlier lens** — points that sit on the hull are the extremes; peeling hulls layer by layer ranks how deep points sit

*Example (italic):* The gardener's hull fences 45.5 m² while the straight-sided bounding box fences 64 m² — the rubber band saves 29% of the lawn from being fenced off.

**Key point:** Whenever the question is "what is the tight outer boundary of these points?", the answer is the convex hull — boxes and circles overshoot, and everything inside the hull comes free.

### Visualization (canvas `c3`, 720×300)

Single-panel overlay on the lawn map: the same ten pins with the hull polygon (green, filled) and the axis-aligned bounding box (orange, dashed) drawn together so the wasted corner area is visible.

- **Title (bold 15px, `#1a5276`, top center):** "Hull 45.5 m² vs Bounding Box 64 m² — the Band Is 29% Tighter".
- **Axes and pixel mapping:** identical to c1 (origin x=60, baseline y=245, plot 600×190, x and y 0 to 10 m, 12px `#444` ticks, `#e5e9ef` gridlines).
- **Pins:** same ten hardcoded points as c1; hull pins 6px green `#008300` dots, interior pins 5px mute `#6b7280` dots, no coordinate labels this time.
- **Hull:** closed 3px green `#008300` polygon through `[[6,1],[9,4],[7,9],[2,8],[1,2]]`, fill `rgba(0,131,0,0.12)`; bold 12px green label "hull: 45.5 m²" inside near (5,5).
- **Bounding box:** dashed (dash 6/4) 2px orange `#d95926` rectangle from (1,1) to (9,9); bold 12px orange label "box: 64 m²" just inside its top-left corner.
- **Waste marker:** small orange `#d95926` hatch or dot pattern in the box corner near (8.5,8.5) with 11px orange label "fenced for nothing".
- **Annotation (bold 12px `#1a5276`, near pixel x=520, y=230):** "same 10 pins, two fences".
- **Caption (12px `#444`, bottom right):** "areas by the shoelace formula, illustrative layout".

## One Stray Pin Stretches the Whole Band

**Tags:** `common mistake` (red), `outlier sensitivity` (orange)

- **The stray** — one extra sprinkler is logged at (14,2), far right of the cluster; just one point out of eleven
- **The band leaps** — the hull must reach it: (9,4) falls off the band and the fence runs straight to (14,2)
- **The cost** — fenced area jumps from 45.5 m² to 66.5 m², a 46% increase caused by a single point
- **Not a density map** — the band says nothing about where points are crowded; inside can be almost empty
- **The mistake** — reading the hull as "where my data lives"; it only shows the extremes, never the bulk

*Example (italic):* Ten sprinklers fence 45.5 m²; add one stray at (14,2) and the same rubber band suddenly fences 66.5 m² — one point moved almost half the answer.

**Common mistake:** Treating the convex hull as a summary of the data. Every hull vertex is an extreme point, so one outlier reshapes the whole boundary — check the strays before you trust the band.

### Visualization (canvas `c4`, 720×300)

Single-panel before/after on a widened lawn map: the original hull (green) and the stretched hull after adding the stray pin (magenta, dashed), with the stray highlighted and the dropped pin marked.

- **Title (bold 15px, `#1a5276`, top center):** "One Stray Pin at (14,2): the Fence Grows 45.5 → 66.5 m²".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = 0 to 15 m with 12px `#444` tick labels "0 m", "3 m", ..., "15 m" every 3; y = 0 to 10 m, ticks every 2; light `#e5e9ef` gridlines. Pixel mapping px = 60 + x/15·600, py = 245 − y/10·190.
- **Pins:** the ten hardcoded points from c1 as 5px `#6b7280` dots, plus the stray `[14,2]` as an 8px red `#e74c3c` dot with bold 12px red label "stray (14,2)".
- **Original hull:** closed 2px green `#008300` polygon through `[[6,1],[9,4],[7,9],[2,8],[1,2]]`, fill `rgba(0,131,0,0.10)`; 12px green label "before: 45.5 m²" near (3,3).
- **Stretched hull:** closed 3px dashed (dash 6/4) magenta `#d55181` polygon through `[[6,1],[14,2],[7,9],[2,8],[1,2]]`; bold 12px magenta label "after: 66.5 m²" near (10,6).
- **Dropped pin:** (9,4) circled with a 10px mute `#6b7280` ring and 11px `#6b7280` label "no longer on the band".
- **Annotation (bold 13px magenta `#d55181`, near pixel x=440, y=65):** two lines: "one point, +46% area —" / "the hull follows extremes".
- **Caption (12px `#444`, bottom right):** "illustrative — same lawn, one mislogged sprinkler".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all pin coordinates and hull orders are the hardcoded arrays above (no randomness); hull order, sweep angles (45°, 76°, 83°), perimeter 25.9 m, and areas (hull 45.5 m², box 64 m², stretched hull 66.5 m² via the shoelace formula) are exact for those points and must match between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
