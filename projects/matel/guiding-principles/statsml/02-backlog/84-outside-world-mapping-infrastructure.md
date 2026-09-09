# Outside-World Mapping Infrastructure — Stitching Partial Captures into One World

**Page type:** detail page (two-column layout table per section: text left 45%, canvas right 55%, one `.lang-section` per topic)
**HTML title tag:** Outside-World Mapping Infrastructure — Stitching Partial Captures into One World

**Subtitle:** Sonar ships, sky surveys, satellites, and street fleets run one pattern: many partial captures registered and stitched into one world model.

**Intro callout:** No sensor sees the whole world — every map is stitched from partial views on shared coordinates. Stitch across viewpoints for 3D structure; stitch across revisits for history.

## 1. The shared pattern — many partial captures, one world model

Every system on this page runs the same stages:

- **Partial capture** — each device sees a small patch from one position at one moment.
- **Registration** — overlap (shared stars, landmarks, GPS) places captures on one frame.
- **Stitching** — registered captures fuse into one model, with seams where they disagree.
- **Two stitch axes** — viewpoints give 3D structure; revisits give a timeline.

**Key point:** The map is a derived artifact — it inherits registration quality and capture density.

### Visualization (canvas `c1`, 720×340)

Convergence diagram: scattered capture frames on the left flowing into a registration stage, then splitting into two outputs — a 3D wireframe and a filmstrip timeline.

- **Title (bold 14px `#1a5276`, top center):** "Partial captures → registration → 3D model or timeline".
- **Capture frames (left, x≈40–170):** six small tilted rectangles (58×40, rotations −12°…+12°), 2px strokes cycling `#1a5276`/`#27ae60`/`#e67e22`, fill `rgba(26,82,118,0.08)`; each with a tiny camera-position dot at one corner; 10px `#666` labels under the cluster: "different positions" / "different times".
- **Registration box (center, x=280–440):** rounded rect 160×70 centered at (360,170), fill `rgba(142,68,173,0.10)`, 2px `#8e44ad` stroke; bold 12px `#8e44ad` "REGISTRATION"; 10px `#666` "shared coordinates," / "overlap as anchor"; `#bbb` connector lines from each frame to the box's left edge.
- **Output A (top right):** 3D wireframe hill — a grid of quadratic curves inside a 170×95 area at (500,60), 1.5px `#27ae60` lines; bold 12px `#27ae60` label "3D structure — stitched across viewpoints".
- **Output B (bottom right):** filmstrip — four 38×30 panels in a row at (500,215), 2px `#e67e22` strokes, small year ticks "t1…t4" under each; bold 12px `#e67e22` label "timeline — stitched across revisits".
- **Arrows:** 2px `#999` from registration box to each output.
- **Caption (12px `#999`, bottom center):** "same pipeline whether the sensor is a sonar ping, a telescope, a satellite, or a camera car".

## 2. Sonar — mapping surfaces light cannot reach

Most of the planet's solid surface is mapped by sound, not photographs:

- **Echo ranging** — a ship pings and times the return; sound travels ~1,500 m/s in water.
- **Depth formula** — speed × round-trip time ÷ 2: 4 s × 1,500 m/s ÷ 2 = 3,000 m.
- **Swath by swath** — multibeam fans a seafloor stripe; surveys mow overlapping stripes.
- **Same trick on land** — lidar and radar altimetry time an echo with light and radio.
- **Coverage reality** — little of the seafloor is mapped at high resolution.
- **Interpolation** — gaps are filled from sparse soundings and satellite gravity.

**Key point:** The "3D map" is a model fitted to echoes — interpolated gaps look as confident as real data.

### Visualization (canvas `c2`, 720×360)

Cutaway ocean scene: ship on the surface, sound cone to the seafloor, echo arcs returning, seafloor profile emerging as stitched swath stripes.

- **Title (bold 14px `#1a5276`, top center):** "Sonar: time an echo, infer a surface, mow the ocean in swaths".
- **Water surface:** wavy 2px `#1a5276` line at y=90 across the width; sky above white, water below filled `rgba(26,82,118,0.06)`.
- **Ship:** simple hull polygon (80px wide) with a small bridge block, 2px `#1a5276` stroke, sitting on the waterline centered at x=200.
- **Ping cone:** two dashed 1.5px `#e67e22` lines from the hull bottom diverging to the seafloor (y≈290), light orange fill `rgba(230,126,34,0.08)`; three concentric echo arcs in 1.5px `#27ae60` rising back toward the hull.
- **Seafloor:** irregular ridge-and-valley profile line, 2.5px `#8e44ad`, from x=40 to x=680 around y=270–320.
- **Swath stripes:** five adjacent vertical stripes along the seafloor (each ~90px wide) alternating fills `rgba(39,174,96,0.15)` / `rgba(39,174,96,0.28)`, showing prior track lines already mapped; 10px `#666` label "overlapping swaths from parallel track lines".
- **Worked number (bold 12px `#e67e22`, right of the cone):** "4 s round trip × 1,500 m/s ÷ 2 = 3,000 m deep".
- **Caption (12px `#999`, bottom center):** "lidar and radar altimetry: the same time-an-echo pattern with light and radio".

## 3. Sky surveys — photographing everything above, repeatedly

Survey telescopes photograph the entire visible sky on a cycle:

- **Tiling** — overlapping fields; each night's exposures tile a stripe of sky.
- **Stars as control points** — shared stars register every exposure onto one celestial frame.
- **Stacking** — co-registered exposures add up; faint objects emerge from the stack.
- **Time domain** — subtracting revisits reveals what moved, brightened, or vanished.

**Key point:** One survey, two maps: stacking builds the deep sky, differencing builds the change movie.

### Visualization (canvas `c3`, 720×360)

Two-panel sky scene: left, overlapping exposure frames tiling a star field with shared stars marked as anchors; right, the same field at three visits with one moving object flagged.

- **Title (bold 14px `#1a5276`, top center):** "One sky, two products: the deep stack and the change movie".
- **Sky background:** full-width dark panel `rgba(26,82,118,0.85)` from y=48 to y=300; ~40 small white star dots (hardcoded positions, radii 1–2px).
- **Left panel (x=40–360):** three overlapping 130×95 exposure frames, 2px strokes `#27ae60`/`#e67e22`/`#8e44ad`, rotated slightly; stars falling in overlap zones circled in 1.5px `#e67e22` r=6 with 10px label "shared stars = registration anchors"; bold 12px white label under panel: "overlapping tiles, different nights".
- **Right panel (x=390–680):** three 90×90 frames side by side labeled "visit 1 / visit 2 / visit 3" (10px white); identical star pattern in each, except one `#e74c3c` dot that shifts position frame to frame with a thin red dashed trajectory arrow; bold 12px `#e74c3c` annotation "moved between visits → flagged".
- **Caption (12px `#999`, bottom center, below the dark panel):** "stack the visits for depth, subtract them for change — the same exposures feed both".

## 4. Revisit fleets — same place, many passes, many devices

Satellites and camera cars map by returning; the revisit schedule is the product:

- **Constellation revisits** — many satellites image the same coordinates on different days.
- **Street fleets** — camera cars re-drive roads; one address accumulates dated panoramas.
- **Cross-device stitching** — one timeline mixes unrelated sensors, resolutions, angles.
- **Access angle** — who can browse these archives is covered under data-acquisition.

**Key point:** A place's timeline is assembled from whichever devices happened to pass — cadence varies wildly.

### Visualization (canvas `c4`, 720×360)

Scene of one land parcel observed from above and from the street, feeding a dated filmstrip.

- **Title (bold 14px `#1a5276`, top center):** "One parcel, many passing devices, one assembled timeline".
- **Parcel:** small map tile (140×100) centered at (170,190) with a line-drawn house and lot boundary, 2px `#1a5276`.
- **Passing devices:** three satellite glyphs (body rect + two solar-panel wings, 1.5px strokes `#27ae60`/`#e67e22`/`#8e44ad`) on dashed arc trajectories over the parcel, each with a 10px date label ("Mar 12" / "Apr 03" / "Jun 21"); one camera-car glyph (car body + roof camera stub, 2px `#e74c3c`) on a ground line passing the parcel, labeled "Sep 40× street pass" style date ("Sep 08").
- **Sight lines:** thin dashed lines from each device to the parcel center.
- **Filmstrip (right, x=420–690):** four 60×46 panels in a row at y=160 with the date under each; panels stroked in the color of the device that contributed them, so the strip visibly interleaves sources; 10px `#666` note "different device, different resolution, same coordinates".
- **Annotation (bold 12px `#e67e22`, above filmstrip):** "the timeline belongs to the place, not to any one sensor".
- **Caption (12px `#999`, bottom center):** "cadence is opportunistic — some places get weekly passes, others wait years".

## 5. Why the stitched world lies a little

The assembled map carries artifacts of its own assembly:

- **Registration error** — offsets show as ghost edges, doubled roads, terrain steps.
- **Seam disagreement** — adjacent captures disagree at the join; blending hides it.
- **Uneven sampling** — a change is dated to the capture that first shows it.
- **Condition confounds** — season, lighting, tide, and sensor generation all change pixels.
- **Absence is not evidence** — an unvisited patch means "nobody looked", not "nothing there".

**Key point:** These are statistics problems in cartography costume: measurement error, irregular sampling, confounded comparisons.

### Visualization (canvas `c5`, 720×340)

Two half-panels: a misregistered seam on the left, a revisit-gap timeline on the right.

- **Title (bold 14px `#1a5276`, top center):** "Assembly artifacts: seams in space, gaps in time".
- **Divider:** dashed 1.5px `#ccc` vertical line at x=360 from y=50 to y=290.
- **Left panel — seam:** two abutting image tiles (150×160 each) meeting at x=180; a road drawn as a 3px `#1a5276` line crosses both tiles but is offset 12px vertically at the join, with a 1.5px `#e74c3c` circle around the discontinuity and bold 12px `#e74c3c` label "registration offset → ghost step"; tile fills two slightly different grays (`rgba(26,82,118,0.06)` vs `rgba(26,82,118,0.12)`) with 10px `#666` note "different pass, different lighting".
- **Right panel — gaps:** horizontal timeline axis (2px `#999`) at y=200 from x=400 to x=680; capture ticks as `#27ae60` dots at uneven spacings (cluster of 5, then a long gap, then 2); a `#e74c3c` "×" above the middle of the long gap marked "real change happens here" (10px), and an `#e67e22` arrow to the next capture dot labeled bold 12px `#e67e22` "…but gets dated here".
- **Caption (12px `#999`, bottom center):** "measurement error, irregular sampling, confounded comparisons — statistics problems in cartography costume".

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style 2-col): h1, `.subtitle`, `.intro` callout, then one `.lang-section` per numbered topic. Each section: `<h2>` with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, full width) with one row: `td.text-col` (45%) holding an intro sentence, a `<ul>` of labeled bullets (bold lead terms), and a `.key-point` div; `td.viz-col` (55%) holding the canvas. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6, full page width (no max-width). h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with 3px `#2980b9` left border; `.key-point` background `#f8f9fa` with 3px `#e74c3c` left border; ul 0.92rem; `strong` inherits bullet color. Canvases `width: 100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvases:** intrinsic width 720, heights as given per chart (340/360/360/360/340); shared `setupCanvas(id, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c` (artifacts/alarm only), orange `#e67e22`, purple accent `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#999`.
- **Data integrity:** all star positions, dates, and depths are hardcoded illustrative literals — no `Math.random()`; the sonar worked number (4 s × 1,500 m/s ÷ 2 = 3,000 m) must match between text and canvas.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
