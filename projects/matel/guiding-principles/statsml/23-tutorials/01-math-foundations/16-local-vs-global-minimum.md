# Local vs Global Minimum

**Page type:** detail page (tutorial layout: 4 card-sections, each h2 + two-column table.layout with text left 50% / canvas right 50%)
**HTML title tag:** Local vs Global Minimum

**Subtitle:** The lowest point nearby is not always the lowest point anywhere — why optimizers get stuck in valleys

## Two Dips, One Fooled Hiker

**Tags:** `core idea` (blue), `running example` (green)

- **The trail** — a 10 km mountain trail in fog; the hiker wants its lowest point
- **Her rule** — always walk toward lower ground; stop when every direction goes up
- **The trap** — at km 2 she reaches a dip at 560 m where both sides rise — she stops
- **The truth** — the real trail floor is 300 m at km 8, hidden behind an 850 m ridge
- **The names** — her dip is a local minimum; the 300 m floor is the global minimum

*Example:* Standing at 560 m, every step she can feel goes uphill — local information says "done", and it is wrong.

**Key point:** "No direction improves" only proves you are at the bottom of *a* valley — never that it is *the* valley.

### Visualization (canvas `c1`, 720×300)

Trail elevation profile with the local dip and the global floor marked.

- **Title (bold 15px, `#1a5276`, top center):** "The Foggy Trail: a 560 m Dip and the 300 m Floor".
- **Data:** altitudes at km 0–10: [900, 700, 560, 620, 780, 850, 700, 480, 300, 350, 500] m; altitude scale 250–950 m; padding top 50 / bottom 46 / left 56 / right 28.
- **Profile:** blue `#2a78d6` 3px polyline with area beneath filled `rgba(42,120,214,0.10)`; gray `#999` L-shaped axes; x tick labels 0, 2, 4, 6, 8, 10 (gray `#444` 12px); x-axis caption "trail position (km)".
- **Markers:** orange `#d95926` 7px dot at (km 2, 560 m) labeled bold 13px "stuck: 560 m" and bold 12px "local minimum" below it; green `#008300` 7px dot at (km 8, 300 m) labeled "300 m — global minimum".
- **Ridge label (bold gray `#6b7280` 12px above km 5):** "850 m ridge blocks the view".
- **Annotation (bold magenta `#d55181` 13px, left-aligned in the open area above the right slope, from km 6.2):** "every step out of the dip goes up —" / "so the greedy hiker never leaves it".

## Walk the Trail by Hand: Three Starting Points

**Tags:** `worked example` (green), `restarts` (blue)

- **The altitudes** — at km 0..10: 900, 700, 560, 620, 780, 850, 700, 480, 300, 350, 500 m
- **Greedy rule** — move 1 km to the lower neighbor; stop when both neighbors are higher
- **Start km 0** — 900 → 700 → 560, stuck: both neighbors (700, 620) are higher
- **Start km 4** — 780 → 620 → 560, stuck in the same local dip
- **Start km 6** — 700 → 480 → 300: this start finds the global floor
- **Best of three** — min(560, 560, 300) = 300 m; the restarts, not the rule, saved us

*Example:* Trace start km 4 yourself: 780's neighbors are 620 and 850, so go left; 620's are 560 and 780, left again; done.

**Key point:** Where you start decides where you finish. Here 2 of 3 starts end in the wrong valley — cheap insurance is running several and keeping the best.

### Visualization (canvas `c2`, 720×300)

Trail profile with three greedy descent walks overlaid, plus a legend.

- **Title (bold 15px, `#1a5276`, top center):** "Greedy Downhill From km 0, 4, and 6: Where Each Walk Ends".
- **Base:** same trail profile as c1 (altitudes [900, 700, 560, 620, 780, 850, 700, 480, 300, 350, 500], scale 250–950), padding right widened to 176 for the legend.
- **Walks** (2.5px polylines drawn 8px above the profile; round dot = start, filled square = end):
  - Orange `#d95926`: km 0 → 1 → 2.
  - Violet `#4a3aa7`: km 4 → 3 → 2.
  - Green `#008300`: km 6 → 7 → 8.
- **Legend (left-aligned at x = width−162, 12×12 swatches, text `#222` 12px):** "km 0 → ends 560 m", "km 4 → ends 560 m", "km 6 → ends 300 m".
- **Annotation (bold green 13px below legend):** "best of 3 restarts:" / "300 m — only one" / "start found the floor".
- **X-axis caption (gray `#444` 12px, centered):** "trail position (km); dot = start, square = where the walk stops".

## Where Models Get Stuck: Loss Landscapes

**Tags:** `where it's used` (blue), `loss landscape` (blue), `optimization traps` (orange)

- **The swap** — trail position becomes model weights; altitude becomes training loss
- **Gradient descent** — is exactly the hiker's greedy rule, so it inherits the same trap
- **k-means** — different random starting centroids routinely give different clusterings
- **Neural nets** — bumpy loss landscapes; SGD's noise and momentum help roll past small dips
- **Convex luck** — linear and logistic regression have one bowl: local = global, no trap
- **In practice** — run 3–10 restarts and keep the lowest loss (illustrative runs shown)

*Example:* Three restarts of the same model settle at loss 0.80, 0.55, and 0.31 — only the seed differed.

**Key point:** A converged optimizer reports the bottom of its own valley. Restarts, noise, and momentum exist to check the other valleys.

### Visualization (canvas `c3`, 720×300)

Three training-loss curves flattening at different plateaus.

- **Title (bold 15px, `#1a5276`, top center):** "Same Model, Three Random Seeds (illustrative training runs)".
- **Data (epochs 0–9, y max 2.7):**
  - Seed 1 (orange `#d95926`): [2.40, 1.60, 1.10, 0.90, 0.83, 0.81, 0.80, 0.80, 0.80, 0.80].
  - Seed 2 (violet `#4a3aa7`): [2.55, 1.75, 1.15, 0.82, 0.65, 0.58, 0.56, 0.55, 0.55, 0.55].
  - Seed 3 (green `#008300`): [2.35, 1.50, 0.95, 0.62, 0.44, 0.36, 0.33, 0.32, 0.31, 0.31].
- **Axes:** gray `#999` L-shape; padding top 52 / bottom 48 / left 62 / right 190; axis titles gray `#444` 12px: "training epoch" bottom center, "loss" rotated on left.
- **Series:** each a 3px line; end labels bold 12px in the series color right of the last point: "seed 1: flat at 0.80", "seed 2: flat at 0.55", "seed 3: flat at 0.31".
- **Annotation (bold magenta `#d55181` 13px, upper-left within plot):** "all three \"converged\" —" / "they sit in different valleys".

## "Loss Stopped Improving" Does Not Mean "Best Model"

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The mix-up** — a flat loss curve gets read as "this is as good as it gets"
- **What flat means** — the optimizer is at the bottom of some valley, nothing more
- **Zoomed in** — around km 2 the trail looks like a perfect bowl; the dip seems global
- **Zoomed out** — the same dip is a minor pothole 260 m above the real floor
- **Rule** — before trusting a converged run, ask: would a different start land lower?

*Example:* The optimizer only ever sees the zoomed-in view — inside its window, 560 m is unbeatable.

**Common mistake:** Declaring victory from one converged run. Convergence certifies a valley bottom, not the lowest valley — compare restarts before you ship.

### Visualization (canvas `c4`, 720×300)

Two panels split by a dashed divider at x=330: zoomed-in bowl vs the full trail.

- **Title (bold 15px, `#1a5276`, top center):** "What the Optimizer Sees vs What Is Actually There".
- **Left panel** (x=60, y=60, 230×165): zoom window km 1–3 only — points km [1, 1.5, 2, 2.5, 3] with altitudes [700, 615, 560, 585, 620] m on a 540–720 m scale, orange `#d95926` 3px curve with a 6px orange dot at (2, 560). Labels: bold 13px `#1a5276` "zoomed in: km 1–3 only" above; bold orange 12px "looks like THE bottom" below the dot; gray `#444` 12px "the optimizer's whole world" beneath the panel.
- **Right panel** (padding left 380 / top 60 / bottom 44 / right 30): full trail profile (altitudes [900, 700, 560, 620, 780, 850, 700, 480, 300, 350, 500], scale 250–950) as blue `#2a78d6` 2.5px line. Dashed orange rectangle (dash 5/3, 2px) boxing km 1–3 × 540–720 m (the zoom window). Green `#008300` 6px dot at (km 8, 300 m) labeled bold 12px "real floor: 300 m". Heading bold 13px `#1a5276` "zoomed out: the full trail".
- **Annotation (bold magenta `#d55181` 12px near the boxed dip):** "same dip: a pothole," / "260 m above the floor".

## Regeneration instructions

- **Template:** tutorials topic page (see `tutorials/CLAUDE.md`): `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` row of colored pill spans (0.72rem bold, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`); one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem) starting with `<strong>Key point:</strong>` or `<strong>Common mistake:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius.
- **Canvases:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart JS palette object: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Shared `trailAlt` array and `drawTrail(ctx, pad, cw, ch)` helper (draws axes, filled profile, km tick labels; returns X/Y mappers) used by c1 and c2. All data arrays hardcoded (no Math.random). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
