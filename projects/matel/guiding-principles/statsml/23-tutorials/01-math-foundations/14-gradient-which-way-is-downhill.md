# Gradient: Which Way Is Downhill

**Page type:** detail page (tutorial layout: 4 card-sections, each h2 + two-column table.layout with text left 50% / canvas right 50%)
**HTML title tag:** Gradient: Which Way Is Downhill

**Subtitle:** Standing on a foggy hillside, the gradient points to the steepest step — the idea that trains most ML models

## A Hiker in the Fog

**Tags:** `core idea` (blue), `running example` (green)

- **The fix** — a hiker on a hillside in thick fog wants the valley floor but can see nothing
- **What she can do** — feel the tilt of the ground under her boots, right where she stands
- **Two slopes** — how steeply the ground rises going east, and going north: two numbers
- **The gradient** — those two slopes bundled into one arrow that points steepest uphill
- **The move** — step exactly opposite the arrow, re-feel the ground, step again, repeat

*Example:* She never sees the valley; she just keeps stepping against the local tilt until the ground feels flat.

**Key point:** The gradient is a purely local compass — one slope per direction, bundled into an arrow. Downhill is simply minus that arrow.

### Visualization (canvas `c1`, 720×300)

Contour map with hiker, uphill gradient arrow and downhill step arrow.

- **Title (bold 15px, `#1a5276`, top center):** "Contour Map of the Hill — the Hiker Only Knows Her Own Square Meter".
- **Contours:** ellipses of altitude = x² + 2y² at levels [1, 4, 9, 17], centered at (300, 172) with scale 38 px/unit (semi-axis √c along x, √(c/2) along y), stroked light gray `#e5e9ef` 1.5px; level labels 1, 4, 9, 17 in gray `#6b7280` 12px at the right edge of each ellipse.
- **Hiker:** orange `#d95926` 7px dot at math point (3, 2), labeled bold 12px "hiker at (3, 2), altitude 17"; a translucent gray fog circle `rgba(107,114,128,0.12)` radius 46 around her.
- **Uphill arrow:** gradient (6, 8) normalized to (0.6, 0.8) × 55px — dashed gray `#6b7280` (dash 5/4, 2px) from the hiker pointing up-right, labeled bold gray 12px "gradient (uphill)".
- **Downhill arrow:** solid green `#008300` 3px line with filled arrowhead in the exact opposite direction, labeled bold green 13px "her step: minus the gradient".
- **Side annotation (bold violet `#4a3aa7` 13px, left-aligned at x=540):** "fog: no map, no valley view —" / "only the tilt underfoot," / "and that is enough".
- **Bottom caption (gray `#6b7280` 12px, centered):** "rings = spots of equal altitude (x² + 2y²)".

## Feel the Slope, Take the Step

**Tags:** `worked example` (green), `steepest step` (blue)

- **The hill** — altitude = x² + 2y² (in hundreds of meters); the hiker stands at (3, 2), altitude 17
- **Slope east** — 2x = 6; **slope north** — 4y = 8; gradient arrow = (6, 8)
- **Step rule** — new spot = old spot − 0.1 × gradient; so (3, 2) becomes (2.4, 1.2)
- **Check it** — altitude at (2.4, 1.2) = 5.76 + 2.88 = 8.64: nearly half the height gone
- **Keep going** — altitudes fall 17 → 8.64 → 4.72 → 2.73 → 1.64 in four steps

*Example:* Redo step two by hand: at (2.4, 1.2) the gradient is (4.8, 4.8), so the next spot is (1.92, 0.72).

**Key point:** Each step needs only arithmetic you can do on paper: read two slopes, multiply by the step size, subtract. That loop is the whole algorithm.

### Visualization (canvas `c2`, 720×300)

Two panels: descent path on the contour map (left) and altitude per step (right), split by a dashed divider at x=368.

- **Title (bold 15px, `#1a5276`, top center):** "Four Steps of Size 0.1 × Gradient: Altitude 17 → 1.64".
- **Left panel:** contours at levels [1, 4, 9, 17] centered at (172, 178), scale 30. Descent path points: [(3, 2), (2.4, 1.2), (1.92, 0.72), (1.536, 0.432), (1.229, 0.259)] connected by orange `#d95926` 2.5px line; 5px dots — start orange, rest blue `#2a78d6`. Start labeled bold orange 12px "start (3, 2)". Caption below (gray 12px): "each step cuts across the rings".
- **Right panel** (x=415, width 260, baseline y=240, height 175, y max 18): altitude vs step number 0–4, values [17, 8.64, 4.72, 2.73, 1.64] as green `#008300` 3px line with 4.5px dots; each point labeled with its altitude in bold 12px `#1a5276`; step numbers 0–4 below in gray.
- **Right panel labels:** "step number" gray 12px bottom center; headings bold 13px centered — `#1a5276` "altitude (hundreds of m)" and green "~90% of the height gone in 4 steps".

## Loss Is the Hill, Weights Are the Hiker

**Tags:** `where it's used` (blue), `gradient descent` (blue), `optimization` (blue)

- **The swap** — position becomes the model's weights; altitude becomes the training error (loss)
- **Millions of directions** — a real model has one slope per weight, not two; same arrow idea
- **Training step** — weights − learning rate × gradient: identical to the hiker's step rule
- **Fog is real** — no one can picture a million-dimension landscape; local slope is all there is
- **Without it** — you would try weight values blindly; the gradient says which tweak helps most

*Example:* A spam filter's training run is the hiker's walk: loss 2.19 down to 0.24 over 10 steps, one gradient at a time.

**Key point:** Gradient descent trains regressions, neural networks, and recommenders alike — the loss curve you watch during training is the hiker's altitude log.

### Visualization (canvas `c3`, 720×300)

Line chart: training loss curve over 10 steps.

- **Title (bold 15px, `#1a5276`, top center):** "Spam Filter Training Loss — Same Walk, Million-Dimensional Hill (illustrative)".
- **Data:** loss = [2.19, 1.62, 1.21, 0.92, 0.71, 0.55, 0.44, 0.36, 0.30, 0.26, 0.24] at steps 0–10; y max 2.4.
- **Axes:** gray `#999` L-shape; step numbers 0–10 as x tick labels; padding top 52 / bottom 50 / left 62 / right 190. Axis titles gray `#444` 12px: "training step (one gradient each)" bottom center, "loss (training error)" rotated on left.
- **Series:** blue `#2a78d6` 3px line with 4px dots at each point.
- **Point labels (bold 12px `#1a5276`):** "loss 2.19" at the first point, "loss 0.24" at the last.
- **Annotation (bold orange `#d95926` 13px, left-aligned right of plot):** "same rule as the hiker:" / "weights − rate × gradient," / "once per step".

## Uphill Arrow, Downhill Step — and Step Size

**Tags:** `common mistake` (red), `learning rate` (orange)

- **Sign flip** — the gradient points steepest UPHILL; descent moves along MINUS the gradient
- **Too small** — step size 0.05 on the bowl y = x²: after 3 steps still at x = 2.92 of a start at 4
- **Just right** — step size 0.45: x goes 4 → 0.4 → 0.04, at the bottom almost immediately
- **Too big** — step size 1.05: x goes 4 → −4.4 → 4.84 → −5.32, bouncing ever higher
- **Rule** — a rising, zig-zagging loss usually means the step size, not the model, is broken

*Example:* Each step multiplies x by a fixed factor here: 0.9 (crawl), 0.1 (sprint), or −1.1 (explode).

**Common mistake:** Blaming the data when loss diverges. Overshooting the valley floor and landing higher on the far wall is the classic too-big-step signature.

### Visualization (canvas `c4`, 720×300)

Parabola bowl y = x² with three descent trajectories at different step sizes.

- **Title (bold 15px, `#1a5276`, top center):** "Step Size on the Bowl y = x²: Crawl, Sprint, Explode".
- **Bowl:** y = x² plotted over x −6..6, clipped at y=32, gray `#999` 2px; light gridline `#e5e9ef` along y=0. Padding top 50 / bottom 46 / left 40 / right 185.
- **Walks** (2.5px polylines with 4px dots, y clipped to 32):
  - Yellow `#c98500`: x = [4, 3.6, 3.24, 2.92] (lr 0.05, crawl).
  - Green `#008300`: x = [4, 0.4, 0.04] (lr 0.45, sprint).
  - Magenta `#d55181`: x = [4, −4.4, 4.84, −5.32] (lr 1.05, explode).
- **Legend (left-aligned at x = width−172, 12×12 color swatches, text `#222` 12px):** "0.05: crawls (4 → 2.92)", "0.45: lands (4 → 0.04)", "1.05: bounces higher".
- **Annotation (bold magenta 13px below legend):** "too-big steps overshoot" / "the floor and climb the" / "far wall — loss diverges".
- **X-axis caption (gray `#444` 12px, bottom center):** "weight value x (bowl bottom at 0)".

## Regeneration instructions

- **Template:** tutorials topic page (see `tutorials/CLAUDE.md`): `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` row of colored pill spans (0.72rem bold, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`); one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem) starting with `<strong>Key point:</strong>` or `<strong>Common mistake:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius.
- **Canvases:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart JS palette object: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Shared helpers `drawContours` (ellipse contours of x² + 2y²) and `toScreen` used by c1/c2. All data arrays hardcoded (no Math.random). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
