# The Kernel Trick

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Kernel Trick

**Subtitle:** Some problems a line cannot split become easy in a bigger space — the kernel trick computes in that huge space without ever building a single coordinate in it

## Sorting Strawberries With One Measurement

**Tags:** `core idea` (blue), `feature map` (green), `linear separation` (orange)

- **The sorter** — a farm machine grades strawberries by one number: diameter, from 1.0 to 4.0 cm
- **The problem** — ripe berries sit in the middle (2.0–3.0 cm); smaller are unripe, bigger overripe
- **No cut works** — any single threshold on diameter leaves reject berries on both of its sides
- **The lift** — invent a second axis, diameter squared: each berry becomes a point (x, x²)
- **Now a line works** — in the lifted picture the line y = 5x − 5.75 puts every ripe berry below it

*Example (italic):* The 2.3 cm berry lands at (2.3, 5.29), just under the line's 5.75 there — ripe; the 3.4 cm berry lands at (3.4, 11.56), above the line's 11.25 — reject.

**Key point:** A problem no line can solve in the original space often becomes linearly separable after mapping the data into a higher-dimensional feature space.

### Visualization (canvas `c1`, 720×300)

Dual-panel: the ten berry diameters on a 1D number line (left) vs the same berries lifted to (x, x²) with a separating line (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Strawberry Sizes: One Line Fails in 1D, Works After the x² Lift".
- **Data:** diameters `[1.0, 1.3, 1.6, 2.0, 2.3, 2.6, 3.0, 3.4, 3.7, 4.0]`; ripe = `{2.0, 2.3, 2.6, 3.0}` (green `#008300` dots), all others reject (magenta `#d55181` dots); squared values `[1.00, 1.69, 2.56, 4.00, 5.29, 6.76, 9.00, 11.56, 13.69, 16.00]`.
- **Left panel (1D):** number line 2px `#999` at y=160 from x=55, width 280, scale 0.8 → 4.2 cm; 6px dots for each diameter in its ripe/reject color; tick labels "1", "2", "3", "4" 12px `#444` below; one dashed `#bdc3c7` vertical trial-cut line at 1.8 cm from y=100 to y=210; magenta bold 12px annotation, two lines: "any single cut leaves" / "rejects on both sides"; caption 12px `#444` "diameter (cm) — the only measurement".
- **Right panel (lifted):** axis origin x=400, width 280, baseline y=245, chart height 185; x range 0.8–4.2, y range 0–17; the ten points plotted at (x, x²) with the same colors and 6px dots; blue `#2a78d6` 2px separating line y = 5x − 5.75 drawn from x=1.4 (y=1.25) to x=4.2 (y=15.25); green bold 13px annotation "line y = 5x − 5.75: all ripe below"; caption 12px `#444` "lifted: (diameter, diameter²) — illustrative data".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Two Roads to the Same Dot Product

**Tags:** `worked example` (blue), `kernel function` (green)

- **What's needed** — SVM-style algorithms never need coordinates, only dot products between pairs
- **The long road** — map φ(x) = (x², √2·x, 1), then dot them: φ(3)·φ(4) = 144 + 24 + 1 = 169
- **The shortcut** — the kernel k(x, z) = (xz + 1)² gives (3·4 + 1)² = 13² = 169 in one line
- **Same answer** — the kernel IS the lifted-space dot product, computed without the map
- **The trick** — swap every dot product for k(x, z), and the algorithm trains in the lifted space

*Example (italic):* φ(3) = (9, 4.24, 1) and φ(4) = (16, 5.66, 1) were never actually needed — 13² = 169 skips both of them.

**Key point:** This is the kernel trick: replace each dot product with a kernel function, and you compute in the lifted space without ever constructing it.

### Visualization (canvas `c2`, 720×300)

Flow diagram: one input box branching into a long top road (build coordinates, then dot) and a short bottom road (kernel), both arriving at the same result box.

- **Title (bold 15px, `#1a5276`, top center):** "Two Roads From x = 3, z = 4 to the Dot Product 169".
- **Input box:** rounded rect (40, 125) to (140, 175), 2px `#1a5276` border, white fill, "x = 3, z = 4" bold 13px `#1a5276` centered.
- **Top road (magenta `#d55181`):** label bold 12px magenta "the long road: build 3 coordinates per point" at y=52 centered over the road; three rounded boxes 130×44 centered at (245, 80), (395, 80), (545, 80), 2px magenta border, texts 12px `#2c3e50`: "φ(3) = (9, 4.24, 1)", "φ(4) = (16, 5.66, 1)", "144 + 24 + 1"; 2px magenta arrows: input box top edge → first box, then box → box left-to-right, then last box → result box.
- **Bottom road (green `#008300`):** one rounded box 180×44 centered at (395, 222), 2px green border, "(3·4 + 1)² = 13²" bold 13px green; label bold 12px green "the kernel shortcut: one line, no coordinates" at y=262 centered; 2px green arrows: input box bottom edge → box → result box.
- **Result box:** rounded rect (610, 128) to (695, 172), 3px `#199e70` border, "169" bold 16px `#199e70` centered.
- **Annotation (bold 13px `#1a5276`, centered at y=290):** "same number — the feature space was never visited".

## Spaces Too Big to Visit

**Tags:** `where it's used` (blue), `scaling` (orange)

- **Blow-up** — with 100 inputs, degree-2 features number 5,151; degree-3 already 176,851
- **Worse fast** — degree 4 needs 4,598,126 coordinates; degree 5 needs 96,560,646 of them
- **Kernel cost** — (x·z + 1)^d is one dot product plus a power: about 200 ops at any degree
- **Infinite case** — the RBF kernel exp(−γ‖x − z‖²) matches an infinite-dimensional lifted space
- **Where it's used** — SVMs, kernel PCA, kernel ridge regression, and Gaussian processes all rely on it

*Example (italic):* A degree-5 sorter over 100 sensor readings would need 96,560,646 coordinates per berry — the kernel gets the same dot product in about 200 operations.

**Key point:** The kernel's cost grows with the number of inputs, not with the size of the feature space — that is what makes huge and even infinite spaces usable.

### Visualization (canvas `c3`, 720×300)

Log-scale bar chart: feature-space dimension for polynomial degrees 2–5 (exploding bars) against a flat dashed line for the kernel's operation count.

- **Title (bold 15px, `#1a5276`, top center):** "100 Inputs: Feature-Space Size Explodes, Kernel Cost Does Not (log scale)".
- **Data:** degrees `[2, 3, 4, 5]`; dimensions `[5151, 176851, 4598126, 96560646]` (= C(100+d, d)); kernel operations flat at `200`.
- **Layout:** axis origin x=70, baseline y=240, chart height 180, plot width 560; y is log10 from 2 to 8; horizontal gridlines 1px `#e5e9ef` at log10 = 2, 4, 6, 8 with labels "100", "10k", "1M", "100M" 12px `#6b7280` on the left.
- **Bars:** width 70, centered at x = 160, 300, 440, 580; fill `rgba(42,120,214,0.45)`, 2px `#2a78d6` border; bar heights from log10 of each dimension; bold 12px `#2a78d6` value labels above each bar: "5,151", "176,851", "4.6M", "96.6M"; x labels "degree 2" ... "degree 5" 12px `#444` below baseline.
- **Kernel line:** green `#008300` dashed 2px horizontal line at log10(200) ≈ 2.30 (just above the baseline), spanning the plot width; green bold 13px label above its right end, drawn with a 4px white halo (strokeText) for legibility over the bars: "kernel: ~200 ops at every degree".
- **Caption (12px `#444`, bottom left):** "features counted as C(100+d, d); kernel is (x·z + 1)^d".

## Nothing Is Ever Transformed

**Tags:** `common mistake` (red), `Gram matrix` (green)

- **The myth** — people picture the data being moved into the big space; no coordinates exist anywhere
- **What exists** — training builds only the Gram matrix: the kernel value for every pair of points
- **Our berries** — sizes 1, 2, 3, 4 cm fill a 4×4 table, from k(1,1) = 4 up to k(4,4) = 289
- **A similarity** — read k(x, z) as "how alike these two points are, as the lifted space sees them"
- **Not universal** — an algorithm must be rewritten to use only dot products before the swap works

*Example (italic):* The trained berry sorter stores this 4×4 table plus a few weights — never one single point in the lifted space.

**Common mistake:** Saying the kernel "projects the data into higher dimensions". It does not — it returns the dot product two points would have there, and that number is all the algorithm ever needs.

### Visualization (canvas `c4`, 720×300)

Left: the actual 4×4 Gram matrix drawn as a shaded grid with numbers. Right: myth-vs-reality panel with a crossed-out lifted scatter and a green reality statement.

- **Title (bold 15px, `#1a5276`, top center):** "What the Algorithm Actually Sees: the 4×4 Gram Matrix".
- **Data:** berry sizes `[1, 2, 3, 4]` (cm); kernel k(x, z) = (xz + 1)²; matrix K = `[[4, 9, 16, 25], [9, 25, 49, 81], [16, 49, 100, 169], [25, 81, 169, 289]]`.
- **Matrix grid:** top-left cell at (130, 80), cells 62 wide × 44 tall, 1px `#e5e9ef` cell borders; column headers "1 cm" ... "4 cm" bold 12px `#1a5276` above, row headers the same to the left; cell fill `rgba(42,120,214, 0.08 + 0.55 * value / 289)`; cell numbers 12px, white when value ≥ 100 else `#2c3e50`.
- **Myth block (right, from x=460):** magenta `#d55181` bold 13px heading "the myth:" at y=85; 12px `#6b7280` text "points relocated into the lifted space" below; a small faded scatter of five 4px dots `rgba(107,114,128,0.5)` around (530, 150)–(650, 190) with two 3px magenta `#d55181` cross strokes drawn over it (an X from (500, 125) to (670, 200) and (670, 125) to (500, 200)).
- **Reality block (right):** green `#008300` bold 13px heading "the reality:" at y=235; 12px `#2c3e50` text "just these 16 similarity numbers" below.
- **Caption (12px `#444`, bottom center):** "k(x, z) = (xz + 1)² for berry sizes 1–4 cm — computed straight from the sizes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
