# Eigenvalues & Eigenvectors

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Eigenvalues & Eigenvectors

**Subtitle:** An eigenvector is a direction a matrix stretches but never turns; the eigenvalue is the stretch factor — PCA is just finding these directions in a data cloud

## The Stretch Tool That Leaves Two Arrows Alone

**Tags:** `core idea` (blue), `kept direction` (green), `stretch factor` (orange)

- **The tool** — a photo app moves every arrow the same way: new x = 2x + y, new y = x + 2y
- **Most arrows turn** — the right-pointing arrow (1, 0) swings up to (2, 1); its direction changed
- **One stays put** — the diagonal arrow (1, 1) maps to (3, 3): same direction, just 3× longer
- **The other diagonal** — (1, −1) maps to (1, −1): the tool leaves it completely alone (×1)
- **The names** — a kept direction is an eigenvector; its stretch factor is the eigenvalue

*Example (italic):* The stretch tool turns every arrow drawn on the photo except the two diagonals — those only change length, never direction.

**Key point:** An eigenvector of a matrix is a direction the matrix does not turn; the eigenvalue is how much that direction gets stretched (here ×3 and ×1).

### Visualization (canvas `c1`, 720×300)

Dual-panel arrow diagram: four arrows before the stretch (left) and the same four arrows after (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One Stretch, Four Arrows: Only the Diagonals Keep Their Direction".
- **Data (matrix `[[2,1],[1,2]]`):** before arrows `(1,0)`, `(0,1)`, `(1,1)`, `(1,−1)`; after arrows `(2,1)`, `(1,2)`, `(3,3)`, `(1,−1)`.
- **Left panel (before):** origin x=185, y=170, unit = 42px; thin `#e5e9ef` x/y axis lines through the origin; 3px arrows with 7px filled heads from the origin: blue `#2a78d6` to (1,0), violet `#4a3aa7` to (0,1), green `#008300` to (1,1), orange `#d95926` to (1,−1); each tip labeled with its coordinates 12px in its own color; caption 12px `#444` bottom "before: four unit arrows".
- **Right panel (after):** origin x=545, y=170, unit = 26px; same axis lines; faded 2px `#bbb` copies of the four before-arrows; solid 3px after-arrows in the same colors: blue to (2,1), violet to (1,2), green to (3,3), orange to (1,−1); green bold 13px annotation near the green tip "(1,1) → (3,3): ×3, same direction"; orange bold 12px annotation "(1,−1) untouched: ×1"; caption "after: new x = 2x + y, new y = x + 2y".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Checking the Special Arrows by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The matrix** — A = [[2, 1], [1, 2]]; multiplying means new x = 2x + y and new y = x + 2y
- **Test (1, 1)** — new x = 2·1 + 1 = 3, new y = 1 + 2·1 = 3; (3, 3) = 3 × (1, 1) — eigenvalue 3
- **Test (1, −1)** — new x = 2 − 1 = 1, new y = 1 − 2 = −1; (1, −1) = 1 × (1, −1) — eigenvalue 1
- **Test (1, 0)** — new x = 2, new y = 1; (2, 1) is no multiple of (1, 0) — not an eigenvector
- **Any length works** — (2, 2) maps to (6, 6), still ×3; only the direction is the eigenvector

*Example (italic):* Every line above is pencil arithmetic: multiply the arrow through A, then ask "is the output just a scaled copy of the input?"

**Key point:** The eigenvector test is one multiplication and one question: does A·v equal λ·v? If the output is a plain multiple of the input, you found one.

### Visualization (canvas `c2`, 720×300)

Three mini-panels, one hand check per panel: two passes and one fail, each showing the input arrow, the output arrow, and the arithmetic.

- **Title (bold 15px, `#1a5276`, top center):** "Three Hand Checks Against A = [[2, 1], [1, 2]]".
- **Data:** checks `(1,1) → (3,3)` pass λ=3; `(1,−1) → (1,−1)` pass λ=1; `(1,0) → (2,1)` fail.
- **Panels:** three equal panels with origins at x=125, x=360, x=595, all at y=150, unit = 28px; thin `#e5e9ef` axis lines through each origin; panel headings bold 12px `#444` at y=48: "A·(1,1)", "A·(1,−1)", "A·(1,0)".
- **Panel 1 (pass):** dashed 2px `#999` input arrow to (1,1); solid 3px green `#008300` output arrow to (3,3); green bold 13px verdict below at y=245 "= 3·(1,1)  ✓  λ = 3".
- **Panel 2 (pass):** dashed input arrow to (1,−1); solid 3px aqua `#199e70` output arrow drawn on top to (1,−1); aqua bold 13px verdict "= 1·(1,−1)  ✓  λ = 1".
- **Panel 3 (fail):** dashed input arrow to (1,0); solid 3px magenta `#d55181` output arrow to (2,1); magenta bold 13px verdict "= (2,1)  ✗  turned"; small magenta arc between the two arrows marking the turn.
- **Takeaway (bold 13px `#1a5276`, bottom center at y=285):** "eigenvector check: output = number × input?".

## A Data Cloud Has the Same Two Directions

**Tags:** `where it's used` (blue), `PCA` (orange)

- **A cloud of dots** — 20 data points, spread mostly along the up-right diagonal (illustrative)
- **Its matrix** — the cloud's covariance matrix is [[5, 4], [4, 5]] — a stretch matrix like our tool's
- **Same trick** — its eigenvectors are again (1, 1) and (1, −1), with eigenvalues 9 and 1
- **PC1** — the big eigenvector (1, 1) points along the cloud's long axis: PCA's first component
- **The split** — eigenvalues 9 vs 1 mean 90% of the spread lies along PC1, since 9 / (9 + 1) = 0.9

*Example (italic):* PCA on this cloud reports "keep direction (1, 1), it carries 90% of the variance" — that sentence is just the top eigenvector and its eigenvalue.

**Key point:** PCA is nothing new: build the covariance matrix, take its eigenvectors. The direction with the biggest eigenvalue is the cloud's long axis.

### Visualization (canvas `c3`, 720×300)

Scatter plot of the 20-point cloud with the two eigenvector arrows drawn from its center: a long PC1 arrow along the diagonal and a short PC2 arrow across it.

- **Title (bold 15px, `#1a5276`, top center):** "PCA = Eigenvectors of the Covariance Matrix (illustrative)".
- **Data (20 points, hardcoded):** `[(-4.1,-2.7), (-3.8,-1.8), (-2.3,-3.3), (-2.5,-1.1), (-1.0,-2.8), (-1.8,-0.4), (-0.3,-1.7), (-1.0,0.4), (0.4,-1.0), (-0.3,0.7), (1.0,-0.5), (0.3,1.7), (1.9,0.1), (1.1,2.5), (2.7,0.9), (1.8,3.2), (3.4,1.6), (2.5,3.9), (4.0,2.6), (3.4,4.2)]`.
- **Layout:** single centered panel, origin x=360, y=160, unit = 24px; thin `#e5e9ef` axis lines through the origin; points as 4px dots fill `rgba(42,120,214,0.55)`.
- **PC1 arrow:** solid 4px green `#008300` from the origin to 3 units along (1,1)/√2, i.e. tip at (2.12, 2.12) in data units, 8px head; green bold 13px label "PC1 = eigenvector (1,1), λ = 9".
- **PC2 arrow:** solid 3px orange `#d95926` from the origin to 1 unit along (−1,1)/√2, i.e. tip at (−0.71, 0.71), 7px head; orange bold 12px label "PC2 = (−1,1), λ = 1".
- **Annotation (bold 13px `#1a5276`, right side):** "90% of the spread lies along PC1 (9 / (9+1))".
- **Caption (12px `#444`, bottom center):** "covariance ≈ [[5, 4], [4, 5]] — same eigenvectors as the stretch tool".

## Shrinks, Flips, and Spins

**Tags:** `common mistake` (red), `edge cases` (orange)

- **Shrink counts too** — eigenvalue 0.5 means the direction is kept but halved; still an eigenvector
- **Negative flips** — eigenvalue −1 reverses the arrow along the same line; the line is unchanged
- **Spins have none** — a 90° rotation turns every arrow, so it has no real eigenvector at all
- **Length is free** — (1, 1) and (2, 2) name the same eigenvector; only the direction is the answer
- **Zero kills** — eigenvalue 0 flattens its direction to nothing; the matrix loses that information

*Example (italic):* A student rejected eigenvalue 0.5 as "not really stretching" — but any kept direction qualifies, whether stretched, shrunk, or flipped.

**Common mistake:** Expecting every eigenvalue to enlarge. "Eigen" only promises the direction survives — the value can stretch (3), keep (1), shrink (0.5), flip (−1), or kill (0) it.

### Visualization (canvas `c4`, 720×300)

Three mini-panels showing a shrink, a flip, and a rotation: the first two keep a direction, the third keeps none.

- **Title (bold 15px, `#1a5276`, top center):** "Eigenvalues Can Shrink or Flip — Rotations Have None".
- **Data:** shrink `[[0.5, 0], [0, 2]]` sends (1,0) → (0.5, 0), λ = 0.5; flip `[[−1, 0], [0, 1]]` sends (1,0) → (−1, 0), λ = −1; rotation `[[0, −1], [1, 0]]` sends (1,0) → (0, 1), no real eigenvector.
- **Panels:** origins at x=125, x=360, x=595, all at y=155, unit = 55px; thin `#e5e9ef` axis lines; panel headings bold 12px `#444` at y=48: "shrink", "flip", "90° spin".
- **Panel 1 (shrink):** dashed 2px `#999` input arrow to (1,0); solid 3px blue `#2a78d6` output arrow to (0.5, 0) drawn 6px above the axis to stay visible; blue bold 13px verdict at y=245 "λ = 0.5: kept, halved".
- **Panel 2 (flip):** dashed input arrow to (1,0); solid 3px yellow `#c98500` output arrow to (−1, 0); yellow bold 13px verdict "λ = −1: kept line, flipped".
- **Panel 3 (spin):** dashed input arrow to (1,0); solid 3px red `#e74c3c` output arrow to (0,1); small red arc from the input tip to the output tip; red bold 13px verdict "every arrow turns: none".
- **Takeaway (bold 13px magenta `#d55181`, bottom center at y=285):** "eigen promises a kept direction, not a bigger arrow".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Arrows are drawn as a 3px line plus a filled triangular head; a small shared `arrow(ctx, x0, y0, x1, y1, color, width)` helper keeps the four canvases consistent. All coordinates above are in data units around each panel's origin, converted by `px = ox + u·x`, `py = oy − u·y`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
