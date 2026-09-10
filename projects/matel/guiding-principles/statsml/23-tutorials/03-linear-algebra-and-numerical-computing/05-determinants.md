# Determinants

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Determinants

**Subtitle:** The determinant is one number that says how much a matrix stretches area or volume — and det = 0 means the matrix flattens everything and cannot be undone

## Stretching a Sticker

**Tags:** `core idea` (blue), `area scaling` (green), `linear transform` (orange)

- **The sticker** — a design app applies a transform matrix to a 1 cm × 1 cm square sticker
- **The stretch** — the matrix [[3, 0], [0, 2]] makes it 3 cm wide and 2 cm tall
- **One number** — the area went from 1 to 6, so this matrix's determinant is 6
- **Any shape** — every shape's area gets multiplied by the same 6, not just the square's
- **The definition** — the determinant is the area (or volume) scaling factor of a matrix

*Example (italic):* A logo of area 2 cm² pushed through the same matrix comes out with area 2 × 6 = 12 cm² — no need to redraw it to know that.

**Key point:** The determinant answers one question about a matrix: "by what factor does it scale area (2×2) or volume (3×3)?". Here the answer is 6.

### Visualization (canvas `c1`, 720×300)

Before/after panels: the 1×1 unit sticker (left) and the 3×2 stretched sticker (right), with an arrow labeled by the matrix between them.

- **Title (bold 15px, `#1a5276`, top center):** "One Sticker Through the Matrix [[3, 0], [0, 2]]".
- **Left panel (before):** light grid (1px `#e5e9ef`, 60px cells) from x=60 to x=240, y=60 to y=240; unit square with corners (0,0),(1,0),(1,1),(0,1) drawn at 60px per unit with origin at pixel (90, 230), fill `rgba(42,120,214,0.35)`, 2px blue `#2a78d6` outline; bold 13px blue label "area = 1" centered in the square; caption 12px `#444` "before: 1 cm × 1 cm" below at y=262.
- **Arrow (center):** 3px ink `#1a5276` horizontal arrow from x=255 to x=335 at y=150 with arrowhead; bold 12px ink label "[[3, 0], [0, 2]]" above it at y=132.
- **Right panel (after):** same-style grid from x=350 to x=680, y=60 to y=240; rectangle with corners (0,0),(3,0),(3,2),(0,2) at 60px per unit with origin at pixel (380, 230), fill `rgba(0,131,0,0.35)`, 2px green `#008300` outline; bold 13px green label "area = 6 = det" centered inside; caption "after: 3 cm × 2 cm".
- **Takeaway (bold 13px green, bottom center at y=292):** "the determinant is the area-scaling factor: 1 → 6".

## Computing It by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The recipe** — for a 2×2 matrix [[a, b], [c, d]], the determinant is ad − bc
- **A shear** — the matrix [[3, 1], [1, 2]] tilts the sticker into a slanted parallelogram
- **Plug in** — det = 3·2 − 1·1 = 6 − 1 = 5, so the slanted sticker has area 5
- **The columns** — the parallelogram's sides are the columns (3, 1) and (1, 2)
- **Cross term** — the −bc part is the tilt penalty: overlap between the two columns

*Example (italic):* Push the sticker's corner (1, 1) through the matrix and it lands at (3+1, 1+2) = (4, 3) — the far corner of an area-5 parallelogram.

**Key point:** For 2×2, det = ad − bc is a one-line hand computation. Bigger matrices use the same idea (signed volume of the columns), delegated to software.

### Visualization (canvas `c2`, 720×300)

Formula panel (left) plus the sheared parallelogram drawn from its column vectors (right).

- **Title (bold 15px, `#1a5276`, top center):** "det [[3, 1], [1, 2]] = 3·2 − 1·1 = 5".
- **Left panel (formula):** the matrix drawn as a 2×2 number grid at x=70–210, y=80–180, entries 3, 1 / 1, 2 in bold 16px `#2c3e50` with 1px `#999` brackets; blue `#2a78d6` diagonal line through 3 and 2 labeled bold 12px blue "a·d = 6"; magenta `#d55181` diagonal through 1 and 1 labeled bold 12px magenta "b·c = 1"; bold 14px ink result "det = 6 − 1 = 5" centered below at y=225.
- **Right panel (parallelogram):** light grid (1px `#e5e9ef`, 45px cells) from x=340 to x=680, y=45 to y=255; origin at pixel (370, 240), scale 45px per unit, y up; parallelogram with vertices (0,0), (3,1), (4,3), (1,2), fill `rgba(42,120,214,0.35)`, 2px blue outline; green `#008300` 3px arrow from (0,0) to (3,1) labeled bold 12px green "col 1 = (3, 1)"; orange `#d95926` 3px arrow from (0,0) to (1,2) labeled bold 12px orange "col 2 = (1, 2)"; bold 13px blue label "area = 5" at the parallelogram's center; 11px `#6b7280` dot label "(4, 3)" at the far vertex.
- **Caption (12px `#444`, bottom right):** "the columns are the sides; det is the signed area they span".

## det = 0: The Flattened Sticker

**Tags:** `where it's used` (blue), `singular matrix` (red), `no inverse` (orange)

- **A bad matrix** — [[2, 1], [4, 2]] has det = 2·2 − 1·4 = 4 − 4 = 0
- **Why zero** — its columns (2, 4) and (1, 2) point the same way, so they span no area
- **The collapse** — the whole sticker is squashed onto one line; area 1 becomes area 0
- **No undo** — many points land on the same spot, so no inverse matrix can separate them
- **In practice** — solving Ax = b, inverting a covariance matrix, and regression all fail here

*Example (italic):* The equations 2x + y = 5 and 4x + 2y = 10 are this matrix's rows — the second is the first doubled, so there is no single solution to find.

**Key point:** det = 0 is the definition of a singular matrix: it flattens space, destroys information, and has no inverse. Closeness to singular is measured by the condition number, not by a small det.

### Visualization (canvas `c3`, 720×300)

Before/after collapse: the unit sticker (left) squashed onto a single line segment (right) by the singular matrix.

- **Title (bold 15px, `#1a5276`, top center):** "det [[2, 1], [4, 2]] = 0: the Sticker Collapses to a Line".
- **Left panel (before):** light grid (1px `#e5e9ef`, 55px cells) from x=60 to x=250, y=60 to y=245; unit square corners (0,0),(1,0),(1,1),(0,1) at 55px per unit, origin at pixel (100, 230), fill `rgba(42,120,214,0.35)`, 2px blue `#2a78d6` outline; bold 13px blue "area = 1" inside; caption 12px `#444` "before" at y=265.
- **Arrow (center):** 3px ink arrow from x=265 to x=340 at y=150; bold 12px ink label "[[2, 1], [4, 2]]" above at y=132.
- **Right panel (after):** grid from x=360 to x=680, y=45 to y=245; origin at pixel (400, 235), scale 30px per unit, y up; red `#e74c3c` 4px line segment from (0,0) to (3,6) — the images of all four corners: (0,0), (2,4), (1,2), (3,6) drawn as 5px red dots on the segment with 11px `#6b7280` labels; dashed `#bdc3c7` outline (dash 4/3) of where the square's image "would be" is omitted — instead bold 13px red annotation to the right of the segment: "area = 0" and below it "every corner lands on the line y = 2x".
- **Takeaway (bold 13px red `#e74c3c`, bottom center at y=292):** "det = 0 = singular: squashed flat, no way to undo".

## The Two Things People Misread

**Tags:** `common mistake` (red), `orientation flip` (orange)

- **Negative det** — the mirror matrix [[0, 1], [1, 0]] has det = 0·0 − 1·1 = −1
- **Sign = flip** — the minus sign means orientation flipped; the area factor is still |−1| = 1
- **Size trap** — [[100, 0], [0, 0.01]] has huge entries but det = 100 · 0.01 = 1
- **What it hides** — det = 1 here, yet the sticker is stretched 100× one way, 100× thinner the other
- **Reading rule** — det measures net area change only, not entry size and not distortion

*Example (italic):* An analyst saw det = 1 for [[100, 0], [0, 0.01]] and called the matrix "harmless" — it was distorting one axis by a factor of 100 the whole time.

**Common mistake:** Treating the determinant as the matrix's "overall size". It is only the net area factor: a mirror scores −1, and a violently distorting matrix can score exactly 1.

### Visualization (canvas `c4`, 720×300)

Two panels: the mirror flip with det = −1 (left) and the big-entry matrix with det = 1 drawn as a long thin strip (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "det = −1 (a Flip) and det = 1 (a Violent Stretch)".
- **Left panel (mirror):** heading bold 12px `#444` "[[0, 1], [1, 0]] → det = −1" at x=175 centered, y=52; two 90px squares side by side: original at x=80–170, y=90–180 with 2px blue `#2a78d6` outline, fill `rgba(42,120,214,0.35)`, a bold 20px blue letter "R" inside; mirrored copy at x=210–300, y=90–180 with 2px orange `#d95926` outline, fill `rgba(217,89,38,0.35)`, the "R" drawn mirrored (ctx.scale(−1, 1)) in orange; small ink arrow between them; bold 12px orange annotation below at y=205: "flipped, area still × 1".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=40 to h-12.
- **Right panel (stretch):** heading bold 12px `#444` "[[100, 0], [0, 0.01]] → det = 1" centered at x=540, y=52; original 90px square at x=400–490, y=80–170, 2px blue outline, fill `rgba(42,120,214,0.35)`, bold 13px blue "area 1" inside; below it the transformed strip: a green `#008300`-outlined rectangle at x=395–675, y=215–221 (280px × 6px), fill `rgba(0,131,0,0.4)`, bold 12px green label "100 wide × 0.01 tall — area still 1 (drawn compressed)" above the strip at y=205; 11px `#6b7280` caption "illustrative: true shape is far too wide to draw to scale" at y=240.
- **Takeaway (bold 13px magenta `#d55181`, bottom center at y=290):** "det tells you the net area factor — nothing about flips it undid or distortion it hides".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
