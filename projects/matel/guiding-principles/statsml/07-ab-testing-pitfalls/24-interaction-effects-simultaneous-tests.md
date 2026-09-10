# Interaction Effects (Simultaneous Tests)

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Interaction Effects (Simultaneous Tests) — A/B Testing Pitfalls

**Subtitle:** Organizational — 5 tests running. Each claims credit independently. Total claimed > actual.

## Section 1: Organizational — 5 tests running. Each claims credit independently. Total claimed > actual.

- Test A: +2%. Test B: +3%. Test C: +1%. Claimed total: +6%. Actual when all live: +2%.
- Interactions (A hurts B, C negates A) never measured.
- 10 simultaneous → 45 pairwise, 120 three-way interactions. Nobody measures any.

**Correct approach:** Holdout (no-tests group) vs all-tests group = TRUE combined effect. Compare to sum of claims. Gap = interactions.

**The tell:** Sum all shipped test claims for quarter. Compare to actual metric movement. Claims 25% but actual 8% — interactions ate 17%.

### Visualization (canvas `c1`, 720×340)

Venn diagram of three overlapping test circles plus a claimed-vs-actual bar comparison on the right, on light gray background (`#f8f9fa`).

- **Venn circles:** three circles of radius 65 centered around (w×0.38, h×0.45) with offset 42 (A upper-left, B upper-right, C lower-center), stroke width 2:
  - A — fill `rgba(26,82,118,0.2)`, stroke `#1a5276`, bold 17px label "A: +2%" above-left in `#1a5276`.
  - B — fill `rgba(39,174,96,0.2)`, stroke `#27ae60`, bold 17px label "B: +3%" above-right in `#27ae60`.
  - C — fill `rgba(230,126,34,0.2)`, stroke `#e67e22`, bold 17px label "C: +1%" below in `#e67e22`.
- **Center overlap label (bold 18px red `#e74c3c`, at Venn center):** "cancel".
- **Right side (left-aligned at x=w×0.7):**
  - Bold 17px `#1a5276` label "Claimed: +6%" (y=50) above a 150×20 bar (y=58), fill `rgba(26,82,118,0.3)`, stroke `#1a5276`.
  - Bold 17px `#27ae60` label "Actual: +2%" (y=110) above a 50×20 bar (y=118), fill `rgba(39,174,96,0.3)`, stroke `#27ae60`.
  - 16px red annotation, two lines (y=165/180): "Gap = unmeasured" / "interactions (-4%)".
- **Bottom label (17px `#555`, centered, 10px above bottom):** "Claimed: 6%. Actual combined: 2%. Gap = unmeasured interactions."

## Section 2: Illustrative Example: The Blue-on-Blue Collision

- On a large website, one team tested making link text blue while another team, unaware, tested making the page background the same shade of blue. Users who happened to be assigned to both experiments got blue text on a blue background that they simply could not read.
- Each team's dashboard only showed its own experiment, so both teams saw a mysterious drop in one slice of their users and neither could explain where it came from. (Two changes interfering with each other like this is called an interaction effect.)
- Mature experimentation platforms now run automated checks that flag pairs of overlapping tests whose combined results look worse than either test does alone.

### Visualization (canvas `c2`, 720×300)

2×2 grid of condition boxes showing the four experiment combinations, on light gray background (`#f8f9fa`).

- **Title (bold 17px `#2a2a2a`, centered at y=28):** "Two Tests, Fine Alone — Broken Together".
- **Cells:** four 250×78 boxes in a 2×2 grid (30px horizontal gap, 18px vertical gap, grid horizontally centered, top y=52), stroke width 2; each cell has a bold 15px label line and a 14px sub line, both centered and colored to match the stroke:
  - "Neither test" / "normal page" — color `#1a5276`, bg `rgba(26,82,118,0.12)`.
  - "Test 1 only: blue link text" / "readable — small lift" — color `#27ae60`, bg `rgba(39,174,96,0.12)`.
  - "Test 2 only: blue background" / "readable — small lift" — color `#27ae60`, bg `rgba(39,174,96,0.12)`.
  - "Both: blue text on blue" / "links invisible — users lost" — color `#e74c3c`, bg `rgba(231,76,60,0.15)`.
- **Takeaway (15px `#555`, centered, 14px above bottom):** "Each team saw only its own test — neither dashboard could explain the drop."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`/`#555`.
