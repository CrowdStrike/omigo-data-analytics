# Treatment Contamination — When Test Users Affect Control Users

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Treatment Contamination — A/B Testing Pitfalls

**Subtitle:** Design Flaw — Control users experience treatment through network effects.

## Section 1: Network Spillover Dilutes Both Arms

- **The hidden assumption:** A/B tests assume users don't affect each other — what one person sees changes only their own behavior, nobody else's. (Statisticians call this SUTVA.)
- Referral feature test. Treatment users send referrals to... control users. Control is now partially treated.
- Marketplace: treatment sellers lower prices → control buyers benefit from lower prices.
- Social networks: treatment users share differently → control users see different feeds.
- Effect is DILUTED (both arms partially treated) or appears where it shouldn't.

**Correct approach:** Cluster randomization (by region/market/network cluster). Measure contamination directly. Caveat: your real sample size becomes the number of clusters, not the number of users — so plan for a much bigger test.

**The tell:** "Can treatment affect control's experience?" If yes + individual randomization → the assumption is broken and the test is contaminated.

### Visualization (canvas `c1`, 720×340)

Two-circle diagram: Treatment and Control groups with red dashed contamination arrows crossing from A to B.

- **Circle A (Treatment):** center (220, 120), radius 80; fill `rgba(39,174,96,0.15)`, stroke `#27ae60` width 3. Title above at (220, 30): bold 17px green `#27ae60` "Treatment (A)". Inside: 8 solid green dots (radius 8) evenly spaced on a ring of radius 45 around the center.
- **Circle B (Control):** center (500, 120), radius 80; fill `rgba(26,82,118,0.1)`, stroke `#1a5276` width 3. Title above at (500, 30): bold 17px blue `#1a5276` "Control (B)". Inside: 8 solid blue `#1a5276` dots (radius 8) on a ring of radius 45.
- **Contamination arrows:** three horizontal red `#e74c3c` dashed lines (dash 5/3, width 2) from x=290 to x=420 at y=90, y=120, y=150, each ending with a small solid red triangular arrowhead pointing right into circle B.
- **Labels (centered at x=360):** bold 16px red "Treatment influences control" at y=185; regular 16px red "Your clean experiment has bleed-through" at y=205.

## Section 2: Real Example: Ride-Hailing Price Tests

- Lyft found that testing prices on individual riders breaks: treatment riders book more rides, which drains the shared pool of drivers that control riders also depend on.
- Control looks worse not because treatment is better, but because treatment literally took its drivers away — the comparison flatters the new feature.
- Their published fix is the switchback test: flip an entire city between A and B in alternating time blocks, so the two versions never compete for the same drivers at the same moment.

### Visualization (canvas `c2`, 720×300)

Switchback timeline: one city alternating between A and B time blocks.

- **Title (bold 17px `#2a2a2a`, centered at 360, 30):** "Switchback Test: Whole City Flips Between A and B".
- **Blocks:** 8 rectangles in a row starting at x=60, y=90, each 75px wide (drawn 71px with 4px gap) and 80px tall, alternating starting with A: A blocks fill `rgba(39,174,96,0.25)` stroke `#27ae60`; B blocks fill `rgba(26,82,118,0.2)` stroke `#1a5276`; each block has a bold 20px centered letter "A" or "B" in its stroke color.
- **Time axis:** thin gray `#999` horizontal line under the blocks (20px below) with a right-pointing open arrowhead; label below in 14px gray `#666`, centered at x=360: "time (e.g. 2-hour blocks)".
- **Caption (15px `#333`, centered at x=360, ~75px below blocks):** "Everyone in the city shares the same version at any moment — no arm can steal drivers from the other".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- **Links:** none on this page; if this spec is linked from a grid, regenerated HTML card links use `.html` extensions.
