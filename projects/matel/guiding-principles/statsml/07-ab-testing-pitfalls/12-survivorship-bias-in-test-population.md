# Survivorship Bias in Test Population

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Survivorship Bias in Test Population — A/B Testing Pitfalls

**Subtitle:** Design Flaw — Only measuring users who STAYED. Ignoring the ones you drove away.

## Section 1: Differential Attrition Corrupts Results

- Treatment has worse UX → 10% leave DURING test. Remaining 90% are tolerant/engaged. Their metrics look great. You declare win based on survivors — ignoring the 10% you lost.
- Differential attrition: one arm loses more users → populations non-comparable even though they started random.
- Intent-to-treat (ITT): include ALL randomized users regardless of whether they "completed" the experience.

**Correct approach:** Monitor arm sizes at END vs START. If treatment lost significantly more → compromised.

**The tell:** Are arm sizes equal at END? If treatment has fewer → survivorship bias in the measured effect.

### Visualization (canvas `c1`, 720×340)

Before/after dot-grid diagram: both arms start at 100 users; at the end, treatment has 90 and 10 red dots have walked away.

- **START section (left):** header "START" in bold 17px blue `#1a5276` at (120, 20).
  - Control start: label "Control: 100" (16px, centered at (70, 45)); a 10×10 grid of blue `#1a5276` dots (radius 3, 9px horizontal / 12px vertical spacing) starting at (30, 55).
  - Treatment start: label "Treatment: 100" (16px blue, centered at (170, 45)); a 10×10 grid of green `#27ae60` dots starting at (130, 55).
- **Middle:** bold 20px gray `#666` "→" at (280, 110) with 16px "Test runs..." at (280, 130).
- **END section (right):** header "END" in bold 17px blue at (500, 20).
  - Control end: label "Control: 100" (16px, centered at (400, 45)); 10×10 grid of blue dots starting at (360, 55).
  - Treatment end: label "Treatment: 90" (16px green, centered at (530, 45)); only 9 rows × 10 columns of green dots starting at (490, 55).
- **Departed users:** red `#e74c3c` label "10 left" at (610, 80); 10 red dots in a 5×2 cluster near (600, 90); a short red line arrow from (615, 125) to (640, 140) with red 16px text "(gone)" at (640, 155).
- **Bottom label (bold 16px red, centered at (360, 225)):** "Measuring survivors ≠ measuring treatment effect".

## Section 2: Real Example: WWII Bomber Armor (Abraham Wald)

- In World War II, the military mapped bullet holes on bombers that returned from missions and planned to add armor where the holes clustered. Statistician Abraham Wald pointed out the flaw: they were only studying planes that made it home.
- Planes hit in the engines mostly never returned, so the "clean" spots on the surviving planes actually marked the deadliest places to be hit. Wald's advice was to armor exactly where the returning planes had NO holes.
- The modern web version is judging a redesign only on users who completed onboarding — the people your change drove away never appear in the data, just like the planes that never came back.

### Visualization (canvas `c2`, 720×320)

Top-view bomber schematic with red bullet holes on survivable areas and green dashed circles on the fatal (hole-free) spots.

- **Title (bold 17px `#2a2a2a`, centered at (360, 26)):** "Returning Bombers: Holes Cluster Where Hits Were Survivable".
- **Airframe (all shapes stroke `#1a5276` width 2, centerline cy=160):**
  - Two trapezoidal wings, fill `rgba(26,82,118,0.35)`: upper wing from (300, cy−14)–(400, cy−14)–(440, cy−95)–(345, cy−95); lower wing mirrored below (cy+14 to cy+95).
  - Two tail fins, same fill: upper from (530, cy−10)–(560, cy−10)–(575, cy−50)–(548, cy−50); lower mirrored.
  - Fuselage: ellipse centered (360, cy), radii 215×20, fill `rgba(26,82,118,0.2)`.
- **Bullet holes:** 14 solid red `#e74c3c` dots (radius 4) on wings, rear fuselage, and tail at: (370, cy−55), (395, cy−75), (352, cy−40), (412, cy−50), (365, cy+60), (400, cy+78), (347, cy+45), (415, cy+65), (470, cy−6), (495, cy+7), (515, cy−4), (455, cy+9), (560, cy−35), (558, cy+32).
- **Fatal spots:** three green `#27ae60` dashed circles (dash 6/4, width 2.5): engines at (325, cy−42) and (325, cy+42) radius 22, cockpit at (185, cy) radius 25.
- **Labels:** bold 15px green, left-aligned at (60, cy−90): "no holes = fatal → armor HERE"; 15px red, right-aligned at (690, cy+90): "holes = survivable hits".
- **Takeaway (bold 16px red, centered at (360, h−14)):** "The missing planes carried the missing data".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- **Links:** none on this page; if this spec is linked from a grid, regenerated HTML card links use `.html` extensions.
