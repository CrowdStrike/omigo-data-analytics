# Random Train/Test Split on Time-Ordered Data

**Page type:** detail page (two card-sections, each an h2 + two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Random Train/Test Split on Time-Ordered Data

**Subtitle:** Be aware of what data goes into training vs testing — make conscious decisions about temporal structure

## The Anti-Pattern

Shuffling time-ordered data randomly mixes future observations into the training set. The model learns patterns that haven't occurred yet at prediction time, inflating performance by 10-30% compared to a proper temporal split.

- Financial datasets — stock prices, transaction volumes
- User behavior — session logs, conversion funnels
- IoT sensors — equipment degradation, temperature readings
- Clinical events — patient vitals, treatment outcomes

**Key point (red left border):** A model that uses tomorrow's data to predict today is not a forecaster — it's a lookup table that fails in production.

### Visualization (canvas `c1`, 720×300)

Timeline diagram: train and test dots scattered randomly across a Jan–Dec timeline, with a highlighted future train point.

- **Title (bold 13px, top center, red `#e74c3c`):** "Random shuffle mixes past and future"
- **Timeline:** horizontal line `#2c3e50` width 2 at y = h*0.5, from x=60 to x=w-40, filled right arrowhead; tick marks and 11px `#666` month labels "Jan" through "Dec" evenly spaced (y+20).
- **Dots:** 24 dots evenly spaced along the timeline, alternating vertical offsets -25/-40, radius 6; each colored by a seeded PRNG (seed 42, linear congruential `seed*1103515245+12345 & 0x7fffffff`, train if rand > 0.4): train `#1a5276`, test `#e67e22` — so past and future are interleaved.
- **Highlight:** dot index 20 (near the end), if train, gets a red `#e74c3c` circle outline (radius 12, width 2), a short red vertical arrow, and a bold 12px red label "Future data in train!" above it.
- **Legend (12px, bottom left, y ≈ h-30):** blue dot `#1a5276` + "Train (scattered randomly)"; orange dot `#e67e22` + "Test (scattered randomly)" (offset 200px right); legend text `#2c3e50`.

## The Design Pattern

Pay attention to what kind of data is used for training and testing. Be aware of the temporal structure and make conscious decisions — don't let a default shuffle decide for you.

- Ask: does this data have a time dimension? If yes, decide explicitly how to handle it
- Temporal split is one option — train on past, test on future
- Expanding/sliding window CV for time-aware cross-validation
- Document your choice and why — the decision itself matters more than any one rule

**Key point (red left border):** The anti-pattern isn't "using random split" — it's doing it unconsciously, without asking whether the data's structure demands something else.

### Visualization (canvas `c2`, 720×300)

Timeline diagram: clean temporal split — all train dots left of a split boundary, all test dots right.

- **Timeline:** same Jan–Dec timeline as `c1` (line `#2c3e50` at y = h*0.5, x=60 to w-40, arrowhead, month ticks/labels).
- **Split boundary:** vertical dashed green line (`#27ae60`, dash 6/4, width 2) at month position 7.5/11 (between Aug and Sep), from y-70 to y+40; bold 12px green label "SPLIT" centered above it.
- **Train dots:** 16 blue `#1a5276` dots (radius 6, alternating offsets -25/-40) evenly spaced left of the split.
- **Test dots:** 8 orange `#e67e22` dots (radius 6, alternating offsets) evenly spaced right of the split.
- **Zone labels (bold 13px, centered under each half at y ≈ h-50):** blue "TRAIN (past)"; orange "TEST (future)".
- **Caption (12px green `#27ae60`, bottom center, y ≈ h-20):** "Clean temporal separation — no future leakage"

## Regeneration instructions

- **Layout:** two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"), each with an `h2` and a `table.layout` (width 100%, border-collapse) containing one row: `td.text-col` (45%) with paragraph + `ul` + `.key-point`, `td.viz-col` (55%) with the canvas. (On this page the `.key-point` comes after the bullet list, and there is no `.example` label.)
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius; `.key-point` background `#f8f9fa`, 3px red `#e74c3c` left border, padding 8px 12px, 0.9rem; `ul` 0.92rem. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; CSS width 100%. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions.
