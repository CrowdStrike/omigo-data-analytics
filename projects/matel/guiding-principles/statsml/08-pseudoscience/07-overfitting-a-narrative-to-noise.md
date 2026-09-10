# Overfitting a Narrative to Noise

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Overfitting a Narrative to Noise — Pseudoscience in Data Analysis

**Subtitle:** Post-hoc story constructed after seeing random variation

## "The Data Clearly Shows..." (After Trying 50 Angles)

- **Market analysis:** "October is always bad for stocks" gets a post-hoc story about seasonal effects and election uncertainty, fitted to random variation. Some month must be worst; that it's October is noise, but the story makes it feel inevitable.
- **Sports:** A study finds teams wearing red win 55%, and the story "red = aggression = confidence" is born. Replication finds 49% — the original result was noise that happened to cross significance, but the narrative made it feel like discovered truth.
- **A/B test:** A two-week test, five segments analyzed, one shows p=0.04: "users aged 25-34 respond to variant B!" But five segments at p<0.05 means one expected false positive — you found the noise, and the narrative ("young users prefer modern design") makes it feel real.
- **The mechanism:** Humans cannot look at random data without finding patterns — random dots become constellations, random stock charts become "head and shoulders" formations. The pattern-finding is the bug, not the feature.

**Why it's pseudoscience:** The narrative was constructed after seeing a pattern that exists in this data but not in the underlying truth — fitting a story to noise is overfitting: perfect explanation of the training data, zero generalization to new data.

### Visualization (canvas `c1`, 720×300)

Scatter of uniformly random dots with a false "cluster" circled.

- **Title (bold 17px `#1a5276`, top center):** "Random Data → Human Brain Finds \"Pattern\" → Story Created"
- **Dots:** 60 points, 4px radius, fill `rgba(41,128,185,0.5)`, positions uniformly random (seeded PRNG, seed 42) over x ∈ [60, 660], y ∈ [50, 170].
- **Circled "cluster":** dashed red circle (`#e74c3c`, dash 4/3, width 2), center (350, 100), radius 50.
- **Label (bold red, centered at x=350, y=165):** "\"Clear cluster here!\""
- **Bottom line (`#555` 17px, centered, y=h-8):** "(Data is uniformly random. The \"cluster\" is your brain pattern-matching on noise.)"

### Visualization (canvas `c2`, 720×300)

Two side-by-side scatter panels: pattern in one dataset fails to replicate in the next.

- **Title (bold 17px `#1a5276`, top center):** "In-Sample vs Out-of-Sample"
- **Panels:** two 280×120 rectangles at y=35, separated by a 40px gap, centered horizontally; borders `#1a5276` width 2; bold 18px `#1a5276` panel titles centered at top: "This Dataset" (left), "Next Dataset" (right).
- **Scatter:** 40 points per panel, 3px radius, fill `rgba(26,82,118,0.5)`, seeded random positions (seed 42 left, seed 99 right) inside each panel.
- **Left panel annotation:** dashed red circle (`#e74c3c`, dash 4/3, width 2.5), center at panel x+140, y+65, radius 30; bold red 16px label "p = 0.03" below the circle near the panel bottom.
- **Right panel annotation:** same circle position/radius but gray dashed (`#bdc3c7`, width 2); gray `#7f8c8d` bold label "p = 0.71".
- **Arrow:** gray `#7f8c8d` horizontal arrow (line width 2 with filled triangle head) between the two panels at mid-height.
- **Bottom label (bold 18px `#c0392b`, centered, y=h-8):** "The pattern was in the NOISE, not the SIGNAL. It doesn't replicate."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` with left `<td>` (40%) holding `.obj-title`, a `<ul>` of bullets, and a closing `<p>`; right `<td>` (60%, centered) holding the two canvases stacked.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Random scatters use seeded linear-congruential PRNGs so renders are reproducible. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark red `#c0392b`, grays `#555`/`#7f8c8d`/`#bdc3c7`.
- In regenerated HTML, any card links use `.html` extensions.
