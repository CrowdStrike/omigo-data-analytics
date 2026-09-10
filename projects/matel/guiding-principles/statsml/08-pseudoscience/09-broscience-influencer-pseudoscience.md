# Broscience / Influencer Pseudoscience

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Broscience / Influencer Pseudoscience — Pseudoscience in Data Analysis

**Subtitle:** Platform visibility + confidence mistaken for expertise

## Looks + Confidence + Platform = "Expert" (Without Evidence)

- **Fitness broscience:** "I'm muscular → my routine works → do what I do." Missing: genetics (high testosterone responder), age (22, peak anabolic state), undisclosed PEDs, and photographic deception (pump, lighting, angles, filters). The result precedes the method — they look good despite their routine, not because of it.
- **Diet influencer:** "I lost 50 lbs eating only [X]!" But any caloric deficit works: success gets attributed to the specific restriction when the mechanism is total calories, and the restriction of X merely created the deficit incidentally. "Eat less" isn't marketable; "secret food hack" is.
- **Business influencer:** "Wake 4am, cold shower, journal, meditate — my morning routine made me a millionaire." They were already wealthy (inheritance, connections, luck); successful people have the luxury to optimize mornings, so the causation is reversed.
- **Tech influencer:** "Learn these 5 languages and get a $300K job." Survivorship: the languages are 5% of the equation; the connections, degree, city, luck, and interview skills that make up the other 95% aren't filmable content.

**The mechanism:** Platform algorithms reward confidence, not accuracy — hedged, nuanced content performs 10× worse than confident proclamations, so the incentive structure selects for pseudoscience over careful science.

### Visualization (canvas `c1`, 720×340)

Text diagram contrasting the real causal chain with the claimed one.

- **Title (bold 17px `#1a5276`, top center):** "Broscience: Outcome Precedes Method (Reverse Causation)"
- **Lines (17px, left-aligned at x=50):**
  - `#333`, y=60: "Reality:   Genetics + Age + Drugs → Looks good → Adopts routine → Claims routine caused it"
  - `#333`, y=90: "Claimed:   Routine → Results (\"do what I do!\")" — with a red `#e74c3c` strikethrough line (width 2) from x=50 to x=400 at y=100 under/through the claimed chain.
  - bold `#e74c3c`, y=130: "The cause (genetics) is invisible. The correlation (routine) is visible."
  - bold `#e74c3c`, y=160: "Platform algorithm: confidence > accuracy. Nuance gets 0 views."
  - `#555`, y=190: "The incentive structure SELECTS for pseudoscience and AGAINST honest science."

### Visualization (canvas `c2`, 720×300)

Scatter plot: platform visibility vs actual evidence quality, showing an inverse relationship via two clusters.

- **Title (bold 17px `#1a5276`, top center):** "The Visibility / Evidence Inversion"
- **Axes:** origin at (80, h−35), width w−140, height h−75; strokes `#2c3e50` width 1.5. X-axis label centered below: "Actual Evidence Quality" with "Low" at left end and "High" at right end. Y-axis label rotated vertical at left: "Platform Visibility / Followers".
- **Broscience cluster (top-left), 6px dots, fill `rgba(231,76,60,0.6)`, normalized (x, y) points:** (0.1, 0.85), (0.15, 0.9), (0.08, 0.75), (0.2, 0.8), (0.12, 0.95), (0.25, 0.7), (0.18, 0.88), (0.22, 0.92).
- **Careful content cluster (bottom-right), 6px dots, fill `rgba(39,174,96,0.6)`, points:** (0.75, 0.15), (0.8, 0.1), (0.85, 0.2), (0.7, 0.12), (0.9, 0.08), (0.78, 0.18), (0.82, 0.25), (0.88, 0.14).
- **Cluster labels (bold 18px):** "Confident Broscience" in `#e74c3c` near the red cluster (left-aligned at 0.25 width, 0.75 height); "Careful, Nuanced Content" in `#27ae60` near the green cluster (right-aligned at 0.68 width, 0.05 height).
- **Insight (bold 18px `#c0392b`, centered above the plot):** "Algorithms select FOR confidence, AGAINST nuance."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` with left `<td>` (40%) holding `.obj-title`, a `<ul>` of bullets, and a closing `<p>`; right `<td>` (60%, centered) holding the two canvases stacked.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark red `#c0392b`, grays `#555`/`#666`, dark slate `#2c3e50`.
- In regenerated HTML, any card links use `.html` extensions.
