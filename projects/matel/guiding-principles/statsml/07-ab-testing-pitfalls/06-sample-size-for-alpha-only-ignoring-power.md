# Sample Size for Alpha Only (Ignoring Power)

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Sample Size for Alpha Only — A/B Testing Pitfalls

**Subtitle:** Statistical Sin — Control false positives but not false negatives. Miss real effects 60%+ of the time.

## Section 1: The Problem

- Sample size calculation has TWO inputs: α (false positive rate, typically 0.05) and β (false negative rate, typically 0.2, giving 80% power). Most teams only think about α.
- They set α=0.05 and pick a "reasonable" sample size without computing power. Result: 60% chance of MISSING a real effect.
- **Power depends on:** sample size, effect size, and variance. If you don't specify the effect size you want to detect, you CANNOT compute required sample.
- **The asymmetry:** Companies carefully guard against false positives (shipping bad features) but have NO protection against false negatives (killing good features). The cost of a missed opportunity is invisible.

**Correct approach:** Before the test: "We want to detect a 3% lift with 80% power at α=0.05." This gives a SPECIFIC required N. If you can't reach that N → don't run the test.

**The tell:** Ask "what's the power of this test?" If blank stare → they only computed for α. Ask "what's the smallest effect you can reliably detect?" If much larger than a realistic effect size → the test is designed to fail.

### Visualization (canvas `c1`, 720×340)

2×2 confusion-matrix grid of hypothesis-test outcomes, highlighting the uncontrolled Type II cell.

- **Grid:** rectangle at x=60, y=30, width 600, height 170, split into 2 columns × 2 rows (each cell 300×85); grid lines gray `#999`, width 1.5.
- **Column headers (bold 16px, `#1a5276`, centered above grid):** "H₀ true (no effect)" over the left column, "H₁ true (real effect)" over the right column.
- **Row headers (rotated -90°, bold 16px `#1a5276`, at left edge):** "Reject" for the top row, "Fail to reject" for the bottom row.
- **Cell [top-left]** (reject when H₀ true): green `#27ae60`, bold 17px "α = 5%" with 16px line "(everyone controls this)" below.
- **Cell [top-right]** (reject when H₁ true): blue `#1a5276`, bold 17px "Power = 40%" with 16px "(correct detection)" below.
- **Cell [bottom-left]** (fail to reject when H₀ true): blue `#1a5276`, bold 17px "95% correct" with 16px "(true negative)" below.
- **Cell [bottom-right]** (fail to reject when H₁ true — the problem): cell interior filled with a light red wash (`#e74c3c` at globalAlpha 0.15 over white); text red `#e74c3c`, bold 17px "β = 60%!!" with bold 18px "(nobody controls this)" below.
- **Caption (bottom center, italic 14px `#666`):** "You guard against false positives but let false negatives run at 60%. You are DESIGNED to miss real effects."

## Section 2: Real Example: The "No Harm" Celebration

- Ron Kohavi, who led experimentation at Microsoft, describes a common anti-pattern: a team ships a change, the test comes back "not significant," and everyone celebrates that the change "did no harm."
- The problem is that a small test simply cannot see small losses. A result that reads as "no difference" can easily be hiding a real 2% revenue drop, because the test never had enough users to notice it (this ability to notice is called statistical power).
- The fix is to ask up front "how small a loss do we need to be able to see?" and only trust a flat result if the test was big enough to have caught a loss of that size.

### Visualization (canvas `c2`, 720×300)

Two horizontal confidence intervals on a shared revenue-change axis, showing a wide underpowered interval vs a narrow properly-powered one around the same true effect.

- **Title (bold 17px `#2a2a2a`, centered at x=360, y=28):** "An Underpowered "Neutral" Result Can Hide a Real Loss".
- **X-axis:** horizontal gray `#999` line at y=225, mapping values -6 to +4 onto x = 80 + (v+6)/10×580; tick marks and 14px `#666` labels at -6%, -4%, -2%, 0%, +2%, +4% (positive values prefixed "+"); axis title "measured revenue change" centered below at y=267.
- **Zero line:** vertical dashed gray `#999` line (dash 4/3, width 1) at x for 0, from y=48 to y=225, labeled "no change" above in 14px `#666`.
- **Small-test interval (y=105):** orange `#e67e22` line width 4 from -5% to +1%, with 16px-tall end caps (width 2); filled orange dot (radius 6) at -2% (the true effect). Bold 15px orange label above: "Small test: "not significant" — range still includes a real loss".
- **Big-test interval (y=180):** red `#e74c3c` line width 4 from -3% to -1%, same end caps; filled red dot (radius 6) at -2%. Bold 15px red label above: "Properly sized test: the same 2% loss is now clearly visible".
- **Takeaway (bottom center, italic 14px `#666`):** ""Not significant" only means the test could not see it — the loss was there all along".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; paragraphs 0.95em `#333`; lists 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
