# Asymmetric Infrastructure

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Asymmetric Infrastructure — A/B Testing Pitfalls

**Subtitle:** Deliberate — Treatment on fast servers. Control on degraded legacy. You tested infra, not the feature.

## Section 1: Confounding Feature With Infrastructure

- Treatment runs on new fast servers. Control on legacy. "Treatment improved latency and conversion!" — because better INFRASTRUCTURE, not the feature. Feature and infra confounded.
- Variant: treatment gets bug fix control doesn't. Treatment has fewer ads. Treatment has splash screen delaying measurement (excluding impatient users).
- Sometimes accidental (feature only deployable on new infra). Sometimes deliberate.

**Correct approach:** EVERYTHING besides the feature must be identical between arms. Infra, latency, error rates, ad load — all same.

**The tell:** Check if anything besides the feature differs. Compare error rates, latency distributions, ad load between arms.

### Visualization (canvas `c1`, 720×340)

Diagram: two server stacks side by side — treatment on new hardware (green) vs control on legacy hardware (red) — running the same feature test.

- **Column headers (bold 17px, `#1a5276`, centered):** "Treatment" at x=180 y=20; "Control" at x=540 y=20.
- **Treatment stack (left):** 3 server boxes 140×35 at x=110, y=40+i×45 (i=0..2) — fill `rgba(39,174,96,0.2)`, stroke `#27ae60` width 2; each box has a green LED dot (radius 4, `#27ae60`) at x=125, box-center height.
- **Treatment labels:** bold 17px green `#27ae60` "10ms" at (180, 190); 16px "New hardware" at (180, 210).
- **Control stack (right):** 3 server boxes 140×35 at x=470, same rows — fill `rgba(231,76,60,0.15)`, stroke `#e74c3c` width 2; LED dots at x=485: first two red `#e74c3c`, third gray `#999` (off); boxes 2 and 3 have small "rust marks" — rectangles `rgba(231,76,60,0.3)` 20×3 at (500, sy+5) and 15×3 at (530, sy+25).
- **Control labels:** bold 17px red `#e74c3c` "80ms" at (540, 190); 16px "Legacy hardware" at (540, 210).
- **Middle:** gray `#666` 17px centered text "Same feature test" at (360, 80); thin gray bracket (`#666`, width 1) from (300,90) down to (300,140) across to (420,140) up to (420,90).
- **Bottom label (bold 16px red `#e74c3c`, centered):** "Testing infrastructure, not the feature" at (360, 232).

## Section 2: Real Example: Google and Bing Slow Themselves Down

- Google ran an experiment where it deliberately delayed its own search results by just 100 to 400 milliseconds — too brief to consciously notice — and users measurably ran fewer searches, with the drop growing the longer the delay stayed on.
- Bing published a similar experiment showing that adding a couple of seconds of delay cut revenue per user by several percent, even though the search results shown were completely identical.
- The lesson for A/B tests: if the treatment build carries extra logging or unoptimized code, it starts the race slower, and it can lose (or win) for speed reasons that have nothing to do with the feature itself.

### Visualization (canvas `c2`, 720×300)

Downward bar chart: search-volume drop from added delay, bars hanging below a zero baseline.

- **Title (bold 17px `#2a2a2a`, centered):** "Google Speed Experiment: Delay Alone Cut Searching" at (360, 26).
- **Zero baseline:** thin gray line (`#999`, width 1) from x=80 to x=640 at y=90, labeled 14px gray `#666` "normal speed" at (80, 80), left-aligned.
- **Bars (extend downward from baseline, scale: 0.6% = 130px):**
  - Bar 1 at x=200, width 120: -0.2% (height ≈ 43px) — fill `rgba(230,126,34,0.35)`, stroke `#e67e22` width 2; bold 17px orange value label "-0.2%" below the bar, 15px `#333` caption "+100 ms delay" below that.
  - Bar 2 at x=440, width 120: -0.6% (height 130px) — fill `rgba(231,76,60,0.35)`, stroke `#e74c3c` width 2; bold 17px red value label "-0.6%", 15px `#333` caption "+400 ms delay".
- **Y-axis meaning:** rotated vertical 15px `#1a5276` label "searches per user" centered around (50, 200).
- **Takeaway (bold 16px red `#e74c3c`, bottom center):** "Milliseconds move metrics — equalize speed between arms before crediting the feature".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; paragraphs 0.95em `#333`; bullets 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
