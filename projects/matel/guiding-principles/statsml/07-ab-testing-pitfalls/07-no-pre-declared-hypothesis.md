# No Pre-Declared Hypothesis

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** No Pre-Declared Hypothesis — A/B Testing Pitfalls

**Subtitle:** Statistical Sin — No hypothesis upfront. After results: "We hypothesized X all along!"

## Section 1: The Problem

- The p-value ONLY has meaning if the hypothesis was declared BEFORE seeing data. If you look at results first, then "hypothesize" what you found — you're confirming that the data contains itself.
- **How it manifests:** Test runs. Results come in. "Oh interesting, segment X responded!" Hypothesis is now declared retroactively. The narrative makes it feel like a prediction. It wasn't.
- **Multiple hypotheses problem:** If you don't pre-declare your hypothesis, you can always find SOMETHING in the data to call your hypothesis. The data always has patterns — the question is whether they're real or noise.

**Correct approach:** Write hypothesis BEFORE the test. Publish it. Include: direction of effect, primary metric, expected magnitude, target population. If the result doesn't match — it's exploratory, not confirmatory.

**The tell:** Ask "what was the hypothesis BEFORE the test ran?" If it matches the result perfectly with no documentation of prior prediction → it was declared after the fact. Check chat/email timestamps.

### Visualization (canvas `c1`, 720×340)

Two horizontal process timelines contrasting the correct pre-registered flow (top, green) with the post-hoc flow (bottom, red).

- **Top timeline (correct):** header bold 16px green `#27ae60` centered above at y=20: "CORRECT (pre-registered)". Horizontal green line (width 2) at y=50 from x=80 to x=640 ending in a filled green arrowhead. Four green dots (radius 6) at x = 120, 260, 440, 580 with 16px `#333` step labels below: "Hypothesis", "Run test", "Result", "Conclusion". Green 16px sub-labels: "(declared first)" under "Hypothesis", "(may or may not match)" under "Result".
- **Bottom timeline (wrong):** header bold 16px red `#e74c3c` centered at y=120: "WRONG (post-hoc)". Horizontal red line (width 2) at y=150 from x=80 to x=640 with red arrowhead. Four red dots at the same x positions with 16px `#333` labels: "Run test", "See result", ""Hypothesize"", ""Confirmed!"". Red 16px sub-labels: "(what you found)" under "See result", "(after seeing data)" under ""Hypothesize"", "(always works!)" under ""Confirmed!"".
- **Caption (bottom center, italic 14px `#666`):** "Bottom path ALWAYS confirms. That is why it is not science."

## Section 2: Real Example: Google's 41 Shades of Blue

- Around 2009, Google famously tested 41 slightly different shades of blue for its links to see which one people clicked most, rather than starting from any idea about why one shade should work better than another.
- The test did crown a winning shade, reportedly worth a large amount of ad revenue, but it was widely criticized as testing without thinking — Google's own lead visual designer, Doug Bowman, publicly resigned over this culture of settling every design question with a data contest.
- The deeper issue is that with 41 options and no advance prediction, some shade was likely to look like a "winner" by luck alone, so a written reason stated beforehand (a hypothesis) is what separates learning from a lottery.

### Visualization (canvas `c2`, 720×300)

Row of 41 blue color swatches with one arbitrary "winner" highlighted.

- **Title (bold 17px `#2a2a2a`, centered at x=360, y=28):** "41 Shades of Blue — Every Option Tested, No Theory Behind Any".
- **Swatches:** 41 rectangles, each 13px wide × 60px tall with 2px gaps, starting at x=52, y=90. Swatch i is filled `rgb(50+round(i*2.2), 110+round(i*1.6), 255)` — a smooth gradient of near-identical blues.
- **Winner highlight:** swatch index 27 outlined in orange `#e67e22`, line width 3, 2px outside the swatch; above it a vertical orange arrow (width 2) pointing down at the swatch, with bold 15px orange label above: "the "winner"".
- **Labels below swatches (centered at x=360):** 15px `#1a5276` line: "41 nearly identical shades, all tested at once — no prediction about which should win, or why"; then 15px `#e74c3c` line: "Something was going to "win" this beauty contest no matter what".
- **Takeaway (bottom center, italic 14px `#666`):** "A hypothesis stated in advance is what turns a test into a question — otherwise it is a lottery draw".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; paragraphs 0.95em `#333`; lists 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
