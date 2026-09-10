# Wrong Randomization Unit

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Wrong Randomization Unit — A/B Testing Pitfalls

**Subtitle:** Design Flaw — Randomize by pageview but analyze by user. Inflated N, deflated variance.

## Section 1: Unit Mismatch Inflates Precision

- Randomize at page-view level (same user sees both A and B). Analyze as independent observations. 100 users × 50 pageviews = 5000 "observations."
- Variance estimate wrong by 50×. Everything "significant" because pretending 5000 independent when you have 100 correlated clusters.
- Variant: device-level randomization but users have multiple devices. Treatment bleeds.
- Under-estimating variance → over-estimating significance. "p=0.001" is actually p=0.15 accounting for clustering.

**Correct approach:** Randomize AND analyze at same unit (user). Cluster-robust standard errors if clustered.

**The tell:** Ask "at what level was randomization done?" and "does each unit appear once in the analysis?" If answers diverge → inflated precision.

### Visualization (canvas `c1`, 720×340)

Side-by-side comparison: 100 real clustered units on the left vs 5000 pretend-independent observations on the right.

- **Left panel:** 16px `#1a5276` heading centered at (150, 20): "100 real units". A 10×10 grid of user circles starting at (60, 40), spacing 18px: each circle radius 7, fill `rgba(26,82,118,0.2)`, stroke `#1a5276` width 1, with two tiny filled `#1a5276` dots (radius 1.5) inside each circle representing pageviews.
- **Middle annotation (red `#e74c3c`, centered at x=330):** bold 24px arrow glyph "→" at y=120, then 16px lines: "Pretending correlation" (y=145), "doesn't exist" (y=160), "→ fake precision" (y=175).
- **Right panel:** 16px `#1a5276` heading centered at (560, 20): "5000 fake units". 200 small dots (radius 3, fill `rgba(231,76,60,0.4)`) in a 20-column grid starting at (420, 35), x spacing 14, y spacing 18. Below, 16px red `#e74c3c` centered text at (560, 220): "... ×25 more rows ...".

## Section 2: Real Example: One Shopper in Both Arms

- Ron Kohavi's experimentation book documents this as a classic error: a website splits traffic per page-view, meaning the coin is flipped every time a page loads instead of once per person.
- The same shopper then sees the new checkout on Monday and the old one on Wednesday, so when analysts measure purchases per person, that shopper's behavior gets mixed into both groups at the same time.
- The results also look far more precise than they really are, because thousands of page loads are being counted as if they were thousands of independent people, when in truth it is the same few visitors returning again and again.

### Visualization (canvas `c2`, 720×300)

Weekday timeline showing one shopper assigned alternately to arms A and B on successive visits.

- **Title (bold 17px `#2a2a2a`, centered at x=360, y=28):** "Coin Flipped Per Visit: One Shopper Lands in Both Arms".
- **Shopper icon (left, centered at x=85, y=140):** stick-figure user drawn with `#1a5276` strokes width 3 — a head circle (radius 14, center y=118) and a shoulders arc (radius 26, lower half at y=168); bold 15px `#1a5276` labels below: "same" / "shopper".
- **Visit boxes:** 5 boxes, 88×70 each with 14px gaps, starting at x=170, y=105. Day labels above in 14px `#666`: Mon, Tue, Wed, Thu, Fri. Arm assignment per day: A, B, A, B, A. "A" boxes: fill `rgba(39,174,96,0.25)`, stroke `#27ae60`; "B" boxes: fill `rgba(26,82,118,0.2)`, stroke `#1a5276`; stroke width 2; each box holds a bold 24px centered letter "A" or "B" in the matching color.
- **Arrow:** thin gray `#999` line (width 1.5) with arrowhead from the shopper icon (x=120, y=140) to the first box.
- **Note (bold 15px red `#e74c3c`, centered at x=420, below the boxes):** "One person, counted in the A group AND the B group".
- **Takeaway (bottom center, italic 14px `#666`):** "Flip the coin once per person, not once per page — otherwise the two groups share the same people".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; paragraphs 0.95em `#333`; lists 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
