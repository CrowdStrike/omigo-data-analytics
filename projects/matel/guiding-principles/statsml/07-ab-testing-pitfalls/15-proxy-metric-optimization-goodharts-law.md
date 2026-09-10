# Proxy Metric Optimization (Goodhart's Law)

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Proxy Metric Optimization (Goodhart's Law) — A/B Testing Pitfalls

**Subtitle:** Design Flaw — Clicks up! Because users are confused and clicking desperately.

## Section 1: Optimizing the Wrong Signal

- +20% CTR! But: users clicking because UI is CONFUSING, not engaging. Desperately searching. Downstream: purchase DOWN, satisfaction DOWN, retention DOWN.
- "Time on site up!" Site is SLOWER. "Page views up!" Navigation broken, 5 clicks for what took 1.
- Optimized proxy (clicks) at expense of actual goal (revenue/satisfaction).

**Correct approach:** Measure FURTHEST downstream metric. Clicks mean nothing without conversion. Conversion meaningless without retention. Define metric hierarchy BEFORE test.

**The tell:** Short-term proxies improve while long-term outcomes degrade. The proxy measures frustration, not value.

### Visualization (canvas `c1`, 720×340)

Three-level funnel: clicks up at the top while conversions and retention degrade downstream.

- **Funnel (centered at x=360):** three trapezoids, each 45px tall, top edges at y=25, 85, 145; each level's top width matches 85% of the level above (first level full width), bottom width = 85% of its own width. Fill = level color + `33` alpha suffix; stroke = level color, width 2. Bold 17px centered label inside each:
  - Level 1: "Clicks +20%", green `#27ae60`, width 280, with a "✓" to the right of the trapezoid.
  - Level 2: "Conversions -5%", orange `#e67e22`, width 200, with a "✗" to the right.
  - Level 3: "Retention -12%", red `#e74c3c`, width 130, with a "✗" to the right.
- **Side annotations (bold 18px, left-aligned):** green "← Optimized HERE" at (520, 50) pointing at the top level; red "← Should optimize HERE" at (450, 172) pointing at the bottom level.
- **Bottom labels (16px gray `#666`, centered at x=360):** "Proxy improves while true goal degrades" at y=215 and "\"When a measure becomes a target, it ceases to be a good measure\"" at y=232.

## Section 2: Real Example: The Bing Bug That "Made Money"

- Ron Kohavi tells the story of a Bing experiment where a bug made the search results noticeably worse, yet the two headline numbers — searches per user and ad revenue — both went UP.
- Users were not happier; they had to re-type and re-search because the first answers were bad, and every extra search showed more ads. The metrics were literally rewarding failure.
- The lesson Bing drew is that a search engine's success metric must capture whether people find things quickly and keep coming back, not how many queries and ad views they rack up this week.

### Visualization (canvas `c2`, 720×300)

Diverging bar chart around a zero line: quality drops below while two vanity metrics rise above.

- **Title (bold 17px `#2a2a2a`, centered at (360, 26)):** "One Bing Bug, Three Moving Metrics".
- **Zero line:** thin gray `#999` horizontal line from x=70 to x=650 at y=155, with 14px gray `#666` label "before bug" to its right.
- **Bar 1 (below the line):** at x=110, 110×75px downward; fill `rgba(231,76,60,0.35)`, stroke `#e74c3c` width 2; bold 16px red "WORSE" inside the bar and "Result quality" below it.
- **Bar 2 (above the line):** at x=310, 110×60px upward; fill `rgba(230,126,34,0.35)`, stroke `#e67e22` width 2; bold 16px orange "UP" inside; label "Searches / user" in `#333` above the bar; 14px gray "(users re-searching)" below the zero line.
- **Bar 3 (above the line):** at x=500, 110×72px upward; same orange fill/stroke; bold "UP" inside; label "Ad revenue" in `#333` above; 14px gray "(more ads seen)" below the zero line.
- **Takeaway (bold 16px red, centered at (360, h−14)):** "If a bug can push your success metric up, you picked the wrong metric".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- **Links:** none on this page; if this spec is linked from a grid, regenerated HTML card links use `.html` extensions.
