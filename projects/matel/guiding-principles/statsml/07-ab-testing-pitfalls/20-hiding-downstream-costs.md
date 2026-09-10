# Hiding Downstream Costs

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Hiding Downstream Costs — A/B Testing Pitfalls

**Subtitle:** Deliberate — Conversion +10%! Refunds +40%, support +25%, NPS -15. Only measured above waterline.

## Section 1: Deliberate — Conversion +10%! Refunds +40%, support +25%, NPS -15. Only measured above waterline.

- Aggressive upsell → conversion +10%. Ship! But: customers with buyer's remorse → 40% more refunds, 25% more support, lower NPS.
- A/B test only MEASURED conversion. Costs show up 30 days later in different team's dashboard.
- Person shipping KNOWS downstream effects but only measures the metric they own.
- Cross-team externalities aren't their problem. Their dashboard = green. Someone else's = red.

**Correct approach:** For every "win," ask: what are 2nd/3rd order effects? Who bears the cost? Include downstream metrics in test design.

**The tell:** Does the test measure ANY negative downstream metric? If only upside measured — externality-blind.

### Visualization (canvas `c1`, 720×340)

Iceberg diagram: small green tip above a dashed blue waterline (the measured win), large red mass below it (the hidden costs).

- **Background:** full-canvas fill `#f8f9fa`. Waterline at y = 40% of canvas height (y=136).
- **Iceberg tip (above water):** triangle from (w/2−80, waterY) up to (w/2, 20) down to (w/2+80, waterY) — fill `rgba(39,174,96,0.3)`, stroke `#27ae60` width 2.
- **Iceberg body (below water):** quadrilateral (w/2−80, waterY) → (w/2−160, h−20) → (w/2+160, h−20) → (w/2+80, waterY) — fill `rgba(231,76,60,0.2)`, stroke `#e74c3c` width 2.
- **Waterline:** dashed blue line (`#2980b9`, dash 6/4, width 2) from x=30 to x=w−30 at waterY, labeled 16px blue "waterline" at the left just above the line.
- **Above-water text (bold 17px green `#27ae60`, right-aligned left of the tip):** "Conversion +10%".
- **Below-water text (bold 18px red `#e74c3c`, left-aligned right of the body, stacked at waterY+35/55/75/95):** "Refunds +40%", "Support +25%", "NPS -15", "LTV -8%".
- **Bottom label (17px `#555`, centered):** "A/B test only measured above the waterline".

## Section 2: Real Example: Google's 30 Search Results

- Google asked searchers how many results they wanted on a page, and people said more would be better — so the team ran a test showing 30 results instead of the usual 10.
- The bigger page took about half a second longer to appear, and that small delay was enough to drive searches and traffic in the test group down by roughly 20%.
- The story, told publicly by Google executive Marissa Mayer, shows that a feature can deliver exactly what users asked for and still lose, because the hidden cost (latency, the extra time a page takes to load) never showed up in the feature's own success metric.

### Visualization (canvas `c2`, 720×320)

Two grouped bar comparisons: page load time up (left group), searches down (right group).

- **Title (bold 17px `#2a2a2a`, centered):** "More Results, Slower Page, Fewer Searches" at y=28.
- **Common layout:** baseline y=235, bar width 70, gap 30 within each group; x labels 14px gray `#666` "10 results" / "30 results" under each bar; group titles bold 15px `#1a5276` centered below at baseline+42.
- **Left group ("Page load time"), starting x=130:** blue bar height 70 (fill `rgba(26,82,118,0.35)`, stroke `#1a5276`) vs orange bar height 140 (fill `rgba(230,126,34,0.3)`, stroke `#e67e22`); bold 16px orange delta label "~ +0.5s" above the second bar.
- **Right group ("Searches / traffic"), starting x=450:** blue bar height 140 vs red bar height 112 (fill `rgba(231,76,60,0.3)`, stroke `#e74c3c`); bold 16px red delta label "~ -20%" above the second bar.
- **Takeaway (15px `#333`, bottom center):** "Users got what they asked for — and half a second of hidden latency erased the win".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; paragraphs 0.95em `#333`; bullets 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart (note c2 here is 720×320); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`, gray text `#666`/`#333`.
