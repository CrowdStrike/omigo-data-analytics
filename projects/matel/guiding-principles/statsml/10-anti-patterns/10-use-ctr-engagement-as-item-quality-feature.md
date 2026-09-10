# Use CTR / Engagement as Item Quality Feature

**Page type:** detail page (two card-sections, each an h2 + two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Use CTR / Engagement as Item Quality Feature

**Subtitle:** CTR is 90% determined by position shown, not item quality

## The Anti-Pattern

Position 1: 30% CTR. Same item at position 10: 2% CTR. Feature encodes YOUR ranking bias, not the entity's property.

**Key point (red left border):** The feature reflects where you placed the item, not how good it is. You are training the model on your own previous decisions.

*Domain examples:*

- Search ranking
- Recommendations
- Ad targeting

### Visualization (canvas `c1`, 720×300)

Vertical bar chart: CTR by position, steep monotone decrease, with dashed trend line.

- **Title (bold 14px, top center, red `#e74c3c`):** "CTR = position, not quality!"
- **Data:** values `[30, 15, 8, 4, 2]` (%), labels `['Pos 1', 'Pos 2', 'Pos 3', 'Pos 4', 'Pos 5']`.
- **Axes/scale:** y max 35%, gridlines and labels every 7% (0%–35% in 5 steps, rounded), labels 11px gray `#666` right-aligned; y-axis line `#ccc`; light gray gridlines `#eee`. Padding: left 60, right 40, top 50, bottom 50. Rotated y-axis title "CTR %" (12px `#666`) at x=16.
- **Bars:** width 60% of slot, fill `#e74c3c`, border `#c0392b` 1px; bold 12px red value labels ("30%" etc.) 8px above each bar; x-axis labels 11px `#333` 18px below baseline.
- **Trend line:** dashed orange `#e67e22` (dash 4/4, width 2) connecting bar-top centers.
- **Annotation (11px orange `#e67e22`, left-aligned, near top right at x = w-padRight-190, y = padTop+20/36):** "Same item, different positions" / "→ CTR follows position, not quality".

## The Design Pattern

Debias: CTR adjusted for position (CTR relative to position-average). Or: random-position holdout traffic (5%) to measure true quality. Or: log position and train position-aware models.

**Key point (green left border `#27ae60`):** Remove the position signal to reveal the item signal.

- Compute position-normalized CTR (item CTR / avg CTR at that position)
- Reserve 5% random-shuffle traffic as unbiased measurement
- Log position as a separate feature; let the model learn position effect
- Use causal inference to estimate counterfactual CTR

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: position-adjusted CTR by item, values varying independently of original position.

- **Title (bold 14px, top center, green `#27ae60`):** "True quality signal revealed"
- **Data:** values `[12, 5, 18, 8, 14]` (%), labels `['Item A', 'Item B', 'Item C', 'Item D', 'Item E']`.
- **Axes/scale:** y max 25%, gridlines/labels in 5 steps (0%–25%, rounded), same padding, axis, and gridline styling as `c1`. Rotated y-axis title "Position-Adjusted CTR %" (12px `#666`).
- **Bars:** width 60% of slot, fill `#27ae60`, border `#1e8449` 1px; bold 12px value labels in `#1a5276` 8px above bars; x-axis labels 11px `#333`.
- **Annotation (11px `#1a5276`, left-aligned at x = w-padRight-280, y = padTop+20/36):** "Same items as left chart — position bias removed" / "Quality varies independently of original position".
- **Sub-labels:** 9px gray `#999` centered under each x-axis label (32px below baseline): "was Pos 1", "was Pos 2", "was Pos 3", "was Pos 4", "was Pos 5".

## Regeneration instructions

- **Layout:** two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"), each with an `h2` and a `table.layout` (width 100%, border-collapse) containing one row: `td.text-col` (45%) with paragraph + `.key-point` + `.example` + `ul`, `td.viz-col` (55%) with the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius; `.key-point` background `#f8f9fa`, 3px red `#e74c3c` left border (green `#27ae60` inline override in Design Pattern), padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; CSS width 100%. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions.
