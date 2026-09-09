# Gap Detection via Adjacent Bucket Variance

**Page type:** detail page (backlog kusto-style: intro callout, numbered h2 sections, each a 2-col table with text left 45% / canvas right 55%)
**HTML title tag:** Gap Detection via Adjacent Bucket Variance — Discussion Backlog

**Subtitle:** Context around the gap determines confidence

**Intro callout:** Empty bin between sparse tails = noise. Empty bin between dense, tight clusters = real gap. The key insight: context around the gap determines confidence.

## 1. Core Idea

- **High confidence:** adjacent buckets have many observations + tight variance
- **Low confidence:** adjacent buckets sparse or high variance
- **Combine:** count + within-bucket variance + gap width

### Visualization (canvas `c1`, 720×300)

Bimodal histogram with a clear 3-bin gap between two dense clusters, gap outlined in dashed red.

- **Title (bold 17px, `#1a5276`, top center):** "Real Gap — Dense Clusters on Both Sides".
- **Margins:** top 40, right 30, bottom 50, left 50.
- **Data (17 bins):** `[2, 5, 18, 42, 55, 48, 35, 0, 0, 0, 38, 50, 52, 40, 15, 4, 1]`; y scale max 60.
- **Axes:** L-shaped y+x axes in `#2c3e50`, width 1; rotated y-axis label "Count" in 14px `#2c3e50`.
- **Bars:** non-empty bins fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 0.5; empty bins (indices 7–9) get a full-height light red wash `rgba(231,76,60,0.1)` instead of a bar.
- **Gap highlight:** dashed (5/3) red (`#e74c3c`) 2px rectangle outlining the full height of bins 7–10 boundary; bold 15px red label "GAP" centered inside it.
- **Cluster labels (bold 14px, green `#27ae60`, below axis):** "n=200, low var" centered under the left cluster (~bin 3.5), "n=195, low var" under the right cluster (~bin 12.5).
- **Verdict (bold 19px, green `#27ae60`, bottom center):** "HIGH confidence".
- **Caption (italic, `.example`):** Real gap — dense clusters on both sides → high confidence

## 2. Confidence Formula

- Gap confidence = f(left_count, right_count, left_var, right_var, gap_width)
- Higher counts on both sides → higher confidence
- Lower within-bucket variance → tighter clusters → higher confidence
- Wider gap (more empty bins) → higher confidence

**Key Question:** Immediate neighbors only, or a window of 2-3 buckets on each side?

### Visualization (canvas `c2`, 720×300)

Sparse, noisy histogram where one empty bin is questioned as a gap, outlined in dashed red.

- **Title (bold 17px, `#1a5276`, top center):** "Noise Gap — Sparse Data with Empty Bin".
- **Margins:** top 40, right 30, bottom 50, left 50.
- **Data (17 bins):** `[1, 0, 2, 1, 3, 2, 0, 1, 2, 0, 1, 3, 2, 1, 0, 1, 2]`; y scale max 8.
- **Axes:** L-shaped y+x axes in `#2c3e50`, width 1; rotated y-axis label "Count" in 14px `#2c3e50`.
- **Bars:** non-empty bins fill `rgba(26,82,118,0.15)` (lighter than c1), stroke `#1a5276` width 0.5; empty bins get full-height `rgba(231,76,60,0.1)` wash.
- **Gap highlight:** dashed (5/3) red (`#e74c3c`) 2px rectangle around the single empty bin at index 6; bold 15px red label "GAP?" centered inside it.
- **Context labels (14px, orange `#e67e22`, below axis):** "n=2, high var" near bin 4, "n=1, high var" near bin 9.
- **Verdict (bold 19px, red `#e74c3c`, bottom center):** "LOW confidence".
- **Caption (italic, `.example`):** Noise gap — sparse data with empty bin → low confidence

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style). Structure: `<h1>` with bottom border `2px solid #2980b9`, `.subtitle` paragraph, one `.intro` callout, then one `.lang-section` per numbered section. Each section: `<h2>` ("N. Title", bottom border `2px solid #2980b9`), then a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (45%) holding bullets and optional `.key-point` callout, right `td.viz-col` (55%) holding the canvas plus an italic `.example` caption. No index number in the h1/title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem. `.intro`: background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point`: background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic, `#555`, 0.9rem. `ul` 0.92rem. `code`: background `#e8f0f8`, color `#1a5276`, padding 2px 6px, radius 3px. Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic size 720×300 per chart; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- In regenerated HTML, any card/page links use `.html` extensions.
