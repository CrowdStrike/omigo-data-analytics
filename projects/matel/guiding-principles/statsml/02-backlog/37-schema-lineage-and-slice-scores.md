# Schema Lineage Extraction & SLiCE Scores

**Page type:** detail page (backlog-style layout: intro callout, numbered h2 sections, one 2-col text/viz table plus a full-width list section; stub page)
**HTML title tag:** Schema Lineage Extraction & SLiCE Scores

**Subtitle:** Tracking column provenance and computing slice-level confidence

**Intro callout (blue accent):** Physical lineage tells you where each feature came from; slice-level scores tell you where the model can actually be trusted. Combining the two reveals which transformations degrade which subpopulations. Stub — to be expanded.

## 1. Core Idea

**Schema Lineage:** Track how each column arrived — source table, transformations applied, joins traversed. Know the provenance of every feature.

**SLiCE Scores:** Score model confidence per data slice (subpopulation), not just globally. A model with 95% overall accuracy might be 60% on a critical slice.

- Column-level: source → transform → derived_name
- Slice-level: subgroup → metric → confidence
- Combined: which transformations degrade which slices?

**Key-point callout (red accent):** **Payoff:** Know exactly where your model is trustworthy and where it's guessing.

### Visualization (canvas `c1`, 720×300)

Bar chart of per-slice confidence scores with a trust-threshold line.

- **Title (bold 16px -apple-system, `#222`, top center):** "SLiCE Scores: Confidence Per Subpopulation".
- **Five bars** (100px wide, 25px gaps, centered as a group; baseline at y = h−60, chart height h−120, bar height = score/100 of chart height). Fill = bar color at ~53% alpha (hex + "88"), stroke = bar color 1.5px:
  - "Overall" — 95%, `#27ae60`
  - "Male 25-40" — 93%, `#27ae60`
  - "Female 60+" — 71%, `#e67e22`
  - "Income<20k" — 62%, `#e74c3c`
  - "Rural ZIP" — 58%, `#e74c3c`
- **Value labels:** bold 15px `#222` percentage above each bar (e.g., "95%"); 13px `#333` slice name below each bar.
- **Trust threshold:** horizontal dashed (5/3) red `#e74c3c` width 1.5 line at the 70% level, extending 20px beyond the bar group on both sides; 13px red label to the right: "Trust threshold".
- **Caption (14px `#555`, bottom center):** "Global accuracy hides slice-level failures".

## 2. Key Questions

- How to represent lineage compactly (DAG? annotation layer?)
- Minimum slice size for meaningful score (sample sufficiency again)
- How slice scores interact with the multi-candidate model approach
- Schema evolution: what happens when upstream adds/removes columns?

## Regeneration instructions

- **Layout:** backlog detail page (stub). h1 with bottom border `2px solid #2980b9`, `.subtitle` paragraph, `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem). Section 1 is a `.lang-section` with an h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` (full width) with one `<tr>`: left `td.text-col` (45%) text, right `td.viz-col` (55%) canvas. Section 2 is a full-width bullet list with no canvas.
- **Callout styles:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` — italic, `#555`, 0.9rem.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem with 20px left margin. Canvases styled `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** declares intrinsic `width="720" height="300"`; shared `setup(id)` helper reads the width/height attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Chart fonts use the `-apple-system, sans-serif` stack.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; chart text `#222`/`#333`/`#555`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
