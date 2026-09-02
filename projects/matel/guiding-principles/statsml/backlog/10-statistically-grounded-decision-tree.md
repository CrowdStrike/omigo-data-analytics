# Statistically-Grounded Decision Tree Redesign

**Page type:** detail page (backlog 2-col layout: text left 45%, canvas right 55%, one `table.layout` row per section)
**HTML title tag:** Statistically-Grounded Decision Tree Redesign — Discussion Backlog

**Subtitle:** Only continue branches with sufficient statistical evidence

**Intro callout:** Standard decision trees split greedily without checking statistical backing. Redesign: only continue branches with sufficient evidence, so tree depth corresponds to how much the data can actually prove.

## 1. Key Principles

- **Asymmetric branches:** A split may produce one branch with significant data + clear signal, another with insufficient data
- **Significant branch only:** If only left child has n > minimum AND passes separation test, only that branch continues
- **Split validation:** Before committing, verify split creates statistically distinct subgroups
- **Partial trees:** Resulting tree may be highly asymmetric — depth corresponds to evidence

**Key-point callout (red left border):**
**Contrast:** A traditional tree forces every split regardless of evidence; a grounded tree grows deep only where splits are statistically validated.

### Visualization (canvas `c2`, 720×300)

Side-by-side comparison diagram: traditional symmetric tree vs statistically-grounded asymmetric tree (this section shows canvas `c2`).

- **Title (bold 17px, `#1a5276`, top center):** "Traditional (symmetric) vs Statistically-Grounded (asymmetric)".
- **Left half (centered at 25% width):** heading "Traditional" (bold 14px, `#555`). A fully symmetric 4-level binary tree drawn in gray `#bdc3c7`: root circle (radius 6) at y=65, 2 children (radius 5) at ±50px, 4 grandchildren (radius 4) at ±75/±25, and 8 leaves (radius 3) spread across ±88px, all connected by 1px gray edges. Caption (13px, `#555`): "All splits forced" / "regardless of evidence".
- **Vertical divider:** light gray `#ecf0f1` line, width 2, at page center from y=35 to h−20.
- **Right half (centered at 72% width):** heading "Statistically-Grounded" (bold 14px, `#27ae60`). Asymmetric tree: root node in `#1a5276` (radius 6); green edge to left child (`#27ae60`), red edge to right side (`#e74c3c`) ending in a small dashed red rectangle (30×16, dash 3/2) labeled "STOP" (8px, `#e74c3c`). The left branch continues two more levels deep with green nodes/edges (radii 5, 4). Small green p-value annotations (8px): "p<0.01" and "p<0.05" beside the validated splits. Caption (13px, `#27ae60`): "Only validated splits" / "depth = evidence".
- **Legend (13px, bottom center):** "Green = statistically validated" in `#27ae60` and "Red = insufficient evidence" in `#e74c3c`.

## 2. Cascade Integration

- "No decision" leaf passes to next feature's tree in cascade
- Each tree handles what it can prove; defers the rest

*Example (italic):* The right branch (n=23) lacks evidence to continue, so it becomes a NO DECISION leaf that hands off to the next feature in the cascade.

**Key-point callout (red left border):**
**Key Questions:**
(1) Minimum significance threshold?
(2) How to combine partial trees?
(3) Preventing over-conservatism?
(4) Confidence penalty for "no decision"?

### Visualization (canvas `c1`, 720×300)

Node-and-edge diagram of a statistically-grounded asymmetric tree (this section shows canvas `c1`).

- **Title (bold 17px, `#1a5276`, top center):** "Statistically-Grounded Asymmetric Tree".
- **Nodes:** rounded rectangles 90×32, corner radius 6, 13px `#2c3e50` label text; green nodes stroke `#27ae60` width 2 with fill `rgba(39,174,96,0.08)`; red nodes stroke `#e74c3c` dashed (dash 4/3, width 1.5) with fill `rgba(231,76,60,0.08)`; the root uses stroke `#1a5276` (solid) with the green fill.
- **Tree structure:**
  - Root at (240, 55): "Age > 45".
  - Level 1 at y=115: left node (140) "Income > 50k" (green, solid); right node (340) "NO DECISION" (red, dashed). Edges labeled "n=1200" (green `#27ae60`) and "n=23" (red `#e74c3c`), 12px labels near edge midpoints. Bold 9px green annotation "p=0.001" to the right of the left node.
  - From "NO DECISION": a dashed orange arrow (`#e67e22`, dash 3/2, width 1.5, filled triangular head) pointing right, labeled in two 12px orange lines: "next feature" / "in cascade".
  - Level 2 at y=180 (only from the left branch): "Predict: YES" (80) and "Hours > 40" (200), both green solid; edges labeled "n=850" and "n=350" in green. Bold 9px green "p=0.003" to the right of "Hours > 40".
  - Level 3 at y=245: "Predict: YES" (160) and "Predict: NO" (250), both green solid; edges labeled "n=280" and "n=70" in green.

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col layout). Structure: h1, `.subtitle` paragraph, `.intro` callout, then one `.lang-section` per section, each with an `<h2>` and a `table.layout` single `<tr>`: left `<td class="text-col">` (45%) with bullets/example/key-point, right `<td class="viz-col">` (55%) with the canvas. Note the canvas order: section 1 uses `c2`, section 2 uses `c1`. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro` background `#f0f4f8`, left border 3px solid `#2980b9`, padding 8px 12px, 0.9rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem. Canvas `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, gray tree `#bdc3c7`.
- **Canvas:** intrinsic 720×300 attributes; scale by `window.devicePixelRatio` via a shared `setupCanvas(id)` helper (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); node drawing uses `ctx.roundRect`.
- In regenerated HTML, any card links use `.html` extensions.
