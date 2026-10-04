# Feedback Loops & Structural Biases — Viz

**Page type:** detail page — one `.bias-section` per bias, each a two-column layout table (text left 50%, canvas right 50%)
**HTML title tag:** Feedback Loops & Structural Biases
**Template:** none named in the source; the page defines its own `.bias-section` / `.key-point` / `.example` structure

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed or the construction invalidates that prose — re-read the computed values and update the text to match. (This page draws diagrams only; no generated data and no computed labels, so no figure in the text depends on a chart.)

**Determinism:** the source states no seeding rule — all four canvases are fixed-geometry diagrams with no generated data, so there is nothing to seed.

---

## 1. Self-Fulfilling Prophecy

**Tag colors:** none — this page has no tag pills.

### canvas `c1` — 720×300

Circular feedback-loop diagram: four colored arc segments around a ring with arrowheads, four node labels.

- **Title (bold 14px `#1a5276`, top center):** "Self-Reinforcing Prediction Loop".
- **Ring:** center (360, 160), radius 100; faint background ring stroke `rgba(26,82,118,0.15)` width 20.
- **Arc segments** (width 4, arrowhead triangles at each end), one per quadrant starting at top and going clockwise; colors in order: red `#e74c3c`, orange `#e67e22`, blue `#1a5276`, green `#27ae60`.
- **Node labels** (bold 13px, colored to match their segment): "Prediction" (top), "Action" (right), "Outcome" (bottom), "Training Data" (left).
- **Center text (11px `#7f8c8d`):** "feedback loop" / "(no counterfactual)".

---

## 2. Omitted Variable Bias

**Tag colors:** none — this page has no tag pills.

### canvas `c2` — 720×300

Causal DAG with three ellipse nodes and arrows.

- **Title (bold 14px `#1a5276`, top center):** "Omitted Variable Creates Overstated Effect".
- **Nodes** (ellipses 55×22 rx/ry, bold 14px centered labels): "Education" at (180, 200) and "Income" at (540, 200) — fill `#eaf2f8`, stroke/text `#1a5276`; "Parental Wealth" at (360, 80) — fill `#fdebd0`, stroke/text orange `#e67e22`.
- **Arrows:** solid red `#e74c3c` width 3 with arrowhead from Education → Income (observed, overstated); dashed (6/4) orange `#e67e22` width 2.5 arrows from Parental Wealth → Education and Parental Wealth → Income (hidden confounder paths). Arrows trimmed to start/end outside the node ellipses.
- **Legend (11px, bottom):** red solid swatch + "Observed (overstated)"; orange dashed line + "Hidden confounder paths".

---

## 3. Collider Bias

**Tag colors:** none — this page has no tag pills.

### canvas `c3` — 720×300

Collider DAG: two independent causes pointing into a conditioned common effect.

- **Title (bold 14px `#1a5276`, top center):** "Collider Bias: Conditioning on Common Effect".
- **Nodes** (ellipses 48×22, bold 14px labels): "Talent" at (180, 90) and "Looks" at (540, 90) — fill `#eaf2f8`, stroke/text `#1a5276`; "Fame" at (360, 210) — fill `#fdebd0`, orange stroke width 2.5 plus a second outer ellipse (54×28, width 1.5) as a double border indicating conditioning, orange text, with 10px orange label "[conditioned on]" below the node.
- **Causal arrows:** solid `#1a5276` width 2.5 with arrowheads: Talent → Fame and Looks → Fame.
- **Spurious link:** dashed (5/4) red `#e74c3c` width 2.5 horizontal line between Talent and Looks at y=90, with 11px red labels above it: "(appears when conditioning on Fame)" at y=60 and "spurious negative correlation" at y=75.
- **Legend (11px, bottom):** blue solid swatch + "Causal path"; red dashed line + "Spurious (selection-induced)".

---

## 4. Position Bias → CTR Feedback Loop

**Tag colors:** none — this page has no tag pills.

### canvas `c4` — 720×300

Circular feedback-loop diagram (same ring style as c1) with a starvation note at the bottom.

- **Title (bold 14px `#1a5276`, top center):** "Position Bias: Rich-Get-Richer Loop".
- **Ring:** center (360, 150), radius 90; background ring stroke `rgba(26,82,118,0.12)` width 22; four arc segments width 4 with arrowheads; colors in order: blue `#1a5276`, green `#27ae60`, orange `#e67e22`, purple `#8e44ad`.
- **Node labels** (bold 12px, colored to match): "High Position" (top), "More Clicks" (right), "Higher CTR Score" (bottom), "Boosted Rank" (left).
- **Center text:** bold 11px red "rich-get-richer" / 10px `#7f8c8d` "(Matthew effect)".
- **Bottom note (11px red, centered at y=280):** "New items never reach top positions → no clicks → no signal → permanent cold start".

---

## Page-specific constraints

- **Bespoke section wrapper:** four `.bias-section` blocks with 40px bottom margin, each opening with a numbered `<h2>` carrying a 2px `#2980b9` bottom border; the h1 uses the same 2px `#2980b9` border.
- **Bespoke text-column elements** (not the standard tags/key-point/src trio): a lead paragraph, then `<ul>`, then a `.key-point` box — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem, `strong` label inside — then an italic `.example` paragraph in `#555` 0.9rem with no box. There is no tag-pill row and no `.src` note on this page.
- **Canvas height is uniform 300 across all four** — shorter than the usual 330–460 band, which these diagrams rely on; the arrow and note coordinates (y=280 note in c4, y=210 node in c3) are tuned to it.
- **Local palette, not the shared `P`:** purple `#8e44ad` and the node fills `#eaf2f8` (blue tint) / `#fdebd0` (orange tint) exist only on this page and are needed by c2, c3 and c4.
- **c4 reuses c1's ring style** — the source specifies it as "same ring style as c1" with its own center/radius/width values, so the two must stay visually matched.
- **Arrow trimming in c2:** edges must start and end outside the node ellipses rather than at their centers.
- **Conditioning is shown by the double ellipse on "Fame"** in c3 plus its "[conditioned on]" label — both are specified, so keep both.
- This page has no outbound links; in regenerated HTML any card links would use `.html` extensions.
