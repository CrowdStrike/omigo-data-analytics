# Repeated Evaluation on Same Test Set

**Page type:** detail page (two `.card-section` blocks — The Anti-Pattern / The Design Pattern — each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Repeated Evaluation on Same Test Set

**Subtitle:** Each iteration fixes errors THIS dataset surfaces — other failure modes remain invisible

## The Anti-Pattern

Evaluate → tune → re-evaluate same test → tune again. Fixes only errors this sample exposes. Published accuracy reflects coverage of one sample's error surface, not generalization.

**Key point (callout):** The test set becomes a second training set through indirect gradient descent — human decision-making optimizes for its specific errors.

*Domain examples:*

- ML model development — iterating hyperparameters against a fixed holdout
- Kaggle-style iteration — climbing leaderboard on a single test split
- Paper benchmarks — tuning until SOTA on a canonical dataset

### Visualization (canvas `c1`, 720×300 — note: HTML omits the `width` attribute; intrinsic size set to 720×300 by the setup helper)

Loop diagram: three rows of Model → Test Set → Fix cycling back, with rising reported accuracy and an overfitting warning box.

- **Nodes (rounded rectangles, radius 4, height 30, width max(text+24, 80), bold 12px centered labels):**
  - Row 1 (y=70): "Model v1" at x=80 (blue `#1a5276`, fill `rgba(26,82,118,0.1)`), "Test Set" at x=230 (red `#e74c3c`, fill `rgba(231,76,60,0.15)`, stroke width 2.5), "Fix" at x=380 (orange `#e67e22`, blue-tint fill).
  - Row 2 (y=160): "Model v2", "Same Test" (red style), "Fix".
  - Row 3 (y=250): "Model v3" at x=80, "Same Test" at x=230 (red style).
- **Arrows:** gray `#666` arrows (width 1.5, filled arrowheads) left-to-right within each row, and from each "Fix" node down and back left to the next "Model" node (down from x=380, across to x=120, into the model box).
- **Reused label:** bold 10px red centered "⟵ REUSED ⟶" above the test-set column at (230, 28).
- **Reported accuracy column (right side, bold 14px, left-aligned at x=500):** 11px gray `#666` header "Reported accuracy:" at (490, 40); "88%" in `#1a5276` at y=70; "91%" in `#e67e22` at y=160; "94%" in `#27ae60` at y=250. A green dashed vertical line (`#27ae60`, width 2, dash 4/3) at x=530 from y=80 to y=240 with an upward green arrowhead at the top.
- **Warning box:** rounded rect (radius 6) 155×100 at (555, 100); fill `rgba(231,76,60,0.1)`, stroke `#e74c3c` width 2. Bold 12px red centered lines: "⚠ Overfitting to" / "ONE sample's" / "errors!". Below, 10px `#888` lines: "Other failure modes" / "remain invisible".

## The Design Pattern

Rotate test sets. After a few evaluation cycles, retire the test set and source a new one from current production data.

**Key point (callout):** Fresh test sets expose fresh failure modes. Retired sets become validation history, not optimization targets.

- Limit evaluations per test set (e.g., 3 rounds max)
- Retire and archive the old test set
- Source a new test set from recent production data
- Re-evaluate on the new set to find NEW blind spots
- Track accuracy across rotations — true generalization trend

### Visualization (canvas `c2`, 720×300 — note: HTML omits the `width` attribute; intrinsic size set to 720×300 by the setup helper)

Test-set rotation diagram: two retired sets and one current set over a timeline.

- **Test set boxes (rounded rect radius 6, 130×55, centered at y=80):**
  - "Test v1" at x=100, status "(retired)": gray `#999`, fill `rgba(150,150,150,0.1)`, label struck through with a thin gray line.
  - "Test v2" at x=300, status "(retired)": same gray retired style with strikethrough.
  - "Test v3" at x=500, status "(current)": green `#27ae60`, fill `rgba(39,174,96,0.12)`.
  - Each box: bold 13px label on top line, 11px status on second line.
- **Progression arrows:** green `#27ae60` arrows (width 2.5, filled arrowheads) from box to box: (165,80)→(232,80) and (365,80)→(432,80).
- **Per-set failure labels (11px centered at y=125):** red "Found: type-A errors" under Test v1, orange `#e67e22` "Found: type-B errors" under Test v2, blue `#1a5276` "Finding: type-C errors" under Test v3.
- **Archived marks (16px gray `#999`, y=145):** "✓ archived" under Test v1 and Test v2.
- **Callout box:** rounded rect (radius 6) 250×70 at (370, 140); fill `rgba(39,174,96,0.1)`, stroke `#27ae60` width 2. Bold 13px green centered lines "New set exposes" / "NEW failures", then 11px `#555` line "Each rotation reveals blind spots".
- **Timeline bar:** rounded rect (radius 4) 620×40 at (50, 240); fill `rgba(26,82,118,0.08)`, stroke `#1a5276` width 1. Centered 11px blue label "Time →". Three 4px blue dot markers with 10px labels: "Rounds 1-3" at x=130, "Rounds 4-6" at x=330, "Rounds 7+" at x=530.
- **Connectors:** thin gray `#aaa` dashed lines (dash 3/3) from the bottom of each test-set box (y=108) to its timeline marker (y=240).

## Regeneration instructions

- **Layout:** two `.card-section` divs, each with an `<h2>` ("The Anti-Pattern", "The Design Pattern", 1.3rem `#1a5276`, bottom border `2px solid #2980b9`) followed by a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (45%) holding a paragraph, a `.key-point` callout, an italic `.example` lead-in (section 1 only), and `<ul>` bullets; right `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 per chart, CSS `width: 100%` with `1px solid #e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)` family.
- Any card links in regenerated HTML use `.html` extensions.
