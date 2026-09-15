# Page Title — Category of Items

**Page type:** other — UI template file (repeated `.card-section` blocks, each h2 + 2-col layout table: text 50% | canvas 50%; placeholder content)
**HTML title tag:** TEMPLATE: Sectioned Cards — Key-Point Callouts with Canvas

**Template header comment (verbatim, kept in the CSS block):**

> ═══ TEMPLATE: Repeated content cards, each with 2-col layout (text 50% | canvas 50%) ═══
> Use for: Bias catalogs, pitfall lists, concept galleries, pattern breakdowns
> Pattern: Title + subtitle → repeated sections (h2 + table with prose/callout | canvas)
> Source: reference/cognitive-biases/06-measurement-reporting.html
> Key differences from template 05:
> - White background (not #fafafa)
> - Each section wraps its own table (not one big table with many rows)
> - .key-point callout with red left-border accent
> - .example italic text for concrete instances
> - Taller canvas (300px) for flow diagrams and annotated charts

**Subtitle:** One-line description of what this page covers and why it matters.

## Document structure (in order)

1. `<h1>` — "Page Title — Category of Items" (ampersand written as `&mdash;` entity in source).
2. `.subtitle` paragraph.
3. Three `.card-section` divs (HTML comments "Section 1/2/3"), each: `<h2>N. Item Title</h2>` + `table.layout` with one `<tr>`: left `td.text-col` (50%) with paragraph, ul, `.key-point` callout, `.example` line; right `td.viz-col` (50%) with `<canvas id="cN" width="720" height="300">`.
4. `<script>` with `setup(id)` devicePixelRatio helper, palette comment, and three example canvas drawings.

## Section 1: 1. Item Title (placeholder text verbatim)

- Paragraph: One-paragraph explanation of the concept. Keep it to 2-3 sentences that establish what this is and why it matters.
- Bullets:
  - Key detail or mechanism #1
  - Key detail or mechanism #2
  - Key detail or mechanism #3
- Key-point callout (`.key-point`): **Impact:** The critical takeaway — what goes wrong and what the fix is. One or two sentences.
- Example line (`.example`, italic): Example: Concrete instance with numbers that makes the abstract real.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart (example placeholder). Script comment: "Canvas 1: Example — grouped bar chart (comparing two measurements)".

- **Title (bold 14px system-ui, `#1a5276`, centered at w/2, y=25):** "Comparison Chart Title"
- **Bars:** width 60, baseline y=260, max bar height 180. Values/colors from two groups — Group A: values `[0.45, 0.20]`, colors `['#e74c3c', '#e67e22']`; Group B: values `[0.25, 0.25]`, colors `['#27ae60', '#27ae60']`. Bar centers at x = 140, 260, 460, 580; bar labels "Bar 1"…"Bar 4" (11px `#444`, below baseline at y = baseY+16).
- **Fill:** each bar filled at globalAlpha 0.8 in its color, stroked (width 2) in the same color at full alpha; percentage value label (`Math.round(val*100) + '%'`, bold 13px, bar color) 8px above each bar.
- **Divider:** vertical dashed line (`#ccc`, dash 4/4) at x = w/2 from y=50 to y=270 separating the two groups.

## Section 2: 2. Item Title (placeholder text verbatim)

- Paragraph: Explanation paragraph for this item.
- Bullets:
  - Detail or mechanism #1
  - Detail or mechanism #2
  - Detail or mechanism #3
- Key-point callout: **Impact:** Critical takeaway with fix or antidote.
- Example line: Example: Concrete instance with specific numbers.

### Visualization (canvas `c2`, 720×300)

Flow diagram (example placeholder). Script comment: "Canvas 2: Example — flow diagram (two pathways with different outcomes)".

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Process Flow Diagram"
- **Top path (red, `#e74c3c`), centerline y=100:** path label "PATH A (PROBLEMATIC)" (12px red, centered, y=55); box 120×40 at x=80 (white fill, red stroke width 2) with text "Step 1" (`#1a5276`, 12px); connecting horizontal line x=200→320; outcome box 160×50 at x=320 (white fill, red stroke width 3) with bold 13px red text "Bad Outcome".
- **Bottom path (green, `#27ae60`), centerline y=220:** path label "PATH B (CORRECT)" (12px green, centered, y=170); box 120×40 at x=80 (white fill, green stroke width 2) with text "Step 1"; connecting line x=200→320; outcome box 160×50 at x=320 (white fill, green stroke width 3) with bold 13px green text "Good Outcome".

## Section 3: 3. Item Title (placeholder text verbatim)

- Paragraph: Explanation paragraph for this item.
- Bullets:
  - Detail or mechanism #1
  - Detail or mechanism #2
  - Detail or mechanism #3
- Key-point callout: **Impact:** Critical takeaway with fix or antidote.
- Example line: Example: Concrete instance with specific numbers.

### Visualization (canvas `c3`, 720×300)

Line chart with convergence and annotation (example placeholder). Script comment: "Canvas 3: Example — line chart with convergence and annotation".

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Metric Over Time — Convergence to True Value"
- **Axes:** left=80, right=660, top=50, bottom=250; L-shaped axes stroke `#999`, width 1.
- **True value line:** horizontal dashed green (`#27ae60`, dash 6/4, width 2) at value 0.25 of the y-range; label "True value" (11px green, left-aligned at right−80, 8px above the line).
- **Convergence curve:** blue `#1a5276`, width 2.5, 51 points over t∈[0,1] generated by `val = 0.25 + 0.6·e^(−4t)·sin(15t) + 0.4·e^(−3t)` — a noisy curve that decays toward the true value 0.25.
- **Annotation:** red dot (`#e74c3c`, radius 6) at 15% of x-range, 65% of y-range, with bold 11px red label to its upper right: "Early snapshot (misleading)".

## Script palette comment (verbatim)

- Primary: #1a5276 (blue)
- Positive: #27ae60 (green)
- Negative: #e74c3c (red)
- Warning: #e67e22 (orange)
- Bar fill: rgba(26,82,118,0.35)
- Text: #2c3e50 (body), #444 (secondary), #666 (muted)

## Regeneration instructions

- **This is a UI template file** in `ui-templates/`, kept as a starting point for bias catalogs, pitfall lists, concept galleries, pattern breakdowns. All item text is placeholder.
- **Layout:** h1 + `.subtitle`, then repeated `.card-section` blocks; each block is `<h2>N. Item Title</h2>` followed by its own `table.layout` with a single row (text-col 50% | viz-col 50%). No single big table.
- **Page CSS:** universal reset `* { margin:0; padding:0; box-sizing:border-box; }`; body `system-ui, -apple-system, sans-serif`, background `#fff`, color `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276`, margin-bottom 8px, border-bottom `2px solid #2980b9`, padding-bottom 8px. `.subtitle` `#666`, 0.95rem, margin-bottom 32px.
- **Section CSS:** `.card-section` margin-bottom 40px; its h2 1.3rem `#1a5276`, margin-bottom 12px, border-bottom `2px solid #2980b9`, padding-bottom 4px.
- **Table CSS:** `table.layout` width 100%, border-collapse collapse, margin-top 8px; td vertical-align top, padding 12px; `.text-col` 50%, `.viz-col` 50%.
- **Canvas CSS:** `canvas { width: 100%; border: 1px solid #e0e0e0; border-radius: 4px; }` with intrinsic attributes `width="720" height="300"`. Script `setup(id)` multiplies backing store 720×300 by `window.devicePixelRatio`, calls `ctx.scale(dpr, dpr)` and `ctx.clearRect`.
- **Callout CSS:** `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, margin-top 12px, 0.9rem. `.example` italic, `#555`, margin-top 8px, 0.9rem. ul margin 8px 0 8px 20px, 0.92rem; li margin-bottom 4px.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, bar fill rgba(26,82,118,0.35).
- **HTML comments to preserve:** the template-header block comment inside `<style>`, "Section 1/2/3", "═══ Canvas setup with devicePixelRatio ═══", "═══ Color palette ═══", and per-canvas example comments.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
