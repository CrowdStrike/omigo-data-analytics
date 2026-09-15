# TEMPLATE: TOC Reference — Numbered Sections with Canvas Visualizations

**Page type:** other (UI template file: long-form reference with TOC box, numbered h2 sections each holding a 2-col obj-table row of text | canvas, optional comparison table, closing callout)
**HTML title tag:** TEMPLATE: TOC Reference — Numbered Sections with Canvas Visualizations

**CSS header comment (verbatim):**
```
═══ TEMPLATE: Long-form reference with TOC + 2-col table (text | canvas) ═══
Use for: Algorithm references, concept catalogs, anything with numbered sections
Pattern: TOC box → h2 sections → obj-table rows (text left 50%, canvas right 50%)
Source: reference/01-foundations-ml-assumptions.html
```

## Document content (in order)

**h1:** Document Title

**Subtitle:** One-line description — what this reference covers and why it matters

### TOC box (`.toc`)

**Contents** (bold), then an ordered list of in-page anchor links:

1. Section One (#section-1)
2. Section Two (#section-2)
3. Section Three (#section-3)
4. Comparison Table (#comparison)

### 1. Section One (h2, id `section-1`)

`.obj-table` with one row — left cell (50%):

**Concept Title & What Breaks** (`.obj-title`)

- **Key point:** Explanation of the first important detail.
- **Key point:** Another point with its implication.
- **Key point:** Third point.
- (class `bad`, red) Violation: what goes wrong when assumptions are violated.
- (class `warn`, orange) Verify: how to check if this applies to your data.
- (class `good`, green) Alternative: what to use instead when it doesn't hold.

**Real example:** Concrete scenario illustrating the problem. (small 0.85em `#555` paragraph)

#### Visualization (canvas `c1`, 720×280)

Placeholder bar chart demonstrating template style.

- **Title (bold 14px `#1a5276`, top center):** "Visualization Title".
- **Axes:** L-shaped axis lines in `#ccc` (1px), padding 50px left, 30px top/bottom, 20px right.
- **Bars:** 12 bars, 44px wide at 52px pitch starting at pad+10, fill `rgba(26,82,118,0.35)`; heights are deterministic placeholder values `30 + sin(i*0.8)*40 + 60`.
- **X-axis label (12px `#555`, bottom center):** "X-axis label".

### 2. Section Two (h2, id `section-2`)

`.obj-table` row — left cell:

**Another Concept** (`.obj-title`)

- **Key point:** Explanation.
- **Key point:** Explanation.
- (class `bad`) Violation: what breaks.
- (class `good`) Alternative: what to use instead.

#### Visualization (canvas `c2`, 720×280)

Placeholder line chart.

- **Title (bold 14px `#1a5276`, top center):** "Second Visualization".
- **Series:** green `#27ae60` line, width 3, 20 points spanning x=50 to w-30, y = h/2 + sin(i*0.5)*60 (deterministic sine wave).

### 3. Section Three (h2, id `section-3`)

`.obj-table` row — left cell:

**Third Concept** (`.obj-title`)

- **Key point:** Explanation.
- **Key point:** Explanation.

#### Visualization (canvas `c3`, 720×280)

Placeholder scatter plot.

- **Title (bold 14px `#1a5276`, top center):** "Third Visualization".
- **Points:** 30 dots, 4px radius, fill `rgba(41,128,185,0.5)`, positions drawn from a seeded Park-Miller LCG (`lcg(20250907)`) within margins (60px left, 40px right/top, 40px bottom).
- **Never `Math.random()`:** every generated figure uses the seeded `lcg(seed)` helper so the chart, its labels, and the prose agree on every load.
- **Computed labels:** if a chart prints a statistic (r, mean, rate, %), compute it from the plotted points at render time — never hardcode it beside the drawing.

### 4. Comparison Table (h2, id `comparison`)

`.compare-table` — header row: Aspect | Approach A | Approach B.

| Aspect | Approach A | Approach B |
|--------|------------|------------|
| Feature 1 | (bad, red) Weakness described. | (good, green) Strength described. |
| Feature 2 | (warn, orange) Partial support. | (good, green) Full support. |
| Feature 3 | (bad, red) Not possible. | (good, green) Built-in. |

### Closing callout (`.philosophy`)

**Key Takeaway:** The core principle summarized in one sentence.

## Regeneration instructions

- **Template:** this file IS ui-template 03-toc-reference — a self-documenting HTML template with placeholder content. Structure: h1 + `.subtitle` + `.toc` box + numbered h2 sections (each an `.obj-table` with one text|canvas row) + `.compare-table` + closing `.philosophy` callout + canvas script. Keep the CSS header comment block.
- **Page style:** body font `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`, background `#ffffff`, text `#2a2a2a`, padding 40px 20px, line-height 1.6, base 0.95em; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`, margin `40px 0 15px`; `.subtitle` `#666` 1.05em; p `#333` 0.95em; `code` background `#e8f0f8`, padding 2px 6px, radius 3px, `#1a5276`; `strong` `#1a5276`.
- **TOC box:** background `#f8f9fa`, border `1px solid #e0e0e0`, radius 6px, padding 16px 24px; links `#2980b9`, no underline (underline on hover).
- **obj-table:** full width, collapsed borders, cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; first td 50%, last td 50% centered; even rows background `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`.
- **Comparison table:** `.compare-table` 0.88em; th background `#1a5276` white text, padding 10px 12px; td border `1px solid #e0e0e0`, padding 8px 12px; even rows `#fafcfe`. Status classes: `.good` `#27ae60`, `.bad` `#e74c3c`, `.warn` `#e67e22`, all weight 600.
- **Callout:** `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 16px 20px, 1em.
- **Canvases:** intrinsic 720×280, centered `display:block`; a `setupCanvas(id)` helper multiplies the backing store by `window.devicePixelRatio`, fixes CSS size in px, and calls `ctx.scale(dpr,dpr)`.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar fill `rgba(26,82,118,0.35)`, accent `#2980b9`.
- In regenerated HTML, card/TOC links use `.html` extensions or in-page `#` anchors as appropriate (this template's TOC links are in-page anchors). No nav bar, no back/home links.
