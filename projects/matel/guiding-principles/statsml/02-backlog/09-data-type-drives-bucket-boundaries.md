# Data Type Distribution Drives Bucket Boundaries

**Page type:** detail page (backlog 2-col layout: text left 45%, canvas right 55%, one `table.layout` row per section)
**HTML title tag:** Data Type Distribution Drives Bucket Boundaries — Discussion Backlog

**Subtitle:** Integer data needs integer boundaries — fractional cuts create artifacts

**Intro callout:** The column's type classification should inform WHERE bucket boundaries are placed. Integer data needs integer boundaries; fractional boundaries on integer data create artifacts.

## 1. Why It Matters

- Equal-width bins on integer data create unequal counts
- Fractional boundaries confuse interpretation
- Integer boundaries preserve natural groupings

*Example (italic):* A boundary at 3.5 splits between 3 and 4 for no data-driven reason — the cut point exists only in the binning algorithm, not in the data.

### Visualization (canvas `c1`, 720×300)

Bad-case histogram: fractional bin boundaries laid over integer data points.

- **Title (bold 17px, `#e74c3c`, top center):** "BAD: Fractional Boundaries on Integer Data".
- **Data:** integer values 1–10; fractional bin boundaries `[0.5, 3.5, 6.0, 8.5, 11.0]` giving 4 bins with counts `[15, 12, 18, 10]`; y scale max 22; x mapped over data range 0.5–11.0.
- **Margins:** top 45, right 30, bottom 55, left 50; L-shaped axes in `#2c3e50`, width 1.
- **Bins:** rectangles spanning boundary-to-boundary, fill `rgba(231,76,60,0.2)`, stroke `#e74c3c` width 1.
- **Boundary lines:** vertical dashed red lines (`#e74c3c`, dash 6/4, width 2) at each boundary, extending 5px below the baseline; boundary values labeled to one decimal ("0.5", "3.5", "6.0", "8.5", "11.0") in 13px `#e74c3c` centered under each line.
- **Integer data points:** a row of filled dots (radius 3, `#2c3e50`) at integer positions 1–10 below the boundary labels, each labeled with its integer value in bold 13px.
- **Problem annotation (italic 11px, `#e74c3c`, top-left inside plot):** "3.5 splits between 3 and 4 — confusing!".

## 2. Rules by Type

- **int_num columns:** Bucket boundaries must fall on integer values. [3,7] makes sense; [3.2, 6.8] splits integers awkwardly
- **flt_num columns:** Boundaries can be fractional, placed at natural breakpoints
- **int_cat columns:** Each unique integer value gets its own bucket (1-to-1 mapping)
- **Mixed columns:** If int_num=0.85, flt_num=0.15, treat as integer and round boundaries

**Key-point callout (red left border):**
**Key Questions:**
(1) For int_num with large range, how to pick integer boundaries?
(2) Threshold for integer-boundary mode?
(3) int_cat cardinality switch point?

### Visualization (canvas `c2`, 720×300)

Good-case histogram: integer bin boundaries on the same data.

- **Title (bold 17px, `#27ae60`, top center):** "GOOD: Integer Boundaries on Integer Data".
- **Data:** integer bin boundaries `[1, 4, 7, 9, 11]` giving bins [1-3], [4-6], [7-8], [9-10] with the same counts `[15, 12, 18, 10]`; y scale max 22; x mapped over data range of 10 starting at 1.
- **Margins:** top 45, right 30, bottom 55, left 50; L-shaped axes in `#2c3e50`.
- **Bins:** fill `rgba(39,174,96,0.2)`, stroke `#27ae60` width 1.
- **Boundary lines:** vertical dashed green lines (`#27ae60`, dash 6/4, width 2) at each boundary; boundary values labeled in bold 13px `#27ae60` ("1", "4", "7", "9"; the 11 boundary is left unlabeled).
- **Integer data points:** the same row of dots (radius 3, `#2c3e50`) at positions 1–10 with bold 13px integer labels (note: point x uses (v−0.5)/range mapping, slightly offset from the boundary mapping).
- **Bin labels (italic 11px, `#27ae60`, near top inside plot):** "[1-3]", "[4-6]", "[7-8]", "[9-10]" centered over each bin.

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col layout). Structure: h1, `.subtitle` paragraph, `.intro` callout, then one `.lang-section` per section, each with an `<h2>` and a `table.layout` single `<tr>`: left `<td class="text-col">` (45%) with bullets/example/key-point, right `<td class="viz-col">` (55%) with the canvas. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro` background `#f0f4f8`, left border 3px solid `#2980b9`, padding 8px 12px, 0.9rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem. Canvas `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic 720×300 attributes; scale by `window.devicePixelRatio` via a shared `setupCanvas(id)` helper (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- In regenerated HTML, any card links use `.html` extensions.
