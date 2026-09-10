# Use One Bin Count for All Features

**Page type:** detail page (anti-pattern/design-pattern pair: two `.card-section` blocks, each a two-column layout table — text left 45%, canvas right 55%)
**HTML title tag:** Use One Bin Count for All Features

**Subtitle:** 20 equal-width bins on a power-law feature: 1 bin holds 80% of the data, 11 bins are completely empty

## Shared dataset (both canvases)

Both charts read one literal, hardcoded, pre-sorted array `DATA` of 200 illustrative feature
values (e.g. support-ticket resolution hours), range 0.3 to 233.0. **No PRNG and no
`Math.random()`** — a literal array is used deliberately rather than a seeded generator because
the *shape* of this distribution is the lesson: where the mass sits and how far the tail runs
determines every bin count printed on the page, so the values are chosen to produce exactly the
taught 20-bin histogram.

```
DATA (200 values, ascending) =
0.3, 0.3, 0.3, 0.31, 0.31, 0.31, 0.31, 0.32, 0.32, 0.32, 0.33, 0.33, 0.33, 0.34, 0.34,
0.35, 0.35, 0.35, 0.36, 0.36, 0.37, 0.37, 0.38, 0.39, 0.39, 0.4, 0.4, 0.41, 0.42, 0.42,
0.43, 0.44, 0.44, 0.45, 0.46, 0.47, 0.48, 0.49, 0.49, 0.5, 0.51, 0.52, 0.53, 0.54, 0.55,
0.56, 0.58, 0.59, 0.6, 0.61, 0.62, 0.64, 0.65, 0.66, 0.68, 0.69, 0.71, 0.72, 0.74, 0.75,
0.77, 0.79, 0.8, 0.82, 0.84, 0.86, 0.88, 0.9, 0.92, 0.94, 0.96, 0.99, 1.01, 1.03, 1.06,
1.08, 1.11, 1.14, 1.16, 1.19, 1.22, 1.25, 1.28, 1.31, 1.35, 1.38, 1.42, 1.45, 1.49, 1.53,
1.57, 1.61, 1.65, 1.69, 1.74, 1.78, 1.83, 1.88, 1.93, 1.98, 2.04, 2.09, 2.15, 2.21, 2.27,
2.33, 2.39, 2.46, 2.53, 2.6, 2.67, 2.75, 2.83, 2.91, 2.99, 3.07, 3.16, 3.25, 3.35, 3.44,
3.54, 3.65, 3.75, 3.86, 3.98, 4.09, 4.22, 4.34, 4.47, 4.6, 4.74, 4.89, 5.03, 5.19, 5.34,
5.51, 5.67, 5.85, 6.03, 6.21, 6.41, 6.6, 6.81, 7.02, 7.24, 7.47, 7.7, 7.95, 8.2, 8.46,
8.73, 9.01, 9.29, 9.59, 9.9, 10.22, 10.55, 10.89, 11.25, 11.61, 13.0, 13.6, 14.2, 14.8,
15.3, 15.9, 16.5, 17.1, 17.7, 18.3, 18.9, 19.5, 20.1, 20.6, 21.2, 21.8, 22.4, 23.0, 25.3,
26.5, 27.6, 28.8, 30.0, 31.2, 32.4, 33.5, 34.7, 37.8, 39.9, 42.0, 44.1, 46.2, 50.5, 54.0,
57.5, 63.4, 68.7, 78.0, 90.0, 233.0
```

Derived facts (all computed in JS at render time, never hardcoded beside the chart):

| Quantity | Value |
|----------|-------|
| n | 200 |
| Range | 0.3 – 233.0 |
| 20 equal-width bins over [0, 240], bin width | 12 units |
| Bin counts | 160, 18, 9, 5, 3, 2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 |
| Counts sum | 200 |
| First bin share | 160 / 200 = 80.0% |
| Remaining 19 bins share | 40 / 200 = 20.0% |
| Empty bins | 11 of 20 (bins 9–19) |
| Occupied bins | 9 of 20 |
| Gini of the bin-count vector | 0.90 |
| Equal-count binning | 8 bins × 25 obs = 200 |
| Quantile edges | 0.30, 0.395, 0.615, 1.07, 2.01, 4.035, 8.595, 21.50, 233.0 |
| Value widths | 0.095, 0.22, 0.455, 0.94, 2.025, 4.56, 12.905, 211.5 |
| Widest / narrowest | 2226× |

## The Anti-Pattern

20 bins for everything — one-size-fits-all. Power-law feature: first bin overwhelms, the tail bins are empty.

**Key point (red-left-border callout):** Equal-width bins on skewed data produce one overloaded bin and many empty ones — you learn nothing about the distribution's structure.

*Illustrative Example — 200 values, range 0.3 to 233, binned into 20 equal-width bins of 12 units:*

- **Bin 1 (0–12):** 160 of 200 observations = 80.0%
- **Bins 2–8:** 39 observations, thinning fast (18, 9, 5, 3, 2, 1, 1)
- **Bins 9–19:** 11 bins, zero observations each
- **Bin 20 (228–240):** 1 observation — the lone outlier at 233
- **Occupied bins:** 9 of 20 — bin-count Gini 0.90

*Domain examples:*

- Income (most clustered low, long tail to millions)
- Hospital charges (bulk under $10k, outliers to $500k)
- File sizes (many small files, few massive ones)
- Any skewed distribution with power-law or exponential tail

### Visualization (canvas `c1`, 720×300)

Histogram of `DATA` in 20 equal-width bins: the first bin dominates, eleven bins are empty, one far-right bin holds the outlier.

- **Plot area:** margins left 50, right 30, top 40, bottom 50; L-shaped axes (left + bottom) in `#2c3e50`, width 1.
- **Binning:** `DATA` is binned in JS into 20 equal-width bins over value domain [0, 240] (bin width 12). Bar heights are `counts[i] / maxCount × (plotH × 0.88)` — every height derives from the data, none is drawn by hand and none is random.
- **Bar strokes:** bin 0 outlined red `#e74c3c` width 2; other non-empty bins outlined `#1a5276` width 0.5. Fill `rgba(26,82,118,0.35)`, 1px inset.
- **Empty bins:** drawn as a dashed red (`#e74c3c`, width 0.75, dash [2,2]) segment along the baseline so the wasted bins are visible rather than invisible.
- **Y-axis ticks (11px `#666`, right-aligned):** "0" at the baseline, the computed `maxCount` (160) at the top of the tallest bar, and the word "count" above the axis.
- **First-bin label (bold 13px red `#e74c3c`, left-aligned beside the tall bar):** computed string `counts[0] + " of " + total + " = " + pct + "%"` → renders "160 of 200 = 80%".
- **Arrow:** horizontal red (`#e74c3c`, width 1.5) line from x = marginLeft + 4 bin-slots to x = plot right − 6, at y = plot bottom − 34, with a filled red arrowhead (8px back, ±5px).
- **Arrow label (12px red, centered 8px above the arrow):** computed → renders "11 of 20 bins empty · other 19 bins share 20%".
- **Gini label (bold 12px red, right-aligned above the plot):** computed Gini of the bin-count vector → renders "bin-count Gini = 0.90".
- **X-axis:** "0" and "240" endpoints (11px `#666`), plus a computed caption (12px `#666`, bottom center) → renders "Feature value — 20 equal-width bins of 12 units".

## The Design Pattern

Multi-resolution: run at multiple bin counts. Equal-count (percentile-width) bins: width encodes density.

**Key point (red-left-border callout):** Meta-distribution Gini: if > 0.7, feature needs adaptive treatment. This feature's bin-count Gini is 0.90 — well past the threshold.

- Variable-width bins — narrow where data is dense, wide where sparse
- Each bin holds the same count: 8 bins × 25 obs = 200
- Narrowest bin spans 0.095 units; widest spans 211.5 units
- Width becomes the signal — a 2226× span, same count in each
- No empty bins, no wasted bins, no information loss

### Visualization (canvas `c2`, 720×300)

Eight equal-count, variable-width bins over the same `DATA`: same height, same count, widths growing left to right.

- **Plot area:** margins left 50, right 30, top 40, bottom 60; L-shaped axes (left + bottom) in `#2c3e50`, width 1.
- **Edges:** computed in JS as quantile cuts of `DATA` at every 25th observation (midpoint of the straddling pair) → 0.30, 0.395, 0.615, 1.07, 2.01, 4.035, 8.595, 21.50, 233.0. Not a hardcoded relative-width array.
- **Counts:** re-counted from `DATA` against those edges to prove each bin holds 25; the count is printed inside every bar (bold 11px `#1a5276`, centered) whenever the bar is wider than 22px — all eight qualify.
- **Bars:** 8 contiguous bars, all height 62% of plot height, vertically centered (−4px); fill `rgba(26,82,118,0.35)`, stroke green `#27ae60` width 2, 1px inset.
- **Pixel widths:** proportional to each bin's span **in log10 value units** (`log10(edge[i+1]/edge[i])`), so a 2226× value range is legible on one canvas; computed widths ≈ 26.5, 42.6, 53.3, 60.6, 67.0, 72.7, 88.2, 229.2 px at 640px plot width.
- **Edge tick labels (10px `#666`):** the first, middle and last cut values plus the right endpoint — 0.30, 2.01, 21.50, 233.0.
- **Arrow:** horizontal `#1a5276` (width 1.5) line 26px below the plot bottom, from marginLeft+10 to plot right−10, filled `#1a5276` arrowhead at the right end.
- **Arrow label (12px `#1a5276`, centered 16px below the arrow):** computed → renders "narrow 0.095 units (dense) → wide 211.5 units (sparse) — 2226× span".
- **Top label (bold 12px green `#27ae60`, centered 12px above plot top):** computed → renders "8 bins × 25 obs = 200 — every bin equally loaded" (falls back to "counts differ" if the re-count is not uniform).
- **Axis note (11px `#666`, bottom center):** "bin edges on a log value axis (drawn width = decades spanned)" — states the scale so drawn widths are not misread as raw value widths.

## Regeneration instructions

- **Layout:** anti-pattern-pairs detail page. h1 with 2px `#2980b9` bottom border, `.subtitle` paragraph, then two `.card-section` divs ("The Anti-Pattern", "The Design Pattern"). Each section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a full-width `table.layout` with one row — `td.text-col` (45%) holding the paragraph, `.key-point` callout, optional `.example` italic lead-in and `<ul>`; `td.viz-col` (55%) holding one `<canvas width="720" height="300">`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem; table cells padding 12px, vertical-align top; canvas `width:100%`, border `1px solid #e0e0e0`, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`.
- **Determinism rule:** all chart data comes from the literal `DATA` array above. No `Math.random()` anywhere in the page, and no PRNG either — bar heights, bin counts, quantile edges, percentages and the Gini are all computed from `DATA` at render time, so the prose, the table and the chart agree to the digit on every load.
- In regenerated HTML, any card links use `.html` extensions.
