# Publish Accuracy Without Sample Size or CI

**Page type:** detail page (anti-pattern-pairs two-section layout: one `.card-section` per pattern, each with a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** Publish Accuracy Without Sample Size or CI

**Subtitle:** '99% accuracy' on 100 examples looks identical to 99% on 100,000 — the format erases the denominator

## The Anti-Pattern

Percentage format hides sample size. Small n leads to high variance, which makes a favorable number more likely to appear by chance. That favorable number then gets published without a confidence interval.

**Key point (red-left-border callout):** The same "99%" can mean wildly different things depending on whether it came from 100 or 100,000 observations.

*Domain examples:*

- ML papers reporting benchmark accuracy without dataset size or CI
- Startup pitches citing model performance on curated demo sets
- Product announcements quoting headline metrics without provenance

### Visualization (canvas `c1`, 720×300)

Side-by-side comparison diagram: two rounded boxes showing the same "99%" with wildly different confidence intervals.

- **Boxes:** each 300×180, 8px corner radius, white fill, 2px stroke; both start at y=30, centered horizontally with a 40px gap. Left box stroked red `#e74c3c`, right box stroked green `#27ae60`.
- **Box contents (both):** big centered "99%" in bold 36px `#1a5276` at y+50; below it a 14px `#666` label — left box "(n = 100)", right box "(n = 100,000)".
- **Left CI bar (wide):** at y = boxTop+100, height 20, spanning from 40px inside the left edge to 40px inside the right edge of the box; fill `rgba(231,76,60,0.2)`, 2px stroke `#e74c3c`. End labels in bold 12px `#e74c3c`: "94.6%" (left-aligned at bar left) and "99.9%" (right-aligned at bar right); centered 11px label "95% CI" below. Bottom-of-box label in bold 12px `#e67e22`: "WIDE — low confidence".
- **Right CI bar (narrow):** centered in the box, half-width 20px (40px total), same y/height; fill `rgba(39,174,96,0.2)`, 2px stroke `#27ae60`. Labels in bold 12px `#27ae60`: "98.9%" left of bar, "99.1%" right of bar, "95% CI" centered below. Bottom-of-box label in bold 12px `#27ae60`: "NARROW — high confidence".
- **Connector:** dashed red line (`#e74c3c`, dash 4/3, width 1.5) between the two boxes at mid-height, with small 11px red "vs" label above it.
- **Bottom message (bold 16px `#e74c3c`, centered, y = h-40):** "Same number, DIFFERENT knowledge!"

## The Design Pattern

Always report n, CI, and dataset provenance alongside any metric.

**Key point (red-left-border callout):** A number without context is not a measurement — it's marketing.

*Steps:*

- Report the sample size (n) prominently next to any metric
- Compute and display the confidence interval at 95% level
- State the data source and time period (provenance)
- If n is small, flag it — don't hide it behind the percentage

### Visualization (canvas `c2`, 720×300)

Single "proper reporting card" mockup showing a complete metric report.

- **Card:** rounded rect (12px radius) from (60,30) to (w-60, h-30), fill `#f0faf4`, 2px stroke `#27ae60`.
- **Contents, all centered at w/2:** "99%" in bold 52px `#1a5276` at y=100; "Accuracy" in 14px `#666` at y=120; "n = 100,000" in bold 15px `#27ae60` at y=155; "95% CI: [98.9%, 99.1%]" in 14px `#27ae60` at y=180; "Production traffic, Jan–Mar 2024" in 13px `#555` at y=205.
- **Checkmark badge:** filled green circle (`#27ae60`, radius 22) at (w/2+180, 90) with a white 3px-stroke checkmark inside (round caps/joins).
- **Bottom label (bold 13px `#1a5276`, centered, y = h-50):** "Complete reporting: metric + n + CI + provenance".

## Regeneration instructions

- **Template/layout:** anti-pattern-pairs detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, then two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"). Each section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` (width 100%, border-collapse) with one row: `td.text-col` (45%) holding a paragraph, a `.key-point` callout, an italic `.example`/bold lead-in and a `<ul>`; `td.viz-col` (55%) holding the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem. Canvas elements `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic size 720×300 via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Canvas `c1` in the HTML carries `height="300"`, `c2` likewise. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar/box fills `rgba(231,76,60,0.2)` and `rgba(39,174,96,0.2)`, gray text `#666`/`#555`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
