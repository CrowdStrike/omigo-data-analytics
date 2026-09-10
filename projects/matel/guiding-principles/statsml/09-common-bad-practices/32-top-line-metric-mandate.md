# Top-Line Metric Mandate

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Top-Line Metric Mandate — Common Bad Practices

**Subtitle:** Measurement — Requiring every feature to move company-wide metrics (GMV, Revenue, DAU) regardless of feature scope or causal distance.

## Section 1: The Signal-to-Noise Problem

- A tiny UX improvement (e.g., better error message on checkout, improved tooltip, faster loading for one widget) affects maybe **2% of sessions**
- Company GMV is determined by millions of factors: marketing spend, seasonality, competitor pricing, macroeconomics
- Requiring this tiny feature to show a statistically significant lift in GMV means needing **enormous sample sizes** (millions of users x weeks) or accepting that you'll never detect the effect
- The feature might genuinely improve user experience but the signal is **buried in noise** of the top-line metric
- Result: good features get killed because "no significant impact on GMV" — which is a **power problem**, not an effectiveness problem
- Teams learn to only build big-swing features, ignoring the death-by-a-thousand-cuts problems that erode product quality

### Visualization (canvas `canvas1`, 720×400)

Signal-detection diagram: horizontal bands comparing a tiny feature effect against GMV noise and MDE thresholds.

- **Background:** `#fafafa` fill over whole canvas.
- **Title (bold 14px, `#1a5276`, top center):** "Signal Detection: Feature Effect vs GMV Noise".
- **Axes:** L-shaped `#999` axes; plot area from x=80 to width−40, y=50 to height−80. X-axis label (`#666` 12px, centered): "Time (weeks of experiment)". Y-axis label (rotated −90°, `#666` 12px): "GMV Change (%)".
- **Noise band:** large gray band centered vertically, 70% of plot height, fill `rgba(200,200,200,0.4)`, dashed `#ccc` borders (dash 5/3); right-aligned `#999` 11px label above band top: "GMV noise band (±3.5%)".
- **MDE line:** horizontal dashed red line (`#e74c3c`, dash 8/4, width 2) at 18% of plot height above center; bold red 11px right-aligned label: "MDE = ±1.2% (n=2M, 4 weeks)".
- **Feature effect line:** solid green (`#27ae60`, width 2.5) horizontal line only 2.5% of plot height above center; bold green 11px left label: "Actual feature effect = +0.04%".
- **Baseline:** dotted `#666` line (dash 2/2) at vertical center, labeled "baseline (0%)" in `#666` 10px.
- **Annotation box:** rounded rect (320×50, radius 6) near bottom left of plot, fill `rgba(231,76,60,0.08)`, stroke `#e74c3c` 1.5px; centered bold red 12px line "To detect +0.04% in GMV:" and 12px line "Required: ~250M sessions over 12+ weeks".
- **Gap arrow:** vertical double-headed orange (`#e67e22`, width 2) arrow between the effect line and the MDE line at x = plot-left + 40, labeled "30x gap" in bold 10px `#e67e22`. (Source draws the label twice — once with a garbled y position, once correctly at the arrow midpoint; regenerate with the single correct midpoint label.)

## Section 2: Proxy Metrics and Causal Distance

- The correct approach: measure what the feature **actually changes** (the proximate metric), then validate the proxy-to-topline relationship separately
- Example hierarchy: tooltip improvement → task completion rate → user satisfaction → retention → revenue. **Measure at the first link, not the last.**
- Proxy metric criteria: (1) causally connected to top-line, (2) sensitive enough to detect the feature's effect, (3) fast enough to measure in reasonable time
- The org should maintain a **validated metric hierarchy / causal graph** showing how proxies connect to top-line, validated periodically with observational data
- Anti-pattern: treating "we can't measure GMV impact" as "this feature doesn't matter" — it's a **measurement limitation**, not a value statement

### Visualization (canvas `canvas2`, 720×400)

Metric-hierarchy diagram: four stacked levels connected by upward arrows, with detection-sensitivity gauges on the right.

- **Background:** `#fafafa` fill.
- **Title (bold 14px, `#1a5276`, top center):** "Metric Hierarchy: Measure at the Right Level".
- **Levels (rounded boxes 220×44, radius 8, stacked bottom-to-top, connected by gray `#aaa` arrows pointing upward), each with a bold 12px label in its color and a `#666` 10px sublabel:**
  | Level (bottom→top) | Label | Sublabel | Color | Sensitivity |
  |---|---|---|---|---|
  | 1 | Feature Change | (tooltip redesign) | `#1a5276` | 95% |
  | 2 | Proximate Metric | (task completion rate) | `#27ae60` | 82% |
  | 3 | Intermediate Metric | (user satisfaction score) | `#e67e22` | 35% |
  | 4 | Top-Line Metric | (GMV / Revenue) | `#e74c3c` | 3% |
- Box fills: Proximate Metric `rgba(39,174,96,0.1)` with thicker 2.5px stroke; Top-Line `rgba(231,76,60,0.08)`; others `rgba(26,82,118,0.05)`. Stroke color = level color.
- **Sensitivity gauges (right column):** heading "Detection Sensitivity" (bold 11px `#1a5276`); for each level a 120×14 rounded gauge, `#eee` background, fill proportional to sensitivity, colored green `#27ae60` (>60%), orange `#e67e22` (20–60%), red `#e74c3c` (<20%); percentage label ("95%", "82%", "35%", "3%") in `#333` 10px to the right.
- **"Measure HERE" annotation:** at the Proximate Metric level (left side), a circle with green checkmark (fill `rgba(39,174,96,0.15)`, stroke `#27ae60` 2.5px), with bold 11px `#27ae60` two-line label "Measure" / "HERE".
- **"NOT here" annotation:** at the Top-Line level, a circle with red X (fill `rgba(231,76,60,0.1)`, stroke `#e74c3c` 2.5px), with bold 11px `#e74c3c` two-line label "NOT" / "here".
- **Causal distance annotation:** `#999` 10px text "increasing causal distance" near top right with a small upward `#bbb` arrow.

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets, right `<td>` (60%, centered) holds the canvas. Single table with two rows.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic size 720×400 for both; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). This page inlines the dpr setup per canvas rather than a shared helper; either style works.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#666`/`#999`/`#333`.
- Note: in regenerated HTML, any card/page links use `.html` extensions.
