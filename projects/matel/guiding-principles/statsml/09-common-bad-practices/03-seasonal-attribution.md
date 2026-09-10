# Seasonal Attribution

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Seasonal Attribution — Common Bad Practices

**Subtitle:** Manufactured Win — Launch before the natural uptick, claim the wave as your impact.

## Section 1: The Practice

- Deploy new recommendation engine November 1. Revenue up 20% by December. "Our engine drove 20% revenue lift!" Reality: it's Christmas shopping. Growth would have happened without you.
- **Variant — Q1 signup spike:** Launch growth initiative in January when new-year signups always spike. "Initiative drove 15% DAU increase!" No — January always has a signup spike (New Year's resolutions, budget cycles, fresh starts).
- **The sophisticated version:** Know another team is about to ship a popular feature. Launch your minor change same week. Their traffic increase shows in your dashboard too.
- **Variant — post-outage recovery:** System has an outage. Ship your fix + new feature simultaneously. Recovery + improvement conflated. The natural bounce-back from the outage gets attributed to your feature.

**Why it persists:** Attribution is nearly impossible to disprove in complex systems. "We launched AND metrics improved" is technically true. The causal link is assumed, never tested. A/B test would reveal the truth — which is exactly why one wasn't run.

**The tell:** Did they run a holdout during the seasonal period? If the launch suspiciously coincides with known seasonal patterns, historical upticks, or other team's launches — it's timing gaming.

### Visualization (canvas `c1`, 720×340)

Line chart: a full-year sinusoidal seasonal revenue curve with a launch marker placed just before the December peak.

- **Padding:** left 40, right 20, top 30, bottom 40. Bottom x-axis line in `#ccc` (1px).
- **Series:** smooth sinusoid across the plot width in `#2980b9`, width 2.5; vertical center at plot mid-height, amplitude 35% of plot height, phase shifted so the peak lands at December (formula `y = midY - amp*sin(t - π/2)` with t sweeping 0→2π across the plot).
- **X labels:** month abbreviations Jan…Dec centered in 12 slots, gray `#666`, 16px sans-serif, just below the axis.
- **Launch marker:** vertical dashed red line (`#e74c3c`, dash 4/3, width 2) at month index 10 (Nov), full plot height, with bold red 16px label "LAUNCH" above it.
- **Arrow annotation:** red quadratic-curve arrow (width 1.5) from the launch point up toward the curve peak at month ~11.5; above it, bold red 18px text: `Claimed: "We caused this +20%!"`.
- **Caption below axis (gray `#666`, 16px, centered):** "Would have happened anyway (it's Christmas)".

### Visualization (canvas `c2`, 720×300)

Two-line counterfactual comparison chart.

- **Title (bold 17px `#1a5276`, top center):** "The Counterfactual".
- **Margins:** left 60, right 30, top 35, bottom 30.
- **Top series label (`#555`, 16px, left-aligned at margin):** "What happened: Launch + seasonal growth = +20%". Below it, a solid green (`#27ae60`, width 2) upward-trending line over 30 steps: starts near y=95, ends near y=55, with a small sine wiggle (`Math.sin(i*0.3)*3`).
- **Bottom series label (`#555`, 16px, left-aligned):** "Counterfactual (no launch): seasonal growth alone = +18%". Below it, a dashed orange (`#e67e22`, dash 4/3, width 2) upward-trending line over 30 steps starting near y=156, ending near y=118, same wiggle.
- **Gap annotation:** short vertical red (`#e74c3c`, width 2) segment at the right end between the two line endpoints (y 55→80); bold red 18px label to its right: "Real impact: 2%".
- **Bottom takeaway (bold red 16px, centered):** "Your real impact: 2%. Claimed impact: 20%. The seasonal wave is 90% of the story."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` with left `<td>` (40%) holding `.obj-title` "The Practice" + bullets/paragraphs, right `<td>` (60%, centered) holding both canvases stacked (`c1` 720×340 above `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; p `#333` 0.95em; ul 0.9em `#333`, li margin 6px 0; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#555`/`#999`.
