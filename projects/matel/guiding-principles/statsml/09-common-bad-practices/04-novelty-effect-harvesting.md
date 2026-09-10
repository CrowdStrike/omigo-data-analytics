# Novelty Effect Harvesting

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Novelty Effect Harvesting — Common Bad Practices

**Subtitle:** Manufactured Win — Ship anything new, declare success in the spike, leave before the decay.

## Section 1: The Pattern: Ship, Spike, Declare, Move

- ANY change produces short-term engagement lift (users explore new things). Ship redesign — engagement +15% for 2 weeks. Declare success. Move to next project. By week 6, engagement is back to baseline or below.
- **Serial novelty:** Do this 4x per year. Each time: short spike, declared win, move on. Annual review shows "4 successful launches." Nobody checks that all 4 decayed to zero.
- The "success" was already logged, the promo doc written, the credit claimed. The 2-week window is the ONLY window ever reported.

**Why it persists:** Product cycles are fast. Nobody revisits "successful" launches after 3 months. The person who shipped it has moved to a new team by then. Institutional memory of the decay doesn't exist. The incentive is to launch and move, not to sustain.

**The tell:** Ask for 90-day post-launch metrics. If they don't exist, or if "we changed too many things to isolate," the novelty decay was never measured — on purpose.

### Visualization (canvas `c1`, 720×340)

Line chart: engagement spike after launch followed by decay back to baseline over 42 days.

- **Padding:** left 50, right 30, top 30, bottom 40. X and Y axes in `#ccc` (1px).
- **Baseline:** horizontal dashed gray line (`#999`, dash 3/3, 1px) at 70% of plot height, right-aligned gray 16px label "Baseline" to its left.
- **Y-axis label:** rotated vertical gray (`#666`, 16px) "Engagement" on the left.
- **Series (green `#27ae60`, width 2.5), 42 days:** starts at baseline on day 0; days 1–14 rise quickly then plateau (`spike = 0.35*plotH * sin((d/14)*π/2)` above baseline); days 15–42 decay back to baseline (`remaining = 0.35*plotH * (1 - t²)` with t = (d-14)/28).
- **Launch marker:** solid vertical red line (`#e74c3c`, width 2) at day 0 spanning plot height; bold red 18px left-aligned label "LAUNCH" near the top.
- **Day-14 marker:** vertical dashed orange line (`#e67e22`, dash 5/4, width 1.5) at day 14, full plot height; bold orange 18px centered labels near the top: "Success declared here" and below it "+15%".
- **Decay zone label (gray `#999`, 16px, centered under the plot at the midpoint of days 14–42):** two lines: "Nobody measures here." / "You've already moved on."
- **X-axis labels (gray `#666`, 16px, centered):** "Day 0" at left edge, "Day 14" at the orange marker, "Day 42" at right edge.

### Visualization (canvas `c2`, 720×300)

Four small repeated spike-decay curves representing quarterly launches.

- **Title (bold 17px `#1a5276`, top center):** "The Serial Novelty Pattern".
- **Four panels** side by side (160px pitch starting at x=30, top y=50), labeled above in bold 18px `#555` centered: "Q1 Launch", "Q2 Launch", "Q3 Launch", "Q4 Launch".
- **Each panel:** blue curve (`#2980b9`, width 2) of an exponential spike-decay shape over 20 steps (`y = top+60 - 50*exp(-i*0.3)` — rises fast then flattens back toward baseline); dashed gray baseline (`#ccc`, dash 3/3) at panel bottom; bold green (`#27ae60`, 17px) label `"Win!"` centered below each curve.
- **Summary lines (centered):** bold 16px `#1a5276`: `Annual review: "4 successful launches"`; then bold 16px red `#e74c3c`: "Combined long-term impact: 0%. Combined launch claims: 4 wins."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` with left `<td>` (40%) holding `.obj-title` "The Pattern: Ship, Spike, Declare, Move" + bullets/paragraphs, right `<td>` (60%, centered) holding both canvases stacked (`c1` 720×340 above `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; p `#333` 0.95em; ul 0.9em `#333`, li margin 6px 0; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#555`/`#999`.
