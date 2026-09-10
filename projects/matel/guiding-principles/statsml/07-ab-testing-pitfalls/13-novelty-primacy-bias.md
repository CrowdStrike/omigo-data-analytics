# Novelty / Primacy Bias

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Novelty / Primacy Bias — A/B Testing Pitfalls

**Subtitle:** Design Flaw — Anything new gets explored. You measured curiosity, not preference.

## Section 1: Temporal Confound: Measuring Transition, Not Equilibrium

- ANY change → users explore → 2-week spike. That's novelty, not improvement. By week 4, habituate → back to baseline.
- Reverse: primacy bias. Users prefer familiar. Treatment looks WORSE initially (change aversion). Given 4 more weeks, treatment wins.
- Result depends ENTIRELY on when you measure. 2 weeks = novelty. 6 weeks = true preference.

**Correct approach:** Run long enough for novelty/primacy to wash out (4+ weeks). Plot effect over time — if decaying or growing, you're in transition, not equilibrium.

**The tell:** Plot treatment effect by day/week. If it decays → novelty. If it grows → primacy. Flat → true effect.

### Visualization (canvas `c1`, 720×340)

Line chart: treatment effect over 28 days, spiking early and decaying to a small true effect.

- **Padding:** left 60, right 30, top 30, bottom 50. Axes drawn as light gray `#ccc` L-shape (left + bottom).
- **Y-axis labels (16px gray `#666`, right-aligned):** "+15%" near top, "+10%" at 35% plot height, "+5%" at 60%, "+2%" at 75%, "0%" at bottom. Y scale maps effect values against a max of 0.16.
- **X-axis labels (centered):** "Day 0", "Day 7", "Day 14", "Day 21", "Day 28" at fractions 0, 0.25, 0.5, 0.75, 1.0 of plot width.
- **Effect line (blue `#1a5276`, width 3):** points as (x-fraction, effect) pairs: `[0, 0.05], [0.07, 0.08], [0.14, 0.12], [0.21, 0.15], [0.28, 0.14], [0.35, 0.11], [0.42, 0.08], [0.5, 0.06], [0.57, 0.05], [0.64, 0.04], [0.71, 0.03], [0.78, 0.025], [0.85, 0.02], [0.92, 0.02], [1.0, 0.02]`.
- **Day-14 marker:** vertical red `#e74c3c` dashed line (dash 5/4, width 2) at x-fraction 0.5, full plot height; bold 18px red label just right of it near the top: "Most tests stop HERE".
- **True-effect label:** bold 18px green `#27ae60` at x-fraction ~0.6, near the bottom of the plot: "True effect is HERE → +2%".
- **Novelty spike label:** 16px orange `#e67e22`, centered at x-fraction ~0.2 near the top: "Novelty spike".
- **X-axis title (16px gray, centered below axis):** "Treatment Effect Over Time".

## Section 2: Real Example: Microsoft's Fading Feature Wins

- Ron Kohavi, who ran the experimentation platform at Microsoft and Bing, documented that brand-new features often win big in their first days simply because anything new draws attention — users click on it just to see what it is.
- When his team plotted the lift day by day instead of reading one summary number, many early "wins" kept shrinking week after week as the newness wore off (a novelty effect).
- The practical habit they built is to run tests for several weeks and watch the trend: a real improvement stays flat over time, while curiosity decays back toward zero.

### Visualization (canvas `c2`, 720×300)

Two-bar comparison of click lift in week 1 vs week 4 with a decay arrow between them.

- **Title (bold 17px `#2a2a2a`, centered at (360, 26)):** "New Feature Click Lift: Week 1 vs Week 4 (illustrative)".
- **Baseline:** thin gray `#999` line from x=100 to x=640 at y=225.
- **Week 1 bar:** at x=180, 120px wide, 160px tall above baseline; fill `rgba(39,174,96,0.35)`, stroke `#27ae60` width 2; value label "+12%" bold 18px green above the bar; captions below baseline in 15px gray `#666`: "Week 1" and "(\"Ship it!\")".
- **Week 4 bar:** at x=440, 120px wide, only 14px tall; fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 2; value label "+1%" bold 18px blue above; captions: "Week 4" and "(novelty gone)".
- **Decay arrow:** red `#e74c3c` dashed quadratic curve (dash 6/4, width 2) from the top of the week-1 bar (310, y=75) sweeping down to the week-4 bar top (435, y=197), ending in a small solid red arrowhead; bold 15px red label "curiosity fades" centered at (380, y=110).
- **Takeaway (bold 16px red, centered at (360, h−12)):** "Week 1 measured curiosity; week 4 is the real effect".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- **Links:** none on this page; if this spec is linked from a grid, regenerated HTML card links use `.html` extensions.
