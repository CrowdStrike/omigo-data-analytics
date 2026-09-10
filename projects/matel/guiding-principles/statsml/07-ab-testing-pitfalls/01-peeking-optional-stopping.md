# Peeking / Optional Stopping

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Peeking / Optional Stopping — A/B Testing Pitfalls

**Subtitle:** Statistical Sin — Checking results daily until p<0.05, then stopping.

## Section 1: The Problem

- Run test. Check p-value DAILY. Day 3: p=0.03! "Significant! Ship!" But: checking N times inflates false positive rate from 5% to 20-30%.
- At any check, random fluctuation can cross 0.05. Check daily for 14 days: P(ever p<0.05 | null true) ≈ 30%.
- **Why:** Impatience + pressure to ship + "we have significance!" Nobody remembers that α=0.05 is valid for ONE check at a pre-determined time.
- **The math:** With 14 daily checks at α=0.05, effective FPR ≈ 0.26. You're running a 26% false positive test, not a 5% one.

**Correct approach:** Fix sample size & duration BEFORE the test. Check ONCE at end. Or: use sequential testing (Bayesian, always-valid p-values, group sequential designs) built for continuous monitoring.

**The tell:** Ask "when did you decide to stop?" If answer is "when it was significant" — invalid.

### Visualization (canvas `c1`, 720×340)

Line chart: a p-value random walk over 14 daily checks, showing an early lucky dip below α.

- **Data:** p-values by day D1–D14: `[0.15, 0.11, 0.03, 0.08, 0.12, 0.09, 0.07, 0.06, 0.04, 0.08, 0.11, 0.09, 0.13, 0.10]`.
- **Axes:** y from 0.00 to 0.20 (labels at 0.00, 0.05, 0.10, 0.20); x labeled D1…D14, one point per day centered in its slot. Light gray plot background `#f9f9f9`, gray axes `#666`, padding 50px.
- **Alpha line:** horizontal dashed red (`#e74c3c`, dash 6/4, width 2) at y=0.05, labeled **α = 0.05** in bold red to the right of the plot.
- **Series:** connected line in `#1a5276`, width 2.5, with 4px-radius dots at each point; dots colored red `#e74c3c` when p<0.05 (days 3 and 9), otherwise blue `#1a5276`.
- **Annotations:** at day 3, small upward red arrow under two lines of bold red text: "Stopped here!" / "p=0.03". Near top at ~70% width, green (`#27ae60`) text: "...but p goes back above 0.05".
- **Caption (bottom center, italic gray):** "Random walk crosses any threshold if you watch long enough."

## Section 2: Real Example: Optimizely Rebuilds Its Stats

- Optimizely, a popular A/B-testing tool, showed customers a live results dashboard, and many users naturally checked it every day and stopped the test the moment their variant looked like a winner.
- That habit meant many "winners" were just lucky streaks: stopping whenever the chart looks good can turn the promised 1-in-20 fluke rate into something closer to 1-in-3.
- In 2015 Optimizely publicly rebuilt its statistics as "Stats Engine" (based on always-valid sequential testing) so that the numbers stay honest no matter how often anyone peeks.

### Visualization (canvas `c2`, 720×300)

Two-bar before/after comparison of false-winner rate with daily peeking vs Stats Engine.

- **Title (bold, top center):** "Optimizely: Same Dashboard, Honest Numbers After 2015".
- **Bars:** 150px wide, baseline at y=225, scale max 35% over 155px height.
  - Left bar at x=130: 30%, red — stroke `#e74c3c`, fill `rgba(231,76,60,0.25)`, value label "~30%" bold above bar, two-line caption below: "Before: peek daily," / "stop when green".
  - Right bar at x=440: 5%, green — stroke `#27ae60`, fill `rgba(39,174,96,0.25)`, value label "5%", caption: "After: Stats Engine," / "peek any time".
- **Baseline:** thin gray line from x=90 to x=630 at y=225.
- **Y-axis meaning:** rotated vertical gray label on the left: "chance a do-nothing change \"wins\"".
- **Takeaway (bottom center):** "The dashboard habit did not change — the math underneath was rebuilt to survive constant peeking".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, gray text `#666`/`#333`.
