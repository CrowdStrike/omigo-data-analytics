# Model Rot & Refresh Theater

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvases right ~60%, single row)
**HTML title tag:** 1. Model Rot & Refresh Theater — Common Bad Practices

**Subtitle:** Manufactured Win — Deliberately not refreshing a model so you can claim a big improvement later.

## Section: "We Improved Model Accuracy by 8%!" — No, You Let It Rot for 6 Months Then Caught Up

- **The practice:** Don't retrain the model for 6 months. Data drift degrades performance slowly (0.1%/week — nobody notices gradual decay). Then "refresh" it, claim 8% improvement. Write it up as a launch. The improvement isn't innovation — it's catching up to where you should have been all along with regular maintenance.
- **Variant — monitoring theater:** You HAD monitoring. You SAW the decay happening week over week. You WAITED until the gap was large enough to be impressive before acting. The "discovery" of degradation was actually a scheduled reveal timed to performance review season.
- **Variant — pipeline bugs:** Let pipeline bugs accumulate quietly. Bugs introduce noise, noise degrades quality. Fix them all at once. "Major reliability improvement — incidents down 70%!" You manufactured 70% of those incidents by neglect.
- **Variant — feature hoarding:** New features have been ready for months. Data team built them in Q1. You wait until Q3 performance review to integrate them. "Improved accuracy by 12% with new feature engineering!" The features existed — you just hoarded the launch timing.

**Why it persists:** Incremental maintenance (0.1% improvement every week) is INVISIBLE in performance reviews. A single 8% jump is a STORY — it has a before/after, it fits a launch narrative, it sounds like innovation not maintenance. The system rewards neglect-then-heroics over steady stewardship.

**The tell:** Ask for the maintenance schedule. If there isn't one, or if "improvements" always coincide with review cycles, the wins are manufactured. Compare: time since last refresh vs. magnitude of claimed improvement. Long gap + large improvement = manufactured neglect.

### Visualization (canvas `c1`, 720×340)

Line chart: accuracy decaying over months, then a sudden vertical "refresh" jump, contrasted with a steady-maintenance reference line.

- **Title (bold 17px, top center, `#1a5276`):** "Manufactured Win: Neglect → Decay → \"Improvement\"".
- **Chart area:** margins left 60, right 40, top 45, bottom 35; gray `#ccc` L-shaped axes.
- **Axis labels (17px `#555`):** "Months" centered below the x-axis; "Accuracy" rotated vertical on the left.
- **Decay line (`#e74c3c`, width 2):** 25 points i=0..24 across the plot width, y = top + 10 + i×5 + sin(i)×2 px — a slow noisy decline from near the top to the bottom of the plot.
- **Jump:** at month 24, a vertical green (`#27ae60`, width 3) segment from the end of the decay line up to y = top+15, with bold 17px green label "\"8% improvement!\"" placed near the top right (x = right edge − 100, y = top+30).
- **Steady maintenance reference:** dashed blue (`#2980b9`, dash 4/3, width 2) horizontal line at y = top+12 across the full plot width, with 17px blue centered label "With regular maintenance: always at peak" near the bottom of the plot.
- **Annotation (17px `#e74c3c`, left-aligned, lower left of plot):** "Deliberate neglect: slow decay nobody notices".

### Visualization (canvas `c2`, 720×340)

Two-year sawtooth line chart: accuracy decays 0.3%/week from 94%, gets "refreshed" back to 94% every 26 weeks (four claimed "+8% wins"), contrasted with a flat dashed green continuously-maintained line at 94%.

- **Title (bold 17px, top center at (w/2, 20), `#1a5276`):** "Two Years of Refresh Theater vs Steady Maintenance".
- **Legend row (12px, left-aligned text, line segments at y=32, text baseline y=36):** green dashed (`#27ae60`, dash 6/4, width 2) segment x=164–192 then `#27ae60` text "Continuously maintained" at x=198; red (`#e74c3c`, width 2) segment x=371–399 then `#e74c3c` text "Neglect + refresh cycle" at x=405.
- **Plot area:** left x=55, right x=675, top y=46, bottom y=262. Linear mappings: x(week) = 55 + 620×week/104 for weeks 0–104; y(v) = 46 + 216×(96 − v)/12 for accuracy 84–96%.
- **Gridlines & y ticks:** horizontal `#e8e8e8` (width 1) gridlines at v = 84, 86, 88, 90, 92, 94, 96; 12px `#555` right-aligned tick labels "84%"…"96%" at x=48, y offset +4. Gray `#ccc` L-shaped axes (left + bottom of plot).
- **x ticks:** 12px `#555` centered labels 0, 26, 52, 78, 104 at their x(week) positions, baseline y=280; 13px `#555` centered axis label "Weeks" at ((55+675)/2, 298); 13px `#555` rotated (−90°) label "Accuracy (%)" at (16, plot mid-height).
- **Maintained line:** dashed green (`#27ae60`, dash 6/4, width 2) horizontal line at v=94 (y=82) across the full plot width.
- **Sawtooth (deterministic, no randomness):** four cycles c = 0..3. Decay segment per cycle: red (`#e74c3c`, width 2) straight line from (x(26c), y(94)) to (x(26(c+1)), y(86.2)) — i.e., accuracy = 94 − 0.3 × weeks-since-last-refresh, bottoming at 86.2%. Refresh snap per cycle: orange (`#e67e22`, width 2.5) vertical segment at weeks 26/52/78/104 from y(86.2) up to y(94), each labeled bold 12px `#e67e22` "+8% win" centered above the snap at baseline y=74.
- **Annotation (bold 14px `#e74c3c`, centered at ((55+675)/2, 248)):** "4 claimed wins. Cumulative gain over steady maintenance: negative."
- **Caption (italic 12px `#666`, centered at ((55+675)/2, 320)):** "Illustrative accuracy trajectory."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title` + `ul` bullets + two `<p><strong>…</strong></p>` paragraphs, right `<td>` (60%, centered) holds the two canvases stacked.
- **Page style:** body `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif`, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `ul` 0.9em; canvases `display: block; margin: 0 auto`.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart text uses `-apple-system` at 16–17px. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, gray text `#555`/`#666`.
- No nav bar, no back/home links. (In regenerated HTML, any card links elsewhere use `.html` extensions; this detail page has no links.)
