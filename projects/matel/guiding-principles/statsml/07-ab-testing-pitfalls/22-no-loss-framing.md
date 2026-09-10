# No-Loss Framing

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** No-Loss Framing — A/B Testing Pitfalls

**Subtitle:** Organizational — Test wins or "we learned something." Both outcomes celebrated. Zero accountability.

## Section 1: Organizational — Test wins or "we learned something." Both outcomes celebrated. Zero accountability.

- Win → "Ship! Data-driven win!" Lose → "Valuable learning! Saved us!" Either way, "win."
- If program can never fail → zero accountability. "Learning" from negatives never quantified.
- Nobody tracks "tests that said don't do X and we actually didn't."
- A/B testing's value = PREVENTING bad launches. If negatives are reframed and launched anyway, prevention disabled.

**Correct approach:** Track decision-reversal rate. Healthy: 30-50% of tests = no-ship. If <10% — tests aren't gates.

**The tell:** How many tests resulted in NO launch? If <10% — tests have zero authority.

### Visualization (canvas `c1`, 720×340)

Converging-paths flow diagram on light gray background (`#f8f9fa`): two outcome boxes both funnel into "Ship anyway" then "Celebrate!".

- **Left path:** bold 16px green (`#27ae60`) header "Test Wins" at (x=180, y=40); below it a 160×30 box at x=120, y=48, fill `rgba(39,174,96,0.15)`, stroke `#27ae60` width 1.5, containing 16px green text "\"Ship! Data-driven win!\"".
- **Right path:** bold 16px red (`#e74c3c`) header "Test Loses" at (x=w-180, y=40); below it a 160×30 box at x=w-280, y=48, fill `rgba(231,76,60,0.15)`, stroke `#e74c3c`, containing 16px red text "\"Valuable learning!\"".
- **Converging arrows:** gray `#555` width 1.5 lines from the bottom of each box down to the center point (w/2, 120).
- **Middle box:** 160×35 at (w/2−80, 120), fill `rgba(230,126,34,0.2)`, stroke `#e67e22` width 2, bold 16px orange centered text "Ship anyway".
- **Arrow down** from the middle box to y=190, then bold 17px green centered text "Celebrate!" at y=205.
- **Bottom label (17px `#555`, centered, 10px above bottom):** "A program that can never fail has zero actual authority."

## Section 2: Illustrative Example: The Wins-Only Highlight Reel

- At a large tech company, teams that ran winning tests presented them proudly at all-hands meetings, while losing tests were quietly filed away in reports that nobody read.
- After a year of hearing only success stories, leadership concluded that the testing program "always works" and stopped asking hard questions about launches. (Hearing only the successes and never the failures is known as publication bias.)
- When someone finally counted every test that had actually run, more than half had been flat or negative — the losses existed all along, they just never traveled upward.

### Visualization (canvas `c2`, 720×300)

Two grids of colored squares comparing all tests run vs what leadership hears, on light gray background (`#f8f9fa`).

- **Title (bold 17px `#2a2a2a`, centered at y=30):** "Wins Travel Up, Losses Stay Buried".
- **Square grids:** 30px squares, 8px gap, 5 per row, stroke width 2; wins fill `rgba(39,174,96,0.35)` stroke `#27ae60`, losses fill `rgba(231,76,60,0.35)` stroke `#e74c3c`.
  - Left grid at (72, 95): 6 green + 8 red squares. Header (bold 15px `#1a5276`, centered at x=163, y=80): "All tests actually run".
  - Right grid at (452, 95): 6 green squares only. Header (bold 15px `#1a5276`, centered at x=543, y=80): "What leadership hears".
- **Arrow between grids:** orange `#e67e22` line width 2.5 from (285, 150) to (420, 150) with filled arrowhead; label above it (14px orange, centered at x=352, y=135): "losses filtered out".
- **Takeaway (15px `#555`, centered, 15px above bottom):** "Leadership sees a program that never loses — because the losses never travel upward."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`/`#555`.
