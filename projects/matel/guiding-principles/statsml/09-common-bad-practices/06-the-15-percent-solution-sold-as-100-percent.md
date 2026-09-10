# The 15% Solution Sold as 100% Vision

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** The 15% Solution Sold as 100% Vision — Common Bad Practices

**Subtitle:** Vision Without Evidence — Build the easy part, sell the hard 85% as 'roadmap' with no proof it's solvable.

## Section 1: The Practice

- Build the part of the problem that's straightforward (data ingestion, basic UI, simple heuristics). This handles 15% of cases. The remaining 85% requires fundamental research — maybe unsolvable with current techniques.
- **The funding trick:** The 15% "proves the approach works!" But the 15% was easy — everyone knew it was solvable. The question was always about the 85%, which has zero evidence behind it.
- **The promotion trick:** By the time the 85% fails to materialize (2 years later), the person who sold the vision has been promoted and moved on. The team left behind "couldn't execute on the vision."
- **Variant — AI demos:** "Our AI handles 15% of support tickets automatically!" Remaining 85% requires understanding context, emotion, policy exceptions. No path from 15% to 100% — but the demo deck shows a linear extrapolation to "90% by Q4."

**Why it persists:** Funding committees buy VISION not evidence. The vision narrative is compelling — "we proved Phase 1, Phase 2 is just more of the same!" It isn't, but by the time that's clear, the promotions are granted.

**The tell:** Ask "what's the evidence that the remaining 85% is technically feasible?" If the answer is "the team will figure it out" or "we'll iterate" — there IS no evidence. It's faith dressed as strategy.

### Visualization (canvas `c1`, 720×340)

Burnup line chart: promised progress vs. actual progress over months 0–24.

- **Title (bold 17px `#1a5276`, centered at y=20):** "Promised vs. Actual Progress".
- **Plot area:** left x=55, right x=700, top y=40, bottom y=290. X maps months 0–24 linearly (month m → x = 55 + m × 645/24); Y maps 0–100% linearly (percent v → y = 290 − v × 2.5).
- **Axes:** `#999` width 1 — left axis (55,40)–(55,290) and bottom axis (55,290)–(700,290).
- **Gridlines:** horizontal `#eee` width 1 across the plot at 0/25/50/75/100%; y-labels ("0%", "25%", "50%", "75%", "100%") 11px `#666`, right-aligned at x=48, baseline at gridline y+4.
- **X ticks:** months 0, 6, 12, 18, 24 with labels "0", "6", "12", "18", "24 mo" 11px `#666` centered at y=306.
- **Promised line:** dashed blue (`#1a5276`, width 2, dash 6/4) straight line from (month 0, 0%) to (month 24, 100%). Series label bold 12px `#1a5276`, left-aligned at (160, 150): "Promised: 100% by month 24".
- **Actual line:** solid red (`#e74c3c`, width 2.5) through 25 deterministic points, one per month 0–24: `[0, 6, 11, 15, 16.5, 17.5, 18.3, 18.9, 19.4, 19.8, 20.1, 20.4, 20.6, 20.8, 21.0, 21.2, 21.3, 21.4, 21.5, 21.6, 21.7, 21.8, 21.9, 21.95, 22]` (fast rise to 15% by month 3, asymptoting to ~22% by month 24). Series label bold 12px `#e74c3c`, right-aligned at (700, 222): "Actual: plateaus near 22%".
- **Funding marker:** dashed orange (`#e67e22`, width 2, dash 4/3) vertical line at month 3 (x≈135.6) from y=290 up to y=75; label 12px `#e67e22`, left-aligned at (142, 90): "funding decision made here".
- **Annotation (bold 14px `#e74c3c`, centered at (400, 265)), one line:** "Funded on the slope, never judged on the plateau."
- **Caption (italic 12px `#666`, centered at (360, 324)):** "Illustrative progress curves."

### Visualization (canvas `c2`, 720×300)

Staircase diagram: 10 ascending steps, first 3 solid green, remaining 7 dashed with question marks.

- **Title (bold 17px `#1a5276`, top center):** "The Feasibility Gap".
- **Steps:** 10 steps starting at x=40, each 60px wide and 15px tall, each 15px higher than the last (base y=170). Steps 1–3: fill `rgba(39,174,96,0.4)`, stroke `#27ae60` width 2, bold green 17px numerals "1", "2", "3" centered. Steps 4–10: dashed gray outline (`#999`, dash 4/3, width 1), gray 17px "?" centered in each.
- **Labels (bold 18px, centered at y=185):** green "15% built (proven)" under the green steps; gray `#999` "85% unproven (dashed)" under the dashed steps.
- **Gap arrow:** short red (`#e74c3c`, width 2) diagonal segment between step 3 and step 4; bold red 17px left-aligned two-line annotation: "No evidence this" / "transition is possible".
- **Promotion marker:** bold orange (`#e67e22`, 17px) centered text "PROMO" with a short tick line above step ~2.5.
- **Failure marker:** bold red 17px centered text "FAILS" with a short tick line above step ~6.5.

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` with left `<td>` (40%) holding `.obj-title` "The Practice" + bullets/paragraphs, right `<td>` (60%, centered) holding both canvases stacked (`c1` 720×340 above `c2` 720×300).
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; p `#333` 0.95em; ul 0.9em `#333`, li margin 6px 0; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#555`/`#999`.
