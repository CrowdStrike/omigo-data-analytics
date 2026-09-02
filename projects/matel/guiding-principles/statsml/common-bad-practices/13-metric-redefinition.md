# Metric Redefinition at Measurement Time

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Metric Redefinition at Measurement Time — Common Bad Practices

**Subtitle:** Metric Manipulation — Can't hit the target? Redefine what the target measures.

## Section 1: The Practice

- Goal: 10K DAU. Actual: 6K genuine active users.
- Solution: redefine "active" to include background app pings, bot traffic, partial page loads, API health checks.
- New number: 12K. Goal "exceeded." Reality unchanged — measurement changed.

### Visualization (canvas `c1`, 720×300)

Old-definition bar vs stacked new-definition bar against the same 10K target line, showing what the inflated number is made of.

- **Layout:** baseline (chartBottom) at y=240; vertical scale 15K = 200px; bar width 100px; left bar centered at mid−180, right bar at mid+180 (mid = w/2).
- **Target line:** horizontal dashed orange line (`#e67e22`, dash 6/4, width 2) at the 10K level from x=80 to x=640; bold 15px orange label "Target: 10K DAU" left-aligned at x=80, 8px above the line.
- **Left bar (old definition):** solid green `#27ae60` (stroke `#1f8c4e` width 1.5), height = 6K. White bold 16px "6K" inside near the top. Below baseline: bold 16px green "Real users: 6K" (y=+20), 14px `#666` "Old definition" (y=+38).
- **Right bar (new definition, stacked bottom-up to 12K):** segments separated by 1px white strokes, each with a left-aligned 12px side label 8px right of the bar at the segment's vertical center:
  - Real users 6K — fill `#27ae60`, label "Real users: 6K" in green.
  - Reinstalls 2K — fill `#aab7b8` (light gray), label "Reinstalls: 2K" in `#666`.
  - Bot traffic 1.5K — fill `#e74c3c` (red), label "Bot traffic: 1.5K" in red.
  - API pings 2.5K — fill `#7f8c8d` (dark gray), label "API pings: 2.5K" in `#666`.
  - Bold 16px `#333` total "12K" centered 8px above the bar top; below baseline: bold 16px `#333` "After redefining \"active\"" (y=+20), 14px `#666` "New definition" (y=+38).
- **Insight annotation (centered at mid):** bold 16px green `#27ae60` "Only the green part is people." at y=148, with 13px `#666` "Same humans, different measurement." at y=170.
- **Caption (bottom center, italic 13px `#666`, y = h−6):** "Illustrative — the target line is unchanged; only what counts as \"active\" changed."

## Section 2: The Ratchet Effect

- Next quarter, baseline is 12K (inflated). Goal: 15K. Still achievable with same tricks + one more redefinition.
- Each quarter inflates the baseline further. The gap between "metric" and "reality" grows every cycle.

### Visualization (canvas `c2`, 720×300)

Two diverging line series over six quarters with the widening gap shaded.

- **Title (bold 16px `#1a5276`, top center):** "The Ratchet: Gap Grows Every Quarter".
- **Data:** x labels Q1–Q6; reported metric `[10, 12, 15, 18, 22, 27]`; actual reality `[10, 9, 8.5, 8, 7.5, 7]`; y scale max 30.
- **Axes:** L-shaped light gray axes (`#ccc`, width 1); chart area x from 80 to w−40, y from 45 (top) to 160 (bottom); 16px gray (`#666`) quarter labels centered under each x position.
- **Series:** reported line in `#e67e22`, width 2.5; real line in `#27ae60`, width 2.5; area between the two lines filled `rgba(231,76,60,0.1)`.
- **Legend (top right, 12px swatches + 16px `#333` text):** orange swatch "Reported metric (inflates)"; green swatch "Actual reality (declines)".
- **Gap label:** bold 16px red (`#e74c3c`) text "Gap" centered inside the shaded wedge near its wide end (x = lastX−45, y=98).
- **Caption (bottom center, italic 14px gray `#666`):** "Each quarter inflates the baseline. Reality diverges further from the number."

### Visualization (canvas `c3`, 720×300)

Mechanism view complementing `c2`'s trend view: the reported metric is a step function that jumps only at redefinition boundaries, flat in between; reality stays flat throughout.

- **Title (bold 16px `#1a5276`, centered, y=20):** "Definition Drift: The Metric Moves Only When the Definition Does".
- **Data (deterministic, index Q1 = 100):** x labels Q1–Q8; reported levels `[100, 100, 120, 120, 140, 140, 160, 160]` (vertical risers at the Q3, Q5, Q7 boundaries); reality flat at 100.
- **Axes:** chart area x = 70–650, y = 70 (top) to 215 (bottom), value range 90–170. Horizontal gridlines `#eeeeee` at 100/120/140/160 with right-aligned 11px `#666` tick labels at x=62; L-shaped `#ccc` axes; 13px `#666` quarter labels centered at y=233.
- **Reality line:** dashed red `#e74c3c` (dash 6/4, width 2.5) flat at 100 across the full width; bold 12px red label "Reality (flat)" right-aligned at chartR, 10px above the line.
- **Reported line:** solid green `#27ae60`, width 3, drawn as a step path (horizontal run, then vertical riser at each boundary where the level changes); bold 12px green label "Reported metric" right-aligned at chartR, 8px above the 160 level.
- **Step labels (bold 12px orange `#e67e22`, left-aligned 6px right of each riser, vertically centered on the riser):** Q3 "redefined \"active\""; Q5 "added reinstalls"; Q7 "counted API pings".
- **Insight annotation (bold 14px `#e74c3c`, left-aligned at x=85, two lines y=95/113):** "Every jump is a definition change —" / "zero organic growth between steps."
- **Caption (bottom center, italic 13px `#666`, y = h−6):** "Illustrative index (Q1 = 100). Flat between steps: no growth, only redefinition."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets, right `<td>` (60%, centered) holds the canvas(es). Row 1: The Practice + canvas `c1`. Row 2: The Ratchet Effect + canvases `c2` and `c3` stacked.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; gray text `#666`/`#333`/`#555`.
- **Note:** in regenerated HTML, any card links use `.html` extensions (this page has none).
