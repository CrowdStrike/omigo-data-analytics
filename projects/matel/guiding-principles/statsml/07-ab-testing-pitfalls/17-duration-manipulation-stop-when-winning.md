# Duration Manipulation (Stop When Winning)

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Duration Manipulation (Stop When Winning) — A/B Testing Pitfalls

**Subtitle:** Deliberate — Asymmetric stopping: stop early when ahead, extend when behind.

## Section 1: Outcome-Dependent Stopping Rules

- Day 7: treatment winning, p=0.04. "We have significance, ship!" If day 7 showed control winning? "Need more data." Stopping rule depends on RESULT, not predetermined plan.
- Advanced: pre-register 2 weeks. At 2 weeks, losing. "Need weekly cycles — extend to 4." At 4 weeks, still losing. "Monthly billing cycle..." Keep extending until noise favors you.

**Correct approach:** Decide duration BEFORE. Symmetric stopping rule: same decision regardless of who's winning at any checkpoint.

**The tell:** Was duration decided BEFORE or AFTER seeing results? Do extensions always happen when treatment is losing?

### Visualization (canvas `c1`, 720×340)

Line chart: a fluctuating p-value over 28 days with asymmetric "STOP"/"EXTEND" decisions marked.

- **Plot area:** padding l=55, r=20, t=30, b=45; light gray axes `#ccc` (left and bottom).
- **Y-axis (p-value, approximate log-style placement):** gray `#666` 16px right-aligned labels — "0.01" near top, "0.05" at 30% of plot height, "0.10" at 50%, "0.50" at 85%; axis title "p-value" above the axis.
- **X-axis:** labels "Day 0", "Day 7", "Day 14", "Day 21", "Day 28" evenly spaced; very light vertical gridlines `#eee` at each.
- **Significance threshold:** dashed red line (`#e74c3c`, dash 4/3, width 1.5) at the 0.05 level (30% plot height), labeled "p = 0.05" in red 16px to the right of the plot.
- **Series (blue `#1a5276` line, width 2.5), points as [day, p], y positioned as p/0.55 fraction of plot height:** `[0, 0.50], [2, 0.30], [4, 0.12], [7, 0.04], [9, 0.08], [11, 0.15], [14, 0.22], [17, 0.18], [19, 0.25], [21, 0.12], [24, 0.08], [26, 0.06], [28, 0.09]`.
- **Annotations:**
  - Green filled dot (radius 6, `#27ae60`) at day 7 / p=0.04, with bold 18px green label "STOP (winning)" above it.
  - Red filled dot (radius 5, `#e74c3c`) at day 14 / p=0.22, with bold 18px red label "EXTEND (losing)" offset right/above.
- **Bottom label (bold 18px red `#e74c3c`, centered below x-axis):** "Two different rules for the same test depending on who's ahead".

## Section 2: Real Example: Peeking on A/B Testing Platforms

- Optimizely, one of the biggest A/B testing tools, noticed that many customers stopped their tests the moment the dashboard first showed a winner instead of waiting for the planned end date.
- Checking every day and stopping on the first good-looking number gives random luck many chances to be crowned a real win, so far more than the promised 1-in-20 "winners" were actually noise (this repeated checking is called peeking).
- In 2015 Optimizely publicly rebuilt its statistics engine so results stay honest no matter how often you look, which was an open admission of how common this mistake had become.

### Visualization (canvas `c2`, 720×300)

Bar chart: false-winner probability vs number of peeks (illustrative numbers).

- **Title (bold 17px `#2a2a2a`, centered):** "Each Peek Gives Noise Another Chance at a Fake Win" at y=28.
- **Bars:** labels `['check once', '5 checks', '10 checks', '30 checks']`, values `[5, 14, 19, 28]` (% chance of a false "winner" when there is no real effect). Bar width 90, gap 70, group centered; baseline y=235, scale max 30% over 160px.
  - Strokes: `#1a5276`, `#e67e22`, `#e67e22`, `#e74c3c` (width 2); fills: `rgba(26,82,118,0.35)`, `rgba(230,126,34,0.3)`, `rgba(230,126,34,0.3)`, `rgba(231,76,60,0.3)`.
  - Value labels bold 16px in the bar's stroke color above each bar, formatted "~5%", "~14%", "~19%", "~28%"; x labels 14px gray `#666` below the baseline.
- **Reference line:** dashed green (`#27ae60`, dash 5/4, width 1.5) horizontal line at the 5% level extending slightly past the bar group, labeled 14px green "promised 5%" to the right.
- **Takeaway (15px `#333`, bottom center):** "Decide the end date up front and look once — or use statistics built for peeking".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; paragraphs 0.95em `#333`; bullets 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
