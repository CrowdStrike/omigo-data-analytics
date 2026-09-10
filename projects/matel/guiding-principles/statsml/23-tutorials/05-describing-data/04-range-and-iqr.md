# Range & IQR

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Range & IQR

**Subtitle:** Two ways to say "how spread out" — one is a hostage to your single worst data point, the other ignores it

**Shared data (used across charts):** the 20 sorted page loads `LOADS = [0.7, 0.8, 0.9, 1.0, 1.0, 1.0, 1.1, 1.1, 1.2, 1.2, 1.2, 1.3, 1.4, 1.6, 1.8, 2.2, 2.6, 3.2, 4.8, 60.0]` (seconds; the last is a 60s network timeout).

## One Timeout Owns the Range

**Tags:** `core idea` (blue), `running example` (green)

- **The data** — the same 20 page loads as before, but the slowest is a 60s network timeout
- **Range** = max − min = 60.0 − 0.7 = **59.3s** — one broken request owns the number
- **IQR** = spread of the middle half = Q3 − Q1 = 2.0 − 1.0 = **1.0s**
- **Shrink the timeout** — were it 6s not 60s, range falls to 5.3s; IQR stays exactly 1.0s
- **The middle 50%** of all loads live inside the IQR: between 1.0s and 2.0s

*Example:* "Load times vary by 59 seconds" and "the middle half varies by 1 second" describe the same day.

**Key point:** Range uses only the 2 most extreme points. IQR uses the middle 50% — so one freak value cannot touch it.

### Visualization (canvas `c1`, 720×300)

Full 0–62s number line — 19 dots crammed left, timeout far right; IQR box vs range arrow.

- **Title (bold 15px, `#1a5276`, top center):** "The Same 20 Loads on One Axis: Range 59.3s, IQR 1.0s".
- **Axis:** horizontal number line at y=165 from x=50 spanning 0–62s, stroke `#999`, ticks and labels at 0s, 10s, 20s, 30s, 40s, 50s, 60s (12px `#6b7280`).
- **Dots:** all 20 LOADS as radius-6 dots; values under 6s jitter ±7px vertically (alternating); blue `#2a78d6`, except the 60.0 value in red `#e74c3c`, labeled above in bold 12px red: "60s timeout".
- **IQR box:** rectangle from x=1.0s to x=2.0s, 24px tall above the line, fill `rgba(0,131,0,0.15)`, stroke green `#008300` 2px; bold 12px green label to the right: "IQR box: 1.0s to 2.0s — spans the middle half (#6-#15)".
- **Range arrow:** double-headed orange arrow (`#d95926`, width 2.5) at y=217 from x=0.7s to x=60s; bold 13px orange label below center: "range = 60.0 - 0.7 = 59.3s — one point stretches it 15x".
- **Caption (12px `#6b7280`, centered near top):** "19 of 20 loads finish under 5s; the whole story of \"spread 59.3s\" is the one red dot".

## Computing Both by Hand

**Tags:** `worked example` (green)

- **Sort** the 20 loads; min = 0.7s (position #1), max = 60.0s (position #20)
- **Range** — one subtraction: 60.0 − 0.7 = 59.3s
- **Q1** — average positions #5 and #6: (1.0 + 1.0) ÷ 2 = 1.0s
- **Q3** — average positions #15 and #16: (1.8 + 2.2) ÷ 2 = 2.0s
- **IQR** = 2.0 − 1.0 = 1.0s — positions #5–#16 never touch the timeout

*Example:* Make the timeout 600s instead of 60s: range becomes 599.3s, IQR is still exactly 1.0s.

**Key point:** The IQR calculation physically cannot see the extremes — it only reads the values a quarter and three quarters of the way in.

### Visualization (canvas `c2`, 720×300)

Sorted rank strip of 20 dots with Q1/Q3 positions highlighted; the timeout sits outside the math.

- **Title (bold 15px, `#1a5276`, top center):** "IQR Reads Positions #5-#6 and #15-#16 — Nothing Else".
- **Strip:** light gridline (`#e5e9ef`) at y=150 from x=45 to x=675, 20 evenly spaced dots. Default dots radius 6 `rgba(42,120,214,0.45)`; positions #5–#6 aqua `#199e70` radius 9; #15–#16 violet `#4a3aa7` radius 9; #20 red `#e74c3c` radius 9.
- **Shaded band:** rectangle covering positions #6 through #15 (60px tall centered on the strip), fill `rgba(0,131,0,0.10)`.
- **Brackets (2px, above the strip):** aqua bracket over #5–#6, labels bold 12px aqua "#5-#6" and "Q1: (1.0+1.0)/2 = 1.0s"; violet bracket over #15–#16, labels "#15-#16" and "Q3: (1.8+2.2)/2 = 2.0s".
- **Endpoint labels (11px `#6b7280`):** "#1" below and "0.7s" above the first dot; "#20" below the last dot with "60.0s" above it in bold 11px red.
- **Annotations (centered below the strip):** bold 13px green "IQR = Q3 - Q1 = 2.0 - 1.0 = 1.0s"; bold 12px red "position #20 could be 60s or 600s — Q1 and Q3 would never know"; 12px muted "shaded band = the middle 50% of loads (#6 through #15)".

## Box Plots and Outlier Fences Run on IQR

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Robust summaries** — IQR survives bad rows, timeouts, sensor glitches; range never does
- **Box plots** — the box IS the IQR; every box plot you have ever read is this idea
- **Outlier fence** — the common rule flags anything beyond Q3 + 1.5×IQR = 2.0 + 1.5 = 3.5s
- **Here that flags two loads** — the 4.8s one and the 60s timeout both deserve a look
- **Alerting on range** = alerting on your single worst request — noisy and unactionable

*Example:* A monitoring alert on "range > 10s" fires on every stray timeout; one on "IQR > 2s" fires when typical users actually slow down.

**Rule of thumb:** Points beyond Q1 − 1.5×IQR or Q3 + 1.5×IQR are flagged as outliers — flagged for inspection, not automatically deleted.

### Visualization (canvas `c3`, 720×300)

Box plot of the 20 loads with the 1.5×IQR fence; axis break for the 60s outlier.

- **Title (bold 15px, `#1a5276`, top center):** "The Box Plot of the 20 Loads — the Box Is the IQR".
- **Axis:** main horizontal axis at y=210 from x=60, 520px wide covering 0–6s with ticks/labels at each second (12px `#6b7280`); then a double-slash axis break, then a short outlier segment to the right labeled "60s".
- **Box plot (centered on y=150):** whiskers (ink `#1a5276`, 2px) from min 0.7s to Q1 1.0s and from Q3 2.0s to 3.2s (last point inside the fence), with end caps; box from 1.0s to 2.0s (56px tall), fill `rgba(0,131,0,0.15)`, stroke green 2px, median line at 1.2s. Labels: bold 12px green above box "box = IQR (1.0s to 2.0s)"; 12px muted below "median 1.2s".
- **Fence:** vertical dashed orange line (`#d95926`, width 2, dash 6/4) at x=3.5s, labeled bold 12px orange: "fence: Q3 + 1.5 x IQR = 3.5s".
- **Outliers:** red dots (radius 7) at 4.8s on the main axis and at the post-break "60s" position, each labeled bold 12px above ("4.8s", "60s").
- **Annotation (bold 13px red, bottom):** "beyond the fence: flagged for a look, not deleted".

## The Caution: Stable Is Not the Same as Fine

**Tags:** `common mistake` (red), `trade-off` (orange)

- **IQR stable ≠ problem solved** — the 60s timeout is real; IQR ignores it, you should not
- **Report a pair** — IQR for the typical spread AND p95 or max for the tail
- **Range is not wrong, it is fragile** — fine for small clean data like one class's test scores
- **Know what each uses** — range: 2 points; IQR: the middle 50%; std dev: all 20 points

*Example:* "Spread improved: range fell from 59.3s to 4.1s" — nothing improved; yesterday's timeout simply didn't recur.

**Common mistake:** Using IQR's stability as permission to ignore outliers. It hides them from the summary, not from your users.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: two days, one timeout apart — range swings wildly, IQR identical.

- **Title (bold 15px, `#1a5276`, top center):** "Two Days, One Timeout Apart: Which Summary Do You Trust?".
- **Axes:** L-shaped axis (`#999`), padding top 52 / bottom 56 / left 65 / right 30; y 0–65s with labels "0s", "20s", "40s", "60s" and light gridlines `#e5e9ef`.
- **Bars** (92px wide, 28px gap, extra 90px gap between groups, minimum visible height 4px; value labeled above in bold 13px `#2c3e50`, day label below in 12px muted):
  - Range group (orange `#d95926`): "Mon (no timeout)" = 4.1s; "Tue (one 60s timeout)" = 59.3s.
  - IQR group (green `#008300`): "Mon" = 1.0s; "Tue" = 1.0s.
- **Group annotations (bold 13px, below the day labels):** orange "range: 14x jump" under the range group; green "IQR: identical" under the IQR group.
- **Takeaway (bold 13px magenta `#d55181`, centered above the plot area):** "typical users had the same experience both days — only range panicked".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each starting with `<b>` term in `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, radius 4px; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
