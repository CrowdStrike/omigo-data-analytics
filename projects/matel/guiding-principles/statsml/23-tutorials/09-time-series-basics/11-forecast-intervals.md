# Forecast Intervals

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + `table.layout` two-column row, text left 50% / canvas right 50%)
**HTML title tag:** Forecast Intervals

**Subtitle:** Reporting a range instead of one number — and why that range fans out the further ahead you look

## "Between 230 and 290" Beats "Exactly 260"

**Tags:** `core idea` (blue), `uncertainty band` (green)

- **The shop** — a coffee shop forecasts Friday's cup sales to plan milk and staff
- **One number** — "we'll sell 260 cups" sounds precise but is almost surely wrong
- **A range** — "230 to 290 cups, 9 Fridays out of 10" tells the whole story
- **Both parts** — the middle (260) is the best guess; the width (±30) is the honesty
- **The name** — the range is a forecast interval; 90% is its stated coverage

*Example:* Friday came in at 274 cups — the point forecast "missed" by 14, but the interval kept its promise.

**Key point:** A forecast interval is a range plus a promise — "the actual value lands inside this band about 9 times out of 10."

### Visualization (canvas `c1`, 720×300)

Line chart of 14 days of history plus a Friday point forecast with an interval whisker and the actual outcome.

- **Title (bold 15px, `#1a5276`, top center):** "Friday's forecast: point 260, 90% interval 230-290 (illustrative)"
- **Data (`hist`):** `[242, 251, 246, 255, 248, 260, 252, 249, 257, 253, 247, 258, 262, 255]`; 3 future slots after the history, Friday is the last.
- **Axes:** y from 200 to 320, tick labels 220, 260, 300 with light `#e5e9ef` gridlines; x labeled "last 14 days" (under the history) and "Friday" (under the last slot); padding top 46, bottom 46, left 58, right 24.
- **Today divider:** dashed (4/4) gray `#6b7280` vertical line at the last history point, labeled 12px gray "today" at the top.
- **History line:** blue `#2a78d6`, width 2.5.
- **Interval whisker at Friday (drawn 14px left of the last slot):** aqua `#199e70` width-3 vertical line from 230 to 290 with 20px-wide horizontal caps at both ends; bold 12px aqua labels "290" and "230" to its left.
- **Point forecast:** radius-6 violet `#4a3aa7` dot at 260, labeled bold 12px violet "point: 260".
- **Actual outcome:** radius-5 green `#008300` dot at 274, labeled bold 12px green "actual: 274".
- **Annotation:** bold 13px aqua centered over the history: "the range IS the forecast".

## The Fan: ±20 Tomorrow, ±80 in Sixteen Days

**Tags:** `worked example` (green), `horizon` (blue)

- **One day out** — the shop's daily swings make tomorrow's band 260 ± 20 cups
- **The rule** — for a drifting series, swings pile up like √days: width 20 × √days
- **Day 4** — 20 × √4 = ±40, so the band is 220 to 300
- **Day 9** — 20 × 3 = ±60 (200 to 320); day 16: 20 × 4 = ±80 (180 to 340)
- **The shape** — plotted together the bands form a fan that widens with the horizon

*Example:* Looking 16x further ahead is only √16 = 4x wider, not 16x.

**Key point:** Uncertainty grows with the horizon — a good forecast chart fans out, and a fan that never widens at all for a drifting series is a red flag.

### Visualization (canvas `c2`, 720×300)

Fan chart: interval band 260 ± 20 × √days widening over a 16-day horizon.

- **Title (bold 15px, `#1a5276`, top center):** "The fan: band = 260 ± 20 × √days ahead"
- **Axes:** x horizon 0 to 16 days, tick labels "1d", "4d", "9d", "16d"; y from 150 to 370, tick labels 180, 260, 340 with light `#e5e9ef` gridlines; padding top 46, bottom 48, left 58, right 24.
- **Fan band:** filled region between 260 + 20√h and 260 − 20√h for h = 0..16, fill `rgba(42,120,214,0.16)`; both edges also stroked in blue `#2a78d6` width 2.
- **Center line:** dashed (6/4) violet `#4a3aa7` width-2.5 horizontal line at 260, labeled bold 12px violet "best guess: 260" at the left.
- **Whiskers:** orange `#d95926` width-2.5 vertical lines at days 1, 4, 9, 16 spanning ±20, ±40, ±60, ±80 respectively, each with a bold 12px orange label "±20", "±40", "±60", "±80" above.
- **Annotation:** bold 13px orange centered: "16x further out, only 4x wider (√days)".

## Checking the Promise: Does 80% Mean 80%?

**Tags:** `why it matters` (orange), `coverage` (blue)

- **The claim** — the shop's 1-day band is 260 ± 25 cups, stated as an 80% interval
- **The audit** — track 20 days: the actual landed inside the band on 16 of them
- **The math** — 16 / 20 = 80%; observed coverage matches the promise, band is honest
- **Decisions** — stock for the top of the band and you run out ~1 day in 10, by choice
- **Without it** — a single number gives no way to trade off waste against sellouts

*Example:* Stocking milk for exactly 260 cups meant selling out by 3pm on the 296-cup day — the band saw it coming.

**Key point:** Coverage is checkable — count how often reality lands inside the band, and compare that to the percentage the band claims.

### Visualization (canvas `c3`, 720×300)

Dot plot of 20 daily actuals against a fixed 235–285 band, inside vs outside colored.

- **Title (bold 15px, `#1a5276`, top center):** "Auditing the 80% band (260 ± 25) over 20 days"
- **Data (`act`):** `[252, 268, 247, 275, 296, 258, 241, 262, 228, 270, 255, 249, 310, 266, 244, 272, 238, 259, 230, 265]`; band lo=235, hi=285.
- **Axes:** y from 200 to 330, tick labels 220, 260, 300; x labeled "day 1", "day 5", "day 10", "day 15", "day 20"; padding top 46, bottom 46, left 58, right 24.
- **Band:** filled rectangle 235–285 across the plot in `rgba(25,158,112,0.15)`, edges dashed (5/4) aqua `#199e70` width 1.5, bold 12px aqua edge labels "285" and "235" at the left.
- **Dots:** radius 4 aqua `#199e70` when inside the band; radius 5.5 red `#e74c3c` when outside (days with 296, 228, 310, 230 — 4 dots).
- **Annotations:** bold 12px red "4 days outside" near the top; bold 13px aqua "16 of 20 inside = 80% — the band keeps its promise" near the bottom.

## Narrow Is Not Better: Confidence Theater

**Tags:** `common mistake` (red), `overconfidence` (orange)

- **The temptation** — a tight band looks smart, so teams shrink it to impress
- **Team A** — claims 90%, band ±10 cups: reality landed inside only 55% of days
- **Team B** — claims 90%, band ±30 cups: reality landed inside 90% of days
- **The verdict** — Team B's wider band is the better forecast; it keeps its promise
- **The test** — judge intervals by claimed vs observed coverage, never by width alone

*Example:* Team A's tight band won the meeting and lost the quarter — the shop sold out on 9 of 20 days.

**Key point:** An interval that misses its own promise is overconfidence in disguise — width should be earned from the data, not chosen for looks.

### Visualization (canvas `c4`, 720×300)

Two-bar chart of observed coverage vs the shared 90% claim.

- **Title (bold 15px, `#1a5276`, top center):** "Both claim 90% coverage — only one delivers it (illustrative)"
- **Axes:** y from 0 to 100%, light `#e5e9ef` gridlines with labels 25%, 50%, 75%, 100%; padding top 52, bottom 48, left 58, right 24.
- **Claimed line:** dashed (6/4) violet `#4a3aa7` width-2 horizontal line at 90%, labeled bold 12px violet "claimed: 90%".
- **Bars:** 150px wide, centered at 27% and 69% of plot width — Team A observed 55% in red `#e74c3c`, Team B observed 90% in green `#008300`; bold 13px labels "observed 55%" / "observed 90%" above the bars; 12px `#2c3e50` labels "Team A: band ±10 cups" and "Team B: band ±30 cups" below the axis.
- **In-bar annotations (bold 13px white, centered):** "tight but dishonest" inside Team A's bar; "wider and honest —" / "the better forecast" (two lines) inside Team B's bar.

## Regeneration instructions

- **Template/layout:** tutorials topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, then 4 `.card-section` blocks: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: `td.text-col` (50%) holding `.tags` pills, one-line bold-term bullets, italic `.example`, and a `.key-point` callout; `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `li b` `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Project palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all four canvases 720×300 intrinsic; scale by `window.devicePixelRatio` via a shared `setup(id)` helper; all data arrays hardcoded (the fan edges are computed deterministically from the 260 ± 20√days formula).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions (this page has none).
