# Moving Averages

**Page type:** detail page (tutorial card-sections: h2 per section, two-column layout table — text left 50% with tag pills/bullets/example/key-point, canvas right 50%)
**HTML title tag:** Moving Averages

**Subtitle:** Average the last few days together and a jumpy line calms down enough to show its direction — at the cost of hearing the news a little late

## Daily Traffic Jumps Around — the Average Calms It Down

**Tags:** `core idea` (blue), `running example` (green)

- **The raw line** — daily visitors bounce between 770 and 1,173; any single day says little
- **The trick** — replace each day with the average of the last 7 days, itself included
- **Moving** — the window slides forward one day at a time, so the average moves with it
- **What appears** — a steady climb from about 920 to 1,100 that the daily jumps were hiding
- **The name** — this smoothed line is called a moving average, or MA for short

*Example:* On day 42 traffic read 933 — a scary drop, until the 7-day average showed 1,096 and rising.

**Key point:** A moving average swaps the noisy daily value for the average of a small recent window — the direction becomes visible.

### Visualization (canvas `c1`, 720×300)

Line chart: 42 days of raw daily visitors with the 7-day moving average overlaid.

- **Title (bold 15px, `#1a5276`, top center):** "Daily visitors vs the 7-day moving average (42 days)"
- **Data (42 daily visitor counts):** `[950,1014,931,999,920,850,806,998,983,923,1071,1019,882,770,1003,1064,1018,1086,984,900,861,1090,1063,987,1115,1083,974,848,1065,1106,1092,1173,1062,954,905,1171,1148,1066,1161,1135,1055,933]`
- **MA series:** trailing 7-day average of the same array (null for the first 6 days, line starts at day 7)
- **Axes:** y from 700 to 1250 with gridlines/labels at 800, 1000, 1200 (gray `#6b7280`, gridlines `#e5e9ef`); L-shaped axis frame `#999`; padding l:58 r:20 t:46 b:44; x labels "day 1", "day 14", "day 28", "day 42"
- **Raw series:** line `rgba(42,120,214,0.55)` width 1.5
- **MA series style:** green `#008300` line width 3
- **Annotations:** bold 13px green, left-aligned near top: "7-day average: a steady climb, ~920 to ~1,100"; bold 12px blue `#2a78d6` near bottom of plot: "raw days: jumps of 200+ hide the climb"
- **Caption (12px gray, bottom center):** "visitors per day (illustrative)"

## One Week by Hand: Seven Numbers, One Average

**Tags:** `worked example` (green), `do it by hand` (blue)

- **The week** — visitors were 980, 1030, 1010, 950, 1070, 890, 1070
- **Add them** — the seven days sum to exactly 7,000
- **Divide by 7** — 7000 / 7 = 1000; that is the moving-average value for day 7
- **Slide** — tomorrow, drop the 980, add the new day, and divide by 7 again
- **Every point** — each point on the smooth line is just this: one window, one average

*Example:* If day 8 brings 1,120 visitors, the new window sums to 7,140 and the average moves to 1,020.

**Key point:** There is no magic — every value of a 7-day moving average is the plain average of the 7 most recent days.

### Visualization (canvas `c2`, 720×300)

Bar chart: the worked week's seven bars with a dashed average line at 1000.

- **Title (bold 15px, `#1a5276`, top center):** "One 7-day window: (980+1030+1010+950+1070+890+1070) ÷ 7 = 1000"
- **Data:** `[980, 1030, 1010, 950, 1070, 890, 1070]`, day labels `['Mon','Tue','Wed','Thu','Fri','Sat','Sun']`
- **Axes:** y scale 0 to 1200; baseline gray `#999`; padding t:56 b:44 l:58 r:20
- **Bars:** 56px wide, evenly spaced; fill `rgba(42,120,214,0.4)` with blue `#2a78d6` outline; 12px value label above each bar, gray day label below
- **Average line:** dashed magenta `#d55181` (dash 7/5, width 2.5) horizontal at 1000, full plot width
- **Annotation (bold 13px magenta, left-aligned above the line):** "average = 1000: the MA point for this window"

## Smoother Is Slower: the Drop Shows Up Late

**Tags:** `why it matters` (orange), `lag` (red)

- **The event** — on day 31 traffic really fell, from about 1,000 a day to about 700
- **7-day MA** — needs about 7 days to register it fully; reads 703 by day 37
- **30-day MA** — still reads 852 at day 45, and only reaches 701 at day 60
- **The rule** — a window of N days needs about N days to fully absorb a change
- **The cost** — the smoothest dashboards are the last to admit something happened

*Example:* A team watching only the 30-day average debated a "slight dip" two weeks after traffic had crashed 30%.

**Key point:** Smoothing always adds delay (lag) — the wider the window, the calmer the line, and the later it tells you the news.

### Visualization (canvas `c3`, 720×300)

Line chart: 60-day series with a real drop after day 30, overlaid with 7-day and 30-day moving averages showing lag.

- **Title (bold 15px, `#1a5276`, top center):** "Traffic drops on day 31 — each average hears about it late"
- **Data (60 days, drop after day 30):** `[1000,1057,984,1039,997,931,1019,983,1004,1072,974,994,999,932,1033,1027,994,1056,962,958,1018,960,1039,1051,968,1022,965,948,1047,995,724,748,641,692,693,661,765,716,686,729,635,681,735,680,756,718,646,713,660,686,768,689,719,710,625,713,704,692,773,683]`
- **Axes:** y from 550 to 1150 with labels at 700, 900, 1100; L-shaped axis frame `#999`; padding t:46 b:58 l:58 r:20; x labels "day 1", "day 15", "day 31", "day 45", "day 60"
- **Drop marker:** dashed red `#e74c3c` vertical line (dash 5/4, width 1.5) at day 31 with bold 12px red label "real drop: day 31"
- **Raw series:** faint line `rgba(42,120,214,0.35)` width 1.5
- **MA series:** trailing 7-day MA in green `#008300` and trailing 30-day MA in orange `#d95926`, both width 3 (each starts once its window fills)
- **Annotations:** bold 12px green: "7-day: reads 703 by day 37"; bold 13px orange: "30-day: still 852 at day 45"
- **Caption (12px gray, bottom center):** "faint line = raw daily visitors (illustrative)"

## 3, 7 or 30 Days? The Window Is a Choice, Not a Truth

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Same data** — one traffic series, three windows, three different-looking stories
- **3-day** — still jittery; reacts fast but keeps much of the noise
- **7-day** — matches the weekly rhythm of visits; a common default for daily data
- **30-day** — very calm, very late; short wiggles disappear entirely
- **No "correct" window** — pick it for the pattern you care about, and say which you used

*Example:* The same week looked "down" on the 3-day average and "up" on the 30-day — both charts were honest.

**Key point:** The window length is a dial you set, not a property of the data — always report which window a smoothed chart uses.

### Visualization (canvas `c4`, 720×300)

Line chart: the 42-day traffic series with 3-day, 7-day and 30-day moving averages overlaid, plus a legend.

- **Title (bold 15px, `#1a5276`, top center):** "Same traffic, three windows: 3-day, 7-day, 30-day"
- **Data:** same 42-day `traffic42` array as canvas `c1`
- **Axes:** y from 700 to 1250 with labels at 800, 1000, 1200; L-shaped axis frame `#999`; padding t:46 b:44 l:58 r:130 (extra right space for the legend); x labels "day 1", "day 14", "day 28", "day 42"
- **Raw series:** faint gray line `rgba(107,114,128,0.3)` width 1
- **MA series (each width 2.5, starting once its window fills):** 3-day in aqua `#199e70`, 7-day in green `#008300`, 30-day in orange `#d95926`
- **Legend (right side, 12px, color swatch squares):** "3-day MA", "7-day MA", "30-day MA", plus a gray swatch "raw days"
- **Annotation (bold 13px violet `#4a3aa7`, left-aligned near bottom of plot):** "three windows, three stories — none is \"the\" truth"

## Regeneration instructions

- **Layout:** tutorial detail page — `<h1>` + `.subtitle`, then four `.card-section` blocks, each an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) followed by `table.layout` (full width, one row): left `td.text-col` 50% with `.tags` pill row, a `<ul>` of one-line bullets each opening with a `<b>` term (bold terms colored `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` 50% holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Charts:** shared JS palette `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; site palette #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange. Canvases declare intrinsic 720×300 and scale by `window.devicePixelRatio` via a shared `setup(id)` helper that reads the `width`/`height` attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `movAvg(a, win)` helper computes trailing moving averages (null until the window fills). Both data arrays (`traffic42`, `drop60`) are hardcoded literal arrays labeled "illustrative" in captions — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
