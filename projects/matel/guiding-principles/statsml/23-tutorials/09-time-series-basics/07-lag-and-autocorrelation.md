# Lag & Autocorrelation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + `table.layout` two-column row, text left 50% / canvas right 50%)
**HTML title tag:** Lag & Autocorrelation

**Subtitle:** Compare a series with a shifted copy of itself — if yesterday predicts today, the series has a memory, and the bars show its rhythm

## Today's Traffic Looks a Lot Like Yesterday's

**Tags:** `core idea` (blue), `running example` (green)

- **The question** — does knowing yesterday's visitor count tell you anything about today's?
- **The trick** — pair each day with the day before: (yesterday, today) gives 29 pairs from 30 days
- **Lag** — that one-day shift is called a lag; these pairs use lag 1
- **The scatter** — the pairs hug the diagonal: high days follow high days, low follow low
- **The number** — the correlation of these pairs is 0.87; that is the lag-1 autocorrelation

*Example:* Yesterday brought 1,142 visitors, so nobody expected today's count to be far away — it was 1,116.

**Key point:** Autocorrelation is just ordinary correlation between the series and a time-shifted copy of itself.

### Visualization (canvas `c1`, 720×300)

Scatter plot of lag-1 pairs from 30 days of drifting traffic.

- **Title (bold 15px, `#1a5276`, top center):** "Each dot is one day: yesterday's visitors vs today's (29 pairs)"
- **Data (30-day series `drift30`, comment: "30 days of slowly drifting traffic (illustrative); lag-1 correlation = 0.87"):** `[1000,1065,1062,1090,1142,1116,1081,1093,1052,974,962,944,877,871,907,891,903,980,1011,1018,1086,1125,1098,1111,1128,1066,1019,1018,961,896]`. Points plotted as (previous day, current day) for i=1..29.
- **Axes:** both x and y span 840–1180; tick labels 900, 1000, 1100 on both axes; L-shaped gray `#999` axes; padding top 46, bottom 48, left 70, right 30.
- **Diagonal:** dashed (5/4) light gray `#e5e9ef` line from (lo,lo) to (hi,hi), width 1.5.
- **Dots:** radius 4.5, fill `rgba(42,120,214,0.75)`.
- **Annotation:** bold 13px green `#008300` text near top-left of plot: "r = 0.87 — yesterday predicts today".
- **Axis labels (12px gray `#6b7280`):** "yesterday's visitors" bottom center; "today's visitors" rotated vertical at left.

## Shift by 7 Days and the Weekend Echo Appears

**Tags:** `worked example` (green), `do it by hand` (blue)

- **Four weeks** — 28 days of traffic; every Saturday dips: 750, 799, 755, 790
- **Every Friday peaks** — 1088, 1053, 1097, 1048; the week repeats like a drumbeat
- **By hand** — mark each day above or below the overall average of 959
- **Lag 7** — compare each day with 7 days earlier: the above/below sign matches 21 of 21 times
- **Lag 3** — the same check at a 3-day shift matches only 11 of 25 times — a coin flip

*Example:* If last Saturday was a slow day, betting that this Saturday would be slow too paid off every single week.

**Key point:** A perfect 21-of-21 match at lag 7 is the weekly rhythm speaking — the series echoes itself every 7 days.

### Visualization (canvas `c2`, 720×300)

Line chart of 28 days of traffic with Fridays and Saturdays highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Four weeks of traffic: the same shape repeats every 7 days"
- **Data (`weekly28`, comment: "28 days with a weekly rhythm (illustrative); overall average 959"):** `[1000,1062,1001,1054,1088,750,763,1008,1018,1045,1046,1053,799,741,986,1067,1010,1039,1097,755,749,1021,1018,1033,1060,1048,790,756]`
- **Axes:** y from 680 to 1180, tick labels 800 and 1000; x labeled "day 1", "day 8", "day 15", "day 22", "day 28"; padding top 46, bottom 44, left 58, right 118.
- **Average line:** dashed (6/5) gray `#6b7280` horizontal line at 959, width 1.5, labeled "average 959" (12px gray, left-aligned above the line).
- **Series:** blue `#2a78d6` line, width 2.
- **Markers:** Fridays (indices 4, 11, 18, 25) as radius-6 green `#008300` dots; Saturdays (indices 5, 12, 19, 26) as radius-6 magenta `#d55181` dots.
- **Legend (right side, 12px):** green dot "Fridays", magenta dot "Saturdays".
- **Annotation:** bold 13px magenta `#d55181` centered near the bottom of the plot: "every Saturday dips below 800 — a 7-day echo".

## The Autocorrelation Bars: Every Lag at Once

**Tags:** `why it matters` (orange), `where it's used` (green)

- **One bar per lag** — compute the correlation at lag 1, 2, ... 14 and draw them side by side
- **The tall bar** — lag 7 stands out at 0.73: the weekly echo, found automatically
- **Echo of the echo** — lag 14 is tall again (0.50): two weeks apart, still in step
- **Negative bars** — lags 2-5 dip below zero: mid-week highs pair with weekend lows
- **In practice** — this plot (the ACF, or correlogram) is step one of any forecasting job

*Example:* Before picking features she plotted the bars, saw the spike at 7, and gave the model "traffic 7 days ago".

**Key point:** The autocorrelation plot turns "I think there's a weekly pattern" into a bar you can point at — and tells you which lags to use.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of autocorrelation at lags 1–14 (ACF/correlogram).

- **Title (bold 15px, `#1a5276`, top center):** "Autocorrelation of the 4-week traffic, lag 1 to 14"
- **Caveat (12px `#6b7280`, left-aligned under the title, y=40):** "library ACF: whole-series mean and variance, so long lags count for less"
- **Data (`acf14`, comment: "autocorrelation of weekly28 at lags 1..14 (precomputed from the array above)"):** `[0.30,-0.37,-0.29,-0.34,-0.37,0.21,0.73,0.25,-0.26,-0.22,-0.23,-0.27,0.12,0.50]`
- **Axes:** y from −0.6 to 1.0, tick labels −0.5, 0, 0.5, 1; zero line in gray `#999` across the plot; x labeled 1–14 under each bar plus axis caption "lag (days shifted)"; padding top 52, bottom 52, left 58, right 24.
- **Bars:** width 30px with even gaps; fill `rgba(42,120,214,0.45)` except lags 7 and 14 in solid green `#008300`; bars grow up from zero when positive, down when negative.
- **Value labels:** each bar's value to 2 decimals (11px, `#2c3e50`) above positive bars / below negative bars.
- **Annotation:** bold 13px green text "tallest bar at lag 7 = the weekly rhythm" placed up-right of the lag-7 bar, with a short green pointer line (width 1.5) down toward the bar top.

## The Confusion: Correlated Days Are Not Extra Evidence

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Two series** — both average about 1,000 with similar spread; one is noise, one has memory
- **Noise** — lag-1 autocorrelation 0.05: yesterday tells you nothing about today
- **Memory** — lag-1 autocorrelation 0.93: each day mostly repeats the day before
- **Fewer facts** — 30 days of the memory series hold far fewer real facts than 30 dice rolls
- **Stats break** — averages look falsely precise and tests get overconfident on correlated days

*Example:* The "significant" traffic uptick vanished once the analyst accounted for day-to-day correlation.

**Key point:** High autocorrelation means your 30 points behave like far fewer — treat the sample size as smaller than it looks.

### Visualization (canvas `c4`, 720×300)

Two side-by-side line panels comparing a memoryless series to a high-memory series.

- **Title (bold 15px, `#1a5276`, top center):** "Same average (~1,000), similar spread — different memory"
- **Divider:** dashed (4/3) vertical line `#bdc3c7` at x=360 splitting the canvas into two panels.
- **Data (comment: "two series, both mean ~1000: no memory (lag-1 r = 0.05) vs memory (lag-1 r = 0.93)"):**
  - `noise30`: `[963,951,1093,1097,1047,954,875,1158,1061,1087,935,938,1014,956,974,914,1108,924,930,1023,845,944,915,895,959,1145,1016,1050,922,1006]`
  - `smooth30`: `[1000,1051,1093,1121,1130,1118,1088,1044,992,942,902,876,870,885,918,964,1015,1064,1103,1126,1129,1111,1076,1029,977,929,892,872,873,893]`
- **Panels:** each 300px wide (left at x0=35, right at x0=390), plot top 66, height 150, y range 800–1200; dashed (4/4) light gray `#e5e9ef` reference line at 1000; L-shaped `#999` axes.
- **Left panel:** magenta `#d55181` line, width 2.5; bold 13px title "no memory: lag-1 r = 0.05"; bold 12px magenta note below axis "yesterday says nothing — 30 real facts"; gray "30 days" caption.
- **Right panel:** green `#008300` line, width 2.5; bold 13px title "memory: lag-1 r = 0.93"; bold 12px green note "each day repeats the last — far fewer facts"; gray "30 days" caption.

## Regeneration instructions

- **Template/layout:** tutorials topic page (social-graph reference style). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` line, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bold-term one-line bullets, an italic `.example` line, and a `.key-point` callout; `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `li b` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Project palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** declare intrinsic width/height attributes as given per chart; scale by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); all data arrays hardcoded (no `Math.random()`).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions (this page has none).
