# Skew & Heavy Tails

**Page type:** detail page (tutorial page: 4 card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** Skew & Heavy Tails

**Subtitle:** When a few huge values pull the average away from where the data actually lives — and why those values are normal, not errors

## A Day of Website Sessions: Short, Short, Short... One Hour Long

**Tags:** `core idea` (blue), `running example` (green)

- **The data** — 1,000 visits to a website, each timed from arrival to leaving
- **Most are quick** — 630 of the 1,000 sessions last under 2 minutes
- **A few are huge** — 15 sessions run over an hour; nothing caps how long someone stays
- **The shape** — a tall pile on the left, then a long thin stretch to the right
- **The name** — that lopsided shape is called right skew; the long stretch is the tail

*Example:* A researcher leaves a dashboard open through a 70-minute meeting — one visit, longer than 35 typical 2-minute visits combined.

**Skew, defined by the example:** durations can't go below 0 but can grow without limit, so the pile sits left and the tail stretches right.

### Visualization (canvas `c1`, 720×300)

Right-skewed histogram of 1,000 session durations.

- **Title (bold 15px, ink #1a5276, centered):** "1,000 Session Durations — the Pile and the Tail (illustrative)"
- **Data:** duration bins (minutes) `0-1, 1-2, 2-4, 4-8, 8-15, 15-30, 30-60, 60+` with counts `[380, 250, 150, 90, 60, 35, 20, 15]`
- **Axes:** padding top 52 / bottom 58 / left 58 / right 25; y max 400; axis lines `#999`; bar width 72% of column
- **Bars:** the last two bins (30-60, 60+) orange `#d95926`; all others blue `#2a78d6`; count labels (12px, `#333`) above each bar; bin labels below
- **Annotations:** green (`#008300`) bold 13px near top-left (~1.3 bins in): "630 of 1,000 under 2 minutes"; orange bold 13px at ~5.4 bins in: "the tail: 35 sessions past 30 min — real users"
- **Axis captions (12px `#444`):** x: "session duration (minutes)"; y (rotated vertical at left): "number of sessions"

## Ten Sessions You Can Average by Hand

**Tags:** `worked example` (green), `mean vs median` (blue)

- **The ten sessions** — 1, 1, 1, 2, 2, 2, 3, 3, 5, and 40 minutes
- **The mean** — add them all: 60 minutes total, divided by 10 sessions = 6 minutes
- **The median** — sort them, take the middle: it sits at 2 minutes
- **The drag** — one 40-minute session pulls the mean to 3x the median
- **The tell** — mean far above median is the fingerprint of right skew

*Example:* Nine visitors stayed 5 minutes or less, yet the "average visit" is 6 minutes — longer than 9 of the 10 visits.

**Do it yourself:** drop the 40 and recompute — the mean falls from 6.0 to about 2.2, but the median barely moves. The median resists the tail.

### Visualization (canvas `c2`, 720×300)

Dot plot on a number line: ten session durations with median and mean markers and a drag arrow.

- **Title (bold 15px, ink #1a5276, centered):** "Ten Sessions: 1, 1, 1, 2, 2, 2, 3, 3, 5, 40 Minutes"
- **Layout:** padding top 56 / bottom 62 / left 58 / right 30; x axis 0–42 minutes with tick labels at 0, 5, 10, 15, 20, 25, 30, 35, 40; axis line `#999`
- **Dots:** values `[1, 1, 1, 2, 2, 2, 3, 3, 5, 40]` as stacked dots (radius 8, stacking upward 20px per duplicate); the 40 dot orange `#d95926`, all others blue `#2a78d6`
- **Median marker:** solid green (`#008300`) vertical line width 3 at x=2 reaching high; green bold 13px label: "median = 2"
- **Mean marker:** dashed magenta (`#d55181`) vertical line width 3 (dash 7/4) at x=6; magenta bold 13px label: "mean = 60 / 10 = 6"
- **Drag arrow:** orange 2px horizontal arrow from x=15 to x=37 at y=140 (arrowhead at the right end, pointing right); orange bold 13px centered label above: "one 40-min session drags the mean to 3x the median"
- **X-axis caption (12px `#444`):** "session duration (minutes)"

## Why a Data Scientist Cares: The Average Describes Nobody

**Tags:** `where it's used` (blue), `reporting trap` (orange)

- **Reports** — "average session: 6 min" gets quoted, but 3 in 4 sessions are shorter than that
- **Percentiles** — p50 = 2, p90 = 12, p99 = 70 minutes tell the real story (illustrative)
- **Capacity** — servers, support staff, and timeouts must handle the tail, not the mean
- **Same shape elsewhere** — incomes, house prices, hospital stays, file sizes, wait times
- **Better habit** — report the median for "typical" and a high percentile for "worst case"

*Example:* A 12-minute session timeout tuned to "twice the average" would cut off every one of the p99 sessions.

**Rule of thumb:** for skewed data, lead with the median and one tail percentile — the mean alone answers a question nobody asked.

### Visualization (canvas `c3`, 720×300)

Percentile bar chart with the mean as a dashed reference line.

- **Title (bold 15px, ink #1a5276, centered):** "Percentiles Tell the Story the Mean Hides (illustrative)"
- **Data:** labels `p50 (median), p75, p90, p99` with values `[2, 3, 12, 70]` minutes; bar colors green `#008300`, aqua `#199e70`, blue `#2a78d6`, violet `#4a3aa7`
- **Axes:** padding top 52 / bottom 58 / left 58 / right 30; y max 80; bar width 50% of column; bold value labels "2 min" … "70 min" (`#333`) above bars
- **Mean line:** horizontal dashed magenta (`#d55181`, width 2.5, dash 7/4) at 6 minutes; magenta bold 13px left-aligned label above it: "mean = 6 min — already above 3 of every 4 sessions"
- **Annotation (violet bold 13px, right-aligned near top):** "the tail is what capacity must survive"
- **X-axis caption (12px `#444`):** "session duration at each percentile (minutes)"

## The Common Confusion: "Those Long Sessions Must Be Errors"

**Tags:** `common mistake` (red), `heavy tails` (orange)

- **The instinct** — a 40-minute value among 2-minute values looks like a logging bug
- **Heavy tails** — in this kind of data, extreme values arrive regularly; they belong
- **The damage** — deleting the 40 changes the mean from 6.0 to 2.2, a 3x swing in the answer
- **Check first** — an error has a cause you can name (clock reset, duplicate row); a tail doesn't
- **Bell-curve habit** — "beyond 3 standard deviations = outlier" only works for bell shapes

*Example:* The 15 hour-plus sessions were real researchers reading long reports — trimming them erased the site's most engaged users.

**The takeaway:** in heavy-tailed data, extremes are expected members of the data. Investigate them; don't reflex-delete them.

### Visualization (canvas `c4`, 720×300)

Two-panel comparison: dot rows with/without the tail value on the left, the two resulting means as bars on the right, divided by a vertical dashed line at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px, ink #1a5276, centered):** "Trim the Tail, Change the Answer"
- **Left panel (two dot rows, x axis 0–42 mapped over 280px starting at x=40):**
  - Row 1 at y=105, label (magenta `#d55181` bold 12px): "all 10 sessions: mean 6.0 min" — the ten values `[1, 1, 1, 2, 2, 2, 3, 3, 5, 40]` as dots (radius 5; the 40 orange `#d95926`, others blue `#2a78d6`; small horizontal jitter for duplicates), with a magenta mean tick (3px vertical line) at 6
  - Row 2 at y=215, label (green `#008300` bold 12px): "drop the 40: mean 2.2 min" — same dots but the 40 drawn as a red (`#e74c3c`) crossed-out circle (outlined circle with a diagonal slash), green mean tick at 2.2
  - Caption (12px `#444`, centered at x=180): "minutes (0 to 42)"
- **Right panel (two bars at x=420, width 90, baseline y=240, height scale max 7):** "tail kept" 6.0 magenta `#d55181`; "tail deleted" 2.2 green `#008300`; bold value labels "6.0 min" / "2.2 min" above bars
- **Annotations:** red (`#e74c3c`) bold 13px centered over right panel: "a 3x swing from one deleted point"; caption (12px `#444`): "mean of the ten sessions"

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` (no index number) + `.subtitle`, then four `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) containing one row: `td.text-col` (50%) and `td.viz-col` (50%), both `vertical-align: top`, padding 12px.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Canvases:** all 720×300 intrinsic; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Data arrays are hardcoded literals (no `Math.random()`); invented tallies are labeled "(illustrative)" in chart titles. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions (this page has none).
