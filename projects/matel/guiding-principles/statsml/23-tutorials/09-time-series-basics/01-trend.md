# Trend

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Trend

**Subtitle:** The slow drift hiding under the daily wiggles — where a series is heading, not where it is today

**Shared running-example data (used by all four canvases):** 24 months of daily coffee-shop sales, day 0 = Monday, opening day. Deterministic generator (no randomness): sales(d) = trend(d) + season(d) + holiday(d) + noise(d), where trend(d) = 200 + 0.15·d (+$0.15/day = +$55/year); season(d) = weekly pattern Mon..Sun `[-18, -20, -16, -12, -2, 30, 38]` (sums to 0) indexed by d mod 7; holiday(d) = +55 when day-of-year is 328–358, else 0 (holiday rush); noise(d) = 10·sin(d·7.9) + 6·sin(d·3.3) (jitter within ±16). Monthly averages of this series, rounded: `[200, 207, 211, 217, 219, 226, 229, 233, 240, 242, 258, 298, 255, 261, 268, 270, 274, 282, 282, 289, 294, 296, 315, 351]`.

## Two Years of Coffee Sales in One Line

**Tags:** `core idea` (blue), `running example` (green)

- **The raw line** — daily sales jump around: quiet Tuesdays, busy Sundays, a December rush
- **Underneath it** — the whole cloud drifts upward, about $55 more per day each year
- **That drift is the trend** — the direction left over when you ignore all the wiggles
- **Eyeballing works** — lay a ruler on the chart and the climb is visible before any math
- **Fitting is better** — a fitted line gives a number, +$0.15 per day, you can check and compare

*Example:* The shop sold about $200 a day at opening and about $309 a day 24 months later — no single day sold either amount.

**Key point:** Trend is the slow movement of the average over months, not the value of any one day. Every day sits above or below it.

### Visualization (canvas `c1`, 720×300)

Line chart: 730 daily sales points (days 0–729 from the shared generator) with the straight trend line overlaid.

- **Title (bold 15px `#1a5276`, top center):** "24 months of daily sales — one slow climb under the wiggles"
- **Daily series:** thin 1px line in `rgba(42,120,214,0.55)` through sales(d) for d = 0…729.
- **Trend line:** orange `#d95926`, 3px, straight from (day 0, $200) to (day 729, $309.4).
- **Axes:** y from $150 to $430 with gridlines/labels at $150, $250, $350 (12px gray `#6b7280`, grid `#e5e9ef`); x ticks at days 0/181/365/546/729 labeled "Jan y1", "Jul y1", "Jan y2", "Jul y2", "Dec y2"; gray `#999` axis lines; padding l 58, r 20, t 46, b 44.
- **Annotations:** bold 13px orange, left: "orange line = trend: +$55 per year"; bold 12px yellow `#c98500`, right: "December spikes: season, not trend".
- **Caption (12px `#6b7280`, bottom right):** "illustrative data".

## Measure the Drift With Two Januaries

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Pick matching months** — January year 1 vs January year 2, so the season cancels out
- **Average each month** — Jan Y1 averages $200 a day; Jan Y2 averages $255 a day
- **Subtract** — 255 − 200 = $55 of growth in exactly one year
- **Per day** — $55 ÷ 365 ≈ $0.15: sales climb fifteen cents a day, on average
- **Why matching months** — Dec Y1 vs Jan Y2 would mix the holiday surge into "growth"

*Example:* Jan Y1: $200 average. Jan Y2: $255 average. Trend = +$55 a year — measured with nothing but a subtraction.

**Key point:** Comparing the same month a year apart cancels seasonality and leaves the trend behind — the simplest honest trend estimate there is.

### Visualization (canvas `c2`, 720×300)

Connected dot plot of the 24 monthly averages with the two Januaries highlighted and bracketed.

- **Title (bold 15px `#1a5276`, top center):** "Monthly averages — compare January to January"
- **Data:** the 24 monthly averages listed above, connected by a 1.5px line in `rgba(42,120,214,0.5)`.
- **Dots:** months 0 and 12 (the two Januaries) green `#008300`, 6px; months 11 and 23 (Decembers) yellow `#c98500`, 3.5px; all others blue `#2a78d6`, 3.5px.
- **Axes:** y from $180 to $370 with gridlines/labels at $200, $275, $350; x labeled with month initials J F M A M J J A S O N D repeated twice (11px gray), plus "year 1" and "year 2" group labels below; gray `#999` axis lines; padding l 58, r 20, t 48, b 46.
- **January value labels (bold 13px green):** "$200" above month 0, "$255" above month 12.
- **Bracket:** green 2px bracket connecting the two Januaries over the top, labeled bold 13px green: "+$55 in one year = the trend".
- **Annotation (bold 12px yellow):** "Dec: holiday — skip for this trick"
- **Caption (12px `#6b7280`, bottom right):** "illustrative data".

## The Trend Line Is the Planning Line

**Tags:** `where it's used` (orange), `rule of thumb` (blue)

- **Ordering beans** — next January needs stock for ~$312/day, not for today's $255
- **Hiring** — the trend says when one barista stops being enough, months in advance
- **Health check** — months drifting below the line flag a stall long before totals fall
- **Judging ideas** — a promo must beat the trend, not just beat last month
- **Without it** — every decision reacts to the last loud day instead of the direction

*Example:* Extending the fitted line 12 months out prices next January at about $312 a day — a forecast made with nothing fancier than a ruler.

**Key point:** Most planning questions are questions about the trend. The raw daily number is the wrong input for all of them.

### Visualization (canvas `c3`, 720×300)

Monthly averages with a fitted trend line, solid through the 24 observed months and dashed 13 months into the future, with a forecast marker.

- **Title (bold 15px `#1a5276`, top center):** "Extend the trend line: next January is already priced"
- **Data:** the 24 monthly averages as blue `#2a78d6` 3px dots connected by a 1.5px `rgba(42,120,214,0.5)` line; x-axis spans 37 month slots (24 real + 13 future).
- **Trend line:** T(i) = 202 + 4.56·i (monthly-average trend); orange `#d95926` 3px solid from month 0 to month 23, then dashed (7/5) from month 23 to month 35.
- **Forecast marker:** white-filled circle with 2.5px orange stroke (6px radius) at month 24, value $312, labeled bold 13px orange: "plan next January for ~$312/day".
- **Axes:** y from $180 to $380 with gridlines/labels at $200, $275, $350; x ticks at months 0/12/24/35 labeled "Jan y1", "Jan y2", "Jan y3", "Dec y3"; gray `#999` axis lines; padding l 58, r 20, t 48, b 46.
- **Annotation (bold 12px aqua `#199e70`, left):** "months sagging below the line = early stall warning"
- **Caption (12px `#6b7280`, bottom right):** "illustrative data".

## Trend Is Not This Month's Number

**Tags:** `common mistake` (red), `running example` (green)

- **December Y1** — the month averages $298 a day; the owner declares "$300 is our new level"
- **January Y2** — back to $255; panic: "we lost $43 a day!"
- **The trend line disagrees** — the underlying level in December was about $252
- **The extra $46** — holiday season sitting on top of the trend, gone with the tinsel
- **January at $255** — almost exactly on the trend line ($257): nothing broke

*Example:* Calling December's $298 "the new level" turns a perfectly on-trend January into a fake crisis.

**Key point:** Judge a month against the trend line, never against last month's peak. The trend is the level; peaks are visitors.

### Visualization (canvas `c4`, 720×300)

Six-month zoom (Nov y1 – Apr y2): monthly points against the trend line, with December's seasonal gap marked.

- **Title (bold 15px `#1a5276`, top center):** "December is not the new level — the trend line is"
- **Data:** x labels "Nov y1", "Dec y1", "Jan y2", "Feb y2", "Mar y2", "Apr y2" with monthly values `[258, 298, 255, 261, 268, 270]` (MAVG months 10–15) and trend values `[247.6, 252.2, 256.7, 261.3, 265.9, 270.4]`.
- **Points:** 6px dots — December yellow `#c98500`, all others blue `#2a78d6`; each labeled with its dollar value in bold 12px `#2c3e50` above.
- **Trend line:** orange `#d95926` 3px straight line through the trend values, labeled "trend line" in bold 12px orange at the left end.
- **Season gap:** dashed (4/3) yellow 2px vertical line on December from the trend ($252.2) up to $298.
- **Axes:** y from $220 to $330 with gridlines/labels at $230, $270, $310; gray `#999` axis lines; padding l 58, r 20, t 48, b 46.
- **Annotations (bold 13px, centered):** yellow: "$46 of season on a $252 trend"; green `#008300`: "Jan $255 vs trend $257: on trend — not a crash".
- **Caption (12px `#6b7280`, bottom right):** "illustrative data".

## Regeneration instructions

- **Template:** tutorial detail page (see `tutorials/CLAUDE.md`). h1 + `.subtitle`, then 4 `.card-section` blocks, each an `<h2>` followed by `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` line, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. Bullets 0.92rem, `li b` colored `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvases:** 720×300 intrinsic, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). The daily-sales generator functions (trendOf, seasonOf, holidayOf, noiseOf, salesOf) and the MAVG monthly-averages array are shared across all four chart IIFEs. Chart palette object `P`: blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
