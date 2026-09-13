# Monitoring Data: Freshness, Volume, Drift

**Page type:** detail page (tutorial card-sections: one h2 per section, two-column table layout — text left 45%, canvas right 55%; one section uses a 3-col 38/31/31 layout with two canvases)
**HTML title tag:** Monitoring Data: Freshness, Volume, Drift

**Subtitle:** Three simple questions asked every morning — did the data arrive on time, in the usual amount, looking like it used to? — catch broken data before the people reading the dashboard do.

## The Morning the Dashboard Showed Yesterday

Tags: `core idea` (blue), `running example` (green), `freshness` (blue)

- **The pipeline** — a nightly sales job loads yesterday's orders, done by about 5:10 each morning
- **The reader** — the sales dashboard refreshes at 6:00 and the team checks it at 9:00
- **The miss** — one Tuesday the upstream export stalls and the load finishes at 9:40
- **The illusion** — at 6:00 the dashboard rendered fine — silently showing Monday's numbers
- **The check** — freshness: "is the newest row's timestamp recent enough?" — one query, run at 6:00

*Example:* The team debated a "flat Tuesday" for two days before anyone asked when the data had actually arrived.

**Key point:** Stale data looks exactly like calm data. Freshness is checked against the clock, not against the numbers.

### Visualization (canvas `c1`, 720×300)

Lollipop chart: nightly load finish times across a week versus the 6:00 dashboard refresh line.

- **Title (bold 16px, `#1a5276`, top center):** "When the Nightly Load Finished vs the 6:00 Dashboard Refresh".
- **Data:** days `['Wed','Thu','Fri','Sat','Sun','Mon','Tue']`; finish times (hours after midnight) `[5.15, 5.05, 5.3, 4.95, 5.2, 5.1, 9.67]` (Tue = 9:40).
- **Axes:** y from 4 to 10.5 hours, labels "04:00", "06:00", "08:00", "10:00" at 4/6/8/10; x = 7 day slots, each lollipop centered in its slot; padding left 70, right 30, top 55, bottom 55; gray axes `#6b7280`.
- **Refresh line:** horizontal dashed orange (`#d95926`, dash 6/4, width 2) at y=6:00, labeled bold "06:00 dashboard refresh" left-aligned above the line.
- **Lollipops:** vertical stem (width 3) from baseline up to the finish time with a 6px-radius dot; blue `#2a78d6` when finish < 6:00, red `#e74c3c` when later (Tue only). Each dot labeled with its time (e.g. "05:09", "09:40"); the late one bold. Day names in `#2c3e50` below the baseline.
- **Caption (bold 13px red, bottom center):** "Tue finished 3h40m after the refresh — the 06:00 dashboard silently showed Monday".

## Counting Rows Against the Usual Band

Tags: `worked example` (green), `row counts` (blue)

- **The habit** — the orders load lands about 48,000 rows a day, give or take
- **The band** — last 14 days average 48,000 rows, spread (sigma) about 2,000 rows
- **The rule** — alert outside mean ± 3 sigma: below 42,000 or above 54,000 rows
- **The day** — Thursday delivers 12,400 rows — 35,600 under the mean, nearly 18 sigmas out
- **The cause** — one of four upstream store files never arrived; the load "succeeded" without it

*Example:* You can redo the check by hand: 48,000 − 3 × 2,000 = 42,000, and 12,400 is far below it.

**Key point:** A volume check needs no knowledge of what the rows mean — just that today should roughly resemble the recent past.

### Visualization (canvas `c2`, 720×300)

Bar chart: 15 days of daily row counts with a shaded ±3-sigma alert band, the last bar far below it.

- **Title (bold 16px, `#1a5276`, top center):** "Daily Rows Loaded, With the 42k–54k Alert Band (mean 48k ± 3σ)".
- **Data (thousands of rows):** `[46.5, 50.6, 45.6, 50.2, 48.4, 45.0, 49.2, 51.1, 47.2, 45.4, 50.4, 48.9, 46.1, 47.4, 12.4]` — the last value is "today".
- **Axes:** y 0–60 with labels "0k", "20k", "40k", "60k"; padding left 70, right 25, top 50, bottom 50; gray axes `#6b7280`. X labels "day -14" under the first bar, "day -7" under the eighth, "today" under the last.
- **Band:** rectangle from 42 to 54 filled `rgba(0,131,0,0.10)` with dashed green (`#008300`, dash 5/4, width 1) top and bottom edges; bold green label "normal band: 42k–54k" left-aligned above the band's top edge.
- **Bars:** width = slot minus 8px; normal bars filled `rgba(42,120,214,0.45)`; any bar below 42 (today only) filled red `#e74c3c` with bold "12.4k" label above it.
- **Caption (bold 13px red, bottom center):** "today: 12,400 rows — one of four store files missing, job still \"green\"".

## On Time, Right Size — and Still Wrong: Drift

Tags: `drift alerts` (blue), `what goes wrong` (red)

(This section uses the 3-column layout: text 38%, two viz columns 31% each.)

- **The change** — upstream renames the payment value "credit_card" to "CC" in its export
- **The stealth** — data arrives at 5:08 with 48,300 rows; freshness and volume both pass
- **The signature** — credit_card share drops 46% → 2% in a day; a new "CC" bucket appears at 45%
- **The victims** — the fraud model treats "CC" as unknown; the payments chart shows a collapse
- **The check** — drift: compare today's value shares or averages to last week's profile

*Example:* No job failed and no row was lost — a string rename quietly gutted every credit-card metric.

**Key point:** Freshness and volume watch the container; drift watches the contents. Renames, unit changes, and null floods only trip the third check.

### Visualization (canvas `c3a`, 420×300)

Line chart: the share of "credit_card" payments over 10 days, cliff-dropping at day 8.

- **Title (bold 15px, `#1a5276`, top center):** "Share of \"credit_card\" payments".
- **Data (% by day 1–10):** `[45, 46, 44, 47, 45, 46, 46, 2, 2, 2]`.
- **Axes:** y 0–60% with labels 0%, 20%, 40%, 60%; padding left 55, right 20, top 45, bottom 45; gray axes `#6b7280`. X labels "day 1", "day 8", "day 10" under days 1, 8, 10.
- **Series:** connected blue line `#2a78d6`, width 3, with 4px-radius dots at every point; dots red `#e74c3c` when the share < 10% (days 8–10), otherwise blue.
- **Annotations:** bold red 13px "day 8: 46% → 2% overnight" near mid-chart; italic gray 11px caption at bottom center: "freshness and volume both passed all 10 days".

### Visualization (canvas `c3b`, 400×300)

Grouped bar chart: payment value shares on day 7 vs day 8, showing the renamed bucket.

- **Title (bold 15px, `#1a5276`, top center):** "Payment values: day 7 vs day 8".
- **Categories:** `['credit_card', 'CC', 'debit', 'wallet']`; day-7 shares `[46, 0, 34, 20]`%, day-8 shares `[2, 45, 33, 20]`%.
- **Axes:** y 0–60% with labels 0%, 20%, 40%, 60%; padding left 55, right 15, top 50, bottom 60; gray axes `#6b7280`. Category labels rotated ~-0.35 rad below the baseline.
- **Bars:** paired per category, 26px wide; day-7 bars filled `rgba(42,120,214,0.45)`; day-8 bars filled `rgba(25,158,112,0.6)` except the "CC" bar, which is solid red `#e74c3c`.
- **Legend (top left):** small squares "day 7" (blue fill) and "day 8" (green fill), 11px text.
- **Annotations:** bold red 12px "same payments, new name: \"CC\" at 45%" near the bottom; italic gray 11px caption below it: "a share profile compared day-over-day catches it".

## "The Job Succeeded" Is Not "The Data Is Fine"

Tags: `common confusion` (red), `rule of thumb` (orange)

- **The confusion** — teams watch the scheduler: green ticks mean "nothing to see here"
- **The gap** — a job can succeed at loading late, half-empty, or renamed data
- **The week** — seven green job runs; the data checks caught a late Tuesday and a thin Thursday
- **The order** — check freshness first, then volume, then drift — cheapest and loudest first
- **The payoff** — the alert reaches the data team hours before the 9:00 dashboard crowd

*Example:* The scheduler's job page showed a perfect week; two of those seven "successes" had shipped bad data.

**Key point:** Job monitoring asks "did the machinery run?" Data monitoring asks "is what it produced believable?" You need both, and they disagree often.

### Visualization (canvas `c4`, 720×300)

Status-board diagram: the same seven runs shown as two rows of check/cross cells.

- **Title (bold 16px, `#1a5276`, top center):** "Same Seven Runs, Two Scoreboards".
- **Columns:** days `['Wed','Thu','Fri','Sat','Sun','Mon','Tue']`, cells 68px wide × 40px high starting at x=175, day labels above.
- **Row 1 "scheduler view" (y=70):** all 7 cells OK — fill `rgba(0,131,0,0.15)`, green `#008300` border and bold "✓".
- **Row 2 "data checks" (y=150):** OK pattern `[true, false, true, true, true, true, false]` — failing cells (Thu, Tue) fill `rgba(231,76,60,0.15)`, red `#e74c3c` border and bold "✗"; row labels bold `#1a5276` right-aligned to the left of each row.
- **Callouts under failing cells (bold red 12px):** "volume: 12.4k rows" under Thu, "freshness: 09:40" under Tue.
- **Captions:** bold orange (`#d95926`) 13px at y=250: "seven green job runs — two of them shipped bad data anyway"; gray 12px at y=275: "run the checks in order: freshness → volume → drift (cheapest and loudest first)".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then 4 `.card-section` blocks, each an `<h2>` (1.3rem, `#1a5276`, bottom border `2px solid #2980b9`) followed by `table.layout` with one `<tr>`: left `td.text-col` (45%) holding `.tags` pills + `<ul>` bullets + italic `.example` + `.key-point` callout; right `td.viz-col` (55%) holding one canvas. Section 3 uses `table.layout3` (text 38%, two viz columns 31% each) with canvases `c3a` (420×300) and `c3b` (400×300).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `<li><b>` bold terms in `#1a5276`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`, red `#e74c3c`. Overall doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes per chart; a shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All data arrays hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No cross-page links; in regenerated HTML any card links would use `.html` extensions.
