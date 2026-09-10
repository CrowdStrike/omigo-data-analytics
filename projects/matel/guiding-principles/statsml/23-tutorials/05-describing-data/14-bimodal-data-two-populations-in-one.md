# Bimodal Data: Two Populations in One

**Page type:** detail page (tutorial topic page: `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%; section 3 uses a three-column 38/31/31 layout with two canvases)
**HTML title tag:** Bimodal Data: Two Populations in One

**Subtitle:** When one dataset secretly contains two different groups, the histogram grows two humps — and the average lands in the empty valley between them

## One Restaurant, Two Kinds of Visits

Tags: `core idea` (blue), `running example` (green)

- **The data** — how long each of 225 customers stayed in a restaurant (illustrative)
- **Hump one** — the takeaway crowd: grab a coffee and sandwich, out in about 12 minutes
- **Hump two** — the sit-down diners: order, eat, chat, pay — around 55 minutes
- **The valley** — almost nobody stays 30-40 minutes; only 2 of 225 visits land there
- **The name** — two humps = bimodal; it almost always means two groups were mixed

*Example:* The espresso-to-go customer and the anniversary dinner are both "a visit" — but they are different populations.

**Key point:** **Bimodal, seen before defined:** one histogram, two peaks. The data isn't weird — it is two ordinary groups sharing one table of numbers.

### Visualization (canvas `c1`, 720×300)

Histogram: 225 visit durations in 5-minute bins showing two humps and an empty valley, with the mean marked in the valley.

- **Title (bold 15px, `#1a5276`, top center):** "225 Restaurant Visits: Two Humps, One Empty Valley (illustrative)"
- **Data:** 15 bins of 5 minutes each from 0 to 75; counts `[2, 30, 48, 20, 6, 2, 1, 1, 9, 22, 28, 34, 13, 6, 3]`.
- **Bar colors:** bins 0–5 blue `#2a78d6` (takeaway hump), bins 6–7 orange `#d95926` (the valley bins), bins 8–14 violet `#4a3aa7` (diner hump); 2px inset per bar.
- **Axes:** y scale 0 to 55; L-shaped gray `#999` axis; padding top 52, bottom 58, left 58, right 25; x tick labels every 15 minutes (0, 15, 30, 45, 60, 75) in 12px `#333`.
- **Mean marker:** dashed magenta `#d55181` vertical line (width 3, dash 7/4) at 34 minutes, full plot height; bold 13px magenta label "mean = 34 min — where almost nobody is" to its right.
- **Annotations:** bold 13px blue "takeaway ~12 min" above the left hump; bold 13px violet "diners ~55 min" above the right hump; bold 12px orange "2 visits in 30-40" in the valley near the baseline.
- **Axis labels (12px `#444`):** x "visit duration (minutes)" bottom center; y "number of visits" rotated vertical.

## Ten Visits: The Average Lands Where Nobody Is

Tags: `worked example` (green), `mean trap` (red)

- **The ten visits** — takeaway: 10, 11, 12, 13, 14 min; diners: 50, 53, 55, 57, 60 min
- **The mean** — total 335 minutes over 10 visits = 33.5 minutes per visit
- **The check** — not one of the ten visits lasted between 15 and 49 minutes
- **Median too** — middle of the sorted list is (14 + 50) / 2 = 32, also in the empty gap
- **The lesson** — with two humps, every one-number summary points at nobody

*Example:* Tell the manager "a typical visit is 33 minutes" and you have described a customer who has never walked in.

**Key point:** **Do it yourself:** 60 takeaway minutes + 275 diner minutes = 335; 335 / 10 = 33.5 — arithmetic is fine, the summary is meaningless.

### Visualization (canvas `c2`, 720×300)

Dot plot on a number line: the ten visits as two clusters, with mean and median markers falling in the empty gap.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Visits: 10, 11, 12, 13, 14 and 50, 53, 55, 57, 60 Minutes"
- **Data:** takeaway dots `[10, 11, 12, 13, 14]` in blue `#2a78d6`; diner dots `[50, 53, 55, 57, 60]` in violet `#4a3aa7`; 8px-radius dots 20px above the baseline.
- **Axes:** horizontal number line 0–65 (scale max 65), gray `#999` line with ticks and 12px `#333` labels every 10 from 0 to 60; padding top 56, bottom 62, left 58, right 30.
- **Cluster labels (bold 12px):** blue "takeaway" above the left cluster (at x=12); violet "diners" above the right cluster (at x=55).
- **Empty gap band:** translucent orange `rgba(217,89,38,0.10)` rectangle covering x 15–49 from y=66 down to the baseline; bold 12px orange `#d95926` label "the empty gap: no visit between 15 and 49" at the top of the band (centered at x=32).
- **Median marker:** solid aqua `#199e70` vertical line (width 3) at 32; bold 13px aqua right-aligned label "median = 32".
- **Mean marker:** dashed magenta `#d55181` vertical line (width 3, dash 7/4) at 33.5; bold 13px magenta left-aligned label "mean = 335 / 10 = 33.5".
- **Axis label (12px `#444`):** "visit duration (minutes)" bottom center.

## The Fix: Split the Mixture, Then Summarize

Tags: `best practice` (green), `worked example` (blue)

This section uses the 3-column layout: text cell `td.text-col3` (38%) plus two viz cells `td.viz-col3` (31% each).

- **Split first** — label each visit takeaway or dine-in, then summarize each group
- **Takeaway** — 10+11+12+13+14 = 60; 60 / 5 = 12 minutes, and every visit is within 2
- **Diners** — 50+53+55+57+60 = 275; 275 / 5 = 55 minutes, all within 5
- **Two honest numbers** — "12 min takeaway, 55 min dine-in" describes everyone
- **Find the label** — order type, time of day, party size — something separates the humps

*Example:* The same 10 numbers that produced the useless 33.5 give two tight, usable averages once split.

**Key point:** **Rule of thumb:** if the histogram shows two humps, report two summaries — one per hump — never one blended number.

### Visualization (canvas `c3a`, 420×300)

Dot plot: the takeaway group alone with its mean.

- **Title (bold 15px, `#1a5276`, top center):** "Takeaway Only"
- **Data:** values `[10, 11, 12, 13, 14]` as 10px-radius blue `#2a78d6` dots 26px above the baseline, each with its value in bold 11px white inside the dot.
- **Axes:** number line 0–20, ticks/labels every 5 (12px `#333`); gray `#999` baseline; padding top 54, bottom 60, left 45, right 20.
- **Mean marker:** solid green `#008300` vertical line (width 3) at 12; bold 14px green "mean 60 / 5 = 12 min" above it; bold 12px green "every visit within 2 min of it" below the label.
- **Axis label (12px `#444`):** "minutes" bottom center.

### Visualization (canvas `c3b`, 400×300)

Dot plot: the diners group alone with its mean.

- **Title (bold 15px, `#1a5276`, top center):** "Diners Only"
- **Data:** values `[50, 53, 55, 57, 60]` as 10px-radius violet `#4a3aa7` dots 26px above the baseline, each with its value in bold 11px white inside the dot.
- **Axes:** number line 45–65, ticks/labels every 5 (12px `#333`); gray `#999` baseline; padding top 54, bottom 60, left 45, right 20.
- **Mean marker:** solid green `#008300` vertical line (width 3) at 55; bold 14px green "mean 275 / 5 = 55 min" above it; bold 12px green "every visit within 5 min of it" below the label.
- **Axis label (12px `#444`):** "minutes" bottom center.

## Why It Matters: Decisions Built on the Valley Fail Both Groups

Tags: `where it's used` (orange), `common mistake` (red)

- **Table planning** — book tables for a 34-minute turn: takeaway seats idle, diners get rushed
- **Staffing** — the counter needs speed at 12 minutes; the floor needs service for 55
- **Same trap elsewhere** — server response times (cache hit vs miss), salaries (two roles)
- **Check the shape** — plot the histogram before averaging; a mean never warns you itself
- **Model note** — mixtures also break "one bell curve" assumptions in tests and models

*Example:* A "34-minute average visit" seating plan double-books the diners' tables and leaves takeaway stools empty all day.

**Key point:** **The takeaway:** a bimodal average is not a compromise between the groups — it is a number that serves neither. Split, then decide.

### Visualization (canvas `c4`, 720×300)

Horizontal bar timeline: the 34-minute plan vs what each group actually does.

- **Title (bold 15px, `#1a5276`, top center):** "Booking Every Table for the 34-Minute \"Average Visit\""
- **Rows (28px-tall horizontal bars from the left edge of the plot, scale 0–65 minutes, padding left 150, right 30; right-aligned bold 12px `#2c3e50` row labels; bold 12px colored value notes after each bar):**
  - y=88, magenta `#d55181`: "plan: table free after" → 34 min, note "34 min — the blended average"
  - y=150, blue `#2a78d6`: "takeaway actually stays" → 12 min, note "12 min — seat idle for 22 min"
  - y=212, violet `#4a3aa7`: "diner actually stays" → 55 min, note "55 min — overruns the plan by 21 min"
- **Plan line:** dashed magenta vertical line (width 2, dash 7/4) at 34 minutes spanning all three rows (y 64–240).
- **Shaded gaps:** translucent blue `rgba(42,120,214,0.12)` band over the takeaway row from 12 to 34 (the idle gap); translucent red `rgba(231,76,60,0.15)` band over the diner row from 34 to 55 (the overrun).
- **Bottom annotation (bold 13px `#e74c3c`, centered, y=266):** "one plan, wrong for both groups — split into 12-min and 55-min plans instead"
- **Axis label (12px `#444`):** "minutes after seating" bottom center.

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (100% width, collapsed). Sections 1, 2, 4 use one row of `td.text-col` (50%) + `td.viz-col` (50%); section 3 uses `td.text-col3` (38%) + two `td.viz-col3` (31% each) holding canvases `c3a` (420×300) and `c3b` (400×300). All cells 12px padding, top-aligned.
- **Text cell structure:** `.tags` pill row, `<ul>` of 5 one-line bullets each opening with `<b>bold term</b>` (bold terms `#1a5276`), one italic `.example` paragraph, one `.key-point` callout.
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic, `#555`, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes as given per chart (`setup(id, cw, chh)` accepts optional width/height, defaulting 720×300); backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`) with `ctx.scale` back to logical coordinates. All data hardcoded/deterministic (no `Math.random()`).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Links:** this page has no card links; any grid page linking here uses the `.html` extension in regenerated HTML.
