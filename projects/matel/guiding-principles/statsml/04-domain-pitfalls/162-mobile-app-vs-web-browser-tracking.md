# Mobile App vs Web Browser Tracking

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 162. Mobile App vs Web Browser Tracking

**Subtitle:** An app and a browser instrument the same human with different identifier lifetimes, different consent gates, and different delivery guarantees. Any metric unioned across them is a mixture of two measurement regimes, not a measurement of users.

## Callout (philosophy box)

**The fundamental problem:** Hold behaviour exactly constant and the two channels still report different numbers, because the identifier that defines the denominator, the gate that decides who is observed at all, and the rule that closes a session are all channel properties. A blended "user analytics" table therefore mixes two distributions with different biases, and the blend moves when instrumentation coverage moves even though no human did anything new.

**Illustrative Example.** Every cohort size, rate, coverage fraction, and arrival share below is constructed so the arithmetic closes exactly. None of them is a measurement of any real product, platform, or population — in particular the permission-grant and banner-consent fractions are chosen for clarity, not sourced.

## The Shared Cohort — Held Constant Across Every Section

One cohort of **100,000 people** is used by every section on this page. Their behaviour never changes;
only the instrument reading them changes.

| Tier | People | True conversion rate | True converters |
|---|---|---|---|
| Light | 40,000 | 10% | 4,000 |
| Heavy | 60,000 | 30% | 18,000 |
| **Total** | **100,000** | **22.0%** | **22,000** |

Each person is reachable through both channels, but each channel only *observes* a fraction of each
tier (its consent gate):

| Tier | App observed | App coverage | Web observed | Web coverage |
|---|---|---|---|---|
| Light | 6,000 | 15% | 18,000 | 45% |
| Heavy | 24,000 | 40% | 42,000 | 70% |
| **Total** | **30,000** | **30.0%** | **60,000** | **60.0%** |

Everything else on the page is derived from those twelve numbers plus a visit schedule, an event
trace, and a device-count distribution, each declared once.

## An Install-Scoped Identifier and a Capped Cookie Count Different Denominators

**Identical 280,000 Visits Report 60.0% Returning In-App and 27.3% on the Web**

- **The cohort's behaviour:** 60,000 people visit on days 1, 4, 13 and 22; the other 40,000 visit only on day 1.
- **Total visits:** 60,000 × 4 + 40,000 = 280,000 visits, and both channels see all 280,000 of them.
- **App identifier:** one install-scoped identifier per person survives all four visits — 100,000 identifiers.
- **Web identifier:** the browser's storage cap expires an unused cookie after 7 days of inactivity.
- **Where it splits:** days 1 and 4 are 3 days apart and survive; days 13 and 22 each start a fresh cookie.
- **Web identifier count:** 60,000 × 3 + 40,000 = 220,000 identifiers for the same 100,000 people.
- **Returning rate in-app:** 60,000 identifiers show ≥2 visits out of 100,000 → 60.0%.
- **Returning rate on web:** the same 60,000 two-visit cookies out of 220,000 → 27.3%.
- **The whole gap is the denominator:** the numerator is 60,000 in both, so 60.0 / 27.3 = 2.20× exactly matches 220,000 / 100,000.
- **What it is not:** app users are not 2.2× more loyal; nobody's loyalty was measured at all.

### Visualization (canvas `c1`, 720×360)

Visit-timeline diagram with two identifier tracks under it — one unbroken app track, three cookie
segments — plus the two returning-user rates. Identifier counts and rates computed in JS from
`VISIT_DAYS = [1,4,13,22]`, `COOKIE_CAP = 7`, `REPEAT = 60000`, `ONCE = 40000`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Same 280,000 Visits, Two Denominators" — subtitle (15px `#555`, y=44): "Illustrative Example — 60,000 people visit on days 1, 4, 13, 22".
- **Day axis:** horizontal `#333` 1px line at y=96 from x=70 to x=660; day 0-24 mapped to x 70-660. Ticks with labels "d1", "d4", "d13", "d22" in `#555` 15px at each visit day, y=90 above the axis, and a 6px tick mark.
- **Visit markers:** 6px `#1a5276` dots on the axis at each of the four visit days.
- **App track (label "App: install-scoped identifier" in `#27ae60` 15px at x=70, y=134):** single rect from the day-1 x to the day-22 x, at y=142, height 30, in `rgba(39,174,96,0.35)` with a 2px `#27ae60` border; centered label "one identifier — 4 visits" in `#27ae60`.
- **Web track (label "Web: cookie expires after 7 idle days" in `#e74c3c` 15px at x=70, y=204):** three rects at y=212, height 30, in `rgba(231,76,60,0.30)` with 2px `#e74c3c` borders, spanning day1→day4, day13→day13, day22→day22 (each single-day segment drawn 26px wide, centered on its day). Segment labels below at y=258 in `#e74c3c` 14px: "id #1 — 2 visits", "id #2 — 1 visit", "id #3 — 1 visit".
- **Expiry markers:** two dashed (4/4) `#e74c3c` 1.5px vertical lines at day 11 and day 20 (7 days after days 4 and 13) from y=206 to y=250, each with a small "✂" glyph in `#e74c3c` 14px at y=204.
- **Readout (two lines, 17px, left aligned at x=70):** y=296 in `#27ae60`: "App: 60,000 / 100,000 identifiers returning = 60.0%"; y=318 in `#e74c3c`: "Web: 60,000 / 220,000 identifiers returning = 27.3%".
- **Bottom bold red (`#e74c3c`, centered, y=346):** "Numerator identical at 60,000 — the 2.20× gap is 220,000 / 100,000, nothing else".

## Two Consent Gates Observe Two Differently-Selected Subsets

**App Reads 26.0%, Web Reads 24.0%, the Cohort's Rate Is 22.0%**

- **The gates differ in kind:** an OS permission prompt is opt-in per install; a consent banner is a page-level choice.
- **App coverage:** 15% of the Light tier and 40% of the Heavy tier grant it → 6,000 + 24,000 = 30,000 observed.
- **Web coverage:** 45% of Light and 70% of Heavy accept → 18,000 + 42,000 = 60,000 observed.
- **Both gates favour the engaged:** heavier users grant more often, so both channels over-sample the Heavy tier.
- **App tier mix:** 24,000 / 30,000 = 80.0% Heavy against a true cohort share of 60.0%.
- **Web tier mix:** 42,000 / 60,000 = 70.0% Heavy — also skewed, but by a different amount.
- **App measured rate:** (6,000 × 0.10 + 24,000 × 0.30) / 30,000 = 7,800 / 30,000 = 26.0%.
- **Web measured rate:** (18,000 × 0.10 + 42,000 × 0.30) / 60,000 = 14,400 / 60,000 = 24.0%.
- **Neither is wrong about itself:** each is the correct rate for the subset it can see, and neither subset is the cohort.
- **The 2.0 pp channel "effect":** 26.0 − 24.0 is a difference in tier mix, since both tiers convert identically in both channels.

### Visualization (canvas `c2`, 720×360)

Per-tier coverage diagram: full-tier bars with the observed fraction filled for each channel, then the
three resulting rates. Every fraction, count, and rate computed in JS from the coverage constants.

- **Title (bold 17px `#1a5276`, centered, y=22):** "One Cohort, Two Gates, Three Rates" — subtitle (15px `#555`, y=44): "Illustrative Example — coverage fractions are constructed, not measured".
- **Scale:** 60,000 people = 300px of bar width; bars 30px tall; left edge x=200.
- **Row headers (15px `#555`, right aligned at x=192):** "Light — 40,000 @ 10%" at y=92, "Heavy — 60,000 @ 30%" at y=136 for the app block; the same two labels at y=214 and y=258 for the web block.
- **App block (heading "App — OS permission prompt" bold `#27ae60` 15px at x=70, y=70):** Light outline rect (200,78) 200×30 stroked `#bbb` 1px, filled portion (200,78) 30×30 in `#27ae60` (15% of 200px), label "6,000 of 40,000 = 15%" in `#333` 14px at x=510; Heavy outline rect (200,122) 300×30, filled (200,122) 120×30 in `#27ae60` (40%), label "24,000 of 60,000 = 40%" at x=510.
- **Web block (heading "Web — consent banner" bold `#2980b9` 15px at x=70, y=192):** Light outline rect (200,200) 200×30, filled 90×30 in `#2980b9` (45%), label "18,000 of 40,000 = 45%"; Heavy outline rect (200,244) 300×30, filled 210×30 in `#2980b9` (70%), label "42,000 of 60,000 = 70%".
- **Rate readout (three items on one line, y=306, bold 17px):** "App 26.0%" in `#27ae60` at x=110; "Web 24.0%" in `#2980b9` at x=300; "Cohort 22.0%" in `#e74c3c` at x=500 — each computed at render time, each with its fraction printed beneath in 14px `#555` at y=326: "7,800 / 30,000", "14,400 / 60,000", "22,000 / 100,000".
- **Bottom bold red (centered, y=350):** "Heavy share: 80.0% in-app, 70.0% on web, 60.0% in truth — the rate gap is the mix gap".

## Unioning the Two Channels Averages Two Biases Instead of Cancelling Them

**Naive Union Reads 24.7%; Deduplicating People Only Gets to 24.0%**

- **What a union table holds:** 30,000 app rows + 60,000 web rows = 90,000 rows for at most 100,000 people.
- **Naive union rate:** (7,800 + 14,400) / 90,000 = 22,200 / 90,000 = 24.7% against a true 22.0%.
- **Direction of the error:** +2.7 pp, and both channels err the same way, so no cancellation is possible.
- **The double-count:** with independent gates, 6.75% of Light and 28% of Heavy pass both → 2,700 + 16,800 = 19,500 people appear twice.
- **Distinct people observed:** 90,000 − 19,500 = 70,500, or 70.5% of the cohort.
- **Deduplicated rate:** (21,300 × 0.10 + 49,200 × 0.30) / 70,500 = 16,890 / 70,500 = 24.0%, still 2.0 pp high.
- **Why dedupe is not the fix:** it removes double counting, which was never the main error — selection was.
- **The unstable part:** raise app Heavy-tier coverage from 40% to 70% and app rows become 48,000 at a 27.5% rate.
- **The union moves on its own:** (13,200 + 14,400) / 108,000 = 25.6%, a +0.9 pp jump caused by better instrumentation.
- **The reporting trap:** that jump will be read as a behaviour change, because nothing in the table records the coverage change.

### Visualization (canvas `c3`, 720×360)

Four-bar comparison — true, naive union, deduplicated union, and the post-coverage-change union — drawn
against a dashed true-rate line, each bar annotated with its own numerator and denominator. All rates
computed in JS; the true line is drawn from the tier constants, not hardcoded.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Four Numbers for One Cohort's Conversion Rate" — subtitle (15px `#555`, y=44): "Illustrative Example — behaviour identical in every bar".
- **Scale:** 1 percentage point = 9px of bar height; baseline y=284; bars 96px wide, first at x=76, gap 60.
- **Bar 1 — Cohort truth:** 22.0% → 198px, rect (76,86) in `#27ae60`. Value "22.0%" bold `#27ae60` centered at y=78; captions "Cohort truth" and "22,000 / 100,000" in `#333` / `#555` 14px at y=304 / 322.
- **Bar 2 — Naive union:** 24.7% → 222px, rect (232,62) in `#e74c3c`. Value "24.7%" bold `#e74c3c` at y=54; captions "Naive union" and "22,200 / 90,000".
- **Bar 3 — Deduplicated:** 24.0% → 216px, rect (388,68) in `#e67e22`. Value "24.0%" bold `#e67e22` at y=60; captions "People-deduped" and "16,890 / 70,500".
- **Bar 4 — After coverage change:** 25.6% → 230px, rect (544,54) in `rgba(231,76,60,0.55)`. Value "25.6%" bold `#e74c3c` at y=46; captions "App coverage 40%→70%" and "27,600 / 108,000".
- **True-rate line:** dashed (6/4) `#27ae60` 2px horizontal line at the 22.0% level (y = 284 − 22×9 = 86) from x=60 to x=680, labeled "cohort truth 22.0%" in `#27ae60` 14px right aligned at x=678, y=80.
- **Error brackets:** thin `#e74c3c` 1.5px vertical lines from the true-rate line to each of bars 2, 3 and 4's tops at their bar centres, labeled "+2.7 pp", "+2.0 pp", "+3.6 pp" in `#e74c3c` 14px — each computed as the bar's rate minus the true rate.
- **Bottom bold red (centered, y=350):** "Every bar describes the same 22,000 converters. Only the instrument changed."

## Offline Queueing Makes the App Series Revise Upward for Days

**Same-Day App Conversions Land 18.0% Low and Flip the Channel Comparison**

- **Why app events queue:** an app can capture an event with no connectivity and buffer it on the device.
- **Why web events do not:** a page-level beacon either reaches the collector during the visit or is lost.
- **App arrival profile:** 82.0% of day-D events arrive on day D, then 9.0%, 4.5%, 2.5%, 1.3%, 0.5%, 0.15%, 0.05%.
- **The shares close:** those eight fractions sum to exactly 1.000, so the app series converges to its true total.
- **Day-D app reading:** 6,396 of an eventual 7,800 conversions → same-day understatement of 18.0%.
- **Web is final immediately:** 14,400 on day D and 14,400 forever, so the web series never revises.
- **The apples-to-oranges ratio:** same-day app/web = 6,396 / 14,400 = 44.4%; settled = 7,800 / 14,400 = 54.2%.
- **The sign flip:** same-day app rate is 6,396 / 30,000 = 21.3%, below web's 24.0% — the settled app rate is 26.0%, above it.
- **The dangerous window:** any dashboard read inside 4 days ranks the channels in the wrong order.
- **The operational rule:** compare channels only at a matched settlement age, and publish the revision curve beside the total.

### Visualization (canvas `c4`, 720×360)

Cumulative revision curve: a rising app series converging to its final total, a flat web line, and the
same-day gap marked. Cumulative values and the understatement percentage computed in JS from
`ARRIVALS = [6396,702,351,195,101,39,12,4]`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "App Data Revises for a Week; Web Data Is Final" — subtitle (15px `#555`, y=44): "Illustrative Example — conversions attributed to a single day D".
- **Axes:** x = days since D, 0 to 7, mapped to px 90-650; y = 0 to 16,000 conversions mapped to py 300-70. Axis lines `#333` 1px. x ticks and labels "D", "D+1" … "D+7" in `#555` 14px at y=318; y ticks at 0, 4,000, 8,000, 12,000, 16,000 with labels right aligned at x=84.
- **Web series:** flat solid `#2980b9` 2.5px line at 14,400 across the full width, 5px `#2980b9` dots at every day, labeled "Web — 14,400, final on day D" in `#2980b9` 15px at x=360, y=88.
- **App series:** solid `#e74c3c` 2.5px polyline through the cumulative values 6,396 / 7,098 / 7,449 / 7,644 / 7,745 / 7,784 / 7,796 / 7,800 with 5px `#e74c3c` dots, labeled "App — settles at 7,800 by D+7" in `#e74c3c` 15px at x=360, y=232.
- **Final-total line:** dashed (6/4) `#e74c3c` 1.5px horizontal line at 7,800 spanning the plot, labeled "7,800" in `#e74c3c` 14px right aligned at x=648, y=216.
- **Same-day gap:** `rgba(231,76,60,0.18)` rect from the day-0 x, spanning 26px wide, between the 6,396 and 7,800 levels; bold `#e74c3c` 15px label "18.0% missing on day D" placed to the right of it, computed as (7800 − 6396) / 7800.
- **Comparison callout (two lines, 15px, left aligned at x=110):** y=254 in `#555`: "Read on day D: app 21.3% vs web 24.0% — app looks worse"; y=274 in `#1a5276`: "Read on D+7: app 26.0% vs web 24.0% — app looks better".
- **Bottom bold red (centered, y=344):** "Same numerator, 8 different answers. A same-day comparison compares a partial series to a complete one." — the count is `ARRIVALS.length`, since all eight cumulative readings differ.

## Tab Lifetime and an Inactivity Timeout Count Different Sessions

**One 95-Minute Trace Is 2 Sessions In-App and 3 on the Web**

- **The trace:** one person emits 12 events at minutes 0, 3, 7, 12, 57, 60, 66, 70, 78, 82, 90, 95.
- **App rule:** a session ends after 30 minutes of inactivity, and only the 45-minute gap qualifies.
- **App result:** 2 sessions — a 4-event, 12-minute one and an 8-event, 38-minute one.
- **Web rule:** a session ends when the tab closes, which happened just before minutes 57 and 78.
- **Web result:** 3 sessions of 4 events each, lasting 12, 13 and 17 minutes.
- **Session count:** 3 vs 2 → the web channel reports 50.0% more sessions from identical behaviour.
- **Events per session:** 12/2 = 6.00 in-app vs 12/3 = 4.00 on web, a 33.3% shortfall.
- **In-session time:** 12 + 38 = 50 minutes in-app vs 12 + 13 + 17 = 42 minutes on web, since gaps inside a session count.
- **At cohort scale:** 100,000 identical traces = 1,200,000 events either way, but 200,000 app sessions vs 300,000 web ones.
- **Every per-session metric inherits it:** conversions per session, bounce rate, and session duration all move by the same artifact.

### Visualization (canvas `c5`, 720×340)

Event timeline with two bracket rows beneath it — app sessions by inactivity timeout, web sessions by
tab close — with counts and durations computed in JS from `EVENTS`, `IDLE = 30`, `TAB_CLOSE = [57,78]`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "One Trace, Two Session Counts" — subtitle (15px `#555`, y=44): "Illustrative Example — 12 events over 95 minutes, one person".
- **Time axis:** `#333` 1px line at y=96 from x=70 to x=660; minutes 0-100 mapped to that range. Ticks every 20 minutes labeled "0", "20", "40", "60", "80", "100" in `#555` 14px at y=114.
- **Event markers:** 5px `#1a5276` dots on the axis at each of the 12 event minutes.
- **Gap annotation:** dashed (4/4) `#e67e22` 1.5px horizontal line between the minute-12 and minute-57 x positions at y=76, with the computed label "45-min gap" in `#e67e22` 14px centered above at y=70.
- **App row (label "App — 30-min inactivity timeout" bold `#27ae60` 15px at x=70, y=152):** two rects at y=160, height 34, in `rgba(39,174,96,0.35)` with 2px `#27ae60` borders, spanning minutes 0→12 and 57→95. Centered labels in `#27ae60` 14px: "4 ev / 12 min" and "8 ev / 38 min".
- **Web row (label "Web — session ends when the tab closes" bold `#e74c3c` 15px at x=70, y=228):** three rects at y=236, height 34, in `rgba(231,76,60,0.30)` with 2px `#e74c3c` borders, spanning 0→12, 57→70, 78→95. Centered labels in `#e74c3c` 14px: "4 ev / 12 min", "4 ev / 13 min", "4 ev / 17 min".
- **Tab-close markers:** two `#e74c3c` 2px vertical lines at minutes 57 and 78 from y=232 to y=274 with a small "×" glyph in `#e74c3c` 14px at y=286.
- **Readout (one line, 15px, centered, y=306):** "Sessions 2 vs 3 (+50.0%) · events/session 6.00 vs 4.00 (−33.3%) · in-session minutes 50 vs 42 (+19.0%)" in `#1a5276`, every figure computed at render time.
- **Bottom bold red (centered, y=332):** "Nothing about the person changed. The session boundary is a property of the channel."

## Cross-Device Stitching Collapses Identifiers by Estimate, Not by Observation

**160,000 Raw Identifiers Become 116,250 Stitched Entities for 100,000 Real People**

- **Device distribution:** 55,000 people use 1 device, 30,000 use 2, and 15,000 use 3.
- **Raw identifier count:** 55,000 + 60,000 + 45,000 = 160,000 identifiers for 100,000 people.
- **Links needed:** collapsing to the truth requires removing 160,000 − 100,000 = 60,000 excess identifiers.
- **Stitching is a classifier:** at 70% recall it finds 42,000 of those 60,000 links and misses 18,000.
- **Precision costs too:** at 96% precision it asserts 42,000 / 0.96 = 43,750 links, of which 1,750 are wrong.
- **Resulting entity count:** 160,000 − 43,750 = 116,250 stitched entities, 16.25% above the true 100,000.
- **Two errors, opposite signs:** 18,000 missed links inflate the count; 1,750 false merges deflate it.
- **They do not cancel:** 100,000 + 18,000 − 1,750 = 116,250, so the miss term dominates by more than 10×.
- **One numerator, three rates:** 22,000 converters read as 13.8% over raw ids, 18.9% over stitched entities, 22.0% over people.
- **What is actually reported:** whichever denominator the join happened to produce, with no error bar on it.
- **Worse than a wrong number:** the count is a model output, so it changes when the stitching model is retrained.

### Visualization (canvas `c6`, 720×340)

Descending three-bar collapse (raw → stitched → truth) with the recall/precision decomposition drawn as
labeled segments, plus the three conversion rates. All counts computed in JS from `DEVICES`,
`RECALL = 0.70`, `PRECISION = 0.96`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Three Denominators, One Set of 22,000 Converters" — subtitle (15px `#555`, y=44): "Illustrative Example — stitching at 70% recall, 96% precision".
- **Scale:** 160,000 = 480px of bar width; bars 40px tall; left edge x=150.
- **Bar 1 — Raw identifiers (y=76):** rect (150,76) 480×40 in `rgba(26,82,118,0.35)`, centered label "160,000 raw identifiers" in `#1a5276`; row label "Raw" bold `#1a5276` 15px right aligned at x=142, y=100.
- **Bar 2 — Stitched entities (y=146):** rect (150,146) 348.75×40 in `#e67e22` (116,250 → 480 × 116250/160000), centered white label "116,250 stitched entities"; row label "Stitched" bold `#e67e22` at y=170.
- **Bar 3 — Truth (y=216):** rect (150,216) 300×40 in `#27ae60`, centered white label "100,000 real people"; row label "Truth" bold `#27ae60` at y=240.
- **Error decomposition (between bars 2 and 3):** `rgba(231,76,60,0.20)` rect from x=450 to x=498.75 spanning y=146 to y=190 (the 16,250 excess), with a bracket and the label "+18,000 missed links − 1,750 false merges = +16,250" in `#e74c3c` 14px at x=506, y=196 — the three figures computed, not typed.
- **Rate readout (three items, y=276, bold 15px):** "22,000 / 160,000 = 13.8%" in `#1a5276` at x=90; "22,000 / 116,250 = 18.9%" in `#e67e22` at x=300; "22,000 / 100,000 = 22.0%" in `#27ae60` at x=510.
- **Assumption note (14px `#555`, centered, y=300):** "Numerator held at 22,000 by construction; false merges also deflate it, so 18.9% is an upper bound."
- **Bottom bold red (centered, y=328):** "The stitched denominator is a model output — retrain the model and the metric moves".

## Reweighting to a Common Observed Fraction Is the Only Union That Recovers the Truth

**Both Channels Return Exactly 22.0% Once Each Tier Carries Weight 1 / Coverage**

- **The requirement:** a channel's rate is unbiased only if every tier is observed at the same fraction.
- **The weight:** give each observed row the reciprocal of its tier's coverage, so each tier sums back to its true size.
- **App weights:** Light 40,000 / 6,000 = 6.667; Heavy 60,000 / 24,000 = 2.500.
- **App reweighted numerator:** 600 × 6.667 + 7,200 × 2.500 = 4,000 + 18,000 = 22,000.
- **App reweighted denominator:** 6,000 × 6.667 + 24,000 × 2.500 = 40,000 + 60,000 = 100,000 → 22.0% exactly.
- **Web weights:** Light 40,000 / 18,000 = 2.222; Heavy 60,000 / 42,000 = 1.429, and the same algebra returns 22.0%.
- **Now the union is safe:** 44,000 / 200,000 = 22.0%, and it no longer moves when channel row-share moves.
- **The cost of the fix:** the weights need an external cohort frame, so the correction inherits that frame's error.
- **No session metric is invariant:** any metric whose unit is a session or an identifier is defined by the channel, not the user.
- **A channel-invariant metric:** one counted per known cohort member over a fixed wall-clock window, which needs the weights anyway.
- **The minimum discipline:** publish per-channel numbers with their observed fractions, and never a blended rate alone.

### Visualization (canvas `c7`, 720×360)

Weight table with the reweighted recovery drawn as three bars landing on the true-rate line. Weights,
reweighted totals, and every bar height computed in JS from the tier and coverage constants.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Weight = 1 / Coverage → Both Channels Recover 22.0%" — subtitle (15px `#555`, y=44): "Illustrative Example — weights require a cohort frame the channels do not contain".
- **Weight table (no header rule, plain text grid, 15px):** column x positions 80 (tier), 230 (coverage), 360 (weight), 500 (reweighted converters), 640 (reweighted base). Header row at y=76 in bold `#333`: "Tier", "Coverage", "Weight", "Weighted conv.", "Weighted base".
- **App rows (label "App" bold `#27ae60` 15px at x=80, y=100):** Light row y=122 — "Light", "15%", "6.667", "4,000", "40,000"; Heavy row y=144 — "Heavy", "40%", "2.500", "18,000", "60,000"; total row y=166 in `#27ae60` — "App total", "30.0%", "—", "22,000", "100,000".
- **Web rows (label "Web" bold `#2980b9` 15px at x=80, y=196):** Light row y=218 — "Light", "45%", "2.222", "4,000", "40,000"; Heavy row y=240 — "Heavy", "70%", "1.429", "18,000", "60,000"; total row y=262 in `#2980b9` — "Web total", "60.0%", "—", "22,000", "100,000".
- **Recovery bars (horizontal, scale 1 pp = 8px, left edge x=210, 18px tall):** app bar at y=292 in `#27ae60` (22.0% → 176px) labeled "App reweighted 22.0%" to its right in `#27ae60` 14px; web bar at y=316 in `#2980b9` labeled "Web reweighted 22.0%".
- **Truth marker:** dashed (5/4) `#e74c3c` 2px vertical line at the 22.0% position (x = 210 + 22×8 = 386) from y=286 to y=340, labeled "cohort truth" in `#e74c3c` 14px at x=392, y=284.
- **Row labels for the bars (14px `#555`, right aligned at x=202):** "reweighted" at y=305 spanning both bars.
- **Bottom bold red (centered, y=352):** "Reweighted union 44,000 / 200,000 = 22.0% — and it stays 22.0% at any channel row-share".

## Reconciliation of the shared scenario

Every figure on the page derives from the constants below. A reviewer can check each line by hand.

**Cohort and truth**
- Light 40,000 × 0.10 = 4,000; Heavy 60,000 × 0.30 = 18,000; total 22,000 / 100,000 = **22.0%**.
- Tier sizes sum: 40,000 + 60,000 = 100,000. ✓

**Identifier lifetime (section 1)**
- Visits: 60,000 × 4 + 40,000 × 1 = 280,000, identical in both channels. ✓
- Cookie grouping of [1, 4, 13, 22] at a 7-day cap: {1,4} (gap 3 ≤ 7), {13} (gap 9 > 7), {22} (gap 9 > 7) → 3 groups.
- App ids 60,000 + 40,000 = 100,000; web ids 60,000 × 3 + 40,000 = 220,000.
- Returning numerator = 60,000 in both. 60,000/100,000 = 60.0%; 60,000/220,000 = 27.2727% → **27.3%**.
- 0.600 / 0.272727 = 2.200 = 220,000 / 100,000. ✓

**Consent gates (section 2)**
- App observed 6,000 + 24,000 = 30,000; web 18,000 + 42,000 = 60,000.
- App conversions 6,000 × 0.10 + 24,000 × 0.30 = 600 + 7,200 = 7,800 → 7,800/30,000 = **26.0%**.
- Web conversions 18,000 × 0.10 + 42,000 × 0.30 = 1,800 + 12,600 = 14,400 → 14,400/60,000 = **24.0%**.
- Heavy share: 24,000/30,000 = 80.0%; 42,000/60,000 = 70.0%; 60,000/100,000 = 60.0%. ✓

**Union (section 3)**
- Naive: (7,800 + 14,400)/(30,000 + 60,000) = 22,200/90,000 = 24.6667% → **24.7%**, +2.67 pp.
- Overlap under independence: Light 0.15 × 0.45 = 0.0675 × 40,000 = 2,700; Heavy 0.40 × 0.70 = 0.28 × 60,000 = 16,800; total 19,500.
- Distinct observed: Light 40,000 × (0.15 + 0.45 − 0.0675) = 21,300; Heavy 60,000 × (0.40 + 0.70 − 0.28) = 49,200; total 70,500 = 90,000 − 19,500. ✓
- Deduped rate: (21,300 × 0.10 + 49,200 × 0.30)/70,500 = (2,130 + 14,760)/70,500 = 16,890/70,500 = 23.957% → **24.0%**, +1.96 pp.
- Coverage change (Heavy app 40% → 70%): app rows 6,000 + 42,000 = 48,000; conversions 600 + 12,600 = 13,200 → 27.5%.
- New union: (13,200 + 14,400)/(48,000 + 60,000) = 27,600/108,000 = **25.556%**, i.e. 25.6%, a +0.889 pp move from the 24.667% baseline and +3.56 pp over truth.

**Late arrivals (section 4)**
- Arrival shares 0.820, 0.090, 0.045, 0.025, 0.013, 0.005, 0.0015, 0.0005 sum to 1.0000. ✓
- Integer counts 6,396 + 702 + 351 + 195 + 101 + 39 + 12 + 4 = 7,800. ✓
- Cumulative: 6,396 / 7,098 / 7,449 / 7,644 / 7,745 / 7,784 / 7,796 / 7,800 → converges to the settled app total. ✓
- Same-day understatement (7,800 − 6,396)/7,800 = 1,404/7,800 = **18.0%**.
- Ratios: 6,396/14,400 = 44.42%; 7,800/14,400 = 54.17%.
- Same-day app rate 6,396/30,000 = **21.32%** < web 24.0%; settled 26.0% > 24.0% → the comparison's sign flips.

**Sessions (section 5)**
- Gaps between consecutive events: 3, 4, 5, 45, 3, 6, 4, 8, 4, 8, 5. Only 45 > 30 → app splits once → 2 sessions.
- App session 1 = {0,3,7,12}: 4 events, 12 min. Session 2 = {57,…,95}: 8 events, 38 min. Events 4 + 8 = 12. ✓
- Web splits at 57 and 78 → {0,3,7,12}, {57,60,66,70}, {78,82,90,95}: 4 + 4 + 4 = 12 events. ✓
- Durations 12 + 13 + 17 = 42 vs 12 + 38 = 50; (50 − 42)/42 = **+19.05%** for the app.
- Session count (3 − 2)/2 = **+50.0%** for the web; events/session (4 − 6)/6 = **−33.3%**.
- At scale: 100,000 × 12 = 1,200,000 events either way; 200,000 vs 300,000 sessions. ✓

**Stitching (section 6)**
- Raw ids 55,000 × 1 + 30,000 × 2 + 15,000 × 3 = 55,000 + 60,000 + 45,000 = 160,000; people 55,000 + 30,000 + 15,000 = 100,000. ✓
- Excess ids 160,000 − 100,000 = 60,000 = links required. Cross-check: 30,000 × 1 + 15,000 × 2 = 60,000. ✓
- True links found = 0.70 × 60,000 = 42,000; missed 18,000.
- Asserted links = 42,000 / 0.96 = 43,750; false = 43,750 − 42,000 = 1,750.
- Entities = 160,000 − 43,750 = 116,250; and 100,000 + 18,000 − 1,750 = 116,250. ✓
- Inflation 116,250/100,000 − 1 = **16.25%**.
- Rates: 22,000/160,000 = 13.75%; 22,000/116,250 = 18.924% → 18.9%; 22,000/100,000 = 22.0%.

**Reweighting (section 7)**
- App weights 40,000/6,000 = 6.6667 and 60,000/24,000 = 2.5.
- Numerator 600 × 6.6667 + 7,200 × 2.5 = 4,000 + 18,000 = 22,000; denominator 6,000 × 6.6667 + 24,000 × 2.5 = 100,000 → **22.0%**. ✓
- Web weights 40,000/18,000 = 2.2222 and 60,000/42,000 = 1.42857.
- Numerator 1,800 × 2.2222 + 12,600 × 1.42857 = 4,000 + 18,000 = 22,000; denominator 100,000 → **22.0%**. ✓
- Reweighted union 44,000/200,000 = **22.0%**, invariant to channel row-share because each channel already sums to the frame.

**Non-degeneracy check:** every probability used lies strictly between 0 and 1 (0.05% is the smallest,
0.82 the largest arrival share; coverages 0.15-0.70; rates 0.10-0.30). No denominator is zero. No
comparison collapses to an identity — the two channels differ on every metric shown, and the corrected
value differs from both raw values.

## Regeneration instructions

- **Layout:** detail page. h1 (no index number) + `.subtitle` + one `.philosophy` callout (which carries the "Illustrative Example" disclaimer as a second paragraph), then one unnumbered `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (50%) holds an `.obj-title` div — a refined restatement of the pitfall carrying a concrete number from that section's own bullets, never a copy of the heading — plus a `<ul>` of labeled bullets; right `<td>` (50%, centered) holds the canvas. Even table rows have background `#fafcfe`. The shared-cohort table and the reconciliation section are `.md`-only; they do not appear in the html except as the constants the script declares.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em. Table cell borders `1px solid #e0e0e0`, padding 20px 24px. `td:first-child` and `td:last-child` are both 50% — shrink a chart via the canvas `style.maxWidth`, never by narrowing the cell. No nav bar, no back/home links, no cross-page links, no `thead`, no status badges.
- **Canvas:** intrinsic `width` 720 on every chart; heights 340-360 as given per section. A shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Chart ids are sequential in page order, `c1` through `c7`.
- **Single source of truth:** the script declares, once, `LIGHT = 40000`, `HEAVY = 60000`, `RL = 0.10`, `RH = 0.30`, `A_LIGHT = 6000`, `A_HEAVY = 24000`, `W_LIGHT = 18000`, `W_HEAVY = 42000`, `REPEAT = 60000`, `ONCE = 40000`, `VISIT_DAYS = [1,4,13,22]`, `COOKIE_CAP = 7`, `EVENTS = [0,3,7,12,57,60,66,70,78,82,90,95]`, `IDLE = 30`, `TAB_CLOSE = [57,78]`, `ARRIVALS = [6396,702,351,195,101,39,12,4]`, `DEVICES = [[1,55000],[2,30000],[3,15000]]`, `RECALL = 0.70`, `PRECISION = 0.96`, `A_HEAVY_2 = 42000`. Every printed count, percentage, ratio, weight, and bar dimension is derived from those at render time. No rate or total is typed as a literal inside a label string.
- **Helpers:** `num(v)` formats an integer with thousands separators; `pct(v, d)` formats a fraction as a percentage with `d` decimals; `pp(v, d)` formats a signed percentage-point delta; `chg(v, d)` formats a signed relative change as a percentage; `cookieGroups(days, cap)` returns the cookie split used by chart 1; `sessions(events, splitFn)` returns the session partition used by chart 5.
- **No `Math.random()` anywhere on this page.** All data is a hardcoded literal count or derived from the constants above. If a future edit needs generated data, add an inline seeded generator per chart function with its own fixed seed: `function lcg(seed){var s=seed;return function(){s=(s*16807)%2147483647;return s/2147483647;};}` — and still compute every printed statistic from the plotted points.
- **Palette:** primary blue `#1a5276` / `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#555`/`#333`, hairline `#bbb`.
- **Naming:** no real company, platform, product, or OS-feature names. Channels are "the app" and "the browser"; gates are "the OS permission prompt" and "the consent banner"; storage limits are "the browser's storage cap"; people are Alice/Bob. Identifiers shown in examples are obvious placeholders ("id #1"). No credential-shaped strings anywhere. Every constructed figure is labeled "Illustrative Example".
