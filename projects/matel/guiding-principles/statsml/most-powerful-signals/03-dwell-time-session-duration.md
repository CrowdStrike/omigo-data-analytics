# Dwell Time & Session Duration

**Page type:** detail page (most-powerful-signals compact style: per-section two-column layout table, text left 45% with tag pills / labeled bullets / example / key-point, canvas right 55%; final full-width meta-point callout)
**HTML title tag:** Dwell Time & Session Duration

**Subtitle:** How long a user stays — the strongest implicit signal of content quality, powering feed ranking at every major platform

## The Anti-Clickbait Signal

**Tags:** `signal` (blue), `best practice` (green)

- **Click vs dwell** — click predicts, dwell verifies delivery
- **Clickbait signature** — high CTR, near-instant abandonment
- **Facebook 2014-2017** — downranked quick returns ("time spent away")
- **Search SAT clicks** — long dwell feeds relevance models
- **Practice** — rank on dwell-weighted clicks, never raw clicks

*Clickbait: 8% CTR but 6s dwell; straight headline: 4% CTR, 90s dwell.*

**Key point:** ranking on clicks without dwell selects for broken promises.

### Visualization (canvas `c1`, 720×300)

Split-panel paired bars: clickbait headline vs quality article, each with a CTR bar and a median-dwell bar.

- **Title (bold 14px `#1a5276`, top center):** "Click Says \"Promising\" — Dwell Says \"Delivered\"".
- **Divider:** dashed gray `#bdc3c7` vertical line (dash 4/3) at mid-width from y=35 to h-30.
- **Panel headings (bold 13px):** left "Clickbait headline" in `#e74c3c`; right "Quality article" in `#27ae60`.
- **Bars** (baseline y=235, max height 150 for a value of 100, bar width 70, 30px gap between the pair): click-rate bar always fill `rgba(26,82,118,0.35)` stroke `#1a5276`; dwell bar in the panel color (0.7 alpha fill).
  - Left panel: CTR height value 82 labeled "8% CTR", dwell height value 8 labeled "6s" (red).
  - Right panel: CTR height value 45 labeled "4% CTR", dwell height value 68 labeled "90s" (green).
- **Under-bar labels (11px `#1a5276`):** "click rate" and "median dwell".
- **Annotations (bold 11px `#e67e22`, centered per panel):** "promise kept, bet lost" (left), "promise modest, bet won" (right).
- **Caption (bottom center, 11px `#1a5276`):** "rank on dwell-weighted clicks: the left panel gets buried, the right gets promoted".

## Heavy-Tailed: Means Mislead

**Tags:** `bias` (orange), `failure mode` (red), `defense` (green)

- **Log-normal shape** — mode seconds, median under a minute, tail hours
- **Mean trap** — sits 2-3x above median, moves with tail
- **A/B damage** — inflated variance; one open tab flips results
- **Fix** — report p25/p50/p90 or log-transform before testing
- **Robust variants** — winsorize or cap (e.g. 30 min/pageview)

*"+14% average session" traced to 40 overnight tabs out of 200,000; median flat.*

**Failure mode:** the mean is a tail-weighted lottery — the typical experience is the median.

### Visualization (canvas `c2`, 720×300)

Log-normal dwell histogram with median and mean markers.

- **Title:** "Dwell Distribution — Log-Normal, Long Right Tail".
- **Bins (24):** `[4, 30, 52, 44, 33, 24, 17, 12, 9, 7, 5, 4, 3, 3, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1]`, scale max 55; padding top 45 / bottom 55 / left 55 / right 25.
- **Style:** bins 0–13 filled `rgba(26,82,118,0.35)`; bins 14+ (tail) filled `rgba(230,126,34,0.55)` over a faint `rgba(230,126,34,0.10)` tail-region shade; gray `#999` baseline.
- **Markers (dashed vertical, dash 5/4, width 2, bold 12px label):** green `#27ae60` at bin 3, "median 34s — the typical user"; red `#e74c3c` at bin 9, "mean 78s — dragged by the tail".
- **Tail annotation:** bold 11px orange "binge sessions + open tabs live here".
- **X labels (gray `#555` 11px):** "0s" left, "dwell (seconds)" center, "300s+" right.
- **Caption (bottom center, 11px `#1a5276`):** "report p25 / p50 / p90 or log-transform — never trust the raw mean of this shape".

## Scroll Depth & Read Velocity

**Tags:** `signal` (blue), `rule of thumb` (blue)

- **Depth funnel** — well under half reach the article's bottom
- **Velocity** — steady scroll = reading; one flick = skimming
- **Industry practice** — Chartbeat engaged time; Medium pays on depth + time
- **Combined signal** — 60s at 90% depth beats parked tab
- **Rule of thumb** — ~250 words/min; dwell below words÷250 means unread

*7 minutes dwell with zero scroll past the first screen is a background tab.*

**Key point:** time × depth × activity is the real signal — any one alone is trivially faked.

### Visualization (canvas `c3`, 720×300)

Horizontal funnel bars: % of readers reaching each scroll depth.

- **Title:** "Scroll Depth Funnel — % of Readers Reaching Each Point".
- **Stages / values / notes:** Headline 100% "everyone starts here"; 25% depth 71%; 50% depth 46% "fewer than half get this far"; 75% depth 24% "committed readers only"; Article end 11% "the true \"read\" rate". Padding top 45 / bottom 40 / left 130 / right 175; one row per stage.
- **Colors:** first three rows fill `rgba(26,82,118,0.35)` stroke `#1a5276`; "75% depth" fill `rgba(230,126,34,0.6)` stroke `#e67e22`; "Article end" fill `rgba(231,76,60,0.6)` stroke `#e74c3c`. Stage names right-aligned 12px `#1a5276` left of bars; bold percent labels in the stroke color right of bars; notes italic 10px `#777`.
- **Caption (bottom center, 11px `#1a5276`):** "pair depth with velocity: steady scroll = reading, one fast flick to the bottom = skimming".

## Short Dwell as a Negative Label

**Tags:** `signal` (blue), `failure mode` (red)

- **Bounce** — land and leave within seconds signals dissatisfaction
- **Pogo-sticking** — quick return to search = negative vote
- **Classic thresholds** — Fox et al. 2005: ≥30s satisfied, <10-15s dissatisfied
- **Value** — abundant negative labels for rankers, no user effort
- **Caveat** — short dwell can mean instant success ("good abandonment")

*A 4-second "pharmacy hours" visit succeeded instantly — good abandonment inverts the label.*

**Failure mode:** treating every short dwell as failure penalizes pages that answer fast.

### Visualization (canvas `c4`, 720×300)

Histogram with three labeled zones: bounce, ambiguous, satisfied.

- **Title:** "Short Dwell = Negative Label — Bounce & Pogo-Sticking Zone".
- **Bins (18):** `[38, 46, 28, 14, 10, 12, 16, 20, 22, 20, 17, 14, 11, 8, 6, 4, 3, 2]`, scale max 50; padding top 45 / bottom 60 / left 55 / right 25. Cut bin 4 (~10s), SAT bin 10 (~30s).
- **Zone shading:** `rgba(231,76,60,0.10)` over bins 0–3; `rgba(39,174,96,0.08)` over bins 10+.
- **Bar colors:** bins < 4 `rgba(231,76,60,0.65)`; bins ≥ 10 `rgba(39,174,96,0.65)`; middle bins `rgba(26,82,118,0.35)`.
- **Boundary markers:** dashed vertical lines (dash 5/4, width 2), red `#e74c3c` at ~10s and green `#27ae60` at ~30s.
- **Zone labels:** bold 12px red "bounce / pogo-stick" plus italic 10px "(or instant success —" / "\"good abandonment\")"; gray 11px "ambiguous"; bold 12px green "SAT clicks (≥ 30s)".
- **X labels (gray `#555` 11px):** "0s", "~10s", "~30s", "dwell (s)".
- **Caption (bottom center, 11px `#1a5276`):** "quick-backs supply abundant negative labels — but model instant-answer intents separately".

## The Thresholding Problem

**Tags:** `trade-off` (orange), `defense` (green)

- **Opposite meanings** — 30s: engaged on recipe, friction on checkout
- **More flips** — long dwell on FAQ/search pages means failure
- **Magic-number trap** — a global 30s cutoff is an unvalidated assumption
- **Fix** — normalize observed ÷ expected dwell per surface
- **Video analogue** — completion rate beats raw watch seconds

*A sitewide "dwell > 30s" KPI celebrates checkout friction and broken search as engagement.*

**Key point:** dwell has no sign until divided by the surface's expectation — normalize first, threshold second.

### Visualization (canvas `c5`, 720×300)

Horizontal bars of expected dwell per page type, with a fixed 30s observation line and per-row verdicts.

- **Title:** "30 Seconds of Dwell — Good or Bad Depends on the Page".
- **Rows (page, typical expected dwell in seconds, verdict):** Recipe page 25s → "✔ engaged" (green, the only good verdict); Long article 180s → "✘ abandoned early" (red); Search results 8s → "✘ ranking failed" (red); Checkout page 12s → "✘ friction" (red). Scale max 200s; padding top 45 / bottom 62 / left 130 / right 130.
- **Bar style:** fill `rgba(26,82,118,0.35)`, stroke `#1a5276`; page names right-aligned 12px `#1a5276`; verdicts bold 12px in `#27ae60`/`#e74c3c` right of the chart.
- **Marker:** dashed orange `#e67e22` vertical line (dash 5/4, width 2) at 30s, labeled bold 11px "the same observed 30s" below.
- **Footnote (11px `#555`):** "bars = typical (expected) dwell per page type".
- **Caption (bottom center, 11px `#1a5276`):** "normalize: observed ÷ expected dwell per surface — one global threshold misreads three of the four rows".

## Measurement Pitfalls & Censoring

**Tags:** `failure mode` (red), `defense` (green)

- **Censoring bias** — last pageview has no next event; dwell unknown
- **Background tabs** — hidden tabs accrue hours of fake dwell
- **Heartbeat pattern** — ping every 5-15s while visible and active
- **Mobile apps** — backgrounding and OS kills truncate differently per platform
- **Consequence** — cross-platform comparisons need instrument-level alignment

*Long-form essays "score lowest" because session-ending pages get censored to zero.*

**Failure mode:** event-delta dwell punishes destination content — if your best pages look worst, suspect the instrument.

### Visualization (canvas `c6`, 720×300)

Two-part diagram: session timeline with a censored final segment, plus a heartbeat-pings strip below.

- **Title:** "Session Timeline — The Last Pageview Has No \"Next Event\"".
- **Timeline (y=105):** gray horizontal axis; five event dots (blue `#1a5276`, 5px) at fractional positions `[0, 0.13, 0.30, 0.52, 0.72]` labeled "page 1"…"page 5 (last)". Segments between consecutive events drawn as thick (8px) green `#27ae60` lines each labeled 10px "measured"; the segment after the last event drawn as thick dashed red `#e74c3c` (dash 6/6) labeled bold 11px "censored — dwell unknown".
- **Italic red caption below timeline:** "the page users END on — often the best one — is the one you cannot measure".
- **Heartbeat strip (y=215):** bold 12px green heading "The fix: visibility-gated heartbeats"; a gray axis with 22 vertical tick pings — first 16 green `#27ae60`, remainder gray `#bdc3c7` (pings stop when tab hidden / session ends). Notes: green 10px "ping every 5-15s while visible + active" (left), gray "pings stop → dwell = pings × interval, no censoring" (right).
- **Caption (bottom center, 11px `#1a5276`):** "heartbeats also kill fake dwell from hidden background tabs".

## Who Ranks on Dwell, and How

**Tags:** `mechanism` (blue), `trade-off` (orange)

- **YouTube 2012** — views to watch time; video length crept up
- **TikTok** — completion and rewatch, dwell normalized by length
- **Netflix** — title/season completion drives recommendations and renewals
- **News feeds** — Facebook, Twitter/X use predicted dwell in ranking
- **Search** — long dwell, no quick return feeds relevance models

*After 2012, mid-roll ads required 10+ minutes and average upload length roughly tripled.*

**Key point:** the dwell variant a platform ranks on becomes the shape of its content — raw seconds breed length, completion breeds brevity.

### Visualization (canvas `c7`, 720×300)

Line chart: average YouTube video length over time with the 2012 ranking-switch marker.

- **Title:** "YouTube Switches Views → Watch Time: Video Length Creeps Up".
- **Data:** years `['2009', '2011', '2012', '2013', '2015', '2017']`, avg length (min) `[3.5, 4.2, 4.5, 6.5, 9.0, 11.5]`, y max 14; padding top 45 / bottom 55 / left 60 / right 200. Rotated y label "avg video length (min)".
- **Segments:** pre-switch (2009–2012) blue `#1a5276`, post-switch orange `#e67e22`, both width 3 with 4px dots colored to match.
- **Switch marker:** dashed orange vertical line (dash 5/4, width 2) at 2012, labeled bold 11px "2012: ranking switch" and italic 10px "views → watch time".
- **Threshold:** dashed red `#e74c3c` horizontal line (dash 3/4) at 10 min, right-side labels 11px "10 min — mid-roll ad threshold" and italic 10px "10:01 videos flood in".
- **Caption (bottom center, 11px `#1a5276`):** "the metric choice, not audience demand, set the new content length".

## Session-Level Signals & "Time Well Spent"

**Tags:** `signal` (blue), `trade-off` (orange), `best practice` (green)

- **Durable signals** — session length, sessions/week, voluntary return frequency
- **Regret curve** — satisfaction is inverted-U in session time
- **Tension** — raw-time maximization selects autoplay, infinite scroll, streaks
- **Industry correction** — YouTube surveys; Facebook 2018 traded ~50M daily hours
- **Better target** — retention and voluntary return, not squeezed minutes

*Autoplay lifts session time 9% but regret rises and next-week returns drop 2%.*

**Failure mode:** optimizing session minutes ignores the regret downslope — the durable objective is time users would choose again.

### Visualization (canvas `c8`, 720×300)

Inverted-U curve: satisfaction vs session length (the regret curve).

- **Title:** "Session Time vs Reported Satisfaction — The Regret Curve".
- **Data:** satisfaction `[10, 38, 62, 78, 85, 84, 78, 68, 55, 42, 30, 20]`, y max 100, peak at index 4; padding top 45 / bottom 55 / left 60 / right 30.
- **Segments:** rising portion (0–peak) green `#27ae60`, falling portion red `#e74c3c`, width 3; blue `#1a5276` 6px dot at the peak; the region right of the peak shaded `rgba(231,76,60,0.07)`.
- **Zone labels (bold 12px):** green "time well spent" (left), red "doomscrolling → regret" (right).
- **Peak annotations:** 11px blue "satisfaction peak" plus italic 10px "the time-maximizing optimizer cannot see this point".
- **Optimizer arrow:** orange `#e67e22` rightward arrow (110px) starting at the peak, labeled bold 10px "optimizer keeps pushing →".
- **Axis labels (11px `#555`):** "session length →" (x), rotated "satisfaction →" (y).
- **Caption (bottom center, 11px `#1a5276`):** "better target: retention and voluntary return rate — a user who leaves happy comes back".

## Gaming & Abuse Vectors

**Tags:** `gaming` (orange), `abuse` (red), `defense` (green)

- **Content stretching** — YouTube's 10-min mid-roll rule bred 10:01 videos
- **Dwell farming** — recipe preambles, 15-slide pagination, withheld answers
- **Bot dwell** — click farms fake timers; check activity entropy
- **Pairing defense** — couple dwell with expensive-to-fake satisfaction checks

*A listicle split into 15 slides doubles dwell while surveys rate it worst.*

**How it's exploited:** friction between user and payoff farms dwell — counter with a paired metric the farmer cannot cheaply move.

### Visualization (canvas `c9`, 720×300)

Split-panel paired bars: single-page article vs 15-slide slideshow, dwell vs satisfaction.

- **Title:** "Farming the Metric: Same Content, Split Into 15 Slides".
- **Divider:** dashed gray `#bdc3c7` vertical mid-line (dash 4/3).
- **Panel headings (bold 13px):** left "Single-page article" in `#27ae60`; right "15-slide slideshow" in `#e74c3c`.
- **Bars** (baseline y=235, max height 145 for value 100, bar width 70, gap 30): dwell bar fill `rgba(26,82,118,0.35)` stroke `#1a5276`; satisfaction bar in panel color at 0.7 alpha. Left panel: dwell 45, satisfaction 72 (green). Right panel: dwell 88, satisfaction 22 (red). Under-bar labels "total dwell" / "satisfaction" (11px `#1a5276`).
- **Annotation:** bold 11px orange "dwell farmed up" above the slideshow's dwell bar with a small orange downward arrow to the bar.
- **Italic 10px panel captions:** green "honest layout: less time, happier reader"; red "Goodhart: the target moved, the value did not".
- **Caption (bottom center, 11px `#1a5276`):** "defense: pair dwell with a satisfaction check (survey, return rate) that is expensive to fake".

## Closing callout (full-width key-point)

**The meta-point:** Dwell is the closest thing to a free satisfaction label — but only when read in context. Its distribution is heavy-tailed (use medians), its meaning flips by page type (normalize, don't hardcode thresholds), its measurement is censored at session ends (use heartbeats), and every producer paid on it will farm it. Optimizing raw minutes eventually optimizes regret; the durable target is time the user would choose to spend again.

## Regeneration instructions

- **Layout:** one `.card-section` per section, each containing an `<h2>` (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` with a single `<tr>`: left `td.text-col` (45%) holding `.tags` pills, `<ul>` bullets, `p.example`, `.key-point`; right `td.viz-col` (55%) with one `<canvas width="720" height="300">` styled `width:100%`, border `1px solid #e0e0e0`, radius 4px. After the last section, a standalone full-width `.key-point` div holds the meta-point paragraph.
- **Page style:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem.
- **Canvas:** shared `setup(id)` helper scaling by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); one IIFE per chart. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML any links use `.html` extensions.
