# Social Media & Content — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, histogram canvas middle 31%, insight canvas right 31%, one table per section)
**HTML title tag:** Social Media & Content — Distribution Patterns

## Post Virality — Why Your Post Probably Won't Go Viral

**Pitfall label:** MOST GET NOTHING, A FEW BLOW UP (WITH A CAP) (color `#795548`)

Almost every post gets ignored, a small fraction spreads widely, and in this simulation spread is hard-capped at 500 shares. The interesting signature: a smooth heavy tail that suddenly terminates in a pile-up at one exact value. Organic decay doesn't do that — an abrupt edge like this is consistent with an imposed limit (rate-limiting, demotion) rather than natural loss of interest.

- About 84% of posts get fewer than 2 shares — basically invisible
- Roughly 1 in 20 breaks 100 shares; those few soak up most of the attention
- Just under 1% hit the 500-share cap and pile up there — the "cliff" in the ECDF
- A sharp edge in an otherwise smooth tail suggests a ceiling was imposed, not emergent

### Visualization (canvas `canvas1`, 420×340)

Histogram of simulated post shares with a hard cap.

- **Data:** 5000 samples, seeded RNG (mulberry32, seed 101). 90% from a shifted Pareto tail: `1/(1-u)^(1/2.5) - 1` (alpha=2.5, almost all below a few shares). 10% from a log-normal `exp(4.5 + 1.3·N(0,1))` hard-capped at 500.
- **Chart:** 50 bins over x range 0–520, x tick labels at 5 evenly spaced values (0 decimals). Bars filled `rgba(142,68,173,0.35)` with 0.5px stroke `#1a5276`. Gaussian-smoothed density line (sigma 1.5 bins) in `#6c3483` (2px) with a 95% SE band filled `rgba(142,68,173,0.18)`. White background, dark gray axes `#333`, padding top 35 / right 20 / bottom 40 / left 50. Y axis labeled with max bin count and 0.
- **Title (bold 13px, `#1a5276`, top center):** "Post Shares — Heavy Tail + Hard Cap at 500"
- **X-axis label:** "Number of Shares"

### Visualization (canvas `canvas1b`, 400×340)

ECDF of the same shares data, showing the cap as a cliff.

- **Title (bold 12px `#1a5276`):** "ECDF — Cap Visible as Cliff"; subtitle (10px `#666`): "Smooth tail terminates in a pile-up at 500".
- **Curve:** ECDF of sorted data over x 0–520, stroke `rgba(142,68,173,0.9)` width 2.5, area under curve filled `rgba(142,68,173,0.12)`. Padding top 40 / right 25 / bottom 45 / left 55.
- **95% marker:** vertical dashed red line (`#e74c3c`, dash 4/3, width 1.5) at the 95th percentile value; annotation below x-axis in `#c0392b` 10px: "95% < {p95} shares" (p95 computed from data, roughly a few shares).
- **Cutoff zone:** 30px-wide band centered at x=480 filled `rgba(231,76,60,0.15)`; bold red (`#e74c3c`) two-line label "CUTOFF" / "CLIFF" with a small downward red arrow triangle pointing at the cliff.
- **Axes labels:** y "100%", "50%", "0%"; x "0", "250", "500"; x-axis title "Shares" in `#666`.

## Time Between Posts — Within-Session vs Between-Session Gaps

**Pitfall label:** TWO PROCESSES, ONE HISTOGRAM (color `#2980b9`)

On a log scale, inter-post gaps show two modes because they mix two different processes: gaps inside a posting session (a few minutes apart, posting streaks) and gaps between sessions (hours to days between visits — including plenty of once-a-day users). Modeling the mixture as one distribution misestimates both: the fitted "typical gap" describes neither the streak rhythm nor the return rate.

- Left mode: gaps of 1-5 minutes — consecutive posts inside the same session
- Right mode: gaps of hours to days — the time between one visit and the next
- The two modes are two processes, not two user types — the same person contributes to both
- Fit one distribution to the mixture and you misestimate both the session rhythm and the return rate

### Visualization (canvas `canvas2`, 420×340)

Histogram of inter-post intervals on a log10 scale, bimodal (two mixed processes).

- **Data:** 4000 samples, seed 202. 55% within-session mode: `1 + 4·u + 0.5·N(0,1)` minutes, floored at 0.5. 45% between-session mode: log-normal `exp(6.6 + N(0,1)*1.0)` minutes (~12 hours median, spanning hours to days), capped at 20000. Values plotted as log10(minutes).
- **Chart:** 50 bins over x range -0.5 to 4.5 (1 decimal on tick labels). Bars `rgba(231,76,60,0.35)` stroked `#27ae60`. Smoothed density line `#6c3483` with SE band `rgba(142,68,173,0.18)` (shared histogram helper).
- **Title:** "Inter-Post Intervals (Log₁₀ Minutes)"
- **X-axis label:** "log₁₀(minutes) — 0=1min, 3=~17hrs, 4=~7days"

### Visualization (canvas `canvas2b`, 400×340)

Session-map scatter: one row per user, one dot per inter-post gap, split by a session-boundary line.

- **Title:** "Session Map — Within-Session vs Between-Session Gaps"; subtitle: "Each dot = one inter-post gap for a user".
- **Scatter:** 80 user rows (y position = user index); dots (2.5px radius) at x = log10(gap) mapped over log range -0.3 to 4.5, sampled every 5th gap from the same 4000-gap dataset. Dots colored red `rgba(231,76,60,0.7)` when gap < 30 min (within session), green `rgba(39,174,96,0.6)` otherwise (between sessions).
- **Session boundary:** vertical dashed orange `#f39c12` line (dash 5/3, width 1.5) at log10(30), full plot height. Centered labels in `#d35400`: bold 11px "SESSION BOUNDARY" and 9px "~30 min of inactivity".
- **Legend (bottom left):** red swatch "Within session (<30 min)", green swatch "Between sessions (hrs-days)".
- **X ticks:** at log values 0,1,2,3,4 labeled "1m", "10m", "1.5h", "17h", "7d"; x-axis title "Gap Duration (log scale)"; y-axis label "Users".

## Follower Counts — A Winner-Take-All Shape

**Pitfall label:** THE RICH GET RICHER (color `#27ae60`)

In this simulated network, over 99% of users have fewer than 1,000 followers while a tiny elite sits at millions — attention inequality with a Gini near 0.75. A power law this steep is consistent with preferential attachment: accounts that already have followers get surfaced more, and the head keeps pulling away from the middle.

- Over 99% of users have fewer than 1,000 followers
- The top 1% of accounts holds roughly half of all followers (Lorenz curve, right)
- One explanation: preferential attachment — visibility compounds like interest
- The steepness of the drop-off is a measure of how concentrated attention is

### Visualization (canvas `canvas3`, 420×340)

Histogram of follower counts on log10 scale, power-law shape.

- **Data:** 8000 samples, seed 303. Power law: `10/(1-u)^(1/1.1)`, capped at 5,000,000. Plotted as log10(max(x,1)).
- **Chart:** 50 bins over x range 0.5 to 7 (1 decimal). Bars `rgba(41,128,185,0.35)` stroked `#e74c3c`. Smoothed density line + SE band as in the shared helper.
- **Title:** "Follower Counts (Log₁₀ Scale)"
- **X-axis label:** "log₁₀(followers) — 1=10, 3=1K, 6=1M"

### Visualization (canvas `canvas3b`, 400×340)

Lorenz curve of follower inequality with Gini annotation.

- **Title:** "Lorenz Curve — Follower Inequality"; subtitle: "How much of total attention the top % commands".
- **Equality line:** gray dashed diagonal (`#bbb`, dash 6/4, width 1.5) from bottom-left to top-right, with rotated (-45°) 9px `#999` label "Perfect Equality" at ~30% width, 60% height.
- **Lorenz curve:** cumulative population vs cumulative followers from the sorted raw data (~200 sampled points), stroke `rgba(41,128,185,0.9)` width 3; area between Lorenz and equality filled `rgba(41,128,185,0.25)`.
- **Gini annotation (center-right of plot):** bold 14px `#c0392b` "Gini = {computed}" (≈0.75) with 10px `#e74c3c` "(extreme inequality)" beneath.
- **Top-1% annotation (top right, bold 10px `#e74c3c`, right-aligned):** "Top 1% holds ~{N}%" / "of all followers" (≈50%), with a small red arrow pointing toward the curve.
- **Axes:** y "0%"–"100%" (% of Total Followers, rotated y-axis title); x "0%", "50%", "100%" with title "% of Users (cumulative)".

## Comment Sentiment — Only Strong Feelings Get Typed Out

**Pitfall label:** THREE CAMPS: LOVE IT, HATE IT, OR SILENT (color `#e74c3c`)

Sentiment scores land in three clumps: very negative, very positive, and a large neutral mass — with valleys where "mildly positive" and "mildly negative" should be. One explanation: typing a comment has a cost, and only strong emotion clears it, so lukewarm reactions never make it into the data. The right chart shows an illustrative "engagement smile" — an action-rate model where the extremes act and the middle stays silent.

- Three modes: angry (~-0.8), neutral (~0), enthusiastic (~+0.75) — mild opinions are missing
- The neutral mass is consistent with lurkers who saw it but felt nothing strongly
- If comments skew polarized, that may reflect who bothers to type, not what the audience felt
- Optimizing on comment volume implicitly weights the extremes — worth checking before using it as a signal

### Visualization (canvas `canvas4`, 420×340)

Trimodal histogram of comment sentiment scores.

- **Data:** 5000 samples, seed 404, clipped to [-1, 1]. 20% angry spike: `-0.8 + 0.12·N(0,1)`; 45% neutral mass: `0 + 0.15·N(0,1)`; 35% enthusiastic spike: `0.75 + 0.12·N(0,1)`.
- **Chart:** 50 bins over x range -1 to 1 (1 decimal). Bars `rgba(230,126,34,0.35)` stroked `#e67e22`. Smoothed density line + SE band as in the shared helper.
- **Title:** "Comment Sentiment Score — Trimodal"
- **X-axis label:** "Sentiment (-1 = negative, 0 = neutral, +1 = positive)"

### Visualization (canvas `canvas4b`, 400×340)

Bar chart of engagement rate by sentiment zone — the "engagement smile" U-shape.

- **Title:** "Engagement Waterfall — Extremes Drive Action"; subtitle: "Sentiment vs engagement rate (illustrative model)".
- **Bars (7 sentiment zones, left to right), with engagement rates and fill colors:**
  - Very Neg [-1,-0.6]: 82%, `rgba(231,76,60,0.75)`
  - Neg [-0.6,-0.3]: 35%, `rgba(231,76,60,0.5)`
  - Mild Neg [-0.3,-0.1]: 12%, `rgba(230,126,34,0.6)`
  - Neutral [-0.1,0.1]: 5%, `rgba(149,165,166,0.6)`
  - Mild Pos [0.1,0.3]: 10%, `rgba(46,204,113,0.5)`
  - Pos [0.3,0.6]: 38%, `rgba(39,174,96,0.6)`
  - Very Pos [0.6,1]: 78%, `rgba(39,174,96,0.8)`
- Each bar stroked `#333` 0.5px, percentage value in bold 11px `#1a5276` above the bar, two-line 9px `#555` zone labels below.
- **Smile connector:** dashed red line (`#e74c3c`, dash 4/3, width 2) joining bar tops; centered labels near baseline in `#c0392b`: bold 10px '"Engagement Smile"' and 9px "Neutrality = silence".
- **Axes:** y "0%" to "100%"; x-axis title "Sentiment Zone".

## Content Half-Life — How Fast Your Post Dies Depends on Where You Posted

**Pitfall label:** EACH PLATFORM AGES CONTENT DIFFERENTLY (color `#8e44ad`)

On a real-time feed, engagement decays within minutes. On a search-driven video platform, content keeps collecting views for weeks. On an algorithmic short-video platform, content either dies in hours or gets a second bump days later. The decay shape mirrors the distribution architecture: feed-based means fast death, search-based means a long tail, recommendation-based means bimodal.

- Real-time feeds: engagement gone within minutes to an hour
- Search-driven video: a long tail — people keep finding it for weeks
- Algorithmic short-video: either dead fast, or a revival bump days later (bimodal)
- Reading the decay curve is a quick way to infer how content gets distributed

### Visualization (canvas `canvas5`, 420×340)

Bimodal histogram of content half-life on a short-video platform.

- **Data:** 4000 samples, seed 505, in hours. 60% dead fast: `0.2 + Exp(λ=2)`, capped at 3. 25% moderate decay: `3 + 0.8·N(0,1)`, floored at 1. 15% algorithmically revived: `24 + Exp(λ=0.05)`, capped at 168 (1 week).
- **Chart:** 60 bins over x range 0–170 (0 decimals). Bars `rgba(39,174,96,0.35)` stroked `#8e44ad`. Smoothed density line + SE band as in the shared helper.
- **Title:** "Content Half-Life — Short-Video Platform (Dead or Revived)"
- **X-axis label:** "Hours until 50% engagement reached"

### Visualization (canvas `canvas5b`, 400×340)

Three overlaid decay curves comparing platform architectures over one week.

- **Title:** "Platform Architecture as Decay Curves"; subtitle: "Engagement remaining over time (normalized)".
- **Curves (200 points over t = 0–168 hours, width 2.5):**
  - "Real-time feed" — `exp(-t/0.3)` (half-life ~18 min), solid `#e74c3c`
  - "Search video" — `0.3 + 0.7·exp(-t/120)` (long tail), dashed 8/4 `#27ae60`
  - "Algo short-video" — `min(1, exp(-t/1.5) + 0.6·exp(-((t-72)/20)²))` (fast decay + Gaussian revival bump at ~72h), dashed 3/3 `#1a5276`
- **Grid:** light horizontal lines `#eee` at quarters; area under the short-video revival bump filled `rgba(26,82,118,0.08)`.
- **Annotations:** bold `#1a5276` two-line "Algorithm" / "Revival!" with arrow at the 72h bump; bold `#e74c3c` "Dead in" / "minutes" near left; bold `#27ae60` right-aligned "Evergreen" / "(search-driven)" near right.
- **Legend (bottom right):** line samples with names in each platform color.
- **Axes:** x ticks at 0, 24, 48, 72, 120, 168 h labeled "0", "1d", "2d", "3d", "5d", "7d", title "Time Since Post"; y "0%", "50%", "100%".

## Reply Chain Depth — Most Threads Are Dead After Two Messages

**Pitfall label:** CONVERSATIONS DIE FAST (color `#e67e22`)

In this simulation each reply has a 35% chance of drawing another — so about 65% of threads end after a single reply, and depth collapses geometrically. The right chart contrasts two modeled populations: ordinary discussions (50% continue) versus arguments (80% continue). Under those assumptions, nearly every thread that reaches 8+ replies is an argument — a reminder that "long thread" and "healthy conversation" are not the same metric.

- ~65% of threads: one reply and done
- ~31%: a quick 2-3 reply exchange
- Under 5% reach 4+ replies; 8+ is vanishingly rare in the base population
- If arguments continue more readily (0.8 vs 0.5 here), they dominate all deep threads

### Visualization (canvas `canvas6`, 420×340)

Histogram of reply-chain depth, geometric collapse.

- **Data:** 2000 threads, seed 606. Depth starts at 1; while `rng() < 0.35` and depth < 20, increment. Geometric(continue=0.35).
- **Chart:** 15 bins over x range 0–15 (0 decimals). Bars `rgba(142,68,173,0.5)` stroked `#8e44ad`. Smoothed density line + SE band as in the shared helper.
- **Title:** "Reply Chain Depth (Replies per Thread)"
- **X-axis label:** "Thread Depth (replies)"

### Visualization (canvas `canvas6b`, 400×340)

Paired-bar comparison of thread-depth distributions: healthy discussions vs arguments.

- **Title:** "What Keeps Threads Alive: Arguments vs Discussions"; subtitle: "Engagement metrics reward arguments over discussions".
- **Data:** two simulated populations of 1000 threads each, depths 1–20. Healthy discussion: geometric with continue probability 0.5 (seed 607). Argument: continue probability 0.8 (seed 608).
- **Bars:** side-by-side half-width bars per depth — healthy in `rgba(39,174,96,0.6)` stroked `#27ae60`, argument in `rgba(231,76,60,0.6)` stroked `#e74c3c`; shared count scale.
- **Legend (top left):** green swatch "Healthy Discussion (p=0.5)", red swatch "Argument (p=0.2, stays alive)".
- **Annotation:** vertical dashed line (`#c0392b`, dash 4/3) at depth 8, with bold 9px `#c0392b` text "8+ replies:" / "arguments dominate".
- **Axes:** x ticks every 2 depths (1, 3, 5, …), title "Thread Depth (replies)"; y labeled max count and 0.

## Posting Frequency vs Growth — There's a Sweet Spot (Stylized Model)

**Pitfall label:** MAGIC MINIMUM TO GET NOTICED (color `#16a085`)

Growth versus posting cadence is not a straight line. In this stylized model, growth is near zero below ~3 posts/week, rises steeply once past the threshold, plateaus around 1-2 posts/day, then declines past ~2 posts/day. Even inside the sweet spot the scatter is wide — cadence sets the ceiling, but it doesn't determine the outcome.

- 0-2 posts/week: near-zero growth — below the visibility threshold
- 3-5 posts/week: growth turns on (~2-6% per week in this model)
- 1-2 posts/day: the plateau (~6-8% weekly) — the marginal post adds little
- Beyond ~2 posts/day: growth declines — a fatigue effect built into the model
- Variance is huge everywhere above the threshold — cadence alone explains little

### Visualization (canvas `canvas7`, 420×340)

Histogram of weekly follower growth rates (mixture model).

- **Data:** 2000 samples, seed 707. 30% below threshold: `0.2 + 0.5·N(0,1)`. 50% sweet spot: `5 + 3·N(0,1)`. 20% over-posting: `3 + 4·N(0,1)`.
- **Chart:** 35 bins over x range -5 to 20 (0 decimals). Bars `rgba(39,174,96,0.5)` stroked `#27ae60`. Smoothed density line + SE band as in the shared helper.
- **Title:** "Weekly Follower Growth Rate Distribution"
- **X-axis label:** "Growth Rate (%/week)"

### Visualization (canvas `canvas7b`, 400×340)

Scatter + mean curve of growth rate vs posting cadence, with threshold and fatigue markers.

- **Title:** "Growth vs Cadence: Threshold, Then Diminishing Returns"; subtitle: "Each dot = one creator. Mean curve shows threshold + fatigue.".
- **Axes ranges:** x = posts/week 0–20 (ticks 0, 5, 10, 15, 20, title "Posts per Week"); y = growth rate -5% to 15% (ticks -5%, 0%, 5%, 10%, 15%, rotated title "Growth Rate (%/week)").
- **Mean curve (solid `#1a5276`, width 2.5):** sigmoid rise then fatigue — `8/(1+exp(-1.5·(p-4)))` plus `-0.5·(p-10)` when p > 10.
- **Variance cloud:** band of mean ± spread filled `rgba(39,174,96,0.15)`, where spread = 1 below 3 posts/week and 4 above.
- **Scatter:** 300 creators (seed 708), uniform cadence 0–20, growth = mean + `N(0,1)·spread·0.7`; 2.5px dots `rgba(39,174,96,0.4)`, clipped to plot.
- **Threshold marker:** vertical dashed red line (`#e74c3c`, dash 5/3) at 3 posts/week, labeled below axis bold "THRESHOLD" / "(3/week)".
- **Fatigue marker:** vertical dashed orange line (`#e67e22`, dash 5/3) at 14 posts/week, labeled "FATIGUE" / "(14+/week)".
- **Zone labels (bold 9px, top of plot):** "Invisible" in `#c0392b` at ~1.5 posts/week, "Optimal Zone" in `#27ae60` at ~8, "Diminishing" in `#e67e22` at ~17.

## Regeneration instructions

- **Layout:** one `.obj-table` (full-width, border-collapse) per section, single `<tr>` with three `<td>`: left 38% text (`.pitfall-label` span, `h3`, paragraph, `ul`), middle 31% centered canvas (420×340), right 31% centered insight canvas (400×340). Cell borders `1px solid #2980b9`, padding 12px.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `h3` in cells `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase with 0.5px letter-spacing. Canvas CSS `width: 100%; height: auto`. No nav bar, no back/home links.
- **Pitfall label colors:** assigned by a small script cycling through `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` in document order.
- **Data generation:** seeded RNG `mulberry32(seed)` per section (seeds 101, 202, 303, 404, 505, 606, 707; insight charts 607/608/708 where noted), Box-Muller `randn()`, inverse-CDF `randExp(lambda)`.
- **Histogram helper:** shared `drawHistogram(canvasId, data, options)` — options bins/title/color/strokeColor/xLabel/min/max/decimals; white plot background; padding top 35 / right 20 / bottom 40 / left 50; title bold 13px `#1a5276` centered; 5 x-tick labels; y axis shows max count and 0; plus a Gaussian-smoothed density line (`#6c3483`, sigma 1.5 bins) with a 95% SE band (`rgba(142,68,173,0.18)`, effective n clamped to [30, 200]).
- **Canvas scaling:** all canvases sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) to intrinsic pixels, and `ctx.scale` back to logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`/`#6c3483`, accent blue `#2980b9`, dark red `#c0392b`, gray text `#555`/`#666`.
- Note: regenerated HTML pages link nowhere (detail page); any grid page linking here uses the `.html` extension.
