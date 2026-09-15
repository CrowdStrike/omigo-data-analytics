# Social Media Priors

**Page type:** detail page (backlog-style two-column layout: text left 50%, canvas right 50%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** Social Media Priors

**Subtitle:** An engagement count is mostly a statement about the audience and the feed. What the post itself did is the small residual left over.

**Intro callout:** Every like count arrives pre-loaded with two priors nobody asked for: how many people could see it, and how hard the ranker pushed it. Raw counts and per-follower rates each silently assume a different exponent for the audience effect, and both assume the average post is a meaningful summary. Neither assumption survives contact with the data.

## 1. Audience Sets the Level

**Tags row (`.tags`):** `violet` "core idea" · `blue` "the line is reach" · `magenta` "the post is the residual"

A post is shown to followers by default, and a predictable slice of them tap like out of habit before content quality enters the picture. Across accounts of different sizes, a raw count is therefore a [`.k.audience`] **follower-count measurement** wearing a content-quality costume.

- **The line is the audience.** Fit likes against followers and the trend line is [`.k.audience`] **pure reach**.
- **The residual is the post.** Only [`.k.post`] **vertical distance from the line** carries content signal.
- **Signal is the minority.** Spread around the line is small next to the range along it.
- **Ranking inherits the bias.** Sorting by raw likes sorts by follower count, lightly shuffled.
- **Comparisons need matching.** Two posts are comparable only at similar audience size.

**Key point:** A leaderboard of raw likes is a leaderboard of audiences with noise added, not a leaderboard of posts.

### Visualization (canvas `c1`, 720×330)

Log-log scatter of likes vs followers with an OLS fit computed from the plotted points at render time.

- **Title (bold 15px, `#1a5276`, top center):** "Likes vs Followers — Illustrative Example (log-log)".
- **Plot area:** x=76, y=46, width = canvas−132, height = canvas−106; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** x from log 2 to log 6, tick labels "10², 10³, 10⁴, 10⁵, 10⁶"; y from log 0.8 to log 5.2 with ticks drawn at 1–5, labels "10¹ … 10⁵" (11px `#5a6875`). The padded y-range is deliberate: with `ly = 0.75·lx + noise` and noise bounded at ±0.54, generated values span 0.96 to 5.04, so **no point can fall outside the axes**. The earlier version clipped everything above 10⁴ likes, which biased the very slope the chart is about.
- **Axis labels (12px `#4a5866`):** "Followers" centered below x-axis; "Likes" rotated −90° at x=20.
- **Scatter:** 140 points from a seeded Park–Miller LCG (**seed 1964**): `lx` uniform in [2,6], `ly = 0.75·lx + noise` where `noise = (r+r+r − 1.5) · 0.36`; every point is drawn, none skipped; 3.4px dots filled `rgba(26,82,118,0.45)`. Seed 1964 is chosen because its OLS fit rounds to exactly 0.75, so the computed label and the prose agree — the seed is tuned to the lesson rather than the label being tuned to the seed. Its values span 1.22 to 4.62, comfortably inside the axes.
- **Fit line:** ordinary least squares of `ly` on `lx` computed in JS from the drawn points; stroke `#e74c3c` 3px across the full x-range.
- **Fit label:** `"fitted slope b = " + slope.toFixed(2)` in 12px `#e74c3c`, positioned below the line near x=4.3. The printed number is the computed fit, never a literal.
- **Scatter fill — banded by account size** so the sweep along the audience axis is visible rather than one flat cloud. Bands by `lx`: ≤3 `rgba(41,128,185,0.55)`, ≤4 `rgba(26,82,118,0.50)`, ≤5 `rgba(142,68,173,0.48)`, above `rgba(192,57,43,0.42)`. The band is cosmetic encoding of the x-value already shown — it adds no claim.
- **Residual marker:** at x=4.9, dashed (4/4) vertical `#27ae60` segment (2.4px) from the fitted line up 0.55 log-units, capped with a 5px `#27ae60` dot with a 1.5px white ring (this is the over-performing post itself), labeled "what the post did" in `#27ae60` at its top right.
- **Footnote (11px `#7f8c8d`, bottom left of canvas):** "Synthetic data — illustrates the decomposition, not a measured platform."

## 2. Neither Raw Count nor Rate Is Neutral

**Tags row (`.tags`):** `aqua` "the exponent is the argument" · `blue` "b = 0 vs b = 1" · `green` "fit it, don't pick it"

The reflex fix is likes per follower. But that only removes the audience effect if likes scale *proportionally* with followers, and they generally do not — large accounts carry followers who have gone inactive, and the feed shows any single post to a [`.k.warn`] **shrinking fraction** of them.

- **The general form.** Write `likes ≈ c · followers^b` and [`.k.audience`] **the exponent `b`** is what matters.
- **Raw count assumes b = 0.** It says audience size does not affect likes at all.
- **Rate assumes b = 1.** It says doubling the audience should exactly double likes.
- **Reality sits between.** Sublinear scaling means `0 < b < 1`, so [`.k.bad`] **both metrics tilt**.
- **Each tilt has a direction.** Raw counts flatter large accounts, rates flatter small ones.
- **The fix is estimation.** Divide by `followers^b` with `b` fitted, not by a chosen extreme.

**Key point:** Picking raw counts or picking rates is picking `b = 0` or `b = 1` without saying so. `b` is an empirical quantity — fit it on your own data rather than inheriting it from a metric name.

**Illustrative Example (worked, `b = 0.75`):** Account A has 200 followers and its post got 30 likes; Account B has 500,000 followers and its post got 9,000 likes. Raw count says B won by 300×. Rate says A won: 15% vs 1.8%, a factor of 8.3. Expected likes scale as `followers^0.75`, giving 53.2 for A and 18,803 for B, so the residuals are 30/53.2 = 0.56 and 9,000/18,803 = 0.48 — A beat its own audience prediction by about 18% more than B did. The honest answer is a near-tie, which neither headline metric reports.

### Visualization (canvas `c2`, 720×330)

Three straight lines on log-log axes showing how a size-adjusted score drifts with account size under each choice of `b`.

- **Title (bold 15px, `#1a5276`, top center):** "Score = likes / followers^b, relative to a 100-follower account".
- **Plot area:** x=76, y=48, width = canvas−150, height = canvas−112; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** x from log 2 to log 6 (labels "10² … 10⁶", 11px `#5a6875`); y is log₁₀ of the score ratio, from −1.4 to 3.4, gridline-free, tick labels "1000×, 100×, 10×, 1×, 0.1×" at log values 3, 2, 1, 0, −1 (11px `#5a6875`).
- **Axis labels (12px `#4a5866`):** "Account followers" below; "Score vs a small account" rotated −90° at x=20.
- **Reference line:** solid `#dfe6ec` 1px horizontal at y-log 0, so the flat case is visually anchored.
- **Drift wedges:** before stroking the lines, fill the triangle between the neutral y=0 line and each biased line's right endpoint — `b = 0` in `rgba(231,76,60,0.10)`, `b = 1` in `rgba(230,126,34,0.12)`. The shaded wedge is the size-driven bias the metric introduces; the flat `b = 0.75` line gets no fill because it has no wedge.
- **Three lines,** each `score_log(lx) = (0.75 − b) · (lx − 2)`, drawn from x=2 to x=6, 2.8px:
  - `b = 0` in `#e74c3c` — rises; right-edge label `"raw count (b=0): " + ratio + "× higher"` where `ratio = 10^(score_log(6))`, computed in JS and formatted with thousands separators.
  - `b = 0.75` in `#27ae60` — flat; label "fitted exponent (b=0.75): no drift".
  - `b = 1` in `#e67e22` — falls; label `"rate (b=1): " + ratio + "× lower"` where `ratio = 10^(−score_log(6))`, again computed.
- **Labels:** 12px, right-aligned at x just inside the plot's right edge, vertically offset 4px from each line's endpoint; no label may be a hardcoded multiplier.
- **Footnote (11px `#7f8c8d`):** "Only the fitted exponent leaves the score independent of account size."

## 3. One Post Carries the Account

**Tags row (`.tags`):** `orange` "how it goes wrong" · `yellow` "mean describes nobody" · `red` "one post is the total"

Even within a single account, holding audience fixed, engagement is not spread evenly across posts. Feed ranking is a [`.k.warn`] **winner-take-most amplifier**: a handful of posts get pushed to non-followers and the rest are shown to a fraction of the account's own audience. [`.k.bad`] **The mean of that distribution describes almost none of it.**

- **The mean is not typical.** One amplified post drags the average far above the common case.
- **The median is the workaday post.** It is [`.k.post`] **what happens with no amplification**.
- **The tail is the platform.** Breakout reach is a ranker decision, not a content property.
- **Averages hide the shape.** Mean engagement per post is a summary of two different regimes.
- **A/B tests feel it too.** Heavy tails inflate variance, so per-post lifts need large n.
- **Report both.** Median for the routine post, tail share for how concentrated reach is.

**Key point:** With a heavy-tailed engagement distribution, "average likes per post" is a number that most posts never come close to. Report the median alongside the share of engagement held by the top posts.

**Illustrative Example (100 posts from one 50,000-follower account):** 60 posts at 300 likes, 25 at 700, 10 at 1,800, 4 at 6,000, and 1 at 90,000. That totals 100 posts and 167,500 likes, so the mean is 1,675 while the median is 300 — the mean is 5.6× the median. The single top post holds 53.7% of all engagement, the top five hold 68.1%, and 85 of the 100 posts land below the mean.

### Visualization (canvas `c3`, 720×330)

Bar chart of post counts per engagement bucket, with mean and median markers derived in JS from the literal data.

- **Title (bold 15px, `#1a5276`, top center):** "100 Posts, One Account — Illustrative Example".
- **Data (hardcoded literal, because the counts are the lesson):** `[{likes:300,n:60},{likes:700,n:25},{likes:1800,n:10},{likes:6000,n:4},{likes:90000,n:1}]`.
- **Derived in JS and printed, never hardcoded in the drawing code:** total posts (Σn), total likes (Σ likes·n), mean = total likes / total posts, median = the likes value at the 50th–51st ranked post, top-bucket share = 90000 / total likes.
- **Plot area:** x=64, y=54, width = canvas−110, height = canvas−124; L-shaped axes `#95a5a6` (1.4px).
- **Bars:** five evenly spaced bars, width = 62% of slot, height proportional to `n` against a y-max of 60, 1.2px stroke. **Fill and stroke ramp by regime** — the routine bulk in blue, the amplified tail warming to red, so the two regimes the mean averages over are visually distinct. Fills: `rgba(41,128,185,0.40)`, `rgba(26,82,118,0.40)`, `rgba(230,126,34,0.35)`, `rgba(211,84,0,0.40)`, `rgba(192,57,43,0.45)`; strokes `#2980b9`, `#1a5276`, `#e67e22`, `#d35400`, `#c0392b`. Count printed above each bar in its own stroke color (11px).
- **X tick labels (11px `#5a6875`):** the bucket's likes value with thousands separators, centered under each bar; axis label "Likes per post (bucket)" (12px `#4a5866`) below.
- **Y axis:** ticks at 0, 20, 40, 60 (11px `#5a6875`); label "Number of posts" rotated −90° at x=18 (12px `#4a5866`).
- **Median marker:** vertical dashed (5/4) `#27ae60` 2.2px line at the center of the bucket containing the median post, labeled `"median " + median` in `#27ae60` 12px near the top of the plot.
- **Mean marker:** vertical dashed (5/4) `#e74c3c` 2.2px line positioned by linear interpolation between bucket slot centers at the computed mean, labeled `"mean " + mean.toLocaleString()` in `#e74c3c` 12px, offset vertically from the median label so the two never collide.
- **Tail callout (12px `#e67e22`, above the last bar):** `"1 post = " + share.toFixed(0) + "% of all likes"` with `share` computed at render time.
- **Footnote (11px `#7f8c8d`):** "Mean and median are computed from the plotted counts."

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 50% and `td.viz-col` 50%, both `vertical-align: top`, 12px padding. No index number in the h1.
- **Text blocks:** intro `<p>`, `<ul>` of bold-label bullets (0.92rem, label in the page accent via `li b`), inline `<code>` (background `#e8f0f8`, color `#1a5276`, 2px 5px padding, 3px radius) for `b`, `likes ≈ c · followers^b`, and the exponent values, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem), `.example` block for the worked illustrative examples (italic `#555`, 0.9rem, leading "Illustrative Example" in non-italic bold `#e67e22`).
- **Semantic tag chips (`.tags` / `.tag`)** — the house detail-page vocabulary from `ui-templates/THEMES.md`; reference implementation `05-cognitive-biases/12-automation-bias.html`. One `.tags` row at the top of each section's text column, three chips each. Chip: `display:inline-block`, 0.72rem, weight 600, `2px 10px` padding, `10px` radius, `6px` right margin. Color classes are rgba tints: `blue` `rgba(26,82,118,0.12)`/`#1a5276`, `green` `rgba(39,174,96,0.15)`/`#27ae60`, `red` `rgba(231,76,60,0.12)`/`#e74c3c`, `orange` `rgba(230,126,34,0.15)`/`#e67e22`, `violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `aqua` `rgba(25,158,112,0.14)`/`#17805d`, `yellow` `rgba(201,133,0,0.15)`/`#a06c00`. Chips carry information — never strip them on regeneration.
- **Inline key-concept highlights (`.k`)** — a light background tint on the two or three phrases per section that name the concept, so the idea is findable without reading the sentence. `padding: 1px 4px`, `border-radius: 3px`, `font-weight: 600`. Variants reuse the fixed semantic colors: `.k.audience` (blue tint, `#1a5276`) for reach/audience terms, `.k.post` (green tint, `#27ae60`) for the residual/content signal, `.k.warn` (orange tint, `#e67e22`) for the failure mode, `.k.bad` (red tint, `#e74c3c`) for the wrong-metric claim. At most three per section — highlighting everything highlights nothing.
- **Per-section accent:** section h2s keep the house-blue accent `#1a5276` with the `#2980b9` underline. Body text stays `#2c3e50` and the semantic colors (red key-point, green good, orange warning) never theme, per THEMES.md.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `height: auto`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; bar fill `rgba(26,82,118,0.35)`, scatter fill `rgba(26,82,118,0.45)`; gray labels `#5a6875`/`#4a5866`, axes `#95a5a6`, footnotes `#7f8c8d`.
- **Canvas:** intrinsic 720×330; a shared `setupCanvas(id)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and `ctx.scale`s back to logical coordinates.
- **Randomness:** the only generated data is `c1`'s scatter, from a seeded Park–Miller LCG (`s = (s * 16807) % 2147483647`, seed 1964). Never `Math.random()`. `c2` and `c3` use closed-form values and a literal array respectively.
- **Computed labels:** every statistic printed on a canvas — `c1`'s slope, `c2`'s two multipliers, `c3`'s mean, median and tail share — is calculated at render time from the data actually drawn. No statistic appears as a literal in a label string.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
