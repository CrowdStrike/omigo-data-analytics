# When Gamification Backfires

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** When Gamification Backfires

**Subtitle:** Rank, points, and streaks make people play the score, not the goal it stood for.

**Intro callout:** Goodhart's law: a measure that becomes a target stops being a good measure. Gamification makes it a target on purpose — the gaming is the mechanic working, aimed at the wrong objective.

## 1. The Leaderboard Effect

Attach a visible rank to a proxy and the proxy starts moving on its own.

- **The mechanic works** — the instinct attaches to the number, not its meaning.
- **Same arc everywhere** — micro-commits, thin patent filings, premature hangups.
- **Benchmarks too** — model leaderboards breed tokenmaxxing.
- **The divergence is quiet** — the dashboard improves, so nobody investigates.
- **The only tell** — measure the goal independently to see the gap.

**Key point:** Pair a leaderboard metric with a counter-metric the gaming would damage.

### Visualization (canvas `c1`, 720×320)

Line chart: measured score vs the underlying goal, diverging after a leaderboard launches.

- **Title (bold 16px, `#1a5276`, top center):** "Score vs the Thing It Measured".
- **Plot area:** x=76, y=48, width = canvas−150, height = canvas−110; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** y from 60 to 160 (index, base 100), tick labels every 20 (12px `#5a6875`, right-aligned); x spans months 0–12, axis label "Months" (13px `#4a5866`, centered below).
- **Launch marker:** vertical dashed orange `#e67e22` line (dash 5/4, 2px) at month 3, label "leaderboard launches" (13px `#e67e22`, left-aligned beside the line near the top).
- **Score curve:** v(t) = 100 for t < 3, else 100 + 50·(1 − e^(−(t−3)/3.5)); drawn t=0→12 in 0.25 steps, stroke `#2980b9` 3px.
- **Goal curve:** v(t) = 100 for t < 3, else 100 − 15·(1 − e^(−(t−3)/4)); same sampling, stroke `#e74c3c` 3px.
- **Labels (13px, left-aligned):** "measured score" in `#2980b9` at (month 8, index 138); "underlying goal" in `#e74c3c` at (month 8, index 90).

## 2. Gaming Your Own Goal

Streaks are the purest case: the user games their own goal.

- **The midnight lesson** — one trivial 11:58pm lesson keeps the streak alive.
- **What it measures** — app-opening, long after learning has stopped.
- **The recycled post** — post-daily rankings push creators to repost old content.
- **Second-order damage** — copies degrade the feed and dilute engagement signals.
- **Poisoned training data** — models trained on posting behavior learn the copies.

**Key point:** The cheapest action that satisfies a streak becomes the modal action.

### Visualization (canvas `c2`, 720×320)

Paired-bar histogram: distribution of lesson lengths before vs after streaks matter.

- **Title (bold 16px, `#1a5276`, top center):** "Lesson Length Once the Streak Matters".
- **Data (percent per duration bucket `<1`, `1–3`, `3–5`, `5–10`, `10+` minutes):**
  - before streaks: `[8, 22, 30, 26, 14]`
  - with streaks: `[34, 26, 18, 14, 8]`
- **Plot area:** x=66, y=68, width = canvas−120, height = canvas−132; scale max 40; L-shaped axes `#95a5a6` (1.4px).
- **Bars:** 5 slots; per slot two bars each 0.34·slot-width wide — left bar (before) fill `rgba(39,174,96,0.50)` stroke `#27ae60` 1.4px; right bar (with streaks) fill `rgba(231,76,60,0.50)` stroke `#e74c3c`.
- **X labels:** bucket names (13px `#4a5866`) under each slot; axis label "Lesson length (minutes)" centered below.
- **Legend (top-left inside plot):** green swatch + "before streaks", red swatch + "with streaks" (13px `#2c3e50`).
- **Annotation (13px `#e74c3c`, left-aligned above the first slot's red bar):** "streak-saver lessons".

## 3. The Gatekeeper Variant

Gatekeeper metrics were never meant to motivate anyone, yet they get gamed.

- **A different mechanism** — nobody was invited to compete on a gatekeeper score.
- **Reverse-engineering** — the gaming probes what the ranker or filter rewards.
- **SEO** — rankings bred keyword stuffing, link farms, crawler-first content.
- **The arms race** — each filter update restores precision; evasions erode it.
- **No end state** — the equilibrium is permanent mutual adaptation, not victory.

**Key point:** Incentive gaming can be redesigned away; adversarial gaming can only be managed.

### Visualization (canvas `c3`, 720×320)

Sawtooth line chart: spam-filter catch rate eroding between periodic filter updates.

- **Title (bold 16px, `#1a5276`, top center):** "Filter vs Spammer — the Arms Race".
- **Plot area:** x=76, y=48, width = canvas−150, height = canvas−110; L-shaped axes `#95a5a6` (1.4px).
- **Scales:** y from 50 to 100 (catch rate %), tick labels every 10 (12px `#5a6875`, right-aligned); x spans months 0–24, axis label "Months" (13px `#4a5866`, centered below).
- **Series (stroke `#2980b9`, 3px):** monthly points `[95, 92, 89, 86, 83, 80, 95, 91, 88, 85, 82, 79, 94, 91, 87, 84, 81, 78, 95, 92, 89, 86, 83, 80, 95]` joined by straight segments.
- **Update markers:** filled green `#27ae60` circles (radius 5) at months 0, 6, 12, 18, 24 on the series.
- **Labels (13px):** green `#27ae60` "filter update" near the month-6 marker (above the point); red `#e74c3c` "evasions accumulate" along the decaying stretch around month 9 (below the line).

## 4. Published Metric, Tribal Metric

A metric gets gamed whether its formula is published or only rumored.

- **Published metrics** — effort concentrates exactly on the known formula.
- **Widest gap** — precise gaming pulls proxy and goal furthest apart.
- **Unpublished metrics** — "post daily or lose reach" spreads as folklore.
- **Beliefs outlive tuning** — fixing the algorithm can't fix folklore about it.
- **Both directions** — platforms push cadence for content; creators game it back.

**Key point:** Hiding the metric only converts precise gaming into cargo-cult gaming.

### Visualization (canvas `c4`, 720×320)

Paired-bar chart: where gaming effort lands when the metric is published vs tribal knowledge.

- **Title (bold 16px, `#1a5276`, top center):** "Where Gaming Effort Lands".
- **Data (percent of effort per target, 6 categories `the metric`, `rumor A`, `rumor B`, `rumor C`, `unused factor`, `real quality`):**
  - published: `[70, 5, 5, 5, 5, 10]`
  - tribal: `[25, 20, 18, 15, 12, 10]`
- **Plot area:** x=66, y=68, width = canvas−120, height = canvas−132; scale max 75; L-shaped axes `#95a5a6` (1.4px).
- **Bars:** 6 slots; per slot two bars each 0.34·slot-width wide — left bar (published) fill `rgba(41,128,185,0.50)` stroke `#2980b9` 1.4px; right bar (tribal) fill `rgba(230,126,34,0.50)` stroke `#e67e22`.
- **X labels:** category names (12px `#4a5866`) under each slot; axis label "What the effort targets" centered below.
- **Legend (top-left inside plot):** blue swatch + "published metric", orange swatch + "tribal knowledge" (13px `#2c3e50`).
- **Annotation (13px `#e67e22`, centered above the `unused factor` slot):** "gamed, never even used".

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 45% and `td.viz-col` 55%, both `vertical-align: top`, 12px padding. No index number in the h1.
- **Text blocks:** intro `<p>`, `<ul>` bullets (0.92rem) with `<strong>` lead-ins, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; secondary `#2980b9`; gray labels `#5a6875`/`#4a5866`, axes `#95a5a6`.
- **Canvas:** intrinsic 720×320; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates via a shared `setupCanvas(id)` helper.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
