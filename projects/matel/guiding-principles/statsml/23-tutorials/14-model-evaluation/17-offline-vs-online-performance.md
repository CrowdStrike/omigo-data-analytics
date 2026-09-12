# Offline vs Online Performance

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks each with a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Offline vs Online Performance

**Subtitle:** A model graded on yesterday's logs and a model serving live users are taking two different exams — a big offline win can vanish the day it ships

## +12% in the Lab, Flat in Production

Tags: `core idea` (blue), `running example` (green)

- **The offline test** — a new recommender is scored on logged history: can it rank past clicks high?
- **The result** — hit rate jumps from 20.0% to 22.4% — a +12% win, champagne is ordered
- **The live test** — shipped to half the users in an A/B test for two weeks
- **The result** — clicks per user: control 5.00%, new model 5.01% — flat, within noise
- **Both are true** — the model really is better at the offline exam; users just didn't care

*Example:* Like acing every practice test and going blank on stage — the rehearsal isn't the show.

**Key point — Offline vs online:** offline scores measure fit to logged history; online metrics measure change in live behavior. They can disagree, and often do.

### Visualization (canvas `c1`, 720×300)

Two side-by-side bar panels split by a vertical dashed divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 16px `#1a5276`, top center):** "The Same Model, Two Scoreboards".
- **Left panel** (x=65, baseline y=225, plot height 145, 250px wide): bold 13px header "offline: hit rate on logged clicks"; y ticks 0–25% every 5%; bars 80px wide: old model 20.0% (blue `#2a78d6`), new model 22.4% (green `#008300`); bold 13px value labels above ("20%" / "22.4%"), names below. Bold 13px green annotation below: "+12% — champagne".
- **Right panel** (x=425, same geometry): header "online: clicks per user, live A/B"; y ticks 0–6% every 2%; bars: control 5.00% (blue), new model 5.01% (green), value labels "5.00%" / "5.01%". Bold 13px red `#e74c3c` annotation below: "flat, within noise".

## The Exam Was Written by the Old Model

Tags: `worked example` (green), `feedback loop` (orange)

- **Where logs come from** — users mostly click what the old model surfaced; the rest come via search
- **Count it** — of 100 logged clicks, 70 were on items the old model recommended
- **The circle** — the offline test asks "can you rank these clicks?" — clicks the old model produced
- **The bias** — a new model gets no credit for great items nobody was ever shown
- **The consequence** — offline scores favor models that imitate the old one, not ones that beat it

*Example:* Grading a new librarian only on books the old librarian put on the display table.

**Key point — Feedback loop:** the logs are not neutral ground truth — they are the old model's opinions, recycled as the answer key.

### Visualization (canvas `c2`, 720×300)

Four-box cycle diagram with orange arrows forming a loop.

- **Title (bold 16px `#1a5276`):** "The Loop: Logged "Truth" Is the Old Model's Output".
- **Boxes (240×56px, fill `#f8f9fa`, 2px colored outline, bold 13px colored first line + 12px `#2c3e50` second line):**
  - top-left (60,70): "old model" / "picks items to show" — outline blue `#2a78d6`
  - top-right (420,70): "users click" / "among what was shown" — outline aqua `#199e70`
  - bottom-right (420,180): "logs record" / "70 of 100 clicks = old picks" — outline yellow `#c98500`
  - bottom-left (60,180): "offline test" / ""rank these clicks!"" — outline violet `#4a3aa7`
- **Arrows (orange `#d95926`, width 2.5, filled triangular heads):** old model → users click; users click → logs record; logs record → offline test; offline test → old model (upward, closing the loop).
- **Arrow labels (muted 12px `#6b7280`):** "shows 30 items" (top edge); "clicks land on" / "shown items" (right edge, two lines); "answer key" (bottom edge); rotated vertical label on the left arrow "rewards imitating it".
- **Bottom annotation (bold 13px red `#e74c3c`, centered):** "the new model is graded on questions the old model set — 70% of the answer key is the old model's taste".

## Two More Gaps: Stale Data and Behavior Shifts

Tags: `why it matters` (red), `mechanisms` (blue)

- **Stale data** — the offline win was measured on last month's tastes; tastes move on
- **Watch it fade** — the same +12% gain remeasured on fresher logs: +8, +4, then +1 after 6 weeks
- **Behavior shifts** — live users react to the change itself: novelty clicks spike, then decay
- **A live example** — a redesign's click lift: +6% week 1, +3% week 2, +1% week 3, 0% week 4
- **Neither shows offline** — logged history can't age forward or react to being changed

*Example:* The demo data was three months old by launch day — the model aced an exam about the past.

**Key point — Why it matters:** offline evaluation freezes the world. Live users age the data and react to the model — two effects no log replay can contain.

### Visualization (canvas `c3`, 720×300)

Two side-by-side decay panels split by a vertical dashed divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 16px `#1a5276`):** "Both Gaps Decay With Time".
- **Left panel — line chart** (x=70, width 250, baseline y=220, plot height 135): header "offline gain vs freshness of logs"; x points at 0w, 2w, 4w, 6w; gains `[+12, +8, +4, +1]` % on a 0–14 scale; violet `#4a3aa7` line width 3 with radius-5 dots and bold 12px "+N%" labels above each point; x axis title "weeks between logs and today"; bold 12px violet annotation "tastes moved on".
- **Right panel — bar chart** (x=420, width 250, same baseline): header "live lift of a redesign, by week"; bars 40px wide for weeks 1–4: `[+6, +3, +1, 0]` % on a 0–7 scale, aqua `#199e70` except week 4 in muted gray `#6b7280` (0 drawn as a 2px stub); bold 12px "+N%" labels above; x labels "wk 1"…"wk 4"; axis title "weeks after the change shipped"; bold 12px aqua annotation "novelty, not value".
- **Bottom annotation (bold 13px red `#e74c3c`, centered):** "logged history can't age forward or react to being changed — live users do both".

## Rehearsal First, Then the Show

Tags: `common mistake` (orange), `best practice` (green)

- **The confusion** — treating the offline score as the result, and the A/B test as a formality
- **The pattern** — across experiments, online gains come in smaller than offline gains promised
- **Use offline for** — filtering out bad candidates cheaply, before risking real traffic
- **Use online for** — the ship decision; only live users can grade live behavior
- **Expect shrinkage** — plan for the live gain to be a fraction of the offline one, sometimes zero

*Example:* The +12% offline model earned +0.2% online — the rehearsal picked the cast, the show set the reviews.

**Key point — Rule:** offline metrics choose which models deserve an A/B test; they never replace one.

### Visualization (canvas `c4`, 720×300)

Scatter plot of offline gain vs online gain for ten experiments (illustrative).

- **Title (bold 16px `#1a5276`):** "Ten Experiments: Offline Promise vs Online Delivery".
- **Axes:** x "offline gain" 0 to +22%, tick labels +0% to +20% every 5%; y "online gain" −2 to +22%, tick labels +0% to +20% every 5% (y title rotated vertical); padding top 55, bottom 55, left 65, right 200; gray `#999` L axes.
- **Diagonal:** dashed light gray `#ccc` line (dash 5/4, width 1.5) from (0,0) to (+21,+21), with rotated muted label along it: "kept its promise".
- **Zero line:** horizontal dashed `#2c3e50` (dash 3/3, width 1.5) at online = 0.
- **Points (radius 6):** `[offline, online]` pairs `[12, 0.2], [8, 3], [5, 1], [15, 4], [3, 0.5], [10, -1], [6, 2], [20, 6], [4, 1.5], [9, 0.8]`; blue `#2a78d6`, except points below zero online (the [10, −1]) in red `#e74c3c`.
- **Highlight:** orange `#d95926` 2.5px ring (radius 10) around the running example at (12, 0.2), with bold 12px orange labels: "our recommender:" / "+12% offline, +0.2% live".
- **Right-side annotation:** bold 13px red `#e74c3c`, three lines: "every point sits below the" / "diagonal — online gains" / "come in smaller, or not at all"; then 12px `#2c3e50`: "offline picks the candidates;" / "the A/B test makes the call"; then italic 11px muted: "illustrative experiments — the" / "shape, not measured data".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials style, per `tutorials/CLAUDE.md`). Structure: `<h1>` (no index number), `.subtitle` paragraph, then 4 `.card-section` divs each containing `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Text column structure per section:** `.tags` row of colored pills (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22), then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold in `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem, with `<strong>` lead).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.card-section h2` 1.3rem `#1a5276` with 2px `#2980b9` bottom border; table cells padding 12px, vertical-align top; canvases `width:100%`, 1px solid `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Hardcoded literal data arrays, no `Math.random()`; invented numbers carry an "illustrative" label in-chart.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; red reserved for error/alarm annotations.
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
