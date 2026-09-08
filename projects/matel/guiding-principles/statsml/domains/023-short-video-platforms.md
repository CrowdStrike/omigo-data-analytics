# short-video platform - Platform-Specific Data Pitfalls

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + one-row table per pitfall)
**HTML title tag:** short-video platform - Platform-Specific Data Pitfalls

**Subtitle:** Cold-start gates, misaligned reward signals, and 24-hour trend cycles that break standard ML assumptions.

## Extreme Cold-Start on For You Page

**A Single Noisy Signal From 300 Users Gates All Downstream Distribution**

- **The gate:** Every new video is first shown to ~300 random users and nobody else at all.
- **The signal:** That tiny cohort's 5-second watch rate gates the 1K, 10K and 100K+ impression tiers.
- **Noise kills:** A disinterested cohort — wrong time zone, wrong demographic slice — buries a viral video.
- **Data problem:** Massive survivorship bias, since engagement is observed only past the gate.
- **No counterfactual:** What a different initial cohort would have done is never observed at all.

### Visualization (canvas `canvas-coldstart`, CSS-sized 100%×280px)

Five-stage distribution funnel (trapezoids, height 40, 6px gaps, fills at `cc` alpha) with a dashed gate line after the first stage.

- **Title (bold 17px, `#1a5276`, centered):** "FYP Cold-Start Distribution Funnel".
- **Stages (white bold 13px label inside; count in the stage color to the right of each trapezoid):**
  1. "300 Random Users (Initial Cohort)" — width 85%, blue `#3498db`, count "300"
  2. "5s Watch Rate Gate (>40%)" — width 60%, orange `#e67e22`, count "~120 watched"
  3. "Tier 1: 1K-5K Views" — width 45%, green `#27ae60`, count "1,000-5,000"
  4. "Tier 2: 10K-100K Views" — width 30%, purple `#8e44ad`, count "10,000-100,000"
  5. "Tier 3: 100K+ (Viral)" — width 15%, dark red `#c0392b`, count "100,000+"
- **Gate annotation:** dashed (4/3) red `#e74c3c` 2px horizontal line between stages 1 and 2, with bold red 12px text at left: "GATE: Single noisy signal" / "decides everything".

## Watch-Time ≠ Enjoyment

**"Car Crash" Videos: 100% Completion Rate, 0% Satisfaction**

- **The mechanism:** Users watch to the end from morbid curiosity or inability to look away, not desire.
- **Misaligned reward:** The reward signal (completion) diverges from the real objective (satisfaction).
- **What gets promoted:** Models trained on completion rate promote addictive-but-regrettable content.
- **The evidence:** Post-session surveys correlate negatively with high-completion content.
- **Net effect:** Optimizing watch-time yields a feed users finish and then regret finishing.

### Visualization (canvas `canvas-watchtime`, CSS-sized 100%×280px)

Scatter plot of completion rate (x) vs user satisfaction (y) with a highlighted "car crash" quadrant.

- **Title (bold 17px, `#1a5276`, centered):** "Completion Rate vs. User Satisfaction".
- **Axes:** x "Completion Rate (%)", y "User Satisfaction" (rotated label); both 0–100% with ticks every 25%; `#555` axis lines (1.5px) and labels; margins top 40 / right 30 / bottom 50 / left 60.
- **Normal content (40 points, blue `#3498db` at `aa` alpha, radius 4):** seeded pseudo-random (LCG seed 42, `s*1664525 + 1013904223`): completion = 20 + rand·60, satisfaction = completion·0.8 + (rand−0.5)·30, clamped to [5, 95] — positive correlation cloud.
- **Car-crash content (12 points, solid red `#e74c3c`, radius 5):** completion = 80 + rand·20, satisfaction = 5 + rand·20 — high completion, low satisfaction cluster.
- **Zone highlight:** dashed (5/3) red 2px rectangle over the bottom-right quadrant (x 75–100%, y 0–25%); bold red label above: "\"Car Crash\" Zone"; 11px caption below: "High completion, low satisfaction".
- **Legend (top left):** blue dot "Normal content", red dot "Addictive/regrettable" (text `#555`).

## Trend Half-Life: 24-48 Hours

**A Sound Goes Viral Monday, Saturates Tuesday, Is Dead Wednesday**

- **Compressed lifecycle:** Trends live 24-48 hours, so a sound saturates before any retrain lands.
- **Learning lag:** By the time a batch-trained model learns a sound is trending, the trend is over.
- **Broken splits:** Traditional train/test splits are meaningless when distributions shift every 24 hours.
- **Degradation:** Train Monday, evaluate Thursday, and trend-related features lose catastrophic accuracy.
- **Not optional:** Real-time feature freshness is existential on trend signals, not a nice-to-have.

### Visualization (canvas `canvas-halflife`, CSS-sized 100%×280px)

Exponential decay curve of trend engagement over a week, with half-life marker, model-training dot, and a shaded dead zone.

- **Title (bold 17px, `#1a5276`, centered):** "short-video platform Sound/Trend Virality Decay".
- **Axes:** x "Days Since Trend Start" with day labels `["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]` (0–6 days); y "Relative Engagement" (rotated) with ticks 0%/25%/50%/75%/100% and light gridlines `#eee`; `#555` axes; margins top 42 / right 30 / bottom 55 / left 60.
- **Decay curve (red `#e74c3c`, 3px):** `v = exp(−1.2·t)` over t = 0–6 days.
- **Half-life marker:** dashed (4/3) gray `#888` guide lines from the 50% point (t = ln2/1.2 ≈ 0.58 days) down to the x-axis and left to the y-axis; bold red 12px label "Half-life: ~14 hours".
- **Model-training annotation:** purple `#8e44ad` dot (radius 6) at t=3 (Thursday) on the curve, with purple 12px text "Model trained here (Thu)" / "already obsolete".
- **Dead zone:** shaded band `rgba(231,76,60,0.08)` from day 2 to day 6, with red 11px label near the top: "Trend effectively dead".

## Sound/Music as Hidden Feature

**Same Dance, Different Sound = 100x Different Reach**

- **The viral vector:** Audio carries the network effect — not the visuals, the creator, or the caption.
- **Pipeline blind spot:** Pipelines engineer visual and text signals but treat audio as a categorical ID.
- **What that misses:** The categorical shortcut skips the platform's core distribution mechanism entirely.
- **Too sparse:** Sound_id alone cannot support virality prediction, since each id has thin history.
- **The fix:** Add sound embeddings plus trend velocity: tempo, novelty, meme-ability, current status.

### Visualization (canvas `canvas-sound`, CSS-sized 100%×280px)

Horizontal bar chart of views by sound choice for otherwise identical videos.

- **Title (bold 17px, `#1a5276`, centered):** "Same Dance, Different Sounds = Vastly Different Reach".
- **Subtitle (12px, `#666`, centered):** "Identical creator, identical choreography, identical posting time".
- **Bars (linear scale, max 2.4M; left labels right-aligned in `#333` 13px; bold value labels after each bar):**
  - "Trending Sound A" — 2,400,000 ("2.4M views"), red `#e74c3c`
  - "Trending Sound B" — 1,800,000 ("1.8M views"), orange `#e67e22`
  - "Popular Song (old)" — 85,000 ("85K views"), yellow-orange `#f39c12`
  - "Original Audio" — 24,000 ("24K views"), blue `#3498db`
  - "Dead Trend Sound" — 12,000 ("12K views"), gray `#7f8c8d`
  - "Obscure Sound" — 3,200 ("3K views"), light gray `#95a5a6`
- **Margins:** top 48 / right 20 / bottom 40 / left 140.
- **Bottom annotation (bold 14px, dark red `#c0392b`, centered):** "750x difference in reach from sound choice alone".

## Creator Burnout Cycles

**Output → Viral Hit → Pressure → Burnout → Hiatus → Comeback**

- **The cycle:** Creators follow a predictable loop, and a viral hit raises audience expectations first.
- **Then the crash:** Pressure builds on the raised expectations and a 2-8 week burnout hiatus follows.
- **Stale averages:** A creator's historical average engagement is meaningless in the burnout phase.
- **Both directions wrong:** Recency-weighted models underpredict comeback virality, overpredict burnout.
- **Hidden state:** Creator state modulates every observable feature and is recorded in none of them.
- **Fake noise:** Unmodeled, it shows up as unexplained variance from a deterministic periodic process.

### Visualization (canvas `canvas-burnout`, CSS-sized 100%×280px)

Cyclical engagement curve over 24 weeks with shaded burnout zones and phase labels.

- **Title (bold 17px, `#1a5276`, centered):** "Creator Output & Engagement Cycle".
- **Axes:** x "Time (weeks)" with labels W0, W4, W8, W12, W16, W20, W24; `#555` axes (1.5px); margins top 45 / right 30 / bottom 50 / left 55.
- **Curve (blue `#3498db`, 2.5px), composed function over t = 0–24 weeks:** baseline 0.5 + ramped sine `sin(0.8t)·0.3·min(t/3,1)`, plus Gaussian bumps: viral spikes +0.3 at t≈4 (width 0.5) and +0.25 at t≈13 (width 0.5); burnout dips −0.4 at t≈7.5 (width 1.5) and −0.5 at t≈18 (width 2); comeback bump +0.2 at t≈21.5 (width 1).
- **Burnout zones:** shaded `rgba(231,76,60,0.12)` bands over weeks 6–9 and 16–20, each with bold red 11px "BURNOUT" label near the bottom.
- **Phase labels (bold 11px, near the top):** green `#27ae60` "VIRAL" at weeks 4 and 13; purple `#8e44ad` "COMEBACK" at week 21.5.
- **Legend (below axis):** blue line segment "Output/Engagement"; `rgba(231,76,60,0.3)` swatch "Burnout phase" (text `#555`).

## Duet/Stitch Context Dependency

**A Reaction Video Without Its Stimulus Is Literally Meaningless**

- **The content IS the juxtaposition:** Duet and Stitch videos only make sense with their source video.
- **Garbage features:** Sentiment analysis of the response alone is simply wrong, not merely imprecise.
- **Random prediction:** Engagement prediction without the original context is no better than a coin flip.
- **The scale:** ~15-20% of platform content is contextually dependent on another video this way.
- **Pipeline cost:** Skip the response-to-original join and a significant fraction trains on partial data.

### Visualization (canvas `canvas-duet`, CSS-sized 100%×280px)

Side-by-side comparison diagram: response video alone (meaningless) vs paired with original (meaningful), split by a dashed divider.

- **Title (bold 17px, `#1a5276`, centered):** "Duet/Stitch: Context Dependency Problem".
- **Left side (at 25% width):** box 110×80 with red `#e74c3c` 2px border and `#fdf2f2` fill, labeled "Response Video" / "(alone)" in `#555` with bold red 24px "???" inside. Below in red 12px: "Sentiment: undefined", "Topic: unknown", "Quality: unmeasurable"; then bold red 28px "✗" and bold 13px "MEANINGLESS".
- **Right side (at 75% width):** two boxes 80×65 with green `#27ae60` 2px borders and `#eaf7ea` fill, labeled "Original / Video" and "Response / Video" in `#555` 11px, joined by a bold green "+" and a green chevron link below. Below in green 12px: "Sentiment: sarcastic reply", "Topic: relationship humor", "Quality: high engagement"; then bold green 28px "✓" and bold 13px "MEANINGFUL".
- **Divider:** vertical dashed (4/4) gray `#bbb` 1px line down the middle.
- **Bottom annotation (bold 12px, `#1a5276`, centered):** "~15-20% of short-video platform content requires context join".

## Regeneration instructions

- **Layout:** domains detail-page template: h1, `.subtitle`, then per pitfall an unnumbered `<h2>` followed by a one-row `.obj-table` — left `<td>` (40%) with `.obj-title` and a `<ul>` of one-sentence labeled bullets, right `<td>` (60%, centered) with a single `<canvas>` (no width/height attributes; CSS-sized). Even table rows have background `#fafcfe`. HTML entities used: `&#8800;` in the "Watch-Time ≠ Enjoyment" h2, `&rarr;` in the burnout obj-title. No nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6. h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border, padding-bottom 8px, margin 40px 0 15px. `.subtitle` `#666` 1.05em. ul 0.9em `#333`, li margin 4px 0; `strong` `#1a5276`. `.philosophy` style defined but unused on this page. `.obj-table` full width, collapsed borders, cells `1px solid #e0e0e0` padding 20px 24px, vertical-align middle. `.obj-title` 1.05em, weight 600, `#1a5276`. Canvas CSS: `display: block; margin: 0 auto; width: 100%; height: 280px`.
- **Canvas scaling:** shared `setupCanvas(id)` helper reads `getBoundingClientRect()` for logical size, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and `ctx.scale` back to logical coordinates; default font 17px system stack. Charts are named functions (`drawColdStart`, `drawWatchTime`, `drawHalfLife`, `drawSound`, `drawBurnout`, `drawDuet`) invoked on window `load` and re-invoked on `resize`.
- **Palette:** primary blue `#1a5276`, blues `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, dark red `#c0392b`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, grays `#555`/`#666`/`#7f8c8d`/`#95a5a6`; tint fills `#fdf2f2` (red) and `#eaf7ea` (green).
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
