# Streaming / Media Domain Pitfalls

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one table per h2 section; each text cell is an `.obj-title` punchline followed by a `<ul>` of labeled one-line bullets)
**HTML title tag:** Streaming / Media Domain Pitfalls

**Subtitle:** Critical failure modes in recommendation systems, content metrics, and user behavior modeling for streaming platforms

## Content Licensing Changes Catalog Overnight

**50M Viewing Hours Vanish the Night One License Expires**

- **The trigger:** A licensing deal lapses and an anchor show leaves the catalog overnight.
- **The cliff:** Hours on that title fall from 50M to 2M in one day, then toward 1M.
- **Stale association:** The model still fires "users who watched it also watched X" for a gone title.
- **Not an edge case:** Deals expire, titles rotate between platforms, regional rights shift constantly.
- **Wrong mental model:** The catalog is a living, volatile entity, not a static reference table.
- **What to change:** Make availability a first-class feature; decay signals from removed content.
- **Fallbacks:** Never recommend unavailable items; keep backup anchors when content disappears.

### Visualization (canvas `canvas1`, 720×200 drawn; HTML attribute 720×300)

Bar chart: viewing hours collapse at license expiry.

- **Title (bold 17px `#1a5276`, centered):** "Viewing Hours After License Expiry".
- **Bars:** 7 bars, width 65, starting x=80 with 90px spacing, baseline y=175, max height 120 (scaled to 52).
- **Data (day, hours in millions):** Day -3: 52; Day -2: 51; Day -1: 50; Expiry: 50; Day +1: 2; Day +2: 1.5; Day +3: 1. Bars from "Expiry" onward are red `#e74c3c`; earlier bars green `#27ae60`.
- **Labels:** day names 13px `#2c3e50` under bars; "NM hrs" value labels (12px) above bars (e.g. "52M hrs", "2M hrs").
- **Annotation:** dashed (4/3) red 2px arrow from the top of the Day -1 bar down to the top of the Day +1 bar; bold 13px red text "License expires!" at y=48 above the Expiry bar.

## Regional Availability Creates Parallel Universes

**One Platform, 8,400 US Titles vs 5,200 UK Titles, ~3,800 Shared**

- **The split:** A title available in the US may simply not exist in the UK catalog.
- **Travel effect:** A user crossing US→UK loses access to 4,600 titles overnight.
- **Broken comparison:** Cross-region collaborative filtering pairs users who cannot watch the same thing.
- **Impossible output:** "Users like you also watched Y" fails when Y is not licensed in your region.
- **The real bug:** Global models silently treat catalog access as if it were universal.
- **What to change:** Region-aware embeddings; filter recommendations by availability after scoring.
- **Keep signals clean:** Separate preference from access; handle travel and VPN without corrupting it.

### Visualization (canvas `canvas2`, 720×200 drawn; HTML attribute 720×300)

Venn-style diagram: US vs UK catalogs with shared overlap.

- **Title (bold 17px `#1a5276`, centered):** "Same Platform, Different Catalogs by Region".
- **US circle:** center (220, 115), radius 65, fill `rgba(41,128,185,0.15)`, stroke `#2980b9` 2px; bold 14px blue labels "US Catalog" / "8,400 titles" (offset left of center).
- **UK circle:** center (500, 115), radius 65, fill `rgba(231,76,60,0.15)`, stroke `#e74c3c` 2px; bold 14px red labels "UK Catalog" / "5,200 titles" (offset right of center).
- **Overlap:** purple circle at (270, 115), radius 32.5, fill `rgba(142,68,173,0.2)`; 12px `#8e44ad` labels "Shared" / "~3,800".
- **Bottom line (13px, left-aligned at (30, 185)):** "User travels US→UK:" in `#2c3e50`, followed by "loses access to 4,600 titles" in red `#e74c3c`.

## Collaborative Filtering Cold-Start

**Launch-Day Audience Guess Becomes the Show's Permanent Verdict**

- **Zero signal:** A new show launches with no watch history, so targeting starts from nothing.
- **The fork:** Right initial audience → good metrics → promoted → hit; wrong one → buried.
- **Self-fulfilling:** The algorithm's guess about "who would like this" becomes ground truth.
- **The death spiral:** Wrong guess → poor early ratings → deprioritized → the show effectively dies.
- **Never corrected:** The system does not learn it was wrong, because it never retries.
- **What to change:** Give new content an exploration budget instead of one shot.
- **Cold-start signals:** Target on metadata — genre, cast, director — not on absent watch history.
- **Measure separately:** Controlled exposure to diverse segments; split discovery from satisfaction metrics.

### Visualization (canvas `canvas3`, 720×200 drawn; HTML attribute 720×300)

Forking-paths diagram: launch-day audience choice determines a show's fate.

- **Title (bold 17px `#1a5276`, centered):** "Cold-Start: Initial Audience Determines Fate".
- **Start node:** dark blue `#1a5276` 8px dot at (60, 85) with white bold 10px "NEW" inside; below it bold 12px `#2c3e50` "Launch" / "Day".
- **Good path:** green `#27ae60` 3px bezier curve from (60, 70) rising gently to (660, 50); label 13px green at (120, 45): "Right initial audience → good metrics → promoted → success"; end label bold 13px green right-aligned at (700, 50): "Hit Show".
- **Bad path:** red `#e74c3c` 3px bezier curve from (60, 100) falling to (660, 170); label 13px red at (120, 185): "Wrong initial audience → poor metrics → buried → \"failure\""; end label bold 13px red at (700, 170): "Cancelled".

## Binge vs Weekly Release Behavior

**Same Content, Two Release Formats, Opposite Verdicts From the Same Data**

- **Binge shape:** 8 episodes in one session — a huge Week 1 spike, then near-zero by Week 2.
- **Why it collapses:** Churn follows immediately because there is nothing left to watch.
- **Weekly shape:** Lower per-session engagement, but 8 weeks of sustained, rising retention.
- **Metric flips:** "Time spent per session" crowns binge; "monthly active days" crowns weekly.
- **Wrong cause:** Release strategy, not content quality, is what moves the metric.
- **Broken tests:** A/B tests ignoring release format compare things that are not comparable.
- **What to change:** Normalize engagement by release format before any comparison.
- **Use curves:** Retention curves over point metrics; content lifetime value over session engagement.
- **Comparison rule:** Binge-to-binge and weekly-to-weekly only, never across formats.

### Visualization (canvas `canvas4`, 720×200 drawn; HTML attribute 720×300)

Dual line chart: binge spike-and-churn vs weekly steady retention over 8 weeks.

- **Title (bold 17px `#1a5276`, centered):** "Binge vs Weekly: Same Content, Different Metrics".
- **Axes:** x from 80 to 670 across 8 evenly spaced points labeled Wk1…Wk8 (12px `#2c3e50`); y baseline 170, top 50; light vertical gridlines `#ecf0f1` at each week.
- **Binge line (red `#e74c3c`, 3px):** `[95, 10, 5, 3, 2, 2, 1, 1]` (% of max, mapped to plot height).
- **Weekly line (green `#27ae60`, 3px):** `[25, 28, 30, 27, 32, 29, 35, 40]`.
- **Legend (bold 13px, swatch rectangles at x=450):** red — "Binge (high spike, fast churn)"; green — "Weekly (steady retention)".

## Completion ≠ Enjoyment

**Watching While Doing Dishes Scores Higher Than Being Moved to Stop**

- **False positive:** A show left playing during chores counts as "completed" and high engagement.
- **False negative:** Three intense episodes then an overwhelmed stop is logged as a dropout failure.
- **Bad proxy:** Completion measures nothing reliable about quality or user satisfaction.
- **Who gains:** Background viewing inflates completion for mediocre, easy-to-ignore content.
- **Who loses:** Content that makes users pause, reflect, or take breaks gets penalized.
- **The perverse target:** The system optimizes for "easy to leave on," not "deeply engaging."
- **What to change:** Pair completion with pause, rewind, volume, and device-interaction signals.
- **Weight intent:** Score "chosen to watch" above "left playing"; separate foreground from background.
- **Calibrate:** Use survey-based satisfaction to anchor what the behavioral signals actually mean.

### Visualization (canvas `canvas5`, 720×200 drawn; HTML attribute 720×300)

Quadrant scatter: completion rate (x) vs enjoyment (y) with a misleading quadrant highlighted.

- **Title (bold 17px `#1a5276`, centered):** "Completion Rate vs Actual Enjoyment".
- **Axes:** crosshair centered at (360, 115), half-width 260, half-height 65, dark `#2c3e50` 2px; x label "Completion Rate →" bottom right; rotated y label "Enjoyment →" at far left.
- **Points (8px dots, 11px multi-line labels below in the dot's color; coordinates on −100…100 scales):**
  - (80, −40) "Background / viewing" `#e74c3c`
  - (−60, 45) "Intense / 3-ep stop" `#27ae60`
  - (70, 50) "True / favorite" `#2980b9`
  - (−70, −35) "Abandoned" `#95a5a6`
- **Danger zone:** bottom-right quadrant shaded `rgba(231,76,60,0.08)` with right-aligned 10px red caption inside: "MISLEADING: high completion, low enjoyment".

## Soundtrack/Music Context ≠ Preference

**Workout Tracks Top the Stream Counts and Bottom the Preference Scores**

- **The setup:** A workout playlist drives high stream counts for energetic songs.
- **The misread:** Those plays measure liking the workout, not liking the songs.
- **Chart contrast:** Workout scores 85 streams against 20 preference; relaxing scores 8 against 70.
- **Hidden variable:** Context — gym, commute, focus, party — dominates actual musical taste.
- **Same song, new intent:** 100 workout plays make a functional tool, not a favorite.
- **Where it breaks:** Recommending that style for a relaxed evening session fails outright.
- **Consequence:** Raw play counts are meaningless as a preference signal on their own.
- **What to change:** Model context explicitly — time, activity, playlist type — as its own feature.
- **Split the modes:** Keep functional listening apart from intentional discovery in the data.
- **Weight by action:** Searching and saving count as preference; passive streaming does not.
- **Reinterpret co-occurrence:** Playlist co-occurrence is a context signal, not a preference signal.

### Visualization (canvas `canvas6`, 720×200 drawn; HTML attribute 720×300)

Paired bar chart: stream count vs actual preference across five listening contexts.

- **Title (bold 17px `#1a5276`, centered):** "Listening Context Dominates \"Preference\"".
- **Groups:** five contexts at x starting 100 with 120px spacing, baseline y=175, max height 110 (scaled to 85); per group two 22px-wide bars — left = stream count in `rgba(41,128,185,0.7)`, right = preference in `rgba(39,174,96,0.7)`.
- **Data (context: streams, preference):** Workout: 85, 20; Commute: 45, 30; Relaxing: 8, 70; Focus: 5, 15; Party: 60, 40.
- **Context labels:** 11px `#2c3e50` below each group.
- **Legend (top left, 14px swatches):** blue — "Stream count"; green — "Actual preference".
- **Annotation (italic 12px `#e74c3c`, right-aligned near top right):** "High streams ≠ high preference (context drives plays)".

## Regeneration instructions

- **Layout:** standard detail-page structure — h1, `.subtitle` paragraph, then per pitfall an `<h2>` section heading followed by a one-row `.obj-table`: left `<td>` (40%) with an `.obj-title` div holding a one-line punchline (not a repeat of the h2 text) plus a `<ul>` of 7-10 `<li>` bullets, each `<strong>Label:</strong> short phrase` fitting on one line; right `<td>` (60%, centered) with a `<canvas>` (HTML attributes `width="720" height="300"`).
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; `strong` `#1a5276`; `ul` margin `8px 0 8px 20px`, 0.9em, `#333`; `li` margin `4px 0`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, even rows background `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout class defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused on this page. No nav bar, no back/home links.
- **Canvas:** shared `setupCanvas(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); constants `CHART_FONT = '17px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'`, `HEADER_COLOR = #1a5276`, `ACCENT_COLOR = #2980b9`. Note the drawn size (720×200) overrides the 720×300 HTML attribute.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, purple `#8e44ad`, dark slate `#2c3e50`, gray `#95a5a6`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
