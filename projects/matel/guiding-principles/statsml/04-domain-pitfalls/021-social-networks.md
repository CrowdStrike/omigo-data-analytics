# social network - Platform-Specific Data Pitfalls

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + one-row table per pitfall)
**HTML title tag:** social network - Platform-Specific Data Pitfalls

**Subtitle:** Selection bias, measurement error, and attribution traps baked into social network engagement data.

## Demographic Skew Shifts Over Time

**The "Average User" Aged From 28 (2015) to ~40 (2024)**

- **The migration:** Young users (13-24) moved off to short-video and photo-sharing platforms.
- **The other end:** Meanwhile the 45+ cohorts grew substantially, reshaping the whole user mix.
- **Stale calibration:** Models trained on 2015 demographic distributions predict 2024 data with bias.
- **Wrong direction:** Click behavior calibrated on younger users mispredicts for older ones.
- **Geographic compounding:** Growth in the Global South while US/EU stagnates adds a second axis of skew.
- **Impact:** A longitudinal study spanning 2015-2024 conflates composition change with behavior change.

### Visualization (canvas `canvas1`, 500×340)

Multi-series line chart of user age-group shares over time, with dots at each point.

- **Title (bold 17px, `#1a5276`, centered):** "social network User Age Distribution Shift".
- **X-axis:** years `["2015", "2017", "2019", "2021", "2023", "2024"]`; y-axis 0–40% with ticks every 10% and light gridlines `#eee`; solid `#666` axis lines; padding top 50 / right 30 / bottom 60 / left 55.
- **Series (2.5px lines with 3px dots):**
  - "13-24" red `#e74c3c`: `[38, 33, 27, 22, 18, 16]`
  - "25-34" orange `#e67e22`: `[28, 27, 26, 24, 22, 21]`
  - "35-44" yellow `#f1c40f`: `[16, 18, 20, 21, 22, 22]`
  - "45-54" green `#27ae60`: `[10, 12, 14, 17, 19, 20]`
  - "55+" blue `#2980b9`: `[8, 10, 13, 16, 19, 21]`
- **Legend:** horizontal row of color swatches with group labels (`#333`) below the x-axis.

## Algorithmic Content Filtering as Selection Bias

**The Observed Population Is Already Filtered by an Unobserved Selection Function**

- **Censored window:** Users only ever interact with content the algorithm already selected for them.
- **Conflated cause:** So high engagement blends content quality with algorithmic affinity, inseparably.
- **SUTVA violation:** A/B tests on group content show inflated effect sizes, treatment leaking into control.
- **Shared membership:** The leak path is the group itself — both arms read the same posts in it.
- **Spillover:** Network effects make individual-level randomization invalid via shared content.
- **Impact:** Models trained on algorithmically-filtered data cannot generalize to the unfiltered population.

### Visualization (canvas `canvas2`, 500×340)

Nested-circles selection diagram (three concentric-ish circles from all content down to observed content).

- **Title (bold 17px, `#1a5276`, centered):** "Algorithmic Selection Funnel".
- **Circles (centered near w/2, h/2+10):**
  - Large: radius 120, gray `#95a5a6` (fill at 0.12 alpha, 2px stroke), label "All content (N=1500)".
  - Medium: radius 75, offset (+15, −10), blue `#2980b9` (fill 0.15 alpha, 2px stroke), label "Algo-filtered (N=300)".
  - Small: radius 38, offset (+25, −15), red `#e74c3c` (fill 0.2 alpha, 2.5px stroke), bold label inside: "Observed" / "(N=50)".
- **Annotation:** dashed (4/3) red pointer line from the small circle toward the upper right, with italic red text "Models trained" / "only on this".
- **Bottom annotation (italic, `#666`, centered):** "Selection bias: 97% of content invisible to measurement".

## Share ≠ Endorsement

**Recommending and Criticizing Are Counted as the Same Signal**

- **Conflated intent:** Recommending and criticizing land in one counter, with no field separating them.
- **Both sound alike:** "Look at this great article!" and "Can you believe this nonsense?" count the same.
- **Debunk effect:** Misinformation gets high share counts precisely because people share it to debunk it.
- **Missing signal:** Sentiment of the accompanying text is needed but rarely feeds engagement metrics.
- **Impact:** Quality models overweight shares, inflating polarizing content scores by 30-40%.

### Visualization (canvas `canvas3`, 500×340)

100%-stacked bar chart of share motivations by content type.

- **Title (bold 17px, `#1a5276`, centered):** "Share Motivations by Content Type".
- **Categories (x-axis, two-line labels):** `["News\nArticles", "Political\nPosts", "Product\nReviews", "Memes", "Personal\nUpdates"]`.
- **Stack segments (bottom to top, white bold % labels inside segments taller than 18px, 0.5px white segment borders):**
  - Recommend — green `#27ae60`: `[25, 15, 55, 60, 70]`
  - Criticize — red `#e74c3c`: `[40, 55, 10, 15, 5]`
  - Inform — blue `#3498db`: `[20, 15, 25, 10, 15]`
  - Other — gray `#95a5a6`: `[15, 15, 10, 15, 10]`
- **Axes:** y 0–100% with ticks every 25% and light gridlines `#eee`; bars 60% of slot width; padding top 55 / right 30 / bottom 70 / left 70.
- **Legend (bottom row):** swatches labeled "Recommend", "Criticize", "Inform", "Other" in `#333`.

## News Feed Ranking Creates Observable Reality

**Of ~1,500 Potential Stories per Session, Only ~300 Are Shown — 80% Are Invisible**

- **What you can't measure:** Engagement only reflects responses to shown content, never latent demand.
- **Censored history:** Past ranking decisions filtered the logs; unshown items leave no trace to learn from.
- **Position bias:** Top-ranked items collect disproportionate engagement regardless of their quality.
- **Feedback loop:** A model trained on engagement inherits the biases of the ranker that generated it.
- **Impact:** The ranker's mistakes get relabeled as user preference and then trained in, round after round.

### Visualization (canvas `canvas4`, 500×340)

Six-stage funnel diagram (trapezoids, narrowing 14% per stage, 80% max width, 0.8 alpha fills, white bold labels inside).

- **Title (bold 17px, `#1a5276`, centered):** "News Feed Content Funnel".
- **Stages (label + count):**
  1. "Total Available Posts (~1,500)" — dark slate `#2c3e50` (100%)
  2. "Pass Relevance Filter (~800)" — blue `#2980b9` (53%)
  3. "Pass Quality Score (~500)" — blue `#3498db` (33%)
  4. "Ranked in Top 300 (~300)" — light blue `#5dade2` (20%)
  5. "Actually Viewed by User (~50)" — green `#27ae60` (3.3%)
  6. "Engaged With (Measured) (~12)" — red `#e74c3c` (0.8%)
- **Annotation:** dashed (4/3) red vertical arrow at the right side spanning stages 2–5, with rotated italic red label "Invisible to metrics".
- **Bottom annotation (italic, `#666`, centered):** "Only 0.8% of available content generates measurable signal".

## Cross-Platform Attribution Impossible

**Every Platform Claims Credit for the Same Conversion**

- **The journey:** User sees a social network ad, searches the product, then reads forum reviews.
- **The purchase:** The buy lands on an e-commerce platform, yet the social network claims view-through credit.
- **Window inflation:** 28-day attribution windows record coincidental exposure as if it were causal.
- **Tracking blackout:** iOS App Tracking Transparency killed cross-app tracking for ~75% of iOS users after 2021.
- **Modeled instead:** Probabilistic conversions now stand in where actual measurement used to exist.
- **Impact:** Reported ROAS runs 2-3x above incrementality-tested ground truth, systematically.

### Visualization (canvas `canvas5`, 500×340)

Top: four-node user-journey flow; bottom: attribution-claims list with mini percentage bars.

- **Title (bold 17px, `#1a5276`, centered):** "Cross-Platform User Journey".
- **Journey nodes (circles radius 24, white bold initials inside, label below in `#333`, connected by gray `#666` 2px arrows):**
  - "social network Ad" — `#3b5998`, icon "FB", x=70
  - "search engine" — `#4285f4`, icon "G", x=200
  - "forum platform Reviews" — `#ff4500`, icon "R", x=330
  - "e-commerce platform Buy" — `#ff9900`, icon "A", x=450 (all at y=95)
- **Claims section (bold `#333` heading):** "Each platform claims credit:" followed by four rows, each with a colored swatch, bold platform name, claim text, and a mini bar (75px, `#eee` background, 0.7-alpha fill in the platform color) with a bold percentage label:
  - social network: "View-through conversion" (saw ad 7 days ago) — 100%
  - search engine: "Last-click attribution" (searched product) — 100%
  - forum platform: "Influenced purchase" (read reviews) — 50%
  - e-commerce platform: "Organic conversion" (bought on platform) — 100%
- **Bottom line (bold red `#e74c3c`, centered):** "Total claimed: 350% of one conversion".

## Memory/Nostalgia "On This Day" Bias

**Selective Resurfacing Rewrites the Historical Record Upward**

- **Self-selection:** Users preferentially reshare positive memories; neutral ones are simply ignored.
- **Actively buried:** Negative memories get skipped or hidden outright, so they never re-enter the record.
- **Algorithmic survivorship:** The platform filters painful memories (breakups, deaths) from the archive.
- **Retroactive inflation:** Reshared memories earn new engagement, raising old content metrics after the fact.
- **Impact:** Platform happiness metrics run 15-25% high on recycled positive content counted repeatedly.

### Visualization (canvas `canvas6`, 500×340)

Grouped bar chart comparing sentiment distribution of actual memories vs reshared memories.

- **Title (bold 17px, `#1a5276`, centered):** "\"On This Day\" Reshare Sentiment vs Reality".
- **Categories (x-axis, may be two lines):** `["Very\nPositive", "Positive", "Neutral", "Negative", "Very\nNegative"]`.
- **Data:** actual distribution `[12, 25, 38, 18, 7]` in gray `#95a5a6`; reshared distribution `[35, 40, 18, 5, 2]` in green `#27ae60`. Bold value labels above bars: gray `#666` for actual, dark green `#1a8c4e` for reshared.
- **Axes:** y 0–40% with ticks every 10% and light gridlines `#eee`; solid `#666` axes; padding top 55 / right 30 / bottom 55 / left 60; paired bars each 33% of group width with 4px gap.
- **Legend (bottom):** gray swatch "Actual Memories", green swatch "Reshared via \"On This Day\"" (text `#333`).

## Regeneration instructions

- **Layout:** domains detail-page template: h1, `.subtitle`, then per pitfall an unnumbered `<h2>` followed by a one-row `.obj-table` — left `<td>` (40%) with `.obj-title` and a `<ul>` of one-sentence labeled bullets, right `<td>` (60%, centered) with a single `<canvas width="500" height="340">`. Even table rows have background `#fafcfe`. The "Share ≠ Endorsement" h2 uses the `&ne;` entity. No nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6. h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border, padding-bottom 8px, margin 40px 0 15px. `.subtitle` `#666` 1.05em. ul 0.9em `#333`, li margin 4px 0; `strong` `#1a5276`. `.philosophy` style defined (background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em) but no callout appears on this page. `.obj-table` full width, collapsed borders, cells `1px solid #e0e0e0` padding 20px 24px, vertical-align middle. `.obj-title` 1.05em, weight 600, `#1a5276`. Canvas `display: block; margin: 0 auto`.
- **Canvas scaling:** shared `setupCanvas(id)` helper using `window.devicePixelRatio` — sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates. Charts are named functions (`drawDemographicChart`, `drawEchoChambers`, `drawShareMotivations`, `drawNewsFeedFunnel`, `drawAttributionFlow`, `drawMemoryBias`) invoked on window `load` and re-invoked on `resize`. Shared constants `CHART_FONT_SIZE = 17`, `HEADER_COLOR = '#1a5276'`, `BORDER_COLOR = '#2980b9'`.
- **Palette:** primary blue `#1a5276`, secondary blues `#2980b9`/`#3498db`/`#5dade2`, green `#27ae60` (dark green `#1a8c4e` for labels), red `#e74c3c`, orange `#e67e22`, yellow `#f1c40f`, grays `#2c3e50`/`#666`/`#95a5a6`; brand-node colors `#3b5998`, `#4285f4`, `#ff4500`, `#ff9900`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
