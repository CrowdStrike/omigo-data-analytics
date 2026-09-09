# photo-sharing platform - Platform-Specific Data Pitfalls

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + one-row table per pitfall; left cell holds an `.obj-title` punchline followed by a `<ul>` of labeled one-line bullets)
**HTML title tag:** photo-sharing platform - Platform-Specific Data Pitfalls

**Subtitle:** Critical failure modes when building ML models on photo-sharing platform behavioral data

## Visual-Only Signal Limitation

**Image Embeddings Predict Commercial Intent at ~52% — Barely Above Random**

- **The ambiguity:** One beach sunset can be a vacation memory, sponsored resort post, art piece, or listing.
- **Pixels aren't intent:** The same pixel matrix maps to completely different behavioral signals.
- **Why it fails:** Visual-only classifiers conflate intent categories with no text or context to separate them.
- **The accuracy floor:** ~52% on a 4-class problem is barely better than guessing at random.
- **Downstream cost:** Brand safety filters run 30-40% false positives, tagging organic content commercial.

### Visualization (canvas `canvas1`, CSS-sized 100%×280px)

Radial diagram: one central image with four dashed arrows pointing outward to intent labels.

- **Title (bold 17px, `#1a5276`, centered):** "One Image, Four Intents".
- **Central image placeholder (80×60 at canvas center):** stylized beach sunset — orange sky `#ff9f43` rectangle, blue sea `#2e86de` lower strip, yellow sun `#feca57` circle (radius 12), 2px `#2c3e50` border. Captions below in `#2c3e50` 13px: "Same Image" / "(beach sunset photo)".
- **Intent arrows (dashed 4/3, 2px, filled arrowheads, radius 100 from center; labels at radius+35 in 14px boxes with `#f8f9fa` fill and 1.5px border in the intent color):**
  - "Vacation Memory" — green `#27ae60`, angle −140°
  - "Sponsored Post" — red `#e74c3c`, angle −40°
  - "Art Portfolio" — purple `#8e44ad`, angle 140°
  - "Real Estate Ad" — orange `#f39c12`, angle 40°
- **Two bold red `#e74c3c` "?" marks** flanking the top of the image (±70px from center, y = center−65).

## Explore Page Feedback Loop

**Explore Engagement Rate Measures the Algorithm, Not the User**

- **The loop:** Show type X → user engages → model infers "prefers X" → shows more X, no exit ramp.
- **Causally contaminated:** Genuine preference and algorithmic force-feeding look identical in the logs.
- **Observation bias:** You only ever see engagement on the content you chose to show.
- **Self-reinforcing:** Interest models keep narrowing because they are fed their own past output.
- **At scale:** Topic diversity drops 60% in 2 weeks of Explore-heavy use — 12 categories to 3 in 14 days.

### Visualization (canvas `canvas2`, CSS-sized 100%×280px)

Circular feedback-loop diagram with four labeled step boxes around a central "Diversity Collapse" badge.

- **Title (bold 17px, `#1a5276`, centered):** "Explore Page Narrowing Loop".
- **Loop:** blue `#2980b9` 2.5px arc segments with arrowheads around a circle of radius 80 (center at w/2, h/2+10).
- **Step labels (14px, `#1a5276` text in `#eaf2f8` boxes with 1px `#2980b9` border, placed at radius+45):**
  - Top (−90°): "Show Type X"
  - Right (0°): "User Engages"
  - Bottom (90°): "Model: \"Prefers X\""
  - Left (180°): "More X, Less Y,Z"
- **Center badge:** circle radius 30, fill `#fdebd0`, border orange `#e67e22` 2px; bold 11px orange text "Diversity" / "Collapse".
- **Bottom annotation (12px, `#7f8c8d`, centered):** "Topic diversity: 12 categories → 3 categories in 14 days".

## Influencer Fake Engagement

**80% of Fake Engagement Arrives in the First 180 Seconds**

- **The sources:** Bought followers, engagement pods (mutual like/comment groups), and comment bots.
- **Metrics polluted:** CPE and engagement rate go unreliable once 15-40% of interactions are artificial.
- **Wrong lesson learned:** "High engagement = quality" teaches the model to replicate bot patterns.
- **The tell:** Fake bursts within 3 min of posting then goes flat; organic builds as a gradual S-curve.
- **Rarely filtered:** That temporal signature is detectable but seldom removed from training data.
- **Impact:** Brand ROI models overvalue influencers 2-5x; recommenders promote pod-boosted content.

### Visualization (canvas `canvas3`, CSS-sized 100%×280px)

Two-line time-series chart of cumulative engagement rate for fake vs organic engagement.

- **Title (bold 17px, `#1a5276`, centered):** "Engagement Timeline: Real vs Fake".
- **X-axis:** time after posting, labels `["0m", "3m", "10m", "30m", "1h", "3h", "6h", "12h", "24h"]` (evenly spaced), axis title "Time after posting"; rotated y-axis label "Engagement Rate"; gray `#bdc3c7` axes, tick text `#7f8c8d`; margins left 60 / right 20 / top 45 / bottom 50.
- **Fake curve (red `#e74c3c`, 2.5px):** `[0, 0.92, 0.95, 0.96, 0.96, 0.97, 0.97, 0.97, 0.97]` — instant spike then flat.
- **Real curve (green `#27ae60`, 2.5px):** `[0, 0.05, 0.15, 0.32, 0.48, 0.62, 0.73, 0.82, 0.88]` — gradual S-curve.
- **Bot burst zone:** shaded band `rgba(231,76,60,0.08)` from x=0 to just past the "3m" tick, labeled in red 11px: "Bot burst" / "zone".
- **Legend (top right):** red line swatch "Fake (pod/bot)", green line swatch "Organic (real)" (text `#2c3e50`).
- **Annotation (red 11px):** "80% of fake engagement" / "arrives here" near the fake curve's burst.

## Story Ephemerality

**Stories Are 50%+ of Daily Engagement and 0% of Historical Training Data**

- **The mechanism:** Stories vanish after 24 hours, leaving no trace for historical user profiles.
- **Survivorship bias:** Only feed posts persist, so the engagement graph is feed-shaped by construction.
- **Feature trap:** A "last 24hr behavior" feature reads an ephemeral-heavy, non-representative slice.
- **Half the signal gone:** Profile vectors built on retrospective data miss stories entirely past 24hr.
- **On day 7:** Feed history is 100% available; stories only 14% — the last day alone.
- **Impact:** Interest models on persistent content miss 50-70% of actual daily engagement patterns.

### Visualization (canvas `canvas4`, CSS-sized 100%×280px)

Timeline diagram of data availability: persistent feed-post bar vs expiring daily story windows, plus a "what the model sees" strip.

- **Title (bold 17px, `#1a5276`, centered):** "Data Availability: Stories vs Feed Posts".
- **Timeline:** 7 days, gray `#bdc3c7` baseline with tick marks and labels "Day 0" … "Day 7" in `#7f8c8d`.
- **Feed posts row (label `#1a5276` 13px "Feed Posts (persistent)"):** solid green `#27ae60` bar spanning the full width (height 20) with white 11px centered text "ALWAYS AVAILABLE FOR ANALYSIS".
- **Stories row (label "Stories (ephemeral)"):** 7 per-day boxes (90% of day width, height 20), fill `rgba(231,76,60,0.3)` with dashed (3/2) red `#e74c3c` border; days 0–5 marked with bold red centered "GONE", the last day marked "LIVE" in red 11px.
- **"What your model sees on Day 7:" section (`#1a5276` 13px):** feed strip = full-width green bar (height 12); stories strip = gray `#bdc3c7` bar for days 0–6 with red `#e74c3c` segment for the last day only. Right-aligned labels: green "Feed: 100% history"; red "Stories: only 14% (last 24hr)".
- **Bottom note (12px, `#7f8c8d`, centered):** "Stories = 50%+ of daily engagement but 0% of historical training data".

## Aesthetic Bias in Moderation

**Engagement Labels Encode a 20-35% Visibility Gap as "Quality"**

- **The origin:** Historical feed training on biased engagement data favored lighter skin tones.
- **The promotion loop:** More visibility → more engagement → higher ranking → more visibility.
- **Proxy learned:** Quality models pick up skin-tone-correlated features as stand-ins for "quality."
- **Moderation inherits it:** Engagement-weighted training reads high engagement as "probably fine."
- **Uneven enforcement:** Lighter-skin content therefore gets systematically more lenient treatment.
- **At scale:** A 20-35% visibility gap across groups under-promotes underrepresented creators.

### Visualization (canvas `canvas5`, CSS-sized 100%×280px)

Vertical bar chart of relative feed visibility by demographic group with a gap-span annotation.

- **Title (bold 17px, `#1a5276`, centered):** "Feed Visibility by Demographic Group".
- **Data:** groups `["Group A", "Group B", "Group C", "Group D", "Group E"]` with visibility index `[100, 82, 68, 55, 45]`.
- **Bars:** 60% of slot width, rounded top corners (radius 3); fill color linearly interpolated between red `rgb(231,76,60)` (`#e74c3c`, low values) and green `rgb(39,174,96)` (`#27ae60`, high values) by value/100. Bold 13px value labels ("100%", "82%", …) above bars in `#2c3e50`; group labels below in `#5d6d7e`.
- **Axes:** y 0–100% with ticks every 25% and gridlines `#ecf0f1`; rotated y-axis title "Relative Visibility Index" in `#7f8c8d`; margins left 60 / right 20 / top 45 / bottom 55.
- **Annotation:** red `#e74c3c` 1.5px horizontal span line (with end ticks) below the axis from the first to last bar, labeled in bold red 12px: "55% visibility gap (systemic)".

## Carousel vs Single Post Behavior

**Carousel Save Rate Is 2.4x — UI Mechanics, Not Better Content**

- **Second chance:** Carousels get re-shown in feeds if the user did not swipe the first time.
- **Built-in dwell:** Multiple frames lengthen dwell time by construction, not by merit.
- **Save inflation:** Saves accumulate at 2-3x the single-post rate for structural reasons.
- **Two distributions:** Carousel curves are right-shifted and fat-tailed; singles left-peaked, fast-decaying.
- **Why it fails:** Unstratified mixing fits one model to two shapes; a singles-trained model breaks.
- **What to change:** Stratify by format — unified models carry 40% higher RMSE and confound A/B tests.

### Visualization (canvas `canvas6`, CSS-sized 100%×280px)

Two engagement-density curves (line + translucent area fill) over hours after posting.

- **Title (bold 17px, `#1a5276`, centered):** "Engagement Distribution: Carousel vs Single".
- **X-axis:** labels `["0h", "1h", "3h", "6h", "12h", "24h", "48h", "72h"]` (evenly spaced), axis title "Hours after posting"; rotated y-axis label "Engagement density"; gray `#bdc3c7` axes, text `#7f8c8d`; margins left 55 / right 20 / top 45 / bottom 50.
- **Single post curve (red `#e74c3c` 2.5px line, fill `rgba(231,76,60,0.12)`):** `[0.05, 0.85, 0.55, 0.30, 0.15, 0.08, 0.04, 0.02]` — sharp early peak, fast decay.
- **Carousel curve (blue `#2980b9` 2.5px line, fill `rgba(41,128,185,0.12)`):** `[0.03, 0.35, 0.60, 0.72, 0.58, 0.40, 0.28, 0.18]` — later peak, fat tail.
- **Legend (top right):** red line swatch "Single post", blue line swatch "Carousel (re-shown)" (text `#2c3e50`).
- **Annotations (11px):** red "Peak: 1hr" near the single-post peak; blue "Peak: 6hr (re-shown)" near the carousel peak; blue right-aligned "Fat tail: 2nd feed insertion" near the carousel tail.

## Regeneration instructions

- **Layout:** domains detail-page template variant: h1, `.subtitle`, then per pitfall an unnumbered `<h2>` followed by a one-row `.obj-table` — left `<td>` (40%) with an `.obj-title` punchline line (a sharp claim, not a repeat of the h2) and a `<ul>` of 4-6 `<li>` labeled one-line bullets (`<strong>Label:</strong> phrase`); right `<td>` (60%, centered) with a single `<canvas>` (no width/height attributes; CSS-sized). Even table rows have background `#fafcfe`. HTML entities used: `&mdash;`, `&rarr;`, `&ne;`. No nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6. h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border, padding-bottom 8px, margin 40px 0 15px. `.subtitle` `#666` 1.05em. `strong` `#1a5276` globally. Bullet lists: `ul { margin: 8px 0 8px 20px; font-size: 0.9em; color: #333; }` and `li { margin: 4px 0; }`. `.pitfall-impact` (margin-top 12px, padding 8px 12px, background `#fef2f2`, left border 3px solid `#e74c3c`, radius 0 4px 4px 0, 13px, color `#7f1d1d`) and `.philosophy` styles are defined but unused. `.obj-table` full width, collapsed borders, cells `1px solid #e0e0e0` padding 20px 24px, vertical-align middle. `.obj-title` 1.05em, weight 600, `#1a5276`. Canvas CSS: `display: block; margin: 0 auto; width: 100%; height: 280px` (no HTML width/height attributes).
- **Canvas scaling:** shared `setupCanvas(id)` helper reads `getBoundingClientRect()` for logical size, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and `ctx.scale` back to logical coordinates; default font 17px system stack. Charts are named functions (`drawCanvas1`…`drawCanvas6`) invoked on window `load` and re-invoked on `resize`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, grays `#2c3e50`/`#5d6d7e`/`#7f8c8d`/`#bdc3c7`; image-placeholder colors `#ff9f43`, `#2e86de`, `#feca57`; loop-badge fill `#fdebd0`; label-box fills `#f8f9fa`/`#eaf2f8`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
