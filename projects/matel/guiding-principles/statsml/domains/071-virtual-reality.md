# Virtual Reality / AR / Spatial Computing

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left ~40%, canvas right ~60%)
**HTML title tag:** Virtual Reality / AR / Spatial Computing - Domain Pitfalls

**Subtitle:** Spatial computing generates rich data, but nearly every VR/AR metric conflates hardware limitations — sickness dropout, comfort ceilings, tracking noise — with user behavior.

Note: canvas elements carry `width="720" height="300"` attributes, but page CSS and the init helper force the drawing surface to 720×200 (CSS `canvas { width: 720px; height: 200px }`, backing store 720×200 × dpr). Effective chart size is 720×200.

## Motion Sickness Dropout Biases All Metrics

**Motion Sickness Dropout Biases All Metrics**

- **The trap:** 40-70% of VR users get motion sickness, and the sensitive ones quit early or never return.
- **Survivor metrics:** Every engagement number then describes only the sickness-resistant survivors.
- **Real-world failure:** A VR fitness app touted 28-minute sessions and 85% weekly retention.
- **The hidden half:** Meanwhile 45% of its new users quit within 5 minutes and never came back.
- **Demographic skew:** Women get VR sickness at 2-3x the male rate, so volunteer studies carry gender bias.
- **Content contamination:** "Users prefer teleport locomotion" comes only from smooth-locomotion survivors.
- **Missing comparers:** The users who got sick and quit never lasted long enough to be in that data.

### Visualization (canvas `canvas1`, 720×200)

Funnel chart of VR user attrition (trapezoid segments narrowing left to right).

- **Title (bold 14px `#1a5276` at x=210, y=18):** "VR User Funnel: Motion Sickness Attrition".
- **Funnel area:** origin (60, 40), 600 wide, 140 tall, five equal-width trapezoid stages, each height proportional to its percentage and tapering to the next stage's height:
  - Try VR — 100% — `#3498db`
  - Complete first session — 55% — `#2980b9`
  - Return for session 2 — 38% — `#f39c12`
  - Weekly user — 22% — `#e67e22`
  - Monthly active — 15% — `#e74c3c`
- **Labels:** bold white 11px percentage centered in each stage; stage names (two lines, 10px `#666`) below the funnel.
- **Annotations (bold 11px `#e74c3c`, below stage labels):** "45% lost to motion sickness before completing session 1" and "All \"engagement metrics\" come from the surviving 15%".

## Session Length Capped by Hardware Comfort

**Session Length Capped by Hardware Comfort**

- **The trap:** Headset weight, heat, and eye strain cap session duration regardless of the content.
- **What time measures:** Time-based engagement metrics then track hardware tolerance, not user interest.
- **Real-world failure:** A VR social platform read an "engagement decline" from 42 to 31 minutes.
- **The real cause:** Summer heat, which made long headset sessions unbearable — not worse content.
- **Comparison invalidity:** "VR is less engaging" than 2-4 hour flat-screen sessions is not a fair claim.
- **Mismatched mediums:** It pits a discomfort-limited medium against an interest-limited one.

### Visualization (canvas `canvas2`, 720×200)

Area/line chart of comfort rating declining over session duration.

- **Title (bold 14px `#1a5276`):** "User Comfort Rating Over VR Session Duration".
- **Axes:** L-shape in `#ccc`, origin (60, 170), plot 620 wide × 130 tall.
- **Data (minutes → comfort fraction):** 5→0.98, 10→0.95, 15→0.88, 20→0.78, 25→0.65, 30→0.52, 35→0.40, 40→0.30, 45→0.22, 50→0.15, 55→0.10, 60→0.07.
- **Series:** line `#2980b9` width 3 with area fill `rgba(52,152,219,0.15)` down to the baseline.
- **Threshold:** horizontal dashed (5/5) red `#e74c3c` line width 1.5 at comfort = 0.5, labeled (11px red): "50% \"uncomfortable\" threshold → session exit".
- **X labels:** every other point as "<N>min" in 10px `#666`.
- **Annotation (bold 11px `#1a5276`, near the 30-minute point):** "← 30min: hardware ceiling" / "   NOT content ceiling".

## Hand Tracking Noise Creates Phantom Interactions

**Hand Tracking Noise Creates Phantom Interactions**

- **The trap:** Hand tracking registers false-positive interactions that no user ever intended.
- **Noise sources:** Stray finger movement, tracking snaps, occlusion, and IR interference all fire events.
- **Real-world failure:** A training app recorded "3.2 Help presses per session" and treated it as confusion.
- **The real number:** 78% were phantom presses from hands passing near the button while gesturing.
- **Analytics pollution:** VR interaction heat maps cluster around camera dead zones and occluded positions.
- **False insight:** So the apparent "UX insight" is really a map of hardware geometry.

### Visualization (canvas `canvas3`, 720×200)

Stacked bar chart: recorded button presses split into intentional (green, top) vs phantom (red, bottom).

- **Title (bold 14px `#1a5276`):** "Recorded Button Presses: Intentional vs Phantom (Hand Tracking)".
- **Layout:** origin (80, 170), plot 540 wide × 120 tall, scale max 22, bar width 32, five categories evenly spaced.
- **Data (category: intentional / phantom presses per session):** Help 0.7/2.5, Menu 2.1/1.4, Back 1.8/3.2, Select 8.5/4.1, Scroll actions 12.0/8.5.
- **Bars:** phantom segment at bottom in `#e74c3c`, intentional stacked above in `#27ae60`. Total value (bold 9px `#1a5276`) above each bar; phantom share printed inside the red segment as "<NN>%" / "noise" (9px `#e74c3c`); category names (10px `#666`) below.
- **Legend (top right):** green swatch "Intentional"; red swatch "Phantom (tracking noise)".

## Gaze ≠ Attention (Peripheral Vision in VR)

**Gaze ≠ Attention (Peripheral Vision in VR)**

- **The trap:** Eye tracking measures foveal gaze only, but VR's field of view spans 90-110°.
- **Unmeasured channel:** Much of the visual processing happens in peripheral vision no tracker records.
- **Real-world failure:** An ad study read "0.3 seconds of gaze" on a billboard as low engagement.
- **The contradiction:** Brand recall came in at 68% because users processed the billboard peripherally.
- **Foveated rendering confound:** Degraded peripheral resolution forces users to look directly at things.
- **Inflated dwell:** That lifts "gaze time" for hardware reasons rather than genuine interest.
- **Vergence-accommodation conflict:** Fixed lens focal distance makes VR dwell times non-comparable.
- **Broken baseline:** So real-world attention research is not a valid yardstick for VR numbers.

### Visualization (canvas `canvas4`, 720×200)

Grouped bar chart: foveal gaze time (solid blue) vs peripheral processing time (translucent dashed orange) per object type.

- **Title (bold 14px `#1a5276`):** "VR Attention: Foveal Gaze Time vs Peripheral Processing".
- **Layout:** origin (100, 170), plot 500 wide × 120 tall, scale max 5 seconds, paired bars 30px wide.
- **Data (object: gaze s / peripheral s):** Billboard ad 0.3/2.8, NPC character 1.2/3.5, UI element 2.5/1.0, Env detail 0.5/4.2, Threat (enemy) 0.8/3.1.
- **Bars:** gaze bars solid `#3498db`; peripheral bars fill `rgba(243,156,18,0.5)` with dashed (3/3) `#f39c12` outline. Value labels (bold 9px, series color) above bars; category names (9px `#666`, two lines) below.
- **Legend:** blue swatch "Foveal gaze (what analytics measure)"; translucent orange swatch "Peripheral processing (invisible to tracking)".

## Spatial Audio Position Errors Compound Over Time

**Spatial Audio Position Errors Compound Over Time**

- **The trap:** Head-tracking drift accumulates over minutes, misaligning spatial audio from the visuals.
- **False attention:** Users unconsciously turn toward the sound, and analytics log that as "attention."
- **Real-world failure:** In a VR theater study, drift made "looking at the stage" need a 5-8° head offset.
- **Inverted scoring:** Comfortable users scored "inattentive" while drift-fighters scored "engaged."
- **HRTF mismatch:** Generic HRTFs cause 10-30° localization errors across different ear geometries.
- **Confounded signal:** So "orients toward audio cues" partly measures HRTF fit, not attention.
- **Multi-user contamination:** In social VR, turn-toward-speaker loops create convergence patterns.
- **Misread as connection:** Those synchronized head turns get reported as genuine engagement.

### Visualization (canvas `canvas5`, 720×200)

Dual line chart: positional drift and audio misalignment growing over session time.

- **Title (bold 14px `#1a5276`):** "Head Tracking Positional Drift Over Session Duration".
- **Axes:** L-shape in `#ccc`, origin (60, 170), plot 620 wide × 120 tall. X labels "0m" through "60m" (10px `#666`).
- **Data (minutes 0, 5, 10, 15, 20, 25, 30, 40, 50, 60):**
  - Position drift (mm, scale max 16): 0, 0.3, 0.8, 1.5, 2.8, 4.2, 5.5, 8.1, 11.2, 15.0 — solid `#e74c3c` line width 3.
  - Audio misalignment (degrees, scale max 25): 0, 0.5, 1.2, 2.5, 4.5, 6.8, 9.0, 13.0, 18.0, 24.0 — dashed (6/4) `#f39c12` line width 3.
- **Threshold:** dotted (3/3) green `#27ae60` line at drift = 5mm, labeled (10px green): "Perceptible threshold (~5mm / ~8°)".
- **Legend:** red line "Position drift (mm)"; dashed orange line "Audio misalignment (°)".

## Room-Scale vs Seated: Incomparable Cohorts

**Room-Scale vs Seated: Incomparable Cohorts**

- **The trap:** Room-scale and seated users differ in movement, interaction, and task-time profiles.
- **Pooled anyway:** Analytics dump both into one cohort and report a single meaningless average.
- **Real-world failure:** "40% of users never explore the left wing" correlated perfectly with play-space size.
- **The mechanism:** Seated users could not get there fluently using thumbstick locomotion.
- **Accessibility confound:** Seated VR includes wheelchair users, the elderly, and small-apartment dwellers.
- **Proxy variable:** Physical play space therefore stands in for age, wealth, and physical ability.
- **Interaction mode shift:** Seated laser-pointer input is higher friction than room-scale reach-and-grab.
- **Meaningless efficiency:** Pooling the two modes makes every task-time comparison uninterpretable.

### Visualization (canvas `canvas6`, 720×200)

Grouped bar chart of task completion times: room-scale (green) vs seated (red), with ratio labels.

- **Title (bold 14px `#1a5276`):** "Task Completion Time: Room-Scale vs Seated Users".
- **Layout:** origin (80, 170), plot 540 wide × 120 tall, paired bars 32px wide; each task normalized to its own scale max.
- **Data (task: room-scale s / seated s / per-task scale max):** Grab object 2.1/3.8/5, Navigate room 4.5/12.0/15, Arrange items 8.2/18.5/20, Social gesture 1.5/4.2/5, Full tutorial 45/78/80.
- **Labels:** ratio "<x.x>x" (bold 10px `#1a5276`) above each pair; task names (9px `#666`, two lines) below.
- **Legend:** green swatch "Room-scale"; red swatch "Seated (1.4-2.7x slower)".

## Early Adopter Bias (Enthusiasts ≠ General Population)

**Early Adopter Bias (Enthusiasts ≠ General Population)**

- **The trap:** Current VR users are self-selected enthusiasts, not a sample of anyone else.
- **No resemblance:** Their behavior and friction tolerance look nothing like the mainstream you design for.
- **Real-world failure:** A standalone headset's UX was built on research from PC-tethered enthusiasts.
- **The result:** 35% of mainstream units were never set up past the initial tutorial.
- **Divergent tolerance:** Enthusiasts accept 5-minute setups; mass-market users abandon at any friction.
- **Divergent taste:** Enthusiasts want long complex experiences, while mainstream prefers short passive content.

### Visualization (canvas `canvas7`, 720×200)

Parallel-coordinates comparison of early adopters vs mainstream across five dimensions.

- **Title (bold 14px `#1a5276`):** "VR User Characteristics: Current (Early Adopter) vs Target (Mainstream)".
- **Layout:** origin (140, 170), plot 480 wide × 120 tall, five vertical axes in `#eee` evenly spaced with two-line labels (9px `#666`) below: Tech comfort, Motion tolerance, Setup patience, Session length, Content complexity.
- **Data (fraction of axis):**
  - Early adopter: 0.95, 0.82, 0.88, 0.75, 0.85 — solid `#3498db` line width 3 with fill `rgba(52,152,219,0.1)` down to the baseline.
  - Mainstream: 0.45, 0.50, 0.25, 0.35, 0.30 — dashed (6/4) `#e74c3c` line width 3.
- **Legend:** blue line "Current users (data source)"; dashed red line "Target users (unknown)".

## FPS Drops Create Non-Random Missing Data

**FPS Drops Create Non-Random Missing Data**

- **The trap:** Below 72Hz, tracking degrades and users change behavior to reduce the scene load.
- **Non-random gaps:** Data goes missing in exactly the most complex, most interesting scenarios.
- **Real-world failure:** "Users move less in crowded spaces" looked like a clear content preference.
- **The real cause:** FPS drops triggered reprojection that makes movement feel sick — a hardware artifact.
- **A/B test contamination:** A "rich vs simple environment" test is really comparing nausea levels.
- **Wrong conclusion:** It reports that users prefer simple when they prefer not being sick.

### Visualization (canvas `canvas8`, 720×200)

Triple line chart: FPS, tracking quality, and user movement all falling as scene complexity rises.

- **Title (bold 14px `#1a5276`):** "Scene Complexity vs Tracking Quality & User Behavior".
- **Axes:** L-shape in `#ccc`, origin (60, 170), plot 620 wide × 120 tall. X axis labeled "Scene Complexity →" centered, with "Simple" at left and "Complex" at right (10px `#666`).
- **Data (complexity 1-10):**
  - FPS (scaled as (fps−30)/65): 90, 90, 88, 82, 75, 68, 60, 52, 45, 38 — solid `#3498db` line width 2.
  - Tracking quality (fraction): 0.98, 0.97, 0.95, 0.90, 0.82, 0.72, 0.60, 0.48, 0.35, 0.22 — solid `#27ae60` line width 2.
  - User movement (fraction): 0.85, 0.83, 0.80, 0.72, 0.60, 0.45, 0.30, 0.20, 0.12, 0.08 — dashed (5/5) `#e74c3c` line width 2.
- **Threshold:** dotted (3/3) `#f39c12` line at 72 FPS, labeled (10px orange): "72 FPS comfort floor".
- **Legend:** blue "FPS"; green "Tracking quality"; red "User movement (mistaken for preference)".

## Social VR Metrics Contaminated by Griefing

**Social VR Metrics Contaminated by Griefing**

- **The trap:** Griefing — space invasion, blocking, noise spam — inflates raw "interaction" counts.
- **Toxicity as success:** High engagement numbers can therefore measure toxicity, not connection.
- **Real-world failure:** A platform reported "12.3 interactions per session" as its headline success metric.
- **What it hid:** For female-presenting avatars, a large share of those interactions were harassment events.
- **Misread proximity:** "Users get closer over time" looked like trust developing between strangers.
- **The real pattern:** Griefers were learning to invade space just below moderation thresholds.
- **Session exit attribution:** A satisfied user leaving and one fleeing harassment log identical metrics.

### Visualization (canvas `canvas9`, 720×200)

Stacked bar chart of interactions per session: positive (green, bottom) vs griefing (red, top) by avatar/user type.

- **Title (bold 14px `#1a5276`):** "Social VR \"Interactions\": Positive vs Griefing (Female Avatars)".
- **Layout:** origin (80, 170), plot 540 wide × 120 tall, scale max 14, bar width 30, five categories.
- **Data (category: positive / griefing per session):** Male avatar 7.2/1.8, Female avatar 4.8/5.5, Non-human avatar 5.5/1.2, New user (any) 3.2/4.8, Veteran user 8.5/0.8.
- **Labels:** total "<N.N>/session" (bold 9px `#1a5276`) above each bar; when griefing > 2, its percentage of total is printed in bold white 9px inside the red segment; category names (9px `#666`, two lines) below.
- **Legend:** green swatch "Positive interactions"; red swatch "Griefing/harassment".
- **Annotation (bold 10px `#e74c3c`, above plot):** "Female avatars: 53% of \"interactions\" are harassment".

## Demo Effect: Amazing First Time, Boring by Session 5

**Demo Effect: Amazing First Time, Boring by Session 5**

- **The trap:** VR's first-use "wow factor" decays rapidly across sessions 1-5 for almost every title.
- **Overestimate:** First-session metrics therefore massively overstate long-term engagement.
- **Real-world failure:** A VR museum scored 9.2/10 with first-timers but 5.8/10 with repeat visitors.
- **What NPS caught:** The demo score measured novelty of the medium, not the value of the content.
- **Retention cliff misdiagnosis:** The universal session 3-5 drop is medium novelty wearing off.
- **Wrong remedy:** Teams read it as a content gap that shipping more content can fix.
- **Investment trap:** Investors fund on first-session demo wow and later see the steady state.
- **The gap:** Steady-state engagement runs 40-60% below what they experienced in the demo.

### Visualization (canvas `canvas10`, 720×200)

Decay curve of engagement score by session number.

- **Title (bold 14px `#1a5276`):** "VR Engagement Score by Session Number (Novelty Decay)".
- **Axes:** L-shape in `#ccc`, origin (60, 170), plot 620 wide × 130 tall, y labels "0/10" to "10/10" every 2 (10px `#666`).
- **Data (session → score out of 10):** S1 9.2, S2 7.8, S3 6.5, S4 5.8, S5 5.2, S6 4.9, S7 4.7, S8 4.5, S10 4.3, S12 4.2, S15 4.1, S20 4.0.
- **Series:** red `#e74c3c` line width 3 with dots (radius 4) at every point; the first point is green `#27ae60` and larger (radius 7). X labels "S<N>" shown for the first six points and the last.
- **Shaded zone:** `rgba(231,76,60,0.1)` band covering sessions 1-5, labeled "Novelty cliff zone" (10px red, top left of band).
- **Annotations:** bold 12px green near the first point: "← Demo/investor day (9.2/10)"; bold 11px red near the tail: "Steady state reality: 4.0/10".

## Regeneration instructions

- **Layout:** detail page in the domains-page style: h1 + `.subtitle`, then one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px, margin 40px 0 15px), each followed by a full-width `.obj-table` with a single `<tr>`: left `<td>` (40%) holds `.obj-title` div (repeating the h2 text) + `<ul>` bullets, right `<td>` (60%, centered) holds the canvas. No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`, margin 8px 0 8px 20px; `strong` in `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; even rows background `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `canvas` CSS fixed at `width: 720px; height: 200px`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) but unused on this page.
- **Canvas:** an `initCanvas(id)` helper sets the backing store to 720×200 × `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates; canvas tag attributes read `width="720" height="300"` but the helper and CSS override the effective size to 720×200. Chart fonts are 9-14px -apple-system (bold 14px titles).
- **Palette:** primary blue `#1a5276`, accent blues `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, amber `#f39c12`, gray text `#666`/`#333`, light axis gray `#ccc`.
