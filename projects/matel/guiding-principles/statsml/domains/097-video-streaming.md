# VIDEO PERSONALIZATION - Domain-Specific Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** VIDEO PERSONALIZATION - Domain-Specific Pitfalls

(No subtitle paragraph on this page.)

## Recommendations = Your Own History Reordered

- 75%+ of recommendations are videos from channels you ALREADY watch or topics you ALREADY engaged with
- "Personalized for you" = mirror of your past, not discovery of your future
- The algorithm is a bubble machine presenting itself as a discovery engine

### Visualization (canvas `canvas1`, 720×200 rendered; HTML attribute 720×300)

Horizontal bar chart: breakdown of where recommendations come from.

- **Title (bold 17px `#1a5276`):** "Recommendation Sources Breakdown".
- **Bars (starting x=30, max width 380, height 28, 44px row pitch; white bold 14px value label inside bar, 14px `#333` label to the right):**
  - "Already Subscribed Channels" 45% red `#e74c3c`
  - "Previously Watched Topics" 30% orange `#e67e22`
  - "Related to Recent Watch" 15% amber `#f39c12`
  - "Genuinely New Discovery" 10% green `#27ae60`
- **Right bracket (dark red `#c0392b`, width 2):** spans the first three bars, with bold 14px annotations: dark red "90% = echo chamber" and green "10% = actual discovery".

## Autoplay Inflates Watch Time

- Video ends → next starts automatically → user is passive → counts as "watch time"
- Model thinks user loved it; but user was making dinner and not watching
- Passive autoplay engagement ≠ active choice engagement; yet both count equally

### Visualization (canvas `canvas2`, 720×200 rendered; HTML attribute 720×300)

Triple-bar chart per video: active attention vs decaying autoplay attention vs constant counted watch time.

- **Title (bold 17px `#1a5276`):** "Active Clicks vs Autoplay: Attention vs Counted Time".
- **Groups (Video 1–Video 5; start x=80, 125px spacing, bars 18px wide, baseline y=180, max height 130):**
  - Active attention (green `#27ae60`): `[92, 88, 85, 90, 87]`.
  - Autoplay attention (red `#e74c3c`): `[60, 42, 28, 20, 15]` — decaying across the autoplay chain.
  - Counted watch time (dashed gray `#7f8c8d` outline rectangle, dash 4/3): `[95, 95, 95, 95, 95]`.
- **Legend (12px, right side):** green swatch "Active attention"; red swatch "Autoplay attention"; dashed outline swatch "Counted watch time".

## Clickbait Thumbnail/Title Gaming

- Creator A: honest thumbnail, accurate title, 40% CTR
- Creator B: misleading thumbnail, outrage title, 70% CTR
- Algorithm rewards B because CTR is a ranking signal
- Quality content systematically outcompeted by manufactured curiosity gap

### Visualization (canvas `canvas3`, 720×200 rendered; HTML attribute 720×300)

Grouped bar chart comparing two creators across four metrics.

- **Title (bold 17px `#1a5276`):** "Creator A (Honest) vs Creator B (Clickbait)".
- **Metrics (x categories, start x=100, 150px spacing, bars 28px wide, baseline y=175, max height 120; white 12px value labels inside bars):** CTR, Completion, Satisfaction, Algo Rank.
- **Creator A (green `#27ae60`):** `[40, 85, 82, 35]` %.
- **Creator B (orange `#e67e22`; the Algo Rank bar rendered red `#e74c3c`):** `[70, 35, 25, 88]` %.
- **Legend (13px with swatches):** green "Creator A (honest)"; orange "Creator B (clickbait)".
- **Annotation (bold 13px dark red `#c0392b`):** "Algorithm promotes B ↑".

## "Not Interested" / "Don't Recommend" DON'T WORK Reliably

- User clicks "not interested" on topic X → still sees X next week
- The negative signal is weak relative to positive engagement signals
- One "not interested" click vs 50 watch-minutes of similar content = 50 wins
- Negative feedback is structurally underpowered

### Visualization (canvas `canvas4`, 720×200 rendered; HTML attribute 720×300)

Two-bar weight comparison: a tiny negative-feedback bar vs a massive watch-time bar.

- **Title (bold 17px `#1a5276`):** "Signal Weight: "Not Interested" vs Watch Time".
- **Left bar (blue `#3498db`, 80px wide at x=120, baseline y=160):** weight 1 (drawn at minimum visible height ~5px in a 120px scale for weight 50); white bold "1x" label; captions below (14px `#333`): ""Not Interested"" / "click (weight: 1x)".
- **Right bar (red `#e74c3c`, 80px wide at x=400):** weight 50 filling 120px height; white bold "50x" label; captions: "50 min watch time" / "of similar content (weight: 50x)".
- **Center:** bold 24px gray `#7f8c8d` "vs".
- **Verdict (bold 14px dark red `#c0392b`, right side):** "Negative feedback is" / "structurally underpowered" / "50:1 imbalance".

## Watch Time ≠ Value

- 2-hour conspiracy video: watch time = 2hr (great for algorithm!)
- User walks away more misinformed and anxious
- 30-second helpful tutorial: watch time = 30s (bad for algorithm!)
- User's problem solved efficiently
- The metric PUNISHES efficiency and REWARDS time-wasting

### Visualization (canvas `canvas5`, 720×200 rendered; HTML attribute 720×300)

Two side-by-side scenarios with inverted algo-score vs user-value bars.

- **Title (bold 17px `#1a5276`):** "Watch Time vs Actual User Value".
- **Left scenario (baseline y=170):** green bar 50×110 labeled inside (white 12px) "Algo / Score / HIGH"; adjacent red bar 50×30 labeled "User / Value"; bold dark-red annotation "↓ Misinformed"; caption (13px `#555`): "2hr Conspiracy Video".
- **Center divider:** vertical dashed light-gray line (`#bdc3c7`, dash 5/5); purple `#8e44ad` bold 15px label "METRIC" / "MISMATCH".
- **Right scenario:** red bar 50×15 labeled (white 11px) "Algo LOW"; adjacent green bar 50×105 labeled "User / Value / HIGH"; bold green annotation "↑ Problem Solved"; caption: "30s Helpful Tutorial".

## Shorts vs Long-Form = Different Algorithm, Different Behavior

- Shorts: 15-60s, swipe-based, dopamine hits, passive consumption
- Long-form: 10-60min, intentional click, lean-forward attention
- Same user but different MODE
- Model trained on long-form fails on Shorts and vice versa
- Cross-pollination between them is noise

### Visualization (canvas `canvas6`, 720×200 rendered; HTML attribute 720×300)

Two behavioral profile panels with mini bar sketches and cross-contamination arrows.

- **Title (bold 17px `#1a5276`):** "Same User, Two Modes: Shorts vs Long-Form".
- **Left panel (heading bold 14px red `#e74c3c` "SHORTS MODE"; 13px label/value rows, values in red):** "Avg View Time: 3-5s", "Videos/Session: 50+", "Interaction: Swipe (passive)", "Intent: Dopamine / kill time". Mini viz: 12 short red bars of heights `[8, 12, 6, 15, 9, 11, 7, 14, 10, 8, 13, 6]` (14px wide, 18px pitch), captioned in 10px gray `#999` "rapid swipes".
- **Right panel (heading bold 14px blue `#2980b9` "LONG-FORM MODE"; values in blue):** "Avg View Time: 15 min", "Videos/Session: 2-3", "Interaction: Click (active)", "Intent: Learn / be entertained". Mini viz: 3 tall blue bars (50px wide, heights 50/45/55), captioned "deliberate watches".
- **Center:** two dashed amber arrows (`#f39c12`, dash 6/4, width 2) crossing between panels; bold 12px amber "NOISE" over 11px "Cross-pollination".

## Creator-Algorithm Co-Evolution

- Algorithm rewards 10+ min videos → all creators make 10+ min videos
- Content padded to hit threshold → quality drops
- Algorithm changes to reward "satisfaction" → creators pivot → quality shifts again
- The CONTENT is shaped by the algorithm, not by creative vision
- You're ranking content that was created to game your ranking

### Visualization (canvas `canvas7`, 720×200 rendered; HTML attribute 720×300)

Horizontal timeline of algorithm changes (above) and creator responses (below).

- **Title (bold 17px `#1a5276`):** "Creator-Algorithm Feedback Loop Over Time".
- **Timeline:** dark slate `#2c3e50` line (width 2) at y=100 from x=50 to x=680 with arrowhead; blue `#2980b9` 5px dots at each event; bold 12px year label below each dot.
- **Events (algo change above in blue 11px, creator response below in red 11px):**
  - 2012 — "Rewards watch time" / "Avg: 4 min"
  - 2016 — "Rewards >10 min" / "Avg: 12 min"
  - 2019 — "Rewards retention%" / "Hook intros"
  - 2022 — "Rewards satisfaction" / "Survey bait"
  - 2024 — "Rewards Shorts" / "All pivot"
- **Bottom label (bold 12px purple `#8e44ad`):** "FEEDBACK LOOP: Content shaped by ranking, then ranked again".
- **Legend (11px with swatches):** blue "Algorithm change"; red "Creator response".

## Subscription Feed is Dead

- User subscribes to 200 channels → subscription feed shows chronological mix
- User never checks it → relies on algorithmic home page
- Algorithm decides which subscriptions to show
- The SUBSCRIPTION is a user's explicit preference signal, completely overridden by engagement prediction
- Stated preference ≠ served content

### Visualization (canvas `canvas8`, 720×200 rendered; HTML attribute 720×300)

Left: uniform grid of 200 subscription dots; right: 17 engagement-weighted dots the algorithm actually serves.

- **Title (bold 17px `#1a5276`):** "Subscriptions (User Intent) vs Algorithm (Served)".
- **Left panel:** heading bold 13px blue `#2980b9` "200 Subscribed Channels" with 11px gray `#666` note "(equal weight intended by user)"; 20×10 grid of 2px light-gray `#bdc3c7` dots (12px pitch).
- **Right panel:** heading bold 13px red `#e74c3c` "Algorithm Shows on Homepage" with note "(15-20 channels, engagement-weighted)"; 17 dots of varying diameter (14 down to 4 px), colored by rank: first 7 red `#e74c3c`, next 7 orange `#e67e22`, last 3 amber `#f39c12`.
- **Center arrow (gray `#7f8c8d`, width 2)** pointing left-to-right, with bold 12px dark-red labels "92% filtered out" and "by engagement prediction".
- **Bottom summary (bold 12px purple `#8e44ad`):** "Stated preference (subscribe) ≠ Served content (homepage)".

## Regeneration instructions

- **Layout:** h1 (no subtitle), then per pitfall an `<h2>` heading (1.4em `#1a5276` with 2px `#2980b9` bottom border) followed by a single-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (repeating the pitfall name) + bullet list, right `<td>` (60%, centered) holds the canvas. Even table rows get background `#fafcfe`. No closing callout (a `.philosophy` style is defined but unused).
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` style defined (`#666` 1.05em) but unused; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** canvas elements declare `width="720" height="300"` in HTML, but the shared `setupCanvas(id)` helper overrides the drawing size to 720×200 CSS pixels (backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), CSS size fixed, `ctx.scale` back to logical coordinates). Default chart font 17px `-apple-system`; titles bold 17px; labels 10-14px.
- **Palette:** primary blue `#1a5276`, secondary blues `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c` (dark `#c0392b`), orange `#e67e22`/`#f39c12`, purple `#8e44ad`, slate `#2c3e50`, grays `#555`/`#666`/`#7f8c8d`/`#999`/`#bdc3c7`.
