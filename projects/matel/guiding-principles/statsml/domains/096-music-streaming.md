# MUSIC STREAMING - Domain-Specific Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** MUSIC STREAMING - Domain-Specific Pitfalls

**Subtitle:** Streaming play data conflates mode, context, novelty, and paid placement with genuine preference — a play is not a like and a skip is not a dislike.

## Playlist-as-product creates staleness

- Discover Weekly generated Monday, user listens all week, same songs
- Daily Mix is "personalized" but rarely changes
- The "recommendation" is a STATIC ARTIFACT that doesn't adapt to mood/moment
- Users outgrow playlists faster than they refresh
- Playlist position bias same as search position bias

### Visualization (canvas `canvas1`, 720×200 rendered; HTML attribute 720×300)

Two-line chart: playlist freshness declining while user interest in new content rises over a week.

- **Title (bold 17px `#1a5276`):** "Playlist Freshness vs User Interest Over Week".
- **Plot area:** left 70, top 25, width 600, height 140; gray `#666` L-shaped axes; y labels 100% / 50% / 0% (13px `#666`, right-aligned); x labels Mon–Sun.
- **Freshness line (red `#e74c3c`, width 3):** `[100, 82, 65, 48, 32, 18, 8]` (% scale 0–100).
- **Interest line (green `#27ae60`, width 3):** `[50, 58, 67, 75, 84, 90, 95]`.
- **Legend (14px, swatch rectangles, top right):** red "Playlist Freshness"; green "User Interest in New Content".
- **Annotation (bold 14px orange `#e67e22`, mid-chart):** "← Growing staleness gap →".

## Radio/station mode vs on-demand = completely different signals

- Station: passive lean-back, skip = mild dislike
- On-demand: active choice = strong preference
- Mixing these signals as "preference" is wrong
- A song played in radio mode has 10% the preference signal of an actively searched song

### Visualization (canvas `canvas2`, 720×200 rendered; HTML attribute 720×300)

Grouped bar chart: preference-signal strength per song in radio vs on-demand mode.

- **Title (bold 17px `#1a5276`):** "Signal Strength: Same "Play" Event, Different Modes".
- **Plot area:** left 80, top 30, width 580, height 130; gray axes; y labels 100% / 50% / 0%.
- **Songs (x categories, 13px `#333`):** Song A, Song B, Song C, Song D; bars 28px wide, paired per song.
- **Radio bars (orange `#e67e22`):** `[12, 18, 10, 15]` % with orange value labels above.
- **On-demand bars (green `#27ae60`):** `[88, 92, 85, 95]` % with green value labels above.
- **Legend (14px with swatches, top left):** orange "Radio/Passive"; green "On-Demand/Active".

## Skip ≠ dislike

- Skipped for: wrong mood, already heard today, too long for commute remaining, started talking to someone
- Skip is CONTEXTUAL not preferential
- Yet most models treat skip = negative signal equally
- A 15-second skip after hearing the chorus vs 2-second skip of unknown song = very different

### Visualization (canvas `canvas3`, 720×200 rendered; HTML attribute 720×300)

Bar chart: breakdown of skip reasons as percent of all skips.

- **Title (bold 17px `#1a5276`):** "Why Users Actually Skip (% of all skips)".
- **Plot area:** left 80, top 30, width 580, height 130; gray axes; y labels 30% / 15% / 0%.
- **Bars (60px wide, evenly gapped; bold 14px `#333` value above each; 12px two-line labels below):**
  - "Wrong Mood" 28% `#3498db`
  - "Already Heard" 25% `#2980b9`
  - "Too Long" 18% `#1abc9c`
  - "Distracted" 14% `#f39c12`
  - "Actual Dislike" 15% `#e74c3c`
- **Annotation (top right):** bold 13px red "Only 15% of skips = true dislike!" over 12px gray `#666` "Models treat ALL skips as negative".

## Listening context dominates preference

- Same person: workout = high BPM aggressive; cooking = chill jazz; work = ambient no-lyrics
- The "preference" is not for SONGS but for ACTIVITIES
- Without context: model averages across contexts and recommends bland middle-ground

### Visualization (canvas `canvas4`, 720×200 rendered; HTML attribute 720×300)

Three Gaussian BPM-preference curves for different contexts plus a dashed model-average line.

- **Title (bold 17px `#1a5276`):** "Same User, Different Contexts: BPM Preferences".
- **Plot area:** left 70, top 30, width 600, height 130; gray axes; x axis BPM 50–170 with labels "60 BPM"…"160 BPM" every 20; rotated y label "Frequency".
- **Curves (Gaussian `exp(-0.5·((x-mean)/std)²)`, amplitude 0.85·h, width 2.5):**
  - Work: blue `#3498db`, mean 70, std 12.
  - Cooking: green `#27ae60`, mean 90, std 14.
  - Workout: red `#e74c3c`, mean 140, std 12.
- **Model average:** vertical dashed orange line (`#e67e22`, dash 6/4, width 2) at BPM 100.
- **Legend (13px, along top):** blue "Work (70 BPM)"; green "Cooking (90 BPM)"; red "Workout (140 BPM)"; orange "Model Avg (~100) - satisfies nobody".

## Repeat plays inflate signal massively

- User discovers a song, plays it 50 times in a week
- Model: "THIS IS THEIR FAVORITE SONG EVER", recommends similar forever
- Actually: novelty spike that will decay to zero in 2 weeks
- Fresh obsession ≠ lasting preference; need decay weighting

### Visualization (canvas `canvas5`, 720×200 rendered; HTML attribute 720×300)

Line chart over 21 days: actual plays spike-and-decay vs model weight staying high.

- **Title (bold 17px `#1a5276`):** "Novelty Spike vs Model Weight Over Time".
- **Plot area:** left 70, top 30, width 600, height 130; gray axes; x labels Week 1 / Week 2 / Week 3; y labels 20 / 10 / 0.
- **Actual plays (blue `#3498db`, width 2.5, area filled `rgba(52,152,219,0.15)`):** daily values `[5, 12, 18, 15, 20, 16, 14, 8, 5, 3, 2, 2, 1, 1, 1, 0, 0, 1, 0, 0, 0]`.
- **Model weight (red `#e74c3c`, width 2.5):** `[5, 12, 17, 17.5, 18, 18, 18, 18, 18, 18, 18, 17.8, 17.5, 17.5, 17.2, 17, 17, 16.8, 16.5, 16.5, 16.2]`.
- **Legend (13px with swatches, top right):** blue "Actual Plays/Day"; red "Model Weight (stays high!)".

## Discovery vs comfort tradeoff

- User SAYS they want new music; behavior: 80% of listening is familiar catalog
- Recommend familiar = high engagement, low exploration, eventual boredom
- Recommend new = low immediate engagement, high long-term satisfaction
- Optimizing for THIS session hurts NEXT MONTH

### Visualization (canvas `canvas6`, 720×200 rendered; HTML attribute 720×300)

Two crossing engagement curves over six months: familiar recommendations declining vs discovery mix growing.

- **Title (bold 17px `#1a5276`):** "Familiar vs Discovery: Short-term vs Long-term Engagement".
- **Plot area:** left 70, top 30, width 600, height 130; gray axes; x labels Month 1–Month 6; y labels "High" / "Low".
- **Familiar line (orange `#e67e22`, width 3):** `[92, 85, 72, 60, 48, 38]` (0–100 scale).
- **Discovery line (green `#27ae60`, width 3):** `[35, 45, 58, 70, 80, 88]`.
- **Crossover marker:** vertical dashed gray line (`#999`, dash 4/3) at ~month 3.2 position, italic 12px gray caption "Crossover ~Month 3".
- **Legend (13px with swatches, top left):** orange "Familiar Recs (declining engagement)"; green "Discovery Mix (growing satisfaction)".

## Collaborative filtering can't explain WHY

- Two users with 90% playlist overlap → recommend each other's remaining 10%
- But: one listens for lyrics, other for production
- The REASON for liking is invisible in play history
- Taste similarity is surface-level, motivation-blind

### Visualization (canvas `canvas7`, 720×200 rendered; HTML attribute 720×300)

Venn diagram: two overlapping user circles with different motivations.

- **Title (bold 17px `#1a5276`):** "Collaborative Filtering: Same Taste, Different Reasons".
- **Circles (radius 65, centers 140px apart at mid-height):** User A blue — fill `rgba(52,152,219,0.2)`, stroke `#3498db` width 2; User B red — fill `rgba(231,76,60,0.2)`, stroke `#e74c3c` width 2.
- **Overlap label (centered between circles):** bold 16px `#1a5276` "90%" over 12px "overlap".
- **User A labels:** bold 14px `#2471a3` "User A"; 12px "Cares about:"; bold 12px green `#27ae60` "Lyrics" / "Storytelling".
- **User B labels:** bold 14px `#c0392b` "User B"; 12px `#333` "Cares about:"; bold 12px orange `#e67e22` "Production" / "Bass & Mixing".
- **Annotation (right side):** bold 13px red `Model sees: "Similar users!"` and "Reality: Coincidental overlap"; 12px gray `#666` "Motivation is invisible in play data".

## Artist gaming and playlist placement

- Artists buy plays via bot farms to get onto algorithmic playlists
- music streaming platform's "organic" playlists contain promoted/paid content
- The "personalized" playlist is partially an ad marketplace
- Organic signal contaminated by paid placement

### Visualization (canvas `canvas8`, 720×200 rendered; HTML attribute 720×300)

Stacked bar chart: playlist composition split into organic, paid placement, and bot-inflated shares.

- **Title (bold 17px `#1a5276`):** "Playlist Composition: What You See vs Reality".
- **Plot area:** left 80, top 30, width 560, height 130; gray axes; y labels 100% / 50% / 0%.
- **Playlists (x categories, 11px two-line labels):** "Discover Weekly", "Release Radar", "Daily Mix", "Genre Playlist"; bars 70px wide, stacked bottom-to-top: bot-inflated red `#e74c3c`, paid orange `#e67e22`, organic green `#27ae60`.
  - Organic: `[52, 60, 45, 35]`; Paid: `[28, 22, 30, 40]`; Bot-inflated: `[20, 18, 25, 25]`.
- **Right bracket (blue `#1a5276`, width 2):** square bracket spanning bar height, with bold 13px labels: "Presented" / "as 100%" / ""organic"".
- **Legend (13px with swatches, above the plot):** green "Organic"; orange "Paid Placement"; red "Bot-Inflated".

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, then per pitfall an `<h2>` heading (1.4em `#1a5276` with 2px `#2980b9` bottom border) followed by a single-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (repeating the pitfall name) + a `.pitfall-desc` bullet list, right `<td>` (60%, centered) holds the canvas. Even table rows get background `#fafcfe`. No closing callout on this page (a `.philosophy` style is defined but unused).
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; ul 0.9em `#333`; `.pitfall-desc` 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** canvas elements declare `width="720" height="300"` in HTML, but the shared `setupCanvas(id)` helper overrides the drawing size to 720×200 CSS pixels (backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), CSS size fixed, `ctx.scale` back to logical coordinates). Default chart font 17px `-apple-system`; titles bold 17px; labels 11-14px.
- **Palette:** primary blue `#1a5276`, secondary blues `#2980b9`/`#3498db`/`#2471a3`, green `#27ae60`, red `#e74c3c` (dark `#c0392b`), orange `#e67e22`/`#f39c12`, teal `#1abc9c`, grays `#666`/`#999`.
