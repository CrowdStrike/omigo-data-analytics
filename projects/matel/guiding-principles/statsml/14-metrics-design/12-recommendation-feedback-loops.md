# Web Recommendation — Feedback Loops & Algorithmic Amplification

**Page type:** detail page (h2 section headings, each followed by a two-column obj-table row: text left 40%, canvas right 60%)
**HTML title tag:** Web Recommendation — Feedback Loops & Algorithmic Amplification

**Subtitle:** How autoplay, view-history-based ranking, and business decisions create self-reinforcing content bubbles and confounded engagement signals.

## 1. Autoplay Feedback Loop

**System plays content → counts it as engagement → promotes it → plays it more**

- **The loop:** YouTube autoplay selects next video → user doesn't skip → system records "watched" → model scores video higher → autoplay selects it for more users.
- **The fiction:** "Watched" ≠ "chose." The user was passive. The system made the choice and then credited the user's "engagement." It's measuring its own decisions.
- **Convergence:** After enough cycles, autoplay recommendations converge to a small set of "winners" — not because users prefer them, but because they were selected first.
- **Who hits this:** Content recommendation teams. Music playlist generators. Podcast "up next" algorithms.

**Fix:** Distinguish active choice (user clicked from list) from passive consumption (autoplay/default). Weight active choices 5-10× higher. Measure "user stopped autoplay" as negative signal.

### Visualization (canvas `ca1`, 720×300)

Circular cycle diagram: four-stage feedback loop drawn as a ring with arrows.

- **Geometry:** circle centered at (360, 145), radius 85; background ring in `rgba(26,82,118,0.1)` at 20px line width.
- **Four arc segments** (line width 4) between angles -90°, 0°, 90°, 180°, each trimmed by 0.2 rad at both ends, each ending in a filled triangular arrowhead of its own color.
- **Stage labels (bold 11px, centered, colored to match their arc):**
  - "Autoplay Selects" — `#1a5276`, above the ring
  - "User Passive" — `#e67e22`, right of the ring
  - "System Records / "Engagement"" (two lines) — `#e74c3c`, below the ring
  - "Model Boosts / Score" (two lines) — `#8e44ad`, left of the ring
- **Caption (10px red `#e74c3c`, bottom center):** "User never actively chose — system measures its own selection".

## 2. View History Echo Chamber

**Recommend based on history → user watches → history reinforces → narrower recommendations**

- **The spiral:** User watches one cooking video → recommended 10 more → watches 3 → now "cooking enthusiast" → feed is 80% cooking. User's actual interest was 5% cooking.
- **Why it's a bias:** Early random clicks get disproportionate weight. The system never explores whether the user would prefer other content — it exploits the signal it already has.
- **Network effect:** Users who watch similar content get similar recommendations → their behavior converges → model sees "validation" of the cluster. Artificial homogeneity.

**Fix:** Topic diversity quotas in recommendations. "Explore" slots that deliberately break the pattern. Decay weight on old history. Let users reset or edit their profile signal.

### Visualization (canvas `ca2`, 720×300)

Declining bar chart with dashed trend line: topic diversity shrinking over time.

- **Title (bold 14px `#1a5276`, top center):** "Topic Diversity Over Recommendation Cycles".
- **Data:** cycles `["Day 1", "Week 1", "Week 2", "Month 1", "Month 3"]` with diversity percentages `[85, 60, 40, 22, 12]`.
- **Layout:** margins left 80, right 60, top 50, bottom 40; 40px-wide bars centered at 5 evenly spaced x positions; y scaled 0–100%.
- **Bar colors by value:** >50% → `rgba(39,174,96,0.5)` (green); >25% → `rgba(230,126,34,0.5)` (orange); else `rgba(231,76,60,0.5)` (red).
- **Labels:** bold 11px `#333` percent value above each bar; 11px `#666` cycle label below each bar.
- **Trend line:** dashed red `#e74c3c` (dash 4/3, width 2) connecting the bar tops.
- **Caption (bold 12px red `#e74c3c`, bottom center):** "One random click → system narrows your entire feed around it".

## 3. Viral Amplification — Business Decisions Confound Organic Signal

**Non-organic boost creates artificial engagement that the algorithm treats as real**

- **The mechanism:** Business decides to promote content (paid placement, trending page, push notification) → content gets 100× normal impressions → gathers clicks → algorithm sees high engagement → promotes organically. The boost becomes self-sustaining.
- **The confusion:** Was the content good, or was it force-fed? Post-boost CTR includes the artificially-generated impressions. You cannot separate organic demand from manufactured demand.
- **Examples:** Spotify playlist placement. App store "featured" section. Twitter/X trending topics. Netflix "Top 10" (shown to everyone → watched by everyone → stays in Top 10).

**Fix:** Tag impressions by source (organic vs promoted). Train separate models or exclude promoted impressions from organic ranking signals. Measure lift ONLY on unexposed control group.

### Visualization (canvas `ca3`, 720×300)

Two-line time series over 12 days with a shaded promotion window.

- **Title (bold 14px `#1a5276`, top center):** "Engagement Signal: Organic vs Business-Boosted".
- **Layout:** margins left 70, right 40, top 50, bottom 35; 12 evenly spaced day points; y scaled to max 65.
- **Organic line (green `#27ae60`, width 2.5):** `[5, 6, 5, 7, 6, 5, 6, 8, 7, 6, 5, 6]`.
- **Boosted line (red `#e74c3c`, width 2.5):** `[5, 6, 5, 7, 45, 62, 55, 48, 35, 25, 18, 14]`.
- **Boost zone:** rectangle spanning day indices 4–6, filled `rgba(231,76,60,0.08)`, labeled "PROMOTED" in 10px red centered near its top.
- **Legend (bold 11px, bottom left):** green "— Organic (no boost)"; red "— With business boost (self-sustaining after removal)".

## Regeneration instructions

- **Layout:** each section is an `<h2>` heading ("1. …", "2. …", "3. …", 1.3em `#1a5276` with 2px `#2980b9` bottom border) followed by a single-row `.obj-table`: full-width, border-collapse, one `<tr>`; left `<td>` (40%) holds `.obj-title` div + `<ul>` bullets + a `<p>` Fix line, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`, li margin 4px 0; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="300"` per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad` (loop diagram), bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
