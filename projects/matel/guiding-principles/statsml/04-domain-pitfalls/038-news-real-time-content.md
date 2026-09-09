# News & Real-Time Content

**Page type:** detail page (obj-table layout: one h2 + one-row table per pitfall, labeled bullets left 50%, wide canvas right 50%)
**HTML title tag:** News & Real-Time Content — Domain-Specific Pitfalls

**Subtitle:** Domain-specific pitfalls in ML and analytics for news and media.

## Freshness Decay

- **The decay:** Article value drops approximately 90% within the first 24 hours of publication.
- **Stale training:** A model trained on last week's "good articles" learns from content nobody wants now.
- **No static threshold:** What counts as "fresh" shifts with news cycle speed and topic category.
- **Ongoing stories:** A story with continuing developments holds its value past the usual window.
- **Non-linear:** "Time since publication" degrades non-linearly, so linear decay corrections are inadequate.

### Visualization (canvas `canvas1`, 720×200)

Exponential decay curve of article value over 24 hours with shaded area under the curve.

- **Title (17px `#1a5276`, centered):** "Article Value Decay Over 24 Hours".
- **Padding:** top 30, right 30, bottom 40, left 60. L-shaped axes `#333` 1.5px.
- **Y-axis:** 14px `#555` labels 0%, 25%, 50%, 75%, 100% with light `#e0e0e0` 0.5px gridlines; rotated axis title "Value".
- **X-axis:** 14px `#555` labels "0h", "4h", "8h", "12h", "16h", "20h", "24h".
- **Curve:** value = exp(−3.5·t) for t in [0,1] (0–24h), stroke `#c0392b` 3px; area under the curve filled `#c0392b` at 10% opacity.
- **Annotation (13px `#c0392b`):** "~90% value lost" near the curve at the 6-hour point.

## Breaking News vs Misinformation Race

- **The race:** First reports of a breaking event are frequently wrong or incomplete.
- **Speed wins:** Yet speed consistently beats accuracy on every engagement metric that ranking uses.
- **Perverse signal:** Unverified claims out-click, out-share, and out-comment accurate reporting hours later.
- **Worst timing:** The engagement signal is strongest precisely when accuracy is lowest.
- **Feedback loop:** By the time corrections land, the model has learned that sensational content wins.

### Visualization (canvas `canvas2`, 720×200)

Crossing lines: engagement spikes early and decays while accuracy starts low and climbs; early "Danger Zone" shaded.

- **Title (17px `#1a5276`, centered):** "Speed/Engagement vs Accuracy After Breaking Event".
- **Padding:** top 30, right 120 (legend gutter), bottom 40, left 60. L-shaped axes `#333` 1.5px.
- **X-axis labels (13px `#555`):** "0min", "30min", "1hr", "2hr", "4hr", "8hr", "24hr" evenly spaced. **Y-axis labels:** "High" at top, "Low" at bottom.
- **Engagement line (`#e74c3c`, 2.5px), normalized points at the seven x ticks:** `[0.1, 0.95, 0.85, 0.6, 0.4, 0.25, 0.1]`.
- **Accuracy line (`#27ae60`, 2.5px):** `[0.15, 0.25, 0.4, 0.6, 0.75, 0.88, 0.95]`.
- **Danger zone:** first 30% of plot width shaded `rgba(231, 76, 60, 0.08)`, labeled 12px `#e74c3c` "Danger Zone".
- **Legend (right gutter, 13px):** red line sample "Engagement"; green line sample "Accuracy".

## Source Reliability Scoring

- **The asymmetry:** The same claim from the NYT or Reuters reads differently than from a blog or anonymous account.
- **The dilemma:** Any credibility ranking risks becoming censorship that silences smaller, independent voices.
- **Brand bias:** Establishment media gets preferential treatment for brand recognition, not accuracy.
- **No neutral ground:** Every scoring system embeds a worldview about what legitimate journalism is.
- **The other side:** Ignoring source quality treats misinformation equally with verified reporting.

### Visualization (canvas `canvas3`, 720×200)

Horizontal credibility spectrum bar with gradient fill and triangular source markers.

- **Title (17px `#1a5276`, centered):** "Source Credibility Spectrum".
- **Padding:** top 35, right 30, bottom 50, left 30.
- **Spectrum bar (y=70, height 30, `#333` 1px border):** left-to-right linear gradient with stops `#c0392b` (0), `#e67e22` (0.3), `#f1c40f` (0.5), `#27ae60` (0.7), `#1a5276` (1).
- **Labels below bar (12px `#333`):** "Low Credibility" at 10%, "Medium" at 50%, "High Credibility" at 90%.
- **Source markers (downward triangles above the bar in marker color; two-line 11px `#333` labels below the axis labels):**
  - Anonymous Blogs — position 0.08 — `#c0392b`
  - Social Media — 0.22 — `#e74c3c`
  - Partisan Outlets — 0.38 — `#e67e22`
  - Local News — 0.55 — `#f39c12`
  - Major Newspapers — 0.72 — `#27ae60`
  - Wire Services (AP/Reuters) — 0.88 — `#1a5276`
- **Bottom annotation (13px `#8e44ad`, centered):** 'Where is the line between "reliability scoring" and "censorship"?'

## Paywalled Content Creates Data Gap

- **The gap:** The highest-quality investigative journalism is locked behind paywalls, out of reach of crawlers.
- **Skewed corpus:** Freely accessible content is therefore massively overrepresented in training data.
- **What's learned:** Models train on aggregated summaries, opinion blogs, rewritten press releases, and SEO filler.
- **The result:** A model that never saw the best journalism confidently mimics the style and depth of free sources.
- **Invisible:** The data gap is systematic yet invisible — nothing in the corpus flags what is missing.

### Visualization (canvas `canvas4`, 720×200)

Two horizontal availability bars: paywalled (mostly inaccessible, dashed outline) vs free content.

- **Title (17px `#1a5276`, centered):** "Data Availability vs Content Quality Gap".
- **Padding:** top 30, right 30, bottom 40, left 100. Bars 35px tall with 20px gap.
- **Paywalled bar:** solid `#1a5276` fill for 20% of plot width; dashed [5,5] `#1a5276` 2px outline rectangle continuing another 60% (exists but inaccessible). Row labels right-aligned 13px `#1a5276`: "Paywalled" / "(High Quality)". White 14px in-bar label: "20% accessible".
- **Free content bar:** solid `#e67e22` fill for 85% of plot width. Row labels `#e67e22`: "Free Content" / "(Lower Quality)". White in-bar label: "85% of training data".
- **Dashed-outline caption (11px `#555`):** "(dashed = exists but inaccessible)".
- **Bottom annotation (14px `#c0392b`, centered):** "DATA GAP: Model never sees the best journalism".

## Clickbait Gaming

- **The mismatch:** "You won't believe what happened next..." earns 3-5x the click-through rate of a factual one.
- **Same story:** The underlying article is identical; only the framing of the headline changed.
- **Wrong target:** Using CTR as a quality signal teaches the model that clickbait equals "good" content.
- **Misalignment:** The optimization target (clicks) is misaligned with the quality target (informed readers).
- **Arms race:** Publishers keep evolving ever more manipulative headline patterns to lift measured CTR.
- **Rewarded as innovation:** The model treats each new wave of engagement bait as improved content.

### Visualization (canvas `canvas5`, 720×200)

Bar chart comparing CTR of clickbait vs accurate headline styles.

- **Title (17px `#1a5276`, centered):** "Click-Through Rate: Clickbait vs Accurate Headlines".
- **Padding:** top 30, right 30, bottom 50, left 60. L-shaped axes `#333` 1.5px; y gridlines `#e8e8e8` 0.5px with 12px `#555` labels 0%, 5%, 10%, 15% (scale max 14%). Bars 80px wide.
- **Data (white 13px CTR label inside bar top; two-line 10px `#333` category labels below):**
  - '"You won't believe..."' — 12.5% — `#e74c3c`
  - '"Shocking revelation..."' — 10.8% — `#e74c3c`
  - '"Breaking: Experts say..."' — 8.2% — `#e67e22`
  - "Factual Summary" — 2.8% — `#27ae60`
  - "Nuanced Analysis" — 2.1% — `#27ae60`
  - "Accurate Headline" — 2.5% — `#27ae60`
- **Top annotation (14px `#c0392b`, centered over the bars):** "~5x CTR difference".

## Retraction/Correction Lag

- **The spread:** An erroneous article gets shared 100,000 times in the first hours after publication.
- **The 200x gap:** The correction issued 6 hours later reaches only about 500 people.
- **Stale copies:** Every reshare creates a copy that will never be updated with the correction.
- **Learned as fact:** Scraped data holds far more copies of the error than the fix, so the model learns the error.
- **Permanent:** Cached copies, screenshots, and syndicated reprints persist in the training data indefinitely.

### Visualization (canvas `canvas6`, 720×200)

Viral S-curve of error spread vs a nearly flat correction-reach curve starting at the 6-hour mark.

- **Title (17px `#1a5276`, centered):** "Viral Spread: Original Error vs Correction".
- **Padding:** top 30, right 120 (legend gutter), bottom 40, left 60. L-shaped axes `#333` 1.5px.
- **X-axis labels (12px `#555`):** "0h", "2h", "4h", "6h", "8h", "12h", "24h" evenly spaced. **Y-axis labels:** "100K" top, "50K" middle, "0" at baseline.
- **Error curve (`#c0392b`, 3px):** logistic S-curve shares = 1/(1 + exp(−8·(t − 0.2))) over t in [0,1], saturating at 100K.
- **Correction curve (`#27ae60`, 3px):** starts at t = 6/24; shares = 0.005·(1 − exp(−3·progress)) of the max — essentially flat near zero (~500 shares).
- **Correction marker:** dashed [4,4] vertical `#7f8c8d` 1px line at the 6h mark, labeled 11px `#7f8c8d` "Correction issued" below the axis.
- **Annotations (13px):** `#c0392b` "100,000 shares" near the top of the error curve; `#27ae60` "~500 shares" near the correction curve; 15px `#8e44ad` "200x gap" between them.
- **Legend (right gutter, 12px):** `#c0392b` line sample "Error spread"; `#27ae60` line sample "Correction" / "reach".

## Regeneration instructions

- **Layout:** standard domains detail page. h1, `.subtitle` paragraph, then per pitfall an `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border, padding-bottom 8px) followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` + a `<ul>` of labeled one-sentence bullets (each bullet begins with a bold `<strong>` label), right `<td>` (60%, centered) holds the canvas. Even table rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; ul 0.9em `#333`, li margin 4px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border 4px `#2980b9`) but unused. No nav bar, no back/home links.
- **Canvases:** each declared `<canvas id="canvasN" style="width:720px;height:200px;">` (inline style, no width/height attributes); a shared `setupCanvas(id)` helper sets the backing store to 720×200 × `window.devicePixelRatio` at 720×200px, and calls `ctx.scale` so drawing stays in logical coordinates. Charts set their own fonts (titles 17px, labels 10–15px) and use `ctx.textAlign` for centered/right-aligned text. No shared background fill — canvases are transparent over the page.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark red `#c0392b`, amber `#f39c12`, yellow `#f1c40f`, purple `#8e44ad`, gray `#7f8c8d`/`#555`/`#333`.
