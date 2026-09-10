# Content Consumption Patterns

**Page type:** detail page (compact card-sections: one numbered h2 per section, two-column layout table with tag pills + labeled bullets left ~45%, canvas right ~55%)
**HTML title tag:** Content Consumption Patterns

**Subtitle:** Watch %, read depth, skips, replays, binges — what people actually consume is the most honest signal they ever emit

## 1. Completion Rate & Length Normalization

Tags: signal (blue), bias (orange), defense (green)

- **TikTok weighting** — fully-watched 15s clip outranks half-watched masterpiece
- **Benchmarks** — <60s: 80-90%; 10-min: 40-60%; hour-long: below 30%
- **Length fix** — rank on watch-time percentile within duration bucket
- **Rule of thumb** — never compare completion across >2x length gap
- **Catalog shrinkage** — raw completion pushes creators toward fragments

*Example:* Netflix distinguishes "started" from "meaningfully watched" — opposite signals in naive play logging.

**Failure mode:** Unnormalized completion teaches the ranker "short beats long," not "good beats bad."

### Visualization (canvas `c1`, 720×300)

Two-series line chart over content duration. Title (bold 14px `#1a5276`, centered): "Raw Completion % Falls with Duration — Normalized Score Does Not". Padding top 40, bottom 50, left 60, right 170; L-shaped gray (`#999`) axes; alternating duration-bucket bands (4 buckets, odd bands filled `rgba(26,82,118,0.05)`).

- **Data (8 points, y max 100, y ticks 0/50/100%):** raw completion `[92, 78, 62, 48, 38, 30, 25, 21]` — solid red `#e74c3c` 3px; bucket-normalized percentile `[55, 57, 54, 56, 53, 55, 54, 56]` — solid green `#27ae60` 3px.
- **Annotations:** bold red 12px "length bias, not quality" with a red arrow pointing down to the raw curve; bold green 12px "comparable across lengths" near the flat green line.
- **X labels:** `#444` 12px "30s" (left), "60 min" (right), "content duration (bands = duration buckets)" (center).
- **Legend (x = w-158):** red swatch "Raw completion %"; green swatch "Bucket percentile".
- **Bottom caption (bold blue 11px, centered):** "Compare within a duration bucket — never across a >2x length gap".

## 2. Watch Time vs Completion — The 2012 Pivot

Tags: signal (blue), gaming (orange), trade-off (orange)

- **YouTube 2012** — switched clicks to watch time; killed clickbait
- **Metric conflict** — 30-min at 40% beats 3-min at 100% on minutes
- **Padding era** — videos stretched past ~10-min mid-roll ad threshold
- **Autoplay inflation** — background and sleep-viewing minutes count as attentive
- **Blend fix** — watch time for value, normalized completion for quality

*Example:* After the pivot, the 10:01 video became a running joke — length tuned to the monetization threshold.

**How it's exploited:** Any single ranked metric becomes the creators' strategy — watch time begets padding, completion begets fragmentation.

### Visualization (canvas `c2`, 720×300)

Paired bar chart. Title (bold 14px `#1a5276`): "Same Videos, Opposite Rankings". Padding top 45, bottom 65, left 60, right 170; baseline gray axis.

- **Data (4 groups, bar pair per group, group bar width 90, gap 40):** labels `['3-min clip', '8-min video', '15-min video', '30-min essay']`; completion % `[95, 70, 50, 38]` (left half-bar, `rgba(26,82,118,0.35)`, blue 11px "N%" labels, scale max 100); minutes watched `[2.9, 5.6, 7.5, 11.4]` (right half-bar, orange `#e67e22`, bold orange 11px "Nm" labels, scale max 12).
- **Ranking annotations (bold 11px):** blue "completion ranks:  clip > essay" left; orange "minutes rank:  essay > clip" right.
- **Legend (x = w-158):** `rgba(26,82,118,0.35)` swatch "Completion %"; orange swatch "Minutes watched".
- **Bottom caption (bold red 11px, centered):** "Rank on either alone and creators pad or fragment — blend both".

## 3. Skips & Abandons as Negative Signals

Tags: signal (blue), mechanism (blue), bias (orange)

- **Spotify negatives** — Discover Weekly built on skip-within-30s labels
- **Bimodal shape** — 0-3s hard rejection spike, diffuse mass past 30s
- **Surface baseline** — discovery playlists skip more; judge locally, not globally
- **Slow-burn penalty** — equal skip weighting punishes jazz, classical, podcasts
- **Muted plays** — background play-through is not positive; skips prove listening

*Example:* Pop-song intros shrank in the streaming era — the first 3 seconds decide the skip.

**Modeling angle:** Adding skip-within-3s as an explicit negative sharpens taste models more than any extra positive.

### Visualization (canvas `c3`, 720×300)

Histogram of time before skip. Title (bold 14px `#1a5276`): "Skip Time Distribution — Rejection Spikes in the First 3 Seconds". Padding top 45, bottom 50, left 60, right 30; baseline gray axis.

- **Data (18 bins, scale max 50):** `[48, 38, 30, 12, 8, 6, 5, 4, 4, 5, 7, 8, 6, 4, 3, 2, 2, 1]`; first 3 bins red `#e74c3c`, rest `rgba(26,82,118,0.35)`.
- **Threshold:** dashed orange (`#e67e22`, 2px, dash 5/4) vertical line at bin 10 (30s), labeled bold orange 12px "30s royalty / \"counted play\" line".
- **Annotations:** bold red 13px two lines "0-3s: hard rejection" / "clean negative label" with red arrow to the spike; `#444` 12px "30s+: sampled then moved on, not rejected" right of the threshold.
- **X labels:** `#444` 12px "0s", "30s", "60s+", axis label "time before skip".
- **Bottom caption (bold blue 11px, centered):** "Bimodal shape: judge skips against the surface baseline — discovery playlists skip more by design".

## 4. Read Depth, Scroll Depth & Reading Progress

Tags: signal (blue), gaming (orange), defense (green)

- **Medium read ratio** — finished / opened outranks claps; Chartbeat sells engaged time
- **Kindle progress** — abandonment points, highlights, books never opened
- **Scroll trap** — 4s flick to bottom logs 100% depth, 0% reading
- **Short-content bias** — depth-only ranking rewards listicles over long-form
- **Depth inflation** — infinite scroll and "continue reading" pad events

*Example:* Kindle Unlimited pays per page read (KENP), spawning page-flip scams that teleport readers to the last page.

**Key point:** Gate depth by per-viewport dwell time — position alone is trivially inflated.

### Visualization (canvas `c4`, 720×300)

Filled area/line chart of read depth. Title (bold 14px `#1a5276`): "Readers Reaching Each Article Position — The Cliff After the Opening". Padding top 45, bottom 50, left 60, right 30; L-shaped gray axes with y ticks 0/50/100%.

- **Data (21 points, y max 100):** `[100, 82, 64, 55, 50, 47, 44, 42, 40, 38, 36, 34, 32, 30, 28, 26, 25, 24, 23, 22, 21]`; area under the curve filled `rgba(26,82,118,0.35)`, line solid blue `#1a5276` 3px.
- **Midpoint marker:** dashed red (1px, dash 4/4) vertical line at 50% width.
- **Annotations:** bold red 13px "headline audit: -36% by first section" and "under half reach midpoint"; bold orange 12px "fast-scroll to bottom = 100% depth, 0% reading" near the bottom.
- **X labels:** `#444` 12px "headline" (left), "midpoint" (center), "end" (right).
- (No bottom caption on this canvas.)

## 5. Replay, Rewatch & Repeat Listens

Tags: signal (blue), mechanism (blue), best practice (green)

- **YouTube heatmap** — "most replayed" spike marks the highlight automatically
- **TikTok loops** — 3x watch is a triple vote; loop rate ranks
- **Spotify repeats** — repeat listens within days beat saves and likes
- **Ambiguity** — replay then abandon = confusion; replay then share = delight
- **Reuse** — replay-heavy segments make best previews, thumbnails, chapters

*Example:* Kindle's "popular highlights" is the reread signal made visible — a book's replay heatmap.

**Key point:** Replay is position-resolved feedback — it says "this exact moment mattered," not just "it was fine."

### Visualization (canvas `c5`, 720×300)

Replay-heat histogram by content position. Title (bold 14px `#1a5276`): "Replay Heat by Position — The Spike Is the Highlight". Padding top 45, bottom 50, left 60, right 30; baseline gray axis.

- **Data (20 bins, scale max 32):** `[3, 2, 2, 3, 2, 2, 3, 4, 3, 3, 4, 6, 22, 30, 18, 5, 3, 2, 2, 1]`; bins with value > 15 (the spike at bins 12-14) orange `#e67e22`, rest `rgba(26,82,118,0.35)`.
- **Annotations:** bold orange 13px two lines "most-replayed segment" / "→ best preview / thumbnail / chapter marker" with orange arrow to the spike; `#444` 12px "baseline replay noise" over the low bins.
- **X labels:** `#444` 12px "start", "position in content", "end".
- **Bottom caption (bold blue 11px, centered):** "Disambiguate by what follows: replay→abandon = confusion, replay→share = delight".

## 6. Drop-off Curves & Retention Anatomy

Tags: signal (blue), rule of thumb (blue), failure mode (red)

- **Per-second curves** — YouTube Studio retention graphs; editors re-cut around dips
- **Baseline** — losing ~1/3 in first 30s is typical, not alarming
- **Flat tail** — committed core audience worth serving sequels
- **Same mean, opposite story** — 50% average hides who left where
- **Formula hooks** — creators optimize first 15s: cold opens, "wait for it"

*Example:* Creators A/B their opening 15 seconds against the retention graph — the highest-leverage edit.

**Failure mode:** Averaging the curve destroys the where-losses-happen information that makes it actionable.

### Visualization (canvas `c6`, 720×300)

Annotated retention curve. Title (bold 14px `#1a5276`): "Retention Curve Anatomy — Where They Leave Tells You What Broke". Padding top 45, bottom 50, left 60, right 30; L-shaped gray axes with y ticks 0/50/100%.

- **Data (20 points, y max 100):** `[100, 78, 68, 65, 63, 62, 61, 60, 54, 49, 53, 55, 54, 53, 52, 51, 50, 48, 38, 25]` — solid blue `#1a5276` 3px.
- **Zone annotations:** bold red 13px "hook cliff (0-30s)" + 11px "~1/3 loss is typical" (top-left); bold orange 13px "mid dip: boring segment" + 11px "re-cut around this" (mid); bold green 13px "flat = committed core" + 11px "audience worth sequels" (right); `#444` 12px "credits (ignore)" (far right).
- **X labels:** `#444` 12px "0:00", "time in video", "end".
- **Bottom caption (bold blue 11px, centered):** "Two curves can share a 50% mean and mean opposite things — read the shape, not the average".

## 7. Binge Patterns & Consumption Sequences

Tags: signal (blue), mechanism (blue), bias (orange)

- **Ep1→ep2 gate** — filters hardest; survivors carry through at rising rates
- **Netflix autoplay** — countdowns built around the hook-episode transition
- **Sessionize** — stitch plays with ~30-min gap; measure carryover per transition
- **Autoplay inflation** — sleep-viewing counts as bingeing unless interaction required
- **Sequence models** — RNNs/transformers over ordered history beat bag-of-items

*Example:* Netflix's "are you still watching?" prompt is a data-quality checkpoint disguised as courtesy.

**Watch out:** Uncorrected autoplay makes every series look bingeable — require an interaction or cap credited episodes.

### Visualization (canvas `c7`, 720×300)

Bar chart of episode carryover plus a session-stitching timeline strip. Title (bold 14px `#1a5276`): "P(Start Next Episode | Finished This One) — Ep1→Ep2 Filters Hardest". Padding top 45, bottom 110, left 60, right 30.

- **Bars (y max 100):** transitions `['ep1→ep2', 'ep2→ep3', 'ep3→ep4', 'ep4→ep5', 'ep5→ep6', 'ep6→ep7']`, carryover `[42, 68, 79, 84, 87, 89]` (%); first bar red `#e74c3c` with bold red label, rest `rgba(26,82,118,0.35)` with `#444` labels.
- **Annotations:** bold red 12px two lines "the gate: selection," / "not persuasion" over the first bar; bold green 12px "survivors carry through at rising rates" over the rest.
- **Session-stitching strip (bottom):** bold blue 11px label "Session stitching:"; three adjacent 55px play blocks (fill `rgba(26,82,118,0.35)`, blue stroke) labeled "session 1 (binge of 3)"; a large gap labeled bold orange 10px "gap > 30 min → new session"; two more blocks (fill `rgba(39,174,96,0.35)`, green stroke) labeled "session 2".
- **Bottom caption (bold blue 11px, centered):** "Correct for autoplay: require an interaction or cap credited episodes per session".

## 8. The Aspiration Gap: Behavior vs Stated Preference

Tags: signal (blue), bias (orange), trade-off (orange)

- **Queue vs play** — behavior beats stated preference for next-play prediction
- **Netflix DVD era** — prestige queues sat unwatched; mainstream titles cycled
- **Ratings bias** — aspirational stars pushed Netflix to thumbs, then behavior
- **Catalog reshaping** — behavior ranking thins the aspirational tail
- **Fix** — use stated preference as diversity prior, not ranking signal

*Example:* The prestige drama queued for a year while comedies cycled weekly — queues measure identity, not demand.

**Key point:** Behavior predicts the next play; aspiration predicts what the subscription is for — retention needs both.

### Visualization (canvas `c8`, 720×300)

Paired bar chart by genre. Title (bold 14px `#1a5276`): "Share of Queue vs Share of Watch Time — Who They Want to Be vs Who They Are". Padding top 45, bottom 65, left 60, right 170; baseline gray axis.

- **Data (y max 60; 4 groups, bar width 90, gap 40):** genres `['Documentary', 'Foreign film', 'Drama', 'Reality TV']`; % of queue `[34, 22, 28, 16]` (left half-bar, `rgba(26,82,118,0.35)`, blue 11px labels); % of watch time `[8, 5, 32, 55]` (right half-bar, green `#27ae60`, bold green 11px labels).
- **Annotations (bold 11px):** red "queued, unwatched" left; green "watched, rarely queued" right.
- **Legend (x = w-158):** `rgba(26,82,118,0.35)` swatch "% of queue"; green swatch "% of watch time".
- **Bottom caption (bold blue 11px, centered):** "Behavior predicts the next play; use stated preference as a diversity prior, not a ranking signal".

## 9. Taste Profiling, Diversity & Rabbit Holes

Tags: signal (blue), defense (green), trade-off (orange)

- **Taste embedding** — word2vec over playlists; user = completions minus skips
- **Diversity metric** — category entropy per user per week; derivative matters most
- **Rabbit hole** — nearest-neighbor exploitation maximizes today, starves next month
- **Confound** — condition on session count before blaming narrowing for churn
- **Early warning** — falling diversity triggers exploration boosts weeks ahead

*Example:* Spotify Wrapped is the taste embedding behind Discover Weekly, shown back as identity.

**Modeling angle:** The weighted-consumption embedding powers recommendations; its entropy derivative powers retention alarms.

### Visualization (canvas `c9`, 720×300)

Two-series line chart over weeks. Title (bold 14px `#1a5276`): "Category Diversity Over Weeks — Narrowing Diet Precedes Churn". Padding top 45, bottom 50, left 60, right 170; L-shaped gray axes; rotated y label (`#888` 11px) "category entropy".

- **Data (10 weekly points, y max 70):** retained users `[62, 60, 63, 61, 64, 62, 63, 61, 62, 63]` — solid green `#27ae60` 3px; churned by wk 12 `[58, 55, 50, 44, 37, 30, 24, 18, 13, 10]` — solid red `#e74c3c` 3px.
- **Alarm marker:** dashed orange (2px, dash 4/4) vertical line at week 4, labeled bold orange 12px two lines "trend flag: boost exploration here," / "weeks before watch time drops".
- **Endpoint:** red 5px dot at the final churned point labeled bold 11px "one genre left".
- **X labels:** `#444` 12px "week 1" (left), "week 10" (right).
- **Legend (x = w-158):** green swatch "Retained users"; red swatch "Churned by wk 12".
- **Bottom caption (bold blue 11px, centered):** "Watch the derivative, not the level — and condition on session count before blaming narrowing".

## 10. Gaming, Fraud & Countermeasures

Tags: abuse (red), gaming (orange), defense (green)

- **Threshold camping** — bots loop 31-35s, just past Spotify's 30s royalty line
- **Metric-shaped content** — white-noise catalogs chopped into 31-second tracks
- **Playlist laundering** — fake playlists funnel bot streams as organic discovery
- **View farms** — credit only foreground + audible + interaction plays
- **Fraud signature** — spikes, uniformity, 24/7 flat activity vs organic decay

*Example:* Spotify has purged billions of artificial streams — an arms race around the 30-second rule.

**Design lesson:** Publish a threshold and you publish the exploit — score the full consumption curve, not one cutoff.

### Visualization (canvas `c10`, 720×300)

Play-duration histogram with a bot spike. Title (bold 14px `#1a5276`): "Play Duration Histogram — The Bot Spike Just Past the 30s Royalty Line". Padding top 45, bottom 50, left 60, right 30; baseline gray axis.

- **Data (22 bins, scale max 50):** `[20, 14, 10, 8, 7, 6, 6, 5, 5, 4, 4, 46, 38, 5, 4, 4, 3, 3, 3, 2, 2, 2]`; bins 11-12 (31-35s bot spike) red `#e74c3c`, rest `rgba(26,82,118,0.35)`.
- **Threshold:** dashed orange (2px, dash 5/4) vertical line just before bin 11, labeled bold orange 12px right-aligned "30s payout threshold".
- **Annotations:** bold red 13px "31-35s: threshold camping" + 11px "bots loop just past the payout line"; bold green 12px two lines "organic tail: smooth decay," / "diverse devices and geography"; `#444` 12px "sampling spike (organic)" over the early bins.
- **X labels:** `#444` 12px "0s", "30s", "play duration", "3 min+".
- **Bottom caption (bold blue 11px, centered):** "Score the full curve, not the cutoff — spikes, uniformity, and 24/7 flat activity are the fraud signature".

## Regeneration instructions

- **Layout:** most-powerful-signals compact style. One `.card-section` per topic: `<h2>` (numbered "N. Title", 1.3rem `#1a5276`, 2px solid `#2980b9` bottom border), then a `table.layout` with one row — left `td.text-col` (45%) holding `.tags` pill row, a `<ul>` of labeled bullets (`<li><b>Label</b> — text`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) holding one `<canvas width="720" height="300">` scaled to `width: 100%` with 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `<strong>` lead-in label.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem with `li b` in `#1a5276`; `.example` italic `#555` 0.9rem. No nav bar, no back/home links.
- **Canvas:** shared `setup(id)` helper — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates, logical size 720×300.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#444`/`#555`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
