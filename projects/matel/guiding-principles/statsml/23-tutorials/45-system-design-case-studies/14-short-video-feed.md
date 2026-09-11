# Short-Video Feed

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Short-Video Feed

**Subtitle:** An interest-ranked feed drops the follow graph entirely — every video is a candidate for every viewer, and a three-stage recommender (candidates, ranking, re-rank rules) decides distribution instead of follower counts

## The Feed That Doesn't Care Who You Follow

**Tags:** `core idea` (blue), `no follow graph` (green), `recommendation-first` (orange)

- **The social feed** — a follow-graph timeline is built from who you follow: only their posts are candidates
- **The fan-out** — those systems precompute by pushing each new post into every follower's stored timeline
- **The flip** — a For-You feed has no follow constraint: every video in the corpus is a candidate for anyone
- **The consequence** — each refresh is a ranking decision over a billion items, not a merge of 300 inboxes
- **The name** — this is a recommendation-first feed: the recommender, not the graph, decides who sees what

*Example (italic):* On day one you follow nobody, yet the feed is full — the system ranks the whole corpus against your first few watches instead of waiting for a follow list.

**Key point:** Removing the follow graph removes the fan-out problem and replaces it with a harder one — picking 8 videos out of a billion for every refresh, for every viewer.

### Visualization (canvas `c1`, 720×300)

Two-lane flow diagram: a follow-graph feed (candidates limited to followed accounts' posts) vs a recommendation-first feed (candidates are the entire corpus).

- **Title (bold 15px, `#1a5276`, top center):** "Two Feed Architectures: Follow Graph vs Recommender".
- **Row 1 (y=95), label 12px `#444` at x=20:** "social feed"; blue `#2a78d6` rounded box at x=150 labeled "you follow 300 accounts" (12px), 3px arrow to a blue box at x=360 labeled "their new posts: ~600/day", 3px arrow to a blue box at x=560 labeled "merge by time → feed".
- **Row 2 (y=205), label:** "for-you feed"; orange `#d95926` rounded box at x=150 labeled "entire corpus: 1B videos", 3px arrow to a green `#008300` box at x=360 labeled "recommender ranks", 3px arrow to a green box at x=560 labeled "8 videos chosen for you".
- **Box style:** 150–170px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "candidate pool: ~600 posts vs 1,000,000,000 videos".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## One Billion Videos Narrowed to Eight

**Tags:** `worked example` (blue), `candidate → rank → re-rank` (green)

- **The funnel** — the standard public recommender shape: candidate generation, ranking, then re-ranking rules
- **Stage 1** — candidate generation uses embeddings/similarity search to cut 1,000,000,000 videos to ~500
- **Stage 2** — a heavier ranking model scores the 500 on predicted watch time and likes, keeping the top 50
- **Stage 3** — re-ranking rules enforce diversity, freshness, and policy, leaving 8 videos for the next batch
- **Hand-check** — at 0.1 ms per candidate, the heavy model scores 500 in 50 ms; 1B would take ~28 hours
- **The loop** — every watch, like, and skip streams back to update the models continuously

*Example (italic):* One refresh runs 1,000,000,000 → 500 (embeddings) → 50 (ranking model) → 8 shown, and the whole funnel finishes in well under a second.

**Key point:** The funnel exists because model cost and corpus size trade off — cheap similarity search narrows billions to hundreds so the expensive model only ever scores hundreds.

### Visualization (canvas `c2`, 720×300)

Horizontal funnel bar chart: items surviving each stage of the recommender, from full corpus to the 8 videos shown; log-feel via hardcoded pixel widths.

- **Title (bold 15px, `#1a5276`, top center):** "The Recommender Funnel: 1,000,000,000 → 500 → 50 → 8".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "corpus (every video is a candidate)": blue `#2a78d6` bar width 420, 11px label "1,000,000,000"
  - "candidate generation — embeddings": aqua `#199e70` bar width 150, 11px label "500"
  - "ranking model — watch-time score": orange `#d95926` bar width 78, 11px label "top 50"
  - "re-rank rules — diversity, freshness, policy": green `#008300` bar width 34, bold 12px green label "8 shown"
- **Bar style:** 14px tall, corpus bar fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, other bars solid.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "heavy model scores 500, never 1B — 50 ms, not 28 hours".
- **Caption (12px `#444`, bottom right):** "counts illustrative, log-feel widths".

## New Videos Get an Audition, Not an Audience

**Tags:** `where it's used` (blue), `cold start` (green), `exploration` (orange)

- **The cold start** — a brand-new video has zero engagement history, so no model can score it from data
- **The probe** — exploration solves it: show the new video to a small probe audience of ~300 viewers
- **The signal** — completion rate, likes, and rewatches stream back within minutes of the probe
- **The ladder** — beat the engagement bar and the audience is promoted roughly 10× per round
- **Graph-free virality** — follower count never enters the score, so a nobody's video can reach millions

*Example (italic):* A video from an account with 12 followers passes its 300-viewer probe on completion rate and climbs four promotion rounds to 2,000,000 views.

**Key point:** Cold start is solved by spending a sliver of traffic auditioning every new video — winners are promoted round by round, and distribution is decided by the recommender, not the follower count.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: audience size per exploration round for one winning video, with a marker showing where most videos stop; log-feel via hardcoded pixel heights.

- **Title (bold 15px, `#1a5276`, top center):** "Exploration Ladder: 300 Probe Viewers to 2,000,000 Views".
- **Axes:** baseline 2px `#999` at y=245 from x=60 to x=660; five bars width 80 centered at x = 130, 240, 350, 460, 570; log-feel achieved by hardcoded pixel heights, not a real log axis.
- **Bars:** rounds `["probe", "round 2", "round 3", "round 4", "round 5"]` (12px `#444` labels under each bar), audiences `[300, 3000, 30000, 300000, 2000000]`, pixel heights `[18, 60, 102, 144, 178]`; green fill `rgba(0,131,0,0.30)` with 2px `#008300` border.
- **Value labels:** bold 12px `#2c3e50` above each bar: "300", "3,000", "30,000", "300,000", "2,000,000 views".
- **Stop marker:** horizontal dashed `#e74c3c` (dash 4/3) line at the probe bar's top (y=227) from x=60 to x=300, bold 12px red label "most videos stop here — below the engagement bar" just above it.
- **Annotation (bold 13px violet `#4a3aa7`, centered at x=300, y=45):** "creator has 12 followers — the ranker decides reach, not the graph".
- **Caption (12px `#444`, bottom right):** "audiences and ~10× promotions illustrative".

## The Video Pipe Is Commodity, the Ranker Is the Product

**Tags:** `common mistake` (red), `infrastructure` (orange)

- **The trap** — spending the whole design on video storage and streaming, the parts every video site shares
- **The CDN** — bytes are served from edge caches like any video product; delivery is a solved, buyable problem
- **No fan-out** — there is no per-follower timeline service at all; nothing is precomputed per follow edge
- **The loop** — watch time, likes, and skips stream back and update the ranking models continuously
- **The lesson** — the differentiator is the candidate → rank → re-rank loop; the recommender is the product

*Example (italic):* A design review that details transcoding tiers and CDN regions but hand-waves "then ML picks videos" has skipped the only part that isn't off-the-shelf.

**Common mistake:** Treating this as a video-serving problem. The video path is commodity CDN work; the system that decides which 8 of a billion videos to show — and learns from every skip — is the actual product.

### Visualization (canvas `c4`, 720×300)

Two-lane flow diagram: the commodity video path (upload to player via CDN) vs the product path (engagement signals feeding continuous model updates back into the next feed).

- **Title (bold 15px, `#1a5276`, top center):** "Commodity Path vs the Product: Where the Design Effort Goes".
- **Row 1 (y=95), label 12px `#444` at x=20:** "video path (commodity)"; blue `#2a78d6` rounded box at x=150 labeled "upload + transcode" (12px), 3px arrow to a blue box at x=360 labeled "CDN edge cache", 3px arrow to a blue box at x=560 labeled "player"; 12px `#6b7280` note under the row: "same as any video site".
- **Row 2 (y=205), label:** "ranking loop (the product)"; green `#008300` rounded box at x=150 labeled "signals: watch time, likes, skips", 3px arrow to a green box at x=360 labeled "continuous model updates", 3px arrow to a green box at x=560 labeled "next feed request".
- **Box style:** 150–175px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the moat is the ranking loop, not the video pipe".
- **Caption (12px `#444`, bottom right):** "schematic — standard published recommender architecture".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); corpus size, funnel counts (1,000,000,000 / 500 / 50 / 8), probe audiences (300 / 3,000 / 30,000 / 300,000 / 2,000,000), and per-candidate cost are invented and labeled illustrative; the arithmetic hand-checks are exact given the stated rates (500 × 0.1 ms = 50 ms; 1,000,000,000 × 0.1 ms = 100,000 s ≈ 28 hours). Frame everything as the standard published recommender-system architecture — candidate generation, ranking, re-ranking, exploration for cold start — with no claims about the real company's undisclosed internals.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
