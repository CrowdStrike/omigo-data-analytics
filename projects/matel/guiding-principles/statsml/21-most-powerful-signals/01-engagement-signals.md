# Engagement Signals — Likes, Shares, Comments, Saves

**Page type:** detail page (ten card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Engagement Signals — Likes, Shares, Comments, Saves

**Subtitle:** The explicit actions users take that reveal intent and preference — how feeds capture, weight, and exploit them for recommendation and ranking

## The Engagement Hierarchy: Cost of Action = Strength of Signal

**Tags:** `signal` (blue), `rule of thumb` (blue)

- **Effort filter** — costly actions are hard to fake or perform accidentally.
- **Cost ordering** — view < like < comment < save < share < author-engaged reply.
- **Twitter weights** — reply ~27x a like; author-engaged reply ~150x.
- **Reciprocal actions** — strongest positives; prove a two-way relationship.
- **Coverage trade-off** — costly signals rare, so models lean on noisy views/likes.

*Example:* Twitter's 2023 open-sourced ranker read like a price list of effort: like ~0.5, retweet ~1, reply ~13.5.

**Key point:** Rank signals by what the action cost the user — effort is the built-in spam filter.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart of engagement weights relative to a like, on a sqrt scale, with a vertical "cost of action" arrow at the right.

- **Title (bold 14px `#1a5276`, top center):** "The Engagement Hierarchy: Weight Relative to a Like = 1x".
- **Data:** labels `[View, Like, Comment, Save, Share, Author-engaged reply]`, values `[0.1, 1, 8, 15, 30, 60]`x; max 60.
- **Bars:** rows start y=44, row height 34, bar height 22; labels right-aligned 11px `#1a5276` at x=175, bar width = sqrt(value/60) × 400 (min 3px). First three bars (low-cost) fill `rgba(26,82,118,0.35)` stroke `#1a5276`; last three (high-cost: Save, Share, Author-engaged reply) fill `#27ae60` at alpha 0.7 stroke `#27ae60`. Bold 11px value labels ("0.1x" … "60x") after each bar, green for high-cost rows, `#555` otherwise.
- **Effort arrow:** orange `#e67e22` vertical arrow at x=660 pointing upward, labeled bold 10px "cost of" / "action".
- **Caption (bold 11px `#1a5276`, bottom center):** "Costly actions (green) are rare but precise; cheap actions provide volume, not truth".

## Explicit vs Implicit: Sparse but High-Precision

**Tags:** `signal` (blue), `bias` (orange)

- **Explicit vs implicit** — deliberate labels vs ambiguous behavioral exhaust.
- **Standard architecture** — train on implicit, anchor with sparse explicit.
- **YouTube** — watch time anchored by surveys and thumbs.
- **90-9-1 pattern** — 90% consume, 9% react, 1% create.
- **Sparse actions** — only ~1–5% of viewers act explicitly per item.

*Example:* 100,000 views and 900 likes: high-precision labels from under 1% of a skewed audience.

**Failure mode:** Models trained on the reacting minority steer the feed for the silent majority.

### Visualization (canvas `c2`, 720×300)

Two-panel chart split by a dashed divider at midline: a 90-9-1 pyramid on the left, volume-vs-precision paired bars on the right.

- **Title (bold 14px `#1a5276`, top center):** "Who Emits Explicit Signals — and How Precise They Are".
- **Left panel header (bold 12px `#1a5276`, centered at 180,48):** "The 90-9-1 pattern".
- **Pyramid layers** (centered at x=180, layer height 46, gap 6, all stroked `#1a5276` width 1, 11px `#2c3e50` labels):
  - "1% create / comment" — width 60, color `#e74c3c` (alpha 0.75).
  - "9% react lightly" — width 140, color `#e67e22` (alpha 0.75).
  - "90% only consume (silent)" — width 280, color `rgba(26,82,118,0.35)`.
  - Bold 10px `#e74c3c` annotation above: "explicit signals come from up here".
- **Right panel header (bold 12px `#1a5276`):** "Volume vs precision (per 1,000 impressions)".
- **Paired bars:** groups `[Views/dwell, Likes, Comments, Saves]`; volume `[1000, 40, 8, 5]` plotted on a log10 scale (blue `rgba(26,82,118,0.35)`, left half of each 70px group); precision `[30, 70, 85, 92]`% linear (green `#27ae60` alpha 0.75, right half). Chart top y=62, height 150; 10px group labels beneath.
- **Legend:** blue swatch "volume (log)", green swatch "precision %".
- **Caption (bold 11px `#e67e22`, bottom center):** "Sparse-but-precise explicit labels calibrate abundant-but-ambiguous implicit signals".

## Likes: Cheap, Abundant, and Slowly Debasing

**Tags:** `signal` (blue), `gaming` (orange)

- **History** — Like launched 2009; Reactions added valence in 2016.
- **Angry weaponized** — ~5x reaction weight cut to zero in 2020.
- **Like inflation** — reciprocity and politeness drift signal toward mere receipt.
- **YouTube 2021** — dislikes hidden publicly, kept as ranking input.
- **Performative** — public likes reflect what users want seen.

*Example:* Facebook found angry reactions clustered on misinformation and toxicity, so the 5x weight was zeroed.

**How it's exploited:** Likes serve as high-volume calibration anchoring the sparse, expensive signals above them.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline (line at y=150, `#95a5a6` width 2) with four event callout boxes alternating above/below.

- **Title (bold 14px `#1a5276`, top center):** "The Like Signal Over Time: Enriched, Weaponized, Rolled Back".
- **Events** (each a 6px-radius dot on the line, a colored connector, and a 150×58 box with `#f8f9fa` fill and 2px colored stroke; bold 11px "year — title" line, 10px `#555` subtitle):
  - x=100, above: "2009 — Like button" / "1 bit of approval" — `#1a5276`.
  - x=270, below: "2016 — Reactions" / "valence added, ~5x weight" — `#27ae60`.
  - x=450, above: "2020 — Angry zeroed" / "weaponized by divisive posts" — `#e74c3c`.
  - x=620, below: "2021 — Dislikes hidden" / "YouTube: display ≠ signal" — `#e67e22`.
- **Caption (bold 11px `#1a5276`, bottom center):** "Cheap public signals inflate and get gamed; platforms keep the data but adjust the weights".

## Shares and Reposts: Spending Social Capital

**Tags:** `signal` (blue), `failure mode` (red)

- **Strongest routine signal** — a share spends social capital and creates distribution.
- **Share types** — repost is broadcast; DM share is private recommendation.
- **Quote-tweets** — often mockery; same action, opposite valence.
- **Platform weights** — Instagram ranks Reels on sends-per-reach; TikTok weights shares heavily.
- **WhatsApp limits** — forwards capped at 5 chats, then 1.

*Example:* On X, 2,000 quote-tweets vs 300 likes means mockery — a naive ranker amplifies the pile-on.

**Failure mode:** Share counts conflate endorsement with outrage — "shareably wrong" content harvests the strongest signal.

### Visualization (canvas `c4`, 720×300)

Two-panel chart split by a dashed divider: a share fan-out diagram on the left, intent-precision bars on the right.

- **Title (bold 14px `#1a5276`, top center):** "Shares Create Distribution — but Not All Shares Are Endorsements".
- **Left panel:** a "post" box (70×44 at x≈80, y=150, fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 2) with three fan-out lines to dots at x=210, each with a bold 10px label and a 9px `#777` sub-label:
  - y=70, green `#27ae60`: "repost → all followers" / "broadcast endorsement".
  - y=150, green `#27ae60`: "DM share → one friend" / "highest intent per event".
  - y=230, orange `#e67e22`: "quote-tweet → followers" / "agreement OR mockery (±)".
- **Right panel header (bold 12px `#1a5276`):** "Positive-intent precision by share type".
- **Bars:** types `[DM share, Repost, Quote-tweet]`, precision `[95, 85, 50]`%, colors `[#27ae60, #27ae60, #e67e22]`; bar width = precision% × 220px, height 24, alpha 0.65 fill with matching stroke; 11px type labels above each bar, bold 11px "~95%" etc after each bar.
- **Annotation (bold 10px `#e74c3c`):** "the \"ratio\": 2,000 quote-tweets + 300 likes = mockery," / "read by a naive ranker as viral approval".
- **Caption (bold 11px `#1a5276`, bottom center):** "A share spends the sharer’s social capital — unless it spends it against you".

## Comments: High Weight, Rage-Prone

**Tags:** `signal` (blue), `gaming` (orange), `failure mode` (red)

- **MSI weight** — Facebook 2018 scored comments 15–30x a like.
- **Compounding** — each reply notifies participants and pulls them back.
- **Rage engagement** — arguments look like enthusiasm to a counter.
- **Quality filters** — length thresholds, author-reply weighting, emoji-only discounts.
- **Engagement bait** — "comment YES", deliberate errors, tag-a-friend chains.

*Example:* Facebook's MSI change boosted divisive content — nothing generates 30x-weighted comments like an argument.

**Failure mode:** Weighting comments without modeling sentiment turns the feed into an outrage auction.

### Visualization (canvas `c5`, 720×300)

Two-panel chart split by a dashed divider: comment-rate columns on the left, weighted score-contribution bars on the right.

- **Title (bold 14px `#1a5276`, top center):** "Why Weighted Comments Boost Outrage".
- **Data:** tones `[Neutral, Opinionated, Outrage]`, comment rate per 1k impressions `[2, 6, 18]`, comment weight 25; colors `[rgba(26,82,118,0.35), #e67e22, #e74c3c]` (alpha 0.7 on the orange/red).
- **Left panel (header "Comments per 1k impressions"):** vertical bars width 70, gap 30, starting x=60, chart top y=62, height 160, scaled to max 18; bold 11px value labels above bars, 10px tone labels below.
- **Right panel (header "Score contribution = rate × 25 (comment weight)"):** horizontal bars at x = mid+40, width up to 240 scaled to max score 450; scores `2×25=50`, `6×25=150`, `18×25=450`; 10px tone labels above, bold 11px score values after (Outrage value in red).
- **Caption (bold 11px `#e74c3c`, bottom center):** "The counter cannot tell a flame war from enthusiasm — outrage wins the sum".

## Saves and Bookmarks: The Honest Private Signal

**Tags:** `signal` (blue), `trade-off` (orange)

- **Future intent** — closest engagement gets to a purchase signal.
- **Private, honest** — no audience, no public count, nearly ungameable.
- **Content split** — recipes/tutorials/workouts get saved; memes get liked.
- **Instagram** — publicly confirmed saves as top feed/Reels signal.
- **Pinterest** — entire platform ranks on the save (pin).

*Example:* A workout video: 3 likes vs 9 saves per 100 viewers — creators now say "save this for later".

**Trade-off:** Save-optimized feeds drift toward utility content and starve entertainment — honest but narrow.

### Visualization (canvas `c6`, 720×300)

Grouped bar chart: likes vs saves per 100 viewers across five content types, with axis lines.

- **Title (bold 14px `#1a5276`, top center):** "Public vs Private Approval by Content Type (per 100 viewers)".
- **Data:** groups `[Meme, Hot take, Recipe, Tutorial, Workout]`; likes `[12, 9, 4, 3, 4]`; saves `[0.5, 0.3, 8, 9, 7]`; y max 12.
- **Bars:** group width 90, gap 24, starting x=55, chart top y=50, height 175; likes = left half, fill `rgba(26,82,118,0.35)` stroke `#1a5276`; saves = right half, fill `#27ae60` alpha 0.75 stroke `#27ae60` (min height 2px). Gray `#95a5a6` L-shaped axis; 11px group labels beneath.
- **Divergence labels (bold 10px `#e67e22`):** "liked, not saved" over the Meme/Hot-take side; "saved, not liked" over the Recipe/Tutorial side.
- **Legend (top right):** blue swatch "likes (public)", green swatch "saves (private)".
- **Caption (bold 11px `#1a5276`, bottom center):** "Saves have no audience, so they measure actual utility, not performance".

## Negative Signals: Hide, Mute, Report — the Strongest Labels

**Tags:** `signal` (blue), `abuse` (red)

- **Report weight** — Twitter: -369 vs +0.5 like, ~700x magnitude.
- **"See less like this"** — suppresses whole content classes per user.
- **Mute/unfollow** — prune the graph, not just the item.
- **Silent churn** — most disliked content never gets a negative label.
- **Buried controls** — negative feedback hidden two taps deep, starving the signal.

*Example:* TikTok's long-press "Not interested" suppresses clusters that hundreds of passive skips failed to dislodge.

**Abuse vector:** Coordinated mass-reporting weaponizes the strongest signal to take down legitimate content.

### Visualization (canvas `c7`, 720×300)

Diverging bar chart around a zero line (zero at 35% of chart height), sqrt-scaled magnitudes.

- **Title (bold 14px `#1a5276`, top center):** "Ranking Weight per Event (Twitter open-sourced ranker, scaled)".
- **Data:** labels `[Like, Retweet, Reply, Neg. feedback, Report]`, values `[0.5, 1, 13.5, -74, -369]`; max abs 369; bar heights = sqrt(|value|/369) × 60% of chart height (min 3px for positives).
- **Bars:** width 90, evenly spaced across padding left 60/right 30; positives green `#27ae60` alpha 0.7 above the zero line with bold "+0.5", "+1", "+13.5" labels; negatives red `#e74c3c` alpha 0.7 below with "-74", "-369" labels. Zero line gray `#95a5a6` labeled "0". 11px `#2c3e50` category labels at the bottom.
- **Magnitude annotation:** dashed orange `#e67e22` line (dash 3/3) spanning from Like to Report above the zero line, labeled bold 11px "one report ≈ 700x a like in magnitude".
- **Caption (bold 11px `#1a5276`, bottom center):** "Rare, costly rejection labels dominate the score — and are the prime target for brigading".

## Signal Weighting: The Weighted-Sum Feed Score

**Tags:** `mechanism` (blue), `trade-off` (orange)

- **Formula** — score ≈ Σ wᵢ · P(actionᵢ), hand-tuned weight vector.
- **Known weights** — MSI comment 15–30x; Twitter combined ~10 probabilities.
- **Editorial math** — editing one weight changes what billions see.
- **Negative brakes** — P(hide), P(report) subtract inside the same sum.
- **Objective** — tuned for engagement lift, not user welfare.

*Example:* A share-heavy post beats a like-heavy one; predicted hides erase "earned" score.

**Key point:** "The algorithm decided" means a human-edited weight vector decided — the policy lives in the weights.

### Visualization (canvas `c8`, 720×300)

Two stacked horizontal composition bars comparing feed scores of two posts.

- **Title (bold 14px `#1a5276`, top center):** "score ≈ w₁·P(like) + w₂·P(comment) + w₃·P(share) + w₄·P(hide)".
- **Segment colors:** like `rgba(26,82,118,0.35)`, comment `#1a5276`, share `#27ae60`, hide (negative) `#e74c3c` drawn at alpha 0.35 with red stroke and a bold 10px "-60" label inside.
- **Post A (share-heavy), bar y=80:** segments `[like 80, comment 140, share 260, hide 0]`; total 480; result label bold 13px green "score 480 — wins".
- **Post B (like-heavy, some hides), bar y=165:** segments `[like 220, comment 90, share 30, hide -60]`; total 280; result label bold 13px red "score 280 — loses".
- Bars are 36px tall, segment width = |value| × 0.85, starting x=40; bold 12px `#2c3e50` post labels above each bar.
- **Legend (y=235):** swatches for "like", "comment", "share", "hide (negative)".
- **Caption (bold 11px `#e67e22`, bottom center):** "The weight vector is editorial policy: edit w(comment) and a billion feeds change".

## Feedback Loops and Goodhart: Optimizing the Signal Corrupts It

**Tags:** `mechanism` (blue), `failure mode` (red), `defense` (green)

- **Rich-get-richer** — first-hour luck compounds into orders-of-magnitude gaps.
- **Institutionalized loop** — TikTok staged rollouts, Reddit early votes, YouTube first 48h.
- **Exposure bias** — "no engagement" mostly means "never shown".
- **Goodhart** — engagement climbs while surveyed satisfaction falls.
- **Fixes** — exploration traffic, inverse-propensity weighting, bait classifiers.

*Example:* Bait classifiers demote "tag a friend" phrasing; creators adapt — a permanent arms race.

**Root cause:** When a measure becomes a target it ceases to be a good measure.

### Visualization (canvas `c9`, 720×300)

Two line-chart panels split by a dashed divider, each 280×150 with gray L-axes (top y=62).

- **Title (bold 14px `#1a5276`, top center):** "The Loop Compounds Luck — Then Goodhart Detaches the Metric".
- **Left panel ("Two equal-quality items"):** two multiplicative-growth curves over 10 points, values capped at 100 (plotted on a 0–100 scale):
  - "lucky first hour" — blue `#1a5276`, starts at 2 and multiplies by 1.55 each step (2, 3.1, 4.8, 7.4, 11.5, 17.9, 27.7, 42.9, 66.5, capped 100).
  - "same quality, no luck" — orange `#e67e22`, starts at 1.8 and multiplies by 1.12 each step (1.8 … ≈5.0).
  - X-label 10px `#777`: "ranking cycles — early velocity is destiny".
- **Right panel ("After optimizing for engagement"):** two 8-point series:
  - "engagement" — orange `#e67e22`: `[40, 46, 54, 63, 72, 80, 86, 91]`.
  - "satisfaction" — red `#e74c3c`: `[70, 69, 66, 61, 55, 49, 44, 40]`.
  - X-label: "quarters after launch — the proxy detaches".
- Lines width 2.5 with bold 10px series name labels near the line ends.
- **Caption (bold 11px `#e74c3c`, bottom center):** "When the measure became the target, it stopped measuring value".

## Gaming, Fraud, and the Ladder of Proxies

**Tags:** `gaming` (orange), `defense` (green)

- **Attack surface** — bought likes, engagement pods, bots buy distribution via fake early velocity.
- **Signatures** — burst without tail, no-consumption accounts, reciprocal graphs.
- **Countermeasures** — engager-credibility weighting, low-trust discounts, fake-account purges.
- **Proxy ladder** — YouTube: clicks → watch time (2012) → valued watch time.
- **Residual gaps** — surveys inherit sparsity; regret signals penalize bait.

*Example:* A 50-creator pod's 10-minute velocity spike gets zeroed once the burst detector sees the closed engagement graph.

**Key point:** Once a costly honest action becomes the optimization target, the ecosystem manufactures cheap imitations of it.

### Visualization (canvas `c10`, 720×300)

Two-panel chart split by a dashed divider: fake vs organic engagement curves on the left, a vertical proxy ladder on the right.

- **Title (bold 14px `#1a5276`, top center):** "Fake Velocity Has a Signature — and the Objective Keeps Climbing".
- **Left panel ("Engagement per hour after posting"):** panel 280×150 at x=45, top y=62, gray L-axis; two 12-point lines width 2.5 on a 0–100 scale:
  - pod/bought — red `#e74c3c`: `[95, 60, 10, 4, 2, 1, 1, 1, 0, 0, 0, 0]`, labeled bold 10px "pod/bought: burst, no tail".
  - organic — green `#27ae60`: `[5, 10, 16, 24, 33, 42, 50, 55, 57, 55, 50, 45]`, labeled "organic: slow rise, sustained".
  - X-label 10px `#777`: "hours since posting".
- **Right panel ("The proxy ladder (YouTube objective)"):** three 245×44 rungs (fill `#f8f9fa`, 2px colored stroke, bold 11px name + 10px `#555` note), listed top to bottom, with blue `#1a5276` upward arrows between rungs:
  - "Valued time + surveys" / "sparse labels, still evolving" — `#27ae60`.
  - "Watch time (2012)" / "gamed by padded, long videos" — `#e67e22`.
  - "Clicks (early era)" / "gamed by bait thumbnails" — `#e74c3c`.
- **Caption (bold 11px `#1a5276`, bottom center):** "Each proxy is abandoned once optimizing it detaches it from real value".

## Regeneration instructions

- **Layout:** ten `.card-section` blocks, each an h2 with blue bottom border followed by a `table.layout` with one row: left `td.text-col` (45%) holding `.tags` pills, a `ul` of labeled bullets, an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) holding one 720×300 canvas (`c1`–`c10`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `ul` 0.92rem; `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `strong` in `#1a5276`. `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Split-panel charts use a dashed `#bdc3c7` vertical divider; axes/gridlines in `#95a5a6`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
