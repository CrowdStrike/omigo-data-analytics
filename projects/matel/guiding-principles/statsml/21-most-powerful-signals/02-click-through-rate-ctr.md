# Signal: Click-Through Rate (CTR)

**Page type:** detail page (most-powerful-signals compact style: per-section two-column layout table, text left 45% with tag pills / labeled bullets / example / key-point, canvas right 55%)
**HTML title tag:** Click-Through Rate (CTR)

**Subtitle:** Clicks divided by impressions — the most optimized, most monetized, and most systematically distorted signal in ads, search, and recommendation

## Why CTR Is So Powerful

**Tags:** `signal` (blue), `mechanism` (blue)

- **Implicit feedback** — every impression yields a free binary label
- **Massive scale** — billions of click labels daily dwarf all human judgments
- **Who runs on it** — Google search, Facebook/Instagram feeds, Amazon, ad exchanges
- **Freshness** — clicks reflect today; editorial labels go stale
- **Noisy label** — a click means curious, not satisfied

*Example: Google retrains on billions of fresh query-click pairs nightly — raters validate, clicks train.*

**Key point:** CTR is free, abundant, fresh, and monetized — and each of those properties is also a failure mode below.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart on a log scale: label volume per day by source.

- **Title (bold 14px `#1a5276`, top center):** "Labels Collected per Day (log scale)".
- **Bars** (horizontal, 30px tall, 18px gap, label column at x=200, bar max width 420, start y=50; width = log10 value / 10):
  - "Editorial ratings", val 3 (10^3), value label "~1K", fill `rgba(26,82,118,0.35)`, stroke `#1a5276`
  - "Survey responses", val 4, "~10K", same blue
  - "Explicit stars / likes", val 6, "~1M", same blue
  - "Click labels", val 9, "~1B", fill/stroke `#27ae60` (drawn at 0.7 alpha), value label in green bold
- **Log ticks:** dashed vertical gray `#ccc` lines (dash 2/3) at 10^3, 10^6, 10^9 with gray `#999` 10px labels "10^3", "10^6", "10^9" above.
- **Annotation:** orange `#e67e22` horizontal arrow below the bars spanning from the 10^3 tick to the 10^9 tick, labeled bold 11px "6 orders of magnitude, zero annotation cost".
- **Caption (bottom center, bold 11px `#1a5276`):** "Noisy label, unmatched volume — the trade every CTR system accepts".

## The Currency of Ad Auctions

**Tags:** `mechanism` (blue), `rule of thumb` (blue)

- **Auction rule** — ads rank by eCPM = bid × pCTR, not raw bid
- **Quality Score** — Google Ads score dominated by expected CTR
- **Cost lever** — 2x better pCTR halves cost per click
- **Birthed industrial ML** — Google logistic regression, Criteo FFM, wide-and-deep, DeepFM
- **Typical rates** — search ads ~2–6%; display banners ~0.1–0.5%

*Example: Ad B at $2.50 bid, 3.0% pCTR (eCPM $75) beats Ad A at $5.00, 0.8% pCTR (eCPM $40).*

**Why it matters:** pCTR multiplied by money billions of times daily turns every click-data bias into a pricing error.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: three ads, each with a bid bar and an eCPM bar.

- **Title:** "Auction Ranking: eCPM = Bid × pCTR" (bold 14px `#1a5276`).
- **Data:** Ad A bid $5.00, pCTR 0.8%, eCPM $40; Ad B bid $2.50, pCTR 3.0%, eCPM $75; Ad C bid $3.00, pCTR 1.5%, eCPM $45. Baseline y=235, chart height 165, bid scale max 6, eCPM scale max 80; group width 170 starting at x=60, bar width 46.
- **Bid bars:** fill `rgba(26,82,118,0.35)`, stroke `#1a5276`, value labels "$5.00"/"$2.50"/"$3.00" in gray `#555` above.
- **eCPM bars:** Ad B green `#27ae60` (winner, stroke width 2, 0.75 alpha fill), Ads A and C orange `#e67e22`; bold value labels "$40"/"$75"/"$45" in the bar color.
- **Group labels:** ad name bold 12px `#2c3e50` below baseline plus "pCTR 0.8%" etc. in `#555`.
- **Winner annotation:** bold green "WINNER" above Ad B's eCPM bar.
- **Legend (top right):** blue swatch "Bid (CPC)", orange swatch "eCPM".
- **Caption (bottom center, bold 11px `#1a5276`):** "Half the bid, 3.75x the pCTR — Ad B takes the slot (Quality Score in action)".

## Calibration and Rare Events

**Tags:** `rule of thumb` (blue), `failure mode` (red)

- **Rare positives** — CTRs run 0.1–2%; optimize log-loss, not accuracy
- **Sample math** — 5% lift at 0.5% base rate needs ~2.5M impressions/arm
- **Magnitude matters** — 20% over-prediction overprices every impression 20%
- **Check calibration** — predicted vs observed CTR per bucket should hug diagonal
- **Common fix** — isotonic or Platt layer, recalibrated as traffic drifts

*Example: perfect ranking with 20% over-prediction charges advertisers for clicks that never arrive.*

**Failure mode:** AUC measures order but auctions consume magnitudes — a well-ranked, badly calibrated model misprices the entire auction.

### Visualization (canvas `c3`, 720×300)

Calibration plot: predicted vs observed CTR by bucket.

- **Title:** "Calibration: Predicted vs Observed CTR (%)".
- **Axes:** L-shaped gray `#999` axes, padding top 40 / bottom 45 / left 80 / right 200; both axes 0 to 2.0; x tick labels "0", "1.0", "2.0"; x axis label "predicted CTR", rotated y axis label "observed CTR" (gray `#555` 11px).
- **Perfect diagonal:** dashed green `#27ae60` line (dash 6/4, width 2) from (0,0) to (2.0,2.0).
- **Model curve:** red `#e74c3c`, width 3, with 3.5px dots; predicted = `[0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4, 1.6, 1.8]`, observed = `[0.19, 0.38, 0.55, 0.68, 0.80, 0.90, 0.98, 1.05, 1.10]`.
- **Gap annotation:** dashed orange `#e67e22` vertical line (dash 3/3) at predicted 1.8 between the diagonal and the model point, labeled bold 10px orange "over-prediction gap" to its left.
- **Legend (right column):** dashed green line sample "Perfect calibration", solid red line sample "Model (by bucket)"; below, bold red three-line note "Top buckets over-predict:" / "every impression there" / "is overpriced".
- **Caption (bottom center, bold 11px `#1a5276`):** "Good AUC, bad calibration — fine for ranking, broken for pricing".

## Position Bias: The Top Slot Gets Clicked Regardless

**Tags:** `bias` (orange), `defense` (green)

- **10x decay** — position 1 draws ~10x position 10's clicks, content aside
- **Swap evidence** — eye-tracking and swap experiments prove the slot earns clicks
- **Fix: randomize** — shuffle 1–2% of traffic to measure examination directly
- **Fix: IPW** — reweight clicks by 1 / P(examined at rank k)
- **Fix: click models** — examination/cascade models factor position out of relevance

*Example: a result swapped into slot #1 inherits most of that slot's clicks.*

**Failure mode:** rankers trained on raw click logs imitate the old ranker's position bias and freeze the status quo.

### Visualization (canvas `c4`, 720×300)

Two-line chart: CTR by rank, observed decay vs debiased relevance.

- **Title:** "CTR by Rank: Observed Decay vs Debiased Relevance".
- **Axes:** gray `#999` L axes, padding top 40 / bottom 50 / left 70 / right 180; y max 35; x = ranks 1–10 labeled below, axis label "rank".
- **Series:** observed CTR red `#e74c3c` `[32, 15, 9, 6, 4.5, 3.5, 2.8, 2.3, 2.0, 1.8]`; IPW-debiased relevance green `#27ae60` `[20, 18, 17, 22, 16, 15, 14, 14, 13, 13]`; both width 3 with 3.5px dots.
- **Annotations:** bold red two-line note top-left "rank 1: ~10x rank 10," / "mostly examination, not relevance"; orange `#e67e22` 9px-radius circle around the rank-4 green point (value 22) with bold 10px orange two-line label "hidden gem: best item," / "buried at rank 4".
- **Legend (right):** red swatch "Observed CTR (%)", green swatch "IPW-debiased relevance".
- **Caption (bottom center, bold 11px `#1a5276`):** "Raw CTR = P(examined at rank) × P(click | examined) — debias before training".

## Presentation Bias: The Thumbnail Confound

**Tags:** `bias` (orange), `gaming` (orange)

- **Thumbnail tests** — YouTube creators move CTR 2–3x on identical videos
- **Banner blindness** — users skip ad-shaped regions entirely
- **Bolded snippets** — query-term bolding lifts CTR independent of relevance
- **Fix** — model presentation features, or hold packaging fixed when comparing

*Example: YouTube's built-in thumbnail "Test & Compare" tool admits packaging alone moves CTR by multiples.*

**How it's exploited:** once the ranker pays for packaging, creator effort migrates from content to a thumbnail arms race.

### Visualization (canvas `c5`, 720×300)

Three bars: same video, three thumbnails, with small thumbnail sketch boxes above each bar.

- **Title:** "Same Video, Three Thumbnails — CTR Measures the Wrapper".
- **Bars** (baseline y=230, chart height 160, scale max 9%, bar width 130, group width 200, start x=60):
  - "Plain screenshot" 2.4% — fill `rgba(26,82,118,0.35)`, stroke `#1a5276`; thumbnail box drawn 40px above bar containing text "[ still ]"
  - "Bold text overlay" 4.9% — same blue; thumbnail box text bold "BIG TEXT"
  - "Face + arrow + CAPS" 7.8% — orange `#e67e22` (0.8 alpha, stroke width 2); thumbnail box text bold ":O  =>"
- **Value labels:** bold 13px in the bar's stroke color, e.g. "2.4% CTR"; category labels 11px `#2c3e50` below baseline.
- **Spread annotation:** dashed red `#e74c3c` line (dash 4/3, width 2) from the top of bar 1 to the top of bar 3, labeled bold 12px red "3.3x spread — content identical".
- **Caption (bottom center, bold 11px `#1a5276`):** "Comparing CTR across different packaging compares wrappers, not content".

## Exploration vs Exploitation

**Tags:** `mechanism` (blue), `best practice` (green)

- **Blind spot** — never-shown items have unknown CTR forever
- **Production bandit** — Yahoo! front-page news ran LinUCB (2010)
- **Cold start** — ad systems inject exploration traffic for new ads
- **Policies** — epsilon-greedy, UCB, Thompson sampling on uncertain items
- **Rule of thumb** — 1–5% exploration traffic keeps estimates honest

*Example: Yahoo! showed a small random slice on unproven articles lifted long-run CTR.*

**Key point:** logged CTR reflects the item *and* the policy that showed it — ignore the policy and the signal is a self-portrait of the old system.

### Visualization (canvas `c6`, 720×300)

Confidence-interval plot: CTR estimates for 7 items A–G with error bars.

- **Title:** "CTR Estimates: You Only Learn What You Show".
- **Axes:** gray L axes, padding top 45 / bottom 55 / left 70 / right 40; y 0–2.0% with right-aligned labels "2%", "1%", "0".
- **Data (name, estimate, lo, hi, shown):** A 1.4 [1.35, 1.45] shown; B 1.2 [1.13, 1.27] shown; C 1.0 [0.9, 1.1] shown; D 0.9 [0.75, 1.05] shown; E 1.1 [0.3, 1.9] not shown; F 0.8 [0.1, 1.7] not shown; G 1.0 [0.2, 1.9] not shown.
- **Style:** shown items in blue `#1a5276`, rarely-shown in orange `#e67e22`; vertical CI whiskers with 7px end caps, 4.5px estimate dots; a faint orange region `rgba(230,126,34,0.08)` shades the plot behind items E–G.
- **Annotations:** bold blue two-line "heavily shown:" / "tight estimates" top-left; bold orange "rarely shown: unknown CTR" / "— bandits explore here" over the shaded region.
- **Caption (bottom center, bold 11px `#1a5276`):** "Zero exploration freezes the wide intervals forever — 1-5% traffic keeps them honest".

## The Clickbait Spiral

**Tags:** `failure mode` (red), `gaming` (orange)

- **Upworthy** — curiosity-gap headlines fueled record growth circa 2013
- **Facebook demotions** — explicit clickbait detection shipped 2014, 2016
- **Goodhart split** — CTR climbs while return visits and satisfaction decay
- **YouTube pivot** — switched ranking from clicks to watch time (2012)
- **Fix** — pair CTR with dwell, return-rate, "hide this" guardrails

*Example: the 2012–2016 listicle era was downstream of feeds ranking by click probability.*

**Failure mode:** once CTR became the target it measured susceptibility to bait, and content degraded to match it.

### Visualization (canvas `c7`, 720×300)

Two diverging lines over 12 weeks: CTR up, return rate down.

- **Title:** "Clickbait Optimization Over 12 Weeks: Proxy vs Goal".
- **Axes:** gray L axes, padding top 45 / bottom 50 / left 70 / right 180; y max 100; x labels "wk 1" and "wk 12" at the ends.
- **Series:** CTR red `#e74c3c` `[40, 44, 49, 55, 60, 66, 71, 75, 79, 82, 85, 87]`; return rate green `#27ae60` `[70, 69, 67, 64, 60, 55, 50, 45, 41, 37, 34, 31]`; both width 3.
- **Divergence wedge:** the area between the two lines from index 5 onward filled `rgba(231,76,60,0.08)`.
- **Crossover marker:** dashed orange `#e67e22` vertical line (dash 4/3, width 2) at index 5, labeled bold 10px orange "proxy and goal decouple" above.
- **Legend (right):** red swatch "CTR (the target)", green swatch "Return rate (the goal)"; below in bold red: "Goodhart split:" / "metric up, product dying".
- **Caption (bottom center, bold 11px `#1a5276`):** "Why YouTube moved to watch time and Facebook demoted clickbait".

## CTR vs Post-Click Satisfaction

**Tags:** `signal` (blue), `defense` (green)

- **Dwell time** — a 3-second visit is a negative label
- **Pogo-sticking** — immediate bounce back signals relevance failure
- **Satisfied clicks** — count only dwell above ~30 seconds as positive
- **Funnel view** — 1% CTR with 2% conversion beats 5% with 0.1%
- **Cascade model** — optimize pCTR × pCVR, not the first click

*Example: identical 8% CTR — bait spikes at 5s dwell, useful content humps near 90s.*

**Fix:** redefine the positive label from "clicked" to "clicked and stayed" — the single cheapest correction in this document.

### Visualization (canvas `c8`, 720×300)

Bimodal histogram of post-click dwell time with a satisfied-click threshold.

- **Title:** "Post-Click Dwell Time: Two Kinds of \"Click\"".
- **Axes:** gray L axes, padding top 45 / bottom 55 / left 70 / right 190; x labels "0s", "dwell time" (center), "200s".
- **Bins (20):** `[42, 30, 14, 6, 4, 5, 8, 13, 18, 22, 24, 22, 18, 13, 9, 6, 4, 2, 1, 1]`, scale max 45. Bins 0–4 (below threshold) filled `rgba(231,76,60,0.45)` stroked `#e74c3c`; bins 5+ filled `rgba(26,82,118,0.35)` stroked `#1a5276`.
- **Threshold:** dashed orange `#e67e22` vertical line (dash 5/4, width 2) at bin 5, labeled bold 11px "~30s satisfied-click threshold".
- **Mode annotations:** bold 10px red over the left spike: "bait + accidents:" / "label as NEGATIVE"; bold blue over the right hump: "real interest:" / "label as POSITIVE".
- **Legend (right):** red-tint swatch "Pogo-stick / bounce", blue-tint swatch "Satisfied click".
- **Caption (bottom center, bold 11px `#1a5276`):** "Identical CTR, opposite meaning — the dwell histogram tells them apart".

## Click Fraud and Gaming

**Tags:** `abuse` (red), `defense` (green)

- **Bot fraud** — invalid traffic hits double-digit % on low-quality display
- **Fat fingers** — large share of mobile banner clicks are accidental
- **The tell** — high CTR with near-zero conversions
- **Incentivized clicks** — click-to-unlock schemes, engagement pods
- **Defenses** — IVT filtering (Google refunds invalid clicks), dwell validation, CPA pricing

*Example: a banner one pixel from a game's "continue" button triples CTR with zero conversions.*

**How it's exploited:** every actor in the chain profits from manufactured clicks — treat raw click counts as adversarial input, not measurement.

### Visualization (canvas `c9`, 720×300)

Stacked horizontal bars: click composition by channel.

- **Title:** "What a \"Click\" Actually Is, by Channel (share of clicks)".
- **Data (stacked 100% shares [genuine, accidental, bot]):** "Desktop search" [88, 4, 8]; "Mobile banner" [45, 35, 20]; "Low-tier display" [50, 15, 35]. Bars 40px tall, 22px gap, label column at x=190, bar max width 430, start y=55.
- **Segment colors:** genuine intent `#27ae60`, accidental tap `#e67e22`, bot/invalid `#e74c3c` (0.75 alpha fill, 1px stroke); white bold percent labels inside segments ≥ 8%.
- **Annotation:** bold red 11px "over half of \"clicks\" carry no intent" below the Mobile banner bar (near its right half).
- **Legend (below bars):** color swatches for "Genuine intent", "Accidental tap", "Bot / invalid".
- **Caption (bottom center, bold 11px `#1a5276`):** "Raw click counts are adversarial input — filter invalid traffic before anything else".

## Best First Signal, Worst Final Objective

**Tags:** `best practice` (green), `rule of thumb` (blue)

- **Filter** — strip bots, click farms, accidental taps first
- **Debias** — randomization, inverse propensity weighting, or learned click models
- **Gate** — keep only dwell-thresholded satisfied clicks as positives
- **Calibrate** — isotonic/Platt so probabilities match observed frequencies
- **Explore + guard** — 1–5% exploration traffic; pair with satisfaction metrics

*Example: YouTube's watch time, Facebook's clickbait demotions, and satisfied-click search modeling all converged here independently.*

**Key point:** treat raw CTR as biased, adversarial measurement — its power comes from volume, its safety entirely from the corrections.

### Visualization (canvas `c10`, 720×300)

Pipeline flow diagram: five boxes with arrows, plus an exploration side-channel.

- **Title:** "The CTR Correction Pipeline: Raw Clicks to Trainable Labels".
- **Steps (110×60 boxes, 26px gaps, centered row at y=55):**
  1. "Raw click / logs", sub "biased, gamed" — stroke `#e74c3c`, bg `#fdedec`
  2. "IVT / filter", sub "bots, accidents" — stroke `#1a5276`, bg `#ebf5fb`
  3. "Position / debias", sub "IPW / randomize" — stroke `#1a5276`, bg `#ebf5fb`
  4. "Dwell / gate", sub "satisfied only" — stroke `#1a5276`, bg `#ebf5fb`
  5. "Calibrate", sub "isotonic / Platt" — stroke `#27ae60`, bg `#eafaf1`
  Blue `#1a5276` arrows connect consecutive boxes; box titles bold 11px `#1a5276`, subs 9px `#555`.
- **Exploration side-channel:** dashed orange `#e67e22` rectangle below the middle of the pipeline (spanning boxes 2–3 width) labeled bold 11px "+ 1-5% exploration traffic (bandits)" and 9px "keeps estimates honest on unshown items".
- **In/out annotations:** bold red "IN: biased, adversarial counts" below box 1; bold green "OUT: priceable, trainable labels" below box 5.
- **Bottom notes:** bold 11px `#1a5276` "Then pair the objective with satisfaction guardrails (dwell, return rate) against Goodhart pressure"; final bold 12px red line "CTR: best first signal, worst final objective".

## Regeneration instructions

- **Layout:** one `.card-section` per section, each containing an `<h2>` (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` (border-collapse, full width) with a single `<tr>`: left `td.text-col` (45%) holding `.tags` pills, a `<ul>` of bullets, `p.example`, and `.key-point`; right `td.viz-col` (55%) holding one `<canvas width="720" height="300">` styled `width:100%`, border `1px solid #e0e0e0`, radius 4px.
- **Page style:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem.
- **Canvas:** shared `setup(id)` helper — intrinsic 720×300, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates, clear before drawing. Each chart is an IIFE keyed to its canvas id.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#555`/`#666`/`#999`.
- No nav bar, no back/home links. In regenerated HTML any links use `.html` extensions.
