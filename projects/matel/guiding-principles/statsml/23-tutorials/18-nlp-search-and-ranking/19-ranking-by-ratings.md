# Ranking by Ratings

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Ranking by Ratings

**Subtitle:** A perfect average from one review should not beat a great average from two hundred — reputation scores rank by evidence, not by the raw average alone

## One Review, Five Stars, Top of the List

**Tags:** `core idea` (blue), `small samples` (orange), `star ratings` (green)

- **Two products** — product A: one review, average 5.0; product B: 200 reviews, average 4.6
- **The naive sort** — order by average rating and A sits above B on the store page
- **What the average hides** — one delighted reviewer, or the seller's cousin; 5.0 from n=1 is noise
- **What B has earned** — 200 opinions agreeing on 4.6 is strong evidence of a genuinely good product
- **The question** — how do you rank so that evidence counts, without banning new items from the list?

*Example (italic):* Sorting a store by raw average crowns whichever item has the fewest, luckiest reviews — the top of the list fills with n=1 five-star flukes.

**Key point:** An average carries no record of how many voices are behind it — a ranking that uses the average alone treats one voice and two hundred as equals.

### Visualization (canvas `c1`, 720×300)

Dot plot of the naive leaderboard: products placed on a 3.5–5.0 rating axis with dot size showing review count, the tiny n=1 dot sitting on top of the big n=200 dot.

- **Title (bold 15px, `#1a5276`, top center):** "Sorted by Raw Average: the Fluke Outranks the Favorite".
- **Axis:** horizontal rating scale 3.5–5.0 at y=200, from x=110 to x=650; ticks and 12px `#444` labels at 3.5, 4.0, 4.5, 5.0; light `#e5e9ef` vertical gridlines at each tick from y=80.
- **Product A:** small dot (6px radius) orange `#d95926` at rating 5.0, y=120; bold 12px orange label above: "A — 5.0 from 1 review"; 11px `#6b7280` "rank #1" beside.
- **Product B:** large dot (16px radius) blue `#2a78d6` at rating 4.6, y=160; bold 12px blue label above-left: "B — 4.6 from 200 reviews"; 11px `#6b7280` "rank #2" beside.
- **Two more flukes for texture:** 6px mute `#6b7280` dots at 4.9 (y=140) and 4.8 (y=175) labeled 11px mute "2 reviews", "3 reviews".
- **Legend note (11px `#6b7280`, top right):** "dot size = number of reviews".
- **Annotation (bold 12px `#e74c3c`, near x=140, y=245):** "the raw-average sort ranks noise above evidence".
- **Caption (11px `#444`, bottom right):** "ratings illustrative".

## Shrink Toward the Crowd: Bayesian Average

**Tags:** `worked example` (blue), `prior` (green), `shrinkage` (orange)

- **The trick** — before counting real reviews, pretend every product starts with 10 imaginary reviews of 4.0
- **The formula** — score = (10 × 4.0 + n × average) ÷ (10 + n), where n is the real review count
- **Product A** — (40 + 1 × 5.0) ÷ 11 = 4.09: one review barely moves it off the 4.0 starting point
- **Product B** — (40 + 200 × 4.6) ÷ 210 = 4.57: two hundred reviews almost fully take over
- **The order flips** — B (4.57) now ranks above A (4.09), and both scores drift toward truth as reviews arrive

*Example (italic):* The 10 imaginary reviews are a skeptic's handicap — every newcomer must out-argue ten average votes before claiming a top rank.

**Key point:** The Bayesian average blends the store-wide prior with the item's own reviews in proportion to how many there are — few reviews, mostly prior; many reviews, mostly item.

### Visualization (canvas `c2`, 720×300)

Pull diagram: a rating number line with the prior at 4.0, showing A dragged hard from its raw 5.0 down to 4.09 while B moves barely at all from 4.6 to 4.57.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Imaginary Reviews at 4.0 — Then Let the Real Ones Argue".
- **Axis:** rating scale 3.8–5.0 at y=210, x=110–650; ticks with 12px `#444` labels at 3.8, 4.0, 4.2, 4.4, 4.6, 4.8, 5.0.
- **Prior marker:** 2px dashed violet `#4a3aa7` vertical line at 4.0 from y=70 to y=210, bold 12px violet label "prior 4.0" at the top of the line.
- **Row A (y=115):** open orange circle (7px, 2px stroke) at raw 5.0 labeled 12px `#d95926` "raw 5.0 (n=1)"; solid orange dot at 4.09 labeled bold 12px "4.09"; thick 3px orange arrow from raw to shrunk position; 11px `#6b7280` under the arrow midpoint: "1 review vs 10 imaginary — the prior wins".
- **Row B (y=165):** open blue circle at raw 4.6 labeled 12px `#2a78d6` "raw 4.6 (n=200)"; solid blue dot at 4.57 labeled bold 12px "4.57"; short 3px blue arrow between them; 11px `#6b7280` note: "200 reviews — the data wins".
- **Annotation (bold 12px green `#008300`, near x=140, y=250):** "order after shrinking: B 4.57 above A 4.09".
- **Caption (11px `#444`, bottom right):** "prior weight 10 reviews at 4.0 — illustrative".

## Rank by the Lower Bound: Wilson Score

**Tags:** `worked example` (blue), `confidence interval` (green), `up/down votes` (orange)

- **Thumbs, not stars** — a comment has u upvotes and d downvotes; the naive score is u ÷ (u + d)
- **Same trap** — 4 up, 0 down gives a perfect 1.00; 90 up, 10 down gives 0.90; the fluke wins again
- **The fix** — compute the 95% confidence range for the true like-rate, then rank by its LOW end
- **Comment X (4/0)** — the range is wide, roughly 0.51 to 1.00: lower bound 0.51
- **Comment Y (90/10)** — the range is tight, roughly 0.83 to 0.94: lower bound 0.83, so Y ranks first
- **The reading** — the lower bound answers "how good is this, even if the few votes so far got lucky?"

*Example (italic):* Ranking by the Wilson lower bound asks each item for a guarantee, not a best case — 4-for-4 can only guarantee "better than a coin flip".

**Key point:** With vote counts, uncertainty shows up as interval width — ranking by the interval's low end automatically demotes small samples without banning them.

### Visualization (canvas `c3`, 720×300)

Interval chart: each comment drawn as a confidence bar on a 0–1 like-rate axis, with the naive point estimate marked and the ranking arrow pointing at the higher lower bound.

- **Title (bold 15px, `#1a5276`, top center):** "Wide Interval, Weak Guarantee: Rank by the Left Edge".
- **Axis:** like-rate scale 0.4–1.0 at y=215, x=110–650; ticks with 12px `#444` labels at 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0; light gridlines up to y=70.
- **Row X (y=115):** horizontal interval bar from 0.51 to 1.00, 10px tall, fill `rgba(217,89,38,0.25)`, 2px orange `#d95926` border; solid orange dot at the naive 1.00 with 12px label "naive 1.00"; bold 13px orange marker + label at the left edge: "0.51"; 12px `#2c3e50` row label at x=100 right-aligned: "X: 4 up, 0 down".
- **Row Y (y=170):** interval bar 0.83–0.94, fill `rgba(42,120,214,0.25)`, 2px blue `#2a78d6` border; solid blue dot at naive 0.90 labeled "naive 0.90"; bold 13px blue "0.83" at the left edge; row label "Y: 90 up, 10 down".
- **Ranking marks:** bold 12px green `#008300` "ranked #1" beside Y's lower bound; bold 12px `#6b7280` "ranked #2" beside X's lower bound.
- **Annotation (bold 12px violet `#4a3aa7`, near x=140, y=255):** "the tight interval wins — its worst case beats X's worst case".
- **Caption (11px `#444`, bottom right):** "95% intervals, values rounded — illustrative".

## Trust Grows With Evidence

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Everywhere lists are voted on** — store sort orders, comment threads, seller scores, app charts
- **The wrong fix** — a hard cutoff like "only rank items with 50+ reviews" bans every new item forever
- **The right fix** — shrinkage lets new items compete from the prior and rise as reviews accumulate
- **Watch it converge** — a truly-4.8 product scores 4.07 at 1 review, 4.53 at 20, 4.73 at 100, 4.78 at 500
- **Same lesson as links** — like trust flowing from seed pages, reputation is earned evidence, not a raw tally

*Example (italic):* With a 50-review cutoff a brilliant new product is invisible for months; with a Bayesian average it debuts mid-list and climbs as evidence arrives.

**Common mistake:** Fixing the small-sample trap with a minimum-review threshold. The cliff at the threshold is arbitrary — 49 reviews invisible, 50 fully trusted — while shrinkage handles every review count smoothly with no cliff at all.

### Visualization (canvas `c4`, 720×300)

Convergence curve: the Bayesian score of a truly-4.8 product as its review count grows from 1 to 500 (log-spaced points), climbing from near the 4.0 prior toward the dashed 4.8 truth line.

- **Title (bold 15px, `#1a5276`, top center):** "A True 4.8 Earns Its Score as Reviews Arrive".
- **Axes:** origin x=80, baseline y=235, plot width 520, plot height 160; y from 3.9 to 4.9 with ticks 4.0, 4.2, 4.4, 4.6, 4.8 (12px `#444`, light gridlines); x positions evenly spaced for review counts 1, 5, 10, 20, 50, 100, 200, 500 with 12px `#444` tick labels; 12px axis label "number of reviews" centered below.
- **Truth line:** 2px dashed green `#008300` horizontal line at 4.8 labeled bold 12px green "true quality 4.8" (top right, above the line).
- **Prior line:** 1.5px dashed violet `#4a3aa7` horizontal line at 4.0 labeled 12px violet "prior 4.0" (left, below the line).
- **Curve:** 2.5px blue `#2a78d6` line with 4px dots through hardcoded points (n, score): (1, 4.07), (5, 4.27), (10, 4.40), (20, 4.53), (50, 4.67), (100, 4.73), (200, 4.76), (500, 4.78); bold 12px blue value labels on the first, fourth, and last points: "4.07", "4.53", "4.78".
- **Annotation (bold 12px blue `#2a78d6`, near x=200, y=90):** "no cliff, no cutoff — evidence slowly overrides the prior".
- **Caption (11px `#444`, bottom right):** "score = (10×4.0 + n×4.8) ÷ (10+n)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; redraw all charts on debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data:** Bayesian averages use prior weight 10 at mean 4.0 — A: (40+5)/11 = 4.09, B: (40+920)/210 = 4.57; the c4 curve is (10×4.0 + n×4.8)/(10+n) at n = 1, 5, 10, 20, 50, 100, 200, 500 → 4.07, 4.27, 4.40, 4.53, 4.67, 4.73, 4.76, 4.78; Wilson 95% lower bounds: 4/0 → 0.51 (interval 0.51–1.00), 90/10 → 0.83 (interval 0.83–0.94); all values hardcoded, text and chart numbers identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
