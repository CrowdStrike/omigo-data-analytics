# Human-in-the-Loop Systems

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Human-in-the-Loop Systems

**Subtitle:** A human-in-the-loop system lets the model decide the easy cases on its own and routes the unsure ones to people — whose answers then become the training data that makes tomorrow's model better

## A Scam Filter That Knows When to Ask

**Tags:** `core idea` (blue), `thresholds` (green), `review queue` (orange)

- **The marketplace** — a used-goods site gets about 1,000 new listings a day and must catch scam posts
- **The model** — a classifier gives every listing a scam score from 0 (surely fine) to 1 (surely scam)
- **Two thresholds** — below 0.20 the listing is auto-approved, above 0.90 auto-blocked; no human touches it
- **The middle band** — scores between 0.20 and 0.90 are the unsure zone: those go to a human review queue
- **Division of labor** — the machine handles the obvious ends, people handle only the ambiguous middle

*Example (italic):* A listing scoring 0.05 goes live instantly, one scoring 0.95 is blocked instantly — only the 0.55 "sealed phone, half price" listing waits for a human to look.

**Key point:** Human-in-the-loop means picking two thresholds: the model acts alone at the confident ends, and everything in the unsure middle is routed to a person.

### Visualization (canvas `c1`, 720×300)

Single-panel histogram of one day's scam scores split into three colored zones (auto-approve, review queue, auto-block) by two dashed threshold lines.

- **Title (bold 15px, `#1a5276`, top center):** "One Day of Scam Scores: Two Thresholds, Three Zones".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = scam score 0 to 1.0 with 12px `#444` tick labels "0", "0.1", ..., "1.0" every 0.1; y = listings 0 to 700, light `#e5e9ef` gridlines at 200, 400, 600 with 12px `#444` labels.
- **Bars:** 10 bars, one per 0.1-wide score bucket, counts = `[650, 170, 45, 30, 22, 16, 12, 9, 6, 40]` (sum 1,000); first two bars fill `rgba(0,131,0,0.30)` with 1px `#008300` border, middle seven bars fill `rgba(217,89,38,0.35)` with 1px `#d95926` border, last bar fill `rgba(231,76,60,0.35)` with 1px `#e74c3c` border; 11px `#444` count label above each bar.
- **Threshold lines:** vertical dashed `#1a5276` (dash 4/3) lines at score 0.20 and 0.90 from baseline to y=55; bold 12px `#1a5276` labels at their tops: "auto-approve below 0.20" (left line, anchored left) and "auto-block above 0.90" (right line, anchored right).
- **Zone captions (12px, below the x-axis tick labels, centered under each zone):** green `#008300` "machine says fine", orange `#d95926` "humans decide", red `#e74c3c` "machine blocks".
- **Annotation (bold 13px orange `#d95926`, near x=0.50, y=110):** two lines: "140 unsure listings" / "→ human review queue".
- **Caption (12px `#444`, bottom right):** "illustrative — one day of 1,000 listings".

## One Day Through the Two Thresholds

**Tags:** `worked example` (blue), `queue math` (green)

- **The day's traffic** — 1,000 listings: 820 score below 0.20, 140 land between 0.20 and 0.90, 40 score 0.90+
- **Machine alone** — 820 auto-approved plus 40 auto-blocked: 860 decisions made with zero human effort
- **The queue** — 140 listings wait for reviewers; at 2 minutes each that is about 4.7 hours of review work
- **Human verdicts** — reviewers approve 110 of the 140 and reject 30 scams the model could not call
- **Final tally** — 930 approved (820 + 110) and 70 blocked (40 + 30); every number checks by hand

*Example (italic):* Move the lower threshold from 0.20 up to 0.30 and the 45 listings in that bucket auto-approve, shrinking the queue from 140 to 95 — threshold choice is staffing choice.

**Key point:** The thresholds set the queue: 1,000 − 820 − 40 = 140 reviews a day, so where you draw the two lines is really a decision about how many reviewers you hire.

### Visualization (canvas `c2`, 720×300)

Two horizontal stacked bars on a shared 0–1,000 listings axis: the model's split before review on top, the final outcome after human verdicts below, showing the orange unsure block resolving into green and red.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Listings: Model Split, Then Human Verdicts".
- **Axis:** horizontal 2px `#999` line at y=250 from x=180 to x=680 (width 500), scale 0 to 1,000 listings; 12px `#444` tick labels "0", "250", "500", "750", "1,000".
- **Row 1 (bar centered y=105, 34px tall), left-aligned 12px `#444` label at x=20:** "model alone"; segments left to right: 820 fill `rgba(0,131,0,0.30)` labeled bold 12px `#008300` "820 auto-approved" (inside), 140 fill `rgba(217,89,38,0.40)` labeled bold 12px `#d95926` "140 to queue" (above the segment), 40 fill `rgba(231,76,60,0.40)` labeled 12px `#e74c3c` "40 blocked" (below the segment, staggered to avoid overlap).
- **Row 2 (bar centered y=195, 34px tall), label at x=20:** "after human review"; segments: 930 fill `rgba(0,131,0,0.30)` labeled bold 12px `#008300` "930 approved (820 + 110)" (inside), 70 fill `rgba(231,76,60,0.40)` labeled 12px `#e74c3c` "70 blocked (40 + 30)" (below the segment).
- **Flow hint:** two thin dashed `#6b7280` (dash 4/3) connector lines from the ends of row 1's orange segment down to row 2, showing the unsure block splitting.
- **Annotation (bold 13px green `#008300`, centered near y=278):** "humans caught 30 scams the model was unsure about".
- **Caption (12px `#444`, bottom right):** "illustrative — one day's counts".

## Reviewer Answers Become Training Data

**Tags:** `where it's used` (blue), `feedback retraining` (green), `compounding` (orange)

- **Free labels** — every queue verdict is a labeled example: 140 a day is roughly 4,200 labels a month
- **Retraining** — each month the model retrains on those verdicts and gets sharper in the middle band
- **Shrinking queue** — the unsure share falls month by month: 14% at month 1 down to 7% by month 6
- **Compounding** — a sharper model sends fewer cases to people, freeing reviewers for the hardest ones
- **Where it lives** — fraud review, content moderation, medical triage, and loan checks all run this loop

*Example (italic):* By month 6 the same 1,000 daily listings produce a 70-listing queue instead of 140 — the reviewers' own past answers taught the model to handle half their old workload.

**Key point:** The loop is the point: humans don't just patch the model's gaps today, their verdicts are the training data that closes those gaps tomorrow.

### Visualization (canvas `c3`, 720×300)

Single-panel line chart of the review-queue share over six monthly retrains, falling as reviewer labels accumulate.

- **Title (bold 15px, `#1a5276`, top center):** "Each Retrain Shrinks the Queue: 14% → 7% in Six Months".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = months 1 to 6, evenly spaced, 12px `#444` tick labels "month 1" ... "month 6"; y = share of listings sent to review, 0% to 16%, light `#e5e9ef` gridlines at 4%, 8%, 12% with 12px `#444` labels.
- **Queue-share line:** blue `#2a78d6` 3px line through points at months `[1, 2, 3, 4, 5, 6]`, shares = `[14.0, 12.5, 10.8, 9.2, 7.9, 7.0]` (percent); 7px blue dots at each point; bold 12px blue value labels above each dot: "14.0%", "12.5%", "10.8%", "9.2%", "7.9%", "7.0%".
- **Retrain markers:** small 11px `#6b7280` label "+4,200 labels, retrain" with a short dashed `#6b7280` tick between months 1–2 (repeat symbol "↻" alone at later gaps to avoid clutter).
- **Reference line:** horizontal dashed `#6b7280` (dash 4/3) line at 14% across the plot, 11px `#6b7280` label "month-1 workload" at its right end.
- **Annotation (bold 13px green `#008300`, near month 4.5, y=115):** two lines: "half the queue gone —" / "trained on reviewers' own verdicts".
- **Caption (12px `#444`, bottom right):** "illustrative — same 1,000 listings/day throughout".

## The Blind Spot Above and Below the Thresholds

**Tags:** `common mistake` (red), `feedback loops` (orange)

- **Labels from the middle** — humans only ever see the queue, so only the 140 unsure listings get labels
- **The silent 860** — the 820 auto-approved and 40 auto-blocked listings are never checked by anyone
- **Hidden misses** — a scam scoring 0.10 sails through today and teaches the model nothing at all
- **Biased retraining** — a model retrained only on middle-band verdicts goes blind at the two ends
- **The fix** — an audit sample: route a random 2% of auto-decided listings (~17 a day) into the queue

*Example (italic):* If scammers learn that cheap camera listings score low, those scams live in the auto-approve zone — exactly where no reviewer will ever create a correcting label.

**Common mistake:** Retraining only on queue verdicts. 860 of 1,000 daily decisions never get a human label, so always add a small random audit slice from the auto-decided zones.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart with one group per zone: listings per day next to human labels per day, making the two unlabeled end zones visually obvious.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Labels Come From — and Where They Don't".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = per day, 0 to 900, light `#e5e9ef` gridlines at 200, 400, 600, 800 with 12px `#444` labels.
- **Groups (three, evenly spaced on x), 12px `#444` group labels below baseline:** "auto-approve (< 0.20)", "review queue (0.20–0.90)", "auto-block (≥ 0.90)".
- **Bars per group (two, side by side, 44px wide, 10px gap):** "listings/day" bar fill `rgba(42,120,214,0.35)` with 1px `#2a78d6` border, heights `[820, 140, 40]`; "human labels/day" bar fill `rgba(0,131,0,0.35)` with 1px `#008300` border, heights `[0, 140, 0]`; bold 12px value labels above every bar ("820", "0", "140", "140", "40", "0"); the two zero-label bars drawn as a 2px `#e74c3c` baseline stub with bold 12px red `#e74c3c` "0" above.
- **Legend (top right, 12px):** blue swatch "listings per day", green swatch "human labels per day".
- **Fix marker:** dashed green `#008300` (dash 4/3) outline box of height ≈17 units on top of each zero-label stub, 11px `#008300` label "+2% audit (~17/day)" beside the first one.
- **Annotation (bold 13px magenta `#d55181`, centered near y=90):** two lines: "860 of 1,000 decisions never checked —" / "the retraining set only sees the middle".
- **Caption (12px `#444`, bottom right):** "illustrative — one day of 1,000 listings".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for the block/error zone and the zero-label stubs.
- **Data:** all bar counts, shares, and line points are the hardcoded arrays above (no randomness); the histogram counts sum to 1,000 and the worked-example arithmetic (820 + 140 + 40 = 1,000; 930 = 820 + 110; 70 = 40 + 30) must stay consistent between text and charts; all invented numbers keep their "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
