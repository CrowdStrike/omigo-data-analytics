# Updating Beliefs with Evidence

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Updating Beliefs with Evidence

**Subtitle:** Each new piece of evidence nudges a probability up or down — no single piece settles the question

## Judging a Seller One Review at a Time

**Tags:** `core idea` (blue)

- **The setup** — on this marketplace, about 5% of sellers are fraudulent
- **The evidence** — "never arrived" complaints: 50% of a fraud's reviews, 5% of an honest one's
- **One complaint** — belief that this seller is fraudulent jumps from 5% to 35%
- **One normal review** — belief eases back down: 35% to 22%
- **Six reviews in** — after 4 complaints and 2 normal reviews, belief reaches 99%

*Example:* The first complaint is a 10-to-1 clue: fraudulent sellers produce it ten times as often as honest ones.

**Key point:** Updating is repeated nudging — each review moves the number, and none of them alone ends the question.

### Visualization (canvas `c1`, 720×300)

Line chart: belief path over six reviews.

- **Title (bold 15px, `#1a5276`, top center):** "Belief the Seller Is Fraudulent, Review by Review".
- **Data:** 7 points; x labels `['start', 'complaint', 'normal', 'complaint', 'complaint', 'normal', 'complaint']`, belief values `[5, 35, 22, 73, 97, 94, 99]` (percent).
- **Axes:** y 0–100%, gridlines and labels every 25% in gray `#6b7280`, gridline color `#e5e9ef`; x points evenly spaced; padding top 55, bottom 75, left 70, right 40; gray `#999` axis lines.
- **Series:** connected line in blue `#2a78d6`, width 3; 5px-radius dots per point — start dot gray `#6b7280`, "normal" review dots green `#008300`, complaint dots orange `#d95926`. Each point has its value ("5%", "35%", …) in bold 12px `#2c3e50` above; the x label beneath each point is colored like its dot.
- **X-axis caption (gray, centered below labels):** "reviews, in the order they arrive".
- **Annotation:** bold 13px green `#008300` text near point 3 (offset +55px right, +28px below the 22% point): "normal reviews nudge it back down".

## The First Nudge, Counted with 1,000 Sellers

**Tags:** `worked example` (green)

- **Start** — 1,000 sellers: 50 fraudulent, 950 honest (that is the 5% prior)
- **Complaints from frauds** — 50% of 50 = 25 frauds whose first review is a complaint
- **Complaints from honest** — 5% of 950 = 48 honest sellers with the same first review
- **The pile** — 25 + 48 = 73 sellers open with a complaint
- **Updated belief** — 25 / 73 ≈ 35%: nudged up a lot, still most likely honest

*Example:* Honest sellers outnumber frauds 19 to 1, so even a rare honest mishap fills most of the complaint pile.

**Key point:** Every update is this same count — who else could have produced this evidence, and how many of them are there?

### Visualization (canvas `c2`, 720×300)

Flow/tree diagram of boxes and arrows counting the first update.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Sellers — Whose First Review Is a Complaint?".
- **Top box** at (280, 38), 160×34: fill `#eaf2fb`, stroke blue `#2a78d6`, label "1,000 sellers".
- **Second row:** box at (90, 112), 180×40, fill `rgba(213,81,129,0.10)`, stroke magenta `#d55181`, two lines "50 fraudulent" / "(the 5% prior)"; box at (440, 112), 190×40, fill `rgba(0,131,0,0.08)`, stroke green `#008300`, "950 honest" / "(95%)". Gray arrows from the top box down to each.
- **Third row:** two orange boxes (fill `rgba(217,89,38,0.12)`, stroke `#d95926`): at (90, 202), 180×40, "25 complain" / "50% of 50"; at (440, 202), 190×40, "48 complain" / "5% of 950". Vertical gray arrows from the row above.
- **Bottom line (bold 13px violet `#4a3aa7`, centered, y=274):** "Complaint pile: 25 + 48 = 73 sellers, of whom 25 are frauds → 25 / 73 ≈ 35%".
- **Caption (12px gray `#6b7280`, centered, y=292):** "the honest crowd is so large that its rare mishaps still dominate the pile".

## Strong Priors Need Strong Evidence

**Tags:** `rule of thumb` (green), `where it's used` (blue)

- **Trusted seller** — a 0.5% prior needs 3 complaints to pass 80% belief
- **Unknown seller** — a 5% prior passes 80% after just 2 complaints
- **Same evidence, different start** — the prior sets where each nudge lands
- **Where it shows up** — fraud scores, medical retests, ranking new content, alert triage
- **Escaping a prior** — overturning a strong belief takes several independent clues

*Example:* One complaint moves the trusted seller only to 4.8% — the same complaint moves the unknown one to 35%.

**Key point:** If a single data point flips your conclusion, the prior was never a real belief to begin with.

### Visualization (canvas `c3`, 720×300)

Two-line chart: belief vs number of complaints under two priors.

- **Title (bold 15px, `#1a5276`, top center):** "Same Complaints, Different Starting Trust".
- **Data:** x = 0..4 complaints; unknown (5% prior) `[5, 34.5, 84.1, 98.1, 99.8]` in orange `#d95926`; trusted (0.5% prior) `[0.5, 4.8, 33.4, 83.4, 98.1]` in aqua `#199e70`. Lines width 3 with 4.5px dots.
- **Axes:** y 0–100%, gridlines every 25% (gray labels `#6b7280`, grid `#e5e9ef`); padding top 55, bottom 65, left 70, right 175. X tick labels 0–4, axis caption "\"never arrived\" complaints received" (gray, centered).
- **Threshold line:** horizontal dashed magenta `#d55181` (dash 5/4, width 1.5) at 80%, labeled bold 12px magenta "80% action threshold" above-left.
- **Legend (right side, x = w−160):** orange swatch "unknown (5% prior)", aqua swatch "trusted (0.5% prior)".
- **Annotation (bold 13px aqua, right side, three lines):** "the trusted seller" / "needs one extra" / "complaint to catch up".

## The Common Confusion: a Nudge Is Not a Verdict

**Tags:** `common mistake` (red)

- **Order-free** — complaint-then-normal and normal-then-complaint both end at 22%
- **Different paths** — one path spikes to 35% first, the other dips to 2.7% first
- **Don't act on spikes** — the in-between numbers are honest but provisional
- **Independence caveat** — a brigade of copy-paste reviews is one clue, not many

*Example:* Suspending the seller at the 35% spike would punish roughly two honest sellers for every fraud.

**Key point:** With independent evidence, only the totals matter — 1 complaint + 1 normal ends at 22% in either order.

### Visualization (canvas `c4`, 720×300)

Two-path line chart showing two orders of the same evidence converging.

- **Title (bold 15px, `#1a5276`, top center):** "Same Two Reviews, Either Order: Same Ending".
- **Data:** 3 x positions ("start", "after 1st review", "after 2nd review"); path A (complaint then normal) `[5, 35, 22]` in orange `#d95926`; path B (normal then complaint) `[5, 2.7, 22]` in aqua `#199e70`. Lines width 3 with 5px dots; x positions inset 10%–90% of plot width.
- **Axes:** y 0–50%, gridlines every 10% (gray labels, grid `#e5e9ef`); padding top 55, bottom 65, left 70, right 185.
- **Point labels (bold 12px):** "35%" in orange above the A middle point; "2.7%" in aqua below the B middle point; "5%" in text color `#2c3e50` left of the start; "both end at 22%" in violet `#4a3aa7` above the shared end point.
- **Legend (right side, x = w−172):** orange swatch "complaint, then normal", aqua swatch "normal, then complaint".
- **Annotation (bold 13px magenta `#d55181`, right side, three lines):** "acting at the 35% spike" / "punishes mostly honest" / "sellers".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: `.text-col` td (50%) and `.viz-col` td (50%), 12px padding.
- **Left column structure per section:** a `.tags` row of colored pill spans, a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms in `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Tag pill styles:** 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** each 720×300 intrinsic, `width:100%` CSS, `1px solid #e0e0e0` border, radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All data as hardcoded literal arrays — no `Math.random()`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
