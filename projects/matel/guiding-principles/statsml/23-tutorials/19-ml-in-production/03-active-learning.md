# Active Learning

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Active Learning

**Subtitle:** When labels are expensive, don't label examples at random — let the model point at the ones it is most unsure about, and spend your labeling budget there

## Five Thousand Emails, Time to Read Two Hundred

**Tags:** `core idea` (blue), `labeling budget` (green), `uncertainty` (orange)

- **The shop** — an online shop has 5,000 unread customer emails and wants a model that flags refund requests
- **The cost** — a label means a person reads the email and marks it "refund" or "other"; there is time for 200
- **The naive plan** — label 200 emails picked at random; most turn out to be easy, obvious cases
- **The active plan** — label 40 at random, train a rough model, then ask it: which emails confuse you most?
- **The loop** — label those, retrain, ask again; each round the model chooses its own next reading list
- **The name** — the model actively picks its training data instead of passively taking whatever arrives

*Example (italic):* After scoring all 5,000 emails, only about 400 land in the unsure zone near 50% — those 400 are where the owner's reading time actually teaches the model something.

**Key point:** Active learning = the model scores the unlabeled pile, and the human labels the examples the model is least sure about — budget goes where confusion lives.

### Visualization (canvas `c1`, 720×300)

Single-panel histogram: the model's refund scores for all 5,000 unlabeled emails in 10 bins, U-shaped (most emails are easy, few are unsure), with the unsure middle band highlighted as the labeling target.

- **Title (bold 15px, `#1a5276`, top center):** "Model Scores on 5,000 Unlabeled Emails — the Unsure Middle Is Where Labels Help".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; x axis = refund score 0 to 1 with 12px `#444` tick labels "0.0", "0.1", ..., "1.0"; y axis = emails per bin 0 to 1,200 with light `#e5e9ef` gridlines at 300, 600, 900, 1200 and 12px `#444` labels.
- **Bars:** 10 bins of width 0.1, counts = `[1050, 700, 390, 250, 190, 210, 260, 350, 620, 980]` (sums to 5,000); default fill `rgba(42,120,214,0.35)` with 1px `#2a78d6` stroke.
- **Unsure zone:** the two bins covering scores 0.4–0.6 (counts 190 and 210) filled orange `rgba(217,89,38,0.45)` with 2px `#d95926` stroke; light orange `rgba(217,89,38,0.08)` background band behind the plot from x=0.4 to x=0.6.
- **Zone labels:** 12px `#6b7280` "confident: other" above the left bars (near score 0.1) and "confident: refund" above the right bars (near score 0.9).
- **Annotation (bold 13px orange `#d95926`, centered above the middle band, y=85):** two lines: "unsure zone: 400 emails" / "label these, not the easy 4,600".
- **Caption (12px `#444`, bottom right):** "illustrative — scores from a rough model trained on 40 random labels".

## Picking the Next Three Emails by Hand

**Tags:** `worked example` (blue), `uncertainty sampling` (green)

- **Eight emails** — model scores: A 0.98, B 0.93, C 0.71, D 0.55, E 0.49, F 0.32, G 0.12, H 0.04
- **The rule** — measure unsureness as distance from 0.5; the smaller the distance, the more confused the model
- **The distances** — A 0.48, B 0.43, C 0.21, D 0.05, E 0.01, F 0.18, G 0.38, H 0.46
- **The picks** — budget of 3 labels goes to the smallest distances: E (0.01), then D (0.05), then F (0.18)
- **The skips** — A and H are nearly settled; reading them confirms what the model already believes
- **Redo it** — sort eight distances, take the bottom three; the whole selection step is that simple

*Example (italic):* Email E scored 0.49 — the model is a coin flip on it — so E is the single most valuable email for the owner to read next.

**Key point:** Uncertainty sampling picks by distance from 0.5: here that means labeling E, D, and F, and leaving near-certain A (0.98) and H (0.04) unread.

### Visualization (canvas `c2`, 720×300)

Single-panel dot strip: the eight emails placed on a 0-to-1 score axis, with a marked 0.5 line, distance ticks, and the three chosen emails circled.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Scored Emails — Label the Three Closest to 0.5".
- **Axis:** horizontal 2px `#999` line at y=180 from x=60 to x=660 (width 600), score 0 to 1; tick labels "0.0"–"1.0" every 0.1 (12px `#444`) below.
- **Midline:** vertical dashed `#6b7280` (dash 4/3) line at score 0.5 from y=70 to the axis; bold 12px `#6b7280` label "0.5 = total coin flip" at its top.
- **Dots:** 8px dots on the axis at scores `[0.98, 0.93, 0.71, 0.55, 0.49, 0.32, 0.12, 0.04]`; letters A–H in bold 13px `#2c3e50` above each dot; unpicked dots blue `#2a78d6`, picked dots (D, E, F) orange `#d95926`.
- **Pick rings:** 14px-radius 2px orange circles around D, E, F; bold 12px orange distance labels below the axis under each pick: "0.05", "0.01", "0.18".
- **Skip labels:** 11px `#6b7280` "already sure" under A (0.98) and under H (0.04).
- **Annotation (bold 13px orange `#d95926`, near x=140, y=90):** two lines: "3-label budget →" / "E, D, F win it".
- **Caption (12px `#444`, bottom right):** "distances from 0.5: A .48, B .43, C .21, D .05, E .01, F .18, G .38, H .46".

## The Same Accuracy for Half the Labels

**Tags:** `where it's used` (blue), `learning curve` (green), `label cost` (orange)

- **The payoff** — at 200 labels the active model reaches 85% accuracy; random labeling sits at 77%
- **The gap** — random labeling still hasn't hit 85% even at 320 labels (it is only at 81% there)
- **Where it lives** — medical images, legal documents, support tickets: anywhere an expert hour is the cost
- **Same start** — both plans begin identically at 40 random labels (62%); the choice of *next* label splits them
- **Diminishing ease** — random keeps re-buying easy examples the model already gets right for free
- **Honest check** — the accuracy numbers come from a separate randomly drawn test set, never the picked pool

*Example (italic):* If an expert labels 30 support tickets an hour, reaching 85% costs under 7 hours with active picking, and random sampling still isn't there after 10.

**Key point:** The value of active learning is the learning curve: 85% accuracy at 200 active labels versus 77% at 200 random ones — the budget buys hard examples, not repeats.

### Visualization (canvas `c3`, 720×300)

Single-panel line chart: two learning curves (accuracy on a held-out test set vs number of labels), active selection in green above random selection in blue, sharing the same starting point.

- **Title (bold 15px, `#1a5276`, top center):** "Accuracy vs Labels Spent: Active Picking Pulls Ahead".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x axis = labels spent 40 to 320 with 12px `#444` tick labels "40", "80", ..., "320" every 40; y axis = accuracy 0.60 to 0.90 with light `#e5e9ef` gridlines and 12px `#444` labels "60%", "70%", "80%", "90%".
- **Shared x points for both curves:** `[40, 80, 120, 160, 200, 240, 280, 320]`.
- **Random curve:** blue `#2a78d6` 3px line with 5px dots, accuracy = `[0.62, 0.68, 0.72, 0.75, 0.77, 0.79, 0.80, 0.81]`; 12px blue label "random labels" near its right end.
- **Active curve:** green `#008300` 3px line with 5px dots, accuracy = `[0.62, 0.73, 0.79, 0.83, 0.85, 0.86, 0.87, 0.87]`; bold 12px green label "active picking" near its right end.
- **Gap marker:** vertical dashed `#6b7280` (dash 4/3) line at x=200 from the blue point (0.77) to the green point (0.85); bold 13px green label to its left: "at 200 labels: 85% vs 77%".
- **Start marker:** 7px `#6b7280` dot at (40, 0.62) with 11px `#6b7280` label "same 40-label start".
- **Caption (12px `#444`, bottom right):** "illustrative — accuracy measured on a separate random test set".

## The Labeled Pile Is Weird on Purpose

**Tags:** `common mistake` (red), `sampling bias` (orange)

- **The pile** — after active picking, the 200 labeled emails are the hardest, most borderline ones in the shop
- **The trap** — someone counts the labels: 96 of 200 are refunds (48%) and reports "half our email is refunds"
- **The truth** — in a random sample the refund share is 12%; the picked pile was never meant to represent
- **Why it skews** — borderline emails are refund-like by construction; easy "other" emails were skipped
- **Same trap, twice** — measuring accuracy on the picked pile fails the same way; it is all hard cases
- **The fix** — keep one small untouched random sample for any statistic: rates, shares, accuracy, error cost

*Example (italic):* The owner's actively labeled pile says 48% refunds while a plain random sample of 100 emails says 12% — the pile is a study set of hard cases, not a survey.

**Common mistake:** Treating the actively labeled set as a mirror of the data. It was deliberately filled with confusing cases, so never estimate class rates or accuracy from it — keep a separate random sample for that.

### Visualization (canvas `c4`, 720×300)

Single-panel grouped bar chart: refund share measured two ways — in the actively picked labeled pile versus in a plain random sample — showing the picked pile wildly overstating the true rate.

- **Title (bold 15px, `#1a5276`, top center):** "Refund Share: the Picked Pile Lies, the Random Sample Doesn't".
- **Axes:** origin x=90, baseline y=245, plot width 540, plot height 175; y axis = refund share 0% to 60% with light `#e5e9ef` gridlines at 10% steps and 12px `#444` labels.
- **Bars:** two bars 120px wide centered at x=260 and x=500 — "actively picked pile (200 emails)" at 48% filled orange `rgba(217,89,38,0.45)` with 2px `#d95926` stroke, "random sample (100 emails)" at 12% filled blue `rgba(42,120,214,0.35)` with 2px `#2a78d6` stroke; category labels 12px `#444` below the baseline, two lines each.
- **Value labels:** bold 15px above each bar in its stroke color: "48%" and "12%".
- **Truth line:** horizontal dashed green `#008300` (dash 4/3) line at 12% across the plot; 12px green label "true refund share ≈ 12%" at its right end.
- **Annotation (bold 13px magenta `#d55181`, near x=330, y=95):** two lines: "the pile is hard cases on purpose —" / "never read a rate off it".
- **Caption (12px `#444`, bottom right):** "illustrative — 96 refunds among the 200 hand-picked hard emails".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all histogram counts, scores, distances, learning-curve points, and bar values are the hardcoded arrays above (no randomness); the c1 bin counts sum to exactly 5,000, c2 distances are exactly |score − 0.5|, and the text's headline numbers (400 unsure emails, picks E/D/F, 85% vs 77% at 200 labels, 48% vs 12%) must match the charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
