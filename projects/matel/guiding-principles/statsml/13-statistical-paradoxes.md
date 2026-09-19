# Statistical Paradoxes

**Page type:** grid page (card navigation grid, 3 columns, philosophy callouts before and after the grid)
**HTML title tag:** Statistical Paradoxes

**Subtitle:** Ways that good data leads a careful person to the wrong answer — each one a different piece of hidden structure that intuition does not account for.

## Callout (philosophy box, before grid)

**Why these matter:** Each of these has reversed a real decision — a treatment declared better, a channel declared dead, a model shipped against the wrong label. None of them involve bad data or a miscount. The arithmetic is right and the conclusion is still wrong, which is what makes them hard to catch.

## Cards

Each card links to a detail page under `statistical-paradoxes/`. The card shows a colored
uppercase category label, a numbered title matching the file index, a one-line description, and
exactly 2 tags. **Tags name the statistical mechanism** (the confound, the bias, the estimator that
misbehaves) — not paraphrases of the description. They are the terms you would search to find the
literature on that paradox, so a reader who knows the term recognizes the failure mode from the tag
alone.

| # | Category | Title | Link | Description | Tags |
|---|----------|-------|------|-------------|------|
| 1 | AGGREGATION | Simpson's Paradox | [13-statistical-paradoxes/01-simpsons-paradox.md](13-statistical-paradoxes/01-simpsons-paradox.md) | Drill into the data and the losing treatment wins — in mild cases and in severe ones. | confounding variable · unequal group weights |
| 2 | PROBABILITY | Base Rate Fallacy | [13-statistical-paradoxes/02-base-rate-fallacy.md](13-statistical-paradoxes/02-base-rate-fallacy.md) | A 99% accurate test flags you for a rare disease, and is still probably wrong. | low prevalence · positive predictive value |
| 3 | SAMPLING | Berkson's Paradox | [13-statistical-paradoxes/03-berksons-paradox.md](13-statistical-paradoxes/03-berksons-paradox.md) | Among patients in a hospital, two unrelated diseases look like they rule each other out. | collider bias · selection on outcome |
| 4 | MEASUREMENT | Regression to the Mean | [13-statistical-paradoxes/04-regression-to-the-mean.md](13-statistical-paradoxes/04-regression-to-the-mean.md) | Last year's top branches slip and the worst ones rebound, with no cause behind either. | imperfect correlation · selection on extremes |
| 5 | AGGREGATION | Ecological Fallacy | [13-statistical-paradoxes/05-ecological-fallacy.md](13-statistical-paradoxes/05-ecological-fallacy.md) | Richer districts lean one way at the ballot box, so we assume richer people do too. | unit of analysis · within-group variance |
| 6 | MEASUREMENT | Multiple Comparisons | [13-statistical-paradoxes/06-multiple-comparisons.md](13-statistical-paradoxes/06-multiple-comparisons.md) | Test a hundred variants and one comes back a winner. Did it earn it? | family-wise error · false discovery rate |
| 7 | SAMPLING | Survivorship Bias | [13-statistical-paradoxes/07-survivorship-bias.md](13-statistical-paradoxes/07-survivorship-bias.md) | Study the funds that still exist and almost every strategy looks profitable. | attrition · truncated sample |
| 8 | AGGREGATION | Will Rogers Phenomenon | [13-statistical-paradoxes/08-will-rogers-phenomenon.md](13-statistical-paradoxes/08-will-rogers-phenomenon.md) | Move one patient from one hospital to another and both hospitals' averages rise. | stage migration · conditional means |
| 9 | MEASUREMENT | Lindley's Paradox | [13-statistical-paradoxes/09-lindleys-paradox.md](13-statistical-paradoxes/09-lindleys-paradox.md) | A big enough sample calls an effect real when the effect is far too small to matter. | effect size vs p-value · overpowered test |
| 10 | SAMPLING | Inspection Paradox | [13-statistical-paradoxes/10-inspection-paradox.md](13-statistical-paradoxes/10-inspection-paradox.md) | Ask passengers how full their train was and every train sounds crowded. | length-biased sampling · wait-time bias |
| 11 | INCENTIVES | Goodhart's Law | [13-statistical-paradoxes/11-goodharts-law.md](13-statistical-paradoxes/11-goodharts-law.md) | Pay a support team for closed tickets, and tickets start closing. | proxy metric · optimization pressure |
| 12 | PROBABILITY | Birthday Paradox | [13-statistical-paradoxes/12-birthday-paradox.md](13-statistical-paradoxes/12-birthday-paradox.md) | Your ID space has room for millions, yet collisions start showing up in the thousands. | pairwise collisions · keyspace size |
| 13 | PROBABILITY | Monty Hall Problem | [13-statistical-paradoxes/13-monty-hall-problem.md](13-statistical-paradoxes/13-monty-hall-problem.md) | A game show prize sits behind one of three doors. You pick one, an empty one opens. Switch? | conditional probability · informed elimination |
| 14 | PROBABILITY | Lottery Paradox | [13-statistical-paradoxes/14-lottery-paradox.md](13-statistical-paradoxes/14-lottery-paradox.md) | Each of a million tickets is almost certain to lose, yet one of them is certain to win. | joint error rate · per-claim vs set-wide |

## Callout (philosophy box, after grid)

**The common thread:** In every case something shaped the data before anyone saw it — a filter, a grouping, a prevalence, an incentive, a sampling method. The defences: ask what the entry rule was, look for the records that are missing, check how common the thing actually is, count how many things were tested, and state the conclusion at the grain the data really has.

## Regeneration instructions

- **Template:** nav-grid style (`docs/statsml/ui-templates/02-nav-grid`). Single page: h1,
  `.subtitle`, a `.philosophy` callout, one `.grid` of `.card` anchors, a closing `.philosophy`.
- **Layout:** `.grid` is CSS grid, `repeat(3, 1fr)`, 16px gap, margin `16px 0 30px`; 2 columns
  below 1100px, 1 column below 600px.
- **Card summary:** one line stating the SITUATION the reader is about to walk into, not the
  resolution and not the mechanism. ~65–90 characters, one sentence, no statistics.
- **Card structure:** `<a class="card">` containing `.card-label` (colored uppercase category),
  `<h3>N. Title</h3>` with the unpadded index matching the filename, a one-line `<p>`, then a
  `.tags` row of `.topic-tag` pills.
- **Category label colors:** AGGREGATION `#8e44ad`; PROBABILITY `#e67e22`; SAMPLING `#1a5276`;
  MEASUREMENT `#27ae60`; INCENTIVES `#e74c3c`.
- **`.topic-tag`:** inline-block, 0.75em weight 500, background `#f4ecf7`, text `#6c3483`, padding
  2px 8px, radius 9px, margin `6px 4px 0 0`. Purple-tinted, deliberately distinct from the blue
  `.philosophy`/card palette — the earlier `#f0f4f8`/`#4a6b82` pill was too low-contrast to read at
  this size.
- **Card style:** background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 16px;
  hover shadow `0 4px 12px rgba(0,0,0,0.1)` and border `#2980b9`. Label 0.72em bold uppercase
  letter-spacing 0.5px; h3 `#1a5276` 1.0em; description 0.85em `#555`.
- **Callout style:** `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`,
  padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white, text `#2a2a2a`, padding 40px 20px, line-height
  1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em, 30px bottom margin.
- **Descriptions carry no statistics.** The previous cards quoted figures ("91% of its alerts are
  false", "2.5 expected false positives") that duplicated the detail pages and could drift from
  them. One plain-language line per card instead; the numbers live on the detail page.
- **No item counts** in the subtitle or the callouts.
- **Links:** cards only — `.md` links to sibling `.md`, `.html` to `.html`. No back/home links,
  no `.nav`.
