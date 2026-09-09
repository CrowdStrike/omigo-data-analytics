# Statistical Paradoxes

**Page type:** grid page (card navigation grid, 3 columns, philosophy callouts before and after the grid)
**HTML title tag:** Statistical Paradoxes

**Subtitle:** Ways that good data leads a careful person to the wrong answer — each one a different piece of hidden structure that intuition does not account for.

## Callout (philosophy box, before grid)

**Why these matter:** Each of these has reversed a real decision — a treatment declared better, a channel declared dead, a model shipped against the wrong label. None of them involve bad data or a miscount. The arithmetic is right and the conclusion is still wrong, which is what makes them hard to catch.

## Cards

Each card links to a detail page under `statistical-paradoxes/`. The card shows a colored
uppercase category label, a numbered title matching the file index, a one-line description, and
2–3 layman keyword tags.

| # | Category | Title | Link | Description | Tags |
|---|----------|-------|------|-------------|------|
| 1 | AGGREGATION | Simpson's Paradox | [13-statistical-paradoxes/01-simpsons-paradox.md](13-statistical-paradoxes/01-simpsons-paradox.md) | The winner of every round can still lose the match. | every subgroup · group sizes · reversal |
| 2 | PROBABILITY | Base Rate Fallacy | [13-statistical-paradoxes/02-base-rate-fallacy.md](13-statistical-paradoxes/02-base-rate-fallacy.md) | Hunt something rare and a great test still cries wolf. | rare things · false alarms · alert queues |
| 3 | SAMPLING | Berkson's Paradox | [13-statistical-paradoxes/03-berksons-paradox.md](13-statistical-paradoxes/03-berksons-paradox.md) | Who gets through the door invents a trade-off nobody had. | entry rules · missing corner · fake link |
| 4 | MEASUREMENT | Regression to the Mean | [13-statistical-paradoxes/04-regression-to-the-mean.md](13-statistical-paradoxes/04-regression-to-the-mean.md) | Nobody got worse — the luck just ran out. | extremes · noise · false credit |
| 5 | AGGREGATION | Ecological Fallacy | [13-statistical-paradoxes/05-ecological-fallacy.md](13-statistical-paradoxes/05-ecological-fallacy.md) | The average belongs to the group, not to anyone in it. | group vs person · coarse joins · wrong grain |
| 6 | MEASUREMENT | Multiple Comparisons | [13-statistical-paradoxes/06-multiple-comparisons.md](13-statistical-paradoxes/06-multiple-comparisons.md) | Search long enough and noise hands you a winner. | many tests · dashboards · forking paths |
| 7 | SAMPLING | Survivorship Bias | [13-statistical-paradoxes/07-survivorship-bias.md](13-statistical-paradoxes/07-survivorship-bias.md) | The failures left no forwarding address. | vanished cases · highlight reel · denominators |
| 8 | AGGREGATION | Will Rogers Phenomenon | [13-statistical-paradoxes/08-will-rogers-phenomenon.md](13-statistical-paradoxes/08-will-rogers-phenomenon.md) | Move one case across a line and both averages rise. | reclassifying · tier metrics · flat totals |
| 9 | MEASUREMENT | Lindley's Paradox | [13-statistical-paradoxes/09-lindleys-paradox.md](13-statistical-paradoxes/09-lindleys-paradox.md) | "Not chance" can still mean "nothing happened". | huge samples · tiny effects · thresholds |
| 10 | SAMPLING | Inspection Paradox | [13-statistical-paradoxes/10-inspection-paradox.md](13-statistical-paradoxes/10-inspection-paradox.md) | Everything looks bigger when you bump into it. | waiting times · snapshots · length bias |
| 11 | INCENTIVES | Goodhart's Law | [13-statistical-paradoxes/11-goodharts-law.md](13-statistical-paradoxes/11-goodharts-law.md) | The number you pay for stops telling you the truth. | targets · proxy labels · guard metrics |
| 12 | PROBABILITY | Birthday Paradox | [13-statistical-paradoxes/12-birthday-paradox.md](13-statistical-paradoxes/12-birthday-paradox.md) | Your ID space runs out long before you think it does. | collisions · pairs not items · hash width |
| 13 | PROBABILITY | Monty Hall Problem | [13-statistical-paradoxes/13-monty-hall-problem.md](13-statistical-paradoxes/13-monty-hall-problem.md) | Who removed the option decides where its odds go. | elimination · what they knew · redistribution |
| 14 | PROBABILITY | Lottery Paradox | [13-statistical-paradoxes/14-lottery-paradox.md](13-statistical-paradoxes/14-lottery-paradox.md) | Each claim is safe; the whole set is certainly wrong. | many claims · joint confidence · pipelines |

## Callout (philosophy box, after grid)

**The common thread:** In every case something shaped the data before anyone saw it — a filter, a grouping, a prevalence, an incentive, a sampling method. The defences: ask what the entry rule was, look for the records that are missing, check how common the thing actually is, count how many things were tested, and state the conclusion at the grain the data really has.

## Regeneration instructions

- **Template:** nav-grid style (`docs/statsml/ui-templates/02-nav-grid`). Single page: h1,
  `.subtitle`, a `.philosophy` callout, one `.grid` of `.card` anchors, a closing `.philosophy`.
- **Layout:** `.grid` is CSS grid, `repeat(3, 1fr)`, 16px gap, margin `16px 0 30px`; 2 columns
  below 900px, 1 column below 500px.
- **Card structure:** `<a class="card">` containing `.card-label` (colored uppercase category),
  `<h3>N. Title</h3>` with the unpadded index matching the filename, a one-line `<p>`, then a
  `.tags` row of `.topic-tag` pills.
- **Category label colors:** AGGREGATION `#8e44ad`; PROBABILITY `#e67e22`; SAMPLING `#1a5276`;
  MEASUREMENT `#27ae60`; INCENTIVES `#e74c3c`.
- **`.topic-tag`:** inline-block, 0.7em, background `#f0f4f8`, text `#4a6b82`, padding 1px 7px,
  radius 9px, margin `6px 4px 0 0`.
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
