# Thresholds & Tradeoffs

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks each with a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Thresholds &amp; Tradeoffs

**Subtitle:** A model outputs a score, not a decision — where you draw the cutoff line decides who gets flagged, and moving it trades one kind of mistake for another

## One Fraud Model, Three Different Fraud Systems

Tags: `core idea` (blue), `running example` (green)

- **The setup** — a fraud model scores 1,000 card transactions from 0 (safe) to 1 (fishy)
- **The truth** — 50 of the 1,000 really are fraud; the model doesn't know which
- **The cutoff** — you pick a line: every score above it gets flagged for review
- **Same model, three systems** — cutoff 0.3 flags 120, cutoff 0.5 flags 60, cutoff 0.8 flags 20
- **Nothing retrained** — the scores never changed; only the line moved

*Example:* Like a smoke alarm's sensitivity dial: the same sensor can wake you for burnt toast or sleep through a fire.

**Key point — Threshold:** the score above which you act. The model gives you the ranking; the threshold is a separate choice you make on top of it.

### Visualization (canvas `c1`, 720×300)

Paired histogram of the two score distributions with three dashed cutoff lines.

- **Title (bold 16px, `#1a5276`, top center):** "Where the 1,000 Scores Land — and Three Places to Cut".
- **Data (10 bins, 0.0–0.1 … 0.9–1.0):** legit counts `[520, 240, 115, 33, 20, 10, 6, 4, 1, 1]` (950 total); fraud counts `[1, 2, 2, 3, 4, 6, 7, 7, 9, 9]` (50 total). Each series is scaled to its own peak (legit max 520, fraud max 9; fraud bars drawn at 85% of chart height).
- **Layout:** padding top 55, bottom 55, left 55, right 25; gray `#999` L-shaped axes. Within each bin, legit bar occupies the left half (fill `rgba(42,120,214,0.35)`), fraud bar the right half (fill `rgba(213,81,129,0.55)`).
- **X axis:** labels 0.0, 0.2, 0.4, 0.6, 0.8, 1.0 in muted gray `#6b7280`, 12px; axis title "model score" below.
- **Cutoff lines:** vertical dashed (dash 6/4, width 2.5) at score positions with bold 12px labels stacked to the right of each line: 0.3 in green `#008300` labeled "0.3: flag 120"; 0.5 in orange `#d95926` labeled "0.5: flag 60"; 0.8 in violet `#4a3aa7` labeled "0.8: flag 20".
- **Legend (upper right area):** swatch `rgba(42,120,214,0.6)` "legit (950)"; swatch `rgba(213,81,129,0.8)` "fraud (50)"; muted 11px note "each scaled to its own peak".
- **Annotation (bold 13px red `#e74c3c`, near the 0.3 cutoff):** two lines "the two piles overlap —" / "no line separates them cleanly".

## Counting What Each Cutoff Does to the 1,000

Tags: `worked example` (green), `arithmetic` (blue)

- **Cutoff 0.3** — flags 120: catches 45 of 50 frauds, but 75 flags are innocent customers
- **Cutoff 0.5** — flags 60: catches 38 frauds, 22 false alarms, 12 frauds slip through
- **Cutoff 0.8** — flags 20: catches 18 frauds, only 2 false alarms, but 32 frauds slip through
- **Check it** — flagged = caught + false alarms: 45+75=120, 38+22=60, 18+2=20
- **The trade** — lower cutoff = more fraud caught AND more innocents bothered, always together

*Example:* Slide from 0.8 down to 0.3 and you catch 27 more frauds — at the price of 73 more false alarms.

**Key point — No free lunch:** no cutoff wins on everything. Every position buys fewer misses with more false alarms, or the reverse.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: three counts per cutoff.

- **Title (bold 16px `#1a5276`):** "What Each Cutoff Does (1,000 transactions, 50 frauds)".
- **Data (three groups, three bars each, bar width 38px):**
  - cutoff 0.3: caught 45, false alarms 75, missed 5
  - cutoff 0.5: caught 38, false alarms 22, missed 12
  - cutoff 0.8: caught 18, false alarms 2, missed 32
- **Series colors:** fraud caught green `#008300`; false alarms yellow `#c98500`; fraud missed red `#e74c3c`.
- **Axes:** y 0–80, ticks every 20 with horizontal gridlines `#e5e9ef`; padding top 55, bottom 60, left 55, right 165; gray `#999` axes. Bold 12px value labels above each bar in `#2c3e50`; bold 13px group labels "cutoff 0.3" / "cutoff 0.5" / "cutoff 0.8" below the baseline.
- **Legend (right column, x = width−150):** the three series swatches with labels.
- **Annotation (bold 12px red `#e74c3c`, below legend, three lines):** "catch more fraud" / "= bother more" / "innocents. Always.".

## The Threshold Is a Business Decision

Tags: `why it matters` (red), `costs` (orange)

- **Price the mistakes** — say a missed fraud costs $500 and a manual review costs $10
- **Cutoff 0.3** — 5 misses × $500 + 120 reviews × $10 = $2,500 + $1,200 = $3,700
- **Cutoff 0.5** — 12 × $500 + 60 × $10 = $6,000 + $600 = $6,600
- **Cutoff 0.8** — 32 × $500 + 20 × $10 = $16,000 + $200 = $16,200
- **Flip the costs** — if reviews cost $200 (angry customers leave), the best cutoff moves up

*Example:* A hospital screening test and a spam filter use the same math but should never use the same cutoff.

**Key point — Why it matters:** the data scientist supplies the score curve; the cost of each mistake decides where to cut. That is a business call, made with numbers.

### Visualization (canvas `c3`, 720×300)

Stacked bar chart: total cost per cutoff, two cost components.

- **Title (bold 16px `#1a5276`):** "Total Cost per Cutoff ($500 per miss, $10 per review)".
- **Data (three stacks, bar width 96px):**
  - cutoff 0.3: missed-fraud cost $2,500 + review cost $1,200; total label "$3,700"
  - cutoff 0.5: $6,000 + $600; total label "$6,600"
  - cutoff 0.8: $16,000 + $200; total label "$16,200"
- **Stack colors:** missed fraud cost red `#e74c3c` (bottom); review cost yellow `#c98500` (top). Bold 13px total labels above each stack; group names below baseline.
- **Axes:** y 0–$18k, ticks at $0k/$6k/$12k/$18k with gridlines `#e5e9ef`; padding top 55, bottom 60, left 70, right 165.
- **Winner marker:** bold 13px green `#008300` "cheapest" above the cutoff 0.3 stack.
- **Legend (right column):** red swatch "missed fraud cost", yellow swatch "review cost".
- **Annotation (bold 12px red `#e74c3c`, four lines):** "with these costs the" / "low cutoff wins —" / "change the costs and" / "the winner changes".

## 0.5 Is a Habit, Not a Law

Tags: `common mistake` (orange), `rule of thumb` (green)

- **The default trap** — libraries cut at 0.5 out of the box; teams ship it without asking
- **Precision moves** — share of flags that are real fraud: 38% at 0.3, 63% at 0.5, 90% at 0.8
- **Recall moves opposite** — share of fraud caught: 90% at 0.3, 76% at 0.5, 36% at 0.8
- **One number hides this** — "accuracy at 0.5" collapses a whole dial into one arbitrary point
- **Do this instead** — plot the trade across cutoffs, then pick the point that fits your costs

*Example:* The team "improved" the model for a month when moving the cutoff would have done the same in an afternoon.

**Key point — Common confusion:** treating 0.5 as part of the model. The model ends at the score; the cutoff belongs to you.

### Visualization (canvas `c4`, 720×300)

Two-series line chart: precision and recall versus cutoff.

- **Title (bold 16px `#1a5276`):** "The Dial Behind the Default: Precision vs Recall".
- **Data:** cutoffs `[0.3, 0.5, 0.8]`; precision `[38, 63, 90]` % (45/120, 38/60, 18/20); recall `[90, 76, 36]` % (45/50, 38/50, 18/50).
- **Axes:** y 0–100%, ticks every 25% with gridlines `#e5e9ef`; x mapped from 0.2 to 0.9 with tick labels 0.3 / 0.5 / 0.8 and axis title "cutoff"; padding top 55, bottom 55, left 60, right 170.
- **Default marker:** vertical dashed gray `#6b7280` line (dash 4/4, width 1.5) at x=0.5, labeled "the library default" above it.
- **Series:** precision in violet `#4a3aa7`, recall in aqua `#199e70`; line width 3, dots radius 5, bold 12px percentage labels at each point (precision labels below points, recall labels above).
- **Legend (right column):** violet swatch "precision", aqua swatch "recall"; in text column left labels read "precision (flags that are real)" and "recall (fraud that gets caught)".
- **Annotation (bold 12px red `#e74c3c`, four lines under legend):** "0.5 is just one point" / "on this dial — pick" / "yours by cost, not" / "by habit".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials style, per `tutorials/CLAUDE.md`). Structure: `<h1>` (no index number), `.subtitle` paragraph, then 4 `.card-section` divs each containing `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Text column structure per section:** `.tags` row of colored pills (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22), then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold in `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem, with `<strong>` lead).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.card-section h2` 1.3rem `#1a5276` with 2px `#2980b9` bottom border; table cells padding 12px, vertical-align top; canvases `width:100%`, 1px solid `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Hardcoded literal data arrays, no `Math.random()`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; red reserved for error/alarm annotations.
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
