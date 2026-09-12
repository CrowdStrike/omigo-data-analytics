# Boosting

**Page type:** detail page (tutorial card-sections: h2 + two-column table.layout, text left 50% / canvas right 50%)
**HTML title tag:** Boosting

**Subtitle:** Train simple rules one after another, and make each new rule focus on the emails the previous ones got wrong — many weak rules add up to one strong filter

## Train a Rule, Circle the Misses, Train Again

**Tags:** `core idea` (blue), `running example` (green)

- **One weak rule** — "spam if it contains FREE" gets only 7 of 10 emails right
- **Circle the misses** — the 3 emails it got wrong get double weight
- **Train the next rule** — with the misses counting twice, rule 2 must fix them
- **Repeat** — every round, whatever is still wrong grows in weight
- **Vote at the end** — the rules vote together, more accurate rules speak louder

*Example (italic):* Three so-so rules — FREE, unknown sender, too many "!" — vote together and beat any one of them alone.

**Key point callout:** **Key point:** Boosting trains weak rules in sequence, growing the weight of misclassified points each round, so every new rule concentrates on what is still wrong.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three rule boxes chained by reweight arrows into a vote box.

- **Title (bold 15px, `#1a5276`, top center):** "Each Rule Trains on the Previous Rules’ Misses"
- **Rule boxes (150×62 at y=80, faint `rgba(0,0,0,0.02)` fill, 2px colored border; bold 13px colored name, 12px `#333` rule text, bold 12px `#c0392b` miss line below the box):**
  - Rule 1 at x=30, blue `#2a78d6`: "Rule 1" / "\"contains FREE\"" / "misses: S4 S5 L2"
  - Rule 2 at x=230, aqua `#199e70`: "Rule 2" / "\"unknown sender\"" / "misses: S1 L4 L5"
  - Rule 3 at x=430, violet `#4a3aa7`: "Rule 3" / "\"3+ ! marks\"" / "misses: S2 S3 L2"
- **Arrows between rules:** orange `#d95926` width 2 with filled triangular heads; above each, bold 11px orange two-line label: "double the" / "misses’ weight".
- **Vote box:** 90×98 at (615,62), fill `rgba(0,131,0,0.10)`, green `#008300` 2px border; bold 13px green "VOTE", bold 15px green "9 / 10", 11px `#333` "all 3 rules" / "weighted". Green arrow with triangular head from Rule 3 to the vote box.
- **Bottom captions (centered):** 12px `#444` "each rule is weak alone — the sequence is the strength"; bold 13px orange "weak + weak + weak → strong: every rule alone scores 7/10, the vote scores 9/10".

## Ten Emails, Three Rounds, by Hand

**Tags:** `worked example` (green), `core idea` (blue)

- **The data** — 10 emails: 5 spam (S1–S5), 5 legit (L1–L5), each starting at weight 1
- **Round 1** — "contains FREE": wrong on S4, S5, L2 → their weights double to 2
- **Round 2** — "sender not in contacts": fixes S4, S5 but is wrong on S1, L4, L5 → doubled
- **Round 3** — "3 or more ! marks": wrong on S2, S3, L2
- **Final vote** — the three rules together get 9 of 10 right; each alone got just 7

*Example (italic):* S4 is spam without the word FREE — it fooled rule 1, so its doubled weight made rule 2 treat it like two emails.

**Key point callout:** **Key point:** Doubling a missed email's weight makes it count twice in the next round's error — the next rule literally cannot afford to ignore it.

### Visualization (canvas `c2`, 720×300)

Check/cross grid: 10 email columns × 3 rule rows plus a VOTE row.

- **Title (bold 15px, `#1a5276`, top center):** "Who Gets Each Email Right — and Whose Weight Doubles"
- **Column headers (bold 12px):** S1–S5 in magenta `#d55181`, L1–L5 in blue `#2a78d6`. Key line above: bold 11px magenta "S = spam", blue "L = legit", and right-aligned orange `#d95926` "✗ doubles that email’s weight for the next round".
- **Rows (row labels bold 12px `#1a5276`, right-aligned; grid starts x=130, cell 52×30):**
  - "R1: FREE" (y=78) — wrong on S4, S5, L2
  - "R2: sender" (y=116) — wrong on S1, L4, L5
  - "R3: ! marks" (y=154) — wrong on S2, S3, L2
  - "VOTE" (y=208, below a dashed `#bdc3c7` separator line) — wrong only on L2
- **Cells:** correct = "✓" bold 13px green `#008300` on fill `rgba(0,131,0,0.10)` with green 1px border; wrong = "✗" bold 13px `#e74c3c` on fill `rgba(231,76,60,0.15)` with `#e74c3c` border.
- **Caption (bold 13px green, bottom center):** "each rule alone gets just 7 of 10 — the vote gets 9, only L2 fools two of three rules"

## Where a Data Scientist Meets Boosting

**Tags:** `where it's used` (blue), `watch out` (orange)

- **Stumps everywhere** — real boosters use one-split "stump" trees as the weak rules
- **Bias killer** — a weak rule underfits alone; boosting stacks fixes until the gap closes
- **Tabular champion** — boosted trees still win most contests on spreadsheet-style data
- **Sequential cost** — round 2 needs round 1's misses, so rounds cannot run in parallel
- **Too many rounds** — once the signal is learned, later rounds start memorizing noise

*Example (italic):* On held-out emails, accuracy here peaks near round 10 — more rounds keep helping training data only.

**Key point callout:** **Key point:** Each round makes the fit stronger, so watch accuracy on held-out data and stop when it stops improving.

### Visualization (canvas `c3`, 720×300)

Dual line chart: training vs held-out accuracy by boosting rounds, with a best-stop marker.

- **Title (bold 15px, `#1a5276`, top center):** "More Rounds Always Help Training — Not Held-Out Data"
- **X axis:** rounds `[1, 2, 3, 5, 10, 20, 30]` evenly spaced, 12px `#222` labels, `#444` axis label "boosting rounds"; **Y axis:** accuracy scaled 60–105 with tick labels 60%–100% (`#444` 12px) and light `#e5e9ef` horizontal gridlines. L-shaped `#999` axes; padding top 52, bottom 52, left 62, right 170.
- **Series (width-3 lines with 4px dots):**
  - training accuracy, blue `#2a78d6`: `[70, 80, 90, 95, 99, 100, 100]`
  - held-out accuracy, orange `#d95926`: `[70, 79, 88, 91, 93, 90, 88]`
- **Best-stop marker:** dashed green `#008300` vertical line (dash 5/4, width 2) at round 10; bold 13px green label above the plot: "held-out peak: 93% at round 10 — stop here".
- **Annotation (bold 12px `#c0392b`, near the falling tail):** "after the peak: memorizing noise"
- **Legend (right margin, 12px squares):** blue "training accuracy", orange "held-out accuracy".

## The Mix-Up: Boosting Is Not Bagging

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Bagging** — trains many models in parallel on random samples, then averages equal votes
- **Boosting** — trains in sequence; each model exists only to fix the previous misses
- **Order test** — shuffle a bagged forest's trees and nothing changes; boosting rounds build on each other
- **Different cures** — bagging calms a model that overfits; boosting strengthens one that underfits
- **Same family** — both are ensembles of many models, with opposite training recipes

*Example (italic):* Random forest is bagging; AdaBoost and the gradient boosting family are boosting.

**Key point callout:** **Common mistake:** Treating "many trees" as one idea — independent trees averaged is bagging; dependent trees trained on each other's mistakes is boosting.

### Visualization (canvas `c4`, 720×300)

Two-panel architecture diagram: bagging (parallel) vs boosting (chain), split by a dashed `#bdc3c7` vertical divider at x=360 (dash 4/3). Boxes have faint `rgba(0,0,0,0.02)` fill, 2px colored border, bold 12px colored centered label; arrows have filled triangular heads.

- **Title (bold 15px, `#1a5276`, top center):** "Bagging Trains Side by Side — Boosting Trains in a Chain"
- **Left panel:** header bold 13px blue `#2a78d6` "BAGGING (e.g. random forest)"; three blue boxes 110×34 stacked at x=40 (y=70/120/170) labeled "model A", "model B", "model C"; gray `#6b7280` arrows converging into a green `#008300` box 110×40 labeled "average".
  Captions centered at x=180: 12px `#444` "independent: each trains on its own" / "random sample — order never matters"; bold 12px blue "cure for overfitting (variance)".
- **Right panel:** header bold 13px orange `#d95926` "BOOSTING (e.g. AdaBoost, XGBoost)"; three violet `#4a3aa7` boxes 84×34 in a row at y=100 (x=390/510/630) labeled "rule 1", "rule 2", "rule 3"; orange arrows between them with bold 11px orange two-line labels "fix rule 1’s" / "misses" and "fix what’s" / "still wrong"; gray arrows from all three down into a green box 150×40 labeled "weighted vote" at (475,165).
  Captions centered at x=540: 12px `#444` "dependent: each rule trains on the" / "previous ones’ reweighted misses"; bold 12px orange "cure for underfitting (bias)".

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (per `tutorials/CLAUDE.md`, modeled on `most-powerful-signals/07-social-graph-connections.html`): h1 + `.subtitle`, then four `.card-section` blocks, each `<h2>` + `table.layout` with `.text-col` (50%) holding text and `.viz-col` (50%) holding one canvas.
- **Left column structure per section:** `.tags` pill row, then `<ul>` of one-line bullets each opening with `<b>bold term</b> —`, one italic `.example` paragraph, one `.key-point` callout with a `<strong>` lead.
- **Tag pill classes:** `.tag.blue` bg `rgba(26,82,118,0.12)` text `#1a5276`; `.tag.green` bg `rgba(39,174,96,0.15)` text `#27ae60`; `.tag.red` bg `rgba(231,76,60,0.12)` text `#e74c3c`; `.tag.orange` bg `rgba(230,126,34,0.15)` text `#e67e22`. Pills 0.72rem, weight 600, padding 2px 10px, radius 10px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px bottom border `#2980b9`; section h2 1.3rem `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 0.95rem; bullets 0.92rem with `li b` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` bg `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas JS:** shared palette object `P` (blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`); shared `setup(id)` helper sized 720×300 that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette reference:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card/grid links use `.html` extensions (this page has no links).
