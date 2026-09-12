# Why Ensembles Win

**Page type:** detail page (tutorial card-sections: one h2 per section, two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** Why Ensembles Win

**Subtitle:** Five so-so models that are wrong on DIFFERENT rows outvote their own mistakes — the gain comes entirely from disagreement, and averaging identical models buys nothing

## Five Mediocre Models, One Good Answer

Tags: `core idea` (blue), `running example` (green)

- **Five models** — each scores 70% on the same 10 rows: mediocre, all of them
- **Different mistakes** — each one is wrong on a different set of 3 rows
- **Vote per row** — take the answer that 3 or more of the 5 models give
- **The outvote** — on most rows only 1 or 2 models err, and the majority buries them
- **The score** — the vote gets 9 of 10 rows right; no member got more than 7

*Example (italic):* It works like five friends guessing trivia: each knows 70% of the answers, but they blank on different questions.

**Key point:** An ensemble wins when its members' errors land on different rows — each mistake is a minority opinion, and the vote overrules it.

### Visualization (canvas `c1`, 720×300)

Bar chart: five member accuracies plus the vote.

- **Title (bold 15px, `#1a5276`, top center):** "No Member Beats 70% — the Vote Scores 90%".
- **Data:** labels `[M1, M2, M3, M4, M5, VOTE]`, values `[70, 70, 70, 70, 70, 90]` (%); bar colors blue `#2a78d6`, aqua `#199e70`, violet `#4a3aa7`, magenta `#d55181`, yellow `#c98500`, green `#008300` (VOTE); alpha 0.6 for members, 0.85 for VOTE.
- **Layout:** plot from x=80, width 570; baseline y=240, chart height 180, y scale 0–100; bars 70px wide, evenly gapped. Y labels "0%", "50%", "70%", "100%" in gray `#444`; dashed light-gray `#bbb` reference line (dash 4/3) at 70%.
- **Labels:** bold colored value labels ("70%", "90%") above each bar; bold dark model labels below.
- **Captions:** gray 12px centered "five models on the same 10 rows — 7 right each, wrong rows mostly different" under the axis; bold green 13px "the vote fixes what only a minority gets wrong" at (250, 68).

## Score the Vote by Hand: 10 Rows, 5 Models

Tags: `worked example` (green), `core idea` (blue)

- **The errors** — M1 is wrong on rows 1,2,3; M2 on 3,4,5; M3 on 5,6,7; M4 on 7,8,9; M5 on 3,9,10
- **Row 6** — only M3 errs: the vote is 4 against 1, correct
- **Row 9** — M4 and M5 err: the vote is 3 against 2, still correct
- **Row 3** — M1, M2 and M5 all err: the vote is 2 against 3, the vote fails
- **Tally** — 9 of 10 rows correct from five members that each scored 7 of 10

*Example (italic):* Count the ✗ marks in any row of the chart: the vote is wrong only where the ✗s pile up to 3 or more.

**Key point:** The vote fails only where a majority errs together — row 3 is the one row where three error sets happen to overlap.

### Visualization (canvas `c2`, 720×300)

10×5 correctness grid (✓/✗ cells) with a VOTE column.

- **Title (bold 15px, `#1a5276`, top center):** "Row by Row: the Vote Fails Only Where 3 Error Sets Overlap".
- **Error sets:** M1 errs on rows {1,2,3}; M2 {3,4,5}; M3 {5,6,7}; M4 {7,8,9}; M5 {3,9,10}.
- **Grid:** starts at (150, 62); cells 62px wide × 19px tall with 2px row gap; 10 rows labeled "row 1"…"row 10" (right-aligned gray at left). Column headers M1–M5 in bold ink `#1a5276`, "VOTE" header in bold green `#008300`.
- **Cells:** wrong = fill `rgba(231,76,60,0.15)` with bold red `#e74c3c` "✗"; right = fill `rgba(0,131,0,0.08)` with bold green "✓".
- **Vote column (56px wide, offset 12px right of grid, outlined):** correct rows show green "✓ n–m" tallies (e.g. "✓ 4–1"); the failing row shows red "✗ 3–2" on fill `rgba(231,76,60,0.25)`. Vote is wrong when 3+ members err (only row 3).
- **Annotations:** bold red 12px "← 3 errors gang up" to the right of the row-3 vote cell; bold green 13px centered at bottom "members: 7/10 each — vote: 9/10".

## Diversity Is the Fuel

Tags: `where it's used` (blue), `rule of thumb` (blue)

- **The math** — 5 models, each 70% right, errors fully independent: the vote is right ~84% of the time
- **More members** — 9 independent models push the vote to ~90%; the curve keeps climbing
- **Clones climb nothing** — 5 copies of one model err on the same rows: the vote stays exactly 70%
- **Correlation is the tax** — partly shared errors land the vote somewhere between 70% and 84%
- **Manufacturing diversity** — different samples, features and algorithms are how real ensembles buy independence

*Example (italic):* Random forests exist to manufacture disagreement: each tree sees a random sample of rows and a random subset of features.

**Key point:** Ensemble gain is a function of two things — member accuracy and member disagreement. Fixing accuracy at 70%, disagreement alone moves the vote from 70% to 84%.

### Visualization (canvas `c3`, 720×300)

Two-line chart: majority-vote accuracy vs number of models, independent errors vs clones.

- **Title (bold 15px, `#1a5276`, top center):** "Majority Vote of 70% Models: Independent Errors vs Clones".
- **Data:** x (number of models voting) `[1, 3, 5, 7, 9]`; independent-errors curve `[70, 78, 84, 87, 90]` (binomial majority-vote accuracy, rounded); clones flat line `[70, 70, 70, 70, 70]`. Y range 60–95, ticks at 60/70/80/90 with "%" labels and light `#e5e9ef` gridlines.
- **Axes:** padding top 52 / bottom 52 / left 62 / right 185; `#999` L-shaped axes; x-axis title "number of models voting".
- **Series:** independent curve solid green `#008300` width 3 with 4px dots and bold value labels ("70%"…"90%") above each point; clones dashed `#c0392b` (dash 6/4) width 3, no dots.
- **Annotations:** bold green 13px "5 independent models: ~84%" below the middle of the green curve; bold `#c0392b` 12px "5 clones: still 70% — same errors, no outvoting" near the flat line.
- **Legend (right side):** green swatch "independent errors", `#c0392b` swatch "identical clones".

## The Mix-Up: More Copies Is Not More Ensemble

Tags: `common mistake` (red), `watch out` (orange)

- **Averaging clones** — same errors in, same errors out: the vote reproduces the single model
- **Reruns barely help** — ten random seeds of one setup make near-identical models, near-identical errors
- **Accuracy is not the tryout** — a 65% model wrong in new places can add more than another correlated 70% one
- **Check the errors** — before ensembling, compare where the candidates fail, not just how often
- **The recipes** — bagging, boosting and stacking are all just ways to manufacture disagreement

*Example (italic):* Adding 4 clones of the 70% model moves the vote nowhere; adding 4 genuinely different 70% models moves it to ~84%.

**Common mistake:** Growing an ensemble by re-training the same model — member count is not the ingredient, error disagreement is.

### Visualization (canvas `c4`, 720×300)

Four-bar comparison: what adding 4 more models of each kind buys.

- **Title (bold 15px, `#1a5276`, top center):** "Add 4 More Models to a 70% Model — What Do You Get?".
- **Bars (two-line labels below, value above):**
  - "the model / alone" — 70%, gray `#6b7280`
  - "+ 4 exact / clones" — 70%, `#c0392b`
  - "+ 4 reruns / (new seeds)" — 72%, yellow `#c98500`
  - "+ 4 diverse / models" — 84%, green `#008300`
- **Layout:** plot from x=110, width 520; baseline y=232, chart height 165, y range 60–95; bars 92px wide, alpha 0.7; y ticks 60/70/80/90 with "%" labels and `#e5e9ef` gridlines.
- **Annotations:** bold `#c0392b` 12px two-line "same errors," / "zero gain" above the clones bar; bold green 13px "only disagreement moves the vote" above the diverse bar.
- **Caption (gray 11px, bottom center):** "vote accuracy; clones and diverse follow the section math, reruns illustrative".

## Regeneration instructions

- **Template/layout:** tutorials topic page. h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (full width, border-collapse) with one row: `td.text-col` 50% (tags, bullets, `.example`, `.key-point`) and `td.viz-col` 50% (one canvas 720×300).
- **Text column structure:** `.tags` row of pill spans (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem, weight 600, radius 10px); `<ul>` of one-line bullets each opening with `<b>` in `#1a5276`; italic `.example` paragraph (`#555`, 0.9rem); `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>` lead-in.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; all data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue/ink `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; chart series use the P palette above plus `#c0392b` for alarm-red annotations.
- **Links:** none on this page (no cross-page links, no nav); in regenerated HTML any card links elsewhere use `.html` extensions.
