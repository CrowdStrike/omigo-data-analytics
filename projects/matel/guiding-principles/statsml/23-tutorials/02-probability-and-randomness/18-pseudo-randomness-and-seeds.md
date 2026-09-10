# Pseudo-Randomness & Seeds

**Page type:** detail page (tutorial card-sections: h2 with blue underline per section, two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** Pseudo-Randomness & Seeds

**Subtitle:** Computers don't roll dice — they run a formula from a starting number called a seed, so the same seed replays the same "randomness".

## Same Seed, Same Split, Every Time

**Tags:** `core idea` (blue), `running example` (green)

- **The task** — split 20 rows into train and test "at random" before fitting a model
- **Run 1, seed 42** — rows 2, 5, 7, 10, 11, 14, 15, 19, 20 land in the test set
- **Run 2, seed 42** — the exact same nine rows land in test again; nothing varies
- **Run 3, seed 7** — a different seed gives a completely different split
- **The name** — pseudo-random: output that looks random but is fully determined by the seed

*Example:* random_state=42 isn't a magic number — it's a bookmark into one fixed sequence.

**Key point:** The seed is the whole secret — fix it and the randomness replays exactly; change it and you get a fresh sequence.

### Visualization (canvas `c1`, 720×300)

Diagram: three horizontal rows of 20 colored cells showing train/test assignment across three runs.

- **Title (bold 15px, `#1a5276`, top center):** "20 Rows Split into Train / Test — Three Runs".
- **Data:** assignment strings (E = test, T = train): run 1 and run 2 both use seed 42 → `TETTETETTEETTEETTTEE` (test rows at positions 2, 5, 7, 10, 11, 14, 15, 19, 20); run 3 uses seed 7 → `EETTTEETTTEETTEETTET` (also 9 test rows — a fixed test fraction changes which rows are test, not how many). Strings were generated once with mulberry32(42) and mulberry32(7).
- **Layout:** rows start at x=150, cell width 26, cell height 34; rows at y=52 ("run 1, seed 42"), y=102 ("run 2, seed 42"), y=178 ("run 3, seed 7"). Row labels bold 13px right-aligned; runs 1–2 labels in ink `#1a5276`, run 3 label in violet `#4a3aa7`.
- **Cells:** test cells filled orange `#d95926` with white cell numbers 1–20 (11px); train cells filled `rgba(42,120,214,0.18)` with ink-colored numbers.
- **Bracket:** green `#008300` bracket to the right of runs 1–2 (from y=56 to y=132) with rotated bold 12px label "identical".
- **Legend (at x=150, y=236):** a `rgba(42,120,214,0.18)` swatch with blue `#2a78d6` border labeled "train", and an orange `#d95926` swatch labeled "test" (12px, text `#2c3e50`).
- **Caption (bold 13px orange `#d95926`, bottom center, y=284):** "same seed = same nine test rows, run after run; a new seed reshuffles everything".

## A Random Number Generator You Can Run on Paper

**Tags:** `worked example` (green), `hand math` (blue)

- **The formula** — next = (5 × current + 3) mod 16, starting from seed 1
- **By hand** — 1 → 8 → 11 → 10 → 5 → 12 → 15 → 14 → 9 → 0 → 3 → 2 → 13 → 4 → 7 → 6 → 1 …
- **Looks random** — the values jump around with no obvious order
- **Isn't random** — after 16 steps it returns to 1 and repeats the loop forever
- **Real generators** — the same idea with huge numbers: cycles so long you never see the repeat

*Example:* Check one step yourself: 5 × 8 + 3 = 43, and 43 mod 16 = 11.

**Key point:** Every "random" number your code produces is just the next line of a fixed, repeatable calculation.

### Visualization (canvas `c2`, 720×300)

Line chart: the toy LCG sequence over 18 steps, with the repeat point highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "next = (5 × current + 3) mod 16, Seed 1 — the Whole \"Random\" Stream".
- **Data:** sequence `[1, 8, 11, 10, 5, 12, 15, 14, 9, 0, 3, 2, 13, 4, 7, 6, 1, 8]` plotted at steps 0–17.
- **Axes:** padding top 50, bottom 56, left 58, right 30; y from 0 to 16 with gridlines and labels at 0, 5, 10, 15 (12px mute `#6b7280`, gridlines `#e5e9ef`, axis lines `#999`); x-axis label "step number" (12px mute, bottom center).
- **Series:** connected line in blue `#2a78d6`, width 2; 5px dots at each point — orange `#d95926` for the repeated tail (steps 16–17), blue otherwise; each point labeled with its value in bold 12px `#2c3e50` above the dot.
- **Shading:** the repeated tail region (steps 16–17) shaded `rgba(217,89,38,0.10)` full plot height.
- **Annotation (bold 13px orange, right-aligned near top, left of step 16):** "step 16: back to 1, 8, … — the loop repeats forever".

## Why Fake Randomness Is a Gift

**Tags:** `where it's used` (blue), `best practice` (green)

- **Reproducibility** — a seeded split means teammates and CI get the same test set, same score
- **Debugging** — a bug that appears "randomly" becomes repeatable once the seed is pinned
- **Honest variance** — rerun with 5 seeds: accuracy 84.2, 83.1, 85.0, 82.5, 84.6 (illustrative)
- **One seed hides risk** — a single run can't tell you whether the score was luck of the split
- **Caveat** — the same seed doesn't survive library upgrades or parallel execution order

*Example:* "Works on my machine" for models is often just "my seed, my split".

**Key point:** Fix the seed to reproduce a result; vary the seed to find out whether the result is real.

### Visualization (canvas `c3`, 720×300)

Bar chart: accuracy across 5 seeds with a dashed mean line.

- **Title (bold 15px, `#1a5276`, top center):** "Same Model, Five Split Seeds — Accuracy Moves on Its Own (illustrative)".
- **Data:** labels `['seed 1', 'seed 2', 'seed 3', 'seed 4', 'seed 5']`, accuracies `[84.2, 83.1, 85.0, 82.5, 84.6]`, mean 83.9.
- **Axes:** padding top 50, bottom 56, left 66, right 30; y from 80 to 87 with "%"-suffixed labels every 2 (80%, 82%, 84%, 86%) and gridlines `#e5e9ef`; axis lines `#999`; seed labels 12px mute below bars.
- **Bars:** width 82px, evenly gapped, filled aqua `#199e70`; value labels bold 12px `#2c3e50` above each bar (e.g. "84.2%").
- **Mean line:** dashed (6/4) violet `#4a3aa7`, width 2.5, at y=83.9, with bold 13px violet label "mean 83.9%" at the left above the line.
- **Caption (bold 13px orange `#d95926`, bottom center, y=288):** "nothing about the model changed — only the split; report 83.9% ± the spread".

## Don't Shop for a Lucky Seed

**Tags:** `common mistake` (red), `common confusion` (orange)

- **The temptation** — try 20 seeds, keep the best: 86.1% goes in the report; the mean was 84.1%
- **Why it's wrong** — the seed changed the split, not the model; you measured luck and kept the max
- **Same trap** — as re-running an A/B test until it happens to "win"
- **Deployment** — production data has no seed; expect the mean (84.1%), not the max (86.1%)
- **Security note** — predictable is fine for ML, fatal for passwords: use a crypto generator there

*Example:* A 2-point "improvement" that came from tuning random_state vanishes on next month's data.

**Key point:** Choose the seed before looking at results — chosen after, it's not a setting, it's a thumb on the scale.

### Visualization (canvas `c4`, 720×300)

Bar chart: 20 seed results with the cherry-picked max highlighted vs the honest mean.

- **Title (bold 15px, `#1a5276`, top center):** "Try 20 Seeds, Report the Best: What the Report Hides (illustrative)".
- **Data:** 20 accuracies `[83.6, 84.5, 82.9, 84.1, 85.2, 83.3, 84.8, 83.9, 84.4, 82.7, 84.0, 85.6, 83.1, 84.3, 86.1, 83.7, 84.9, 82.4, 84.2, 83.5]`; mean 84.1; max bar is index 14 (86.1).
- **Axes:** padding top 50, bottom 56, left 66, right 30; y from 81 to 88 with "%"-suffixed labels every 2 and gridlines `#e5e9ef`; axis lines `#999`; x-axis label "20 different values of random_state" (12px mute, centered).
- **Bars:** width = plot width / 20 (2px inner margin); the max bar (index 14) filled red `#e74c3c`, all others `rgba(42,120,214,0.45)`.
- **Mean line:** dashed (6/4) violet `#4a3aa7`, width 2.5, at y=84.1, with bold 13px violet label "honest expectation: 84.1%" at the left below the line.
- **Callout on max bar:** bold 13px red `#e74c3c` text "86.1% — the \"lucky\" seed" above-left of the max bar, with a red pointer line (width 2) down to the bar top.
- **Caption (bold 13px red `#e74c3c`, bottom center, y=288):** "reporting the max is overfitting to the seed — production gets the mean".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then 4 `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets, an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding a `<canvas>` 720×300 at `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `<ul>` 0.92rem; `li b` colored `#1a5276`.
- **Tag pills:** `.tag` inline-block 0.72rem weight 600, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)` bg / `#1a5276` text; green = `rgba(39,174,96,0.15)` / `#27ae60`; red = `rgba(231,76,60,0.12)` / `#e74c3c`; orange = `rgba(230,126,34,0.15)` / `#e67e22`.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` — italic, `#555`, 0.9rem.
- **Canvas:** logical size 720×300 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Hardcoded data arrays, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
