# Bayesian Optimization

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Bayesian Optimization

**Subtitle:** When every trial is slow and expensive, fit a cheap belief model to the results so far and let its uncertainty choose the next experiment — few trials, smart guesses

## Twelve Batches of Cookies, One Oven Dial

**Tags:** `core idea` (blue), `surrogate model` (green), `expensive trials` (orange)

- **The bakery** — a baker tunes oven temperature (150–210°C) for taste score 0–10; each batch takes an hour
- **Three tries so far** — 155°C scored 4.6, 175°C scored 7.8, 200°C scored 6.8
- **The belief curve** — a smooth guess (the surrogate) threads the three scores it has actually seen
- **The uncertainty band** — the band is zero at tried temperatures and widens between them
- **The definition** — Bayesian optimization: model results so far, then test where the model looks best
- **The prize** — the hidden true curve peaks at 185°C with score 9.0 — untried and inside a wide band

*Example (italic):* At 175°C the band is zero (we baked there, it was 7.8); at 185°C the belief says ~7.6 give or take 1.1 — maybe great, maybe not.

**Key point:** After three batches the baker does not know the taste curve — but the surrogate says where it is known, where it is guessed, and how unsure each guess is.

### Visualization (canvas `c1`, 720×300)

Single panel: the surrogate mean line with its uncertainty band over three observed batches, plus the hidden true taste curve as a dashed reference.

- **Title (bold 15px, `#1a5276`, top center):** "What the Baker Believes After 3 Batches (illustrative)".
- **Axes:** origin x=60, plot width 620, baseline y=250, plot height 190; x maps temperature 150–210°C; y maps taste 0–10; axis lines 1.5px `#1a5276`; x ticks every 10°C labeled 12px `#444` ("150" ... "210", axis caption "oven °C"); y ticks 0, 5, 10 labeled 12px `#444`.
- **Data (13 grid points, temps 150–210 step 5):** true curve `[4.0, 4.6, 5.3, 6.1, 6.9, 7.8, 8.6, 9.0, 8.7, 7.9, 6.8, 5.5, 4.2]`; surrogate mean `[4.2, 4.6, 5.4, 6.4, 7.3, 7.8, 7.8, 7.6, 7.3, 7.0, 6.8, 6.4, 5.9]`; band half-width `[0.9, 0.0, 0.8, 1.2, 0.9, 0.0, 0.7, 1.1, 1.2, 0.9, 0.0, 0.9, 1.4]`; observed points (155, 4.6), (175, 7.8), (200, 6.8).
- **Band:** filled polygon mean±half-width, fill `rgba(42,120,214,0.18)`, no stroke; drawn first.
- **Surrogate mean:** blue `#2a78d6` 3px line over the band.
- **True curve:** dashed (dash 5/4) 2px `#6b7280` line; mute 11px label "true taste curve (hidden)" near its peak at 185°C.
- **Observed dots:** three 6px ink `#1a5276` dots with bold 12px labels "4.6", "7.8", "6.8" above each.
- **Annotation:** orange `#d95926` bold 13px near 185°C, two lines: "band still wide here —" / "the 9.0 peak could be hiding".
- **Caption (12px `#444`, bottom right):** "band = surrogate uncertainty; zero at tried temperatures".

## Picking the Fourth Batch

**Tags:** `worked example` (blue), `acquisition function` (green)

- **The question** — with one hour to spend, which temperature should batch 4 use?
- **The score** — an acquisition score rates each temperature: high mean AND high uncertainty both help
- **Read it off** — the score peaks at 0.62 at 185°C (illustrative units); the tried temps score near 0
- **Why 185** — its mean 7.6 is close to the best seen (7.8) and its band ±1.1 leaves room above it
- **The payoff** — batch 4 at 185°C bakes a 9.0 — a new best found on the fourth try
- **The loop** — refit the surrogate with the new point, rescore, pick again; that is the whole algorithm

*Example (italic):* 165°C has a wider band (±1.2) but a lower mean (6.4), so it scores 0.35 — promising, yet 185°C's 0.62 wins the hour.

**Key point:** The acquisition function turns "where should I experiment next?" into arithmetic: it trades off the surrogate's mean (exploit) against its band (explore) at every candidate.

### Visualization (canvas `c2`, 720×300)

Two stacked panels sharing one x-axis: the surrogate with band on top, the acquisition curve below, with an arrow linking the acquisition peak to the chosen temperature.

- **Title (bold 15px, `#1a5276`, top center):** "The Acquisition Score Picks Batch 4: 185°C (illustrative)".
- **Shared x:** x=60 to x=680 maps 150–210°C; tick labels 12px `#444` only on the bottom panel.
- **Top panel (surrogate):** y from 40 to 160 maps taste 3–10; same 13-point mean `[4.2, 4.6, 5.4, 6.4, 7.3, 7.8, 7.8, 7.6, 7.3, 7.0, 6.8, 6.4, 5.9]` and band half-width `[0.9, 0.0, 0.8, 1.2, 0.9, 0.0, 0.7, 1.1, 1.2, 0.9, 0.0, 0.9, 1.4]` as in c1; band fill `rgba(42,120,214,0.18)`, mean blue `#2a78d6` 2.5px; three 5px ink dots at (155,4.6), (175,7.8), (200,6.8); panel label bold 12px `#1a5276` at left: "belief (mean ± band)"; dashed 1px `#e5e9ef` horizontal line at best-so-far 7.8, mute 11px label "best so far 7.8".
- **Bottom panel (acquisition):** y from 195 to 275 maps score 0–0.7; acquisition values (same 13 temps) `[0.10, 0.00, 0.12, 0.35, 0.28, 0.02, 0.30, 0.62, 0.55, 0.25, 0.01, 0.18, 0.30]`; filled area under the curve `rgba(0,131,0,0.25)` with green `#008300` 2.5px line; panel label bold 12px `#008300` at left: "acquisition score"; 6px green dot at the peak (185, 0.62) labeled bold 12px "0.62".
- **Arrow:** green `#008300` 2px vertical arrow at x(185°C) from the acquisition peak up to the top panel, arrowhead up; bold 13px green label beside it: "bake batch 4 at 185°C".
- **Caption (12px `#444`, bottom right):** "score is near 0 where we already baked — nothing left to learn there".

## Why Tuning a Model Feels Like Baking

**Tags:** `where it's used` (blue), `hyperparameter tuning` (orange), `rule of thumb` (green)

- **Same shape** — swap oven °C for learning rate and taste for validation accuracy: same problem
- **Expensive black box** — one training run can cost hours of GPU time; you cannot try everything
- **Grid search** — marches 150, 155, 160... blindly; it reached the 9.0 recipe on batch 8
- **Random search** — lucky early (6.8 first try) but wandered; it hit 9.0 on batch 11
- **Bayesian optimization** — used every result to aim the next trial; it hit 9.0 on batch 4
- **Rule of thumb** — the slower one trial is, the more it pays to think between trials

*Example (italic):* A tuning job at one training run per hour: BO finished by lunch (4 runs); grid search needed a full working day (8 runs).

**Key point:** Grid and random search never look at their own results; Bayesian optimization spends a little math between trials to save a lot of expensive trials.

### Visualization (canvas `c3`, 720×300)

Best-so-far line chart: three search strategies over the same 12-batch budget, each line stepping up as its best taste score improves.

- **Title (bold 15px, `#1a5276`, top center):** "Best Score Found vs Batches Used (illustrative)".
- **Axes:** origin x=60, plot width 590, baseline y=245, plot height 180; x maps batch 1–12 with integer ticks 12px `#444`, axis caption "batches baked"; y maps taste 3–10, ticks 4, 6, 8, 10 labeled 12px `#444`.
- **Data (best-so-far, batches 1–12):** BO blue `#2a78d6` `[4.6, 7.8, 7.8, 9.0, 9.0, 9.0, 9.0, 9.0, 9.0, 9.0, 9.0, 9.0]`; grid orange `#d95926` `[4.0, 4.6, 5.3, 6.1, 6.9, 7.8, 8.6, 9.0, 9.0, 9.0, 9.0, 9.0]`; random violet `#4a3aa7` `[6.8, 6.8, 7.9, 7.9, 7.9, 8.6, 8.6, 8.6, 8.7, 8.7, 9.0, 9.0]`.
- **Lines:** 3px step-after lines with 4px dots at each batch; legend swatches top-left inside the plot, bold 12px: "Bayesian opt" (blue), "grid search" (orange), "random search" (violet).
- **Reference line:** dashed 1px `#e5e9ef` horizontal at 9.0, mute 11px label "best possible 9.0".
- **Annotations:** blue bold 13px at (4, 9.0): "BO: 9.0 in 4 batches"; orange bold 12px at (8, 9.0) offset below: "grid: batch 8"; violet bold 12px at (11, 9.0) offset below: "random: batch 11".
- **Caption (12px `#444`, bottom right):** "same oven, same hidden curve, same budget — only the strategy differs".

## The Greedy Baker's Trap

**Tags:** `common mistake` (red), `explore vs exploit` (orange)

- **The trap** — always baking right next to the current best is exploitation only, not Bayesian optimization
- **Two-peak curve** — this dough has a chewy peak, 7.2 at 165°C, and a taller crispy peak, 9.0 at 200°C
- **Greedy run** — batches at 160, 165, 170, 175 score 6.4, 7.2, 6.6, 5.6 — stuck circling 7.2 forever
- **Balanced run** — batches at 155, 170, 185, 200 score 5.2, 6.6, 5.4, 9.0 — the wide band pulled it right
- **Why it works** — untried regions keep wide bands, so their acquisition scores stay competitive

*Example (italic):* The greedy baker never bakes above 175°C because nearby scores look best — and never learns the 9.0 recipe exists at 200°C.

**Common mistake:** Treating "sample near the best point so far" as Bayesian optimization. Without the uncertainty band pushing exploration, you converge fast — to the wrong peak.

### Visualization (canvas `c4`, 720×300)

Dual-panel comparison on a two-peak taste curve: exploit-only sampling stuck on the small peak (left) vs explore-and-exploit finding the tall peak (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Two Bakers, Same Two-Peak Dough (illustrative)".
- **Shared data (13 temps 150–210 step 5):** two-peak true curve `[4.0, 5.2, 6.4, 7.2, 6.6, 5.6, 5.0, 5.4, 6.5, 8.0, 9.0, 8.2, 6.5]` — small peak 7.2 at 165°C, tall peak 9.0 at 200°C.
- **Left panel (exploit only):** origin x=55, plot width 280, baseline y=245, plot height 175, x maps 150–210, y maps 3–10; true curve dashed 2px `#6b7280`; four orange `#d95926` 6px dots at (160, 6.4), (165, 7.2), (170, 6.6), (175, 5.6); heading bold 12px `#444` "exploit only: bake near the best"; red `#e74c3c` bold 13px annotation, two lines: "stuck at 7.2 —" / "never tries 200°C"; caption 12px `#444` "all 4 batches crowd one peak".
- **Right panel (explore + exploit):** origin x=400, same dimensions and scales; same dashed true curve; four green `#008300` 6px dots at (155, 5.2), (170, 6.6), (185, 5.4), (200, 9.0); heading "explore + exploit: band pulls outward"; green bold 13px annotation at the tall peak: "wide band at 200°C → tried it → 9.0"; caption "4 batches spread, tall peak found".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data arrays above are hardcoded literals — no `Math.random()`; charts carrying invented numbers keep their "(illustrative)" label.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
