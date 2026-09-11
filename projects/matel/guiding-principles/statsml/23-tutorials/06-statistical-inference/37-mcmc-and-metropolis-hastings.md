# MCMC & Metropolis-Hastings

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** MCMC & Metropolis-Hastings

**Subtitle:** A random walk that only ever compares two options head-to-head ends up visiting every option in proportion to a distribution you could never write down in full

## One Boardwalk, Seven Food Stalls

**Tags:** `core idea` (blue), `sampling` (green), `unknown total` (orange)

- **The critic** — wants visits to seven stalls to match popularity scores 1, 3, 6, 10, 7, 4, 2
- **The catch** — no one knows the total (33); she can only compare her stall against a neighbor
- **The target** — stall 4 scores 10 of 33, so it deserves about 30% of all her visits
- **The trick** — a random walk that uses only score ratios still produces the right visit shares
- **The name** — the walk is MCMC; its compare-and-move rule is called Metropolis-Hastings

*Example (italic):* Stall 4 scores 10 and stall 7 scores 2, so stall 4 deserves five visits for every one to stall 7 — a ratio, no total needed.

**Key point:** When you can score any single option but cannot sum over all of them, ratios are all you have — and ratios are all Metropolis-Hastings needs.

### Visualization (canvas `c1`, 720×300)

Single-panel bar chart of the seven stall popularity scores, with the visit share each stall deserves labeled above its bar.

- **Title (bold 15px, `#1a5276`, top center):** "Seven Stalls: Popularity Scores and the Visit Shares They Deserve".
- **Data:** stalls 1–7 with scores `[1, 3, 6, 10, 7, 4, 2]`; target shares `["3%", "9%", "18%", "30%", "21%", "12%", "6%"]` (scores/33, rounded).
- **Axes:** origin x=70, baseline y=240, chart height 170, plot width 580; y scale 0–11; 1px `#999` axis lines; y label 12px `#6b7280` "popularity score" rotated or placed top-left.
- **Bars:** width 52, evenly spaced across the plot; fill `rgba(42,120,214,0.45)` with 2px `#2a78d6` top edge; stall 4 alone filled `rgba(0,131,0,0.4)` with `#008300` edge to mark the busiest stall.
- **Labels:** "stall 1"..."stall 7" 12px `#444` below baseline; score value bold 12px `#2a78d6` just above each bar (green `#008300` for stall 4); target share bold 12px `#008300` above the score labels.
- **Annotation (magenta `#d55181`, bold 12px, two lines, upper right):** "the walker never sees" / "the total (33)".
- **Caption (12px `#444`, bottom center):** "scores are relative popularity — only ratios between neighbors are ever used".

## The Compare-and-Move Rule, Twelve Steps by Hand

**Tags:** `worked example` (blue), `accept/reject` (orange), `rule of thumb` (green)

- **Propose** — from the current stall, flip a coin to pick the left or the right neighbor
- **Uphill** — if the neighbor scores higher, always move: 6 → 10 is an automatic yes
- **Downhill** — if lower, move with probability new/old: from 10 down to 7, chance 0.70
- **Reject = stay** — a refused move still counts; the current stall gets logged one more time
- **Twelve steps** — from stall 2 the fixed walk logs 2, 3, 4, 5, 4, 4, 5, 5, 4, 3, 4, 4, 5

*Example (italic):* At step 5 the random draw 0.85 beat the 0.60 ratio for moving 10 → 6, so the critic stayed put at stall 4 and logged it again.

**Key point:** The rule uses only two scores at a time — here and proposed — so the unknown total 33 cancels out of every decision.

### Visualization (canvas `c2`, 720×300)

Trace plot of the fixed 12-step walk: step number on x, stall on y, with rejected proposals drawn as dashed stubs ending in an X.

- **Title (bold 15px, `#1a5276`, top center):** "Twelve Steps of the Walk (fixed illustrative sequence)".
- **Data:** positions by step 0–12: `[2, 3, 4, 5, 4, 4, 5, 5, 4, 3, 4, 4, 5]`; rejected proposals `{step 5: proposed 3, step 7: proposed 6, step 11: proposed 5}` (chain stays put on those steps).
- **Axes:** x from x=70 (step 0) to x=650 (step 12), spacing ≈48.3px; y maps stalls 1–7 from y=245 (stall 1) up to y=65 (stall 7), spacing 30px; step numbers 0–12 12px `#444` below; stall labels "1"–"7" 12px `#444` on the left.
- **Gridlines:** horizontal 1px `#e5e9ef` at each stall level.
- **Chain:** blue `#2a78d6` 3px polyline through the 13 position points, 5px blue dots at each step.
- **Rejections:** from each rejecting step's dot, a magenta `#d55181` dashed 2px stub (dash 4/3) toward the proposed stall's y, ending in a bold magenta "×" 14px at steps 5 (toward stall 3), 7 (toward stall 6), 11 (toward stall 5).
- **Annotations:** green `#008300` bold 12px near step 3: "downhill 10→7: ratio 0.70, draw 0.42 → accept"; magenta `#d55181` bold 12px near step 5: "ratio 0.60, draw 0.85 → reject, stay".
- **Caption (12px `#444`, bottom right):** "reject = log the current stall again".

## 500 Steps Later: The Shares Appear

**Tags:** `where it's used` (blue), `convergence` (green)

- **Long run** — after 500 steps the stall visit counts are 17, 44, 93, 148, 103, 64, 31
- **Match** — stall 4 collected 148/500 = 29.6% of visits against its 30.3% target share
- **Why it's used** — Bayesian posteriors are easy to score at one point, impossible to total
- **Real engines** — Stan and PyMC run this same rule with smarter proposal moves
- **Payoff** — the chain's visit histogram IS the distribution you could not write down

*Example (italic):* A posterior over a conversion rate works the same way — you can score any single rate, so the walk can sample the whole curve.

**Key point:** Run the compare-and-move rule long enough and visit frequencies converge to the target shares: MCMC turns "can score a point" into "can sample the distribution".

### Visualization (canvas `c3`, 720×300)

Grouped bar chart per stall: target share vs the walk's actual visit share after 500 steps.

- **Title (bold 15px, `#1a5276`, top center):** "Target Share vs Visit Share After 500 Steps (illustrative)".
- **Data:** stalls 1–7; target % `[3.0, 9.1, 18.2, 30.3, 21.2, 12.1, 6.1]` (scores/33); observed % `[3.4, 8.8, 18.6, 29.6, 20.6, 12.8, 6.2]` (from counts `[17, 44, 93, 148, 103, 64, 31]`, sum 500).
- **Axes:** origin x=70, baseline y=240, chart height 165, plot width 580; y scale 0–34%; ticks at 0/10/20/30 with 11px `#6b7280` labels and 1px `#e5e9ef` gridlines.
- **Bars:** per stall a pair of 24px bars, 4px inner gap; target fill `rgba(42,120,214,0.5)` with `#2a78d6` edge, observed fill `rgba(0,131,0,0.45)` with `#008300` edge; stall labels "stall 1"..."stall 7" 12px `#444` below.
- **Value labels:** on stall 4 only, bold 12px above each bar: blue "30.3%" and green "29.6%".
- **Legend (top left, 12px):** blue swatch "target (score/33)", green swatch "walk's visits (500 steps)".
- **Annotation (green `#008300`, bold 13px, upper right):** "the walk recovered the shares it was never told".

## The First Steps Lie: Burn-In and Correlation

**Tags:** `common mistake` (red), `burn-in` (orange), `rule of thumb` (green)

- **Bad start** — a chain begun at quiet stall 1 spends its first 8 steps just reaching the middle
- **Burn-in** — early steps echo the starting point, not the target; the fix is to discard them
- **Correlated** — each step sits next door to the last; 500 steps ≠ 500 independent draws
- **Not a maximizer** — the walk must keep visiting stall 3 (18%), not park forever at stall 4
- **Check it** — run several chains from different starts and keep samples only after they mix

*Example (italic):* Averaging the chain's first 8 stalls gives 2.5 — nowhere near the true average stall of about 4.1 — because the walk was still leaving its start.

**Common mistake:** Treating every chain step as an independent draw from the target. Early steps echo where you started, and neighboring steps are near-duplicates — discard burn-in and expect fewer effective samples than steps.

### Visualization (canvas `c4`, 720×300)

Trace plot of a fixed 40-step chain started at stall 1, with the first 8 steps shaded as burn-in and the rest shown wandering around the busy middle stalls.

- **Title (bold 15px, `#1a5276`, top center):** "A Chain Started at Stall 1: Burn-In, Then Business as Usual".
- **Data:** positions by step 0–40 (fixed illustrative sequence): `[1, 1, 2, 2, 3, 3, 4, 4, 4, 5, 4, 5, 5, 4, 4, 3, 4, 5, 6, 5, 4, 4, 3, 4, 4, 5, 5, 6, 5, 4, 4, 3, 2, 3, 4, 4, 5, 4, 4, 5, 4]`.
- **Axes:** x from x=70 (step 0) to x=660 (step 40), spacing ≈14.75px; y maps stalls 1–7 from y=245 (stall 1) up to y=65 (stall 7), spacing 30px; x ticks at 0/8/20/30/40 12px `#444`; stall labels "1"–"7" 12px `#444` on the left; horizontal 1px `#e5e9ef` gridlines per stall.
- **Burn-in band:** rectangle from step 0 to step 8, full chart height, fill `rgba(201,133,0,0.12)` with dashed `#c98500` right edge (dash 4/3).
- **Chain:** first 9 points (steps 0–8) in orange `#d95926` 3px polyline with 4px dots; remaining points (steps 8–40) in green `#008300` 3px polyline with 4px dots.
- **Annotations:** orange `#d95926` bold 12px, two lines, above the band: "burn-in: still leaving" / "the start — discard"; green `#008300` bold 12px, upper right: "after: wandering the busy middle, keep these".
- **Caption (12px `#444`, bottom center):** "same rule as before — only the starting point was bad".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **No randomness:** all chain sequences, counts, and shares are the hardcoded literal arrays above — the walk is a fixed illustrative sequence, never generated at render time.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
