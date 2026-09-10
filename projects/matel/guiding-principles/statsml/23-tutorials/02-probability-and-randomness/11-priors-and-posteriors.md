# Priors & Posteriors

**Page type:** detail page (tutorial page: 4 `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Priors & Posteriors

**Subtitle:** Your belief before the data (prior) plus the data itself gives your belief after (posterior)

## Is This Coin Biased? Start Honest: 50/50

**Tags:** `core idea` (blue)

- **The suspect** — a coin from a magic shop: either fair (50% heads) or weighted (80% heads)
- **The prior** — before any flip, you have no reason to lean: 50% belief in each
- **The evidence** — every flip is data; heads point to "weighted", tails to "fair"
- **The posterior** — your belief after a flip; it becomes the prior for the next flip
- **The names** — prior is belief before the evidence, posterior is belief after

*Example:* A single head shifts belief in "weighted" from 50% to 62%, because heads come more often from a weighted coin.

**Key point:** Prior and posterior are the same belief at two moments — evidence is what moves it.

### Visualization (canvas `c1`, 720×300)

Two-panel bar chart: how each coin behaves vs the 50/50 prior belief.

- **Title (bold 15px ink `#1a5276`, centered):** "Two Hypotheses, One Honest Starting Belief".
- **Divider:** vertical dashed line `#bdc3c7` (dash 4/3) at x=360 from y=38 to y=285.
- **Left panel** — header bold 13px `#2c3e50` at y=52: "How each coin behaves". Baseline `#999` at y=235 from x=50 to x=330, bar height scale 140px for 0–100%. Two 70px-wide bars: "fair coin" at x=90, 50% heads, blue `#2a78d6`; "weighted coin" at x=220, 80% heads, orange `#d95926`. Bold 13px value labels "50% heads" / "80% heads" above; 12px labels below. Caption 12px mute `#6b7280` below: "the coin is one of these — we do not know which".
- **Right panel** — header bold 13px at y=52: "Belief before any flip (the prior)". Baseline `#999` at y=235 from x=410 to x=680, same 140px scale. Two 70px-wide bars, both 50%: "fair" at x=450 blue, "weighted" at x=580 orange; bold 13px "50%" labels above, 12px labels below. Caption bold 13px violet `#4a3aa7` below: "one head moves this to 38% / 62%".

## Ten Flips, Watch the Belief Move

**Tags:** `worked example` (green)

- **The flips** — H H T H H H T H H H: 8 heads, 2 tails
- **Update rule** — each head multiplies the odds of "weighted" by 0.8 / 0.5 = 1.6
- **Tails push back** — each tail multiplies the odds by 0.2 / 0.5 = 0.4
- **After flip 3 (a tail)** — belief drops from 72% back to 51%
- **After flip 10** — belief in "weighted" reaches 87%: leaning, not proven

*Example:* Redo it by hand: start at odds 1, multiply 1.6 per head and 0.4 per tail, then belief = odds / (1 + odds).

**Key point:** Beliefs move in both directions — evidence against the hypothesis pulls the posterior down.

### Visualization (canvas `c2`, 720×300)

Line chart: posterior belief in "weighted" after each of the ten flips.

- **Title (bold 15px ink, centered):** "Belief in \"Weighted\" After Each Flip (H H T H H H T H H H)".
- **Axes:** padding top 55, bottom 70, left 70, right 40; L-shaped axis `#999`. Y 0–100% with labels every 25% (12px mute `#6b7280`), gridlines `#e5e9ef`. Horizontal dashed mute reference line (dash 5/4) at 50%.
- **Data:** x labels `['start', 'H', 'H', 'T', 'H', 'H', 'H', 'T', 'H', 'H', 'H']`; belief values `[50, 61.5, 71.9, 50.6, 62.1, 72.4, 80.7, 62.6, 72.9, 81.1, 87.3]` (11 points, evenly spaced).
- **Series:** blue `#2a78d6` line, width 3, with 5px-radius dots; dots and x labels at tail flips ('T', indices 3 and 7) in magenta `#d55181` bold, others blue dots with plain 12px `#2c3e50` labels.
- **X-axis caption (12px mute, centered):** "flip result, in order".
- **Annotations:** bold 13px magenta near the flip-3 dip: "each tail knocks the belief back down"; bold 13px blue right-aligned near the endpoint: "87% after 10 flips — leaning, not proven".

## Different Priors, Same Ten Flips

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Skeptic** — starts at 10% ("shop coins are usually fine"), ends at 43% after the flips
- **Neutral** — starts at 50%, ends at 87%
- **Believer** — starts at 90%, ends at 98%
- **Data wins slowly** — with enough flips, all three beliefs converge to the truth
- **In practice** — priors carry small-data decisions: cold-start ranking, spam scores, rare diseases

*Example:* Ten flips were not enough to make the skeptic and the believer agree — a hundred flips nearly would.

**Key point:** The prior matters most when data is scarce; every extra observation shrinks its influence.

### Visualization (canvas `c3`, 720×300)

Three-line chart: three different priors updated on the same ten flips.

- **Title (bold 15px ink, centered):** "Three Priors, Same Flips: the Gap Narrows Slowly".
- **Axes:** padding top 55, bottom 60, left 70, right 165 (legend space); L-shaped axis `#999`. Y 0–100% with labels every 25% (12px mute), gridlines `#e5e9ef`. X: flips seen, 0–10, tick labels every 2 (12px `#2c3e50`), caption 12px mute: "flips seen".
- **Data (11 points each):**
  - Skeptic, aqua `#199e70`: `[10, 15.1, 22.1, 10.2, 15.4, 22.6, 31.8, 15.7, 23.0, 32.3, 43.3]`
  - Neutral, blue `#2a78d6`: `[50, 61.5, 71.9, 50.6, 62.1, 72.4, 80.7, 62.6, 72.9, 81.1, 87.3]`
  - Believer, violet `#4a3aa7`: `[90, 93.5, 95.8, 90.2, 93.6, 95.9, 97.4, 93.8, 96.0, 97.5, 98.4]`
  Each line width 3 with 3.5px-radius dots.
- **Legend (right side, 12px, color swatch squares):** violet "believer: 90 → 98%"; blue "neutral: 50 → 87%"; aqua "skeptic: 10 → 43%".
- **Annotation (bold 13px orange `#d95926`, under legend, two lines):** "10 flips did not close" / "an 80-point prior gap".

## The Common Confusion: 8 Heads Does Not Prove Bias

**Tags:** `common mistake` (red)

- **Fair coins do this** — a fair coin gives 8 or more heads in 10 flips about 5.5% of the time
- **Posterior ≠ verdict** — 87% belief still leaves a 13% chance the coin is fair
- **Rare ≠ impossible** — surprising runs from fair coins happen all the time at scale
- **Keep flipping** — the fix for an unsure posterior is more evidence, not a forced call

*Example:* If 1,000 people each flip a fair coin 10 times, about 55 of them will see 8+ heads by pure luck.

**Key point:** A posterior is a degree of belief to act on, not a proof — report it as a probability.

### Visualization (canvas `c4`, 720×300)

Binomial bar chart: heads counts in 10 flips of a fair coin, with the 8+ tail highlighted.

- **Title (bold 15px ink, centered):** "Heads in 10 Flips of a FAIR Coin".
- **Axes:** padding top 55, bottom 65, left 70, right 40; L-shaped axis `#999`. Y 0–26% with labels every 5% up to 25% (12px mute), gridlines `#e5e9ef`. X: number of heads 0–10.
- **Data (exact binomial(10, 0.5) probabilities in percent):** `[0.1, 1.0, 4.4, 11.7, 20.5, 24.6, 20.5, 11.7, 4.4, 1.0, 0.1]` for k = 0…10.
- **Bars:** 11 bars, each 72% of its slot width; bars for k ≥ 8 in orange `#d95926`, the rest blue `#2a78d6`. Value labels (one decimal) shown only for probabilities ≥ 1%: bold 12px orange for k ≥ 8, plain 12px mute otherwise. X labels 12px `#2c3e50` (0–10); caption 12px mute: "number of heads".
- **Annotation (bold 13px orange, right-aligned near the top):** "8+ heads: about 5.5% of the time — from a perfectly fair coin".

## Regeneration instructions

- **Template:** tutorials topic-page layout. `<h1>` concept name (no index number), `.subtitle` line, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pill row, a 4–5 bullet `<ul>` (each `<li>` opens with a `<b>` term in `#1a5276`), one italic `.example` paragraph, one `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; table cells padded 12px, no borders; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius; `.key-point` background `#f8f9fa`, 3px solid `#e74c3c` left border, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` / `#1a5276`; green: bg `rgba(39,174,96,0.15)` / `#27ae60`; red: bg `rgba(231,76,60,0.12)` / `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Canvas:** all canvases 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart titles bold 15px, labels 12–13px. Hardcoded literal data arrays, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
