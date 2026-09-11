# Why Answers Vary Between Runs

**Page type:** detail page (tutorial layout: h1 + subtitle, then one `.card-section` per concept, each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Why Answers Vary Between Runs

**Subtitle:** Run the exact same prompt three times and you get three different summaries — the model rolls a weighted die at every word, and the rolls compound

## One Prompt, Three Different Summaries

**Tags:** `core idea` (blue), `counter-intuitive` (orange)

- **The setup** — one article, one prompt, one model, run three times in a row
- **Run 1** — leads with the commute saving: "cuts commutes by 15 minutes"
- **Run 2** — leads with the timeline: "opens in March after two years of delays"
- **Run 3** — leads with the money: "the $40M line will serve 12,000 riders daily"
- **All correct** — three faithful summaries; none of them is "the" answer

*Example (italic):* Nothing changed between the runs — not the prompt, not the article, not the model.

**Key point:** By default the model samples its words, so re-running redraws — different valid answers are expected behavior.

### Visualization (canvas `c1`, 720×300)

Branching flow diagram: one prompt box fanning out to three different summary boxes.

- **Title (bold 15px, `#1a5276`, top center):** "Same Input, Three Draws"
- **Prompt box:** rectangle at x=30, y=118, 170×64; fill `rgba(26,82,118,0.10)`, 2px `#1a5276` border; two centered bold 12px `#1a5276` lines: '"Summarize the bus-line' / 'article in one sentence"'
- **Connectors:** 2px bezier curves from the prompt box's right edge to each run box, in the run's color
- **Run boxes (300×52 at x=330; fill in run color at 0.10 alpha, 1.5px border; run tag bold 12px in color at the left of the box; two 12px `#222` text lines inside; bold 12px colored lead label below the box):**
  - run 1, blue `#2a78d6` (y=62): '"The new bus line cuts downtown' / 'commutes by 15 minutes."' — lead label "lead: the commute saving"
  - run 2, aqua `#199e70` (y=132): '"The line opens in March after' / 'two years of delays."' — "lead: the timeline"
  - run 3, violet `#4a3aa7` (y=202): '"The $40M line will serve' / '12,000 riders daily."' — "lead: the money"
- **Takeaway (bold 13px orange `#d95926`, bottom center):** "all three are faithful to the article — the draw picked the emphasis"

## A Weighted Die at Every Word — and the Rolls Compound

**Tags:** `worked example` (green), `mechanism` (blue)

- **Each word** — the model rolls a weighted die over its ranked next words
- **The first fork** — opening word: "The" 60%, "Officials" 25%, "After" 15%
- **Never rejoins** — once two runs pick different words, everything downstream shifts
- **Compounding** — favorite wins 80% per word: 0.8 × 0.8 = 0.64 after just 2 words
- **Twenty words** — 0.8^20 ≈ 0.01: an identical sentence about once in 100 runs

*Example (italic):* Check it: 0.8^10 ≈ 0.11 — even a 10-word answer repeats exactly only one run in nine.

**Key point:** Small per-word randomness compounds across the sentence — identical long outputs are the exception, not the rule.

### Visualization (canvas `c2`, 720×300)

Exponential-decay line chart: probability two runs match word-for-word vs answer length.

- **Title (bold 15px, `#1a5276`, top center):** "Chance a Run Repeats the Most-Likely Wording, by Answer Length"
- **Axes:** L-shaped `#999` axis; padding top 52 / bottom 52 / left 62 / right 190; x = words in the answer, 0–20 with 12px ticks at 0, 5, 10, 15, 20; y = P(most-likely wording) %, 0–100
- **Axis captions (12px `#444`):** "words in the answer" centered below; rotated "P(most-likely wording), %" on the left
- **Curves (3px lines, computed as `p^n × 100` for n = 0..20):**
  - p = 0.95, green `#008300`
  - p = 0.90, blue `#2a78d6`
  - p = 0.80, magenta `#d55181`
- **Marked points on the p=0.8 curve (5px magenta dots with bold 12px magenta labels):** (2, 64) labeled "64%"; (10, 10.7) labeled "11%"; (20, 1.2) labeled "~1%"
- **Legend (right side, x = w−182):** 12×12 swatches with labels "95% per word" (green), "90% per word" (blue), "80% per word" (magenta), 12px `#222`
- **Annotation (orange `#d95926`, bold 13px, three lines under legend):** "at 80% per word, a" / "20-word sentence repeats" / "exactly ~1 run in 100"

## Pin It to 0, or Let It Vary?

**Tags:** `rule of thumb` (blue), `best practice` (green)

- **Pin to 0** — regression tests, evals, extraction: anything you diff run-to-run
- **Pin to 0** — comparing prompt A vs B; otherwise dice luck poses as prompt effect
- **Let it vary** — brainstorming, names, drafts: sample 5 and keep the best
- **Best-of-N** — five varied draws explore more than five identical ones
- **Log the settings** — record temperature and model version with every saved output

*Example (italic):* An A/B prompt test "won" by 3% — rerunning with sampling pinned showed the two prompts were tied.

**Key point:** Decide per task — measurement wants repeatability (temperature 0); creation wants spread (temperature up).

### Visualization (canvas `c3`, 720×300)

Two-panel tile grid: ten reruns of the same summary prompt at temperature 0 vs 0.8; vertical dashed divider (`#bdc3c7`, dash 4/3) at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Reruns of the Same Summary Prompt"
- **Tile grids:** each panel is a 5×2 grid of 46px squares (10px gaps) starting y=84; each square is colored by WHICH distinct answer came out and carries a bold 12px white letter (A, B, C, … for distinct answers). Answer-color palette in order: A blue `#2a78d6`, B aqua `#199e70`, C violet `#4a3aa7`, D yellow `#c98500`, E magenta `#d55181`, F orange `#d95926`; squares at 0.8 alpha
- **Left panel (x=50), header bold 13px `#1a5276`:** "temperature 0"; run assignment all A: `[0,0,0,0,0,0,0,0,0,0]` (1 distinct); sub-caption bold 13px green `#008300`: "1 distinct answer — diffable"
- **Right panel (x=400), header:** "temperature 0.8"; run assignment `[0,1,0,2,1,3,0,4,2,5]` (6 distinct); sub-caption bold 13px magenta `#d55181`: "6 distinct answers — pick the best"
- **Legend note (12px `#444`, centered near bottom):** "same letter = same answer text"
- **Takeaway (bold 13px orange `#d95926`, bottom center):** "pin for measurement, vary for exploration — never mix the two in one experiment"

## The Confusion: It Didn't Learn Overnight — and 0 Isn't Bit-Perfect

**Tags:** `common mistake` (red), `mechanism` (blue)

- **No memory** — the model doesn't change between runs; the dice do, not learning
- **Even at T = 0** — tiny floating-point rounding can flip a near-tie word occasionally
- **Why** — GPU math sums numbers in varying order; ties at the 8th decimal can swap
- **One flip cascades** — a single swapped word early rewrites the rest of the sentence
- **Realistic target** — "mostly identical" is the ceiling, not "bit-for-bit identical"

*Example (italic):* 100 runs at temperature 0: 97 identical, 3 diverge at one near-tie word and stay different afterwards.

**Key point:** Sampling explains most run-to-run variation, numeric jitter the last few percent — neither one is the model learning.

### Visualization (canvas `c4`, 720×300)

Two-panel chart: outcome bars for 100 reruns at temperature 0 (left) and a zoomed near-tie bar pair (right); vertical dashed divider (`#bdc3c7`, dash 4/3) at x=340.

- **Title (bold 15px, `#1a5276`, top center):** "Temperature 0, 100 Reruns — Why 3 Still Diverged"
- **Left panel (subtitle bold 13px `#1a5276`, centered at x=190):** "outcome of 100 reruns"
  - Two bars (90px wide, chart height 140, baseline y=220, scale max 100): "identical" 97, green `#008300`; "diverged" 3, orange `#d95926`; bars at 0.78 alpha; bold 14px value labels in bar color above; 12px `#222` category labels below; thin `#999` baseline
  - Note (muted `#6b7280`, 12px): "illustrative counts"
- **Right panel (subtitle bold 13px `#1a5276`, centered at x=535):** "the near-tie word where they split"
  - Two horizontal bars (24px tall) on an axis zoomed to 49.9%–50.1%: `"by"` at 50.001%, blue `#2a78d6`; `"with"` at 49.999%, violet `#4a3aa7`; word labels bold 12px `#222` right-aligned; bold 12px value labels ("50.001%", "49.999%") in bar color to the right
  - Caption (12px `#444`, centered): "axis zoomed to 49.9% – 50.1%"
  - Annotation (orange `#d95926`, bold 12px, two lines): "GPU rounding can swap a tie this close —" / "and one swap rewrites the rest"
- **Takeaway (bold 13px red `#e74c3c`, bottom center):** "none of this is the model learning — the weights never moved"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (social-graph reference style). Structure: `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks; each has an `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border. Section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem; `li b` colored `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all four canvases 720×300 logical; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and clears. All data arrays hardcoded (the c2 curves are the deterministic function `p^n`; no randomness).
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
