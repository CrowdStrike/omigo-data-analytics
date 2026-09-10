# Independence

**Page type:** detail page (tutorial card-sections: one h2 per section, two-column table layout — text left 50%, canvas right 50%)
**HTML title tag:** Independence

**Subtitle:** Two coin flips don't affect each other, so you may multiply their probabilities — rain and umbrella sales do, so you may not

## Two Coin Flips: One Tells You Nothing About the Other

Tags: `core idea` (blue), `the shortcut` (green)

- **The setup** — flip a fair coin twice; each flip lands heads half the time
- **No memory** — seeing heads on flip 1 doesn't change flip 2's chances at all
- **The shortcut** — when events don't affect each other, multiply: ½ × ½ = ¼
- **Check it** — the four outcomes HH, HT, TH, TT are equally likely, 25% each
- **Definition (after the example)** — events are independent when knowing one leaves the other's odds unchanged

*Example:* Flip 100 pairs of coins: about 25 pairs come up heads-heads, just as ½ × ½ predicts.

**Key point:** Multiplying probabilities is only legal when the events are independent — that is what the shortcut assumes.

### Visualization (canvas `c1`, 720×300)

Probability tree diagram: two coin flips, four equally likely outcomes.

- **Title (bold 15px, `#1a5276`, top center):** "Two Flips: Every Path Is ½ × ½ = ¼".
- **Nodes (filled circles with white bold labels):** root "flip" (radius 17, ink `#1a5276`); flip-1 nodes "H" (blue `#2a78d6`) and "T" (violet `#4a3aa7`, radius 15); four flip-2 leaf nodes — the HH leaf green `#008300`, the other three gray `#9aa6b2`.
- **Edges:** thin gray `#aaa` lines, each labeled "½" in gray 12px.
- **Leaf labels (right of each leaf):** "HH  =  25%" (bold 14px green), "HT  =  25%", "TH  =  25%", "TT  =  25%" (13px `#2c3e50`).
- **Level labels (gray 12px, bottom):** "flip 1" under the first column, "flip 2" under the second.
- **Caption (bold orange `#d95926` 13px, bottom center):** "The second-flip odds are ½ on every branch — flip 1 changed nothing".

## Rain and Umbrella Sales: The Shortcut Silently Breaks

Tags: `worked example` (green), `shortcut misuse` (red)

- **The setup** — over 100 days: it rains on 30 (30%), umbrella sales spike on 30 (30%)
- **The shortcut says** — both on the same day: 0.30 × 0.30 = 9 days expected
- **Count the days** — sales spiked on 27 of the 30 rainy days (90%), on only 3 dry days
- **Reality** — rain AND spike happened on 27 days, three times the shortcut's 9
- **Why it broke** — rain causes umbrella buying; the events move together

*Example:* Both events are "30% likely", yet knowing it rained lifts the spike odds from 30% to 90%.

**Key point:** The multiplication gave a clean, precise, wrong answer — nothing warned you the assumption failed.

### Visualization (canvas `c2`, 720×300)

Two-panel chart split by a dashed vertical divider at x=300: a 2×2 count grid on the left, shortcut-vs-reality bars on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Days With Rain AND a Sales Spike (out of 100)".
- **Left 2×2 grid (cells 100×62 with 8px gaps):** column headers "spike" (aqua `#199e70`) and "no spike" (gray `#6b7280`); row headers "rain (30)" (blue `#2a78d6`) and "dry (70)" (yellow `#c98500`). Cell counts: 27 (highlighted — fill `rgba(25,158,112,0.20)`, aqua border), 3, 3, 67 (plain — fill `#f4f6f8`, gray border), bold 16px numbers. Below the grid: bold aqua 12px "27 of the 30 rainy days spiked (90%)" and gray 12px "actual counts over 100 days (illustrative)".
- **Right bars:** y scale 0–30 days over a 150px-high axis; two bars 120px wide — 9 days in gray `#6b7280` labeled "shortcut: 0.3 × 0.3 × 100", 27 days in orange `#d95926` labeled "actually counted"; bold 15px "9 days" / "27 days" value labels above the bars.
- **Caption (bold orange 13px, bottom of right panel):** "3x the shortcut — rain and umbrellas move together".

## Where Wrongly Assuming Independence Hurts

Tags: `where it's used` (blue), `common mistake` (red)

- **System reliability** — two servers each fail 5% of days; "both fail" = 0.25%... if independent
- **Shared cause** — same power supply: when it dies, both die — both-fail days hit 4%
- **16x off** — the shortcut underestimated the joint failure by a factor of 16
- **Correlated features** — naive Bayes multiplies feature odds as if independent; often they aren't
- **Portfolio thinking** — "diversified" bets that share one driver all crash together

*Example:* "A double failure is a 1-in-400 event" — until one storm takes out the shared power line.

**Key point:** Multiplied small probabilities look reassuringly tiny — a shared cause can make the true joint risk many times larger.

### Visualization (canvas `c3`, 720×300)

Two-bar chart: assumed vs actual joint failure rate for two servers.

- **Title (bold 15px, `#1a5276`, top center):** "\"Both Servers Down\" — Assumed vs Actual (each fails 5% of days)".
- **Data:** 0.25% vs 4%, bars labeled "if truly independent: 5% × 5%" (blue `#2a78d6`) and "with a shared power supply" (red `#e74c3c`).
- **Axes:** y 0–5% with labels every 1% and light gridlines `#e5e9ef`; padding top 55, bottom 72, left 75, right 40; gray `#999` axis lines. Sub-caption below the labels (gray 12px): "illustrative failure rates".
- **Bars:** 170px wide, evenly spaced, bold 15px value labels "0.25% of days" / "4% of days" above the bars.
- **Caption (bold red `#e74c3c` 13px, bottom center):** "One hidden shared cause made the real risk 16x the multiplied estimate".

## The One-Line Test: Compare P(B) With P(B | A)

Tags: `rule of thumb` (green), `how to check` (blue)

- **The test** — ask: does knowing A happened change the odds of B?
- **Coins pass** — P(heads on flip 2) = 50%, and P(heads 2 | heads 1) = 50%: unchanged
- **Umbrellas fail** — P(spike) = 30%, but P(spike | rain) = 90%: changed a lot
- **Equal means independent** — only then is P(A and B) = P(A) × P(B)
- **In data** — filter your table by A and recompute B's rate; a shift means dependence

*Example:* One GROUP BY rain, then a rate of spike days per group, settles the question in one query.

**Key point:** Independence is a checkable claim, not a default — verify it with a filtered count before you multiply.

### Visualization (canvas `c4`, 720×300)

Two-panel paired-bar chart split by a dashed vertical divider at x=360: the independence test applied to coins and umbrellas.

- **Title (bold 15px, `#1a5276`, top center):** "Does Knowing A Change the Odds of B?".
- **Each panel:** its own axis (baseline y=218, 130px tall, y scale 0–100%), two bars 90px wide with bold 13px % labels above and 12px labels below, panel title bold 13px at the top, verdict line bold 13px below the bars.
  - Left panel (title green `#008300`): "Coins: heads on flip 2" — bars 50% "P(H2)" and 50% "P(H2 | H1)", both blue `#2a78d6`; verdict "unchanged → independent: multiply".
  - Right panel (title red `#e74c3c`): "Umbrellas: sales spike" — bars 30% "P(spike)" (gray `#6b7280`) and 90% "P(spike | rain)" (orange `#d95926`); verdict "changed → dependent: don't multiply".
- **Caption (bold violet `#4a3aa7` 13px, bottom center):** "One filtered count is the whole test — equal bars earn you the multiplication shortcut".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then 4 `.card-section` blocks, each an `<h2>` (1.3rem, `#1a5276`, bottom border `2px solid #2980b9`) followed by `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills + `<ul>` bullets + italic `.example` + `.key-point` callout; right `td.viz-col` (50%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `<li><b>` bold terms in `#1a5276`. Fractions in bullets use HTML entities `&frac12;`/`&times;`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; red `#e74c3c` used directly for alarm bars. Overall doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all charts 720×300 logical; a shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No cross-page links; in regenerated HTML any card links would use `.html` extensions.
