# Hypothesis Testing

**Page type:** detail page (tutorial layout: `.card-section` blocks)
**HTML title tag:** Hypothesis Testing

**Subtitle:** Assume nothing unusual is going on, then ask one question: could plain luck explain what I'm seeing? A coin flipped 100 times carries the whole idea.

## A Coin Shows 61 Heads in 100 Flips — Cheating or Luck?

Tags: `core idea` (blue), `courtroom logic` (orange)

- **The setup** — a friend's coin lands heads 61 times out of 100; he swears it is fair
- **The trap** — 61 is more than 50, but a fair coin almost never lands exactly 50/50
- **The courtroom move** — treat the coin as innocent (fair) until the evidence is strong
- **The real question** — not "is 61 above 50?" but "could plain luck produce 61?"
- **The verdict rule** — if luck alone would rarely do this, stop believing "fair"

*Example (italic):* A defendant is presumed innocent; the coin is presumed fair — the data plays prosecutor.

**Hypothesis testing:** assume the boring explanation (a fair coin), then measure how badly the data clashes with it.

This section's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c1a`, 310×300)

Two-bar chart of the observed flip result against the fair-coin expectation.

- **Title (bold 15px, `#1a5276`, top center):** "The Evidence: 100 Flips".
- **Bars:** "heads" = 61 in `#2a78d6` and "tails" = 39 in `#d95926`, both at 75% alpha; bar width 70, gap 40, left edge x=30; baseline y=240, y scale max 70 over 165px. Value labels bold 14px above bars; category labels 13px below; gray `#999` baseline.
- **Expected-50 line:** dashed `#6b7280` horizontal (width 1.5, dash 5/4) at the 50 level, two-line label at right: "fair coin" / "expects 50".
- **Annotation (bold violet `#4a3aa7`, bottom center, two lines):** "11 heads above expected" / "— luck or bias?".

### Visualization (canvas `c1b`, 310×300)

Vertical three-step flow diagram of the courtroom logic.

- **Title (bold 15px, `#1a5276`, top center):** "The Courtroom Logic".
- **Boxes** (x=20, width = canvas−40, height 48, fill `#f8f9fa`, colored 2px border; bold 13px main line in `#1a5276`, 12px muted sub-line):
 1. y=48, border `#2a78d6`: "1. Presume innocence" / "assume the coin is fair"
 2. y=118, border `#c98500`: "2. Weigh the evidence" / "how surprising is 61 if fair?"
 3. y=188, border `#008300`: "3. Verdict" / 'rare under "fair"? reject "fair"'
- **Connectors:** gray `#6b7280` vertical arrows (line width 2 + filled triangle heads) between the boxes.
- **Footer:** bold green `#008300`: "the data must overturn the presumption"; muted 12px below: "the coin never has to prove it is fair".

## How Surprising Is 61? Working It Out by Hand

Tags: `worked example` (green), `small numbers` (blue)

- **Fair-coin average** — 100 flips of a fair coin average 50 heads
- **Typical wobble** — the head count wobbles about ±5 around 50
- **Distance** — 61 sits (61−50)/5 = 2.2 wobbles above the average
- **Tail chance** — a fair coin gives 61 or more heads only 1.8% of the time
- **Both directions** — count equally lopsided tails results too: about 3.5% total
- **Verdict** — 3.5% is under the usual 5% cutoff, so we reject "the coin is fair"

*Example (italic):* Redo it at home: expected heads 0.5 × 100 = 50; wobble √(100×0.5×0.5) = 5; distance (61−50)/5 = 2.2.

**Key point:** 61 heads is not impossible for a fair coin — just rare enough (3.5%) that "fair" stops being the best story.

### Visualization (canvas `c2`, 720×300)

Histogram of the fair-coin Binomial(100, 0.5) head-count distribution with both rejection tails highlighted red.

- **Title (bold 15px, `#1a5276`, top center):** "If the Coin Is Fair: Chance of Each Head Count (100 flips)".
- **Bars:** exact Binomial(100, 0.5) pmf for k = 35..65: `[0.0009, 0.0016, 0.0027, 0.0045, 0.0071, 0.0108, 0.0159, 0.0223, 0.0301, 0.039, 0.0485, 0.058, 0.0666, 0.0735, 0.078, 0.0796, 0.078, 0.0735, 0.0666, 0.058, 0.0485, 0.039, 0.0301, 0.0223, 0.0159, 0.0108, 0.0071, 0.0045, 0.0027, 0.0016, 0.0009]`; y scale max 0.09. Bars with k ≥ 61 or k ≤ 39 filled solid red `#e74c3c`; the rest `rgba(42,120,214,0.4)`. Padding: top 56, bottom 52, left 55, right 25; L-shaped gray `#999` axes.
- **X labels:** every 5 from 35 to 65; axis caption "heads out of 100 flips".
- **Annotations:** dashed red vertical line (width 1.5, dash 4/3) at the left edge of the k=61 bar; bold red 13px: "61+ heads: 1.8% of the time" (near the line) and "both red tails together: 3.5%" (top center); bold blue `#2a78d6` 12px mid-distribution: "luck lives here".

## What Luck Alone Can Do — and Where You Meet This Daily

Tags: `where it's used` (blue), `rule of thumb` (green)

- **Luck makes 55s** — 55 or more heads happens 18% of the time; it feels biased but is routine
- **Luck rarely makes 61s** — 20 fair-coin sessions of 100 flips: four hit 55+, none hit 61
- **A/B tests** — "the new page beat the old one" gets the same trial: could luck do that?
- **Model updates** — "v2 scored higher than v1" on one test set is 61-heads logic too
- **Without the test** — every lucky wiggle gets promoted as a real win

*Example (italic):* A team shipped a "winning" button color that was a 55-heads fluke; the lift vanished the next month.

**Key point:** the test's one job is separating results luck produces routinely from results luck almost never produces.

### Visualization (canvas `c3`, 720×300)

Dot plot of 20 fair-coin sessions with 55 and 61 reference lines.

- **Title (bold 15px, `#1a5276`, top center):** "20 Sessions With a Truly Fair Coin (100 flips each, illustrative)".
- **Data (heads per session):** `[47, 53, 50, 55, 44, 51, 58, 49, 46, 52, 54, 48, 45, 56, 50, 43, 57, 51, 49, 53]`; dots radius 5, one per session evenly spaced; dots ≥ 55 colored yellow `#c98500`, others blue `#2a78d6`.
- **Y scale:** 35–70; y labels at 40, 50, 55, 61; padding top 56, bottom 52, left 55, right 25; L-shaped gray axes.
- **Reference lines:** solid light grid line `#e5e9ef` at 50; dashed yellow `#c98500` (width 1.5, dash 5/4) at 55; dashed red `#e74c3c` at 61.
- **Annotations:** bold yellow 12px above the 55 line: "55 line: crossed 4 times — luck does this 18% of the time"; bold red 13px above the 61 line: "61 line: never crossed — luck does this only 1.8% of the time".
- **X caption:** "session number (each dot = heads in 100 fair flips)".

## What the Verdict Does and Does Not Say

Tags: `common mistake` (red), `courtroom logic` (orange)

- **Reject ≠ proof** — calling the coin biased can still be wrong; luck does hit 3.5%
- **Keep ≠ fair** — failing to reject means "not enough evidence", not "proven fair"
- **Not guilty ≠ innocent** — an acquittal is the same kind of statement
- **Weak bias hides** — a coin with a true 55% heads rate escapes this trial 87% of the time
- **5% is a convention** — a chosen tolerance for false alarms, not a law of nature

*Example (italic):* A 55%-biased coin usually lands in the 50s — a 100-flip trial rarely gathers enough evidence to convict it.

**Key point:** a hypothesis test outputs a decision under uncertainty, never a proof.

### Visualization (canvas `c4`, 720×300)

Two overlapping pmf curves (fair vs 55%-biased coin) with the "biased" verdict cutoff at 61.

- **Title (bold 15px, `#1a5276`, top center):** "A Weakly Biased Coin (55% heads) Usually Escapes the Trial".
- **Curves** (line width 3, over k = 30..70, y scale max 0.09; padding top 56, bottom 52, left 55, right 25):
  - Fair coin in `#2a78d6`: `[0, 0.0001, 0.0001, 0.0002, 0.0005, 0.0009, 0.0016, 0.0027, 0.0045, 0.0071, 0.0108, 0.0159, 0.0223, 0.0301, 0.039, 0.0485, 0.058, 0.0666, 0.0735, 0.078, 0.0796, 0.078, 0.0735, 0.0666, 0.058, 0.0485, 0.039, 0.0301, 0.0223, 0.0159, 0.0108, 0.0071, 0.0045, 0.0027, 0.0016, 0.0009, 0.0005, 0.0002, 0.0001, 0.0001, 0]`
  - 55%-biased coin in `#c98500`: `[0, 0, 0, 0, 0, 0, 0.0001, 0.0001, 0.0002, 0.0005, 0.0009, 0.0016, 0.0027, 0.0045, 0.0071, 0.0108, 0.0157, 0.0221, 0.0298, 0.0386, 0.0482, 0.0577, 0.0665, 0.0736, 0.0782, 0.08, 0.0786, 0.0741, 0.0672, 0.0584, 0.0488, 0.0391, 0.0301, 0.0222, 0.0157, 0.0106, 0.0069, 0.0043, 0.0025, 0.0014, 0.0008]`
- **Cutoff:** dashed red `#e74c3c` vertical line (width 2, dash 5/4) at k=61; bold red label right of it: '"biased" verdict zone'.
- **Shading:** area under the biased curve left of the cutoff filled `rgba(201,133,0,0.20)`.
- **Curve labels:** bold blue "fair coin" (near k=43); bold yellow "55%-biased coin" (near k=64).
- **Annotation (bold yellow 12px, upper area):** "87% of the biased coin's results land here — acquitted, not innocent".
- **X labels:** every 10 from 30 to 70; axis caption "heads out of 100 flips"; L-shaped gray axes.

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, 2px `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks, each `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout`. Every section uses `<td class="text-col">` (50%) + `<td class="viz-col">` (50%); sections 2–4 hold one 720×300 canvas. One section places canvases `c1a`/`c1b` (310×300 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`). Left cell: `.tags` pill row, `<ul>` bullets (each starting with `<b>` term in `#1a5276`), one italic `.example`, one `.key-point` callout.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c` (red on this page), padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` / `#1a5276`; green: bg `rgba(39,174,96,0.15)` / `#27ae60`; red: bg `rgba(231,76,60,0.12)` / `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Canvas:** intrinsic width/height attributes as given per chart (setup helper reads the attributes), CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML any card links use `.html` extensions (this page has none).
