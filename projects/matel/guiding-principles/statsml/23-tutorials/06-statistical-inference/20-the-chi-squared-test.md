# The Chi-Squared Test

**Page type:** detail page (tutorial layout: `.card-section` blocks, each with h2 + two-column `table.layout` — text left 50% with tag pills/bullets/example/key-point, canvas right 50%)
**HTML title tag:** The Chi-Squared Test

**Subtitle:** For counts and categories: do the tallies you observed differ from what chance alone would produce?

## Is This Die Loaded? 60 Rolls, 6 Tallies

**Tags:** `core idea` (blue), `running example` (green), `counts` (orange)

- **The setup** — roll a die 60 times; a fair die should give each face about 10
- **The tallies** — faces 1-6 came up 7, 12, 9, 14, 6, 12 times
- **The suspicion** — face 4 hit 14 and face 5 only 6; the die looks lopsided
- **The catch** — a perfectly fair die almost never lands exactly 10-10-10-10-10-10
- **The test** — chi-squared totals up the surprise across all six faces at once

*Example:* Expected 10 per face; observed 7, 12, 9, 14, 6, 12 — lopsided, or just an ordinary Tuesday for a fair die?

**Key point:** counts always wobble around what's expected — chi-squared asks if the total wobble is bigger than chance normally makes.

### Visualization (canvas `c1`, 720×300)

Bar chart of observed tallies per face with the expected line at 10.

- **Title (bold 15px, `#1a5276`, top center):** "60 Rolls: Observed Tallies vs the Fair-Die Expectation".
- **Data:** observed = `[7, 12, 9, 14, 6, 12]` for faces 1–6.
- **Axes:** L-shaped gray `#999`, padding top 56, bottom 52, left 60, right 30; y max 16, right-aligned gray labels at 0, 5, 10, 15 (12px).
- **Bars:** 56px wide, fill `rgba(42,120,214,0.45)`; value labels bold 13px `#2c3e50` above each bar; category labels "face 1"…"face 6" 13px below baseline.
- **Expected line:** dashed orange `#d95926` horizontal line (dash 7/5, width 2.5) at 10, labeled bold 13px orange left-aligned: "expected: 10 each".
- **Annotation (bold 13px magenta `#d55181`, top center of plot):** "every face wobbles — is the TOTAL wobble too much?".

## Scoring the Surprise, Face by Face

**Tags:** `worked example` (green), `small numbers` (blue)

- **The recipe** — per face: (observed − expected)² ÷ expected, then add them up
- **Face 1** — (7 − 10)² / 10 = 9/10 = 0.9; face 2: (12 − 10)² / 10 = 0.4
- **All six** — 0.9 + 0.4 + 0.1 + 1.6 + 1.6 + 0.4 = 5.0; that total is chi-squared
- **Squaring** — makes shortfalls and excesses both count; dividing scales by what's expected
- **The verdict** — a fair die scores 5.0 or more about 42% of the time: p ≈ 0.42

*Example:* Faces 4 and 5 each contribute 1.6 — the biggest surprises — yet even the total (5.0) is unremarkable.

**Key point:** the die that "looked loaded" scores 5.0, and fair dice beat that 42% of the time — no evidence at all.

### Visualization (canvas `c2`, 720×300)

Bar chart of per-face surprise contributions with a sum box on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Surprise per Face: (Observed − 10)² / 10".
- **Data:** contributions = `[0.9, 0.4, 0.1, 1.6, 1.6, 0.4]`; observed counts = `[7, 12, 9, 14, 6, 12]`.
- **Axes:** L-shaped gray `#999`, padding top 56, bottom 66, left 60, right 160; y max 2.0, gray labels at 0, 0.5, 1.0, 1.5, 2.0.
- **Bars:** 50px wide, 0.75 alpha; orange `#d95926` when contribution ≥ 1.5 (faces 4 and 5), aqua `#199e70` otherwise; value labels bold 13px above bars; "face N" 12px and "saw N" (observed count) 11px gray below the baseline.
- **Sum box (right side, x = w−145, y=95, 120×78):** background `#f8f9fa`, 2px `#1a5276` border; contents centered: "add them up:" (bold 13px `#1a5276`), "χ² = 5.0" (bold 20px), "p ≈ 0.42" (12px gray).
- **Annotation (bold 13px orange, top center of plot):** "faces 4 and 5 look dramatic, contribute just 1.6 each".

## Where the 42% Comes From — the Fair-Die Scoreboard

**Tags:** `where it's used` (blue), `why it matters` (orange)

- **The scoreboard** — imagine thousands of fair dice each rolled 60 times and scored
- **The shape** — their chi-squared scores pile up near 5 and thin out past 11
- **The line** — only 5% of fair dice score above 11.1; that's the usual alarm line
- **Our die** — 5.0 sits right in the crowd; a score of 13 would be a genuine outlier
- **Everywhere** — same test checks A/B conversion counts, survey answers, defect types

*Example:* "Did signups by plan tier shift after the redesign?" is exactly the die question with different labels.

**Key point:** without the scoreboard, every wobbly tally chart looks like a finding — chi-squared says how much wobble is normal.

### Visualization (canvas `c3`, 720×300)

Chi-squared (df = 5) density curve with the observed score 5.0 and the 5% alarm line at 11.1.

- **Title (bold 15px, `#1a5276`, top center):** "Scores Fair Dice Get (60 rolls each) — Where Does 5.0 Sit?".
- **Curve:** unnormalized chi-squared df=5 density `x^1.5 · exp(−x/2)`, x from 0 to 20, normalized to peak at x=3; blue `#2a78d6` stroke width 2.5; L-shaped gray `#999` axes, padding top 56, bottom 52, left 60, right 30; x ticks 0, 5, 10, 15, 20; axis caption 12px: "chi-squared score (6 categories)".
- **Shaded region:** area under the curve beyond 5.0 filled `rgba(42,120,214,0.20)` (the 42%).
- **Our score:** solid green `#008300` vertical line (width 2.5) at 5.0, labeled bold 13px green: "our die: 5.0 — right in the crowd"; shaded-region label bold 12px blue: "42% of fair dice" / "score higher".
- **Alarm line:** dashed red `#e74c3c` vertical line (dash 5/4, width 2) at 11.1, labeled bold 13px red: "alarm line: 11.1" and 12px: "only 5% of fair dice get past this".

## What People Get Wrong: Percentages Instead of Counts

**Tags:** `common mistake` (red), `sample size` (orange)

- **The trap** — feeding percentages (11.7%, 20%, ...) into the formula erases the sample size
- **Same shape, 600 rolls** — tallies 70, 120, 90, 140, 60, 120 give chi-squared = 50
- **The flip** — at 60 rolls, p ≈ 0.42 (nothing); at 600 rolls, p < 0.000001 (loaded!)
- **Why** — the same percentage wobble on 10x the rolls is 10x harder for luck to fake
- **Fine print** — expected counts should be at least ~5 per category, or merge categories

*Example:* Two analysts see "23% vs 10%": one has 60 rolls (shrug), the other 600 (loaded die) — percentages hide which.

**Key point:** chi-squared runs on raw counts, never percentages — the same proportions mean completely different evidence at different sample sizes.

### Visualization (canvas `c4`, 720×300)

Two side-by-side tally-bar panels: identical percentages at 60 vs 600 rolls with opposite verdicts.

- **Title (bold 15px, `#1a5276`, top center):** "Identical Percentages, Opposite Verdicts".
- **Divider:** dashed `#bdc3c7` vertical line (dash 4/3) at x=360.
- **Each panel:** 6 bars (32px wide, fill `rgba(42,120,214,0.45)`, panel width 280, bar area height 130, baseline y=205), count labels bold 11px above bars, face numbers 1–6 11px gray below.
- **Left panel (x0=45):** counts `[7, 12, 9, 14, 6, 12]`, y max 16; header bold 13px `#1a5276`: "60 rolls"; result bold 14px green `#008300`: "χ² = 5.0,  p ≈ 0.42"; verdict bold 13px green: "looks fair".
- **Right panel (x0=400):** counts `[70, 120, 90, 140, 60, 120]`, y max 160; header bold 13px `#1a5276`: "600 rolls — same percentages"; result bold 14px red `#e74c3c`: "χ² = 50,  p < 0.000001"; verdict bold 13px red: "loaded die!".
- **Caption (bold 13px violet `#4a3aa7`, bottom center):** "10x the rolls, 10x the score — counts carry the evidence, percentages throw it away".

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each starting with `<b>` term in `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** each 720×300 intrinsic, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
