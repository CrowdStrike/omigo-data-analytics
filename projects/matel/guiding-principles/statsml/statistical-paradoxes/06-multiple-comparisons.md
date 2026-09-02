# Multiple Comparisons: Search Long Enough and Noise Hands You a Winner

**Page type:** detail page — card-section template (see `tutorials/llms-generative-ai/08-positional-encoding.html`)
**HTML title tag:** Multiple Comparisons — Statistical Paradoxes

**Subtitle:** Not one thing in the batch works. Check fifty of them against the usual bar and a "winner" still turns up more than nine times in ten — what you found is the searching, not the thing

---

## Section 1 — Fifty Tries at a One-in-Twenty Bar

**Tags:** `core idea` (blue), `nothing works` (green), `the bar` (orange)

**Bullets:**
- **The setup** — a team tries 50 email subject lines against the one they already send
- **The truth** — not one of the 50 is any better; every gap they see is noise
- **The usual bar** — a line "wins" if pure noise would fake that gap under 1 time in 20
- **One test, fine** — with a single test the bar is honest: a 5% chance of a fake winner
- **Fifty tests** — the chance nothing fakes a win is 0.95 multiplied by itself 50 times
- **The arithmetic** — that comes to 7.7%, so a fake winner turns up 92.3% of the time
- **How many** — 50 × 5% means about 2.5 fake winners expected, and 3 showed up here
- **The wrong read** — the team ships three subject lines that do exactly nothing

**Example line (italic):** One test at the 1-in-20 bar risks a fake winner 5.0% of the time; fifty tests at the same bar risk one 92.3% of the time.

**Key point:** The bar is set for one test. Reuse it across a batch and the batch-level risk is no longer 5% — it climbs to 1 − 0.95 raised to the number of tests, which is 92.3% at fifty.

**Source note (`.src`):** Illustrative Example — the 50 outcomes come from a seeded generator in which no subject line is better than the control.

### Visualization — canvas `c1`, 720×320

A dot grid of the 50 tests on the left, with the flagged ones lit, beside two bars comparing the batch-level risk at one test and at fifty. The flag count is derived from the seeded data; both percentages are computed from `1 − 0.95^n`.

- **Data:** seeded Park–Miller LCG, seed 42; 50 draws treated as the noise-only outcomes. A test is flagged when its draw is below 0.05. The seed gives 3 flags — the drawn count and the printed count come from the same array.
- **Title (bold 15px `P.ink`, centered, y=22):** "Fifty Tries. Nothing Works. 3 "Winners."" — the count is interpolated from the flag count, so it can never disagree with the lit circles.
- **Left header (bold 12px `P.ink`, left at x=40, y=48):** "50 SUBJECT LINES — NOT ONE IS ACTUALLY BETTER"
- **Grid:** 10 columns × 5 rows, pitch 34, first centre at `(54, 76)`, radius 13. Flagged circles filled `rgba(213,81,129,0.75)` stroked `P.magenta` with a bold 12px white "✓"; the rest filled `rgba(107,114,128,0.14)` stroked `#d6dae0`.
- **Left legend (two 12px rows at y=252 and y=270, swatch radius 6 at x=46):** `P.magenta` — "cleared the bar — 3 of 50" (count printed from the data); `P.mute` — "did not clear it — 47 of 50" (also derived).
- **Right header (bold 12px `P.ink`, left at x=436, y=48):** "CHANCE AT LEAST ONE FAKES A WIN"
- **Bars:** baseline `#ccc` at y=232, height scaled so 100% = 150px; width 66. Bar 1 at x=470 — `rgba(0,131,0,0.35)` stroked `P.green`, value `(1−0.95^1)×100`. Bar 2 at x=590 — `rgba(213,81,129,0.45)` stroked `P.magenta`, value `(1−0.95^50)×100`. Bold 19px value in the bar's own colour 8px above each bar; 12px `P.mute` "1 test" / "50 tests" 15px below the baseline.
- **Right note (three left-aligned rows at x=436):** bold 12px `P.orange` at y=264 — "50 × 5% = 2.5 fake winners expected", the 2.5 computed as `N × 0.05`; 12px `P.mute` at y=279 and y=294 — "0.95 multiplied by itself 50 times = 7.7%" / "— the chance nothing fakes a win", the 7.7% printed from `Math.pow(0.95,50)`. These sit below the bar tick labels at y=247 so nothing collides.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The winner is the searching, not the subject line."

---

## Section 2 — Hundreds of Silent Tests Before the Coffee Cools

**Tags:** `where it bites` (blue), `dashboards` (green), `never declared` (orange)

**Bullets:**
- **The morning ritual** — an analyst opens a dashboard of 12 metrics before the coffee cools
- **Cut by segment** — each metric is broken out by 8 segments: region, device, plan tier
- **Cut by window** — and each of those is read over 4 windows: day, week, month, quarter
- **The real count** — 12 × 8 × 4 = 384 comparisons, every one eyeballed for a move
- **Nothing declared** — none was written down as a test, so no correction was ever applied
- **Expected noise** — at 1 in 20, those 384 readings throw up about 19 fake movers a day
- **The forking path** — the analyst retries: new window, new segment, until something moves
- **Unrecorded tries** — 6 quiet retries alone lift the chance of a fake winner to 26.5%

**Example line (italic):** 384 readings at the 1-in-20 bar expect 19.2 fake movers; in the seeded morning, 17 of them lit up while nothing at all had changed.

**Key point:** The dangerous test count is not the one you declared — it is every cut you looked at, plus every variant you quietly retried. An unrecorded retry costs exactly as much as a declared one.

**Source note (`.src`):** Illustrative Example — the 384 readings are seeded noise; no metric moved for any real reason.

### Visualization — canvas `c2`, 720×340

A dense grid of one morning's 384 readings with the noise-driven movers lit, beside a staircase of how the risk grows with each quiet retry. Both the lit count and every staircase percentage are computed at render time.

- **Data:** seeded LCG, seed 42; `M=12`, `S=8`, `W=4`, `N = M×S×W = 384` draws. Lit when the draw is below 0.05 — the seed gives 17. The grid header multiplies `M×S×W` in code rather than printing "384" as a literal.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Morning, 384 Unrecorded Comparisons"
- **Left header (bold 12px `P.ink`, left at x=40, y=46):** "TODAY'S DASHBOARD — 384 READINGS"; 12px `P.mute` at y=60 — "12 metrics × 8 segments × 4 time windows".
- **Grid:** 24 columns × 16 rows, cell 13, origin `(40, 72)`. Lit cells filled `rgba(213,81,129,0.70)` stroked `P.magenta`; quiet cells `rgba(107,114,128,0.12)` stroked `#e3e6ea`. Inset each square by 1px.
- **Left legend (12px, at y=300 and y=316, 11×11 swatch at x=40):** `P.magenta` — "moved past the 1-in-20 bar — 17 of 384"; `P.mute` — "expected from noise alone — 19.2 of 384", the 19.2 computed as `N × 0.05`.
- **Right header (bold 12px `P.ink`, left at x=400, y=46):** "THE UNRECORDED RETRIES"; 12px `P.mute` at y=60 — "risk after k quiet tries at the same bar".
- **Staircase:** 6 bars, `k = 1…6`, baseline `#ccc` at y=250, x from 420 on a 44px pitch, width 30, height `(1−0.95^k)/0.30 × 162`. Fill `rgba(217,89,38,0.40)` stroked `P.orange`. Bold 12px `P.orange` value above each bar (`5.0`, `9.8`, `14.3`, `18.5`, `22.6`, `26.5` — all printed from the formula); 12px `P.mute` `k` beneath the baseline.
- **Right axis label (12px `P.mute`, centered at `sx0 + KMAX×sPitch/2 − 8` = x=544, y=283):** "quiet retries before one "works""; bold 12px `P.orange` left at x=400, y=300 and y=316 — "6 tries you never wrote down" / "→ 26.5% chance of a fake winner", the 26.5 carried from the last bar's computed risk.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The test count that matters is every cut you looked at."

---

## Section 3 — A Real Case: A Dead Salmon "Responded" to Photographs

**Tags:** `real case` (blue), `brain scans` (green), `Ig Nobel` (red)

**Bullets:**
- **The experiment** — a dead Atlantic salmon was put in a brain scanner and shown photographs
- **The task** — it was asked to judge what emotion each person in the photo was feeling
- **The result** — several spots in the dead fish's brain showed significant activation
- **Why** — the scanner splits the brain into about 130,000 tiny cubes, each tested alone
- **The arithmetic** — at 1 in 20, those 130,000 separate tests expect about 6,500 fake hits
- **With correction** — the adjusted bar becomes 1 in 2.6 million, and the fish goes quiet
- **The point** — the authors ran it to show how many published scans skipped that correction
- **The honour** — the paper won an Ig Nobel Prize and is now the standard teaching case

**Example line (italic):** Judging 130,000 cubes at the 1-in-20 bar expects about 6,500 to light up in a dead fish; dividing that bar by 130,000 leaves 0.05 expected.

**Key point:** Correction is not a formality — without it a dead animal produces publishable brain activity. Bennett et al. (2009), "Neural correlates of interspecies perspective taking in the post-mortem Atlantic Salmon."

**Source note (`.src`):** The salmon study is real and published; the 448-cube patch drawn here is an illustrative sample standing in for the roughly 130,000 cubes of a full scan.

### Visualization — canvas `c3`, 720×340

The same seeded patch of brain cubes drawn twice — judged at the plain bar, then at the bar divided by the cube count. Both lit counts come from the same array; both scaled-up expectations are computed.

- **Data:** seeded LCG, seed 42; `COLS=32`, `ROWS=14`, so 448 draws standing in for a patch of the scan. Left panel lights a cube when its draw is below 0.05 — the seed gives 21. Right panel lights it when the draw is below `0.05 / 130000` — none clear that, so 0.
- **Title (bold 15px `P.ink`, centered, y=22):** "The Same Dead Brain, Judged Two Ways"
- **Panels:** cell 9, `COLS × 9 = 288` wide; left origin `(40, 80)`, right origin `(392, 80)`.
- **Left header (left at x=40):** bold 12px `P.magenta` at y=52 — "NO CORRECTION"; 12px `P.mute` at y=66 — "each cube judged at 1 in 20".
- **Right header (left at x=392):** bold 12px `P.green` at y=52 — "CORRECTED FOR THE CUBE COUNT"; 12px `P.mute` at y=66 — "bar ÷ 130,000 = 1 in 2,600,000", the 2,600,000 computed as `Math.round(1 / (0.05/130000))` and comma-grouped.
- **Cubes:** lit cubes use the panel's accent — `rgba(213,81,129,0.75)` stroked `P.magenta` on the left, `rgba(0,131,0,0.55)` stroked `P.green` on the right, though nothing clears the corrected bar so the right panel draws none. Quiet cubes `rgba(107,114,128,0.13)` stroked `#e3e6ea`.
- **Per-panel figure:** bold 19px in the panel's accent at y=234 — the lit count from the data ("21" / "0"); 12px `P.mute` at y=232, offset right of that figure — "cubes lit in this patch of 448", the 448 printed from `COLS × ROWS`.
- **Per-panel arithmetic (two lines):** 12px `P.mute` at y=262 — "across all 130,000 cubes:"; then bold 12px in the panel's accent at y=277 — left "about 6,500 fake hits expected" (`130000 × 0.05`), right "0.05 expected — the fish goes quiet" (`130000 × 0.05/130000`).
- **Separator:** a `P.grid` vertical rule between the two panels at `40 + COLS×CELL + 28`, from y=44 to y=284.
- **Footnote (12px `P.mute`, left at x=40, y=305):** "Each square = one cube of brain tissue. Nothing in this fish was alive."
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "Skip the correction and a dead fish has feelings."

---

## Section 4 — Fixing It Costs You Real Findings

**Tags:** `rule of thumb` (blue), `the trade-off` (green), `what it costs` (red)

**Bullets:**
- **The strict fix** — divide the bar by the test count: 50 tests means 1 in 1,000, not 1 in 20
- **What it buys** — the chance of even one fake winner across the whole batch stays near 5%
- **What it costs** — at 1,000 tests the bar is 1 in 20,000 and real effects stop clearing it
- **The softer fix** — accept a known share of duds among the winners, say one in twenty of them
- **Where it settles** — that bar sits near 1 in 357 at 1,000 tests instead of 1 in 20,000
- **The trade** — at 1,000 tests it finds 54 of the 100 real effects; the strict bar finds 14
- **The cheapest fix** — write down how many things you will test before you look at any of them
- **The best fix** — hold every winner out and test it again on data it has never touched

**Example line (italic):** At 1,000 tests the strict bar finds 14 of the 100 real effects and no duds; the softer bar finds 54, with 2 duds among its 56 winners.

**Key point:** Both fixes are honest and they buy different things — the strict bar protects the batch from any fake winner, the softer one protects the findings. Declaring the count in advance costs nothing and prevents both problems.

**Source note (`.src`):** Illustrative Example — swept from a seeded batch in which one test in ten has a real effect of a fixed size.

### Visualization — canvas `c4`, 720×320

Both thresholds plotted against the number of tests on log axes, with the uncorrected bar as a flat reference, plus a side panel showing how many real effects each finds at 1,000 tests. Every plotted point and printed figure is computed in the draw function.

- **Construction:** for each `n`, a seeded LCG (seed 42) draws `n` outcomes; the first `round(0.10n)` carry a real shift of 3.2 standard errors, the rest are pure noise. Two-sided tail areas come from a normal approximation (Abramowitz–Stegun 7.1.26 `erf`). The strict bar is `0.05/n`. The softer bar is the largest `0.05·j/n` that the `j`-th smallest outcome still clears, which is the false-discovery-rate cutoff — described on the page as "a known share of duds allowed among the winners", never by that name.
- **Swept n:** 10, 20, 40, 70, 100, 200, 400, 700, 1,000, 2,000, 4,000, 7,000, 10,000.
- **Title (bold 15px `P.ink`, centered, y=22):** "Strict Protects the Batch. Softer Keeps the Findings."
- **Plot box:** `PX0=96`, `PX1=516`, `PY0=56`, `PY1=h−58`. x is `log10(n)` from 10 to 10,000; y is `log10(threshold)` from 1-in-10 at the top down to 1e−6 at the bottom.
- **Grid:** `P.grid` lines at 1e−1 … 1e−6 with right-aligned 12px `P.mute` labels "1 in 10", "1 in 100", "1 in 1,000", "1 in 10,000", "1 in 100,000", "1 in a million".
- **Axes:** `#ccc` baseline; 12px `P.mute` x-ticks at 10 / 100 / 1,000 / 10,000; x title "how many things you tested" at `PY1+32`. The y axis is titled **horizontally above the plot** at y=42 — "how strong a result must be to count  (lower = stricter)" — because a rotated title crosses the "1 in 100,000" tick label, which is already 72px wide at the 12px floor.
- **Series:** the uncorrected bar as a `P.magenta` dashed horizontal (dash 6/4) at 0.05, labelled bold 12px "the uncorrected 1-in-20 bar — never moves" just below it; the strict bar `P.violet` width 2.5; the softer bar `P.green` width 2.5.
- **Legend:** a two-row legend in the plot's lower-left (y=214 and y=232, 24px colour rule at x=96, 12px label at x=127) — `P.violet` "strict: bar ÷ number of tests", `P.green` "softer: a known share of duds allowed". Both curves descend rightward, so that corner is free at every window size.
- **Marker at n=1,000:** dashed `P.mute` vertical (dash 4/3), a 4px dot on each curve, and bold 12px labels placed 8px to the *right* of the dots — `P.violet` "1 in 20,000" 16px below the strict dot, `P.green` "1 in 357" 12px above the softer dot — both printed as `Math.round(1/threshold)`.
- **Side panel** at x=536, width 148: bold 12px `P.ink` at y=56 — "AT 1,000 TESTS"; 12px `P.mute` at y=70 — "100 real effects are in there". Two rows, each a 140-wide gray track `rgba(107,114,128,0.16)` 14px tall with the found share filled — row 1 at y=104 labelled bold 12px `P.violet` "strict bar" at y=92, value bold 12px "14 of 100 found" at y=134; row 2 at y=168 labelled bold 12px `P.green` "softer bar" at y=156, value "54 of 100 found" at y=198. Then 12px `P.mute` at y=222 and y=237 — "the softer bar's price:" / "2 duds among its 56 winners", the 56 computed as found + duds.
- **Caption (bold 13px `P.ink`, centered, `h−10`):** "Declare the count before you look — that fix is free."

---

## Regeneration instructions

- **Template:** the card-section layout from `tutorials/llms-generative-ai/08-positional-encoding.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. The canvas is capped at 720px, so a wide cell leaves slack — centering puts the chart in the middle of the right half instead of flush against the text.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>term</b>` → one italic `.example` line → one `.key-point` callout → one `.src` note. No paragraph blocks, no `.philosophy` box, no data tables, no `.labels` strip, no `.subhead` line, no section-accent divs.
- **Bullets:** 8 per section, each ONE line that does not wrap at 50% column width (~≤100 characters), opening with a `<b>bold term</b>` followed by an em dash and the fact.
- **Language:** "p-value" and "false discovery rate" never appear. The threshold is "the 1-in-20 bar"; controlling the discovery rate is "accepting a known share of duds among the winners".
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.red` `rgba(231,76,60,0.12)`/`#e74c3c`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart `height` (320, 340, 340, 320). `setup(id)` caches the logical size in `dataset` on the first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart section header bold 12–13px; body/axis labels 12px (floor); the single big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Magenta carries "the misleading view" — the uncorrected bar and everything it lights up. Green is the honest or safe rule. Orange is the mechanism, here the pile-up of retries. Violet is the strict correction as a named series.
- **Determinism:** no `Math.random()` anywhere. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, in every chart.
- **Every figure computed:** `1 − 0.95^n` for all batch-level risks, `M×S×W` for the reading count, `n × 0.05` for expected fake movers, `0.05/n` and `Math.round(1/threshold)` for both bars, and flag counts derived by filtering the seeded array and printing `.length`. No count is asserted as a literal in the drawing code.
- **Shared helpers:** `lcg(seed)`, `erf(x)` and `Phi(z)` for normal tail areas, and `commas(n)` for thousands grouping. `erf` uses Abramowitz–Stegun 7.1.26, accurate to about 1.5e−7.
- **Chart order in the document:** `c1`, `c2`, `c3`, `c4` — one per section, in order.
