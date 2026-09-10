# Base Rate Fallacy: Hunt Something Rare and a Great Test Still Cries Wolf

**Page type:** detail page — card-section template (see `tutorials/llms-generative-ai/08-positional-encoding.html`)
**HTML title tag:** Base Rate Fallacy — Statistical Paradoxes

**Subtitle:** Why a test that is right 99 times out of 100 is still wrong about 91 times out of every 100 people it flags — nothing is broken, the thing it hunts is simply rare

---

## Section 1 — The Test Is Right 99 Times in 100. Its Yes Is Right 9.

**Tags:** `core idea` (blue), `count the people` (green), `rarity` (orange)

**Bullets:**
- **The setup** — one person in a thousand has the condition, and the test is right 99 times in 100
- **Test everyone** — screen 100,000 people: 100 of them truly have it and 99,900 do not
- **The catch** — the test finds 99 of those 100 sick people and misses only one
- **The flood** — it also calls 1% of the 99,900 healthy people positive, which is 999 people
- **The inbox** — 99 plus 999 makes 1,098 positive results, and only 99 of them are real
- **The answer** — a positive result is right 99 times out of 1,098, about 9 in every 100
- **Why it shocks** — the 99% describes the test, the 9% describes you after it says yes
- **The trap** — people hear 99% accurate and answer 99%, ignoring how rare the condition is

**Example line (italic):** Out of 100,000 people tested, 1,098 come back positive and only 99 of those are real — so a positive result is right about 9 times in 100.

**Key point:** "Accurate" and "how often it is right when it says yes" are two different numbers. When the condition is rare the second one collapses, because the huge healthy group contributes far more wrong yeses than the tiny sick group contributes right ones.

**Source note (`.src`):** Illustrative Example — a 1-in-1,000 condition and a test that is right 99 times in 100 on both the sick and the healthy.

### Visualization — canvas `c1`, 720×340

A waffle of every positive result the screen produces, with the true-positive block visibly dwarfed. All counts derived in the draw function from `N=100000`, `prev=0.001`, `sens=0.99`, `fpr=0.01`.

- **Derived:** 100 sick, 99 true positives, 1 missed, 99,900 healthy, 999 false alarms, 98,901 correctly cleared, 1,098 positives, 9.0% of positives real, 91.0% false. `99 + 1 + 999 + 98,901 = 100,000` exactly.
- **Waffle grid is exact:** `61 columns × 18 rows = 1,098` cells, one per positive result, painted row-major — the first 99 cells are the real cases, the remaining 999 are false alarms. No rounding anywhere.
- **Title (bold 15px `P.ink`, centered, y=22):** "Test 100,000 People — Then Look at Who Tested Positive"
- **Sub-note (12px `P.mute`, left at `LX=30`, y=44):** "Each square = one positive result. 61 × 18 = 1,098 positives." — the `1,098` printed from the computed total.
- **Grid geometry:** `LX=30`, `TOP=62`, `cell = floor(min((w − 2·LX − 150) / 61, 10))`, so `GW = 61·cell`; 18 rows tall. Real cases `rgba(0,131,0,0.55)` stroked `P.green`; false alarms `rgba(213,81,129,0.30)` stroked `rgba(213,81,129,0.55)`.
- **Legend column** at `LX + GW + 18`: an 11×11 green swatch + bold 12px `P.green` "99 real cases" and 12px `P.mute` "the test caught them"; below it a magenta swatch + bold 12px `P.magenta` "999 false alarms" and 12px `P.mute` "healthy, flagged anyway".
- **Big figure (below the grid, left at `LX`):** bold 19px `P.magenta` "9.0%" then 12px `P.mute` "of the 1,098 positives are real", both printed from the computed rate.
- **Two derivation lines (12px `P.mute`, left at `LX`, spaced 20px in pixels):** "100 truly sick → 99 found, 1 missed" and "99,900 healthy → 999 wrongly flagged" — counts printed from variables.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The test is 99% accurate. Its yes is right 9 times in 100."

---

## Section 2 — The Alert Queue Nobody Can Read

**Tags:** `where it bites` (blue), `at scale` (green), `review capacity` (orange)

**Bullets:**
- **The scale** — a payments team screens 2,000,000 card transactions every single day
- **The rarity** — 1 in 2,000 is fraud, so about 1,000 real cases hide in that stream
- **The model** — it catches 90% of the fraud, 900 cases, and misses the other 100
- **The noise** — it also flags 1% of the 1,999,000 clean payments, which is 19,990 alerts
- **The queue** — 20,890 alerts a day, 900 of them real: 23 opened per real case found
- **The capacity** — 20 reviewers at 4 minutes an alert open 2,400 a shift, 11% of the queue
- **The fallout** — 18,490 alerts are never opened, and about 797 of those were real fraud
- **The cruel part** — a model that flagged nothing at all would be 99.95% accurate

**Example line (italic):** Clearing one day's 20,890 alerts by hand would take 1,393 reviewer-hours — 174 full shifts — to find 900 real cases.

**Key point:** A rare target plus even a 1% false-alarm rate makes a queue humans cannot finish, so the cases that matter go unopened. Size the alert queue against review capacity before shipping the model.

**Source note (`.src`):** Illustrative Example — volumes, fraud rate, and review speed are constructed; every count below is computed from them.

### Visualization — canvas `c2`, 720×340

One day's queue as a single stacked bar, with the review-capacity cut marked, then the per-case cost as a strip of alert squares. All figures derived from `N=2000000`, `p=0.0005`, `sens=0.90`, `fpr=0.01`, `reviewers=20`, `minsPerAlert=4`, `shiftHours=8`.

- **Derived:** 1,000 fraud, 900 caught, 100 missed, 1,999,000 clean, 19,990 false alarms, 20,890 alerts, 4.3% real, 23 alerts per real case, 2,400 reviewed (11.5%), 18,490 unopened, ~797 real cases unopened, 1,393 reviewer-hours, 174 shifts, 99.95% accuracy for flagging nothing. `900 + 100 + 19,990 + 1,979,010 = 2,000,000`.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Day's Alert Queue — 20,890 Flags, 900 Real"
- **Scale note (12px `P.mute`, left at x=40, y=42):** "2,000,000 payments screened; 1,000 of them truly fraudulent."
- **Segment labels (y=60):** bold 12px `P.green` left at x=40 "900 real — 4.3%"; bold 12px `P.magenta` right-aligned at `w−30` "19,990 false alarms — 95.7%".
- **Stacked bar:** `BX=40`, `BW=w−70`, `BY=66`, `BH=32`. Real segment width `BW·900/20890` filled `rgba(0,131,0,0.55)` stroked `P.green`; the rest `rgba(213,81,129,0.28)` stroked `rgba(213,81,129,0.55)`.
- **Capacity cut:** dashed `P.orange` (dash 5/4) vertical line at `BX + BW·2400/20890`, from `BY−8` to `BY+BH+8`; bold 12px `P.orange` at the line + 6px, `BY+BH+22` "capacity: 2,400 opened per shift (11.5%)"; 12px `P.orange` 16px below "everything to the right is never opened".
- **Cost strip:** bold 12px `P.ink` at x=40, y=162 "ALERTS OPENED PER REAL CASE FOUND"; then 23 squares of side 18 from x=40, y=171 — the first green (`rgba(0,131,0,0.55)`/`P.green`), the other 22 magenta (`rgba(213,81,129,0.28)`/`rgba(213,81,129,0.55)`); bold 19px `P.ink` "23" at x=500 with 12px `P.mute` "opened per real fraud" beside it. Square count computed as `round(queue/tp)`.
- **Two big figures (y=246 / label y=264):** bold 19px `P.magenta` "797" at x=40 with 12px `P.mute` "real frauds in the unopened pile"; bold 19px `P.violet` "99.95%" at x=340 with 12px `P.mute` "accuracy of a model that flags nothing".
- **Footnote (12px `P.mute`, left at x=40, y=294):** "Clearing the queue by hand: 1,393 reviewer-hours a day, 174 full shifts."
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "A rare target turns a good model into an unreadable inbox."

---

## Section 3 — A Real Case: Physicians Overshot by Tenfold

**Tags:** `real case` (blue), `screening` (green), `documented error` (red)

**Bullets:**
- **The study** — in 1982 David Eddy asked physicians one plain screening question
- **The setup** — 1 woman in 100 in that age group has cancer at the time of the scan
- **The scan** — it spots 80% of real cancers and wrongly flags about 1 in 10 healthy women
- **The question** — her scan comes back positive, so what is the chance she has cancer?
- **The answers** — most physicians said about 75%, treating a positive as near-certain
- **The count** — per 100,000 women: 800 true flags plus 9,504 false ones, 10,304 in all
- **The truth** — 800 of those 10,304 positive scans are real cancers, about 8 in every 100
- **The gap** — the common answer was roughly ten times the correct one

**Example line (italic):** Per 100,000 women screened: 800 real cancers flagged, 9,504 healthy women flagged, so a positive scan is real 7.8% of the time — not 75%.

**Key point:** The physicians were not bad at arithmetic; they answered with the scan's detection rate instead of asking how often it is right when it says yes. Reported in Eddy (1982), "Probabilistic reasoning in clinical medicine".

**Source note (`.src`):** Rates as stated in the study — 1% prevalence, 80% detection, 9.6% false-positive rate. The per-100,000 counts and the 7.8% are computed from those rates.

### Visualization — canvas `c3`, 720×340

The positive-scan pile split into real and false on the left; the answer physicians gave against the answer the same rates give on the right. Every figure derived from `N=100000`, `prev=0.01`, `sens=0.80`, `fpr=0.096`.

- **Derived:** 1,000 cancers, 800 flagged, 200 missed, 99,000 healthy, 9,504 wrongly flagged, 89,496 cleared, 10,304 positives, 7.8% real, physician answer 75% is 9.7× the truth. `800 + 200 + 9,504 + 89,496 = 100,000`.
- **Title (bold 15px `P.ink`, centered, y=22):** "What a Positive Mammogram Actually Means"
- **Left panel header:** bold 12px `P.ink` at x=30, y=48 "THE 10,304 POSITIVE SCANS"; 12px `P.mute` at x=30, y=64 "per 100,000 women screened".
- **Stacked column:** x=48, width 64, from y=76 to y=276 (200px). Real share `800/10304` drawn at the bottom in `rgba(0,131,0,0.60)` stroked `P.green`; the rest above in `rgba(213,81,129,0.28)` stroked `rgba(213,81,129,0.55)`. A short `P.green` leader line from the boundary out to x=122.
- **Left labels at x=124:** bold 12px `P.magenta` "9,504 false alarms" (y=150) with 12px `P.mute` "healthy, wrongly flagged" (y=166); bold 12px `P.green` "800 real cancers" (y=262) with 12px `P.mute` "the whole true signal" (y=278).
- **Right panel header:** bold 12px `P.ink` at x=300, y=48 "CHANCE OF CANCER GIVEN A POSITIVE SCAN".
- **Two bars** on a shared 0–100% scale, `SX=310`, `SW=250`, height 28:
  - 12px `P.mute` at x=300, y=76 "what most physicians answered"; bar y=84, width `SW·0.75`, `rgba(213,81,129,0.35)` stroked `P.magenta`; bold 19px `P.magenta` "75%" 10px past the bar end (zero decimals).
  - 12px `P.mute` at x=300, y=146 "the correct answer from those same rates"; bar y=154, width `SW·7.8/100`, `rgba(0,131,0,0.55)` stroked `P.green`; bold 19px `P.green` "7.8%" 10px past the bar end, printed from the computed rate (one decimal).
- **Scale:** `#ccc` line at y=196 from `SX` to `SX+SW`; 12px `P.mute` ticks 0 / 25 / 50 / 75 / 100% at y=212.
- **Annotations:** bold 12px `P.violet` at x=300, y=242 "the common answer was 9.7× the truth" (ratio computed); 12px `P.mute` at x=300, y=264 "800 real ÷ 10,304 positives = 7.8%"; 12px `P.mute` at x=300, y=286 "The scan also missed 200 of the 1,000 cancers."
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "A positive scan moves her from 1 in 100 to about 8 in 100."

---

## Section 4 — Make the Condition Common and the Same Test Turns Honest

**Tags:** `the boundary` (blue), `rule of thumb` (green), `the fix` (orange)

**Bullets:**
- **The condition** — the trap springs only when the thing you hunt is rare in the group you test
- **Common disease, same test** — at 30 in 100 prevalence a yes is right 97.7% of the time
- **The turning point** — for a test right 99 times in 100, a yes beats even odds above 1 in 100
- **The weaker test** — the mammography numbers need about 11 in 100 before a yes is even money
- **Fix one** — test a group where the condition is common instead of screening everybody
- **Fix two** — state results as counts in a real population: 99 real, 999 false, out of 100,000
- **Fix three** — ask how often it is right when it says yes, not how often it is right overall
- **Fix four** — a second check that fails differently lifts a 9-in-100 chance to about 91 in 100

**Example line (italic):** The same 99-times-in-100 test: its yes is right 9 times in 100 when 1 person in 1,000 is affected, and 96 times in 100 when 1 in 5 is.

**Key point:** Nothing about the test changed between those two numbers — only how common the condition was in the group tested. Always ask the rarity question first; the test's own accuracy cannot answer it.

**Source note (`.src`):** Illustrative Example — both curves computed from the two tests used above, so every point on them follows from the stated detection and false-alarm rates.

### Visualization — canvas `c4`, 720×320

How often a yes is right, plotted against how common the condition is, for both tests on a stretched (logarithmic) rarity axis. Curves sampled and every marked value computed in the draw function.

- **Curves:** for each test, `right(p) = 100·p·sens / (p·sens + (1−p)·fpr)`, sampled at 200 points across the axis. Test A `sens=0.99`, `fpr=0.01` in `P.blue` width 2.5; Test B (the mammography numbers) `sens=0.80`, `fpr=0.096` in `P.violet` width 2.5.
- **Even-odds crossings, computed:** `p = fpr / (sens + fpr)` → Test A at 1 in 100, Test B at about 11 in 100.
- **Title (bold 15px `P.ink`, centered, y=22):** "The Same Test Turns Trustworthy as the Condition Gets Common"
- **Plot box:** `PX0=70`, `PX1=w−30`, `PY0=52`, `PY1=h−62`. x is prevalence from 0.05% to 60% on a log scale; y is 0–100%.
- **Grid:** `P.grid` lines at 0 / 25 / 50 / 75 / 100 with right-aligned 12px `P.mute` labels; the 50% line drawn `#999` dashed (dash 4/3) with bold 12px `P.mute` "a coin flip" above its left end.
- **Axes:** `#ccc` baseline; 12px `P.mute` x-ticks at 0.1% / 0.3% / 1% / 3% / 10% / 30%; x title "how common the condition is in the group you test"; rotated y title "how often a yes is right".
- **Curve labels:** 12px `P.blue` "test right 99 times in 100" placed on the blue curve near 5% prevalence; 12px `P.violet` "the mammography test" on the violet curve near 5%.
- **Markers:** `P.magenta` dot on blue at 0.1% prevalence, label "1 in 1,000 → 9.0%"; `P.orange` dot at the blue even-odds crossing, label "even odds at 1 in 100"; `P.orange` dot at the violet crossing, label "even odds at 11 in 100" set below the 50% line; `P.green` dot on blue at 30%, label "30 in 100 → 97.7%" right-aligned below the point. Every printed value read from the curve function, never typed.
- **Caption (bold 13px `P.ink`, centered, `h−8`):** "Rarity, not the test, is what makes a yes meaningless."

---

## Regeneration instructions

- **Template:** the card-section layout from `tutorials/llms-generative-ai/08-positional-encoding.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. The canvas is capped at 720px, so a wide cell leaves slack — centering puts the chart in the middle of the right half instead of flush against the text.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>term</b>` → one italic `.example` line → one `.key-point` callout → optional `.src` note. No paragraph blocks, no `.philosophy` box, no data tables, no `.labels` strip, no `.subhead` line.
- **Bullets:** 6–8 per section, each ONE line that does not wrap at 50% column width (~≤100 characters), opening with a `<b>bold term</b>` followed by an em dash and the fact.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.red` `rgba(231,76,60,0.12)`/`#e74c3c`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart `height`. `setup(id)` caches the logical size in `dataset` on the first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart section header bold 12–13px; body/axis labels 12px (floor); the single big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Green is the honest/real quantity, magenta the misleading one, orange the mechanism (the false-alarm rate, the capacity cut).
- **Determinism:** no `Math.random()` anywhere. Every chart is arithmetic on stated rates, so no pseudo-random data is needed; a shared `ppv(prev, sens, fpr)` helper computes how often a yes is right and every printed percentage comes from it. Counts are computed from population size and rates in the draw function so a label can never drift from the plotted bar.
- **Chart order in the document:** `c1`, `c2`, `c3`, `c4` — one per section, in order.
